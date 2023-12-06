package arangodb

import (
	"context"
	"encoding/json"

	driver "github.com/arangodb/go-driver"
	"github.com/golang/glog"
	"github.com/jalapeno/topology/pkg/dbclient"
	notifier "github.com/jalapeno/topology/pkg/kafkanotifier"
	"github.com/sbezverk/gobmp/pkg/bmp"
	"github.com/sbezverk/gobmp/pkg/message"
	"github.com/sbezverk/gobmp/pkg/tools"
)

type arangoDB struct {
	dbclient.DB
	*ArangoConn
	stop      chan struct{}
	lsprefix  driver.Collection
	lssrv6sid driver.Collection
	lsnode    driver.Collection
	lsnodeExt driver.Collection
}

// NewDBSrvClient returns an instance of a DB server client process
func NewDBSrvClient(arangoSrv, user, pass, dbname, lsprefix, lssrv6sid, lsnode, lsnodeExt string) (dbclient.Srv, error) {
	if err := tools.URLAddrValidation(arangoSrv); err != nil {
		return nil, err
	}
	arangoConn, err := NewArango(ArangoConfig{
		URL:      arangoSrv,
		User:     user,
		Password: pass,
		Database: dbname,
	})
	if err != nil {
		return nil, err
	}
	arango := &arangoDB{
		stop: make(chan struct{}),
	}
	arango.DB = arango
	arango.ArangoConn = arangoConn

	// Check if vertex collection exists, if not fail as Jalapeno topology is not running
	arango.lsprefix, err = arango.db.Collection(context.TODO(), lsprefix)
	if err != nil {
		return nil, err
	}
	// Check if vertex collection exists, if not fail as Jalapeno topology is not running
	arango.lssrv6sid, err = arango.db.Collection(context.TODO(), lssrv6sid)
	if err != nil {
		return nil, err
	}
	// Check if graph exists, if not fail as Jalapeno ipv4_topology is not running
	arango.lsnode, err = arango.db.Collection(context.TODO(), lsnode)
	if err != nil {
		return nil, err
	}

	// check for ls_node_extended collection
	found, err := arango.db.CollectionExists(context.TODO(), lsnodeExt)
	if err != nil {
		return nil, err
	}
	if found {
		c, err := arango.db.Collection(context.TODO(), lsnodeExt)
		if err != nil {
			return nil, err
		}
		if err := c.Remove(context.TODO()); err != nil {
			return nil, err
		}
	}
	// create sr node collection
	var lsnodeExt_options = &driver.CreateCollectionOptions{ /* ... */ }
	glog.V(5).Infof("ls_node_extended not found, creating")
	arango.lsnodeExt, err = arango.db.CreateCollection(context.TODO(), "ls_node_extended", lsnodeExt_options)
	if err != nil {
		return nil, err
	}
	// check if collection exists, if not fail as processor has failed to create collection
	arango.lsnodeExt, err = arango.db.Collection(context.TODO(), lsnodeExt)
	if err != nil {
		return nil, err
	}

	return arango, nil
}

func (a *arangoDB) Start() error {
	if err := a.loadCollection(); err != nil {
		return err
	}
	glog.Infof("Connected to arango database, starting monitor")
	go a.monitor()

	return nil
}

func (a *arangoDB) Stop() error {
	close(a.stop)

	return nil
}

func (a *arangoDB) GetInterface() dbclient.DB {
	return a.DB
}

func (a *arangoDB) GetArangoDBInterface() *ArangoConn {
	return a.ArangoConn
}

func (a *arangoDB) StoreMessage(msgType dbclient.CollectionType, msg []byte) error {
	event := &notifier.EventMessage{}
	if err := json.Unmarshal(msg, event); err != nil {
		return err
	}
	event.TopicType = msgType
	switch msgType {
	case bmp.LSSRv6SIDMsg:
		return a.lsSRv6SIDHandler(event)
	case bmp.LSNodeMsg:
		return a.lsNodeHandler(event)
	case bmp.LSPrefixMsg:
		return a.lsPrefixHandler(event)
	}

	return nil
}

func (a *arangoDB) monitor() {
	for {
		select {
		case <-a.stop:
			// TODO Add clean up of connection with Arango DB
			return
		}
	}
}

func (a *arangoDB) loadCollection() error {
	ctx := context.TODO()
	lsn_query := "for l in " + a.lsnode.Name() + " insert l in " + a.lsnodeExt.Name() + ""
	cursor, err := a.db.Query(ctx, lsn_query, nil)
	if err != nil {
		return err
	}
	defer cursor.Close()

	// BGP-LS generates a level-1 and a level-2 entry for level-1-2 nodes
	// remove duplicate entries in the lsnodeExt collection
	dup_query := "LET duplicates = ( FOR d IN " + a.lsnodeExt.Name() +
		" COLLECT id = d.igp_router_id, domain = d.domain_id, area = d.area_id WITH COUNT INTO count " +
		" FILTER count > 1 RETURN { id: id, domain: domain, area: area, count: count }) " +
		"FOR d IN duplicates FOR m IN ls_node_extended " +
		"FILTER d.id == m.igp_router_id filter d.domain == m.domain_id RETURN m "
	pcursor, err := a.db.Query(ctx, dup_query, nil)
	if err != nil {
		return err
	}
	defer pcursor.Close()
	for {
		var doc duplicateNode
		dupe, err := pcursor.ReadDocument(ctx, &doc)

		if err != nil {
			if !driver.IsNoMoreDocuments(err) {
				return err
			}
			break
		}
		glog.Infof("Got doc with key '%s' from query\n", dupe.Key)

		if doc.ProtocolID == 1 {
			glog.Infof("remove level-1 duplicate node: %s + igp id: %s area id: %s protocol id: %v +  ", doc.Key, doc.IGPRouterID, doc.AreaID, doc.ProtocolID)
			if _, err := a.lsnodeExt.RemoveDocument(ctx, doc.Key); err != nil {
				if !driver.IsConflict(err) {
					return err
				}
			}
		}
		if doc.ProtocolID == 2 {
			update_query := "for l in " + a.lsnodeExt.Name() + " filter l._key == " + "\"" + doc.Key + "\"" +
				" UPDATE l with { protocol: " + "\"" + "ISIS Level 1-2" + "\"" + " } in " + a.lsnodeExt.Name() + ""
			cursor, err := a.db.Query(ctx, update_query, nil)
			glog.Infof("update query: %s ", update_query)
			if err != nil {
				return err
			}
			defer cursor.Close()
		}
	}

	// add sr-mpls prefix sids
	sr_query := "for p in  " + a.lsprefix.Name() +
		" filter p.mt_id_tlv.mt_id != 2 && p.prefix_attr_tlvs.ls_prefix_sid != null return p "
	cursor, err = a.db.Query(ctx, sr_query, nil)
	if err != nil {
		return err
	}
	defer cursor.Close()
	for {
		var p message.LSPrefix
		meta, err := cursor.ReadDocument(ctx, &p)
		if driver.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			return err
		}
		if err := a.processPrefixSID(ctx, meta.Key, meta.ID.String(), p); err != nil {
			glog.Errorf("Failed to process ls_prefix_sid %s with error: %+v", p.ID, err)
		}
	}

	// add srv6 sids
	srv6_query := "for s in  " + a.lssrv6sid.Name() + " return s "
	cursor, err = a.db.Query(ctx, srv6_query, nil)
	if err != nil {
		return err
	}
	defer cursor.Close()
	for {
		var p message.LSSRv6SID
		meta, err := cursor.ReadDocument(ctx, &p)
		if driver.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			return err
		}
		if err := a.processLSSRv6SID(ctx, meta.Key, meta.ID.String(), &p); err != nil {
			glog.Errorf("Failed to process ls_srv6_sid %s with error: %+v", p.ID, err)
		}
	}

	// add ipv6 iBGP peering address
	ibgp6_query := "for s in peer filter s.remote_ip like " + "\"%:%\"" + " return s "
	cursor, err = a.db.Query(ctx, ibgp6_query, nil)
	if err != nil {
		return err
	}
	defer cursor.Close()
	for {
		var p message.PeerStateChange
		meta, err := cursor.ReadDocument(ctx, &p)
		if driver.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			return err
		}
		if err := a.processibgp6(ctx, meta.Key, meta.ID.String(), &p); err != nil {
			glog.Errorf("Failed to process ibgp peering %s with error: %+v", p.ID, err)
		}
	}

	return nil
}
