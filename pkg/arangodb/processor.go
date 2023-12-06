package arangodb

import (
	"context"
	"strconv"

	driver "github.com/arangodb/go-driver"
	"github.com/golang/glog"
	"github.com/sbezverk/gobmp/pkg/base"
	"github.com/sbezverk/gobmp/pkg/message"
)

func (a *arangoDB) processLSSRv6SID(ctx context.Context, key, id string, e *message.LSSRv6SID) error {
	query := "for l in " + a.lsnodeExt.Name() +
		" filter l.igp_router_id == " + "\"" + e.IGPRouterID + "\"" +
		" filter l.domain_id == " + strconv.Itoa(int(e.DomainID))
	//" filter l.domain_id == " + "\"" + strconv.Itoa(int(e.DomainID)) + "\""
	query += " return l"
	ncursor, err := a.db.Query(ctx, query, nil)
	glog.Infof("query: %+v", query)
	if err != nil {
		return err
	}
	defer ncursor.Close()
	var sn LSNodeExt
	ns, err := ncursor.ReadDocument(ctx, &sn)
	if err != nil {
		if !driver.IsNoMoreDocuments(err) {
			return err
		}
	}
	glog.Infof("ls_node_extended %s + srv6sid %s", ns.Key, e.SRv6SID)
	glog.Infof("existing sids: %+v", &sn.SIDS)

	//sids := make([]SID, 0)

	newsid := SID{
		SRv6SID:              e.SRv6SID,
		SRv6EndpointBehavior: e.SRv6EndpointBehavior,
		SRv6BGPPeerNodeSID:   e.SRv6BGPPeerNodeSID,
		SRv6SIDStructure:     e.SRv6SIDStructure,
	}
	var result bool = false
	for _, x := range sn.SIDS {
		if x == newsid {
			result = true
			break
		}
	}
	if result {
		glog.Infof("sid %+v exists in ls_node_extended document", e.SRv6SID)
	} else {

		// srn := LSNodeExt{
		// 	SIDS: append(sids, newsid),
		// }
		sn.SIDS = append(sn.SIDS, newsid)
		srn := LSNodeExt{
			SIDS: sn.SIDS,
		}
		glog.Infof("appending %s + srv6sid %+v", ns.Key, newsid)

		if _, err := a.lsnodeExt.UpdateDocument(ctx, ns.Key, &srn); err != nil {
			if !driver.IsConflict(err) {
				return err
			}
		}
	}
	return nil
}

func (a *arangoDB) processPrefixSID(ctx context.Context, key, id string, e message.LSPrefix) error {
	query := "for l in  " + a.lsnodeExt.Name() +
		" filter l.igp_router_id == " + "\"" + e.IGPRouterID + "\""
	query += " return l"
	pcursor, err := a.db.Query(ctx, query, nil)
	if err != nil {
		return err
	}
	defer pcursor.Close()
	for {
		var ln LSNodeExt
		nl, err := pcursor.ReadDocument(ctx, &ln)
		if err != nil {
			if !driver.IsNoMoreDocuments(err) {
				return err
			}
			break
		}
		glog.V(6).Infof("ls_node_extended: %s + prefix sid %v +  ", ln.Key, e.PrefixAttrTLVs.LSPrefixSID)

		obj := srObject{
			PrefixAttrTLVs: e.PrefixAttrTLVs,
		}

		if _, err := a.lsnodeExt.UpdateDocument(ctx, nl.Key, &obj); err != nil {
			if !driver.IsConflict(err) {
				return err
			}
		}
	}
	return nil
}

func (a *arangoDB) processLSNodeExt(ctx context.Context, key string, e *message.LSNode) error {
	if e.ProtocolID == base.BGP {
		// EPE Case cannot be processed because LS Node collection does not have BGP routers
		return nil
	}
	query := "for l in " + a.lsnode.Name() +
		" filter l._key == " + "\"" + e.Key + "\""
	query += " return l"
	ncursor, err := a.db.Query(ctx, query, nil)
	if err != nil {
		return err
	}
	defer ncursor.Close()
	var sn LSNodeExt
	ns, err := ncursor.ReadDocument(ctx, &sn)
	if err != nil {
		if !driver.IsNoMoreDocuments(err) {
			return err
		}
	}

	if _, err := a.lsnodeExt.CreateDocument(ctx, &sn); err != nil {
		glog.V(5).Infof("adding ls_node_extnended: %s ", sn.Key)
		if !driver.IsConflict(err) {
			return err
		}
		if err := a.findPrefixSID(ctx, sn.Key, e); err != nil {
			if err != nil {
				return err
			}
		}
		// The document already exists, updating it with the latest info
		if _, err := a.lsnodeExt.UpdateDocument(ctx, ns.Key, e); err != nil {
			return err
		}
		return nil
	}

	if err := a.processLSNodeExt(ctx, ns.Key, e); err != nil {
		glog.Errorf("Failed to process ls_node_extended %s with error: %+v", ns.Key, err)
	}
	return nil
}

func (a *arangoDB) findPrefixSID(ctx context.Context, key string, e *message.LSNode) error {
	query := "for l in " + a.lsprefix.Name() +
		" filter l.igp_router_id == " + "\"" + e.IGPRouterID + "\"" +
		" filter l.prefix_attr_tlvs.ls_prefix_sid != null"
	query += " return l"
	ncursor, err := a.db.Query(ctx, query, nil)
	if err != nil {
		return err
	}
	defer ncursor.Close()
	var lp message.LSPrefix
	pl, err := ncursor.ReadDocument(ctx, &lp)
	if err != nil {
		if !driver.IsNoMoreDocuments(err) {
			return err
		}
	}
	obj := srObject{
		PrefixAttrTLVs: lp.PrefixAttrTLVs,
	}
	if _, err := a.lsnodeExt.UpdateDocument(ctx, e.Key, &obj); err != nil {
		glog.V(5).Infof("adding prefix sid: %s ", pl.Key)
		return err
	}
	return nil
}

func (a *arangoDB) processibgp6(ctx context.Context, key, id string, e *message.PeerStateChange) error {
	query := "for l in  " + a.lsnodeExt.Name() +
		" filter l.router_id == " + "\"" + e.RemoteBGPID + "\""
	query += " return l"
	pcursor, err := a.db.Query(ctx, query, nil)
	if err != nil {
		return err
	}
	defer pcursor.Close()
	for {
		var ln LSNodeExt
		nl, err := pcursor.ReadDocument(ctx, &ln)
		if err != nil {
			if !driver.IsNoMoreDocuments(err) {
				return err
			}
			break
		}
		glog.V(6).Infof("ls_node_extended: %s + peer %v +  ", ln.Key, e.RemoteBGPID)

		obj := peerObject{
			BGPRouterID: e.RemoteIP,
		}

		if _, err := a.lsnodeExt.UpdateDocument(ctx, nl.Key, &obj); err != nil {
			if !driver.IsConflict(err) {
				return err
			}
		}
	}
	return nil
}

// processLSNodeExtRemoval removes records from the sn_node collection which are referring to deleted LSNode
func (a *arangoDB) processLSNodeExtRemoval(ctx context.Context, key string) error {
	query := "FOR d IN " + a.lsnodeExt.Name() +
		" filter d._key == " + "\"" + key + "\""
	query += " return d"
	ncursor, err := a.db.Query(ctx, query, nil)
	if err != nil {
		return err
	}
	defer ncursor.Close()

	for {
		var nm LSNodeExt
		m, err := ncursor.ReadDocument(ctx, &nm)
		if err != nil {
			if !driver.IsNoMoreDocuments(err) {
				return err
			}
			break
		}
		if _, err := a.lsnodeExt.RemoveDocument(ctx, m.ID.Key()); err != nil {
			if !driver.IsNotFound(err) {
				return err
			}
		}
	}

	return nil
}
