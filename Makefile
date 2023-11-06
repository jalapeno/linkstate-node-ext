REGISTRY_NAME?=docker.io/iejalapeno
IMAGE_VERSION?=latest

.PHONY: all linkstate-node-ext container push clean test

ifdef V
TESTARGS = -v -args -alsologtostderr -v 5
else
TESTARGS =
endif

all: linkstate-node-ext

linkstate-node-ext:
	mkdir -p bin
	$(MAKE) -C ./cmd compile-linkstate-node-ext

linkstate-node-ext-container: linkstate-node-ext
	docker build -t $(REGISTRY_NAME)/linkstate-node-ext:$(IMAGE_VERSION) -f ./build/Dockerfile.linkstate-node-ext .

push: linkstate-node-ext-container
	docker push $(REGISTRY_NAME)/linkstate-node-ext:$(IMAGE_VERSION)

clean:
	rm -rf bin

test:
	GO111MODULE=on go test `go list ./... | grep -v 'vendor'` $(TESTARGS)
	GO111MODULE=on go vet `go list ./... | grep -v vendor`
