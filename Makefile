
TEST_ES_ADDR ?= http://192.168.99.100:9200

test:
	@ES_ADDR=$(TEST_ES_ADDR) go test -cover ./...
.PHONY: test
