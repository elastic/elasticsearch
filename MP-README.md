Tests to run
gr -p server test --tests org.elasticsearch.action.search.TransportSearchActionTests -Dtests.iters=3
gr ':qa:multi-cluster-search:v8.10.0#multi-cluster' --tests "org.elasticsearch.search.CCSDuelIT.testMatchAll"
