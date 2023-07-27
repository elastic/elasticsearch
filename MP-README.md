Tests to run
grs -p server test --tests org.elasticsearch.action.search.TransportSearchActionTests -Dtests.iters=3
grs ':qa:multi-cluster-search:v8.10.0#multi-cluster' --tests "org.elasticsearch.search.CCSDuelIT"
grs ':server:internalClusterTest' --tests "org.elasticsearch.search.ccs.CrossClusterSearchIT"
grs ':x-pack:plugin:async-search:internalClusterTest' --tests "org.elasticsearch.xpack.search.CrossClusterAsyncSearchIT"
