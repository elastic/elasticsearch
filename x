➜  elasticsearch git:(metric_name_validation) ./gradlew run -Dtests.es.logger.org.elasticsearch.telemetry.apm=debug

> Configure project :x-pack:plugin:searchable-snapshots:qa:hdfs
hdfsFixture unsupported, please set HADOOP_HOME and put HADOOP_HOME\bin in PATH
=======================================
Elasticsearch Build Hamster says Hello!
  Gradle Version        : 8.5
  OS Info               : Mac OS X 14.1.2 (aarch64)
  JDK Version           : 17.0.2+8-LTS-86 (Oracle)
  JAVA_HOME             : /Library/Java/JavaVirtualMachines/jdk-17.0.2.jdk/Contents/Home
  Random Testing Seed   : B705DEF03AA4BF36
  In FIPS 140 mode      : false
=======================================

> Task :run
[2023-12-18T13:31:10.882688Z] [BUILD] Copying additional config files from distro [/Users/przemyslawgomulka/workspace/pgomulka/elasticsearch/distribution/archives/darwin-aarch64-tar/build/install/elasticsearch-8.13.0-SNAPSHOT/config/jvm.options.d, /Users/przemyslawgomulka/workspace/pgomulka/elasticsearch/distribution/archives/darwin-aarch64-tar/build/install/elasticsearch-8.13.0-SNAPSHOT/config/users_roles, /Users/przemyslawgomulka/workspace/pgomulka/elasticsearch/distribution/archives/darwin-aarch64-tar/build/install/elasticsearch-8.13.0-SNAPSHOT/config/role_mapping.yml, /Users/przemyslawgomulka/workspace/pgomulka/elasticsearch/distribution/archives/darwin-aarch64-tar/build/install/elasticsearch-8.13.0-SNAPSHOT/config/users, /Users/przemyslawgomulka/workspace/pgomulka/elasticsearch/distribution/archives/darwin-aarch64-tar/build/install/elasticsearch-8.13.0-SNAPSHOT/config/elasticsearch.yml, /Users/przemyslawgomulka/workspace/pgomulka/elasticsearch/distribution/archives/darwin-aarch64-tar/build/install/elasticsearch-8.13.0-SNAPSHOT/config/roles.yml, /Users/przemyslawgomulka/workspace/pgomulka/elasticsearch/distribution/archives/darwin-aarch64-tar/build/install/elasticsearch-8.13.0-SNAPSHOT/config/log4j2.properties, /Users/przemyslawgomulka/workspace/pgomulka/elasticsearch/distribution/archives/darwin-aarch64-tar/build/install/elasticsearch-8.13.0-SNAPSHOT/config/elasticsearch-plugins.example.yml, /Users/przemyslawgomulka/workspace/pgomulka/elasticsearch/distribution/archives/darwin-aarch64-tar/build/install/elasticsearch-8.13.0-SNAPSHOT/config/jvm.options]
[2023-12-18T13:31:10.885813Z] [BUILD] Creating elasticsearch keystore with password set to []
[2023-12-18T13:31:11.630988Z] [BUILD] Adding 1 keystore settings and 0 keystore files
[2023-12-18T13:31:12.293546Z] [BUILD] Installing 0 modules
[2023-12-18T13:31:12.293769Z] [BUILD] Setting up 1 users
[2023-12-18T13:31:13.096132Z] [BUILD] Setting up roles.yml
[2023-12-18T13:31:13.096494Z] [BUILD] Starting Elasticsearch process
CompileCommand: exclude org/apache/lucene/util/MSBRadixSorter.computeCommonPrefixLengthAndBuildHistogram bool exclude = true
CompileCommand: exclude org/apache/lucene/util/RadixSelector.computeCommonPrefixLengthAndBuildHistogram bool exclude = true
Dec 18, 2023 2:31:14 PM sun.util.locale.provider.LocaleProviderAdapter <clinit>
WARNING: COMPAT locale provider will be removed in a future release
[2023-12-18T14:31:15,025][INFO ][o.a.l.i.v.PanamaVectorizationProvider] [runTask-0] Java vector incubator API enabled; uses preferredBitSize=128
[2023-12-18T14:31:15,437][INFO ][o.e.n.Node               ] [runTask-0] version[8.13.0-SNAPSHOT], pid[43801], build[tar/25d9bbbb5327023f7f1896fb3044fb4c9d231342/2023-12-18T13:27:39.871866Z], OS[Mac OS X/14.1.2/aarch64], JVM[Oracle Corporation/OpenJDK 64-Bit Server VM/21/21+35-2513]
[2023-12-18T14:31:15,438][INFO ][o.e.n.Node               ] [runTask-0] JVM home [/Users/przemyslawgomulka/.gradle/jdks/oracle_corporation-21-aarch64-os_x/jdk-21.jdk/Contents/Home], using bundled JDK [false]
[2023-12-18T14:31:15,438][INFO ][o.e.n.Node               ] [runTask-0] JVM arguments [-Des.networkaddress.cache.ttl=60, -Des.networkaddress.cache.negative.ttl=10, -Djava.security.manager=allow, -XX:+AlwaysPreTouch, -Xss1m, -Djava.awt.headless=true, -Dfile.encoding=UTF-8, -Djna.nosys=true, -XX:-OmitStackTraceInFastThrow, -Dio.netty.noUnsafe=true, -Dio.netty.noKeySetOptimization=true, -Dio.netty.recycler.maxCapacityPerThread=0, -Dlog4j.shutdownHookEnabled=false, -Dlog4j2.disable.jmx=true, -Dlog4j2.formatMsgNoLookups=true, -Djava.locale.providers=SPI,COMPAT, --add-opens=java.base/java.io=org.elasticsearch.preallocate, -Des.distribution.type=tar, -XX:+UseG1GC, -Djava.io.tmpdir=/Users/przemyslawgomulka/workspace/pgomulka/elasticsearch/build/testclusters/runTask-0/tmp, --add-modules=jdk.incubator.vector, -XX:CompileCommand=exclude,org.apache.lucene.util.MSBRadixSorter::computeCommonPrefixLengthAndBuildHistogram, -XX:CompileCommand=exclude,org.apache.lucene.util.RadixSelector::computeCommonPrefixLengthAndBuildHistogram, -XX:+HeapDumpOnOutOfMemoryError, -XX:+ExitOnOutOfMemoryError, -XX:HeapDumpPath=/Users/przemyslawgomulka/workspace/pgomulka/elasticsearch/build/testclusters/runTask-0/logs, -XX:ErrorFile=/Users/przemyslawgomulka/workspace/pgomulka/elasticsearch/build/testclusters/runTask-0/logs/hs_err_pid%p.log, -Xlog:gc*,gc+age=trace,safepoint:file=/Users/przemyslawgomulka/workspace/pgomulka/elasticsearch/build/testclusters/runTask-0/logs/gc.log:utctime,level,pid,tags:filecount=32,filesize=64m, -Xms512m, -Xmx512m, -ea, -esa, -Dingest.geoip.downloader.enabled.default=true, -Dio.netty.leakDetection.level=paranoid, -XX:MaxDirectMemorySize=268435456, -XX:G1HeapRegionSize=4m, -XX:InitiatingHeapOccupancyPercent=30, -XX:G1ReservePercent=15, --module-path=/Users/przemyslawgomulka/workspace/pgomulka/elasticsearch/distribution/archives/darwin-aarch64-tar/build/install/elasticsearch-8.13.0-SNAPSHOT/lib, --add-modules=jdk.net, --add-modules=ALL-MODULE-PATH, -Djdk.module.main=org.elasticsearch.server]
[2023-12-18T14:31:15,438][WARN ][o.e.n.Node               ] [runTask-0] version [8.13.0-SNAPSHOT] is a pre-release version of Elasticsearch and is not suitable for production
[2023-12-18T14:31:17,172][INFO ][o.e.p.PluginsService     ] [runTask-0] loaded module [repository-url]
[2023-12-18T14:31:17,172][INFO ][o.e.p.PluginsService     ] [runTask-0] loaded module [rest-root]
[2023-12-18T14:31:17,172][INFO ][o.e.p.PluginsService     ] [runTask-0] loaded module [x-pack-core]
[2023-12-18T14:31:17,172][INFO ][o.e.p.PluginsService     ] [runTask-0] loaded module [x-pack-redact]
[2023-12-18T14:31:17,173][INFO ][o.e.p.PluginsService     ] [runTask-0] loaded module [ingest-user-agent]
[2023-12-18T14:31:17,173][INFO ][o.e.p.PluginsService     ] [runTask-0] loaded module [x-pack-async-search]
[2023-12-18T14:31:17,173][INFO ][o.e.p.PluginsService     ] [runTask-0] loaded module [test-error-query]
[2023-12-18T14:31:17,173][INFO ][o.e.p.PluginsService     ] [runTask-0] loaded module [x-pack-monitoring]
[2023-12-18T14:31:17,173][INFO ][o.e.p.PluginsService     ] [runTask-0] loaded module [repository-s3]
[2023-12-18T14:31:17,173][INFO ][o.e.p.PluginsService     ] [runTask-0] loaded module [x-pack-analytics]
[2023-12-18T14:31:17,173][INFO ][o.e.p.PluginsService     ] [runTask-0] loaded module [x-pack-ent-search]
[2023-12-18T14:31:17,173][INFO ][o.e.p.PluginsService     ] [runTask-0] loaded module [x-pack-autoscaling]
[2023-12-18T14:31:17,173][INFO ][o.e.p.PluginsService     ] [runTask-0] loaded module [lang-painless]
[2023-12-18T14:31:17,173][INFO ][o.e.p.PluginsService     ] [runTask-0] loaded module [x-pack-ml]
[2023-12-18T14:31:17,174][INFO ][o.e.p.PluginsService     ] [runTask-0] loaded module [test-die-with-dignity]
[2023-12-18T14:31:17,174][INFO ][o.e.p.PluginsService     ] [runTask-0] loaded module [legacy-geo]
[2023-12-18T14:31:17,174][INFO ][o.e.p.PluginsService     ] [runTask-0] loaded module [lang-mustache]
[2023-12-18T14:31:17,174][INFO ][o.e.p.PluginsService     ] [runTask-0] loaded module [x-pack-ql]
[2023-12-18T14:31:17,174][INFO ][o.e.p.PluginsService     ] [runTask-0] loaded module [rank-rrf]
[2023-12-18T14:31:17,174][INFO ][o.e.p.PluginsService     ] [runTask-0] loaded module [analysis-common]
[2023-12-18T14:31:17,174][INFO ][o.e.p.PluginsService     ] [runTask-0] loaded module [test-seek-tracking-directory]
[2023-12-18T14:31:17,174][INFO ][o.e.p.PluginsService     ] [runTask-0] loaded module [health-shards-availability]
[2023-12-18T14:31:17,174][INFO ][o.e.p.PluginsService     ] [runTask-0] loaded module [transport-netty4]
[2023-12-18T14:31:17,174][INFO ][o.e.p.PluginsService     ] [runTask-0] loaded module [aggregations]
[2023-12-18T14:31:17,174][INFO ][o.e.p.PluginsService     ] [runTask-0] loaded module [ingest-common]
[2023-12-18T14:31:17,174][INFO ][o.e.p.PluginsService     ] [runTask-0] loaded module [x-pack-identity-provider]
[2023-12-18T14:31:17,175][INFO ][o.e.p.PluginsService     ] [runTask-0] loaded module [frozen-indices]
[2023-12-18T14:31:17,175][INFO ][o.e.p.PluginsService     ] [runTask-0] loaded module [x-pack-text-structure]
[2023-12-18T14:31:17,175][INFO ][o.e.p.PluginsService     ] [runTask-0] loaded module [x-pack-shutdown]
[2023-12-18T14:31:17,175][INFO ][o.e.p.PluginsService     ] [runTask-0] loaded module [snapshot-repo-test-kit]
[2023-12-18T14:31:17,175][INFO ][o.e.p.PluginsService     ] [runTask-0] loaded module [ml-package-loader]
[2023-12-18T14:31:17,175][INFO ][o.e.p.PluginsService     ] [runTask-0] loaded module [test-delayed-aggs]
[2023-12-18T14:31:17,175][INFO ][o.e.p.PluginsService     ] [runTask-0] loaded module [kibana]
[2023-12-18T14:31:17,175][INFO ][o.e.p.PluginsService     ] [runTask-0] loaded module [constant-keyword]
[2023-12-18T14:31:17,175][INFO ][o.e.p.PluginsService     ] [runTask-0] loaded module [x-pack-logstash]
[2023-12-18T14:31:17,175][INFO ][o.e.p.PluginsService     ] [runTask-0] loaded module [x-pack-graph]
[2023-12-18T14:31:17,175][INFO ][o.e.p.PluginsService     ] [runTask-0] loaded module [x-pack-ccr]
[2023-12-18T14:31:17,175][INFO ][o.e.p.PluginsService     ] [runTask-0] loaded module [x-pack-esql]
[2023-12-18T14:31:17,176][INFO ][o.e.p.PluginsService     ] [runTask-0] loaded module [parent-join]
[2023-12-18T14:31:17,176][INFO ][o.e.p.PluginsService     ] [runTask-0] loaded module [counted-keyword]
[2023-12-18T14:31:17,176][INFO ][o.e.p.PluginsService     ] [runTask-0] loaded module [x-pack-enrich]
[2023-12-18T14:31:17,176][INFO ][o.e.p.PluginsService     ] [runTask-0] loaded module [repositories-metering-api]
[2023-12-18T14:31:17,176][INFO ][o.e.p.PluginsService     ] [runTask-0] loaded module [transform]
[2023-12-18T14:31:17,176][INFO ][o.e.p.PluginsService     ] [runTask-0] loaded module [repository-azure]
[2023-12-18T14:31:17,176][INFO ][o.e.p.PluginsService     ] [runTask-0] loaded module [repository-gcs]
[2023-12-18T14:31:17,176][INFO ][o.e.p.PluginsService     ] [runTask-0] loaded module [spatial]
[2023-12-18T14:31:17,176][INFO ][o.e.p.PluginsService     ] [runTask-0] loaded module [apm]
[2023-12-18T14:31:17,176][INFO ][o.e.p.PluginsService     ] [runTask-0] loaded module [mapper-extras]
[2023-12-18T14:31:17,176][INFO ][o.e.p.PluginsService     ] [runTask-0] loaded module [mapper-version]
[2023-12-18T14:31:17,176][INFO ][o.e.p.PluginsService     ] [runTask-0] loaded module [x-pack-rollup]
[2023-12-18T14:31:17,176][INFO ][o.e.p.PluginsService     ] [runTask-0] loaded module [percolator]
[2023-12-18T14:31:17,176][INFO ][o.e.p.PluginsService     ] [runTask-0] loaded module [data-streams]
[2023-12-18T14:31:17,176][INFO ][o.e.p.PluginsService     ] [runTask-0] loaded module [x-pack-stack]
[2023-12-18T14:31:17,176][INFO ][o.e.p.PluginsService     ] [runTask-0] loaded module [rank-eval]
[2023-12-18T14:31:17,177][INFO ][o.e.p.PluginsService     ] [runTask-0] loaded module [reindex]
[2023-12-18T14:31:17,177][INFO ][o.e.p.PluginsService     ] [runTask-0] loaded module [test-apm-integration]
[2023-12-18T14:31:17,177][INFO ][o.e.p.PluginsService     ] [runTask-0] loaded module [x-pack-security]
[2023-12-18T14:31:17,177][INFO ][o.e.p.PluginsService     ] [runTask-0] loaded module [blob-cache]
[2023-12-18T14:31:17,177][INFO ][o.e.p.PluginsService     ] [runTask-0] loaded module [searchable-snapshots]
[2023-12-18T14:31:17,177][INFO ][o.e.p.PluginsService     ] [runTask-0] loaded module [x-pack-slm]
[2023-12-18T14:31:17,177][INFO ][o.e.p.PluginsService     ] [runTask-0] loaded module [test-latency-simulating-directory]
[2023-12-18T14:31:17,177][INFO ][o.e.p.PluginsService     ] [runTask-0] loaded module [snapshot-based-recoveries]
[2023-12-18T14:31:17,177][INFO ][o.e.p.PluginsService     ] [runTask-0] loaded module [x-pack-watcher]
[2023-12-18T14:31:17,177][INFO ][o.e.p.PluginsService     ] [runTask-0] loaded module [old-lucene-versions]
[2023-12-18T14:31:17,177][INFO ][o.e.p.PluginsService     ] [runTask-0] loaded module [x-pack-ilm]
[2023-12-18T14:31:17,177][INFO ][o.e.p.PluginsService     ] [runTask-0] loaded module [x-pack-inference]
[2023-12-18T14:31:17,177][INFO ][o.e.p.PluginsService     ] [runTask-0] loaded module [x-pack-voting-only-node]
[2023-12-18T14:31:17,177][INFO ][o.e.p.PluginsService     ] [runTask-0] loaded module [x-pack-deprecation]
[2023-12-18T14:31:17,177][INFO ][o.e.p.PluginsService     ] [runTask-0] loaded module [x-pack-fleet]
[2023-12-18T14:31:17,178][INFO ][o.e.p.PluginsService     ] [runTask-0] loaded module [x-pack-profiling]
[2023-12-18T14:31:17,178][INFO ][o.e.p.PluginsService     ] [runTask-0] loaded module [x-pack-aggregate-metric]
[2023-12-18T14:31:17,178][INFO ][o.e.p.PluginsService     ] [runTask-0] loaded module [x-pack-downsample]
[2023-12-18T14:31:17,178][INFO ][o.e.p.PluginsService     ] [runTask-0] loaded module [ingest-geoip]
[2023-12-18T14:31:17,178][INFO ][o.e.p.PluginsService     ] [runTask-0] loaded module [x-pack-write-load-forecaster]
[2023-12-18T14:31:17,178][INFO ][o.e.p.PluginsService     ] [runTask-0] loaded module [search-business-rules]
[2023-12-18T14:31:17,178][INFO ][o.e.p.PluginsService     ] [runTask-0] loaded module [wildcard]
[2023-12-18T14:31:17,178][INFO ][o.e.p.PluginsService     ] [runTask-0] loaded module [ingest-attachment]
[2023-12-18T14:31:17,178][INFO ][o.e.p.PluginsService     ] [runTask-0] loaded module [x-pack-apm-data]
[2023-12-18T14:31:17,178][INFO ][o.e.p.PluginsService     ] [runTask-0] loaded module [unsigned-long]
[2023-12-18T14:31:17,178][INFO ][o.e.p.PluginsService     ] [runTask-0] loaded module [x-pack-sql]
[2023-12-18T14:31:17,179][INFO ][o.e.p.PluginsService     ] [runTask-0] loaded module [runtime-fields-common]
[2023-12-18T14:31:17,179][INFO ][o.e.p.PluginsService     ] [runTask-0] loaded module [x-pack-async]
[2023-12-18T14:31:17,179][INFO ][o.e.p.PluginsService     ] [runTask-0] loaded module [vector-tile]
[2023-12-18T14:31:17,179][INFO ][o.e.p.PluginsService     ] [runTask-0] loaded module [lang-expression]
[2023-12-18T14:31:17,179][INFO ][o.e.p.PluginsService     ] [runTask-0] loaded module [x-pack-eql]
[2023-12-18T14:31:17,224][INFO ][o.e.c.u.FeatureFlag      ] [runTask-0] The current build is a snapshot, feature flag [failure_store] is enabled
[2023-12-18T14:31:17,520][INFO ][o.e.e.NodeEnvironment    ] [runTask-0] using [1] data paths, mounts [[/System/Volumes/Data (/dev/disk3s5)]], net usable_space [636.9gb], net total_space [926.3gb], types [apfs]
[2023-12-18T14:31:17,520][INFO ][o.e.e.NodeEnvironment    ] [runTask-0] heap size [512mb], compressed ordinary object pointers [true]
[2023-12-18T14:31:17,543][INFO ][o.e.n.Node               ] [runTask-0] node name [runTask-0], node ID [RpeX_621SdCZSBh8-RSQvg], cluster name [runTask], roles [ingest, data_frozen, ml, data_hot, transform, data_content, data_warm, master, remote_cluster_client, data, data_cold]
[2023-12-18T14:31:19,294][DEBUG][o.e.t.a.APMMeterRegistry ] [runTask-0] Registering an instrument with name: es.allocator.desired_balance.shards.unassigned.count
[2023-12-18T14:31:19,294][DEBUG][o.e.t.a.APMMeterRegistry ] [runTask-0] Registering an instrument with name: es.allocator.desired_balance.shards.count
[2023-12-18T14:31:19,295][DEBUG][o.e.t.a.APMMeterRegistry ] [runTask-0] Registering an instrument with name: es.allocator.desired_balance.allocations.undesired.count
[2023-12-18T14:31:19,295][DEBUG][o.e.t.a.APMMeterRegistry ] [runTask-0] Registering an instrument with name: es.allocator.desired_balance.allocations.undesired.ratio
[2023-12-18T14:31:19,309][INFO ][o.e.c.u.FeatureFlag      ] [runTask-0] The current build is a snapshot, feature flag [semantic_text] is enabled
[2023-12-18T14:31:19,326][DEBUG][o.e.t.a.APMMeterRegistry ] [runTask-0] Registering an instrument with name: es.breaker.parent.trip.total
[2023-12-18T14:31:19,326][DEBUG][o.e.t.a.APMMeterRegistry ] [runTask-0] Registering an instrument with name: es.breaker.field_data.trip.total
[2023-12-18T14:31:19,326][DEBUG][o.e.t.a.APMMeterRegistry ] [runTask-0] Registering an instrument with name: es.breaker.request.trip.total
[2023-12-18T14:31:19,326][DEBUG][o.e.t.a.APMMeterRegistry ] [runTask-0] Registering an instrument with name: es.breaker.in_flight_requests.trip.total
[2023-12-18T14:31:19,329][DEBUG][o.e.t.a.APMMeterRegistry ] [runTask-0] Registering an instrument with name: es.breaker.model_inference.trip.total
[2023-12-18T14:31:19,329][DEBUG][o.e.t.a.APMMeterRegistry ] [runTask-0] Registering an instrument with name: es.breaker.eql_sequence.trip.total
[2023-12-18T14:31:19,350][INFO ][o.e.f.FeatureService     ] [runTask-0] Registered local node features [features_supported, health.dsl.info, usage.data_tiers.precalculate_stats]
[2023-12-18T14:31:19,497][INFO ][o.e.x.m.p.l.CppLogMessageHandler] [runTask-0] [controller/43802] [Main.cc@123] controller (64 bit): Version 8.13.0-SNAPSHOT (Build c9c232240dd04f) Copyright (c) 2023 Elasticsearch BV
[2023-12-18T14:31:19,659][INFO ][o.e.t.a.APM              ] [runTask-0] Sending apm metrics is disabled
[2023-12-18T14:31:19,660][INFO ][o.e.t.a.APM              ] [runTask-0] Sending apm tracing is disabled
[2023-12-18T14:31:19,670][DEBUG][o.e.t.a.APMMeterRegistry ] [runTask-0] Registering an instrument with name: es.test.long_counter.count
[2023-12-18T14:31:19,671][DEBUG][o.e.t.a.APMMeterRegistry ] [runTask-0] Registering an instrument with name: es.test.double_counter.count
[2023-12-18T14:31:19,671][DEBUG][o.e.t.a.APMMeterRegistry ] [runTask-0] Registering an instrument with name: es.test.double_histogram.histogram
[2023-12-18T14:31:19,671][DEBUG][o.e.t.a.APMMeterRegistry ] [runTask-0] Registering an instrument with name: es.test.long_histogram.histogram
[2023-12-18T14:31:19,672][DEBUG][o.e.t.a.APMMeterRegistry ] [runTask-0] Registering an instrument with name: es.test.double_gauge.total
[2023-12-18T14:31:19,672][DEBUG][o.e.t.a.APMMeterRegistry ] [runTask-0] Registering an instrument with name: es.test.long_gauge.total
[2023-12-18T14:31:19,672][DEBUG][o.e.t.a.APMMeterRegistry ] [runTask-0] Registering an instrument with name: es.test.async_long_counter.count
[2023-12-18T14:31:19,672][DEBUG][o.e.t.a.APMMeterRegistry ] [runTask-0] Registering an instrument with name: es.test.async_double_counter.count
[2023-12-18T14:31:19,672][INFO ][o.e.x.s.Security         ] [runTask-0] Security is enabled
[2023-12-18T14:31:19,850][INFO ][o.e.x.s.a.s.FileRolesStore] [runTask-0] parsed [1] roles from file [/Users/przemyslawgomulka/workspace/pgomulka/elasticsearch/build/testclusters/runTask-0/config/roles.yml]
[2023-12-18T14:31:20,137][DEBUG][o.e.t.a.APMMeterRegistry ] [runTask-0] Registering an instrument with name: es.blob_cache.miss_that_triggered_read.count
[2023-12-18T14:31:20,138][DEBUG][o.e.t.a.APMMeterRegistry ] [runTask-0] Registering an instrument with name: es.blob_cache.evicted_used_regions.count
[2023-12-18T14:31:20,138][DEBUG][o.e.t.a.APMMeterRegistry ] [runTask-0] Registering an instrument with name: es.blob_cache.cache_miss_load.time
[2023-12-18T14:31:20,231][INFO ][o.e.x.w.Watcher          ] [runTask-0] Watcher initialized components at 2023-12-18T13:31:20.231Z
[2023-12-18T14:31:20,256][INFO ][o.e.x.p.ProfilingPlugin  ] [runTask-0] Profiling is enabled
[2023-12-18T14:31:20,267][INFO ][o.e.x.p.ProfilingPlugin  ] [runTask-0] profiling index templates will not be installed or reinstalled
[2023-12-18T14:31:20,287][INFO ][o.e.x.a.APMPlugin        ] [runTask-0] APM ingest plugin is disabled
[2023-12-18T14:31:20,408][INFO ][o.e.c.u.FeatureFlag      ] [runTask-0] The current build is a snapshot, feature flag [connector_api] is enabled
[2023-12-18T14:31:20,553][INFO ][o.e.t.n.NettyAllocator   ] [runTask-0] creating NettyAllocator with the following configs: [name=unpooled, suggested_max_allocation_size=1mb, factors={es.unsafe.use_unpooled_allocator=null, g1gc_enabled=true, g1gc_region_size=4mb, heap_size=512mb}]
[2023-12-18T14:31:20,566][INFO ][o.e.i.r.RecoverySettings ] [runTask-0] using rate limit [40mb] with [default=40mb, read=0b, write=0b, max=0b]
[2023-12-18T14:31:20,567][DEBUG][o.e.t.a.APMMeterRegistry ] [runTask-0] Registering an instrument with name: es.repositories.requests.count
[2023-12-18T14:31:20,567][DEBUG][o.e.t.a.APMMeterRegistry ] [runTask-0] Registering an instrument with name: es.repositories.exceptions.count
[2023-12-18T14:31:20,567][DEBUG][o.e.t.a.APMMeterRegistry ] [runTask-0] Registering an instrument with name: es.repositories.throttles.count
[2023-12-18T14:31:20,567][DEBUG][o.e.t.a.APMMeterRegistry ] [runTask-0] Registering an instrument with name: es.repositories.operations.count
[2023-12-18T14:31:20,568][DEBUG][o.e.t.a.APMMeterRegistry ] [runTask-0] Registering an instrument with name: es.repositories.operations.unsuccessful.count
[2023-12-18T14:31:20,568][DEBUG][o.e.t.a.APMMeterRegistry ] [runTask-0] Registering an instrument with name: es.repositories.exceptions.histogram
[2023-12-18T14:31:20,568][DEBUG][o.e.t.a.APMMeterRegistry ] [runTask-0] Registering an instrument with name: es.repositories.throttles.histogram
[2023-12-18T14:31:20,568][DEBUG][o.e.t.a.APMMeterRegistry ] [runTask-0] Registering an instrument with name: es.repositories.requests.http_request_time.histogram
[2023-12-18T14:31:20,591][INFO ][o.e.d.DiscoveryModule    ] [runTask-0] using discovery type [multi-node] and seed hosts providers [settings, file]
[2023-12-18T14:31:20,619][DEBUG][o.e.t.a.APMMeterRegistry ] [runTask-0] Registering an instrument with name: es.health.overall.red.status
[2023-12-18T14:31:21,154][INFO ][o.e.n.Node               ] [runTask-0] initialized
[2023-12-18T14:31:21,155][INFO ][o.e.n.Node               ] [runTask-0] starting ...
[2023-12-18T14:31:21,182][INFO ][o.e.x.s.c.f.PersistentCache] [runTask-0] persistent cache index loaded
[2023-12-18T14:31:21,183][INFO ][o.e.x.d.l.DeprecationIndexingComponent] [runTask-0] deprecation component started
[2023-12-18T14:31:21,232][INFO ][o.e.t.TransportService   ] [runTask-0] publish_address {127.0.0.1:9300}, bound_addresses {[::1]:9300}, {127.0.0.1:9300}
[2023-12-18T14:31:21,322][WARN ][o.e.b.BootstrapChecks    ] [runTask-0] Transport SSL must be enabled if security is enabled. Please set [xpack.security.transport.ssl.enabled] to [true] or disable security by setting [xpack.security.enabled] to [false]; for more information see [https://www.elastic.co/guide/en/elasticsearch/reference/master/bootstrap-checks-xpack.html#bootstrap-checks-tls]
[2023-12-18T14:31:21,323][INFO ][o.e.c.c.ClusterBootstrapService] [runTask-0] this node has not joined a bootstrapped cluster yet; [cluster.initial_master_nodes] is set to [runTask-0]
[2023-12-18T14:31:21,325][WARN ][o.e.d.FileBasedSeedHostsProvider] [runTask-0] expected, but did not find, a dynamic hosts list at [/Users/przemyslawgomulka/workspace/pgomulka/elasticsearch/build/testclusters/runTask-0/config/unicast_hosts.txt]
[2023-12-18T14:31:21,325][INFO ][o.e.c.c.Coordinator      ] [runTask-0] setting initial configuration to VotingConfiguration{RpeX_621SdCZSBh8-RSQvg}
[2023-12-18T14:31:21,367][INFO ][o.e.h.AbstractHttpServerTransport] [runTask-0] publish_address {127.0.0.1:9200}, bound_addresses {[::1]:9200}, {127.0.0.1:9200}
[2023-12-18T14:31:21,372][DEBUG][o.e.t.a.APMMeterRegistry ] [runTask-0] Registering an instrument with name: es.node.stats.indices.get.total
[2023-12-18T14:31:21,372][DEBUG][o.e.t.a.APMMeterRegistry ] [runTask-0] Registering an instrument with name: es.node.stats.indices.get.time
[2023-12-18T14:31:21,372][DEBUG][o.e.t.a.APMMeterRegistry ] [runTask-0] Registering an instrument with name: es.node.stats.indices.search.fetch.total
[2023-12-18T14:31:21,373][DEBUG][o.e.t.a.APMMeterRegistry ] [runTask-0] Registering an instrument with name: es.node.stats.indices.search.fetch.time
[2023-12-18T14:31:21,373][DEBUG][o.e.t.a.APMMeterRegistry ] [runTask-0] Registering an instrument with name: es.node.stats.indices.merge.total
[2023-12-18T14:31:21,373][DEBUG][o.e.t.a.APMMeterRegistry ] [runTask-0] Registering an instrument with name: es.node.stats.indices.merge.time
[2023-12-18T14:31:21,373][DEBUG][o.e.t.a.APMMeterRegistry ] [runTask-0] Registering an instrument with name: es.node.stats.indices.translog.operations.count
[2023-12-18T14:31:21,373][DEBUG][o.e.t.a.APMMeterRegistry ] [runTask-0] Registering an instrument with name: es.node.stats.indices.translog.size
[2023-12-18T14:31:21,373][DEBUG][o.e.t.a.APMMeterRegistry ] [runTask-0] Registering an instrument with name: es.node.stats.indices.translog.uncommitted_operations.count
[2023-12-18T14:31:21,373][DEBUG][o.e.t.a.APMMeterRegistry ] [runTask-0] Registering an instrument with name: es.node.stats.indices.translog.uncommitted.size
[2023-12-18T14:31:21,373][DEBUG][o.e.t.a.APMMeterRegistry ] [runTask-0] Registering an instrument with name: es.node.stats.indices.translog.earliest_last_modified_age.time
[2023-12-18T14:31:21,373][DEBUG][o.e.t.a.APMMeterRegistry ] [runTask-0] Registering an instrument with name: es.node.stats.transport.rx.size
[2023-12-18T14:31:21,373][DEBUG][o.e.t.a.APMMeterRegistry ] [runTask-0] Registering an instrument with name: es.node.stats.transport.tx.size
[2023-12-18T14:31:21,374][DEBUG][o.e.t.a.APMMeterRegistry ] [runTask-0] Registering an instrument with name: es.node.stats.jvm.mem.pools.young.size
[2023-12-18T14:31:21,374][DEBUG][o.e.t.a.APMMeterRegistry ] [runTask-0] Registering an instrument with name: es.node.stats.jvm.mem.pools.survivor.size
[2023-12-18T14:31:21,374][DEBUG][o.e.t.a.APMMeterRegistry ] [runTask-0] Registering an instrument with name: es.node.stats.jvm.mem.pools.old.size
[2023-12-18T14:31:21,374][DEBUG][o.e.t.a.APMMeterRegistry ] [runTask-0] Registering an instrument with name: es.node.stats.fs.io_stats.io_time.total
[2023-12-18T14:31:21,374][DEBUG][o.e.t.a.APMMeterRegistry ] [runTask-0] Registering an instrument with name: es.node.stats.indices.indexing.docs.total
[2023-12-18T14:31:21,374][DEBUG][o.e.t.a.APMMeterRegistry ] [runTask-0] Registering an instrument with name: es.node.stats.indices.indexing.docs.total
[2023-12-18T14:31:21,374][DEBUG][o.e.t.a.APMMeterRegistry ] [runTask-0] Registering an instrument with name: es.node.stats.indices.indexing.failed.total
[2023-12-18T14:31:21,374][DEBUG][o.e.t.a.APMMeterRegistry ] [runTask-0] Registering an instrument with name: es.node.stats.indices.deletion.docs.total
[2023-12-18T14:31:21,374][DEBUG][o.e.t.a.APMMeterRegistry ] [runTask-0] Registering an instrument with name: es.node.stats.indices.deletion.docs.total
[2023-12-18T14:31:21,375][DEBUG][o.e.t.a.APMMeterRegistry ] [runTask-0] Registering an instrument with name: es.node.stats.indices.indexing.time
[2023-12-18T14:31:21,375][DEBUG][o.e.t.a.APMMeterRegistry ] [runTask-0] Registering an instrument with name: es.node.stats.indices.deletion.time
[2023-12-18T14:31:21,375][DEBUG][o.e.t.a.APMMeterRegistry ] [runTask-0] Registering an instrument with name: es.node.stats.indices.throttle.time
[2023-12-18T14:31:21,375][DEBUG][o.e.t.a.APMMeterRegistry ] [runTask-0] Registering an instrument with name: es.node.stats.indices.noop.total
[2023-12-18T14:31:21,375][DEBUG][o.e.t.a.APMMeterRegistry ] [runTask-0] Registering an instrument with name: es.node.stats.indices.indexing.coordinating_operations.memory.size.total
[2023-12-18T14:31:21,375][DEBUG][o.e.t.a.APMMeterRegistry ] [runTask-0] Registering an instrument with name: es.node.stats.indices.indexing.coordinating_operations.count.total
[2023-12-18T14:31:21,375][DEBUG][o.e.t.a.APMMeterRegistry ] [runTask-0] Registering an instrument with name: es.node.stats.indices.indexing.coordinating_operations.memory.size.total
[2023-12-18T14:31:21,375][DEBUG][o.e.t.a.APMMeterRegistry ] [runTask-0] Registering an instrument with name: es.node.stats.indices.indexing.coordinating_operations.count
[2023-12-18T14:31:21,376][DEBUG][o.e.t.a.APMMeterRegistry ] [runTask-0] Registering an instrument with name: es.node.stats.indices.indexing.coordinating_operations.rejections.total
[2023-12-18T14:31:21,376][DEBUG][o.e.t.a.APMMeterRegistry ] [runTask-0] Registering an instrument with name: es.node.stats.indices.indexing.primary_operations.memory.size.total
[2023-12-18T14:31:21,376][DEBUG][o.e.t.a.APMMeterRegistry ] [runTask-0] Registering an instrument with name: es.node.stats.indices.indexing.primary_operations.count.total
[2023-12-18T14:31:21,376][DEBUG][o.e.t.a.APMMeterRegistry ] [runTask-0] Registering an instrument with name: es.node.stats.indices.indexing.primary_operations.memory.size.total
[2023-12-18T14:31:21,376][DEBUG][o.e.t.a.APMMeterRegistry ] [runTask-0] Registering an instrument with name: es.node.stats.indices.indexing.primary_operations.count.total
[2023-12-18T14:31:21,376][DEBUG][o.e.t.a.APMMeterRegistry ] [runTask-0] Registering an instrument with name: es.node.stats.indices.indexing.primary_operations.rejections.total
[2023-12-18T14:31:21,376][DEBUG][o.e.t.a.APMMeterRegistry ] [runTask-0] Registering an instrument with name: es.node.stats.indices.indexing.memory.limit.total
[2023-12-18T14:31:21,376][INFO ][o.e.n.Node               ] [runTask-0] started {runTask-0}{RpeX_621SdCZSBh8-RSQvg}{2w_LQi0HTTetBZfSdpLG6Q}{runTask-0}{127.0.0.1}{127.0.0.1:9300}{cdfhilmrstw}{8.13.0}{7000099-8500007}{ml.machine_memory=68719476736, ml.config_version=12.0.0, ml.max_jvm_size=536870912, ml.allocated_processors=10, xpack.installed=true, transform.config_version=10.0.0, testattr=test, ml.allocated_processors_double=10.0}
[2023-12-18T14:31:21,421][INFO ][o.e.c.s.MasterService    ] [runTask-0] elected-as-master ([1] nodes joined in term 1)[_FINISH_ELECTION_, {runTask-0}{RpeX_621SdCZSBh8-RSQvg}{2w_LQi0HTTetBZfSdpLG6Q}{runTask-0}{127.0.0.1}{127.0.0.1:9300}{cdfhilmrstw}{8.13.0}{7000099-8500007} completing election], term: 1, version: 1, delta: master node changed {previous [], current [{runTask-0}{RpeX_621SdCZSBh8-RSQvg}{2w_LQi0HTTetBZfSdpLG6Q}{runTask-0}{127.0.0.1}{127.0.0.1:9300}{cdfhilmrstw}{8.13.0}{7000099-8500007}]}
[2023-12-18T14:31:21,459][INFO ][o.e.c.c.CoordinationState] [runTask-0] cluster UUID set to [8TjOrE8yTw-_eqrhD95JPw]
[2023-12-18T14:31:21,484][INFO ][o.e.c.s.ClusterApplierService] [runTask-0] master node changed {previous [], current [{runTask-0}{RpeX_621SdCZSBh8-RSQvg}{2w_LQi0HTTetBZfSdpLG6Q}{runTask-0}{127.0.0.1}{127.0.0.1:9300}{cdfhilmrstw}{8.13.0}{7000099-8500007}]}, term: 1, version: 1, reason: Publication{term=1, version=1}
[2023-12-18T14:31:21,498][INFO ][o.e.c.f.AbstractFileWatchingService] [runTask-0] starting file watcher ...
[2023-12-18T14:31:21,500][INFO ][o.e.c.f.AbstractFileWatchingService] [runTask-0] file settings service up and running [tid=60]
[2023-12-18T14:31:21,503][INFO ][o.e.c.c.NodeJoinExecutor ] [runTask-0] node-join: [{runTask-0}{RpeX_621SdCZSBh8-RSQvg}{2w_LQi0HTTetBZfSdpLG6Q}{runTask-0}{127.0.0.1}{127.0.0.1:9300}{cdfhilmrstw}{8.13.0}{7000099-8500007}] with reason [completing election]
[2023-12-18T14:31:21,560][INFO ][o.e.g.GatewayService     ] [runTask-0] recovered [0] indices into cluster_state
[2023-12-18T14:31:21,638][INFO ][o.e.c.m.MetadataIndexTemplateService] [runTask-0] adding component template [behavioral_analytics-events-mappings]
[2023-12-18T14:31:21,650][INFO ][o.e.c.m.MetadataIndexTemplateService] [runTask-0] adding component template [elastic-connectors-sync-jobs-mappings]
[2023-12-18T14:31:21,652][INFO ][o.e.c.m.MetadataIndexTemplateService] [runTask-0] adding component template [elastic-connectors-settings]
[2023-12-18T14:31:21,654][INFO ][o.e.c.m.MetadataIndexTemplateService] [runTask-0] adding component template [elastic-connectors-sync-jobs-settings]
[2023-12-18T14:31:21,674][INFO ][o.e.c.m.MetadataIndexTemplateService] [runTask-0] adding index template [.monitoring-ent-search-mb] for index patterns [.monitoring-ent-search-8-*]
[2023-12-18T14:31:21,681][INFO ][o.e.c.m.MetadataIndexTemplateService] [runTask-0] adding component template [elastic-connectors-mappings]
[2023-12-18T14:31:21,694][INFO ][o.e.c.m.MetadataIndexTemplateService] [runTask-0] adding index template [.monitoring-logstash-mb] for index patterns [.monitoring-logstash-8-*]
[2023-12-18T14:31:21,697][INFO ][o.e.c.m.MetadataIndexTemplateService] [runTask-0] adding index template [.ml-state] for index patterns [.ml-state*]
[2023-12-18T14:31:21,702][INFO ][o.e.c.m.MetadataIndexTemplateService] [runTask-0] adding index template [.ml-notifications-000002] for index patterns [.ml-notifications-000002]
[2023-12-18T14:31:21,705][INFO ][o.e.c.m.MetadataIndexTemplateService] [runTask-0] adding index template [search-acl-filter] for index patterns [.search-acl-filter-*]
[2023-12-18T14:31:21,723][INFO ][o.e.c.m.MetadataIndexTemplateService] [runTask-0] adding index template [.monitoring-kibana-mb] for index patterns [.monitoring-kibana-8-*]
[2023-12-18T14:31:21,728][INFO ][o.e.c.m.MetadataIndexTemplateService] [runTask-0] adding template [.monitoring-kibana] for index patterns [.monitoring-kibana-7-*]
[2023-12-18T14:31:21,734][INFO ][o.e.c.m.MetadataIndexTemplateService] [runTask-0] adding template [.monitoring-logstash] for index patterns [.monitoring-logstash-7-*]
[2023-12-18T14:31:21,756][INFO ][o.e.c.m.MetadataIndexTemplateService] [runTask-0] adding template [.monitoring-es] for index patterns [.monitoring-es-7-*]
[2023-12-18T14:31:21,763][INFO ][o.e.c.m.MetadataIndexTemplateService] [runTask-0] adding template [.monitoring-beats] for index patterns [.monitoring-beats-7-*]
[2023-12-18T14:31:21,766][INFO ][o.e.c.m.MetadataIndexTemplateService] [runTask-0] adding template [.monitoring-alerts-7] for index patterns [.monitoring-alerts-7]
[2023-12-18T14:31:21,775][INFO ][o.e.c.m.MetadataIndexTemplateService] [runTask-0] adding index template [.ml-anomalies-] for index patterns [.ml-anomalies-*]
[2023-12-18T14:31:21,786][INFO ][o.e.c.m.MetadataIndexTemplateService] [runTask-0] adding index template [.monitoring-beats-mb] for index patterns [.monitoring-beats-8-*]
[2023-12-18T14:31:21,802][INFO ][o.e.c.m.MetadataIndexTemplateService] [runTask-0] adding index template [.monitoring-es-mb] for index patterns [.monitoring-es-8-*]
[2023-12-18T14:31:21,806][INFO ][o.e.c.m.MetadataIndexTemplateService] [runTask-0] adding index template [.ml-stats] for index patterns [.ml-stats-*]
[2023-12-18T14:31:21,810][INFO ][o.e.c.m.MetadataIndexTemplateService] [runTask-0] adding component template [synthetics-mappings]
[2023-12-18T14:31:21,811][INFO ][o.e.c.m.MetadataIndexTemplateService] [runTask-0] adding component template [metrics-tsdb-settings]
[2023-12-18T14:31:21,813][INFO ][o.e.c.m.MetadataIndexTemplateService] [runTask-0] adding component template [logs-mappings]
[2023-12-18T14:31:21,816][INFO ][o.e.c.m.MetadataIndexTemplateService] [runTask-0] adding component template [metrics-mappings]
[2023-12-18T14:31:21,820][INFO ][o.e.c.m.MetadataIndexTemplateService] [runTask-0] adding component template [data-streams-mappings]
[2023-12-18T14:31:21,824][INFO ][o.e.c.m.MetadataIndexTemplateService] [runTask-0] adding component template [ecs@dynamic_templates]
[2023-12-18T14:31:21,825][INFO ][o.e.c.m.MetadataIndexTemplateService] [runTask-0] adding component template [synthetics-settings]
[2023-12-18T14:31:21,826][INFO ][o.e.c.m.MetadataIndexTemplateService] [runTask-0] adding component template [metrics-settings]
[2023-12-18T14:31:21,828][INFO ][o.e.c.m.MetadataIndexTemplateService] [runTask-0] adding component template [logs@mappings]
[2023-12-18T14:31:21,829][INFO ][o.e.c.m.MetadataIndexTemplateService] [runTask-0] adding component template [synthetics@mappings]
[2023-12-18T14:31:21,831][INFO ][o.e.c.m.MetadataIndexTemplateService] [runTask-0] adding component template [metrics@settings]
[2023-12-18T14:31:21,832][INFO ][o.e.c.m.MetadataIndexTemplateService] [runTask-0] adding component template [synthetics@settings]
[2023-12-18T14:31:21,834][INFO ][o.e.c.m.MetadataIndexTemplateService] [runTask-0] adding component template [metrics@mappings]
[2023-12-18T14:31:21,836][INFO ][o.e.c.m.MetadataIndexTemplateService] [runTask-0] adding component template [data-streams@mappings]
[2023-12-18T14:31:21,839][INFO ][o.e.c.m.MetadataIndexTemplateService] [runTask-0] adding component template [ecs@mappings]
[2023-12-18T14:31:21,842][INFO ][o.e.c.m.MetadataIndexTemplateService] [runTask-0] adding index template [.kibana-reporting] for index patterns [.kibana-reporting*]
[2023-12-18T14:31:21,843][INFO ][o.e.c.m.MetadataIndexTemplateService] [runTask-0] adding component template [metrics@tsdb-settings]
[2023-12-18T14:31:21,846][INFO ][o.e.c.m.MetadataIndexTemplateService] [runTask-0] adding index template [.slm-history] for index patterns [.slm-history-6*]
[2023-12-18T14:31:21,849][INFO ][o.e.c.m.MetadataIndexTemplateService] [runTask-0] adding index template [ilm-history] for index patterns [ilm-history-6*]
[2023-12-18T14:31:21,851][INFO ][o.e.c.m.MetadataIndexTemplateService] [runTask-0] adding component template [.deprecation-indexing-mappings]
[2023-12-18T14:31:21,852][INFO ][o.e.c.m.MetadataIndexTemplateService] [runTask-0] adding component template [.deprecation-indexing-settings]
[2023-12-18T14:31:21,856][INFO ][o.e.c.m.MetadataIndexTemplateService] [runTask-0] adding index template [.fleet-fileds-fromhost-data] for index patterns [.fleet-fileds-fromhost-data-*]
[2023-12-18T14:31:21,859][INFO ][o.e.c.m.MetadataIndexTemplateService] [runTask-0] adding index template [.fleet-fileds-tohost-data] for index patterns [.fleet-fileds-tohost-data-*]
[2023-12-18T14:31:21,862][INFO ][o.e.c.m.MetadataIndexTemplateService] [runTask-0] adding index template [.fleet-fileds-tohost-meta] for index patterns [.fleet-fileds-tohost-meta-*]
[2023-12-18T14:31:21,867][INFO ][o.e.c.m.MetadataIndexTemplateService] [runTask-0] adding index template [.fleet-fileds-fromhost-meta] for index patterns [.fleet-fileds-fromhost-meta-*]
[2023-12-18T14:31:21,872][INFO ][o.e.c.m.MetadataIndexTemplateService] [runTask-0] adding index template [.watch-history-16] for index patterns [.watcher-history-16*]
[2023-12-18T14:31:21,943][INFO ][o.e.c.m.MetadataIndexTemplateService] [runTask-0] adding index template [elastic-connectors-sync-jobs] for index patterns [.elastic-connectors-sync-jobs-v1]
[2023-12-18T14:31:21,946][INFO ][o.e.c.m.MetadataIndexTemplateService] [runTask-0] adding index template [synthetics] for index patterns [synthetics-*-*]
[2023-12-18T14:31:21,949][INFO ][o.e.c.m.MetadataIndexTemplateService] [runTask-0] adding index template [elastic-connectors] for index patterns [.elastic-connectors-v1]
[2023-12-18T14:31:21,952][INFO ][o.e.c.m.MetadataIndexTemplateService] [runTask-0] adding index template [metrics] for index patterns [metrics-*-*]
[2023-12-18T14:31:21,955][INFO ][o.e.c.m.MetadataIndexTemplateService] [runTask-0] adding index template [.deprecation-indexing-template] for index patterns [.logs-deprecation.*]
[2023-12-18T14:31:21,989][INFO ][o.e.x.i.a.TransportPutLifecycleAction] [runTask-0] adding index lifecycle policy [.monitoring-8-ilm-policy]
[2023-12-18T14:31:22,028][INFO ][o.e.x.i.a.TransportPutLifecycleAction] [runTask-0] adding index lifecycle policy [ml-size-based-ilm-policy]
[2023-12-18T14:31:22,063][INFO ][o.e.x.i.a.TransportPutLifecycleAction] [runTask-0] adding index lifecycle policy [metrics]
[2023-12-18T14:31:22,091][INFO ][o.e.x.i.a.TransportPutLifecycleAction] [runTask-0] adding index lifecycle policy [synthetics]
[2023-12-18T14:31:22,119][INFO ][o.e.x.i.a.TransportPutLifecycleAction] [runTask-0] adding index lifecycle policy [30-days-default]
[2023-12-18T14:31:22,207][INFO ][o.e.x.c.t.IndexTemplateRegistry] [runTask-0] adding ingest pipeline behavioral_analytics-events-final_pipeline
[2023-12-18T14:31:22,208][INFO ][o.e.x.c.t.IndexTemplateRegistry] [runTask-0] adding ingest pipeline logs-default-pipeline
[2023-12-18T14:31:22,208][INFO ][o.e.x.c.t.IndexTemplateRegistry] [runTask-0] adding ingest pipeline logs@default-pipeline
[2023-12-18T14:31:22,208][INFO ][o.e.x.c.t.IndexTemplateRegistry] [runTask-0] adding ingest pipeline ent-search-generic-ingestion
[2023-12-18T14:31:22,208][INFO ][o.e.x.c.t.IndexTemplateRegistry] [runTask-0] adding ingest pipeline logs@json-pipeline
[2023-12-18T14:31:22,208][INFO ][o.e.x.c.t.IndexTemplateRegistry] [runTask-0] adding ingest pipeline logs@json-message
[2023-12-18T14:31:22,210][INFO ][o.e.c.m.MetadataIndexTemplateService] [runTask-0] adding component template [behavioral_analytics-events-settings]
[2023-12-18T14:31:22,211][INFO ][o.e.c.m.MetadataIndexTemplateService] [runTask-0] adding component template [logs@settings]
[2023-12-18T14:31:22,211][INFO ][o.e.c.m.MetadataIndexTemplateService] [runTask-0] adding component template [logs-settings]
[2023-12-18T14:31:22,258][INFO ][o.e.c.m.MetadataIndexTemplateService] [runTask-0] adding index template [behavioral_analytics-events-default] for index patterns [behavioral_analytics-events-*]
[2023-12-18T14:31:22,261][INFO ][o.e.c.m.MetadataIndexTemplateService] [runTask-0] adding index template [logs] for index patterns [logs-*-*]
[2023-12-18T14:31:22,293][INFO ][o.e.x.i.a.TransportPutLifecycleAction] [runTask-0] adding index lifecycle policy [logs]
[2023-12-18T14:31:22,319][INFO ][o.e.x.i.a.TransportPutLifecycleAction] [runTask-0] adding index lifecycle policy [180-days-default]
[2023-12-18T14:31:22,347][INFO ][o.e.x.i.a.TransportPutLifecycleAction] [runTask-0] adding index lifecycle policy [365-days-default]
[2023-12-18T14:31:22,378][INFO ][o.e.x.i.a.TransportPutLifecycleAction] [runTask-0] adding index lifecycle policy [90-days-default]
[2023-12-18T14:31:22,406][INFO ][o.e.x.i.a.TransportPutLifecycleAction] [runTask-0] adding index lifecycle policy [7-days-default]
[2023-12-18T14:31:22,448][INFO ][o.e.x.i.a.TransportPutLifecycleAction] [runTask-0] adding index lifecycle policy [logs@lifecycle]
[2023-12-18T14:31:22,488][INFO ][o.e.x.i.a.TransportPutLifecycleAction] [runTask-0] adding index lifecycle policy [metrics@lifecycle]
[2023-12-18T14:31:22,522][INFO ][o.e.x.i.a.TransportPutLifecycleAction] [runTask-0] adding index lifecycle policy [synthetics@lifecycle]
[2023-12-18T14:31:22,551][INFO ][o.e.x.i.a.TransportPutLifecycleAction] [runTask-0] adding index lifecycle policy [90-days@lifecycle]
[2023-12-18T14:31:22,587][INFO ][o.e.x.i.a.TransportPutLifecycleAction] [runTask-0] adding index lifecycle policy [180-days@lifecycle]
[2023-12-18T14:31:22,621][INFO ][o.e.x.i.a.TransportPutLifecycleAction] [runTask-0] adding index lifecycle policy [7-days@lifecycle]
[2023-12-18T14:31:22,648][INFO ][o.e.x.i.a.TransportPutLifecycleAction] [runTask-0] adding index lifecycle policy [365-days@lifecycle]
[2023-12-18T14:31:22,675][INFO ][o.e.x.i.a.TransportPutLifecycleAction] [runTask-0] adding index lifecycle policy [30-days@lifecycle]
[2023-12-18T14:31:22,701][INFO ][o.e.x.i.a.TransportPutLifecycleAction] [runTask-0] adding index lifecycle policy [watch-history-ilm-policy-16]
[2023-12-18T14:31:22,729][INFO ][o.e.x.i.a.TransportPutLifecycleAction] [runTask-0] adding index lifecycle policy [slm-history-ilm-policy]
[2023-12-18T14:31:22,770][INFO ][o.e.x.i.a.TransportPutLifecycleAction] [runTask-0] adding index lifecycle policy [ilm-history-ilm-policy]
[2023-12-18T14:31:22,809][INFO ][o.e.x.i.a.TransportPutLifecycleAction] [runTask-0] adding index lifecycle policy [.deprecation-indexing-ilm-policy]
[2023-12-18T14:31:22,846][INFO ][o.e.x.i.a.TransportPutLifecycleAction] [runTask-0] adding index lifecycle policy [.fleet-file-tohost-data-ilm-policy]
[2023-12-18T14:31:22,875][INFO ][o.e.x.i.a.TransportPutLifecycleAction] [runTask-0] adding index lifecycle policy [.fleet-file-fromhost-meta-ilm-policy]
[2023-12-18T14:31:22,903][INFO ][o.e.x.i.a.TransportPutLifecycleAction] [runTask-0] adding index lifecycle policy [.fleet-file-fromhost-data-ilm-policy]
[2023-12-18T14:31:22,932][INFO ][o.e.x.i.a.TransportPutLifecycleAction] [runTask-0] adding index lifecycle policy [.fleet-actions-results-ilm-policy]
[2023-12-18T14:31:22,959][INFO ][o.e.x.i.a.TransportPutLifecycleAction] [runTask-0] adding index lifecycle policy [.fleet-file-tohost-meta-ilm-policy]
[2023-12-18T14:31:23,055][INFO ][o.e.h.n.s.HealthNodeTaskExecutor] [runTask-0] Node [{runTask-0}{RpeX_621SdCZSBh8-RSQvg}] is selected as the current health node.
[2023-12-18T14:31:23,113][INFO ][o.e.x.s.a.Realms         ] [runTask-0] license mode is [basic], currently licensed security realms are [reserved/reserved,file/default_file,native/default_native]
[2023-12-18T14:31:23,114][INFO ][o.e.l.ClusterStateLicenseService] [runTask-0] license [d539d324-3578-49b7-8410-aa7c09249acf] mode [basic] - valid
<============-> 99% EXECUTING [34s]
> IDLE
> IDLE
> IDLE
> :run



