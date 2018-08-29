/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.collector.node;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.cache.query.QueryCacheStats;
import org.elasticsearch.index.cache.request.RequestCacheStats;
import org.elasticsearch.index.engine.SegmentsStats;
import org.elasticsearch.index.fielddata.FieldDataStats;
import org.elasticsearch.index.search.stats.SearchStats;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.index.shard.IndexingStats;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.indices.NodeIndicesStats;
import org.elasticsearch.monitor.fs.FsInfo;
import org.elasticsearch.monitor.jvm.JvmStats;
import org.elasticsearch.monitor.os.OsStats;
import org.elasticsearch.monitor.process.ProcessStats;
import org.elasticsearch.threadpool.ThreadPoolStats;
import org.elasticsearch.xpack.core.monitoring.MonitoredSystem;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringDoc;
import org.elasticsearch.xpack.monitoring.exporter.BaseFilteredMonitoringDocTestCase;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;

public class NodeStatsMonitoringDocTests extends BaseFilteredMonitoringDocTestCase<NodeStatsMonitoringDoc> {

    private String nodeId;
    private boolean isMaster;
    private NodeStats nodeStats;
    private boolean mlockall;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        nodeId = randomAlphaOfLength(5);
        isMaster = randomBoolean();
        nodeStats = mock(NodeStats.class);
        mlockall = randomBoolean();
    }

    @Override
    protected NodeStatsMonitoringDoc createMonitoringDoc(String cluster, long timestamp, long interval, MonitoringDoc.Node node,
                                                         MonitoredSystem system, String type, String id) {
        return new NodeStatsMonitoringDoc(cluster, timestamp, interval, node, nodeId, isMaster, nodeStats, mlockall);
    }

    @Override
    protected void assertFilteredMonitoringDoc(final NodeStatsMonitoringDoc document) {
        assertThat(document.getSystem(), is(MonitoredSystem.ES));
        assertThat(document.getType(), is(NodeStatsMonitoringDoc.TYPE));
        assertThat(document.getId(), nullValue());

        assertThat(document.isNodeMaster(), equalTo(isMaster));
        assertThat(document.getNodeId(), equalTo(nodeId));
        assertThat(document.getNodeStats(), is(nodeStats));
        assertThat(document.isMlockall(), equalTo(mlockall));
    }

    @Override
    protected Set<String> getExpectedXContentFilters() {
        return NodeStatsMonitoringDoc.XCONTENT_FILTERS;
    }

    public void testConstructorNodeIdMustNotBeNull() {
        expectThrows(NullPointerException.class, () ->
                new NodeStatsMonitoringDoc(cluster, timestamp, interval, node, null, isMaster, nodeStats, mlockall));
    }

    public void testConstructorNodeStatsMustNotBeNull() {
        expectThrows(NullPointerException.class, () ->
                new NodeStatsMonitoringDoc(cluster, timestamp, interval, node, nodeId, isMaster, null, mlockall));
    }

    @Override
    public void testToXContent() throws IOException {
        final MonitoringDoc.Node node = new MonitoringDoc.Node("_uuid", "_host", "_addr", "_ip", "_name", 1504169190855L);
        final NodeStats nodeStats = mockNodeStats();
        final ShardRouting unassignedShard = nodeStats.getShardsStats().get(0).getShardRouting();
        final ShardRouting startedShard = nodeStats.getShardsStats().get(1).getShardRouting();

        final NodeStatsMonitoringDoc doc =
                new NodeStatsMonitoringDoc("_cluster", 1502107402133L, 1506593717631L, node, "_node_id", true, nodeStats, false);

        final BytesReference xContent = XContentHelper.toXContent(doc, XContentType.JSON, false);
        assertEquals("{"
                     + "\"cluster_uuid\":\"_cluster\","
                     + "\"timestamp\":\"2017-08-07T12:03:22.133Z\","
                     + "\"interval_ms\":1506593717631,"
                     + "\"type\":\"node_stats\","
                     + "\"source_node\":{"
                       + "\"uuid\":\"_uuid\","
                       + "\"host\":\"_host\","
                       + "\"transport_address\":\"_addr\","
                       + "\"ip\":\"_ip\","
                       + "\"name\":\"_name\","
                       + "\"timestamp\":\"2017-08-31T08:46:30.855Z\""
                     + "},"
                     + "\"node_stats\":{"
                       + "\"node_id\":\"_node_id\","
                       + "\"node_master\":true,"
                       + "\"mlockall\":false,"
                       + "\"indices\":{"
                         + "\"docs\":{"
                           + "\"count\":1"
                         + "},"
                         + "\"store\":{"
                           + "\"size_in_bytes\":5"
                         + "},"
                         + "\"indexing\":{"
                           + "\"index_total\":5,"
                           + "\"index_time_in_millis\":6,"
                           + "\"throttle_time_in_millis\":8"
                         + "},"
                         + "\"search\":{"
                           + "\"query_total\":17,"
                           + "\"query_time_in_millis\":18"
                         + "},"
                         + "\"query_cache\":{"
                           + "\"memory_size_in_bytes\":9,"
                           + "\"hit_count\":10,"
                           + "\"miss_count\":11,"
                           + "\"evictions\":13"
                         + "},"
                         + "\"fielddata\":{"
                           + "\"memory_size_in_bytes\":3,"
                           + "\"evictions\":4"
                         + "},"
                         + "\"segments\":{"
                           + "\"count\":19,"
                           + "\"memory_in_bytes\":20,"
                           + "\"terms_memory_in_bytes\":21,"
                           + "\"stored_fields_memory_in_bytes\":22,"
                           + "\"term_vectors_memory_in_bytes\":23,"
                           + "\"norms_memory_in_bytes\":24,"
                           + "\"points_memory_in_bytes\":25,"
                           + "\"doc_values_memory_in_bytes\":26,"
                           + "\"index_writer_memory_in_bytes\":27,"
                           + "\"version_map_memory_in_bytes\":28,"
                           + "\"fixed_bit_set_memory_in_bytes\":29"
                         + "},"
                         + "\"request_cache\":{"
                           + "\"memory_size_in_bytes\":13,"
                           + "\"evictions\":14,"
                           + "\"hit_count\":15,"
                           + "\"miss_count\":16"
                         + "}"
                       + "},"
                       + "\"os\":{"
                         + "\"cpu\":{"
                           + "\"load_average\":{"
                             + "\"1m\":41.0,"
                             + "\"5m\":42.0,"
                             + "\"15m\":43.0"
                           + "}"
                         + "},"
                         + "\"cgroup\":{"
                           + "\"cpuacct\":{"
                             + "\"control_group\":\"_cpu_acct_ctrl_group\","
                             + "\"usage_nanos\":47"
                           + "},"
                           + "\"cpu\":{"
                             + "\"control_group\":\"_cpu_ctrl_group\","
                             + "\"cfs_period_micros\":48,"
                             + "\"cfs_quota_micros\":49,"
                             + "\"stat\":{"
                               + "\"number_of_elapsed_periods\":44,"
                               + "\"number_of_times_throttled\":45,"
                               + "\"time_throttled_nanos\":46"
                             + "}"
                           + "},"
                           + "\"memory\":{"
                             + "\"control_group\":\"_memory_ctrl_group\","
                             + "\"limit_in_bytes\":\"2000000000\","
                             + "\"usage_in_bytes\":\"1000000000\""
                           + "}"
                        + "}"
                       + "},"
                       + "\"process\":{"
                         + "\"open_file_descriptors\":51,"
                         + "\"max_file_descriptors\":52,"
                         + "\"cpu\":{"
                           + "\"percent\":50"
                         + "}"
                       + "},"
                       + "\"jvm\":{"
                         + "\"mem\":{"
                           + "\"heap_used_in_bytes\":53,"
                           + "\"heap_used_percent\":98,"
                           + "\"heap_max_in_bytes\":54"
                         + "},"
                         + "\"gc\":{"
                           + "\"collectors\":{"
                             + "\"young\":{"
                               + "\"collection_count\":55,"
                               + "\"collection_time_in_millis\":56"
                             + "},"
                             + "\"old\":{"
                               + "\"collection_count\":57,"
                               + "\"collection_time_in_millis\":58"
                             + "}"
                            + "}"
                        + "}"
                       + "},"
                       + "\"thread_pool\":{"
                         + "\"generic\":{"
                           + "\"threads\":59,"
                           + "\"queue\":60,"
                           + "\"rejected\":61"
                         + "},"
                         + "\"get\":{"
                           + "\"threads\":62,"
                           + "\"queue\":63,"
                           + "\"rejected\":64"
                         + "},"
                         + "\"management\":{"
                           + "\"threads\":65,"
                           + "\"queue\":66,"
                           + "\"rejected\":67"
                         + "},"
                         + "\"search\":{"
                           + "\"threads\":68,"
                           + "\"queue\":69,"
                           + "\"rejected\":70"
                         + "},"
                         + "\"watcher\":{"
                           + "\"threads\":71,"
                           + "\"queue\":72,"
                           + "\"rejected\":73"
                         + "},"
                         + "\"write\":{"
                           + "\"threads\":74,"
                           + "\"queue\":75,"
                           + "\"rejected\":76"
                          + "}"
                       + "},"
                       + "\"fs\":{"
                         + "\"total\":{"
                           + "\"total_in_bytes\":38,"
                           + "\"free_in_bytes\":39,"
                           + "\"available_in_bytes\":40"
                         + "},"
                         + "\"io_stats\":{"
                           + "\"total\":{"
                             + "\"operations\":8,"
                             + "\"read_operations\":4,"
                             + "\"write_operations\":4,"
                             + "\"read_kilobytes\":2,"
                             + "\"write_kilobytes\":2"
                           + "}"
                          + "}"
                        + "},"
                        + "\"shards\":{"
                          + "\"stats\":["
                            + "{"
                              + "\"routing\":{"
                                + "\"state\":\"INITIALIZING\","
                                + "\"primary\":false,"
                                + "\"node\":\"_node_id\","
                                + "\"relocating_node\":null,"
                                + "\"shard\":77,"
                                + "\"index\":\"init-1\","
                                + "\"recovery_source\":{"
                                  + "\"type\":\"PEER\""
                                + "},"
                                +  "\"allocation_id\":{"
                                  + "\"id\":\"" + unassignedShard.allocationId().getId() + "\""
                                + "},"
                                + "\"unassigned_info\":{"
                                  + "\"reason\":\"" + unassignedShard.unassignedInfo().getReason() + "\","
                                  + "\"at\":\"" + formatMillis(unassignedShard.unassignedInfo().getUnassignedTimeInMillis()) + "\","
                                  + (unassignedShard.unassignedInfo().getNumFailedAllocations() != 0 ? "\"failed_attempts\":1," : "")
                                  + "\"delayed\":false,"
                                  + "\"details\":\"auto generated for test\","
                                  + "\"allocation_status\":\"no_attempt\""
                                + "}"
                              + "}"
                            + "},"
                            + "{"
                              + "\"routing\":{"
                                + "\"state\":\"STARTED\","
                                + "\"primary\":true,"
                                + "\"node\":\"_node_id\","
                                + "\"relocating_node\":null,"
                                + "\"shard\":78,"
                                + "\"index\":\"started-1\","
                                + "\"allocation_id\":{"
                                  + "\"id\":\"" + startedShard.allocationId().getId()
                                + "\"}"
                              + "},"
                              + "\"docs\":{"
                                + "\"count\":79,"
                                + "\"deleted\":80"
                              + "},"
                              + "\"store\":{"
                                + "\"size_in_bytes\":83"
                              + "},"
                              + "\"indexing\":{"
                                + "\"index_total\":0,"
                                + "\"index_time_in_millis\":0,"
                                + "\"index_current\":0,"
                                + "\"index_failed\":0,"
                                + "\"delete_total\":0,"
                                + "\"delete_time_in_millis\":0,"
                                + "\"delete_current\":0,"
                                + "\"noop_update_total\":0,"
                                + "\"is_throttled\":false,"
                                + "\"throttle_time_in_millis\":0"
                              + "},"
                              + "\"get\":{"
                                + "\"total\":0,"
                                + "\"time_in_millis\":0,"
                                + "\"exists_total\":0,"
                                + "\"exists_time_in_millis\":0,"
                                + "\"missing_total\":0,"
                                + "\"missing_time_in_millis\":0,"
                                + "\"current\":0"
                              + "},"
                              + "\"search\":{"
                                + "\"open_contexts\":0,"
                                + "\"query_total\":0,"
                                + "\"query_time_in_millis\":0,"
                                + "\"query_current\":0,"
                                + "\"fetch_total\":0,"
                                + "\"fetch_time_in_millis\":0,"
                                + "\"fetch_current\":0,"
                                + "\"scroll_total\":0,"
                                + "\"scroll_time_in_millis\":0,"
                                + "\"scroll_current\":0,"
                                + "\"suggest_total\":0,"
                                + "\"suggest_time_in_millis\":0,"
                                + "\"suggest_current\":0"
                              + "},"
                              + "\"merges\":{"
                                + "\"current\":0,"
                                + "\"current_docs\":0,"
                                + "\"current_size_in_bytes\":0,"
                                + "\"total\":0,"
                                + "\"total_time_in_millis\":0,"
                                + "\"total_docs\":0,"
                                + "\"total_size_in_bytes\":0,"
                                + "\"total_stopped_time_in_millis\":0,"
                                + "\"total_throttled_time_in_millis\":0,"
                                + "\"total_auto_throttle_in_bytes\":0"
                              + "},"
                              + "\"refresh\":{"
                                + "\"total\":0,"
                                + "\"total_time_in_millis\":0,"
                                + "\"listeners\":0"
                              + "},"
                              + "\"flush\":{"
                                + "\"total\":0,"
                                + "\"periodic\":0,"
                                + "\"total_time_in_millis\":0"
                              + "},"
                              + "\"warmer\":{"
                                + "\"current\":0,"
                                + "\"total\":0,"
                                + "\"total_time_in_millis\":0"
                              + "},"
                              + "\"query_cache\":{"
                                + "\"memory_size_in_bytes\":0,"
                                + "\"total_count\":0,"
                                + "\"hit_count\":0,"
                                + "\"miss_count\":0,"
                                + "\"cache_size\":0,"
                                + "\"cache_count\":0,"
                                + "\"evictions\":0"
                              + "},"
                              + "\"fielddata\":{"
                                + "\"memory_size_in_bytes\":81,"
                                + "\"evictions\":82"
                              + "},"
                              + "\"completion\":{"
                                + "\"size_in_bytes\":0"
                              + "},"
                              + "\"segments\":{"
                                + "\"count\":0,"
                                + "\"memory_in_bytes\":0,"
                                + "\"terms_memory_in_bytes\":0,"
                                + "\"stored_fields_memory_in_bytes\":0,"
                                + "\"term_vectors_memory_in_bytes\":0,"
                                + "\"norms_memory_in_bytes\":0,"
                                + "\"points_memory_in_bytes\":0,"
                                + "\"doc_values_memory_in_bytes\":0,"
                                + "\"index_writer_memory_in_bytes\":0,"
                                + "\"version_map_memory_in_bytes\":0,"
                                + "\"fixed_bit_set_memory_in_bytes\":0,"
                                + "\"max_unsafe_auto_id_timestamp\":-9223372036854775808,"
                                + "\"file_sizes\":{}"
                              + "},"
                              + "\"translog\":{"
                                + "\"operations\":0,"
                                + "\"size_in_bytes\":0,"
                                + "\"uncommitted_operations\":0,"
                                + "\"uncommitted_size_in_bytes\":0,"
                                + "\"earliest_last_modified_age\":0"
                              + "},"
                              + "\"request_cache\":{"
                                + "\"memory_size_in_bytes\":0,"
                                + "\"evictions\":0,"
                                + "\"hit_count\":0,"
                                + "\"miss_count\":0"
                              + "},"
                              + "\"recovery\":{"
                                + "\"current_as_source\":0,"
                                + "\"current_as_target\":0,"
                                + "\"throttle_time_in_millis\":0"
                              + "}"
                            + "}"
                          + "],"
                          + "\"routing\":{"
                            + "\"primaries\":1,"
                            + "\"total\":2,"
                            + "\"replicas\":1,"
                            + "\"relocating_from_node\":0,"
                            + "\"active\":1,"
                            + "\"relocating_to_node\":0,"
                            + "\"initializing\":1,"
                            + "\"unassigned\":0"
                          + "}"
                        + "}"
                      + "}"
                    + "}", xContent.utf8ToString());
    }

    private String formatMillis(long millis) {
        return UnassignedInfo.DATE_TIME_FORMATTER.format(Instant.ofEpochMilli(millis));
    }

    private NodeStats mockNodeStats() {

        // This value is used in constructors of various stats objects,
        // when the value is not printed out in the final XContent.
        final int no = -1;

        // This value is used in constructors of various stats objects,
        // when the value is printed out in the XContent. Must be
        // incremented for each usage.
        int iota = 0;

        // Indices
        final CommonStats indicesCommonStats = createCommonStats(iota);
        // createCommonStats did not used to be a method, so we add the same number to ensure that the rest of the JSON stays the same
        iota += 4;

        final IndexingStats.Stats indexingStats = new IndexingStats.Stats(++iota, ++iota, ++iota, no, no, no, no, no, false, ++iota);
        indicesCommonStats.getIndexing().add(new IndexingStats(indexingStats, null));
        indicesCommonStats.getQueryCache().add(new QueryCacheStats(++iota, ++iota, ++iota, ++iota, no));
        indicesCommonStats.getRequestCache().add(new RequestCacheStats(++iota, ++iota, ++iota, ++iota));

        final SearchStats.Stats searchStats = new SearchStats.Stats(++iota, ++iota, no, no, no, no, no, no, no, no, no, no);
        indicesCommonStats.getSearch().add(new SearchStats(searchStats, no, null));

        indicesCommonStats.getSegments().add(createSegmentsStats(iota));
        // createSegmentsStats did not used to be a method, so we add the same number to ensure that the rest of the JSON stays the same
        iota += 11;

        final NodeIndicesStats indices = new NodeIndicesStats(indicesCommonStats, emptyMap());

        // Filesystem
        final FsInfo.DeviceStats ioStatsOne = new FsInfo.DeviceStats(no, no, null, ++iota, ++iota, ++iota, ++iota, null);
        final FsInfo.DeviceStats ioStatsTwo = new FsInfo.DeviceStats(no, no, null, ++iota, ++iota, ++iota, ++iota, ioStatsOne);

        final FsInfo.IoStats ioStats = new FsInfo.IoStats(new FsInfo.DeviceStats[]{ioStatsTwo});
        final FsInfo fs = new FsInfo(no, ioStats, new FsInfo.Path[]{new FsInfo.Path(null, null, ++iota, ++iota, ++iota)});

        // Os
        final OsStats.Cpu osCpu = new OsStats.Cpu((short) no, new double[]{++iota, ++iota, ++iota});
        final OsStats.Cgroup.CpuStat osCpuStat = new OsStats.Cgroup.CpuStat(++iota, ++iota, ++iota);
        final OsStats.Cgroup osCgroup = new OsStats.Cgroup("_cpu_acct_ctrl_group", ++iota, "_cpu_ctrl_group", ++iota, ++iota, osCpuStat,
                "_memory_ctrl_group", "2000000000", "1000000000");

        final OsStats.Mem osMem = new OsStats.Mem(no, no);
        final OsStats.Swap osSwap = new OsStats.Swap(no, no);
        final OsStats os = new OsStats(no, osCpu, osMem, osSwap, osCgroup);

        // Process
        final ProcessStats.Cpu processCpu = new ProcessStats.Cpu((short) ++iota, no);
        final ProcessStats process = new ProcessStats(no, ++iota, ++iota, processCpu, null);

        // Jvm
        final JvmStats.Threads jvmThreads = new JvmStats.Threads(no, no);
        final JvmStats.Classes jvmClasses = new JvmStats.Classes(no, no, no);
        final JvmStats.Mem jvmMem = new JvmStats.Mem(no, ++iota, ++iota, no, no, emptyList());
        final JvmStats.GarbageCollectors gcs = new JvmStats.GarbageCollectors(new JvmStats.GarbageCollector[]{
                new JvmStats.GarbageCollector("young", ++iota, ++iota),
                new JvmStats.GarbageCollector("old", ++iota, ++iota)});
        final JvmStats jvm = new JvmStats(no, no, jvmMem, jvmThreads, gcs, emptyList(), jvmClasses);

        // Threadpools
        final List<ThreadPoolStats.Stats> threadpools = new ArrayList<>();
        threadpools.add(new ThreadPoolStats.Stats("generic", ++iota, ++iota, no, ++iota, no, no));
        threadpools.add(new ThreadPoolStats.Stats("get", ++iota, ++iota, no, ++iota, no, no));
        threadpools.add(new ThreadPoolStats.Stats("management", ++iota, ++iota, no, ++iota, no, no));
        threadpools.add(new ThreadPoolStats.Stats("search", ++iota, ++iota, no, ++iota, no, no));
        threadpools.add(new ThreadPoolStats.Stats("watcher", ++iota, ++iota, no, ++iota, no, no));
        threadpools.add(new ThreadPoolStats.Stats("write", ++iota, ++iota, no, ++iota, no, no));
        final ThreadPoolStats threadPool = new ThreadPoolStats(threadpools);

        final DiscoveryNode discoveryNode = new DiscoveryNode("_node_name",
                                                                "_node_id",
                                                                "_ephemeral_id",
                                                                "_host_name",
                                                                "_host_address",
                                                                new TransportAddress(TransportAddress.META_ADDRESS, 1234),
                                                                emptyMap(),
                                                                emptySet(),
                                                                Version.V_6_0_0_beta1);

        // Shards
        final List<ShardStats> shards = createShardStats(iota);

        return new NodeStats(discoveryNode, no, indices, os, process, jvm, threadPool, fs,
                    null, null, null, null, null, null, null,
                     shards);
    }

    private List<ShardStats> createShardStats(int iota) {
        final List<ShardStats> shards = new ArrayList<>();

        // Initializing shard
        final ShardId initializingShardId = new ShardId("init-1", "initUuid1", ++iota);
        final ShardRouting initializingRoute =
            TestShardRouting.newShardRouting(initializingShardId, "_node_id", false, ShardRoutingState.INITIALIZING);
        final ShardPath initializingPath = createShardPath(initializingShardId);
        final ShardStats initializingShard = new ShardStats(initializingRoute, initializingPath, new CommonStats(), null, null);

        // Started shard
        final ShardId startedShardId = new ShardId("started-1", "startedUuid2", ++iota);
        final ShardRouting startedRoute =
            TestShardRouting.newShardRouting(startedShardId, "_node_id", true, ShardRoutingState.STARTED);
        final ShardPath startedPath = createShardPath(startedShardId);
        final ShardStats startedShard =
            new ShardStats(startedRoute, startedPath, createCommonStats(iota), null, null);

        // Shard stats
        shards.add(initializingShard);
        shards.add(startedShard);

        return shards;
    }

    private ShardPath createShardPath(final ShardId id) {
        final Path path = createTempDir().resolve(id.getIndex().getUUID()).resolve(Integer.toString(id.getId()));

        return new ShardPath(false, path, path, id);
    }

    private CommonStats createCommonStats(int iota) {
        final CommonStats commonStats = new CommonStats(CommonStatsFlags.ALL);
        commonStats.getDocs().add(new DocsStats(++iota, ++iota, randomNonNegativeLong()));
        commonStats.getFieldData().add(new FieldDataStats(++iota, ++iota, null));
        commonStats.getStore().add(new StoreStats(++iota));
        return commonStats;
    }

    private SegmentsStats createSegmentsStats(int iota) {
        final SegmentsStats segmentsStats = new SegmentsStats();
        segmentsStats.add(++iota, ++iota);
        segmentsStats.addTermsMemoryInBytes(++iota);
        segmentsStats.addStoredFieldsMemoryInBytes(++iota);
        segmentsStats.addTermVectorsMemoryInBytes(++iota);
        segmentsStats.addNormsMemoryInBytes(++iota);
        segmentsStats.addPointsMemoryInBytes(++iota);
        segmentsStats.addDocValuesMemoryInBytes(++iota);
        segmentsStats.addIndexWriterMemoryInBytes(++iota);
        segmentsStats.addVersionMapMemoryInBytes(++iota);
        segmentsStats.addBitsetMemoryInBytes(++iota);
        return segmentsStats;
    }

}
