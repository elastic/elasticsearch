/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.collector.indices;

import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.bulk.stats.BulkStats;
import org.elasticsearch.index.search.stats.SearchStats;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.index.shard.IndexingStats;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.xpack.core.monitoring.MonitoredSystem;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringDoc;
import org.elasticsearch.xpack.monitoring.exporter.BaseFilteredMonitoringDocTestCase;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IndicesStatsMonitoringDocTests extends BaseFilteredMonitoringDocTestCase<IndicesStatsMonitoringDoc> {

    private List<IndexStats> indicesStats;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        indicesStats = Collections.singletonList(new IndexStats("index-0", "dcvO5uZATE-EhIKc3tk9Bg", new ShardStats[] {
                // Primaries
                new ShardStats(mockShardRouting(true), mockShardPath(), mockCommonStats(), null, null, null),
                new ShardStats(mockShardRouting(true), mockShardPath(), mockCommonStats(), null, null, null),
                // Replica
                new ShardStats(mockShardRouting(false), mockShardPath(), mockCommonStats(), null, null, null)
        }));
    }

    @Override
    protected IndicesStatsMonitoringDoc createMonitoringDoc(String cluster, long timestamp, long interval, MonitoringDoc.Node node,
                                                            MonitoredSystem system, String type, String id) {
        return new IndicesStatsMonitoringDoc(cluster, timestamp, interval, node, indicesStats);
    }

    @Override
    protected void assertFilteredMonitoringDoc(final IndicesStatsMonitoringDoc document) {
        assertThat(document.getSystem(), is(MonitoredSystem.ES));
        assertThat(document.getType(), is(IndicesStatsMonitoringDoc.TYPE));
        assertThat(document.getId(), nullValue());

        assertThat(document.getIndicesStats(), is(indicesStats));
    }

    @Override
    protected Set<String> getExpectedXContentFilters() {
        return IndicesStatsMonitoringDoc.XCONTENT_FILTERS;
    }

    public void testConstructorIndexStatsMustNotBeNull() {
        expectThrows(NullPointerException.class, () -> new IndicesStatsMonitoringDoc(cluster, timestamp, interval, node, null));
    }

    @Override
    public void testToXContent() throws IOException {
        final MonitoringDoc.Node node = new MonitoringDoc.Node("_uuid", "_host", "_addr", "_ip", "_name", 1504169190855L);
        final IndicesStatsMonitoringDoc document =
                new IndicesStatsMonitoringDoc("_cluster", 1502266739402L, 1506593717631L, node, indicesStats);

        final BytesReference xContent = XContentHelper.toXContent(document, XContentType.JSON, false);
        final String expected = XContentHelper.stripWhitespace(
            "{"
                + "  \"cluster_uuid\": \"_cluster\","
                + "  \"timestamp\": \"2017-08-09T08:18:59.402Z\","
                + "  \"interval_ms\": 1506593717631,"
                + "  \"type\": \"indices_stats\","
                + "  \"source_node\": {"
                + "    \"uuid\": \"_uuid\","
                + "    \"host\": \"_host\","
                + "    \"transport_address\": \"_addr\","
                + "    \"ip\": \"_ip\","
                + "    \"name\": \"_name\","
                + "    \"timestamp\": \"2017-08-31T08:46:30.855Z\""
                + "  },"
                + "  \"indices_stats\": {"
                + "    \"_all\": {"
                + "      \"primaries\": {"
                + "        \"docs\": {"
                + "          \"count\": 2"
                + "        },"
                + "        \"store\": {"
                + "          \"size_in_bytes\": 4"
                + "        },"
                + "        \"indexing\": {"
                + "          \"index_total\": 6,"
                + "          \"index_time_in_millis\": 8,"
                + "          \"is_throttled\": true,"
                + "          \"throttle_time_in_millis\": 10"
                + "        },"
                + "        \"search\": {"
                + "          \"query_total\": 12,"
                + "          \"query_time_in_millis\": 14"
                + "        },"
                + "        \"bulk\": {"
                + "          \"total_operations\": 0,"
                + "          \"total_time_in_millis\": 0,"
                + "          \"total_size_in_bytes\": 0,"
                + "          \"avg_time_in_millis\": 0,"
                + "          \"avg_size_in_bytes\": 0"
                + "        }"
                + "      },"
                + "      \"total\": {"
                + "        \"docs\": {"
                + "          \"count\": 3"
                + "        },"
                + "        \"store\": {"
                + "          \"size_in_bytes\": 6"
                + "        },"
                + "        \"indexing\": {"
                + "          \"index_total\": 9,"
                + "          \"index_time_in_millis\": 12,"
                + "          \"is_throttled\": true,"
                + "          \"throttle_time_in_millis\": 15"
                + "        },"
                + "        \"search\": {"
                + "          \"query_total\": 18,"
                + "          \"query_time_in_millis\": 21"
                + "        },"
                + "        \"bulk\": {"
                + "          \"total_operations\": 0,"
                + "          \"total_time_in_millis\": 0,"
                + "          \"total_size_in_bytes\": 0,"
                + "          \"avg_time_in_millis\": 0,"
                + "          \"avg_size_in_bytes\": 0"
                + "        }"
                + "      }"
                + "    }"
                + "  }"
                + "}"
        );
        assertEquals(expected, xContent.utf8ToString());
    }

    private CommonStats mockCommonStats() {
        final CommonStats commonStats = new CommonStats(CommonStatsFlags.ALL);
        commonStats.getDocs().add(new DocsStats(1L, 0L, randomNonNegativeLong()));
        commonStats.getStore().add(new StoreStats(2L));

        final IndexingStats.Stats indexingStats = new IndexingStats.Stats(3L, 4L, 0L, 0L, 0L, 0L, 0L, 0L, true, 5L);
        commonStats.getIndexing().add(new IndexingStats(indexingStats));

        final SearchStats.Stats searchStats = new SearchStats.Stats(6L, 7L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L);
        commonStats.getSearch().add(new SearchStats(searchStats, 0L, null));

        final BulkStats bulkStats = new BulkStats(0L, 0L, 0L, 0L, 0L);
        commonStats.getBulk().add(bulkStats);

        return commonStats;
    }

    private ShardPath mockShardPath() {
        // Mock paths in a way that pass ShardPath constructor assertions
        final int shardId = randomIntBetween(0, 10);
        final Path getFileNameShardId = mock(Path.class);
        when(getFileNameShardId.toString()).thenReturn(Integer.toString(shardId));

        final String shardUuid =  randomAlphaOfLength(5);
        final Path getFileNameShardUuid = mock(Path.class);
        when(getFileNameShardUuid.toString()).thenReturn(shardUuid);

        final Path getParent = mock(Path.class);
        when(getParent.getFileName()).thenReturn(getFileNameShardUuid);

        final Path path = mock(Path.class);
        when(path.getParent()).thenReturn(getParent);
        when(path.getFileName()).thenReturn(getFileNameShardId);

        // Mock paths for ShardPath#getRootDataPath()
        final Path getParentOfParent = mock(Path.class);
        when(getParent.getParent()).thenReturn(getParentOfParent);
        when(getParentOfParent.getParent()).thenReturn(mock(Path.class));

        return new ShardPath(false, path, path, new ShardId(randomAlphaOfLength(5), shardUuid, shardId));
    }

    private ShardRouting mockShardRouting(final boolean primary) {
        return TestShardRouting.newShardRouting(randomAlphaOfLength(5), randomInt(), randomAlphaOfLength(5), primary, INITIALIZING);
    }
}
