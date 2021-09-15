/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.monitoring.collector.indices;

import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.cluster.health.ClusterIndexHealth;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.cache.query.QueryCacheStats;
import org.elasticsearch.index.cache.request.RequestCacheStats;
import org.elasticsearch.index.engine.SegmentsStats;
import org.elasticsearch.index.fielddata.FieldDataStats;
import org.elasticsearch.index.refresh.RefreshStats;
import org.elasticsearch.index.search.stats.SearchStats;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.index.shard.IndexingStats;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.xpack.core.monitoring.MonitoredSystem;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringDoc;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringTemplateUtils;
import org.elasticsearch.xpack.monitoring.exporter.BaseFilteredMonitoringDocTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.Date;
import java.util.Locale;
import java.util.Set;

import static org.elasticsearch.common.xcontent.XContentHelper.convertToJson;
import static org.elasticsearch.common.xcontent.XContentHelper.stripWhitespace;
import static org.elasticsearch.xpack.core.ilm.CheckShrinkReadyStepTests.randomUnassignedInfo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IndexStatsMonitoringDocTests extends BaseFilteredMonitoringDocTestCase<IndexStatsMonitoringDoc> {

    private final Index index = new Index("logstash-2017.10.27", "aBcDeFg");
    private final int primaries = randomIntBetween(1, 5);
    private final int replicas = randomIntBetween(0, 2);
    private final int total = primaries + (primaries * replicas);
    private final int activePrimaries = randomInt(primaries);
    private final int activeReplicas = randomInt(activePrimaries * replicas);
    private final int initializing = randomInt(primaries - activePrimaries + Math.max(0, activePrimaries * replicas - activeReplicas));
    // to simplify the test code, we only allow active primaries to relocate, rather than also include active replicas
    private final int relocating = randomInt(activePrimaries);

    private IndexStats indexStats;
    private IndexMetadata metadata;
    private IndexRoutingTable routingTable;
    private ClusterIndexHealth indexHealth;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        indexStats = mock(IndexStats.class);
        metadata = mockIndexMetadata(index, primaries, replicas);
        routingTable = mockIndexRoutingTable(index, primaries, replicas, activePrimaries, activeReplicas, initializing, relocating);
        indexHealth = new ClusterIndexHealth(metadata, routingTable);
    }

    @Override
    protected IndexStatsMonitoringDoc createMonitoringDoc(String cluster, long timestamp, long interval, MonitoringDoc.Node node,
                                                          MonitoredSystem system, String type, String id) {
        return new IndexStatsMonitoringDoc(cluster, timestamp, interval, node, indexStats, metadata, routingTable);
    }

    @Override
    protected void assertFilteredMonitoringDoc(final IndexStatsMonitoringDoc document) {
        assertThat(document.getSystem(), is(MonitoredSystem.ES));
        assertThat(document.getType(), is(IndexStatsMonitoringDoc.TYPE));
        assertThat(document.getId(), nullValue());

        assertThat(document.getIndexStats(), is(indexStats));
        assertThat(document.getIndexMetadata(), is(metadata));
        assertThat(document.getIndexRoutingTable(), is(routingTable));
    }

    @Override
    protected Set<String> getExpectedXContentFilters() {
        return IndexStatsMonitoringDoc.XCONTENT_FILTERS;
    }

    public void testConstructorIndexStatsCanBeNull() {
        new IndexStatsMonitoringDoc(cluster, timestamp, interval, node, null, metadata, routingTable);
    }

    public void testConstructorMetadataMustNotBeNull() {
        final IndexStats indexStats = randomFrom(this.indexStats, null);

        expectThrows(NullPointerException.class,
                     () -> new IndexStatsMonitoringDoc(cluster, timestamp, interval, node, indexStats, null, routingTable));
    }

    public void testConstructorRoutingTableMustNotBeNull() {
        final IndexStats indexStats = randomFrom(this.indexStats, null);

        expectThrows(NullPointerException.class,
                     () -> new IndexStatsMonitoringDoc(cluster, timestamp, interval, node, indexStats, metadata, null));
    }

    @Override
    public void testToXContent() throws IOException {
        final MonitoringDoc.Node node = new MonitoringDoc.Node("_uuid", "_host", "_addr", "_ip", "_name", 1504169190855L);
        when(indexStats.getTotal()).thenReturn(mockCommonStats());
        when(indexStats.getPrimaries()).thenReturn(mockCommonStats());

        final IndexStatsMonitoringDoc document =
                new IndexStatsMonitoringDoc("_cluster", 1502266739402L, 1506593717631L, node, indexStats, metadata, routingTable);

        final BytesReference xContent;
        try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
            document.toXContent(builder, ToXContent.EMPTY_PARAMS);
            xContent = BytesReference.bytes(builder);
        }

        final String expected = stripWhitespace(String.format(Locale.ROOT, "{"
            + "  \"cluster_uuid\": \"_cluster\","
            + "  \"timestamp\": \"2017-08-09T08:18:59.402Z\","
            + "  \"interval_ms\": 1506593717631,"
            + "  \"type\": \"index_stats\","
            + "  \"source_node\": {"
            + "    \"uuid\": \"_uuid\","
            + "    \"host\": \"_host\","
            + "    \"transport_address\": \"_addr\","
            + "    \"ip\": \"_ip\","
            + "    \"name\": \"_name\","
            + "    \"timestamp\": \"2017-08-31T08:46:30.855Z\""
            + "  },"
            + "  \"index_stats\": {"
            + "    %s," // indexStatsSummary()
            + "    \"total\": {"
            + "      \"docs\": {"
            + "        \"count\": 1"
            + "      },"
            + "      \"store\": {"
            + "        \"size_in_bytes\": 13"
            + "      },"
            + "      \"indexing\": {"
            + "        \"index_total\": 16,"
            + "        \"index_time_in_millis\": 17,"
            + "        \"throttle_time_in_millis\": 18"
            + "      },"
            + "      \"search\": {"
            + "        \"query_total\": 19,"
            + "        \"query_time_in_millis\": 20"
            + "      },"
            + "      \"merges\": {"
            + "        \"total_size_in_bytes\": 4"
            + "      },"
            + "      \"refresh\": {"
            + "        \"total_time_in_millis\": 14,"
            + "        \"external_total_time_in_millis\": 15"
            + "      },"
            + "      \"query_cache\": {"
            + "        \"memory_size_in_bytes\": 5,"
            + "        \"hit_count\": 6,"
            + "        \"miss_count\": 7,"
            + "        \"evictions\": 9"
            + "      },"
            + "      \"fielddata\": {"
            + "        \"memory_size_in_bytes\": 2,"
            + "        \"evictions\": 3"
            + "      },"
            + "      \"segments\": {"
            + "        \"count\": 21,"
            + "        \"memory_in_bytes\": 0,"
            + "        \"terms_memory_in_bytes\": 0,"
            + "        \"stored_fields_memory_in_bytes\": 0,"
            + "        \"term_vectors_memory_in_bytes\": 0,"
            + "        \"norms_memory_in_bytes\": 0,"
            + "        \"points_memory_in_bytes\": 0,"
            + "        \"doc_values_memory_in_bytes\": 0,"
            + "        \"index_writer_memory_in_bytes\": 22,"
            + "        \"version_map_memory_in_bytes\": 23,"
            + "        \"fixed_bit_set_memory_in_bytes\": 24"
            + "      },"
            + "      \"request_cache\": {"
            + "        \"memory_size_in_bytes\": 9,"
            + "        \"evictions\": 10,"
            + "        \"hit_count\": 11,"
            + "        \"miss_count\": 12"
            + "      },"
            + "      \"bulk\": {"
            + "        \"total_operations\": 0,"
            + "        \"total_time_in_millis\": 0,"
            + "        \"total_size_in_bytes\": 0,"
            + "        \"avg_time_in_millis\": 0,"
            + "        \"avg_size_in_bytes\": 0"
            + "      }"
            + "    },"
            + "    \"primaries\": {"
            + "      \"docs\": {"
            + "        \"count\": 1"
            + "      },"
            + "      \"store\": {"
            + "        \"size_in_bytes\": 13"
            + "      },"
            + "      \"indexing\": {"
            + "        \"index_total\": 16,"
            + "        \"index_time_in_millis\": 17,"
            + "        \"throttle_time_in_millis\": 18"
            + "      },"
            + "      \"search\": {"
            + "        \"query_total\": 19,"
            + "        \"query_time_in_millis\": 20"
            + "      },"
            + "      \"merges\": {"
            + "        \"total_size_in_bytes\": 4"
            + "      },"
            + "      \"refresh\": {"
            + "        \"total_time_in_millis\": 14,"
            + "        \"external_total_time_in_millis\": 15"
            + "      },"
            + "      \"query_cache\": {"
            + "        \"memory_size_in_bytes\": 5,"
            + "        \"hit_count\": 6,"
            + "        \"miss_count\": 7,"
            + "        \"evictions\": 9"
            + "      },"
            + "      \"fielddata\": {"
            + "        \"memory_size_in_bytes\": 2,"
            + "        \"evictions\": 3"
            + "      },"
            + "      \"segments\": {"
            + "        \"count\": 21,"
            + "        \"memory_in_bytes\": 0,"
            + "        \"terms_memory_in_bytes\": 0,"
            + "        \"stored_fields_memory_in_bytes\": 0,"
            + "        \"term_vectors_memory_in_bytes\": 0,"
            + "        \"norms_memory_in_bytes\": 0,"
            + "        \"points_memory_in_bytes\": 0,"
            + "        \"doc_values_memory_in_bytes\": 0,"
            + "        \"index_writer_memory_in_bytes\": 22,"
            + "        \"version_map_memory_in_bytes\": 23,"
            + "        \"fixed_bit_set_memory_in_bytes\": 24"
            + "      },"
            + "      \"request_cache\": {"
            + "        \"memory_size_in_bytes\": 9,"
            + "        \"evictions\": 10,"
            + "        \"hit_count\": 11,"
            + "        \"miss_count\": 12"
            + "      },"
            + "      \"bulk\": {"
            + "        \"total_operations\": 0,"
            + "        \"total_time_in_millis\": 0,"
            + "        \"total_size_in_bytes\": 0,"
            + "        \"avg_time_in_millis\": 0,"
            + "        \"avg_size_in_bytes\": 0"
            + "      }"
            + "    }"
            + "  }"
            + "}",
            // Since the summary is being merged with other data, remove the enclosing braces.
            indexStatsSummary().replaceAll("(^\\{|}$)", "")));
        assertThat(xContent.utf8ToString(), equalTo(expected));
    }

    public void testToXContentWithNullStats() throws IOException {
        final MonitoringDoc.Node node = new MonitoringDoc.Node("_uuid", "_host", "_addr", "_ip", "_name", 1504169190855L);
        final IndexStats indexStats;

        if (randomBoolean()) {
            indexStats = this.indexStats;

            when(indexStats.getTotal()).thenReturn(null);
            when(indexStats.getPrimaries()).thenReturn(null);
        } else {
            indexStats = null;
        }

        final IndexStatsMonitoringDoc document =
                new IndexStatsMonitoringDoc("_cluster", 1502266739402L, 1506593717631L, node, indexStats, metadata, routingTable);

        final BytesReference xContent = XContentHelper.toXContent(document, XContentType.JSON, false);
        final String expected = stripWhitespace(
            String.format(
                Locale.ROOT,
                "{"
                    + "  \"cluster_uuid\": \"_cluster\","
                    + "  \"timestamp\": \"2017-08-09T08:18:59.402Z\","
                    + "  \"interval_ms\": 1506593717631,"
                    + "  \"type\": \"index_stats\","
                    + "  \"source_node\": {"
                    + "    \"uuid\": \"_uuid\","
                    + "    \"host\": \"_host\","
                    + "    \"transport_address\": \"_addr\","
                    + "    \"ip\": \"_ip\","
                    + "    \"name\": \"_name\","
                    + "    \"timestamp\": \"2017-08-31T08:46:30.855Z\""
                    + "  },"
                    + "  \"index_stats\": %s"
                    + "}",
                indexStatsSummary()
            )
        );
        assertEquals(expected, xContent.utf8ToString());
    }

    private String indexStatsSummary() throws IOException {
        final XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .field("index", index.getName())
            .field("uuid", index.getUUID())
            .field("created", metadata.getCreationDate())
            .field("status", indexHealth.getStatus().name().toLowerCase(Locale.ROOT));
        {
            builder.startObject("shards")
                .field("total", total)
                .field("primaries", primaries)
                .field("replicas", replicas)
                .field("active_total", activePrimaries + activeReplicas)
                .field("active_primaries", activePrimaries)
                .field("active_replicas", activeReplicas)
                .field("unassigned_total", total - (activePrimaries + activeReplicas))
                .field("unassigned_primaries", primaries - activePrimaries)
                .field("unassigned_replicas", total - (activePrimaries + activeReplicas) - (primaries - activePrimaries))
                .field("initializing", initializing)
                .field("relocating", relocating)
                .endObject();
        }

        builder.endObject();

        return convertToJson(BytesReference.bytes(builder), false, XContentType.JSON);
    }

    private static CommonStats mockCommonStats() {
        // This value is used in constructors of various stats objects,
        // when the value is not printed out in the final XContent.
        final long no = -1;

        // This value is used in constructors of various stats objects,
        // when the value is printed out in the XContent. Must be
        // incremented for each usage.
        long iota = 0L;

        final CommonStats commonStats = new CommonStats(CommonStatsFlags.ALL);
        commonStats.getDocs().add(new DocsStats(++iota, no, randomNonNegativeLong()));
        commonStats.getFieldData().add(new FieldDataStats(++iota, ++iota, null));
        commonStats.getMerge().add(no, no, no, ++iota, no, no, no, no, no, no);
        commonStats.getQueryCache().add(new QueryCacheStats(++iota, ++iota, ++iota, ++iota, no));
        commonStats.getRequestCache().add(new RequestCacheStats(++iota, ++iota, ++iota, ++iota));
        commonStats.getStore().add(new StoreStats(++iota, no, no));
        commonStats.getRefresh().add(new RefreshStats(no, ++iota, no, ++iota, (int) no));

        final IndexingStats.Stats indexingStats = new IndexingStats.Stats(++iota, ++iota, no, no, no, no, no, no, false, ++iota);
        commonStats.getIndexing().add(new IndexingStats(indexingStats));

        final SearchStats.Stats searchStats = new SearchStats.Stats(++iota, ++iota, no, no, no, no, no, no, no, no, no, no);
        commonStats.getSearch().add(new SearchStats(searchStats, no, null));

        final SegmentsStats segmentsStats = new SegmentsStats();
        segmentsStats.add(++iota);
        segmentsStats.addIndexWriterMemoryInBytes(++iota);
        segmentsStats.addVersionMapMemoryInBytes(++iota);
        segmentsStats.addBitsetMemoryInBytes(++iota);
        commonStats.getSegments().add(segmentsStats);

        return commonStats;
    }

    private static IndexMetadata mockIndexMetadata(final Index index,
                                                   final int primaries, final int replicas) {
        final Settings.Builder settings = Settings.builder();

        settings.put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID());
        settings.put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, primaries);
        settings.put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, replicas);
        settings.put(IndexMetadata.SETTING_VERSION_CREATED, MonitoringTemplateUtils.LAST_UPDATED_VERSION);
        settings.put(IndexMetadata.SETTING_CREATION_DATE, (new Date()).getTime());

        return IndexMetadata.builder(index.getName()).settings(settings).build();
    }

    private static IndexRoutingTable mockIndexRoutingTable(final Index index,
                                                           final int primaries, final int replicas,
                                                           final int activePrimaries, final int activeReplicas,
                                                           final int initializing, final int relocating) {
        final int total = primaries + (primaries * replicas);
        int unassignedTotal = total - (activePrimaries + activeReplicas);
        int unassignedPrimaries = primaries - activePrimaries;
        int unassignedReplicas = unassignedTotal - unassignedPrimaries;
        int activePrimariesRemaining = activePrimaries;
        int activeReplicasRemaining = activeReplicas;
        int initializingTotal = initializing; // we count initializing as a special type of unassigned!
        int relocatingTotal = relocating;

        assertThat("more initializing shards than unassigned", unassignedTotal, greaterThanOrEqualTo(initializingTotal));
        // we only relocate primaries to simplify this method -- replicas can be relocating
        assertThat("more relocating shards than active primaries", activePrimaries, greaterThanOrEqualTo(relocatingTotal));

        final IndexRoutingTable.Builder builder = IndexRoutingTable.builder(index);

        for (int i = 0; i < primaries; ++i) {
            final ShardId shardId = new ShardId(index, i);
            final IndexShardRoutingTable.Builder shard = new IndexShardRoutingTable.Builder(shardId);
            final int primariesLeft = primaries - i - 1;

            // randomly mark unassigned shards
            if (activePrimariesRemaining == 0 || (activePrimariesRemaining < primariesLeft && randomBoolean())) {
                --unassignedTotal;
                --unassignedPrimaries;

                final UnassignedInfo unassignedInfo = randomUnassignedInfo(randomAlphaOfLength(3));
                final String nodeId;
                final ShardRoutingState state;

                if (initializingTotal > 0) {
                    --initializingTotal;

                    nodeId = "abc";
                    state = ShardRoutingState.INITIALIZING;
                } else {
                    nodeId = null;
                    state = ShardRoutingState.UNASSIGNED;
                }

                shard.addShard(TestShardRouting.newShardRouting(shardId, nodeId, null, true, state, unassignedInfo));

                // mark all as unassigned
                for (int j = 0; j < replicas; ++j) {
                    --unassignedTotal;
                    --unassignedReplicas;

                    shard.addShard(TestShardRouting.newShardRouting(shardId, null, false, ShardRoutingState.UNASSIGNED));
                }
            // primary should be allocated, but replicas can still be unassigned
            } else {
                --activePrimariesRemaining;

                final String relocatingNodeId;
                final ShardRoutingState state;

                if (relocatingTotal > activePrimariesRemaining || (relocatingTotal > 0 && randomBoolean())) {
                    --relocatingTotal;

                    relocatingNodeId = "def";
                    state = ShardRoutingState.RELOCATING;
                } else {
                    relocatingNodeId = null;
                    state = ShardRoutingState.STARTED;
                }

                // Primary shard is STARTED (active)
                shard.addShard(TestShardRouting.newShardRouting(shardId, "abc", relocatingNodeId, true, state));

                for (int j = 0; j < replicas; ++j) {
                    final int replicasForActivePrimariesLeft = replicas - j - 1 + activePrimariesRemaining * replicas;

                    if (activeReplicasRemaining == 0 || (activeReplicasRemaining < replicasForActivePrimariesLeft && randomBoolean())) {
                        --unassignedTotal;
                        --unassignedReplicas;

                        final String replicaNodeId;
                        final ShardRoutingState replicaState;

                        // first case means that we MUST assign it because it's this unassigned shard
                        if (initializingTotal > 0) {
                            --initializingTotal;

                            replicaNodeId = "abc" + j;
                            replicaState = ShardRoutingState.INITIALIZING;
                        } else {
                            replicaNodeId = null;
                            replicaState = ShardRoutingState.UNASSIGNED;
                        }

                        shard.addShard(TestShardRouting.newShardRouting(shardId, replicaNodeId, false, replicaState));
                    } else {
                        --activeReplicasRemaining;

                        // Replica shard is STARTED (active)
                        shard.addShard(TestShardRouting.newShardRouting(shardId, "abc" + j, false, ShardRoutingState.STARTED));
                    }
                }
            }

            builder.addIndexShard(shard.build());
        }

        // sanity checks
        assertThat("unassigned shards miscounted", unassignedTotal, is(0));
        assertThat("unassigned primary shards miscounted", unassignedPrimaries, is(0));
        assertThat("unassigned replica shards miscounted", unassignedReplicas, is(0));
        assertThat("initializing shards miscounted", initializingTotal, is(0));
        assertThat("relocating shards miscounted", relocatingTotal, is(0));
        assertThat("active primaries miscounted", activePrimariesRemaining, is(0));
        assertThat("active replicas miscounted", activeReplicasRemaining, is(0));

        return builder.build();
    }

}
