/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.autoscaling.search;

import co.elastic.elasticsearch.stateless.autoscaling.MetricQuality;
import co.elastic.elasticsearch.stateless.autoscaling.memory.MemoryMetrics;
import co.elastic.elasticsearch.stateless.autoscaling.memory.MemoryMetricsService;
import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;
import co.elastic.elasticsearch.stateless.lucene.stats.ShardSize;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Strings;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLogAppender;
import org.junit.Before;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.cluster.routing.TestShardRouting.newShardRouting;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SearchMetricsServiceTests extends ESTestCase {

    private static final MemoryMetrics FIXED_MEMORY_METRICS = new MemoryMetrics(4096, 8192, MetricQuality.EXACT);
    private static final PrimaryTermAndGeneration ZERO = PrimaryTermAndGeneration.ZERO;

    private AtomicLong currentRelativeTimeInNanos;
    private SearchMetricsService service;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        currentRelativeTimeInNanos = new AtomicLong(1L);
        MemoryMetricsService memoryMetricsService = mock(MemoryMetricsService.class);
        when(memoryMetricsService.getMemoryMetrics()).thenReturn(FIXED_MEMORY_METRICS);
        service = new SearchMetricsService(createClusterSettings(), currentRelativeTimeInNanos::get, memoryMetricsService);
    }

    public void testExposesCompleteMetrics() {

        var indexMetadata = createIndex(1, 1);
        var state = ClusterState.builder(ClusterState.EMPTY_STATE)
            .nodes(createNodes(1))
            .metadata(Metadata.builder().put(indexMetadata, false))
            .build();

        service.clusterChanged(new ClusterChangedEvent("test", state, ClusterState.EMPTY_STATE));
        service.processShardSizesRequest(
            new PublishShardSizesRequest("search_node_1", Map.of(new ShardId(indexMetadata.getIndex(), 0), new ShardSize(1024, 1024, ZERO)))
        );

        assertThat(
            service.getSearchTierMetrics(),
            equalTo(
                new SearchTierMetrics(
                    FIXED_MEMORY_METRICS,
                    new MaxShardCopies(1, MetricQuality.EXACT),
                    new StorageMetrics(1024, 1024, 2048, MetricQuality.EXACT)
                )
            )
        );
    }

    public void testHandlesOutOfOrderMessages() {

        var indexMetadata = createIndex(1, 1);
        var state = ClusterState.builder(ClusterState.EMPTY_STATE)
            .nodes(createNodes(2))
            .metadata(Metadata.builder().put(indexMetadata, false))
            .build();

        service.clusterChanged(new ClusterChangedEvent("test", state, ClusterState.EMPTY_STATE));
        service.processShardSizesRequest(
            new PublishShardSizesRequest(
                "search_node_1",
                Map.of(new ShardId(indexMetadata.getIndex(), 0), new ShardSize(1024, 1024, new PrimaryTermAndGeneration(1, 2)))
            )
        );
        service.processShardSizesRequest(
            new PublishShardSizesRequest("search_node_1", Map.of(new ShardId(indexMetadata.getIndex(), 0), new ShardSize(512, 512, ZERO)))
        );

        // sticks to the first received metric
        assertThat(
            service.getSearchTierMetrics(),
            equalTo(
                new SearchTierMetrics(
                    FIXED_MEMORY_METRICS,
                    new MaxShardCopies(1, MetricQuality.EXACT),
                    new StorageMetrics(1024, 1024, 2048, MetricQuality.EXACT)
                )
            )
        );
    }

    public void testHandlesMetricsFromMultipleReplicas() {

        var indexMetadata = createIndex(1, 2);
        var state = ClusterState.builder(ClusterState.EMPTY_STATE)
            .nodes(createNodes(2))
            .metadata(Metadata.builder().put(indexMetadata, false))
            .build();

        service.clusterChanged(new ClusterChangedEvent("test", state, ClusterState.EMPTY_STATE));
        sendInRandomOrder(
            service,
            new PublishShardSizesRequest(
                "search_node_1",
                Map.of(new ShardId(indexMetadata.getIndex(), 0), new ShardSize(1024, 1024, ZERO))
            ),
            new PublishShardSizesRequest("search_node_2", Map.of(new ShardId(indexMetadata.getIndex(), 0), new ShardSize(1025, 1025, ZERO)))
        );

        // any of the replica sizes should be accepted
        var metrics = service.getSearchTierMetrics();
        assertThat(metrics.getMaxShardCopies(), equalTo(new MaxShardCopies(2, MetricQuality.EXACT)));
        assertThat(
            metrics.getStorageMetrics(),
            anyOf(
                equalTo(new StorageMetrics(1024, 1024, 2048, MetricQuality.EXACT)),
                equalTo(new StorageMetrics(1025, 1025, 2050, MetricQuality.EXACT))
            )
        );
    }

    public void testHandlesReorderedMetricsForDifferentShards() {

        var indexMetadata1 = createIndex(1, 1);
        var indexMetadata2 = createIndex(1, 1);
        var state = ClusterState.builder(ClusterState.EMPTY_STATE)
            .nodes(createNodes(2))
            .metadata(Metadata.builder().put(indexMetadata1, false).put(indexMetadata2, false))
            .build();

        service.clusterChanged(new ClusterChangedEvent("test", state, ClusterState.EMPTY_STATE));
        sendInRandomOrder(
            service,
            new PublishShardSizesRequest(
                "search_node_1",
                Map.of(new ShardId(indexMetadata1.getIndex(), 0), new ShardSize(1024, 1024, new PrimaryTermAndGeneration(1L, 1L)))
            ),
            new PublishShardSizesRequest(
                "search_node_1",
                Map.of(new ShardId(indexMetadata2.getIndex(), 0), new ShardSize(512, 512, new PrimaryTermAndGeneration(1L, 2L)))
            )
        );

        // both messages should be accepted
        assertThat(
            service.getSearchTierMetrics(),
            equalTo(
                new SearchTierMetrics(
                    FIXED_MEMORY_METRICS,
                    new MaxShardCopies(1, MetricQuality.EXACT),
                    new StorageMetrics(1024, 1536, 3072, MetricQuality.EXACT)
                )
            )
        );
    }

    public void testDiscardsOutdatedMetric() {

        var indexMetadata = createIndex(1, 1);
        var state = ClusterState.builder(ClusterState.EMPTY_STATE)
            .nodes(createNodes(2))
            .metadata(Metadata.builder().put(indexMetadata, false))
            .build();

        service.clusterChanged(new ClusterChangedEvent("test", state, ClusterState.EMPTY_STATE));
        sendInRandomOrder(
            service,
            new PublishShardSizesRequest(
                "search_node_1",
                Map.of(new ShardId(indexMetadata.getIndex(), 0), new ShardSize(512, 512, new PrimaryTermAndGeneration(1L, 1L)))
            ),
            new PublishShardSizesRequest(
                "search_node_1",
                Map.of(new ShardId(indexMetadata.getIndex(), 0), new ShardSize(1024, 1024, new PrimaryTermAndGeneration(1L, 2L)))
            )
        );

        // only newer metric should be accepted
        assertThat(
            service.getSearchTierMetrics(),
            equalTo(
                new SearchTierMetrics(
                    FIXED_MEMORY_METRICS,
                    new MaxShardCopies(1, MetricQuality.EXACT),
                    new StorageMetrics(1024, 1024, 2048, MetricQuality.EXACT)
                )
            )
        );
    }

    public void testMetricBecomesNotExactWhenOutdated() {

        var indexMetadata = createIndex(1, 1);
        var state = ClusterState.builder(ClusterState.EMPTY_STATE)
            .nodes(createNodes(1))
            .metadata(Metadata.builder().put(indexMetadata, false))
            .build();

        service.clusterChanged(new ClusterChangedEvent("test", state, ClusterState.EMPTY_STATE));
        service.processShardSizesRequest(
            new PublishShardSizesRequest("search_node_1", Map.of(new ShardId(indexMetadata.getIndex(), 0), new ShardSize(1024, 1024, ZERO)))
        );

        // after metric becomes outdated
        currentRelativeTimeInNanos.addAndGet(SearchMetricsService.ACCURATE_METRICS_WINDOW_SETTING.get(Settings.EMPTY).nanos() + 1);
        assertThat(
            service.getSearchTierMetrics(),
            equalTo(
                new SearchTierMetrics(
                    FIXED_MEMORY_METRICS,
                    new MaxShardCopies(1, MetricQuality.EXACT),
                    new StorageMetrics(1024, 1024, 2048, MetricQuality.MINIMUM)
                )
            )
        );

        // metrics become exact again when receiving empty ping from the node
        service.processShardSizesRequest(new PublishShardSizesRequest("search_node_1", Map.of()));
        assertThat(
            service.getSearchTierMetrics(),
            equalTo(
                new SearchTierMetrics(
                    FIXED_MEMORY_METRICS,
                    new MaxShardCopies(1, MetricQuality.EXACT),
                    new StorageMetrics(1024, 1024, 2048, MetricQuality.EXACT)
                )
            )
        );
    }

    public void testReportStaleShardMetric() {
        int numShards = randomIntBetween(2, 5);
        var indexMetadata = createIndex(numShards, 1);
        var state = ClusterState.builder(ClusterState.EMPTY_STATE)
            .nodes(createNodes(1))
            .metadata(Metadata.builder().put(indexMetadata, false))
            .build();

        service.clusterChanged(new ClusterChangedEvent("test", state, ClusterState.EMPTY_STATE));
        for (int i = 0; i < numShards; i++) {
            service.processShardSizesRequest(
                new PublishShardSizesRequest(
                    "search_node_1",
                    Map.of(new ShardId(indexMetadata.getIndex(), i), new ShardSize(randomNonNegativeInt(), randomNonNegativeInt(), ZERO))
                )
            );
        }
        currentRelativeTimeInNanos.addAndGet(SearchMetricsService.ACCURATE_METRICS_WINDOW_SETTING.get(Settings.EMPTY).nanos() + 1);
        currentRelativeTimeInNanos.addAndGet(SearchMetricsService.STALE_METRICS_CHECK_INTERVAL_SETTING.get(Settings.EMPTY).nanos() + 1);

        var memoryMetricsServiceLogger = LogManager.getLogger(SearchMetricsService.class);
        var mockLogAppender = new MockLogAppender();
        mockLogAppender.start();
        Loggers.addAppender(memoryMetricsServiceLogger, mockLogAppender);
        try {
            // Verify that all the shards are reported as stale
            for (int i = 0; i < numShards; i++) {
                mockLogAppender.addExpectation(
                    new MockLogAppender.SeenEventExpectation(
                        "expected warn log about stale storage metrics",
                        SearchMetricsService.class.getName(),
                        Level.WARN,
                        Strings.format(
                            "Storage metrics are stale for shard: %s, ShardMetrics{timestamp=1, shardSize=[interactive_in_bytes=*, "
                                + "non-interactive_in_bytes=*][primary term=0, generation=0]}",
                            new ShardId(indexMetadata.getIndex(), i)
                        )
                    )
                );
            }
            service.getSearchTierMetrics();
            mockLogAppender.assertAllExpectationsMatched();

            // Duplicate call doesn't cause new log warnings
            mockLogAppender.addExpectation(
                new MockLogAppender.UnseenEventExpectation("no warnings", SearchMetricsService.class.getName(), Level.WARN, "*")
            );
            service.getSearchTierMetrics();
            mockLogAppender.assertAllExpectationsMatched();

            // Refresh shard metrics, make sure there are no warning anymore
            mockLogAppender.addExpectation(
                new MockLogAppender.UnseenEventExpectation("no warnings", SearchMetricsService.class.getName(), Level.WARN, "*")
            );
            service.processShardSizesRequest(new PublishShardSizesRequest("search_node_1", Map.of()));
            service.getSearchTierMetrics();
            mockLogAppender.assertAllExpectationsMatched();
        } finally {
            Loggers.removeAppender(memoryMetricsServiceLogger, mockLogAppender);
            mockLogAppender.stop();
        }
    }

    public void testMetricsAreExactWithEmptyClusterState() {

        var state = ClusterState.builder(ClusterState.EMPTY_STATE).nodes(createNodes(1)).build();
        service.clusterChanged(new ClusterChangedEvent("test", state, ClusterState.EMPTY_STATE));

        assertThat(
            service.getSearchTierMetrics(),
            equalTo(
                new SearchTierMetrics(
                    FIXED_MEMORY_METRICS,
                    new MaxShardCopies(0, MetricQuality.EXACT),
                    new StorageMetrics(0, 0, 0, MetricQuality.EXACT)
                )
            )
        );
    }

    public void testMetricsAreNotExactWhenThereIsMetadataReadBlock() {

        var state = ClusterState.builder(ClusterState.EMPTY_STATE)
            .nodes(createNodes(1))
            .blocks(ClusterBlocks.builder().addGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK))
            .build();
        service.clusterChanged(new ClusterChangedEvent("test", state, ClusterState.EMPTY_STATE));

        assertThat(
            service.getSearchTierMetrics(),
            equalTo(
                new SearchTierMetrics(
                    FIXED_MEMORY_METRICS,
                    new MaxShardCopies(0, MetricQuality.MINIMUM),
                    new StorageMetrics(0, 0, 0, MetricQuality.MINIMUM)
                )
            )
        );
    }

    public void testMetricsAreNotExactRightAfterMasterElection() {
        // no cluster state updates yet

        assertThat(
            service.getSearchTierMetrics(),
            equalTo(
                new SearchTierMetrics(
                    FIXED_MEMORY_METRICS,
                    new MaxShardCopies(0, MetricQuality.MINIMUM),
                    new StorageMetrics(0, 0, 0, MetricQuality.MINIMUM)
                )
            )
        );
    }

    public void testInitialValueIsNotExact() {
        var indexMetadata = createIndex(1, 1);
        var state = ClusterState.builder(ClusterState.EMPTY_STATE)
            .nodes(createNodes(1))
            .metadata(Metadata.builder().put(indexMetadata, false))
            .build();
        service.clusterChanged(new ClusterChangedEvent("test", state, ClusterState.EMPTY_STATE));

        assertThat(
            service.getSearchTierMetrics(),
            equalTo(
                new SearchTierMetrics(
                    FIXED_MEMORY_METRICS,
                    new MaxShardCopies(1, MetricQuality.EXACT),
                    new StorageMetrics(0, 0, 0, MetricQuality.MINIMUM)
                )
            )
        );
    }

    public void testMetricsAreExactAfterCreatingNewIndex() {

        var state1 = ClusterState.builder(ClusterState.EMPTY_STATE).nodes(createNodes(1)).build();
        service.clusterChanged(new ClusterChangedEvent("test", state1, ClusterState.EMPTY_STATE));
        var indexMetadata = createIndex(1, 1);
        var state2 = ClusterState.builder(state1).metadata(Metadata.builder().put(indexMetadata, false)).build();
        service.clusterChanged(new ClusterChangedEvent("test", state2, state1));
        // index-1 is just created no size metrics received yet

        assertThat(
            service.getSearchTierMetrics(),
            equalTo(
                new SearchTierMetrics(
                    FIXED_MEMORY_METRICS,
                    new MaxShardCopies(1, MetricQuality.EXACT),
                    new StorageMetrics(0, 0, 0, MetricQuality.EXACT)
                )
            )
        );
    }

    public void testIsNotCompleteWithMissingShardSizes() {

        var indexMetadata1 = createIndex(1, 1);
        var indexMetadata2 = createIndex(1, 1);
        var state = ClusterState.builder(ClusterState.EMPTY_STATE)
            .nodes(createNodes(1))
            .metadata(Metadata.builder().put(indexMetadata1, false).put(indexMetadata2, false))
            .build();

        service.clusterChanged(new ClusterChangedEvent("test", state, ClusterState.EMPTY_STATE));
        service.processShardSizesRequest(
            new PublishShardSizesRequest(
                "search_node_1",
                Map.of(new ShardId(indexMetadata1.getIndex(), 0), new ShardSize(1024, 1024, ZERO))
            )
        );
        // index-2 stats are missing

        assertThat(
            service.getSearchTierMetrics(),
            equalTo(
                new SearchTierMetrics(
                    FIXED_MEMORY_METRICS,
                    new MaxShardCopies(1, MetricQuality.EXACT),
                    new StorageMetrics(1024, 1024, 2048, MetricQuality.MINIMUM)
                )
            )
        );
    }

    public void testWithAutoExpandIndex() {

        var indexMetadata = IndexMetadata.builder(randomIdentifier())
            .settings(indexSettings(1, 5).put("index.auto_expand_replicas", "1-all").put("index.version.created", Version.CURRENT))
            .build();
        var state = ClusterState.builder(ClusterState.EMPTY_STATE)
            .nodes(createNodes(1))
            .metadata(Metadata.builder().put(indexMetadata, false))
            .build();
        service.clusterChanged(new ClusterChangedEvent("test", state, ClusterState.EMPTY_STATE));
        service.processShardSizesRequest(
            new PublishShardSizesRequest("search_node_1", Map.of(new ShardId(indexMetadata.getIndex(), 0), new ShardSize(1024, 1024, ZERO)))
        );

        // should use min replicas when computing disk sizes
        assertThat(
            service.getSearchTierMetrics(),
            equalTo(
                new SearchTierMetrics(
                    FIXED_MEMORY_METRICS,
                    new MaxShardCopies(1, MetricQuality.EXACT),
                    new StorageMetrics(1024, 1024, 2048, MetricQuality.EXACT)
                )
            )
        );
    }

    public void testChangeReplicaCount() {

        var indexMetadata = createIndex(1, 1);
        var state = ClusterState.builder(ClusterState.EMPTY_STATE)
            .nodes(createNodes(1))
            .metadata(Metadata.builder().put(indexMetadata, false))
            .build();

        service.clusterChanged(new ClusterChangedEvent("test", state, ClusterState.EMPTY_STATE));
        service.processShardSizesRequest(
            new PublishShardSizesRequest("search_node_1", Map.of(new ShardId(indexMetadata.getIndex(), 0), new ShardSize(1024, 1024, ZERO)))
        );

        StorageMetrics storageMetrics = new StorageMetrics(1024, 1024, 2048, MetricQuality.EXACT);

        assertThat(
            service.getSearchTierMetrics(),
            equalTo(new SearchTierMetrics(FIXED_MEMORY_METRICS, new MaxShardCopies(1, MetricQuality.EXACT), storageMetrics))
        );

        indexMetadata = IndexMetadata.builder(indexMetadata)
            .settings(indexSettings(1, 2).put("index.version.created", Version.CURRENT))
            .build();
        var newState = ClusterState.builder(state).metadata(Metadata.builder().put(indexMetadata, false)).build();
        service.clusterChanged(new ClusterChangedEvent("test", newState, state));

        assertThat(
            service.getSearchTierMetrics(),
            equalTo(new SearchTierMetrics(FIXED_MEMORY_METRICS, new MaxShardCopies(2, MetricQuality.EXACT), storageMetrics))
        );
    }

    public void testRelocateShardDoesNotAffectMetrics() {

        var indexMetadata = createIndex(1, 1);
        var index = indexMetadata.getIndex();
        var shardId = new ShardId(index, 0);

        var state1 = ClusterState.builder(ClusterState.EMPTY_STATE)
            .nodes(createNodes(2))
            .metadata(Metadata.builder().put(indexMetadata, false))
            .routingTable(
                RoutingTable.builder()
                    .add(
                        IndexRoutingTable.builder(index)
                            .addShard(newShardRouting(shardId, "index_node_1", true, ShardRoutingState.STARTED))
                            .addShard(newShardRouting(shardId, "search_node_1", false, ShardRoutingState.STARTED))
                    )
            )
            .build();

        service.clusterChanged(new ClusterChangedEvent("test", state1, ClusterState.EMPTY_STATE));
        service.processShardSizesRequest(
            new PublishShardSizesRequest("search_node_1", Map.of(new ShardId(indexMetadata.getIndex(), 0), new ShardSize(1024, 1024, ZERO)))
        );

        var expectedSearchTierMetrics = new SearchTierMetrics(
            FIXED_MEMORY_METRICS,
            new MaxShardCopies(1, MetricQuality.EXACT),
            new StorageMetrics(1024, 1024, 2048, MetricQuality.EXACT)
        );
        assertThat(service.getSearchTierMetrics(), equalTo(expectedSearchTierMetrics));

        var state2 = ClusterState.builder(state1)
            .routingTable(
                RoutingTable.builder()
                    .add(
                        IndexRoutingTable.builder(index)
                            .addShard(newShardRouting(shardId, "index_node_1", true, ShardRoutingState.STARTED))
                            .addShard(newShardRouting(shardId, "search_node_1", "search_node_2", false, ShardRoutingState.RELOCATING))
                    )
            )
            .build();
        service.clusterChanged(new ClusterChangedEvent("test", state2, state1));
        assertThat(service.getSearchTierMetrics(), equalTo(expectedSearchTierMetrics));

        var state3 = ClusterState.builder(state2)
            .routingTable(
                RoutingTable.builder()
                    .add(
                        IndexRoutingTable.builder(index)
                            .addShard(newShardRouting(shardId, "index_node_1", true, ShardRoutingState.STARTED))
                            .addShard(newShardRouting(shardId, "search_node_2", false, ShardRoutingState.STARTED))
                    )
            )
            .build();
        service.clusterChanged(new ClusterChangedEvent("test", state3, state2));
        assertThat(service.getSearchTierMetrics(), equalTo(expectedSearchTierMetrics));
    }

    public void testShouldNotCountDeletedIndices() {

        var indexMetadata = createIndex(1, 1);
        var state = ClusterState.builder(ClusterState.EMPTY_STATE)
            .nodes(createNodes(1))
            .metadata(Metadata.builder().put(indexMetadata, false))
            .build();

        service.clusterChanged(new ClusterChangedEvent("test", state, ClusterState.EMPTY_STATE));
        service.processShardSizesRequest(
            new PublishShardSizesRequest("search_node_1", Map.of(new ShardId(indexMetadata.getIndex(), 0), new ShardSize(1024, 1024, ZERO)))
        );
        var newState = ClusterState.builder(state)
            .metadata(Metadata.builder(state.metadata()).remove(indexMetadata.getIndex().getName()))
            .build();
        service.clusterChanged(new ClusterChangedEvent("test", newState, state));

        assertThat(
            service.getSearchTierMetrics(),
            equalTo(
                new SearchTierMetrics(
                    FIXED_MEMORY_METRICS,
                    new MaxShardCopies(0, MetricQuality.EXACT),
                    new StorageMetrics(0, 0, 0, MetricQuality.EXACT)
                )
            )
        );
    }

    public void testDeletedNodesAreRemovedFromState() {

        var indexMetadata = createIndex(1, 2);
        var state1 = ClusterState.builder(ClusterState.EMPTY_STATE)
            .nodes(createNodes(2))
            .metadata(Metadata.builder().put(indexMetadata, false))
            .build();
        var state2 = ClusterState.builder(ClusterState.EMPTY_STATE)
            .nodes(createNodes(1))
            .metadata(Metadata.builder().put(indexMetadata, false))
            .build();

        sendInRandomOrder(
            service,
            new PublishShardSizesRequest(
                "search_node_1",
                Map.of(new ShardId(indexMetadata.getIndex(), 0), new ShardSize(1024, 1024, ZERO))
            ),
            new PublishShardSizesRequest("search_node_2", Map.of(new ShardId(indexMetadata.getIndex(), 0), new ShardSize(1024, 1024, ZERO)))
        );

        assertThat(service.getNodeMetrics(), allOf(aMapWithSize(2), hasKey("search_node_1"), hasKey("search_node_2")));

        service.clusterChanged(new ClusterChangedEvent("test", state2, state1));

        assertThat(service.getNodeMetrics(), allOf(aMapWithSize(1), hasKey("search_node_1"), not(hasKey("search_node_2"))));
    }

    public void testDeletedIndicesAreRemovedFromState() {

        var indexMetadata1 = createIndex(1, 2);
        var indexMetadata2 = createIndex(1, 2);
        var state1 = ClusterState.builder(ClusterState.EMPTY_STATE)
            .nodes(createNodes(1))
            .metadata(Metadata.builder().put(indexMetadata1, false).put(indexMetadata2, false))
            .build();
        var state2 = ClusterState.builder(ClusterState.EMPTY_STATE)
            .nodes(createNodes(1))
            .metadata(Metadata.builder().put(indexMetadata1, false))
            .build();

        sendInRandomOrder(
            service,
            new PublishShardSizesRequest(
                "search_node_1",
                Map.of(new ShardId(indexMetadata1.getIndex(), 0), new ShardSize(1024, 1024, new PrimaryTermAndGeneration(1, 1)))
            ),
            new PublishShardSizesRequest(
                "search_node_1",
                Map.of(new ShardId(indexMetadata1.getIndex(), 1), new ShardSize(1024, 1024, new PrimaryTermAndGeneration(1, 2)))
            ),
            new PublishShardSizesRequest(
                "search_node_1",
                Map.of(new ShardId(indexMetadata2.getIndex(), 0), new ShardSize(1024, 1024, new PrimaryTermAndGeneration(1, 3)))
            ),
            new PublishShardSizesRequest(
                "search_node_1",
                Map.of(new ShardId(indexMetadata2.getIndex(), 1), new ShardSize(1024, 1024, new PrimaryTermAndGeneration(1, 4)))
            )
        );

        service.clusterChanged(new ClusterChangedEvent("test", state1, ClusterState.EMPTY_STATE));

        assertThat(service.getIndices(), allOf(aMapWithSize(2), hasKey(indexMetadata1.getIndex()), hasKey(indexMetadata2.getIndex())));
        assertThat(
            service.getShardMetrics(),
            allOf(
                aMapWithSize(4),
                hasKey(new ShardId(indexMetadata1.getIndex(), 0)),
                hasKey(new ShardId(indexMetadata1.getIndex(), 1)),
                hasKey(new ShardId(indexMetadata2.getIndex(), 0)),
                hasKey(new ShardId(indexMetadata2.getIndex(), 1))
            )
        );

        service.clusterChanged(new ClusterChangedEvent("test", state2, state1));

        assertThat(service.getIndices(), allOf(aMapWithSize(1), hasKey(indexMetadata1.getIndex()), not(hasKey(indexMetadata2.getIndex()))));
        assertThat(
            service.getShardMetrics(),
            allOf(
                aMapWithSize(2),
                hasKey(new ShardId(indexMetadata1.getIndex(), 0)),
                hasKey(new ShardId(indexMetadata1.getIndex(), 1)),
                not(hasKey(new ShardId(indexMetadata2.getIndex(), 0))),
                not(hasKey(new ShardId(indexMetadata2.getIndex(), 1)))
            )
        );
    }

    private static DiscoveryNodes createNodes(int searchNodes) {
        var builder = DiscoveryNodes.builder();
        builder.masterNodeId("master").localNodeId("master");
        builder.add(DiscoveryNodeUtils.builder("master").roles(Set.of(DiscoveryNodeRole.MASTER_ROLE)).build());
        builder.add(DiscoveryNodeUtils.builder("index_node_1").roles(Set.of(DiscoveryNodeRole.INDEX_ROLE)).build());
        for (int i = 1; i <= searchNodes; i++) {
            builder.add(DiscoveryNodeUtils.builder("search_node_" + i).roles(Set.of(DiscoveryNodeRole.SEARCH_ROLE)).build());
        }
        return builder.build();
    }

    private static IndexMetadata createIndex(int shards, int replicas) {
        return IndexMetadata.builder(randomIdentifier())
            .settings(indexSettings(shards, replicas).put("index.version.created", Version.CURRENT))
            .build();
    }

    private static void sendInRandomOrder(SearchMetricsService service, PublishShardSizesRequest... requests) {
        shuffledList(List.of(requests)).forEach(service::processShardSizesRequest);
    }

    private static ClusterSettings createClusterSettings() {
        return new ClusterSettings(
            Settings.EMPTY,
            Sets.addToCopy(
                ClusterSettings.BUILT_IN_CLUSTER_SETTINGS,
                SearchMetricsService.ACCURATE_METRICS_WINDOW_SETTING,
                SearchMetricsService.STALE_METRICS_CHECK_INTERVAL_SETTING
            )
        );
    }
}
