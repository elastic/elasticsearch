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

import co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings;
import co.elastic.elasticsearch.stateless.autoscaling.MetricQuality;
import co.elastic.elasticsearch.stateless.autoscaling.memory.MemoryMetrics;
import co.elastic.elasticsearch.stateless.autoscaling.memory.MemoryMetricsService;
import co.elastic.elasticsearch.stateless.autoscaling.search.load.NodeSearchLoadSnapshot;
import co.elastic.elasticsearch.stateless.autoscaling.search.load.PublishNodeSearchLoadRequest;
import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;
import co.elastic.elasticsearch.stateless.lucene.stats.ShardSize;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
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
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLogAppender;
import org.hamcrest.Matchers;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static co.elastic.elasticsearch.stateless.autoscaling.search.SearchMetricsService.ACCURATE_METRICS_WINDOW_SETTING;
import static co.elastic.elasticsearch.stateless.autoscaling.search.SearchMetricsService.STALE_METRICS_CHECK_INTERVAL_SETTING;
import static org.elasticsearch.cluster.routing.TestShardRouting.newShardRouting;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SearchMetricsServiceTests extends ESTestCase {

    private static final MemoryMetrics FIXED_MEMORY_METRICS = new MemoryMetrics(4096, 8192, MetricQuality.EXACT);
    private static final PrimaryTermAndGeneration ZERO = PrimaryTermAndGeneration.ZERO;

    private AtomicLong currentRelativeTimeInNanos;
    private SearchMetricsService service;

    private MemoryMetricsService memoryMetricsService;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        currentRelativeTimeInNanos = new AtomicLong(1L);
        memoryMetricsService = mock(MemoryMetricsService.class);
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
        service.processSearchLoadRequest(new PublishNodeSearchLoadRequest("search_node_1", 1L, 1.0));
        service.processSearchLoadRequest(new PublishNodeSearchLoadRequest("search_node_2", 1L, 2.0));

        assertThat(
            service.getSearchTierMetrics(),
            equalTo(
                new SearchTierMetrics(
                    FIXED_MEMORY_METRICS,
                    new MaxShardCopies(1, MetricQuality.EXACT),
                    new StorageMetrics(1024, 1024, 2048, MetricQuality.EXACT),
                    List.of(
                        new NodeSearchLoadSnapshot("search_node_1", 1.0, MetricQuality.EXACT),
                        new NodeSearchLoadSnapshot("search_node_2", 2.0, MetricQuality.EXACT)
                    )
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
        service.processSearchLoadRequest(new PublishNodeSearchLoadRequest("search_node_1", 2L, 1.0));
        service.processSearchLoadRequest(new PublishNodeSearchLoadRequest("search_node_1", 1L, 5.0));
        service.processSearchLoadRequest(new PublishNodeSearchLoadRequest("search_node_2", 2L, 2.0));
        service.processSearchLoadRequest(new PublishNodeSearchLoadRequest("search_node_2", 1L, 5.0));

        // sticks to the first received metric
        assertThat(
            service.getSearchTierMetrics(),
            equalTo(
                new SearchTierMetrics(
                    FIXED_MEMORY_METRICS,
                    new MaxShardCopies(1, MetricQuality.EXACT),
                    new StorageMetrics(1024, 1024, 2048, MetricQuality.EXACT),
                    List.of(
                        new NodeSearchLoadSnapshot("search_node_1", 1.0, MetricQuality.EXACT),
                        new NodeSearchLoadSnapshot("search_node_2", 2.0, MetricQuality.EXACT)
                    )
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
        service.processSearchLoadRequest(new PublishNodeSearchLoadRequest("search_node_1", 1L, 1.0));
        service.processSearchLoadRequest(new PublishNodeSearchLoadRequest("search_node_2", 1L, 2.0));

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
        assertThat(
            metrics.getNodesLoad(),
            Matchers.containsInAnyOrder(
                new NodeSearchLoadSnapshot("search_node_1", 1.0, MetricQuality.EXACT),
                new NodeSearchLoadSnapshot("search_node_2", 2.0, MetricQuality.EXACT)
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
        sendInRandomOrder(
            service,
            new PublishNodeSearchLoadRequest("search_node_1", 2L, 1.0),
            new PublishNodeSearchLoadRequest("search_node_1", 1L, 5.0),
            new PublishNodeSearchLoadRequest("search_node_2", 2L, 2.0),
            new PublishNodeSearchLoadRequest("search_node_2", 1L, 5.0)
        );

        // both messages should be accepted
        assertThat(
            service.getSearchTierMetrics(),
            equalTo(
                new SearchTierMetrics(
                    FIXED_MEMORY_METRICS,
                    new MaxShardCopies(1, MetricQuality.EXACT),
                    new StorageMetrics(1024, 1536, 3072, MetricQuality.EXACT),
                    List.of(
                        new NodeSearchLoadSnapshot("search_node_1", 1.0, MetricQuality.EXACT),
                        new NodeSearchLoadSnapshot("search_node_2", 2.0, MetricQuality.EXACT)
                    )
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
        sendInRandomOrder(
            service,
            new PublishNodeSearchLoadRequest("search_node_1", 2L, 1.0),
            new PublishNodeSearchLoadRequest("search_node_1", 1L, 5.0),
            new PublishNodeSearchLoadRequest("search_node_2", 2L, 2.0),
            new PublishNodeSearchLoadRequest("search_node_2", 1L, 5.0)
        );

        // only newer metric should be accepted
        assertThat(
            service.getSearchTierMetrics(),
            equalTo(
                new SearchTierMetrics(
                    FIXED_MEMORY_METRICS,
                    new MaxShardCopies(1, MetricQuality.EXACT),
                    new StorageMetrics(1024, 1024, 2048, MetricQuality.EXACT),
                    List.of(
                        new NodeSearchLoadSnapshot("search_node_1", 1.0, MetricQuality.EXACT),
                        new NodeSearchLoadSnapshot("search_node_2", 2.0, MetricQuality.EXACT)
                    )
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
        service.processSearchLoadRequest(new PublishNodeSearchLoadRequest("search_node_1", 1L, 1.0));
        service.processSearchLoadRequest(new PublishNodeSearchLoadRequest("search_node_2", 1L, 2.0));

        // after metric becomes outdated
        currentRelativeTimeInNanos.addAndGet(ACCURATE_METRICS_WINDOW_SETTING.get(Settings.EMPTY).nanos() + 1);
        assertThat(
            service.getSearchTierMetrics(),
            equalTo(
                new SearchTierMetrics(
                    FIXED_MEMORY_METRICS,
                    new MaxShardCopies(1, MetricQuality.EXACT),
                    new StorageMetrics(1024, 1024, 2048, MetricQuality.MINIMUM),
                    List.of(
                        new NodeSearchLoadSnapshot("search_node_1", 1.0, MetricQuality.MINIMUM),
                        new NodeSearchLoadSnapshot("search_node_2", 2.0, MetricQuality.MINIMUM)
                    )
                )
            )
        );

        // metrics become exact again when receiving empty ping from the node
        service.processShardSizesRequest(new PublishShardSizesRequest("search_node_1", Map.of()));
        // Metrics become exact for the node that is updated.
        service.processSearchLoadRequest(new PublishNodeSearchLoadRequest("search_node_1", 2L, 5.0));
        assertThat(
            service.getSearchTierMetrics(),
            equalTo(
                new SearchTierMetrics(
                    FIXED_MEMORY_METRICS,
                    new MaxShardCopies(1, MetricQuality.EXACT),
                    new StorageMetrics(1024, 1024, 2048, MetricQuality.EXACT),
                    List.of(
                        new NodeSearchLoadSnapshot("search_node_1", 5.0, MetricQuality.EXACT),
                        new NodeSearchLoadSnapshot("search_node_2", 2.0, MetricQuality.MINIMUM)
                    )
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
        currentRelativeTimeInNanos.addAndGet(ACCURATE_METRICS_WINDOW_SETTING.get(Settings.EMPTY).nanos() + 1);
        currentRelativeTimeInNanos.addAndGet(STALE_METRICS_CHECK_INTERVAL_SETTING.get(Settings.EMPTY).nanos() + 1);

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
                    new StorageMetrics(0, 0, 0, MetricQuality.EXACT),
                    List.of(new NodeSearchLoadSnapshot("search_node_1", 0.0, MetricQuality.MISSING))
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
                    new StorageMetrics(0, 0, 0, MetricQuality.MINIMUM),
                    List.of()
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
                    new StorageMetrics(0, 0, 0, MetricQuality.MINIMUM),
                    List.of()
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
                    new StorageMetrics(0, 0, 0, MetricQuality.MINIMUM),
                    List.of(new NodeSearchLoadSnapshot("search_node_1", 0.0, MetricQuality.MISSING))
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
                    new StorageMetrics(0, 0, 0, MetricQuality.EXACT),
                    List.of(new NodeSearchLoadSnapshot("search_node_1", 0.0, MetricQuality.MISSING))
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
                    new StorageMetrics(1024, 1024, 2048, MetricQuality.MINIMUM),
                    List.of(new NodeSearchLoadSnapshot("search_node_1", 0.0, MetricQuality.MISSING))
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
                    new StorageMetrics(1024, 1024, 2048, MetricQuality.EXACT),
                    List.of(new NodeSearchLoadSnapshot("search_node_1", 0.0, MetricQuality.MISSING))
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
            equalTo(
                new SearchTierMetrics(
                    FIXED_MEMORY_METRICS,
                    new MaxShardCopies(1, MetricQuality.EXACT),
                    storageMetrics,
                    List.of(new NodeSearchLoadSnapshot("search_node_1", 0.0, MetricQuality.MISSING))
                )
            )
        );

        indexMetadata = IndexMetadata.builder(indexMetadata)
            .settings(indexSettings(1, 2).put("index.version.created", Version.CURRENT))
            .build();
        var newState = ClusterState.builder(state).metadata(Metadata.builder().put(indexMetadata, false)).build();
        service.clusterChanged(new ClusterChangedEvent("test", newState, state));

        assertThat(
            service.getSearchTierMetrics(),
            equalTo(
                new SearchTierMetrics(
                    FIXED_MEMORY_METRICS,
                    new MaxShardCopies(2, MetricQuality.EXACT),
                    storageMetrics,
                    List.of(new NodeSearchLoadSnapshot("search_node_1", 0.0, MetricQuality.MISSING))
                )
            )
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
            new StorageMetrics(1024, 1024, 2048, MetricQuality.EXACT),
            List.of(
                new NodeSearchLoadSnapshot("search_node_1", 0.0, MetricQuality.MISSING),
                new NodeSearchLoadSnapshot("search_node_2", 0.0, MetricQuality.MISSING)
            )
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
                    new StorageMetrics(0, 0, 0, MetricQuality.EXACT),
                    List.of(new NodeSearchLoadSnapshot("search_node_1", 0.0, MetricQuality.MISSING))
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

        assertThat(service.getNodeTimingForShardMetrics(), allOf(aMapWithSize(2), hasKey("search_node_1"), hasKey("search_node_2")));

        service.clusterChanged(new ClusterChangedEvent("test", state2, state1));

        assertThat(service.getNodeTimingForShardMetrics(), allOf(aMapWithSize(1), hasKey("search_node_1"), not(hasKey("search_node_2"))));
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

    public void testServiceOnlyReturnDataWhenLocalNodeIsElectedAsMaster() {
        var localNode = DiscoveryNodeUtils.create(UUIDs.randomBase64UUID());
        var remoteNode = DiscoveryNodeUtils.create(UUIDs.randomBase64UUID());
        var nodes = DiscoveryNodes.builder().add(localNode).add(remoteNode).localNodeId(localNode.getId()).build();
        var searchTierMetrics = service.getSearchTierMetrics();
        // If the node is not elected as master (i.e. we haven't got any cluster state notification) it shouldn't return any info
        assertThat(searchTierMetrics.getNodesLoad(), is(empty()));

        service.clusterChanged(
            new ClusterChangedEvent(
                "Local node not elected as master",
                clusterState(DiscoveryNodes.builder(nodes).masterNodeId(remoteNode.getId()).build()),
                clusterState(nodes)
            )
        );

        var searchTierMetricsAfterClusterStateEvent = service.getSearchTierMetrics();
        assertThat(searchTierMetricsAfterClusterStateEvent.getNodesLoad(), is(empty()));
    }

    public void testOnlySearchNodesAreTracked() {
        final var localNode = DiscoveryNodeUtils.builder(UUIDs.randomBase64UUID()).roles(Set.of(DiscoveryNodeRole.MASTER_ROLE)).build();

        final var nodes = DiscoveryNodes.builder()
            .add(localNode)
            .add(DiscoveryNodeUtils.builder(UUIDs.randomBase64UUID()).roles(Set.of(DiscoveryNodeRole.INDEX_ROLE)).build())
            .add(DiscoveryNodeUtils.builder(UUIDs.randomBase64UUID()).roles(Set.of(DiscoveryNodeRole.SEARCH_ROLE)).build())
            .localNodeId(localNode.getId())
            .build();

        service.clusterChanged(
            new ClusterChangedEvent(
                "Local node elected as master",
                clusterState(DiscoveryNodes.builder(nodes).masterNodeId(localNode.getId()).build()),
                clusterState(nodes)
            )
        );
        var searchTierMetrics = service.getSearchTierMetrics();
        var metricQualityCount = searchTierMetrics.getNodesLoad()
            .stream()
            .collect(Collectors.groupingBy(NodeSearchLoadSnapshot::metricQuality, Collectors.counting()));

        // When the node hasn't published a metric yet, we consider it as missing
        assertThat(searchTierMetrics.toString(), metricQualityCount.get(MetricQuality.MISSING), is(equalTo(1L)));
    }

    private static ClusterState clusterState(DiscoveryNodes nodes) {
        assert nodes != null;
        return ClusterState.builder(ClusterName.DEFAULT).nodes(nodes).build();
    }

    public void testSearchLoadIsKeptDuringNodeLifecycle() {
        final var masterNode = DiscoveryNodeUtils.builder(UUIDs.randomBase64UUID()).roles(Set.of(DiscoveryNodeRole.MASTER_ROLE)).build();
        final var searchNode = DiscoveryNodeUtils.create(UUIDs.randomBase64UUID());

        final var nodes = DiscoveryNodes.builder().add(masterNode).localNodeId(masterNode.getId()).build();

        final var nodesWithElectedMaster = DiscoveryNodes.builder(nodes).masterNodeId(masterNode.getId()).build();

        var fakeClock = new AtomicLong();

        var inaccurateMetricTime = TimeValue.timeValueSeconds(25);
        var staleLoadWindow = TimeValue.timeValueMinutes(10);
        var service = new SearchMetricsService(
            createClusterSettings(
                Settings.builder()
                    .put(ACCURATE_METRICS_WINDOW_SETTING.getKey(), inaccurateMetricTime)
                    .put(STALE_METRICS_CHECK_INTERVAL_SETTING.getKey(), staleLoadWindow)
                    .build()
            ),
            fakeClock::get,
            memoryMetricsService
        );

        service.clusterChanged(new ClusterChangedEvent("master node elected", clusterState(nodesWithElectedMaster), clusterState(nodes)));

        // Take into account the case where the search node sends the metric to the new master node before it applies the new cluster state
        if (randomBoolean()) {
            fakeClock.addAndGet(TimeValue.timeValueSeconds(1).nanos());
            service.processSearchLoadRequest(new PublishNodeSearchLoadRequest(searchNode.getId(), 1, 0.5));
        }

        var nodesWithSearchNode = DiscoveryNodes.builder(nodesWithElectedMaster).add(searchNode).build();

        service.clusterChanged(
            new ClusterChangedEvent("search node joins", clusterState(nodesWithSearchNode), clusterState(nodesWithElectedMaster))
        );

        fakeClock.addAndGet(TimeValue.timeValueSeconds(1).nanos());
        service.processSearchLoadRequest(new PublishNodeSearchLoadRequest(searchNode.getId(), 2, 1.5));

        var searchTierMetrics = service.getSearchTierMetrics();
        assertThat(searchTierMetrics.getNodesLoad(), hasSize(1));

        var searchNodeLoad = searchTierMetrics.getNodesLoad().get(0);
        assertThat(searchNodeLoad.load(), is(equalTo(1.5)));
        assertThat(searchTierMetrics.toString(), searchNodeLoad.metricQuality(), is(equalTo(MetricQuality.EXACT)));

        if (randomBoolean()) {
            service.clusterChanged(
                new ClusterChangedEvent("search node leaves", clusterState(nodesWithElectedMaster), clusterState(nodesWithSearchNode))
            );
        }

        fakeClock.addAndGet(inaccurateMetricTime.getNanos());

        var searchTierMetricsAfterNodeMetricIsInaccurate = service.getSearchTierMetrics();
        assertThat(searchTierMetricsAfterNodeMetricIsInaccurate.getNodesLoad(), hasSize(1));

        var searchNodeLoadAfterMissingMetrics = searchTierMetricsAfterNodeMetricIsInaccurate.getNodesLoad().get(0);
        assertThat(searchNodeLoadAfterMissingMetrics.load(), is(equalTo(1.5)));
        assertThat(searchNodeLoadAfterMissingMetrics.metricQuality(), is(equalTo(MetricQuality.MINIMUM)));

        // The node re-joins before the metric is considered to be inaccurate
        if (randomBoolean()) {
            service.clusterChanged(
                new ClusterChangedEvent("search node re-joins", clusterState(nodesWithSearchNode), clusterState(nodesWithElectedMaster))
            );
            fakeClock.addAndGet(TimeValue.timeValueSeconds(1).nanos());
            service.processSearchLoadRequest(new PublishNodeSearchLoadRequest(searchNode.getId(), 3, 0.5));

            var searchTierMetricsAfterNodeReJoins = service.getSearchTierMetrics();
            assertThat(searchTierMetricsAfterNodeReJoins.getNodesLoad(), hasSize(1));

            var searchNodeLoadAfterRejoining = searchTierMetricsAfterNodeReJoins.getNodesLoad().get(0);
            assertThat(searchNodeLoadAfterRejoining.load(), is(equalTo(0.5)));
            assertThat(searchNodeLoadAfterRejoining.metricQuality(), is(equalTo(MetricQuality.EXACT)));
        } else {
            // The node do not re-join after the max time
            fakeClock.addAndGet(staleLoadWindow.getNanos());

            var searchTierMetricsAfterTTLExpires = service.getSearchTierMetrics();
            assertThat(searchTierMetricsAfterTTLExpires.getNodesLoad(), hasSize(0));
        }
    }

    public void testOutOfOrderMetricsAreDiscarded() {
        final var masterNode = DiscoveryNodeUtils.builder(UUIDs.randomBase64UUID()).roles(Set.of(DiscoveryNodeRole.MASTER_ROLE)).build();
        final var searchNode = DiscoveryNodeUtils.create(UUIDs.randomBase64UUID());

        final var nodes = DiscoveryNodes.builder().add(masterNode).add(searchNode).localNodeId(masterNode.getId()).build();

        final var nodesWithElectedMaster = DiscoveryNodes.builder(nodes).masterNodeId(masterNode.getId()).build();

        var service = new SearchMetricsService(createClusterSettings(), () -> 0, memoryMetricsService);

        service.clusterChanged(new ClusterChangedEvent("master node elected", clusterState(nodesWithElectedMaster), clusterState(nodes)));

        var maxSeqNo = randomIntBetween(10, 20);
        var maxSeqNoSearchLoad = randomSearchLoad();
        service.processSearchLoadRequest(new PublishNodeSearchLoadRequest(searchNode.getId(), maxSeqNo, maxSeqNoSearchLoad));

        var numberOfOutOfOrderMetricSamples = randomIntBetween(1, maxSeqNo);
        var unorderedSeqNos = IntStream.of(numberOfOutOfOrderMetricSamples).boxed().collect(Collectors.toCollection(ArrayList::new));
        Collections.shuffle(unorderedSeqNos, random());
        for (long seqNo : unorderedSeqNos) {
            service.processSearchLoadRequest(new PublishNodeSearchLoadRequest(searchNode.getId(), maxSeqNo, maxSeqNoSearchLoad));
        }

        var searchTierMetrics = service.getSearchTierMetrics();
        assertThat(searchTierMetrics.getNodesLoad().toString(), searchTierMetrics.getNodesLoad(), hasSize(1));

        var searchNodeLoad = searchTierMetrics.getNodesLoad().get(0);
        assertThat(searchNodeLoad.load(), is(equalTo(maxSeqNoSearchLoad)));
        assertThat(searchNodeLoad.metricQuality(), is(equalTo(MetricQuality.EXACT)));
    }

    public void testServiceStopsReturningInfoAfterMasterTakeover() {
        final var localNode = DiscoveryNodeUtils.builder(UUIDs.randomBase64UUID()).roles(Set.of(DiscoveryNodeRole.MASTER_ROLE)).build();

        final var remoteNode = DiscoveryNodeUtils.builder(UUIDs.randomBase64UUID()).roles(Set.of(DiscoveryNodeRole.SEARCH_ROLE)).build();
        final var nodes = DiscoveryNodes.builder()
            .add(localNode)
            .add(remoteNode)
            .add(DiscoveryNodeUtils.builder(UUIDs.randomBase64UUID()).roles(Set.of(DiscoveryNodeRole.INDEX_ROLE)).build())
            .localNodeId(localNode.getId())
            .build();

        var service = new SearchMetricsService(createClusterSettings(), () -> 0, memoryMetricsService);

        service.clusterChanged(
            new ClusterChangedEvent(
                "Local node elected as master",
                clusterState(DiscoveryNodes.builder(nodes).masterNodeId(localNode.getId()).build()),
                clusterState(nodes)
            )
        );
        var searchTierMetrics = service.getSearchTierMetrics();
        var metricQualityCount = searchTierMetrics.getNodesLoad()
            .stream()
            .collect(Collectors.groupingBy(NodeSearchLoadSnapshot::metricQuality, Collectors.counting()));

        // When the node hasn't published a metric yet, we consider it as missing
        assertThat(searchTierMetrics.getNodesLoad().toString(), metricQualityCount.get(MetricQuality.MISSING), is(equalTo(1L)));

        service.clusterChanged(
            new ClusterChangedEvent(
                "Remote node elected as master",
                clusterState(DiscoveryNodes.builder(nodes).masterNodeId(remoteNode.getId()).build()),
                clusterState(DiscoveryNodes.builder(nodes).masterNodeId(localNode.getId()).build())
            )
        );

        var searchTierMetricsAfterMasterHandover = service.getSearchTierMetrics();
        assertThat(searchTierMetricsAfterMasterHandover.getNodesLoad(), is(empty()));
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

    private static void sendInRandomOrder(SearchMetricsService service, PublishNodeSearchLoadRequest... requests) {
        shuffledList(List.of(requests)).forEach(service::processSearchLoadRequest);
    }

    private static ClusterSettings createClusterSettings() {
        return new ClusterSettings(Settings.EMPTY, defaultClusterSettings());
    }

    private static ClusterSettings createClusterSettings(Settings settings) {
        return new ClusterSettings(settings, defaultClusterSettings());
    }

    private static Set<Setting<?>> defaultClusterSettings() {
        return Sets.addToCopy(
            ClusterSettings.BUILT_IN_CLUSTER_SETTINGS,
            SearchMetricsService.ACCURATE_METRICS_WINDOW_SETTING,
            SearchMetricsService.STALE_METRICS_CHECK_INTERVAL_SETTING,
            ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING,
            ServerlessSharedSettings.SEARCH_POWER_MAX_SETTING,
            ServerlessSharedSettings.SEARCH_POWER_SETTING
        );
    }

    private static double randomSearchLoad() {
        return randomDoubleBetween(0, 16, true);
    }
}
