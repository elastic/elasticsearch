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
import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;
import co.elastic.elasticsearch.stateless.lucene.stats.ShardSize;

import org.apache.lucene.util.ThreadInterruptedException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.indices.settings.put.TransportUpdateSettingsAction;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpNodeClient;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.newInstance;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ReplicasUpdaterTests extends ESTestCase {
    private SearchMetricsService searchMetricsService;
    private MockClient mockClient;
    private ReplicasUpdater replicasUpdater;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        AtomicLong currentRelativeTimeInNanos = new AtomicLong(1L);
        MemoryMetricsService memoryMetricsService = mock(MemoryMetricsService.class);
        when(memoryMetricsService.getMemoryMetrics()).thenReturn(new MemoryMetrics(4096, 8192, MetricQuality.EXACT));
        ClusterSettings clusterSettings = createClusterSettings();
        searchMetricsService = new SearchMetricsService(clusterSettings, currentRelativeTimeInNanos::get, memoryMetricsService);
        mockClient = new MockClient();
        replicasUpdater = new ReplicasUpdater(searchMetricsService, clusterSettings, mockClient);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        mockClient.assertNoUpdate();
    }

    private record SearchPowerSteps(int sp, IndexMetadata expectedAdditionalIndex) {}

    /**
     * Test that increases search power in steps and asserts that indices that
     * replica calculation for indices that originally have 1 replica is increased
     * to 2
     */
    public void testGetNumberOfReplicaChangesSP100To250() {
        int initialReplicas = 1;
        Metadata.Builder clusterMetadata = Metadata.builder();
        var index1 = createIndex(1, initialReplicas, clusterMetadata);
        var index2 = createIndex(1, initialReplicas, clusterMetadata);
        var systemIndex = createSystemIndex(1, initialReplicas, clusterMetadata);

        String dataStream1Name = "ds1";
        IndexMetadata ds1BackingIndex1 = createBackingIndex(dataStream1Name, 1, initialReplicas, 1714000000000L).build();
        clusterMetadata.put(ds1BackingIndex1, false);
        IndexMetadata ds1BackingIndex2 = createBackingIndex(dataStream1Name, 2, initialReplicas, 1715000000000L).build();
        clusterMetadata.put(ds1BackingIndex2, false);
        DataStream ds1 = newInstance(dataStream1Name, List.of(ds1BackingIndex1.getIndex(), ds1BackingIndex2.getIndex()));
        clusterMetadata.put(ds1);

        String dataStream2Name = "ds2";
        IndexMetadata ds2BackingIndex1 = createBackingIndex(dataStream2Name, 1, initialReplicas, 1713000000000L).build();
        clusterMetadata.put(ds2BackingIndex1, false);
        IndexMetadata ds2BackingIndex2 = createBackingIndex(dataStream2Name, 2, initialReplicas, 1714000000000L).build();
        clusterMetadata.put(ds2BackingIndex2, false);
        DataStream ds2 = newInstance(dataStream2Name, List.of(ds2BackingIndex1.getIndex(), ds2BackingIndex2.getIndex()));
        clusterMetadata.put(ds2);

        var state1 = ClusterState.builder(ClusterState.EMPTY_STATE).nodes(createNodes(1)).metadata(clusterMetadata).build();
        searchMetricsService.clusterChanged(new ClusterChangedEvent("test", state1, ClusterState.EMPTY_STATE));

        Map<ShardId, ShardSize> shards = new HashMap<>();
        addShard(shards, index1, 0, 2000, 0);
        addShard(shards, index2, 0, 1000, 0);
        addShard(shards, systemIndex, 0, 1000, 0);
        addShard(shards, ds1BackingIndex1, 0, 500, 500);
        addShard(shards, ds1BackingIndex2, 0, 750, 500);
        addShard(shards, ds2BackingIndex1, 0, 500, 500);
        addShard(shards, ds2BackingIndex2, 0, 250, 500);
        searchMetricsService.processShardSizesRequest(new PublishShardSizesRequest("search_node_1", shards));

        replicasUpdater.updateSearchPowerMin(100);
        mockClient.assertNoUpdate();

        List<SearchPowerSteps> steps = List.of(
            new SearchPowerSteps(125, systemIndex),
            new SearchPowerSteps(175, index1),
            new SearchPowerSteps(200, index2),
            new SearchPowerSteps(220, ds1BackingIndex2),
            new SearchPowerSteps(225, ds2BackingIndex2),
            new SearchPowerSteps(249, ds1BackingIndex1),
            new SearchPowerSteps(250, ds2BackingIndex1)
        );

        Set<String> indices = new HashSet<>();
        steps.forEach(step -> {
            replicasUpdater.updateSearchPowerMin(step.sp);
            indices.add(step.expectedAdditionalIndex.getIndex().getName());
            mockClient.assertUpdates(Map.of(2, indices));
        });

        // check that size changes affect the ranking.
        // We'll swap interactive size for index1 and index2
        // also swap ds1BackingIndex2 and ds2BackingIndex2 since they should use size as tie-breaker, their
        // recency is "now" because they are both write indices
        shards = new HashMap<>();
        addShard(shards, index1, 0, 1000, 0);
        addShard(shards, index2, 0, 2000, 0);
        addShard(shards, ds1BackingIndex2, 0, 250, 500);
        addShard(shards, ds2BackingIndex2, 0, 750, 500);
        searchMetricsService.processShardSizesRequest(new PublishShardSizesRequest("search_node_1", shards));

        steps = List.of(
            new SearchPowerSteps(125, systemIndex),
            new SearchPowerSteps(175, index2),
            new SearchPowerSteps(200, index1),
            new SearchPowerSteps(220, ds2BackingIndex2),
            new SearchPowerSteps(225, ds1BackingIndex2),
            new SearchPowerSteps(249, ds1BackingIndex1),
            new SearchPowerSteps(250, ds2BackingIndex1)
        );

        indices.clear();
        steps.forEach(step -> {
            replicasUpdater.updateSearchPowerMin(step.sp);
            indices.add(step.expectedAdditionalIndex.getIndex().getName());
            mockClient.assertUpdates(Map.of(2, indices));
        });
    }

    /**
     * Test that decreases search power in steps and asserts that indices that
     * replica calculation for indices that originally have 2 replicas is decreased
     * to 1
     */
    public void testGetNumberOfReplicaChangesSP250To100() {
        int initialReplicas = 2;
        Metadata.Builder clusterMetadata = Metadata.builder();
        var index1 = createIndex(1, initialReplicas, clusterMetadata);
        var index2 = createIndex(1, initialReplicas, clusterMetadata);
        var systemIndex = createSystemIndex(1, initialReplicas, clusterMetadata);

        String dataStream1Name = "ds1";
        IndexMetadata ds1BackingIndex1 = createBackingIndex(dataStream1Name, 1, initialReplicas, 1714000000000L).build();
        clusterMetadata.put(ds1BackingIndex1, false);
        IndexMetadata ds1BackingIndex2 = createBackingIndex(dataStream1Name, 2, initialReplicas, 1715000000000L).build();
        clusterMetadata.put(ds1BackingIndex2, false);
        DataStream ds1 = newInstance(dataStream1Name, List.of(ds1BackingIndex1.getIndex(), ds1BackingIndex2.getIndex()));
        clusterMetadata.put(ds1);

        String dataStream2Name = "ds2";
        IndexMetadata ds2BackingIndex1 = createBackingIndex(dataStream2Name, 1, initialReplicas, 1713000000000L).build();
        clusterMetadata.put(ds2BackingIndex1, false);
        IndexMetadata ds2BackingIndex2 = createBackingIndex(dataStream2Name, 2, initialReplicas, 1714000000000L).build();
        clusterMetadata.put(ds2BackingIndex2, false);
        DataStream ds2 = newInstance(dataStream2Name, List.of(ds2BackingIndex1.getIndex(), ds2BackingIndex2.getIndex()));
        clusterMetadata.put(ds2);

        var state1 = ClusterState.builder(ClusterState.EMPTY_STATE).nodes(createNodes(1)).metadata(clusterMetadata).build();
        searchMetricsService.clusterChanged(new ClusterChangedEvent("test", state1, ClusterState.EMPTY_STATE));

        Map<ShardId, ShardSize> shards = new HashMap<>();
        addShard(shards, index1, 0, 2000, 0);
        addShard(shards, index2, 0, 1000, 0);
        addShard(shards, systemIndex, 0, 1000, 0);
        addShard(shards, ds1BackingIndex1, 0, 500, 500);
        addShard(shards, ds1BackingIndex2, 0, 750, 500);
        addShard(shards, ds2BackingIndex1, 0, 500, 500);
        addShard(shards, ds2BackingIndex2, 0, 250, 500);
        searchMetricsService.processShardSizesRequest(new PublishShardSizesRequest("search_node_1", shards));

        List<SearchPowerSteps> steps = List.of(
            new SearchPowerSteps(249, ds2BackingIndex1),
            new SearchPowerSteps(225, ds1BackingIndex1),
            new SearchPowerSteps(220, ds2BackingIndex2),
            new SearchPowerSteps(200, ds1BackingIndex2),
            new SearchPowerSteps(175, index2),
            new SearchPowerSteps(150, index1),
            new SearchPowerSteps(100, systemIndex)
        );
        replicasUpdater.updateSearchPowerMin(250);
        mockClient.assertNoUpdate();

        Set<String> indices = new HashSet<>();
        steps.forEach(step -> {
            replicasUpdater.updateSearchPowerMin(step.sp);
            indices.add(step.expectedAdditionalIndex.getIndex().getName());
            mockClient.assertUpdates(Map.of(1, indices));
        });
    }

    public void testGetNumberOfReplicaChangesOnSearchPowerUpdate() {
        IndexMetadata outsideBoostWindowMetadata = createIndex(1, 1);
        IndexMetadata withinBoostWindowMetadata = createIndex(3, 1);

        var state = ClusterState.builder(ClusterState.EMPTY_STATE)
            .nodes(createNodes(1))
            .metadata(Metadata.builder().put(withinBoostWindowMetadata, false).put(outsideBoostWindowMetadata, false))
            .build();

        searchMetricsService.clusterChanged(new ClusterChangedEvent("test", state, ClusterState.EMPTY_STATE));
        assertEquals(0, replicasUpdater.getNumberOfReplicaChanges().size());

        searchMetricsService.processShardSizesRequest(
            new PublishShardSizesRequest(
                "search_node_1",
                Map.of(new ShardId(outsideBoostWindowMetadata.getIndex(), 0), new ShardSize(0, 1024, PrimaryTermAndGeneration.ZERO))
            )
        );
        assertEquals(0, replicasUpdater.getNumberOfReplicaChanges().size());

        int numShardsWithinBoostWindow = randomIntBetween(1, 3);
        searchMetricsService.processShardSizesRequest(
            new PublishShardSizesRequest(
                "search_node_1",
                Map.of(
                    new ShardId(withinBoostWindowMetadata.getIndex(), 0),
                    new ShardSize(1024, randomBoolean() ? 1024 : 0, PrimaryTermAndGeneration.ZERO),
                    new ShardId(withinBoostWindowMetadata.getIndex(), 1),
                    new ShardSize(numShardsWithinBoostWindow > 1 ? 1024 : 0, randomBoolean() ? 1024 : 0, PrimaryTermAndGeneration.ZERO),
                    new ShardId(withinBoostWindowMetadata.getIndex(), 2),
                    new ShardSize(numShardsWithinBoostWindow > 2 ? 1024 : 0, randomBoolean() ? 1024 : 0, PrimaryTermAndGeneration.ZERO)
                )
            )
        );
        assertEquals(0, replicasUpdater.getNumberOfReplicaChanges().size());
        assertEquals(1, searchMetricsService.getSearchTierMetrics().getMaxShardCopies().maxCopies());

        int spMin = randomIntBetween(250, 1000);
        replicasUpdater.updateSearchPowerMax(randomIntBetween(spMin, 1500));
        replicasUpdater.updateSearchPowerMin(spMin);
        mockClient.assertUpdates(Map.of(2, Set.of(withinBoostWindowMetadata.getIndex().getName())));

        state = updateIndexMetadata(state, withinBoostWindowMetadata, 3, 2);
        assertEquals(2, searchMetricsService.getSearchTierMetrics().getMaxShardCopies().maxCopies());

        replicasUpdater.updateSearchPowerMax(50);
        // there's a single index, its size will always exceed the threshold for getting replicas
        replicasUpdater.updateSearchPowerMin(randomIntBetween(1, 240));
        mockClient.assertUpdates(Map.of(1, Set.of(withinBoostWindowMetadata.getIndex().getName())));
        updateIndexMetadata(state, withinBoostWindowMetadata, 3, 1);
        assertEquals(1, searchMetricsService.getSearchTierMetrics().getMaxShardCopies().maxCopies());
    }

    public void testGetNumberOfReplicaChangesIndexRemoved() {
        Map<ShardId, ShardSize> shardSizeMap = new HashMap<>();
        Metadata.Builder metadataBuilder = Metadata.builder();
        for (int i = 0; i < 100; i++) {
            IndexMetadata index = createIndex(1, 1);
            metadataBuilder.put(index, false);
            shardSizeMap.put(new ShardId(index.getIndex(), 0), new ShardSize(0, 1024, PrimaryTermAndGeneration.ZERO));
        }

        var state = ClusterState.builder(ClusterState.EMPTY_STATE).nodes(createNodes(1)).metadata(metadataBuilder).build();

        searchMetricsService.clusterChanged(new ClusterChangedEvent("test", state, ClusterState.EMPTY_STATE));
        searchMetricsService.processShardSizesRequest(new PublishShardSizesRequest("search_node_1", shardSizeMap));

        ExecutorService executorService = Executors.newFixedThreadPool(2);
        try {
            ClusterState newState = ClusterState.builder(state)
                .metadata(Metadata.builder(state.metadata()).removeAllIndices().build())
                .build();
            Future<?> numReplicaChangesFuture = executorService.submit(
                () -> assertEquals(0, replicasUpdater.getNumberOfReplicaChanges().size())
            );
            Future<?> clusterChangedFuture = executorService.submit(
                () -> searchMetricsService.clusterChanged(new ClusterChangedEvent("test", newState, state))
            );
            numReplicaChangesFuture.get();
            clusterChangedFuture.get();
        } catch (InterruptedException e) {
            throw new ThreadInterruptedException(e);
        } catch (ExecutionException e) {
            fail(e);
        } finally {
            terminate(executorService);
        }
    }

    public void testGetNumberOfReplicaChangesOnShardSizeUpdateHighSearchPower() {
        replicasUpdater.updateSearchPowerMax(500);
        replicasUpdater.updateSearchPowerMin(250);
        // no indices yet, no update
        mockClient.assertNoUpdate();
        IndexMetadata outsideBoostWindowMetadata = createIndex(1, 1);
        IndexMetadata withinBoostWindowMetadata = createIndex(3, 1);

        var state = ClusterState.builder(ClusterState.EMPTY_STATE)
            .nodes(createNodes(1))
            .metadata(Metadata.builder().put(withinBoostWindowMetadata, false).put(outsideBoostWindowMetadata, false))
            .build();

        searchMetricsService.clusterChanged(new ClusterChangedEvent("test", state, ClusterState.EMPTY_STATE));
        assertEquals(0, replicasUpdater.getNumberOfReplicaChanges().size());

        searchMetricsService.processShardSizesRequest(
            new PublishShardSizesRequest(
                "search_node_1",
                Map.of(new ShardId(outsideBoostWindowMetadata.getIndex(), 0), new ShardSize(0, 1024, PrimaryTermAndGeneration.ZERO))
            )
        );
        assertEquals(0, replicasUpdater.getNumberOfReplicaChanges().size());

        int numShardsWithinBoostWindow = randomIntBetween(1, 3);
        searchMetricsService.processShardSizesRequest(
            new PublishShardSizesRequest(
                "search_node_1",
                Map.of(
                    new ShardId(withinBoostWindowMetadata.getIndex(), 0),
                    new ShardSize(1024, randomBoolean() ? 1024 : 0, PrimaryTermAndGeneration.ZERO),
                    new ShardId(withinBoostWindowMetadata.getIndex(), 1),
                    new ShardSize(numShardsWithinBoostWindow > 1 ? 1024 : 0, randomBoolean() ? 1024 : 0, PrimaryTermAndGeneration.ZERO),
                    new ShardId(withinBoostWindowMetadata.getIndex(), 2),
                    new ShardSize(numShardsWithinBoostWindow > 2 ? 1024 : 0, randomBoolean() ? 1024 : 0, PrimaryTermAndGeneration.ZERO)
                )
            )
        );
        {
            Map<Integer, Set<String>> numberOfReplicaChanges = replicasUpdater.getNumberOfReplicaChanges();
            assertEquals(1, numberOfReplicaChanges.size());
            assertEquals(Set.of(withinBoostWindowMetadata.getIndex().getName()), numberOfReplicaChanges.get(2));
        }

        state = updateIndexMetadata(state, withinBoostWindowMetadata, 3, 2);
        assertEquals(2, searchMetricsService.getSearchTierMetrics().getMaxShardCopies().maxCopies());

        // simulate the index falling out of the boost window
        searchMetricsService.processShardSizesRequest(
            new PublishShardSizesRequest(
                "search_node_1",
                Map.of(
                    new ShardId(withinBoostWindowMetadata.getIndex(), 0),
                    new ShardSize(0, 1024, PrimaryTermAndGeneration.ZERO),
                    new ShardId(withinBoostWindowMetadata.getIndex(), 1),
                    new ShardSize(0, 1024, PrimaryTermAndGeneration.ZERO),
                    new ShardId(withinBoostWindowMetadata.getIndex(), 2),
                    new ShardSize(0, 1024, PrimaryTermAndGeneration.ZERO)
                )
            )
        );
        {
            Map<Integer, Set<String>> numberOfReplicaChanges = replicasUpdater.getNumberOfReplicaChanges();
            assertEquals(1, numberOfReplicaChanges.size());
            assertEquals(Set.of(withinBoostWindowMetadata.getIndex().getName()), numberOfReplicaChanges.get(1));
            updateIndexMetadata(state, withinBoostWindowMetadata, 3, 1);
            assertEquals(1, searchMetricsService.getSearchTierMetrics().getMaxShardCopies().maxCopies());
        }
    }

    public void testEnableReplicasForInstantFailoverSetting() {
        // Simulate a scenario where indices have additional replicas to start with
        int initialReplicas = 2;
        Metadata.Builder clusterMetadata = Metadata.builder();
        var index1 = createIndex(3, initialReplicas, clusterMetadata);
        var index2 = createIndex(5, initialReplicas, clusterMetadata);
        var index3 = createIndex(1, 1, clusterMetadata);
        var systemIndex = createSystemIndex(1, initialReplicas, clusterMetadata);
        String dataStream1Name = "ds1";
        IndexMetadata ds1BackingIndex1 = createBackingIndex(dataStream1Name, 1, initialReplicas, 1714000000000L).build();
        clusterMetadata.put(ds1BackingIndex1, false);
        IndexMetadata ds1BackingIndex2 = createBackingIndex(dataStream1Name, 2, initialReplicas, 1715000000000L).build();
        clusterMetadata.put(ds1BackingIndex2, false);
        DataStream ds1 = newInstance(dataStream1Name, List.of(ds1BackingIndex1.getIndex(), ds1BackingIndex2.getIndex()));
        clusterMetadata.put(ds1);
        var state1 = ClusterState.builder(ClusterState.EMPTY_STATE).nodes(createNodes(1)).metadata(clusterMetadata).build();
        searchMetricsService.clusterChanged(new ClusterChangedEvent("test", state1, ClusterState.EMPTY_STATE));
        Map<ShardId, ShardSize> shards = new HashMap<>();
        addShard(shards, index1, 0, 2000, 0);
        addShard(shards, index2, 0, 1000, 0);
        addShard(shards, index3, 0, 1000, 0);
        addShard(shards, systemIndex, 0, 1000, 0);
        addShard(shards, ds1BackingIndex1, 0, 500, 500);
        addShard(shards, ds1BackingIndex2, 0, 750, 500);
        searchMetricsService.processShardSizesRequest(new PublishShardSizesRequest("search_node_1", shards));
        assertEquals(2, searchMetricsService.getSearchTierMetrics().getMaxShardCopies().maxCopies());

        try {
            // check that disabling replicas for instant failover removes additional replicas for all indices that have them
            replicasUpdater.updateEnableReplicasForInstantFailover(false);
            mockClient.assertUpdates(
                Map.of(
                    1,
                    Set.of(
                        index1.getIndex().getName(),
                        index2.getIndex().getName(),
                        systemIndex.getIndex().getName(),
                        ds1BackingIndex1.getIndex().getName(),
                        ds1BackingIndex2.getIndex().getName()
                    )
                )
            );
            state1 = updateIndexMetadata(state1, index1, 3, 1);
            state1 = updateIndexMetadata(state1, index2, 5, 1);
            state1 = updateIndexMetadata(state1, systemIndex, 1, 1);
            state1 = updateIndexMetadata(state1, ds1BackingIndex1, 1, 1);
            state1 = updateIndexMetadata(state1, ds1BackingIndex2, 1, 1);
            assertEquals(1, searchMetricsService.getSearchTierMetrics().getMaxShardCopies().maxCopies());
            // updating search power triggers no update because the setting is off
            replicasUpdater.updateSearchPower(randomIntBetween(250, 1000));
            mockClient.assertNoUpdate();
            assertEquals(1, searchMetricsService.getSearchTierMetrics().getMaxShardCopies().maxCopies());
        } finally {
            replicasUpdater.updateEnableReplicasForInstantFailover(true);
            // check that re-enabling replicas for instant failover increases replicas (SPmin is > 250)
            mockClient.assertUpdates(
                Map.of(
                    2,
                    Set.of(
                        index1.getIndex().getName(),
                        index2.getIndex().getName(),
                        index3.getIndex().getName(),
                        systemIndex.getIndex().getName(),
                        ds1BackingIndex1.getIndex().getName(),
                        ds1BackingIndex2.getIndex().getName()
                    )
                )
            );
            updateIndexMetadata(state1, index3, 1, 2);
            assertEquals(2, searchMetricsService.getSearchTierMetrics().getMaxShardCopies().maxCopies());
        }
    }

    private static IndexMetadata createIndex(int shards, int replicas, Metadata.Builder clusterMetadataBuilder) {
        return createIndex(shards, replicas, false, clusterMetadataBuilder);
    }

    private static IndexMetadata createIndex(int shards, int replicas) {
        return createIndex(shards, replicas, false, null);
    }

    private static IndexMetadata createSystemIndex(int shards, int replicas, Metadata.Builder clusterMetadataBuilder) {
        return createIndex(shards, replicas, true, clusterMetadataBuilder);
    }

    private static IndexMetadata createIndex(int shards, int replicas, boolean system, Metadata.Builder clusterMetadataBuilder) {
        IndexMetadata indexMetadata = IndexMetadata.builder(randomIdentifier())
            .settings(indexSettings(shards, replicas).put("index.version.created", Version.CURRENT))
            .system(system)
            .build();
        if (clusterMetadataBuilder != null) {
            clusterMetadataBuilder.put(indexMetadata, false);
        }
        return indexMetadata;
    }

    public static IndexMetadata.Builder createBackingIndex(String dataStreamName, int generation, int numberOfReplicas, long epochMillis) {
        return IndexMetadata.builder(DataStream.getDefaultBackingIndexName(dataStreamName, generation, epochMillis))
            .settings(ESTestCase.settings(IndexVersion.current()).put("index.hidden", true))
            .numberOfShards(1)
            .numberOfReplicas(numberOfReplicas)
            .creationDate(epochMillis);
    }

    private static void addShard(
        Map<ShardId, ShardSize> shardSizes,
        IndexMetadata index,
        int shardId,
        long interactiveSize,
        long nonInteractiveSize
    ) {
        shardSizes.put(
            new ShardId(index.getIndex(), shardId),
            new ShardSize(interactiveSize, nonInteractiveSize, new PrimaryTermAndGeneration(1, 1))
        );
    }

    private static ClusterSettings createClusterSettings() {
        Set<Setting<?>> defaultClusterSettings = Sets.addToCopy(
            ClusterSettings.BUILT_IN_CLUSTER_SETTINGS,
            SearchMetricsService.ACCURATE_METRICS_WINDOW_SETTING,
            SearchMetricsService.STALE_METRICS_CHECK_INTERVAL_SETTING,
            ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING,
            ServerlessSharedSettings.SEARCH_POWER_MAX_SETTING,
            ServerlessSharedSettings.SEARCH_POWER_SETTING,
            ServerlessSharedSettings.ENABLE_REPLICAS_FOR_INSTANT_FAILOVER
        );
        return new ClusterSettings(
            Settings.builder().put(ServerlessSharedSettings.ENABLE_REPLICAS_FOR_INSTANT_FAILOVER.getKey(), true).build(),
            defaultClusterSettings
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

    private ClusterState updateIndexMetadata(
        ClusterState previousState,
        IndexMetadata previousIndexMetadata,
        int numShards,
        int numReplicas
    ) {
        IndexMetadata newIndexMetadata = IndexMetadata.builder(previousIndexMetadata)
            .settings(indexSettings(numShards, numReplicas).put("index.version.created", Version.CURRENT))
            .build();
        var newState = ClusterState.builder(previousState)
            .metadata(Metadata.builder(previousState.metadata()).put(newIndexMetadata, false))
            .build();
        searchMetricsService.clusterChanged(new ClusterChangedEvent("test", newState, previousState));
        return newState;
    }

    private static class MockClient extends NoOpNodeClient {
        Map<Integer, Set<String>> updates = new HashMap<>();
        boolean updateSettingsToBeVerified = false;

        MockClient() {
            super(null);
        }

        @Override
        public <Request extends ActionRequest, Response extends ActionResponse> Task executeLocally(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            assertEquals(TransportUpdateSettingsAction.TYPE, action);
            assertThat(request, instanceOf(UpdateSettingsRequest.class));
            UpdateSettingsRequest updateSettingsRequest = (UpdateSettingsRequest) request;
            assertEquals(1, updateSettingsRequest.settings().size());
            int numReplicas = updateSettingsRequest.settings().getAsInt(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), null);
            updates.put(numReplicas, Set.of(updateSettingsRequest.indices()));
            updateSettingsToBeVerified = true;
            return super.executeLocally(action, request, listener);
        }

        void assertUpdates(Map<Integer, Set<String>> expectedUpdates) {
            assertTrue("update settings not yet called", updateSettingsToBeVerified);
            assertEquals(expectedUpdates, updates);
            updates.clear();
            updateSettingsToBeVerified = false;
        }

        void assertNoUpdate() {
            assertFalse(updateSettingsToBeVerified);
        }
    }
}
