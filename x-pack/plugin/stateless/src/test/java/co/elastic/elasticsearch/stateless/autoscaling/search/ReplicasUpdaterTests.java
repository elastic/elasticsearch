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
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.newInstance;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ReplicasUpdaterTests extends ESTestCase {

    private SearchMetricsService searchMetricsService;
    private ReplicasUpdater replicasUpdater;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        AtomicLong currentRelativeTimeInNanos = new AtomicLong(1L);
        MemoryMetricsService memoryMetricsService = mock(MemoryMetricsService.class);
        when(memoryMetricsService.getMemoryMetrics()).thenReturn(new MemoryMetrics(4096, 8192, MetricQuality.EXACT));
        ClusterSettings clusterSettings = createClusterSettings();
        searchMetricsService = new SearchMetricsService(clusterSettings, currentRelativeTimeInNanos::get, memoryMetricsService);
        replicasUpdater = new ReplicasUpdater(searchMetricsService, clusterSettings, null);
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

        replicasUpdater.updateSearchPower(100);

        assertEquals(Collections.emptyMap(), replicasUpdater.getNumberOfReplicaChanges());

        List<SearchPowerSteps> steps = List.of(
            new SearchPowerSteps(125, systemIndex),
            new SearchPowerSteps(175, index1),
            new SearchPowerSteps(200, index2),
            new SearchPowerSteps(220, ds1BackingIndex2),
            new SearchPowerSteps(225, ds2BackingIndex2),
            new SearchPowerSteps(249, ds1BackingIndex1),
            new SearchPowerSteps(250, ds2BackingIndex1)
        );

        List<String> indices = new ArrayList<>();
        steps.forEach(step -> {
            replicasUpdater.updateSearchPower(step.sp);
            indices.add(step.expectedAdditionalIndex.getIndex().getName());
            assertEquals(1, replicasUpdater.getNumberOfReplicaChanges().size());
            List<String> twoReplicasIndices = replicasUpdater.getNumberOfReplicaChanges().get(2);
            assertThat("Searchpower " + step.sp, indices, containsInAnyOrder(twoReplicasIndices.toArray(new String[0])));
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
            replicasUpdater.updateSearchPower(step.sp);
            indices.add(step.expectedAdditionalIndex.getIndex().getName());
            List<String> twoReplicasIndices = replicasUpdater.getNumberOfReplicaChanges().get(2);
            assertThat("Searchpower " + step.sp, indices, containsInAnyOrder(twoReplicasIndices.toArray(new String[0])));
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

        replicasUpdater.updateSearchPower(250);
        searchMetricsService.clusterChanged(new ClusterChangedEvent("test", state1, ClusterState.EMPTY_STATE));
        assertEquals(Collections.emptyMap(), replicasUpdater.getNumberOfReplicaChanges());

        List<String> indices = new ArrayList<>();
        steps.forEach(step -> {
            replicasUpdater.updateSearchPower(step.sp);
            indices.add(step.expectedAdditionalIndex.getIndex().getName());
            assertEquals(1, replicasUpdater.getNumberOfReplicaChanges().size());
            List<String> oneReplicaIndices = replicasUpdater.getNumberOfReplicaChanges().get(1);
            assertThat("Searchpower " + step.sp, indices, containsInAnyOrder(oneReplicaIndices.toArray(new String[0])));
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
        replicasUpdater.updateSearchPowerMax(500);
        replicasUpdater.updateSearchPowerMin(250);
        {
            Map<Integer, List<String>> numberOfReplicaChanges = replicasUpdater.getNumberOfReplicaChanges();
            assertEquals(1, numberOfReplicaChanges.size());
            assertEquals(List.of(withinBoostWindowMetadata.getIndex().getName()), numberOfReplicaChanges.get(2));
        }

        // simulate updating replica count according to last decision
        withinBoostWindowMetadata = IndexMetadata.builder(withinBoostWindowMetadata)
            .settings(indexSettings(3, 2).put("index.version.created", Version.CURRENT))
            .build();
        var newState = ClusterState.builder(state).metadata(Metadata.builder().put(withinBoostWindowMetadata, false)).build();
        searchMetricsService.clusterChanged(new ClusterChangedEvent("test", newState, state));
        replicasUpdater.updateSearchPowerMax(50);
        replicasUpdater.updateSearchPowerMin(150);
        {
            Map<Integer, List<String>> numberOfReplicaChanges = replicasUpdater.getNumberOfReplicaChanges();
            assertEquals(1, numberOfReplicaChanges.size());
            assertEquals(List.of(withinBoostWindowMetadata.getIndex().getName()), numberOfReplicaChanges.get(1));
        }
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
            Map<Integer, List<String>> numberOfReplicaChanges = replicasUpdater.getNumberOfReplicaChanges();
            assertEquals(1, numberOfReplicaChanges.size());
            assertEquals(List.of(withinBoostWindowMetadata.getIndex().getName()), numberOfReplicaChanges.get(2));
        }

        // simulate updating replica count according to last decision
        withinBoostWindowMetadata = IndexMetadata.builder(withinBoostWindowMetadata)
            .settings(indexSettings(3, 2).put("index.version.created", Version.CURRENT))
            .build();
        var newState = ClusterState.builder(state).metadata(Metadata.builder().put(withinBoostWindowMetadata, false)).build();
        searchMetricsService.clusterChanged(new ClusterChangedEvent("test", newState, state));

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
            Map<Integer, List<String>> numberOfReplicaChanges = replicasUpdater.getNumberOfReplicaChanges();
            assertEquals(1, numberOfReplicaChanges.size());
            assertEquals(List.of(withinBoostWindowMetadata.getIndex().getName()), numberOfReplicaChanges.get(1));
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
        return new ClusterSettings(Settings.EMPTY, defaultClusterSettings());
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

}
