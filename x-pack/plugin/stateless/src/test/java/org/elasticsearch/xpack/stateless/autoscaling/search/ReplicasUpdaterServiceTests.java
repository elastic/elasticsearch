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

package org.elasticsearch.xpack.stateless.autoscaling.search;

import co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings;
import co.elastic.elasticsearch.stateless.api.ShardSizeStatsReader.ShardSize;

import org.apache.lucene.util.ThreadInterruptedException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.test.client.NoOpNodeClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.xpack.stateless.autoscaling.DesiredClusterTopology;
import org.elasticsearch.xpack.stateless.autoscaling.DesiredTopologyContext;
import org.elasticsearch.xpack.stateless.autoscaling.MetricQuality;
import org.elasticsearch.xpack.stateless.autoscaling.memory.MemoryMetrics;
import org.elasticsearch.xpack.stateless.autoscaling.memory.MemoryMetricsService;
import org.elasticsearch.xpack.stateless.autoscaling.search.ReplicasLoadBalancingScaler.ReplicasLoadBalancingResult;
import org.elasticsearch.xpack.stateless.autoscaling.search.SearchMetricsService.ShardMetrics;
import org.junit.After;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.newInstance;
import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.elasticsearch.xpack.stateless.autoscaling.DesiredClusterTopologyTestUtils.randomDesiredClusterTopology;
import static org.elasticsearch.xpack.stateless.autoscaling.search.ReplicasLoadBalancingScaler.EMPTY_RESULT;
import static org.elasticsearch.xpack.stateless.autoscaling.search.ReplicasUpdaterService.REPLICA_UPDATER_SCALEDOWN_REPETITIONS;
import static org.elasticsearch.xpack.stateless.autoscaling.search.ReplicasUpdaterService.SEARCH_POWER_MIN_FULL_REPLICATION;
import static org.elasticsearch.xpack.stateless.autoscaling.search.ReplicasUpdaterService.SEARCH_POWER_MIN_NO_REPLICATION;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class ReplicasUpdaterServiceTests extends ESTestCase {

    private SearchMetricsService searchMetricsService;
    private ReplicasUpdaterService replicasUpdaterService;
    private TestThreadPool testThreadPool;
    private MockClient mockClient;
    private DesiredTopologyContext desiredTopologyContext;
    private ReplicasLoadBalancingScaler replicasLoadBalancingScaler;
    private RecordingMeterRegistry recordingMeterRegistry;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        AtomicLong currentRelativeTimeInNanos = new AtomicLong(1L);
        MemoryMetricsService memoryMetricsService = mock(MemoryMetricsService.class);
        when(memoryMetricsService.getSearchTierMemoryMetrics()).thenReturn(new MemoryMetrics(4096, 8192, MetricQuality.EXACT));
        ClusterSettings clusterSettings = createClusterSettings();
        searchMetricsService = spy(
            new SearchMetricsService(
                clusterSettings,
                currentRelativeTimeInNanos::get,
                memoryMetricsService,
                TelemetryProvider.NOOP.getMeterRegistry()
            )
        );
        this.testThreadPool = new TestThreadPool(getTestName());
        mockClient = new MockClient();
        desiredTopologyContext = new DesiredTopologyContext(ClusterServiceUtils.createClusterService(this.testThreadPool));
        ClusterService clusterService = createClusterService(testThreadPool, createClusterSettings());
        replicasLoadBalancingScaler = new ReplicasLoadBalancingScaler(clusterService, mockClient);
        recordingMeterRegistry = new RecordingMeterRegistry();
        replicasUpdaterService = new ReplicasUpdaterService(
            testThreadPool,
            clusterService,
            mockClient,
            searchMetricsService,
            replicasLoadBalancingScaler,
            desiredTopologyContext,
            recordingMeterRegistry
        );
        replicasUpdaterService.init();
        replicasUpdaterService.updatedEnableReplicasLoadBalancing(false);
        // start job but with a very high interval to control calls manually
        replicasUpdaterService.setInterval(TimeValue.timeValueMinutes(60));
        replicasUpdaterService.onMaster();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        mockClient.assertNoUpdate();
    }

    @After
    public void afterTest() {
        this.testThreadPool.shutdown();
    }

    public void testUpdatePollInterval() {
        ClusterService clusterService = createClusterService(testThreadPool, createClusterSettings());
        ReplicasLoadBalancingScaler replicasLoadBalancingScaler = new ReplicasLoadBalancingScaler(clusterService, mockClient);
        ReplicasUpdaterService instance = new ReplicasUpdaterService(
            testThreadPool,
            clusterService,
            mockClient,
            searchMetricsService,
            replicasLoadBalancingScaler,
            new DesiredTopologyContext(ClusterServiceUtils.createClusterService(this.testThreadPool)),
            TelemetryProvider.NOOP.getMeterRegistry()
        );
        instance.init();
        instance.scheduleTask();
        TimeValue defaultValue = ReplicasUpdaterService.REPLICA_UPDATER_INTERVAL.getDefault(Settings.EMPTY);
        assertThat(instance.getInterval(), equalTo(defaultValue));
        assertThat(instance.getJob().toString(), containsString(defaultValue.toString()));
        instance.setInterval(TimeValue.timeValueMinutes(1));
        assertThat(instance.getInterval(), equalTo(TimeValue.timeValueMinutes(1)));
        assertThat(instance.getJob().toString(), containsString("1m"));
    }

    public void testUpdatePollIntervalUnscheduled() {
        ClusterService clusterService = createClusterService(testThreadPool, createClusterSettings());
        ReplicasLoadBalancingScaler replicasLoadBalancingScaler = new ReplicasLoadBalancingScaler(clusterService, mockClient);
        ReplicasUpdaterService instance = new ReplicasUpdaterService(
            testThreadPool,
            clusterService,
            mockClient,
            searchMetricsService,
            replicasLoadBalancingScaler,
            new DesiredTopologyContext(ClusterServiceUtils.createClusterService(this.testThreadPool)),
            TelemetryProvider.NOOP.getMeterRegistry()
        );
        instance.init();
        TimeValue defaultValue = ReplicasUpdaterService.REPLICA_UPDATER_INTERVAL.getDefault(Settings.EMPTY);
        assertThat(instance.getInterval(), equalTo(defaultValue));
        assertThat(instance.getJob(), nullValue());
        instance.setInterval(TimeValue.timeValueMinutes(1));
        assertThat(instance.getInterval(), equalTo(TimeValue.timeValueMinutes(1)));
        assertThat(instance.getJob(), nullValue());
    }

    public void testCancelingTaskClearsState() {
        updateSpMin(SEARCH_POWER_MIN_NO_REPLICATION);
        this.replicasUpdaterService.setInterval(TimeValue.timeValueMillis(10));

        Index index1 = new Index("index1", "uuid");
        when(this.searchMetricsService.getIndices()).thenReturn(
            new ConcurrentHashMap<>(Map.of(index1, new SearchMetricsService.IndexProperties(index1.getName(), 1, 2, true, false, 0)))
        );
        when(searchMetricsService.getShardMetrics()).thenReturn(
            new ConcurrentHashMap<>(Map.of(new ShardId(index1, 0), shardMetricOf(1000)))
        );
        // make sure we get a couple of updates before we cancel the thread
        waitUntil(() -> this.mockClient.executionCount.get() > 5, 3, TimeUnit.SECONDS);

        this.replicasUpdaterService.offMaster();
        assertTrue(this.replicasUpdaterService.replicasScaleDownState.isEmpty());

        // clear mock client before teardown
        mockClient.updateSettingsToBeVerified = false;
    }

    /**
     * Test that the scheduled job runs at least once when a node running ReplicasUpdaterService becomes master.
     * We increase the schuled task interval for this and only call the onMaster() method to see
     * if the configured test client gets at least one call.
     */
    public void testSchedulingRunsJobOnce() {
        SearchMetricsService searchMetricsServiceMock = Mockito.mock(SearchMetricsService.class);
        Metadata.Builder clusterMetadata = Metadata.builder();
        Index index = new Index("index", "uuid");
        ShardMetrics shardMetrics = new ShardMetrics();
        shardMetrics.shardSize = new ShardSize(1000, 0, 0, 0);
        when(searchMetricsServiceMock.createRankingContext()).thenReturn(
            new ReplicaRankingContext(
                Map.of(index, new SearchMetricsService.IndexProperties("index", 1, 1, true, false, 0)),
                Map.of(new ShardId(index, 0), shardMetrics),
                250
            )
        );
        ClusterService clusterService = createClusterService(testThreadPool, createClusterSettings());
        ReplicasLoadBalancingScaler replicasLoadBalancingScaler = new ReplicasLoadBalancingScaler(clusterService, mockClient);
        ReplicasUpdaterService instance = new ReplicasUpdaterService(
            testThreadPool,
            clusterService,
            mockClient,
            searchMetricsServiceMock,
            replicasLoadBalancingScaler,
            new DesiredTopologyContext(ClusterServiceUtils.createClusterService(this.testThreadPool)),
            TelemetryProvider.NOOP.getMeterRegistry()
        );
        instance.init();
        // start job but with a very high interval to control calls manually
        replicasUpdaterService.setInterval(TimeValue.timeValueMinutes(60));
        mockClient.assertNoUpdate();
        instance.onMaster();
        mockClient.assertUpdates("SPmin: " + 250, Map.of(2, Set.of(index.getName())));
    }

    private record SearchPowerSteps(int sp, IndexMetadata expectedAdditionalIndex) {}

    /**
     * Test that increases search power in steps and asserts that indices that
     * replica calculation for indices that originally have 1 replica is increased to 2
     */
    public void testGetNumberOfReplicaChangesSP100To250() {
        int initialReplicas = 1;
        Metadata.Builder clusterMetadata = Metadata.builder();
        var index1 = createIndex(1, initialReplicas, clusterMetadata);
        var index2 = createIndex(1, initialReplicas, clusterMetadata);
        var systemIndexNonInteractive = createSystemIndex();
        clusterMetadata.put(systemIndexNonInteractive, false);
        var systemIndexInteractive = createSystemIndex();
        clusterMetadata.put(systemIndexInteractive, false);

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

        var state1 = ClusterState.builder(ClusterState.EMPTY_STATE).nodes(createNodes()).metadata(clusterMetadata).build();
        searchMetricsService.clusterChanged(new LocalMasterClusterChangedEvent("test", state1, ClusterState.EMPTY_STATE));

        Map<ShardId, ShardSize> shards = new HashMap<>();
        addShard(shards, index1, 0, 1400, 0);
        addShard(shards, index2, 0, 1000, 0);
        addShard(shards, systemIndexNonInteractive, 0, 0, 256);
        addShard(shards, systemIndexInteractive, 0, 1600, 0);
        addShard(shards, ds1BackingIndex1, 0, 500, 500);
        addShard(shards, ds1BackingIndex2, 0, 750, 500);
        addShard(shards, ds2BackingIndex1, 0, 500, 500);
        addShard(shards, ds2BackingIndex2, 0, 250, 500);
        searchMetricsService.processShardSizesRequest(new PublishShardSizesRequest("search_node_1", shards));

        updateSpMin(SEARCH_POWER_MIN_NO_REPLICATION);
        Map<String, Integer> recommendedReplicasState = getRecommendedReplicasState(SEARCH_POWER_MIN_NO_REPLICATION + 1);
        assertEquals(8, recommendedReplicasState.size());
        recommendedReplicasState.forEach((key, value) -> assertThat("index with more replicas: " + key, value, is(1)));
        mockClient.assertNoUpdate();

        List<SearchPowerSteps> steps = List.of(
            new SearchPowerSteps(140, systemIndexInteractive),
            new SearchPowerSteps(175, index1),
            new SearchPowerSteps(200, index2),
            new SearchPowerSteps(220, ds1BackingIndex2),
            new SearchPowerSteps(225, ds2BackingIndex2),
            new SearchPowerSteps(249, ds1BackingIndex1),
            new SearchPowerSteps(250, ds2BackingIndex1)
        );

        Set<String> indices = new HashSet<>();
        steps.forEach(step -> {
            updateSpMin(step.sp);
            indices.add(step.expectedAdditionalIndex.getIndex().getName());
            mockClient.assertUpdates("SPmin: " + step.sp, Map.of(2, indices));
        });

        // check that size changes affect the ranking.
        // We'll swap interactive size for index1 and index2
        // also swap ds1BackingIndex2 and ds2BackingIndex2 since they should use size as tie-breaker, their
        // recency is "now" because they are both write indices
        shards = new HashMap<>();
        addShard(shards, index1, 0, 1000, 0);
        addShard(shards, index2, 0, 1400, 0);
        addShard(shards, ds1BackingIndex2, 0, 250, 500);
        addShard(shards, ds2BackingIndex2, 0, 750, 500);
        searchMetricsService.processShardSizesRequest(new PublishShardSizesRequest("search_node_1", shards));

        steps = List.of(
            new SearchPowerSteps(140, systemIndexInteractive),
            new SearchPowerSteps(175, index2),
            new SearchPowerSteps(200, index1),
            new SearchPowerSteps(220, ds2BackingIndex2),
            new SearchPowerSteps(225, ds1BackingIndex2),
            new SearchPowerSteps(249, ds1BackingIndex1),
            new SearchPowerSteps(250, ds2BackingIndex1)
        );

        indices.clear();
        steps.forEach(step -> {
            updateSpMin(step.sp);
            indices.add(step.expectedAdditionalIndex.getIndex().getName());
            mockClient.assertUpdates("SPmin: " + step.sp, Map.of(2, indices));
        });
    }

    private Map<String, Integer> getRecommendedReplicasState(int searchPowerMin) {
        return ReplicasUpdaterService.getRecommendedReplicasState(
            new ReplicaRankingContext(searchMetricsService.getIndices(), searchMetricsService.getShardMetrics(), searchPowerMin)
        );
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
        var systemIndexInteractive = createIndex(1, initialReplicas, clusterMetadata);

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

        var state1 = ClusterState.builder(ClusterState.EMPTY_STATE).nodes(createNodes()).metadata(clusterMetadata).build();
        searchMetricsService.clusterChanged(new LocalMasterClusterChangedEvent("test", state1, ClusterState.EMPTY_STATE));

        Map<ShardId, ShardSize> shards = new HashMap<>();
        addShard(shards, index1, 0, 1100, 0);
        addShard(shards, index2, 0, 1000, 0);
        addShard(shards, systemIndexInteractive, 0, 1900, 0);
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
            new SearchPowerSteps(100, systemIndexInteractive)
        );
        updateSpMin(SEARCH_POWER_MIN_FULL_REPLICATION);
        mockClient.assertNoUpdate();

        Set<String> indices = new HashSet<>();
        steps.forEach(step -> {
            indices.add(step.expectedAdditionalIndex.getIndex().getName());
            updateSpMin(step.sp);
            mockClient.assertUpdates("SPmin: " + step.sp, Map.of(1, indices));
        });
    }

    public void testGetNumberOfReplicaChangesOnSearchPowerUpdate() {
        IndexMetadata outsideBoostWindowMetadata = createIndex(1);
        IndexMetadata withinBoostWindowMetadata = createIndex(3);

        var state = ClusterState.builder(ClusterState.EMPTY_STATE)
            .nodes(createNodes())
            .metadata(Metadata.builder().put(withinBoostWindowMetadata, false).put(outsideBoostWindowMetadata, false))
            .build();

        searchMetricsService.clusterChanged(new LocalMasterClusterChangedEvent("test", state, ClusterState.EMPTY_STATE));
        Map<String, Integer> recommendedReplicasState = getRecommendedReplicasState(SEARCH_POWER_MIN_NO_REPLICATION + 1);
        assertEquals(2, recommendedReplicasState.size());
        recommendedReplicasState.forEach((key, value) -> assertThat("index with more replicas: " + key, value, is(1)));

        searchMetricsService.processShardSizesRequest(
            new PublishShardSizesRequest(
                "search_node_1",
                Map.of(new ShardId(outsideBoostWindowMetadata.getIndex(), 0), new ShardSize(0, 1024, 0, 0))
            )
        );
        recommendedReplicasState = getRecommendedReplicasState(SEARCH_POWER_MIN_NO_REPLICATION + 1);
        assertEquals(2, recommendedReplicasState.size());
        recommendedReplicasState.forEach((key, value) -> assertThat("index with more replicas: " + key, value, is(1)));

        int numShardsWithinBoostWindow = randomIntBetween(1, 3);
        searchMetricsService.processShardSizesRequest(
            new PublishShardSizesRequest(
                "search_node_1",
                Map.of(
                    new ShardId(withinBoostWindowMetadata.getIndex(), 0),
                    new ShardSize(1024, randomBoolean() ? 1024 : 0, 0, 0),
                    new ShardId(withinBoostWindowMetadata.getIndex(), 1),
                    new ShardSize(numShardsWithinBoostWindow > 1 ? 1024 : 0, randomBoolean() ? 1024 : 0, 0, 0),
                    new ShardId(withinBoostWindowMetadata.getIndex(), 2),
                    new ShardSize(numShardsWithinBoostWindow > 2 ? 1024 : 0, randomBoolean() ? 1024 : 0, 0, 0)
                )
            )
        );
        recommendedReplicasState = getRecommendedReplicasState(SEARCH_POWER_MIN_NO_REPLICATION + 1);
        assertEquals(2, recommendedReplicasState.size());
        recommendedReplicasState.forEach((key, value) -> assertThat("index with more replicas: " + key, value, is(1)));
        assertEquals(1, searchMetricsService.getSearchTierMetrics().getMaxShardCopies().maxCopies());

        int spMin = randomIntBetween(250, 1000);

        updateSpMin(spMin);
        mockClient.assertUpdates("SPmin: " + spMin, Map.of(2, Set.of(withinBoostWindowMetadata.getIndex().getName())));

        state = updateIndexMetadata(state, withinBoostWindowMetadata, 3, 2);
        assertEquals(2, searchMetricsService.getSearchTierMetrics().getMaxShardCopies().maxCopies());

        // there's a single index, its size will always exceed the threshold for getting replicas
        spMin = randomIntBetween(1, 240);
        updateSpMin(spMin);
        mockClient.assertUpdates("SPmin: " + spMin, Map.of(1, Set.of(withinBoostWindowMetadata.getIndex().getName())));
        updateIndexMetadata(state, withinBoostWindowMetadata, 3, 1);
        assertEquals(1, searchMetricsService.getSearchTierMetrics().getMaxShardCopies().maxCopies());
    }

    public void testGetNumberOfReplicaChangesIndexRemoved() {
        Map<ShardId, ShardSize> shardSizeMap = new HashMap<>();
        Metadata.Builder metadataBuilder = Metadata.builder();
        for (int i = 0; i < 100; i++) {
            IndexMetadata index = createIndex(1);
            metadataBuilder.put(index, false);
            shardSizeMap.put(new ShardId(index.getIndex(), 0), new ShardSize(0, 1024, 0, 0));
        }

        var state = ClusterState.builder(ClusterState.EMPTY_STATE).nodes(createNodes()).metadata(metadataBuilder).build();

        searchMetricsService.clusterChanged(new LocalMasterClusterChangedEvent("test", state, ClusterState.EMPTY_STATE));
        searchMetricsService.processShardSizesRequest(new PublishShardSizesRequest("search_node_1", shardSizeMap));

        ExecutorService executorService = Executors.newFixedThreadPool(2);
        try {
            ClusterState newState = ClusterState.builder(state)
                .metadata(Metadata.builder(state.metadata()).removeAllIndices().build())
                .build();
            Future<?> replicasRecommendationsState = executorService.submit(() -> {
                var recommendedReplicasState = getRecommendedReplicasState(SEARCH_POWER_MIN_NO_REPLICATION + 1);
                // the number of indices we see depends on whether the cluster state change processing
                // race condition with searchMetricsService.clusterChanged()
                if (recommendedReplicasState.isEmpty() == false) {
                    // if we saw any index, they should all 1 replica
                    recommendedReplicasState.forEach((key, value) -> assertThat("index with more replicas: " + key, value, is(1)));
                }
            });
            Future<?> clusterChangedFuture = executorService.submit(
                () -> searchMetricsService.clusterChanged(new LocalMasterClusterChangedEvent("test", newState, state))
            );
            replicasRecommendationsState.get();
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
        updateSpMin(SEARCH_POWER_MIN_FULL_REPLICATION);
        // no indices yet, no update
        mockClient.assertNoUpdate();
        IndexMetadata outsideBoostWindowMetadata = createIndex(1);
        IndexMetadata withinBoostWindowMetadata = createIndex(3);
        IndexMetadata systemIndexInteractive = createSystemIndex();

        var state = ClusterState.builder(ClusterState.EMPTY_STATE)
            .nodes(createNodes())
            .metadata(
                Metadata.builder()
                    .put(withinBoostWindowMetadata, false)
                    .put(outsideBoostWindowMetadata, false)
                    .put(systemIndexInteractive, false)
            )
            .build();

        searchMetricsService.clusterChanged(new LocalMasterClusterChangedEvent("test", state, ClusterState.EMPTY_STATE));
        Map<String, Integer> recommendedReplicasState = getRecommendedReplicasState(SEARCH_POWER_MIN_FULL_REPLICATION);
        assertEquals(3, recommendedReplicasState.size());
        // all indices have no interactive data, so they remain on 1 replica
        assertThat(recommendedReplicasState.get(outsideBoostWindowMetadata.getIndex().getName()), is(1));
        assertThat(recommendedReplicasState.get(withinBoostWindowMetadata.getIndex().getName()), is(1));
        assertThat(recommendedReplicasState.get(systemIndexInteractive.getIndex().getName()), is(1));

        searchMetricsService.processShardSizesRequest(
            new PublishShardSizesRequest(
                "search_node_1",
                Map.of(new ShardId(outsideBoostWindowMetadata.getIndex(), 0), new ShardSize(0, 1024, 0, 0))
            )
        );
        recommendedReplicasState = getRecommendedReplicasState(SEARCH_POWER_MIN_FULL_REPLICATION);
        assertEquals(3, recommendedReplicasState.size());
        // all indices have still no interactive data, so they remain on 1 replica
        assertThat(recommendedReplicasState.get(outsideBoostWindowMetadata.getIndex().getName()), is(1));
        assertThat(recommendedReplicasState.get(withinBoostWindowMetadata.getIndex().getName()), is(1));
        assertThat(recommendedReplicasState.get(systemIndexInteractive.getIndex().getName()), is(1));

        int numShardsWithinBoostWindow = randomIntBetween(1, 3);
        searchMetricsService.processShardSizesRequest(
            new PublishShardSizesRequest(
                "search_node_1",
                Map.of(
                    new ShardId(withinBoostWindowMetadata.getIndex(), 0),
                    new ShardSize(1024, randomBoolean() ? 1024 : 0, 0, 0),
                    new ShardId(withinBoostWindowMetadata.getIndex(), 1),
                    new ShardSize(numShardsWithinBoostWindow > 1 ? 1024 : 0, randomBoolean() ? 1024 : 0, 0, 0),
                    new ShardId(withinBoostWindowMetadata.getIndex(), 2),
                    new ShardSize(numShardsWithinBoostWindow > 2 ? 1024 : 0, randomBoolean() ? 1024 : 0, 0, 0),
                    new ShardId(systemIndexInteractive.getIndex(), 0),
                    new ShardSize(1024, randomBoolean() ? 1024 : 0, 0, 0)
                )
            )
        );
        {
            recommendedReplicasState = getRecommendedReplicasState(SEARCH_POWER_MIN_FULL_REPLICATION);
            assertEquals(3, recommendedReplicasState.size());
            assertThat(recommendedReplicasState.get(withinBoostWindowMetadata.getIndex().getName()), is(2));
            assertThat(recommendedReplicasState.get(systemIndexInteractive.getIndex().getName()), is(2));
            // outside boost window remains on 1 replica
            assertThat(recommendedReplicasState.get(outsideBoostWindowMetadata.getIndex().getName()), is(1));
        }

        state = updateIndexMetadata(state, withinBoostWindowMetadata, 3, 2);
        state = updateIndexMetadata(state, systemIndexInteractive, 1, 2);
        assertEquals(2, searchMetricsService.getSearchTierMetrics().getMaxShardCopies().maxCopies());

        // simulate the index falling out of the boost window
        searchMetricsService.processShardSizesRequest(
            new PublishShardSizesRequest(
                "search_node_1",
                Map.of(
                    new ShardId(withinBoostWindowMetadata.getIndex(), 0),
                    new ShardSize(0, 1024, 0, 0),
                    new ShardId(withinBoostWindowMetadata.getIndex(), 1),
                    new ShardSize(0, 1024, 0, 0),
                    new ShardId(withinBoostWindowMetadata.getIndex(), 2),
                    new ShardSize(0, 1024, 0, 0)
                )
            )
        );
        {
            recommendedReplicasState = getRecommendedReplicasState(SEARCH_POWER_MIN_FULL_REPLICATION);
            assertEquals(3, recommendedReplicasState.size());
            // withinBoostWindowMetadata is now non-interactive, so they get 1 replica
            assertThat(recommendedReplicasState.get(withinBoostWindowMetadata.getIndex().getName()), is(1));
            assertThat(recommendedReplicasState.get(outsideBoostWindowMetadata.getIndex().getName()), is(1));
            assertThat(recommendedReplicasState.get(systemIndexInteractive.getIndex().getName()), is(2));

            state = updateIndexMetadata(state, withinBoostWindowMetadata, 3, 1);
            assertEquals(2, searchMetricsService.getSearchTierMetrics().getMaxShardCopies().maxCopies());
        }

        // simulate the system index falling out of the boost window
        searchMetricsService.processShardSizesRequest(
            new PublishShardSizesRequest(
                "search_node_1",
                Map.of(new ShardId(systemIndexInteractive.getIndex(), 0), new ShardSize(0, 1024, 0, 0))
            )
        );
        {
            recommendedReplicasState = getRecommendedReplicasState(SEARCH_POWER_MIN_FULL_REPLICATION);
            assertEquals(3, recommendedReplicasState.size());
            // the system index is non-interactive now as well, so all indices have no interactive data, so they get 1 replica
            assertThat(recommendedReplicasState.get(outsideBoostWindowMetadata.getIndex().getName()), is(1));
            assertThat(recommendedReplicasState.get(withinBoostWindowMetadata.getIndex().getName()), is(1));
            assertThat(recommendedReplicasState.get(systemIndexInteractive.getIndex().getName()), is(1));

            updateIndexMetadata(state, systemIndexInteractive, 1, 1);
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
        var systemIndexInteractive = createIndex(1, initialReplicas, clusterMetadata);
        String dataStream1Name = "ds1";
        IndexMetadata ds1BackingIndex1 = createBackingIndex(dataStream1Name, 1, initialReplicas, 1714000000000L).build();
        clusterMetadata.put(ds1BackingIndex1, false);
        IndexMetadata ds1BackingIndex2 = createBackingIndex(dataStream1Name, 2, initialReplicas, 1715000000000L).build();
        clusterMetadata.put(ds1BackingIndex2, false);
        DataStream ds1 = newInstance(dataStream1Name, List.of(ds1BackingIndex1.getIndex(), ds1BackingIndex2.getIndex()));
        clusterMetadata.put(ds1);
        var state1 = ClusterState.builder(ClusterState.EMPTY_STATE).nodes(createNodes()).metadata(clusterMetadata).build();
        searchMetricsService.clusterChanged(new LocalMasterClusterChangedEvent("test", state1, ClusterState.EMPTY_STATE));
        Map<ShardId, ShardSize> shards = new HashMap<>();
        addShard(shards, index1, 0, 2000, 0);
        addShard(shards, index2, 0, 1000, 0);
        addShard(shards, index3, 0, 1000, 0);
        addShard(shards, systemIndexInteractive, 0, 1000, 0);
        addShard(shards, ds1BackingIndex1, 0, 500, 500);
        addShard(shards, ds1BackingIndex2, 0, 750, 500);
        searchMetricsService.processShardSizesRequest(new PublishShardSizesRequest("search_node_1", shards));
        assertEquals(2, searchMetricsService.getSearchTierMetrics().getMaxShardCopies().maxCopies());

        try {
            replicasUpdaterService.updatedEnableReplicasLoadBalancing(false);
            // check that disabling replicas for instant failover removes additional replicas for all indices that have them
            replicasUpdaterService.updateEnableReplicasForInstantFailover(false);
            // run the main loop manually once to trigger the update
            replicasUpdaterService.performReplicaUpdates();
            mockClient.assertUpdates(
                "service disabled",
                Map.of(
                    1,
                    Set.of(
                        index1.getIndex().getName(),
                        index2.getIndex().getName(),
                        systemIndexInteractive.getIndex().getName(),
                        ds1BackingIndex1.getIndex().getName(),
                        ds1BackingIndex2.getIndex().getName()
                    )
                )
            );
            state1 = updateIndexMetadata(state1, index1, 3, 1);
            state1 = updateIndexMetadata(state1, index2, 5, 1);
            state1 = updateIndexMetadata(state1, systemIndexInteractive, 1, 1);
            state1 = updateIndexMetadata(state1, ds1BackingIndex1, 1, 1);
            state1 = updateIndexMetadata(state1, ds1BackingIndex2, 1, 1);
            assertEquals(1, searchMetricsService.getSearchTierMetrics().getMaxShardCopies().maxCopies());
            // updating search power triggers no update because the setting is off
            updateSpMin(randomIntBetween(250, 1000));
            replicasUpdaterService.performReplicaUpdates();
            mockClient.assertNoUpdate();
            assertEquals(1, searchMetricsService.getSearchTierMetrics().getMaxShardCopies().maxCopies());
        } finally {
            // check that re-enabling replicas for instant failover increases replicas (SPmin is > 250)
            replicasUpdaterService.updateEnableReplicasForInstantFailover(true);
            replicasUpdaterService.performReplicaUpdates();
            mockClient.assertUpdates(
                "service enabled",
                Map.of(
                    2,
                    Set.of(
                        index1.getIndex().getName(),
                        index2.getIndex().getName(),
                        index3.getIndex().getName(),
                        systemIndexInteractive.getIndex().getName(),
                        ds1BackingIndex1.getIndex().getName(),
                        ds1BackingIndex2.getIndex().getName()
                    )
                )
            );
            updateIndexMetadata(state1, index3, 1, 2);
            assertEquals(2, searchMetricsService.getSearchTierMetrics().getMaxShardCopies().maxCopies());
        }
    }

    /**
     * Check repeating scale down suggestion triggers update only after certain number of repetitions
     */
    public void testNumberOfRepetitionsScaleDown() {
        // disable scheduled task so we can trigger it manually
        replicasUpdaterService.setInterval(TimeValue.timeValueMinutes(SEARCH_POWER_MIN_NO_REPLICATION));

        // case 100 < SPmin < 250
        int spMin = randomIntBetween(SEARCH_POWER_MIN_NO_REPLICATION + 1, 249);
        updateSpMin(spMin);

        Index index1 = new Index("index1", "uuid");
        when(searchMetricsService.getIndices()).thenReturn(
            new ConcurrentHashMap<>(Map.of(index1, new SearchMetricsService.IndexProperties("index1", 1, 2, false, false, 0)))
        );
        when(searchMetricsService.getShardMetrics()).thenReturn(
            new ConcurrentHashMap<>(Map.of(new ShardId(index1, 0), shardMetricOf(1000)))
        );

        int repetitionsNeeded = REPLICA_UPDATER_SCALEDOWN_REPETITIONS.getDefault(Settings.EMPTY);
        assertNoUpdateAfterRepeats(repetitionsNeeded - 1, this.replicasUpdaterService::performReplicaUpdates);
        replicasUpdaterService.performReplicaUpdates();
        mockClient.assertUpdates("SPmin: " + spMin, Map.of(1, Set.of("index1")));

        // case SP >= 250
        spMin = randomIntBetween(250, 400);
        updateSpMin(spMin);
        // report index shard size as non-interactive so this gets scale down decision even with SP >= 250
        when(searchMetricsService.getShardMetrics()).thenReturn(
            new ConcurrentHashMap<>(Map.of(new ShardId(index1, 0), shardMetricOf(0, 1000)))
        );

        assertNoUpdateAfterRepeats(repetitionsNeeded - 1, this.replicasUpdaterService::performReplicaUpdates);
        replicasUpdaterService.performReplicaUpdates();
        mockClient.assertUpdates("SPmin: " + spMin, Map.of(1, Set.of("index1")));

        // only topology checks don't reduce the number of replicas
        assertNoUpdateAfterRepeats(repetitionsNeeded + 1, () -> this.replicasUpdaterService.performReplicaUpdates(false, true));

        when(searchMetricsService.getIndices()).thenReturn(
            new ConcurrentHashMap<>(
                Map.of(new Index("index2", "uuid"), new SearchMetricsService.IndexProperties("index2", 1, 2, false, false, 0))
            )
        );

        // check for SP < 100 update is immediate
        spMin = randomIntBetween(0, 99);
        updateSpMin(spMin); // this implicitely re-calculates the settings updates
        mockClient.assertUpdates("SPmin: " + spMin, Map.of(1, Set.of("index2")));
    }

    /**
     * During repeated scale down recommendations, the ReplicasUpdaterService should maintain the maximum # of
     * replicas suggested for each index and scale down to that value.
     * e.g. Recommended: 1, 1, 2, 1, 1, 1 (6th in a row => scale down, but we should scale down to 2 not 1).
     */
    public void testScalesDownToMaxRecommended() {
        int index1Max = 0;
        int index2Max = 0;

        Map<Integer, Set<String>> scaleDownUpdatesToSend = new HashMap<>();
        int repetitionsNeeded = REPLICA_UPDATER_SCALEDOWN_REPETITIONS.getDefault(Settings.EMPTY);
        for (int i = 0; i < repetitionsNeeded; i++) {
            // Randomly generate scale down recommendations for index1 and index2
            Map<Integer, Set<String>> indicesToScaleDown = new HashMap<>();
            if (randomBoolean()) {
                int targetReplicasCount = randomIntBetween(1, 5);
                index1Max = Math.max(targetReplicasCount, index1Max);
                index2Max = Math.max(targetReplicasCount, index2Max);
                indicesToScaleDown.put(targetReplicasCount, Set.of("index1", "index2"));
            } else {
                int targetReplicasCount1 = randomIntBetween(1, 5);
                int targetReplicasCount2 = randomIntBetween(1, 5);
                index1Max = Math.max(targetReplicasCount1, index1Max);
                index2Max = Math.max(targetReplicasCount2, index2Max);
                indicesToScaleDown.put(targetReplicasCount1, Set.of("index1"));
                indicesToScaleDown.put(targetReplicasCount2, Set.of("index2"));
            }

            // For each recommendation, run populateScaleDownUpdates
            for (var entry : indicesToScaleDown.entrySet()) {
                replicasUpdaterService.populateScaleDownUpdates(scaleDownUpdatesToSend, entry.getValue(), entry.getKey());
            }

            if (i < repetitionsNeeded - 1) {
                assertTrue("scaleDownUpdatesToSend should be empty until we see enough repetitions", scaleDownUpdatesToSend.isEmpty());
            }
        }

        // After {repetitionsNeeded} iterations, we should have scaleDownUpdatesToSend
        for (var entry : scaleDownUpdatesToSend.entrySet()) {
            // The numReplicas to update should be the max recommendation seen across all iterations
            int numReplicas = entry.getKey();
            Set<String> indices = entry.getValue();
            if (indices.size() == 2) {
                assertEquals(index1Max, numReplicas);
                assertEquals(index2Max, numReplicas);
            } else {
                if (indices.contains("index1")) {
                    assertEquals(index1Max, numReplicas);
                } else {
                    assertEquals(index2Max, numReplicas);
                }
            }
        }
    }

    public void testScaleUpIsImmediateSP100To250() {

        // disable scheduled task so we can trigger it manually
        replicasUpdaterService.setInterval(TimeValue.timeValueMinutes(60));
        // we need at least 200 SP to fit the very small system index1 in
        int spMin = randomIntBetween(200, 249);
        updateSpMin(spMin);

        Index index1 = new Index("index1", "uuid");
        Index index2 = new Index("index2", "uuid");
        when(searchMetricsService.getIndices()).thenReturn(
            new ConcurrentHashMap<>(
                Map.of(
                    index1,
                    new SearchMetricsService.IndexProperties("index1", 1, 1, false, false, 0),
                    index2,
                    new SearchMetricsService.IndexProperties("index2", 1, 1, false, false, 0)
                )
            )
        );
        when(searchMetricsService.getShardMetrics()).thenReturn(
            new ConcurrentHashMap<>(Map.of(new ShardId(index1, 0), shardMetricOf(2000), new ShardId(index2, 0), shardMetricOf(1000)))
        );
        replicasUpdaterService.performReplicaUpdates();
        mockClient.assertUpdates("SPmin: " + spMin, Map.of(2, Set.of("index1")));
    }

    public void testScaleUpIsImmediateSPGreater250() {
        // disable scheduled task so we can trigger it manually
        replicasUpdaterService.setInterval(TimeValue.timeValueMinutes(60));
        int spMin = randomIntBetween(250, 400);
        updateSpMin(spMin);

        // index with 1 replica should get 2 replica with SPmin >= 250
        Index index1 = new Index("index1", "uuid");
        when(searchMetricsService.getIndices()).thenReturn(
            new ConcurrentHashMap<>(Map.of(index1, new SearchMetricsService.IndexProperties("index1", 1, 1, false, false, 0)))
        );
        when(searchMetricsService.getShardMetrics()).thenReturn(new ConcurrentHashMap<>(Map.of(new ShardId(index1, 0), shardMetricOf(1))));
        replicasUpdaterService.performReplicaUpdates();
        mockClient.assertUpdates("SPmin: " + spMin, Map.of(2, Set.of("index1")));
    }

    /**
     * Check that a missing update resets the repetition counter for scaling down decisions
     */
    public void testNumberOfRepetitionsMissingUpdateClearsCounter() {
        // disable scheduled task so we can trigger it manually
        replicasUpdaterService.setInterval(TimeValue.timeValueMinutes(60));
        int spMin = randomIntBetween(101, 400);
        updateSpMin(spMin);

        Index index1 = new Index("index1", "uuid");
        when(searchMetricsService.getIndices()).thenReturn(
            new ConcurrentHashMap<>(Map.of(index1, new SearchMetricsService.IndexProperties("index1", 1, 2, false, false, 0)))
        );
        when(searchMetricsService.getShardMetrics()).thenReturn(
            new ConcurrentHashMap<>(Map.of(new ShardId(index1, 0), shardMetricOf(0, 1000)))
        );
        final int repetitionsNeeded = REPLICA_UPDATER_SCALEDOWN_REPETITIONS.getDefault(Settings.EMPTY);
        assertNoUpdateAfterRepeats(repetitionsNeeded - 1, this.replicasUpdaterService::performReplicaUpdates);
        // send empty map to reset counter
        when(searchMetricsService.getIndices()).thenReturn(new ConcurrentHashMap<>(Collections.emptyMap()));
        replicasUpdaterService.performReplicaUpdates();
        // and again n-1 times
        when(searchMetricsService.getIndices()).thenReturn(
            new ConcurrentHashMap<>(
                Map.of(new Index("index1", "uuid"), new SearchMetricsService.IndexProperties("index1", 1, 2, false, false, 0))
            )
        );
        assertNoUpdateAfterRepeats(repetitionsNeeded - 1, this.replicasUpdaterService::performReplicaUpdates);
        replicasUpdaterService.performReplicaUpdates();
        mockClient.assertUpdates("SPmin: " + spMin, Map.of(1, Set.of("index1")));
    }

    public void testAutoExpandIndicesScaleUpAndDown() {
        ClusterState initialState = createClusterState(ClusterState.EMPTY_STATE, 3, 3);
        replicasUpdaterService.clusterChanged(new LocalMasterClusterChangedEvent("test", initialState, ClusterState.EMPTY_STATE));
        mockClient.assertNoUpdate();

        // disable scheduled task so we can trigger it manually
        replicasUpdaterService.setInterval(TimeValue.timeValueMinutes(60));
        int spMin = randomIntBetween(250, 1_000_000);
        updateSpMin(spMin);
        mockClient.assertNoUpdate();

        // index with 1 replica should get 2 replica with SPmin >= 250
        Index index1 = new Index("index1", "uuid1");
        Index index2 = new Index("index2", "uuid2");
        when(searchMetricsService.getIndices()).thenReturn(
            new ConcurrentHashMap<>(
                Map.of(
                    index1,
                    new SearchMetricsService.IndexProperties("index1", 1, 1, false, false, 0),
                    index2,
                    new SearchMetricsService.IndexProperties("index2", 1, 1, false, false, 0)
                )
            )
        );
        when(searchMetricsService.getShardMetrics()).thenReturn(
            new ConcurrentHashMap<>(Map.of(new ShardId(index1, 0), shardMetricOf(1), new ShardId(index2, 0), shardMetricOf(1)))
        );
        replicasUpdaterService.performReplicaUpdates();
        mockClient.assertUpdates("SPmin: " + spMin, Map.of(2, Set.of("index1", "index2")));

        replicasUpdaterService.updateAutoExpandReplicaIndices(List.of("index1"));
        mockClient.assertUpdates("SPmin: " + spMin, Map.of(3, Set.of("index1"), 2, Set.of("index2")));

        ClusterState scaleUpState = createClusterState(initialState, 4, 5);
        replicasUpdaterService.clusterChanged(new LocalMasterClusterChangedEvent("test", scaleUpState, initialState));
        mockClient.assertUpdates("SPmin: " + spMin, Map.of(5, Set.of("index1"), 2, Set.of("index2")));

        ClusterState scaleDownState = createClusterState(scaleUpState, 3, 2);
        replicasUpdaterService.clusterChanged(new LocalMasterClusterChangedEvent("test", scaleDownState, scaleUpState));
        mockClient.assertUpdates("SPmin: " + spMin, Map.of(2, Set.of("index1", "index2")));
    }

    public void testAutoExpandIndicesScaleDownIgnoringCounters() {
        ClusterState initialState = createClusterState(ClusterState.EMPTY_STATE, 3, 5);
        replicasUpdaterService.clusterChanged(new LocalMasterClusterChangedEvent("test", initialState, ClusterState.EMPTY_STATE));
        mockClient.assertNoUpdate();

        // disable scheduled task so we can trigger it manually
        replicasUpdaterService.setInterval(TimeValue.timeValueMinutes(60));
        int spMin = randomIntBetween(250, 1_000_000);
        updateSpMin(spMin);
        mockClient.assertNoUpdate();

        Index index1 = new Index("index1", "uuid1");
        Index index2 = new Index("index2", "uuid2");
        replicasUpdaterService.updateAutoExpandReplicaIndices(List.of("index1"));

        // starting with index1 already scaled up at maximum 5 replicas
        when(searchMetricsService.getIndices()).thenReturn(
            new ConcurrentHashMap<>(
                Map.of(
                    index1,
                    new SearchMetricsService.IndexProperties("index1", 1, 5, false, false, 0),
                    index2,
                    new SearchMetricsService.IndexProperties("index2", 1, 2, false, false, 0)
                )
            )
        );
        when(searchMetricsService.getShardMetrics()).thenReturn(
            new ConcurrentHashMap<>(Map.of(new ShardId(index1, 0), shardMetricOf(1), new ShardId(index2, 0), shardMetricOf(1)))
        );
        replicasUpdaterService.performReplicaUpdates();
        // nothing expected to change as index1 is already at max replicas
        mockClient.assertNoUpdate();

        ClusterState scaleDownState = createClusterState(initialState, 3, 3);
        replicasUpdaterService.clusterChanged(new LocalMasterClusterChangedEvent("test", scaleDownState, initialState));
        // we must scale down index1 immediately ignoring the repetition counter
        mockClient.assertUpdates("SPmin: " + spMin, Map.of(3, Set.of("index1")));
    }

    public void testAutoExpandIndicesLowSPMin() {
        replicasUpdaterService.updateAutoExpandReplicaIndices(List.of("index1"));
        mockClient.assertNoUpdate();

        ClusterState initialState = createClusterState(ClusterState.EMPTY_STATE, 3, 3);
        replicasUpdaterService.clusterChanged(new LocalMasterClusterChangedEvent("test", initialState, ClusterState.EMPTY_STATE));
        mockClient.assertNoUpdate();

        // disable scheduled task so we can trigger it manually
        replicasUpdaterService.setInterval(TimeValue.timeValueMinutes(60));
        int spMin = randomIntBetween(5, 249);
        updateSpMin(spMin);
        mockClient.assertNoUpdate();

        // index with 1 replica should get 2 replica with SPmin >= 250
        Index index1 = new Index("index1", "uuid1");
        Index index2 = new Index("index2", "uuid2");
        when(searchMetricsService.getIndices()).thenReturn(
            new ConcurrentHashMap<>(
                Map.of(
                    index1,
                    new SearchMetricsService.IndexProperties("index1", 1, 1, false, false, 0),
                    index2,
                    new SearchMetricsService.IndexProperties("index2", 1, 1, false, false, 0)
                )
            )
        );
        when(searchMetricsService.getShardMetrics()).thenReturn(
            new ConcurrentHashMap<>(Map.of(new ShardId(index1, 0), shardMetricOf(100), new ShardId(index2, 0), shardMetricOf(1)))
        );
        replicasUpdaterService.performReplicaUpdates();
        // either we don't get updates, or if we do, they are unrelated to the number of search nodes, because SPMin is too low
        Map<Integer, Set<String>> updates = mockClient.updates;
        for (Integer numReplicas : updates.keySet()) {
            assertThat(numReplicas, lessThanOrEqualTo(2));
        }
        mockClient.clear();

        ClusterState newState = createClusterState(ClusterState.EMPTY_STATE, 3, 5);
        replicasUpdaterService.clusterChanged(new LocalMasterClusterChangedEvent("test", newState, initialState));
        // either we don't get updates, or if we do, they are unrelated to the number of search nodes, because SPMin is too low
        updates = mockClient.updates;
        for (Integer numReplicas : updates.keySet()) {
            assertThat(numReplicas, lessThanOrEqualTo(2));
        }
        mockClient.clear();
    }

    public void testAutoExpandIndicesServiceDisabled() {
        replicasUpdaterService.updateEnableReplicasForInstantFailover(false);

        replicasUpdaterService.updateAutoExpandReplicaIndices(List.of("index1"));
        mockClient.assertNoUpdate();

        ClusterState initialState = createClusterState(ClusterState.EMPTY_STATE, 3, 3);
        replicasUpdaterService.clusterChanged(new LocalMasterClusterChangedEvent("test", initialState, ClusterState.EMPTY_STATE));
        mockClient.assertNoUpdate();

        // disable scheduled task so we can trigger it manually
        replicasUpdaterService.setInterval(TimeValue.timeValueMinutes(60));
        int spMin = randomIntBetween(250, 1_000_000);
        updateSpMin(spMin);
        mockClient.assertNoUpdate();

        // index with 1 replica should get 2 replica with SPmin >= 250
        Index index1 = new Index("index1", "uuid1");
        Index index2 = new Index("index2", "uuid2");
        when(searchMetricsService.getIndices()).thenReturn(
            new ConcurrentHashMap<>(
                Map.of(
                    index1,
                    new SearchMetricsService.IndexProperties("index1", 1, 1, false, false, 0),
                    index2,
                    new SearchMetricsService.IndexProperties("index2", 1, 1, false, false, 0)
                )
            )
        );
        when(searchMetricsService.getShardMetrics()).thenReturn(
            new ConcurrentHashMap<>(Map.of(new ShardId(index1, 0), shardMetricOf(100), new ShardId(index2, 0), shardMetricOf(1)))
        );
        mockClient.assertNoUpdate();

        replicasUpdaterService.updateAutoExpandReplicaIndices(List.of("index1", "index2"));
        mockClient.assertNoUpdate();

        replicasUpdaterService.updateEnableReplicasForInstantFailover(true);
        replicasUpdaterService.performReplicaUpdates();
        mockClient.assertUpdates("SPmin: " + spMin, Map.of(3, Set.of("index1", "index2")));

        when(searchMetricsService.getIndices()).thenReturn(
            new ConcurrentHashMap<>(
                Map.of(
                    index1,
                    new SearchMetricsService.IndexProperties("index1", 1, 3, false, false, 0),
                    index2,
                    new SearchMetricsService.IndexProperties("index2", 1, 3, false, false, 0)
                )
            )
        );
        when(searchMetricsService.getShardMetrics()).thenReturn(
            new ConcurrentHashMap<>(Map.of(new ShardId(index1, 0), shardMetricOf(100), new ShardId(index2, 0), shardMetricOf(1)))
        );

        replicasUpdaterService.updatedEnableReplicasLoadBalancing(false);
        replicasUpdaterService.updateEnableReplicasForInstantFailover(false);
        replicasUpdaterService.performReplicaUpdates();
        mockClient.assertUpdates("SPmin: " + spMin, Map.of(1, Set.of("index1", "index2")));
    }

    public void testEnforceTopologyBoundsWhenTopologyAvailable() {
        boolean[] runWithOnlyTopologyBoundsValue = new boolean[] { false };
        ReplicasUpdaterExecutionListener listener = new ReplicasUpdaterExecutionListener.NoopExecutionListener() {
            @Override
            public void onRunStart(boolean immediateScaleDown, boolean onlyScaleDownToTopologyBounds) {
                super.onRunStart(immediateScaleDown, onlyScaleDownToTopologyBounds);
                runWithOnlyTopologyBoundsValue[0] = onlyScaleDownToTopologyBounds;
            }
        };
        NoOpNodeClient client = new NoOpNodeClient(testThreadPool);
        ClusterService clusterService = createClusterService(testThreadPool, createClusterSettings());
        ReplicasLoadBalancingScaler replicasLoadBalancingScaler = new ReplicasLoadBalancingScaler(clusterService, client);
        replicasUpdaterService = new ReplicasUpdaterService(
            testThreadPool,
            clusterService,
            client,
            searchMetricsService,
            replicasLoadBalancingScaler,
            new DesiredTopologyContext(ClusterServiceUtils.createClusterService(this.testThreadPool)),
            listener,
            TelemetryProvider.NOOP.getMeterRegistry()
        );
        replicasUpdaterService.init();
        replicasUpdaterService.setInterval(TimeValue.timeValueMinutes(600));

        DesiredClusterTopology desiredClusterTopology = randomDesiredClusterTopology();
        replicasUpdaterService.onDesiredTopologyAvailable(desiredClusterTopology);

        assertThat(runWithOnlyTopologyBoundsValue[0], is(true));
    }

    public void testUseDesiredTopologyForAutoexpandIndicesWhenAvailable() {
        ClusterState initialState = createClusterState(ClusterState.EMPTY_STATE, 3, 6);
        replicasUpdaterService.clusterChanged(new LocalMasterClusterChangedEvent("test", initialState, ClusterState.EMPTY_STATE));
        mockClient.assertNoUpdate();

        replicasUpdaterService.setInterval(TimeValue.timeValueMinutes(60));
        int spMin = randomIntBetween(250, 1_000_000);
        updateSpMin(spMin);
        mockClient.assertNoUpdate();

        Index index1 = new Index("index1", "uuid1");
        Index index2 = new Index("index2", "uuid2");
        when(searchMetricsService.getIndices()).thenReturn(
            new ConcurrentHashMap<>(
                Map.of(
                    index1,
                    new SearchMetricsService.IndexProperties(index1.getName(), 1, 1, false, false, 0),
                    index2,
                    new SearchMetricsService.IndexProperties(index2.getName(), 1, 1, false, false, 0)
                )
            )
        );
        when(searchMetricsService.getShardMetrics()).thenReturn(
            new ConcurrentHashMap<>(Map.of(new ShardId(index1, 0), shardMetricOf(1000), new ShardId(index2, 0), shardMetricOf(1000)))
        );

        replicasUpdaterService.updateAutoExpandReplicaIndices(List.of("index1", "index2"));
        // auto expanded to 6 replicas (as we have 6 search nodes)
        mockClient.assertUpdates("SPmin: " + spMin, Map.of(6, Set.of("index1", "index2")));

        // receive a desired topology with 3 search nodes
        DesiredClusterTopology desiredTopology = new DesiredClusterTopology(
            new DesiredClusterTopology.TierTopology(
                3,
                randomLongBetween(1, 8_589_934_592L) + "b",
                randomFloat(),
                randomFloat(),
                randomFloat()
            ),
            new DesiredClusterTopology.TierTopology(
                randomIntBetween(1, 10),
                randomLongBetween(1, 8_589_934_592L) + "b",
                randomFloat(),
                randomFloat(),
                randomFloat()
            )
        );
        // receiving a topology in the context will trigger a run as the replicas updater service is a listener to these events
        desiredTopologyContext.updateDesiredClusterTopology(desiredTopology);
        // indices should be scaled to match the desired topology (3 replicas) not the cluster state (5 replicas)
        mockClient.assertUpdates("SPmin: " + spMin, Map.of(3, Set.of("index1", "index2")));

        // update cluster state to have more search nodes, but desired topology remains at 3
        ClusterState scaleUpState = createClusterState(initialState, 3, 7);
        replicasUpdaterService.clusterChanged(new LocalMasterClusterChangedEvent("test", scaleUpState, initialState));
        mockClient.assertUpdates("SPmin: " + spMin, Map.of(3, Set.of("index1", "index2")));
    }

    public void testReplicaLoadBalancingAndInstantFailoverIntegration() {
        ClusterService clusterService = createClusterService(testThreadPool, createClusterSettings());
        // rolling our own sleeves for stubbing mechanism for load balancing results (avoiding mockito here and having the ability to avoid
        // client mocking to fetch indices stats. the scaler itself is tested in its own unit tests, we're interested in the integration of
        // both instant failover and load balancing here)
        final LinkedList<ReplicasLoadBalancingResult> results = new LinkedList<>();
        Supplier<ReplicasLoadBalancingResult> stubLoadBalancingResult = results::pop;
        ReplicasLoadBalancingScaler replicasLoadBalancingScaler = new ReplicasLoadBalancingScaler(
            clusterService,
            new NoOpClient(testThreadPool)
        ) {
            @Override
            public void getRecommendedReplicas(
                ClusterState state,
                ReplicaRankingContext rankingContext,
                DesiredClusterTopology desiredClusterTopology,
                boolean onlyScaleDownToTopologyBounds,
                ActionListener<ReplicasLoadBalancingResult> listener
            ) {
                listener.onResponse(stubLoadBalancingResult.get());
            }
        };

        ReplicasUpdaterService service = new ReplicasUpdaterService(
            testThreadPool,
            clusterService,
            mockClient,
            searchMetricsService,
            replicasLoadBalancingScaler,
            desiredTopologyContext,
            TelemetryProvider.NOOP.getMeterRegistry()
        );
        service.init();
        service.setInterval(TimeValue.timeValueMinutes(60));
        service.updateEnableReplicasForInstantFailover(true);
        replicasLoadBalancingScaler.updateEnableReplicasForLoadBalancing(true);
        // spmin is 100 by default
        results.offer(EMPTY_RESULT);
        service.onMaster();

        int spMin = randomIntBetween(250, 1_000_000);
        updateSpMin(spMin);
        mockClient.assertNoUpdate();

        Index index1 = new Index("index1", "uuid1");
        Index index2 = new Index("index2", "uuid2");

        // note that all scenarios START with both indices at 1 replica
        when(searchMetricsService.getIndices()).thenReturn(
            new ConcurrentHashMap<>(
                Map.of(
                    index1,
                    new SearchMetricsService.IndexProperties("index1", 1, 1, false, false, 0),
                    index2,
                    new SearchMetricsService.IndexProperties("index2", 1, 1, false, false, 0)
                )
            )
        );
        when(searchMetricsService.getShardMetrics()).thenReturn(
            new ConcurrentHashMap<>(Map.of(new ShardId(index1, 0), shardMetricOf(2000), new ShardId(index2, 0), shardMetricOf(1000)))
        );

        {
            // both systems are active but indices have no search load and as the indices already have 1 replica the load balancing scaler
            // returns an empty results
            results.offer(new ReplicasLoadBalancingResult(Map.of(), new TreeMap<>(), randomInt(100)));

            service.performReplicaUpdates();
            mockClient.assertUpdates("SPmin: " + spMin, Map.of(2, Set.of("index1", "index2")));
        }

        {
            // now simulate that index1 has high search load and index2 low search load
            // we expect index1 to get 4 replicas from load balancing scaler and index2 to get 2 replicas from instant failover
            assert results.isEmpty() : "we should've consumed the previous result";
            results.offer(new ReplicasLoadBalancingResult(Map.of(), new TreeMap<>(Map.of("index1", 4)), randomInt(100)));
            service.performReplicaUpdates();

            mockClient.assertUpdates("SPmin: " + spMin, Map.of(4, Set.of("index1"), 2, Set.of("index2")));
        }

        {
            // disabling instant failover whilst index1 has high search load so it gets 4 replicas from the load balancing scaler
            // however index2 should now remain on 1 replica
            assert results.isEmpty() : "we should've consumed the previous result";
            results.offer(new ReplicasLoadBalancingResult(Map.of(), new TreeMap<>(Map.of("index1", 4)), randomInt(100)));
            service.updateEnableReplicasForInstantFailover(false);
            service.performReplicaUpdates();

            mockClient.assertUpdates("SPmin: " + spMin, new TreeMap<>(Map.of(4, Set.of("index1"))));
            // reenable instant failover
            service.updateEnableReplicasForInstantFailover(true);
        }

        {
            // disable replicas for load balancing so instant failover gives both indices 2 replicas (SPmin >= 250)
            assert results.isEmpty() : "we should've consumed the previous result";
            results.offer(EMPTY_RESULT);
            replicasLoadBalancingScaler.updateEnableReplicasForLoadBalancing(false);
            service.performReplicaUpdates();

            mockClient.assertUpdates("SPmin: " + spMin, Map.of(2, Set.of("index1", "index2")));
            // reenable load balancing
            replicasLoadBalancingScaler.updateEnableReplicasForLoadBalancing(true);
        }
    }

    public void testImmediateReplicasScaleDownDueToObeyTopologyBounds() {
        ClusterService clusterService = createClusterService(testThreadPool, createClusterSettings());
        final LinkedList<ReplicasLoadBalancingResult> results = new LinkedList<>();
        Supplier<ReplicasLoadBalancingResult> stubLoadBalancingResult = results::pop;
        ReplicasLoadBalancingScaler replicasLoadBalancingScaler = new ReplicasLoadBalancingScaler(
            clusterService,
            new NoOpClient(testThreadPool)
        ) {
            @Override
            public void getRecommendedReplicas(
                ClusterState state,
                ReplicaRankingContext rankingContext,
                DesiredClusterTopology desiredClusterTopology,
                boolean onlyScaleDownToTopologyBounds,
                ActionListener<ReplicasLoadBalancingResult> listener
            ) {
                listener.onResponse(stubLoadBalancingResult.get());
            }
        };

        ReplicasUpdaterService service = new ReplicasUpdaterService(
            testThreadPool,
            clusterService,
            mockClient,
            searchMetricsService,
            replicasLoadBalancingScaler,
            desiredTopologyContext,
            TelemetryProvider.NOOP.getMeterRegistry()
        );
        service.init();
        service.setInterval(TimeValue.timeValueMinutes(60));
        service.updateEnableReplicasForInstantFailover(true);
        replicasLoadBalancingScaler.updateEnableReplicasForLoadBalancing(true);
        // spmin is 100 by default
        results.offer(EMPTY_RESULT);
        service.onMaster();

        int spMin = randomIntBetween(250, 1_000_000);
        updateSpMin(spMin);
        mockClient.assertNoUpdate();

        Index index1 = new Index("index1", "uuid1");
        Index index2 = new Index("index2", "uuid2");

        when(searchMetricsService.getIndices()).thenReturn(
            new ConcurrentHashMap<>(
                Map.of(
                    index1,
                    new SearchMetricsService.IndexProperties("index1", 1, 5, false, false, 0),
                    index2,
                    new SearchMetricsService.IndexProperties("index2", 1, 3, false, false, 0)
                )
            )
        );
        when(searchMetricsService.getShardMetrics()).thenReturn(
            new ConcurrentHashMap<>(Map.of(new ShardId(index1, 0), shardMetricOf(2000), new ShardId(index2, 0), shardMetricOf(1000)))
        );

        {
            // both systems are active, the repetition counters are empty, replicas load balancing reports that index1 must immediately go
            // down to 3 replicas, so this must happen regardless of the scale down counters
            results.offer(new ReplicasLoadBalancingResult(Map.of("index1", 3), new TreeMap<>(), randomInt(100)));

            service.performReplicaUpdates();
            mockClient.assertUpdates("SPmin: " + spMin, Map.of(3, Set.of("index1")));
        }

        {
            // same as before, except let's make sure we're adding a couple of counts to the scale down for index1 before the immediate
            // request from the replicas for load balancing (we're doing that by reporting non interactive only data, which, even at
            // SPmin>=250 should scale the index down, but only after 6 repetitions)
            when(searchMetricsService.getShardMetrics()).thenReturn(
                new ConcurrentHashMap<>(
                    Map.of(new ShardId(index1, 0), shardMetricOf(0, 20000), new ShardId(index2, 0), shardMetricOf(1000))
                )
            );
            results.offer(new ReplicasLoadBalancingResult(Map.of(), new TreeMap<>(), randomInt(100)));
            service.performReplicaUpdates();
            results.offer(new ReplicasLoadBalancingResult(Map.of(), new TreeMap<>(), randomInt(100)));
            service.performReplicaUpdates();
            // we should've recorded 2 ticks in the scale down counters
            mockClient.assertNoUpdate();

            results.offer(new ReplicasLoadBalancingResult(Map.of("index1", 3), new TreeMap<>(), randomInt(100)));
            service.performReplicaUpdates();
            mockClient.assertUpdates("SPmin: " + spMin, Map.of(3, Set.of("index1")));
        }
    }

    public void testReplicasForInstantFailoverRunsIfLoadBalancingFails() {
        // instant failover should give indices 2 replicas even if load balancing fails
        ClusterService clusterService = createClusterService(testThreadPool, createClusterSettings());
        ReplicasLoadBalancingScaler replicasLoadBalancingScaler = new ReplicasLoadBalancingScaler(
            clusterService,
            new NoOpClient(testThreadPool)
        ) {
            @Override
            public void getRecommendedReplicas(
                ClusterState state,
                ReplicaRankingContext rankingContext,
                DesiredClusterTopology desiredClusterTopology,
                boolean onlyScaleDownToTopologyBounds,
                ActionListener<ReplicasLoadBalancingResult> listener
            ) {
                listener.onFailure(new IllegalStateException("oh no"));
            }
        };

        ReplicasUpdaterService service = new ReplicasUpdaterService(
            testThreadPool,
            clusterService,
            mockClient,
            searchMetricsService,
            replicasLoadBalancingScaler,
            desiredTopologyContext,
            TelemetryProvider.NOOP.getMeterRegistry()
        );
        service.init();
        service.setInterval(TimeValue.timeValueMinutes(60));
        service.updateEnableReplicasForInstantFailover(true);
        replicasLoadBalancingScaler.updateEnableReplicasForLoadBalancing(true);
        service.onMaster();

        int spMin = randomIntBetween(250, 1_000_000);
        updateSpMin(spMin);
        mockClient.assertNoUpdate();

        Index index1 = new Index("index1", "uuid1");
        Index index2 = new Index("index2", "uuid2");

        when(searchMetricsService.getIndices()).thenReturn(
            new ConcurrentHashMap<>(
                Map.of(
                    index1,
                    new SearchMetricsService.IndexProperties("index1", 1, 5, false, false, 0),
                    index2,
                    new SearchMetricsService.IndexProperties("index2", 1, 3, false, false, 0)
                )
            )
        );
        when(searchMetricsService.getShardMetrics()).thenReturn(
            new ConcurrentHashMap<>(Map.of(new ShardId(index1, 0), shardMetricOf(2000), new ShardId(index2, 0), shardMetricOf(1000)))
        );

        int repetitionsNeeded = REPLICA_UPDATER_SCALEDOWN_REPETITIONS.getDefault(Settings.EMPTY);
        assertNoUpdateAfterRepeats(repetitionsNeeded - 1, service::performReplicaUpdates);
        service.performReplicaUpdates();
        mockClient.assertUpdates("SPmin: " + spMin, Map.of(2, Set.of("index1", "index2")));
    }

    public void testReplicaCountMetrics() {
        ClusterState initialState = createClusterState(ClusterState.EMPTY_STATE, 3, 5);
        replicasUpdaterService.clusterChanged(new LocalMasterClusterChangedEvent("test", initialState, ClusterState.EMPTY_STATE));

        // disable scheduled task so we can trigger it manually
        replicasUpdaterService.setInterval(TimeValue.timeValueMinutes(60));
        int spMin = randomIntBetween(250, 1_000_000);
        updateSpMin(spMin);
        mockClient.assertNoUpdate();

        Index index1 = new Index("index1", "uuid1");
        Index index2 = new Index("index2", "uuid2");
        when(searchMetricsService.getIndices()).thenReturn(
            new ConcurrentHashMap<>(
                Map.of(
                    index1,
                    new SearchMetricsService.IndexProperties("index1", 1, 1, false, false, 0),
                    index2,
                    new SearchMetricsService.IndexProperties("index2", 1, 1, false, false, 0)
                )
            )
        );
        when(searchMetricsService.getShardMetrics()).thenReturn(
            new ConcurrentHashMap<>(Map.of(new ShardId(index1, 0), shardMetricOf(1), new ShardId(index2, 0), shardMetricOf(1)))
        );

        // Instant failover scale up
        // index1: 1 -> 2 replicas
        // index2: 1 -> 2 replicas
        {
            replicasUpdaterService.performReplicaUpdates();
            mockClient.assertUpdates("SPmin: " + spMin, Map.of(2, Set.of("index1", "index2")));
            recordingMeterRegistry.getRecorder().collect();

            Measurement currentReplicasMeasurement = recordingMeterRegistry.getRecorder()
                .getMeasurements(InstrumentType.LONG_GAUGE, "es.autoscaling.search.total_replicas.current")
                .getLast();
            Measurement desiredReplicasMeasurement = recordingMeterRegistry.getRecorder()
                .getMeasurements(InstrumentType.LONG_GAUGE, "es.autoscaling.search.total_desired_replicas.current")
                .getLast();
            assertEquals(2, currentReplicasMeasurement.getLong());
            assertEquals(4, desiredReplicasMeasurement.getLong());

            List<Measurement> increaseHistogram = recordingMeterRegistry.getRecorder()
                .getMeasurements(InstrumentType.LONG_HISTOGRAM, "es.autoscaling.search.replica_increase.histogram");
            List<Measurement> decreaseHistogram = recordingMeterRegistry.getRecorder()
                .getMeasurements(InstrumentType.LONG_HISTOGRAM, "es.autoscaling.search.replica_decrease.histogram");
            assertEquals(2, increaseHistogram.size());
            assertThat(increaseHistogram.stream().map(Measurement::getLong).sorted().toList(), equalTo(List.of(1L, 1L)));
            assertTrue(decreaseHistogram.isEmpty());
        }
        // The mockClient doesn't actually do the update, so let's set 2 replicas as the new state
        when(searchMetricsService.getIndices()).thenReturn(
            new ConcurrentHashMap<>(
                Map.of(
                    index1,
                    new SearchMetricsService.IndexProperties("index1", 1, 2, false, false, 0),
                    index2,
                    new SearchMetricsService.IndexProperties("index2", 1, 2, false, false, 0)
                )
            )
        );

        // Auto-expand scale up
        // index1: 2 -> 5 replicas
        // index2: no change
        {
            replicasUpdaterService.updateAutoExpandReplicaIndices(List.of("index1"));
            mockClient.assertUpdates("SPmin: " + spMin, Map.of(5, Set.of("index1")));
            recordingMeterRegistry.getRecorder().collect();

            Measurement currentReplicasMeasurement = recordingMeterRegistry.getRecorder()
                .getMeasurements(InstrumentType.LONG_GAUGE, "es.autoscaling.search.total_replicas.current")
                .getLast();
            Measurement desiredReplicasMeasurement = recordingMeterRegistry.getRecorder()
                .getMeasurements(InstrumentType.LONG_GAUGE, "es.autoscaling.search.total_desired_replicas.current")
                .getLast();
            assertEquals(4, currentReplicasMeasurement.getLong());
            assertEquals(7, desiredReplicasMeasurement.getLong());

            List<Measurement> increaseHistogram = recordingMeterRegistry.getRecorder()
                .getMeasurements(InstrumentType.LONG_HISTOGRAM, "es.autoscaling.search.replica_increase.histogram");
            List<Measurement> decreaseHistogram = recordingMeterRegistry.getRecorder()
                .getMeasurements(InstrumentType.LONG_HISTOGRAM, "es.autoscaling.search.replica_decrease.histogram");
            assertEquals(3, increaseHistogram.size());
            assertThat(increaseHistogram.stream().map(Measurement::getLong).sorted().toList(), equalTo(List.of(1L, 1L, 3L)));
            assertTrue(decreaseHistogram.isEmpty());
        }
        when(searchMetricsService.getIndices()).thenReturn(
            new ConcurrentHashMap<>(
                Map.of(
                    index1,
                    new SearchMetricsService.IndexProperties("index1", 1, 5, false, false, 0),
                    index2,
                    new SearchMetricsService.IndexProperties("index2", 1, 2, false, false, 0)
                )
            )
        );

        // Auto-expand scale down
        // index1: 5 -> 2 replicas
        // index2: no change
        {
            ClusterState scaleDownState = createClusterState(initialState, 3, 2);
            replicasUpdaterService.clusterChanged(new LocalMasterClusterChangedEvent("test", scaleDownState, initialState));
            mockClient.assertUpdates("SPmin: " + spMin, Map.of(2, Set.of("index1")));
            recordingMeterRegistry.getRecorder().collect();

            Measurement currentReplicasMeasurement = recordingMeterRegistry.getRecorder()
                .getMeasurements(InstrumentType.LONG_GAUGE, "es.autoscaling.search.total_replicas.current")
                .getLast();
            Measurement desiredReplicasMeasurement = recordingMeterRegistry.getRecorder()
                .getMeasurements(InstrumentType.LONG_GAUGE, "es.autoscaling.search.total_desired_replicas.current")
                .getLast();
            assertEquals(7, currentReplicasMeasurement.getLong());
            assertEquals(4, desiredReplicasMeasurement.getLong());

            List<Measurement> increaseHistogram = recordingMeterRegistry.getRecorder()
                .getMeasurements(InstrumentType.LONG_HISTOGRAM, "es.autoscaling.search.replica_increase.histogram");
            List<Measurement> decreaseHistogram = recordingMeterRegistry.getRecorder()
                .getMeasurements(InstrumentType.LONG_HISTOGRAM, "es.autoscaling.search.replica_decrease.histogram");
            assertEquals(3, increaseHistogram.size());
            assertThat(increaseHistogram.stream().map(Measurement::getLong).sorted().toList(), equalTo(List.of(1L, 1L, 3L)));
            assertEquals(1, decreaseHistogram.size());
            assertThat(decreaseHistogram.getFirst().getLong(), equalTo(3L));
        }
        when(searchMetricsService.getIndices()).thenReturn(
            new ConcurrentHashMap<>(
                Map.of(
                    index1,
                    new SearchMetricsService.IndexProperties("index1", 1, 2, false, false, 0),
                    index2,
                    new SearchMetricsService.IndexProperties("index2", 1, 2, false, false, 0)
                )
            )
        );

        // Systems disabled scale down
        // index1: 2 -> 1 replica
        // index2: 2 -> 1 replica
        {
            replicasUpdaterService.updateEnableReplicasForInstantFailover(false);
            replicasUpdaterService.updatedEnableReplicasLoadBalancing(false);
            replicasUpdaterService.performReplicaUpdates();
            mockClient.assertUpdates("SPmin: " + spMin, Map.of(1, Set.of("index1", "index2")));
            recordingMeterRegistry.getRecorder().collect();

            Measurement currentReplicasMeasurement = recordingMeterRegistry.getRecorder()
                .getMeasurements(InstrumentType.LONG_GAUGE, "es.autoscaling.search.total_replicas.current")
                .getLast();
            Measurement desiredReplicasMeasurement = recordingMeterRegistry.getRecorder()
                .getMeasurements(InstrumentType.LONG_GAUGE, "es.autoscaling.search.total_desired_replicas.current")
                .getLast();
            assertEquals(4, currentReplicasMeasurement.getLong());
            assertEquals(2, desiredReplicasMeasurement.getLong());

            List<Measurement> increaseHistogram = recordingMeterRegistry.getRecorder()
                .getMeasurements(InstrumentType.LONG_HISTOGRAM, "es.autoscaling.search.replica_increase.histogram");
            List<Measurement> decreaseHistogram = recordingMeterRegistry.getRecorder()
                .getMeasurements(InstrumentType.LONG_HISTOGRAM, "es.autoscaling.search.replica_decrease.histogram");
            assertEquals(3, increaseHistogram.size());
            assertThat(increaseHistogram.stream().map(Measurement::getLong).sorted().toList(), equalTo(List.of(1L, 1L, 3L)));
            assertEquals(3, decreaseHistogram.size());
            assertThat(increaseHistogram.stream().map(Measurement::getLong).sorted().toList(), equalTo(List.of(1L, 1L, 3L)));
        }
    }

    public void testIndicesBlockedFromScalingUpMetrics() {
        ClusterService clusterService = createClusterService(testThreadPool, createClusterSettings());
        final LinkedList<ReplicasLoadBalancingResult> results = new LinkedList<>();
        Supplier<ReplicasLoadBalancingResult> stubLoadBalancingResult = results::pop;
        ReplicasLoadBalancingScaler replicasLoadBalancingScaler = new ReplicasLoadBalancingScaler(
            clusterService,
            new NoOpClient(testThreadPool)
        ) {
            @Override
            public void getRecommendedReplicas(
                ClusterState state,
                ReplicaRankingContext rankingContext,
                DesiredClusterTopology desiredClusterTopology,
                boolean onlyScaleDownToTopologyBounds,
                ActionListener<ReplicasLoadBalancingResult> listener
            ) {
                listener.onResponse(stubLoadBalancingResult.get());
            }
        };
        recordingMeterRegistry = new RecordingMeterRegistry();
        ReplicasUpdaterService service = new ReplicasUpdaterService(
            testThreadPool,
            clusterService,
            mockClient,
            searchMetricsService,
            replicasLoadBalancingScaler,
            desiredTopologyContext,
            recordingMeterRegistry
        );
        service.init();
        service.setInterval(TimeValue.timeValueMinutes(60));
        replicasLoadBalancingScaler.updateEnableReplicasForLoadBalancing(true);
        results.offer(EMPTY_RESULT);
        service.onMaster();

        // Recommend no changes, with two indices marked as blocked from scaling up
        results.offer(new ReplicasLoadBalancingResult(Map.of(), new TreeMap<>(Map.of("index1", 1, "index2", 1)), 2));
        service.performReplicaUpdates();
        mockClient.assertNoUpdate();

        recordingMeterRegistry.getRecorder().collect();
        Measurement indicesBlockedScalingUp = recordingMeterRegistry.getRecorder()
            .getMeasurements(InstrumentType.LONG_GAUGE, "es.autoscaling.search.indices_blocked_scaling_up.current")
            .getLast();
        assertEquals(2, indicesBlockedScalingUp.getLong());
    }

    private static ClusterState createClusterState(ClusterState previousState, int numIndexNodes, int numSearchNodes) {
        DiscoveryNodes.Builder builder = DiscoveryNodes.builder();
        for (int i = 0; i < numIndexNodes; i++) {
            builder.add(DiscoveryNodeUtils.builder(Integer.toString(i)).roles(Set.of(DiscoveryNodeRole.INDEX_ROLE)).build());
        }
        for (int i = numIndexNodes; i < numSearchNodes + numIndexNodes; i++) {
            builder.add(DiscoveryNodeUtils.builder(Integer.toString(i)).roles(Set.of(DiscoveryNodeRole.SEARCH_ROLE)).build());
        }
        return ClusterState.builder(previousState).nodes(builder.build()).build();
    }

    static ShardMetrics shardMetricOf(long interactiveSize) {
        return shardMetricOf(interactiveSize, 0);
    }

    private static ShardMetrics shardMetricOf(long interactiveSize, long nonInteractiveSize) {
        ShardMetrics sm = new ShardMetrics();
        sm.shardSize = new ShardSize(interactiveSize, nonInteractiveSize, 0, 0);
        return sm;
    }

    private void updateSpMin(int spMin) {
        this.searchMetricsService.updateSearchPowerMin(spMin);
        this.replicasUpdaterService.updateSearchPowerMin(spMin);
    }

    private void assertNoUpdateAfterRepeats(int repetitions, Runnable call) {
        repeatNTimes(repetitions, () -> {
            call.run();
            mockClient.assertNoUpdate();
        });
    }

    private void repeatNTimes(int repetitions, Runnable call) {
        for (int i = 0; i < repetitions; i++) {
            call.run();
        }
    }

    private static IndexMetadata createIndex(int shards) {
        return createIndex(shards, 1, null);
    }

    private static IndexMetadata createSystemIndex() {
        Settings indexSettings = indexSettings(1, 1).put("index.version.created", Version.CURRENT).build();
        return IndexMetadata.builder(randomIdentifier()).system(true).settings(indexSettings).build();
    }

    private static IndexMetadata createIndex(int shards, int replicas, Metadata.Builder clusterMetadataBuilder) {
        IndexMetadata indexMetadata = IndexMetadata.builder(randomIdentifier())
            .settings(indexSettings(shards, replicas).put("index.version.created", Version.CURRENT))
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
        shardSizes.put(new ShardId(index.getIndex(), shardId), new ShardSize(interactiveSize, nonInteractiveSize, 1, 1));
    }

    private static ClusterSettings createClusterSettings() {
        Set<Setting<?>> defaultClusterSettings = Sets.addToCopy(
            ClusterSettings.BUILT_IN_CLUSTER_SETTINGS,
            ReplicasUpdaterService.REPLICA_UPDATER_INTERVAL,
            ReplicasUpdaterService.REPLICA_UPDATER_SCALEDOWN_REPETITIONS,
            ReplicasScalerCacheBudget.REPLICA_CACHE_BUDGET_RATIO,
            ReplicasUpdaterService.AUTO_EXPAND_REPLICA_INDICES,
            SearchMetricsService.ACCURATE_METRICS_WINDOW_SETTING,
            SearchMetricsService.STALE_METRICS_CHECK_INTERVAL_SETTING,
            ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING,
            ServerlessSharedSettings.SEARCH_POWER_MAX_SETTING,
            ServerlessSharedSettings.ENABLE_REPLICAS_FOR_INSTANT_FAILOVER,
            ServerlessSharedSettings.ENABLE_REPLICAS_LOAD_BALANCING,
            ReplicasLoadBalancingScaler.MAX_REPLICA_RELATIVE_SEARCH_LOAD
        );
        return new ClusterSettings(
            Settings.builder().put(ServerlessSharedSettings.ENABLE_REPLICAS_FOR_INSTANT_FAILOVER.getKey(), true).build(),
            defaultClusterSettings
        );
    }

    private static DiscoveryNodes createNodes() {
        var builder = DiscoveryNodes.builder();
        builder.masterNodeId("master").localNodeId("master");
        builder.add(DiscoveryNodeUtils.builder("master").roles(Set.of(DiscoveryNodeRole.MASTER_ROLE)).build());
        builder.add(DiscoveryNodeUtils.builder("index_node_1").roles(Set.of(DiscoveryNodeRole.INDEX_ROLE)).build());
        builder.add(DiscoveryNodeUtils.builder("search_node_1").roles(Set.of(DiscoveryNodeRole.SEARCH_ROLE)).build());
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
        searchMetricsService.clusterChanged(new LocalMasterClusterChangedEvent("test", newState, previousState));
        return newState;
    }

    private static class LocalMasterClusterChangedEvent extends ClusterChangedEvent {
        LocalMasterClusterChangedEvent(String source, ClusterState state, ClusterState previousState) {
            super(source, state, previousState);
        }

        @Override
        public boolean localNodeMaster() {
            return true;
        }
    }

    private static class MockClient extends NoOpNodeClient {
        Map<Integer, Set<String>> updates = new HashMap<>();
        boolean updateSettingsToBeVerified = false;

        final AtomicInteger executionCount = new AtomicInteger(0);

        MockClient() {
            super(null);
        }

        @Override
        public <Request extends ActionRequest, Response extends ActionResponse> Task executeLocally(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            assertEquals(TransportUpdateReplicasAction.TYPE, action);
            assertThat(request, instanceOf(TransportUpdateReplicasAction.Request.class));
            TransportUpdateReplicasAction.Request updateReplicasRequest = (TransportUpdateReplicasAction.Request) request;
            int numReplicas = updateReplicasRequest.getNumReplicas();
            Set<String> put = updates.put(numReplicas, Set.of(updateReplicasRequest.indices()));
            // allow for more executions to update the map, but without any overrides for the same key
            assertNull(put);
            executionCount.incrementAndGet();
            updateSettingsToBeVerified = true;
            return super.executeLocally(action, request, listener);
        }

        void assertUpdates(String message, Map<Integer, Set<String>> expectedUpdates) {
            assertTrue("update settings not yet called", updateSettingsToBeVerified);
            assertEquals(message, expectedUpdates, updates);
            assertThat(executionCount.get(), greaterThanOrEqualTo(1));
            clear();
        }

        void clear() {
            updates.clear();
            updateSettingsToBeVerified = false;
            this.executionCount.set(0);
        }

        void assertNoUpdate() {
            assertFalse(updates.toString(), updateSettingsToBeVerified);
        }
    }
}
