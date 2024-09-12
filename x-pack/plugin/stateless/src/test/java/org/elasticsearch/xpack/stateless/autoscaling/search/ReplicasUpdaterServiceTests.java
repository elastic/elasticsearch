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
import co.elastic.elasticsearch.stateless.api.ShardSizeStatsReader.ShardSize;
import co.elastic.elasticsearch.stateless.autoscaling.MetricQuality;
import co.elastic.elasticsearch.stateless.autoscaling.memory.MemoryMetrics;
import co.elastic.elasticsearch.stateless.autoscaling.memory.MemoryMetricsService;
import co.elastic.elasticsearch.stateless.autoscaling.search.SearchMetricsService.ShardMetrics;

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
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpNodeClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.junit.After;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static co.elastic.elasticsearch.stateless.autoscaling.search.ReplicasUpdaterService.REPLICA_UPDATER_SCALEDOWN_REPETITIONS;
import static co.elastic.elasticsearch.stateless.autoscaling.search.ReplicasUpdaterService.SEARCH_POWER_MIN_FULL_REPLICATION;
import static co.elastic.elasticsearch.stateless.autoscaling.search.ReplicasUpdaterService.SEARCH_POWER_MIN_NO_REPLICATION;
import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.newInstance;
import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class ReplicasUpdaterServiceTests extends ESTestCase {

    private SearchMetricsService searchMetricsService;
    private ReplicasUpdaterService replicasUpdaterService;
    private TestThreadPool testThreadPool;
    private MockClient mockClient;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        AtomicLong currentRelativeTimeInNanos = new AtomicLong(1L);
        MemoryMetricsService memoryMetricsService = mock(MemoryMetricsService.class);
        when(memoryMetricsService.getMemoryMetrics()).thenReturn(new MemoryMetrics(4096, 8192, MetricQuality.EXACT));
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
        replicasUpdaterService = new ReplicasUpdaterService(
            testThreadPool,
            createClusterService(testThreadPool, createClusterSettings()),
            mockClient,
            searchMetricsService
        );
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
        ReplicasUpdaterService instance = new ReplicasUpdaterService(
            testThreadPool,
            createClusterService(testThreadPool, createClusterSettings()),
            mockClient,
            searchMetricsService
        );
        instance.scheduleTask();
        TimeValue defaultValue = ReplicasUpdaterService.REPLICA_UPDATER_INTERVAL.getDefault(Settings.EMPTY);
        assertThat(instance.getInterval(), equalTo(defaultValue));
        assertThat(instance.getJob().toString(), containsString(defaultValue.toString()));
        instance.setInterval(TimeValue.timeValueMinutes(1));
        assertThat(instance.getInterval(), equalTo(TimeValue.timeValueMinutes(1)));
        assertThat(instance.getJob().toString(), containsString("1m"));
    }

    public void testUpdatePollIntervalUnscheduled() {
        ReplicasUpdaterService instance = new ReplicasUpdaterService(
            testThreadPool,
            createClusterService(testThreadPool, createClusterSettings()),
            mockClient,
            searchMetricsService
        );
        TimeValue defaultValue = ReplicasUpdaterService.REPLICA_UPDATER_INTERVAL.getDefault(Settings.EMPTY);
        assertThat(instance.getInterval(), equalTo(defaultValue));
        assertThat(instance.getJob(), nullValue());
        instance.setInterval(TimeValue.timeValueMinutes(1));
        assertThat(instance.getInterval(), equalTo(TimeValue.timeValueMinutes(1)));
        assertThat(instance.getJob(), nullValue());
    }

    public void testCancelingTaskClearsState() throws InterruptedException {
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
        assertTrue(this.replicasUpdaterService.scaleDownCounters.isEmpty());

        // clear mock client before teardown
        mockClient.updateSettingsToBeVerified = false;
    }

    /**
     * Test that the scheduled job runs at least once when a node running ReplicasUpdaterService becomes master.
     * We increase the schuled task interval for this and only call the onMaster() method to see
     * if the configured test client gets at least one call.
     */
    public void testSchedulingRunsJobOnce() throws InterruptedException {
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
        ReplicasUpdaterService instance = new ReplicasUpdaterService(
            testThreadPool,
            createClusterService(testThreadPool, createClusterSettings()),
            mockClient,
            searchMetricsServiceMock
        );
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
        searchMetricsService.clusterChanged(new ClusterChangedEvent("test", state1, ClusterState.EMPTY_STATE));

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
        assertEquals(Collections.emptyMap(), getRecommendedReplicaChanges(replicasUpdaterService, SEARCH_POWER_MIN_NO_REPLICATION));
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

    private Map<Integer, Set<String>> getRecommendedReplicaChanges(ReplicasUpdaterService replicasUpdaterService, int searchPowerMin) {
        return replicasUpdaterService.getRecommendedReplicaChanges(
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
        searchMetricsService.clusterChanged(new ClusterChangedEvent("test", state1, ClusterState.EMPTY_STATE));

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

        searchMetricsService.clusterChanged(new ClusterChangedEvent("test", state, ClusterState.EMPTY_STATE));
        assertEquals(0, getRecommendedReplicaChanges(replicasUpdaterService, SEARCH_POWER_MIN_NO_REPLICATION).size());

        searchMetricsService.processShardSizesRequest(
            new PublishShardSizesRequest(
                "search_node_1",
                Map.of(new ShardId(outsideBoostWindowMetadata.getIndex(), 0), new ShardSize(0, 1024, 0, 0))
            )
        );
        assertEquals(0, getRecommendedReplicaChanges(replicasUpdaterService, SEARCH_POWER_MIN_NO_REPLICATION).size());

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

        assertEquals(0, getRecommendedReplicaChanges(replicasUpdaterService, SEARCH_POWER_MIN_NO_REPLICATION).size());
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

        searchMetricsService.clusterChanged(new ClusterChangedEvent("test", state, ClusterState.EMPTY_STATE));
        searchMetricsService.processShardSizesRequest(new PublishShardSizesRequest("search_node_1", shardSizeMap));

        ExecutorService executorService = Executors.newFixedThreadPool(2);
        try {
            ClusterState newState = ClusterState.builder(state)
                .metadata(Metadata.builder(state.metadata()).removeAllIndices().build())
                .build();
            Future<?> numReplicaChangesFuture = executorService.submit(
                () -> assertEquals(0, getRecommendedReplicaChanges(replicasUpdaterService, SEARCH_POWER_MIN_NO_REPLICATION).size())
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

        searchMetricsService.clusterChanged(new ClusterChangedEvent("test", state, ClusterState.EMPTY_STATE));
        assertEquals(0, getRecommendedReplicaChanges(replicasUpdaterService, SEARCH_POWER_MIN_FULL_REPLICATION).size());

        searchMetricsService.processShardSizesRequest(
            new PublishShardSizesRequest(
                "search_node_1",
                Map.of(new ShardId(outsideBoostWindowMetadata.getIndex(), 0), new ShardSize(0, 1024, 0, 0))
            )
        );
        assertEquals(0, getRecommendedReplicaChanges(replicasUpdaterService, SEARCH_POWER_MIN_FULL_REPLICATION).size());

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
            Map<Integer, Set<String>> numberOfReplicaChanges = getRecommendedReplicaChanges(
                replicasUpdaterService,
                SEARCH_POWER_MIN_FULL_REPLICATION
            );
            assertEquals(1, numberOfReplicaChanges.size());
            assertEquals(
                Set.of(withinBoostWindowMetadata.getIndex().getName(), systemIndexInteractive.getIndex().getName()),
                numberOfReplicaChanges.get(2)
            );
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
            Map<Integer, Set<String>> numberOfReplicaChanges = getRecommendedReplicaChanges(
                replicasUpdaterService,
                SEARCH_POWER_MIN_FULL_REPLICATION
            );
            assertEquals(1, numberOfReplicaChanges.size());
            assertEquals(Set.of(withinBoostWindowMetadata.getIndex().getName()), numberOfReplicaChanges.get(1));
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
            Map<Integer, Set<String>> numberOfReplicaChanges = getRecommendedReplicaChanges(
                replicasUpdaterService,
                SEARCH_POWER_MIN_FULL_REPLICATION
            );
            assertEquals(1, numberOfReplicaChanges.size());
            assertEquals(Set.of(systemIndexInteractive.getIndex().getName()), numberOfReplicaChanges.get(1));
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
        searchMetricsService.clusterChanged(new ClusterChangedEvent("test", state1, ClusterState.EMPTY_STATE));
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

        // case 100 <= SPmin < 250
        int spMin = randomIntBetween(SEARCH_POWER_MIN_NO_REPLICATION, 249);
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
        int spMin = randomIntBetween(100, 400);
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

    private static ShardMetrics shardMetricOf(long interactiveSize) {
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
            REPLICA_UPDATER_SCALEDOWN_REPETITIONS,
            SearchMetricsService.ACCURATE_METRICS_WINDOW_SETTING,
            SearchMetricsService.STALE_METRICS_CHECK_INTERVAL_SETTING,
            ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING,
            ServerlessSharedSettings.ENABLE_REPLICAS_FOR_INSTANT_FAILOVER
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
        searchMetricsService.clusterChanged(new ClusterChangedEvent("test", newState, previousState));
        return newState;
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
            updates.put(numReplicas, Set.of(updateReplicasRequest.indices()));
            executionCount.incrementAndGet();
            updateSettingsToBeVerified = true;
            return super.executeLocally(action, request, listener);
        }

        void assertUpdates(String message, Map<Integer, Set<String>> expectedUpdates) {
            assertTrue("update settings not yet called", updateSettingsToBeVerified);
            assertEquals(message, expectedUpdates, updates);
            assertEquals(1, this.executionCount.get());
            updates.clear();
            updateSettingsToBeVerified = false;
            this.executionCount.set(0);
        }

        void assertNoUpdate() {
            assertFalse(updateSettingsToBeVerified);
        }
    }
}
