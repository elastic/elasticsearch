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

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.xpack.stateless.autoscaling.DesiredClusterTopology;
import org.elasticsearch.xpack.stateless.autoscaling.DesiredClusterTopologyTestUtils;
import org.elasticsearch.xpack.stateless.autoscaling.DesiredTopologyContext;
import org.junit.After;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.elasticsearch.xpack.stateless.autoscaling.search.ReplicasUpdaterServiceTests.shardMetricOf;
import static org.hamcrest.Matchers.equalTo;

public class ReplicasScalerCacheBudgetTests extends ESTestCase {

    private static final Comparator<String> REVERSED_KEY_COMPARATOR = Comparator.<String>naturalOrder().reversed();
    private TestThreadPool testThreadPool;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        this.testThreadPool = new TestThreadPool(getTestName());
    }

    @After
    public void afterTest() {
        this.testThreadPool.shutdown();
    }

    public void testGetCacheFreedByScaleDowns_ImmediateReplicaScaleDownResult() {
        ReplicasScalerCacheBudget replicasScalerCacheBudget = new ReplicasScalerCacheBudget(
            Settings.EMPTY,
            new DesiredTopologyContext(ClusterServiceUtils.createClusterService(testThreadPool)),
            new ReplicasScaleDownState(),
            -1
        );

        Index index1 = new Index("index1", "uuid1");
        ReplicaRankingContext rankingContext = new ReplicaRankingContext(
            Map.of(index1, new SearchMetricsService.IndexProperties(index1.getName(), 1, 3, false, false, 0)),
            Map.of(new ShardId(index1, 0), shardMetricOf(100_000_000L)),
            -1
        );

        // Scale down from 3 to 1 replicas => frees 2 * 100MB = 200MB
        ReplicasLoadBalancingScaler.ReplicasLoadBalancingResult result = new ReplicasLoadBalancingScaler.ReplicasLoadBalancingResult(
            Map.of("index1", 1),
            new TreeMap<>(),
            0
        );

        long cacheFreed = replicasScalerCacheBudget.getCacheFreedByScaleDowns(rankingContext, Map.of(), result, false);

        assertThat(cacheFreed, equalTo(200_000_000L));
    }

    public void testGetCacheFreedByScaleDowns_ImmediateScaleDownFlag() {
        ReplicasScalerCacheBudget replicasScalerCacheBudget = new ReplicasScalerCacheBudget(
            Settings.EMPTY,
            new DesiredTopologyContext(ClusterServiceUtils.createClusterService(testThreadPool)),
            new ReplicasScaleDownState(),
            -1
        );

        Index index1 = new Index("index1", "uuid1");
        ReplicaRankingContext rankingContext = new ReplicaRankingContext(
            Map.of(index1, new SearchMetricsService.IndexProperties(index1.getName(), 1, 4, false, false, 0)),
            Map.of(new ShardId(index1, 0), shardMetricOf(100_000_000L)),
            -1
        );

        Map<String, Integer> instantFailoverReplicaChanges = Map.of("index1", 3);
        SortedMap<String, Integer> desiredReplicas = new TreeMap<>();
        desiredReplicas.put("index1", 1);
        ReplicasLoadBalancingScaler.ReplicasLoadBalancingResult result = new ReplicasLoadBalancingScaler.ReplicasLoadBalancingResult(
            Map.of(),
            desiredReplicas,
            0
        );

        // With immediateScaleDown=true, scale from 4 to max(3, 1) => frees 1 * 100MB = 100MB
        long cacheFreed = replicasScalerCacheBudget.getCacheFreedByScaleDowns(rankingContext, instantFailoverReplicaChanges, result, true);

        assertThat(cacheFreed, equalTo(100_000_000L));
    }

    public void testGetCacheFreedByScaleDowns_RecommendationsWithSufficientRepeatedSignals() {
        var replicasScaleDownState = new ReplicasScaleDownState();
        for (int i = 0; i < 5; i++) {
            replicasScaleDownState.updateMaxReplicasRecommended("index1", 2);
        }

        ReplicasScalerCacheBudget replicasScalerCacheBudget = new ReplicasScalerCacheBudget(
            Settings.EMPTY,
            new DesiredTopologyContext(ClusterServiceUtils.createClusterService(testThreadPool)),
            replicasScaleDownState,
            6
        );

        Index index1 = new Index("index1", "uuid1");
        ReplicaRankingContext rankingContext = new ReplicaRankingContext(
            Map.of(index1, new SearchMetricsService.IndexProperties(index1.getName(), 1, 3, false, false, 0)),
            Map.of(new ShardId(index1, 0), shardMetricOf(100_000_000L)),
            -1
        );

        SortedMap<String, Integer> desiredReplicas = new TreeMap<>();
        desiredReplicas.put("index1", 1); // recommend 1, but maxReplicasRecommended is 2
        ReplicasLoadBalancingScaler.ReplicasLoadBalancingResult result = new ReplicasLoadBalancingScaler.ReplicasLoadBalancingResult(
            Map.of(),
            desiredReplicas,
            0
        );

        // Scale from 3 to max(1, 2)=2 => frees 1 * 100MB = 100MB
        long cacheFreed = replicasScalerCacheBudget.getCacheFreedByScaleDowns(rankingContext, Map.of(), result, false);

        assertThat(cacheFreed, equalTo(100_000_000L));
    }

    public void testGetCacheFreedByScaleDowns_RecommendationsWithInsufficientRepeatedSignals() {
        var replicasScaleDownState = new ReplicasScaleDownState();
        // Only 3 signals
        for (int i = 0; i < 3; i++) {
            replicasScaleDownState.updateMaxReplicasRecommended("index1", 2);
        }

        ReplicasScalerCacheBudget replicasScalerCacheBudget = new ReplicasScalerCacheBudget(
            Settings.EMPTY,
            new DesiredTopologyContext(ClusterServiceUtils.createClusterService(testThreadPool)),
            replicasScaleDownState,
            6
        );

        Index index1 = new Index("index1", "uuid1");
        ReplicaRankingContext rankingContext = new ReplicaRankingContext(
            Map.of(index1, new SearchMetricsService.IndexProperties(index1.getName(), 1, 4, false, false, 0)),
            Map.of(new ShardId(index1, 0), shardMetricOf(100_000_000L)),
            -1
        );

        Map<String, Integer> instantFailoverReplicaChanges = Map.of("index1", 3);
        SortedMap<String, Integer> desiredReplicas = new TreeMap<>();
        desiredReplicas.put("index1", 2);
        ReplicasLoadBalancingScaler.ReplicasLoadBalancingResult result = new ReplicasLoadBalancingScaler.ReplicasLoadBalancingResult(
            Map.of(),
            desiredReplicas,
            0
        );

        // Not enough signals, so no cache freed
        long cacheFreed = replicasScalerCacheBudget.getCacheFreedByScaleDowns(rankingContext, instantFailoverReplicaChanges, result, false);

        assertThat(cacheFreed, equalTo(0L));
    }

    public void testGetCacheNeededForInstantFailoverScaleUps_SingleIndexScaleUp() {
        Index index1 = new Index("index1", "uuid1");
        ReplicaRankingContext rankingContext = new ReplicaRankingContext(
            Map.of(index1, new SearchMetricsService.IndexProperties(index1.getName(), 1, 2, false, false, 0)),
            Map.of(new ShardId(index1, 0), shardMetricOf(100_000_000L)),
            -1
        );

        Map<String, Integer> instantFailoverReplicaChanges = Map.of("index1", 4);

        // Scale up from 2 to 4 replicas => needs 2 * 100MB = 200MB
        long cacheNeeded = ReplicasScalerCacheBudget.getCacheNeededForInstantFailoverScaleUps(
            rankingContext,
            instantFailoverReplicaChanges
        );

        assertThat(cacheNeeded, equalTo(200_000_000L));
    }

    public void testGetCacheNeededForInstantFailoverScaleUps_MultipleIndicesScaleUp() {
        Index index1 = new Index("index1", "uuid1");
        Index index2 = new Index("index2", "uuid2");
        ReplicaRankingContext rankingContext = new ReplicaRankingContext(
            Map.of(
                index1,
                new SearchMetricsService.IndexProperties(index1.getName(), 1, 1, false, false, 0),
                index2,
                new SearchMetricsService.IndexProperties(index2.getName(), 1, 2, false, false, 0)
            ),
            Map.of(new ShardId(index1, 0), shardMetricOf(100_000_000L), new ShardId(index2, 0), shardMetricOf(200_000_000L)),
            -1
        );

        Map<String, Integer> instantFailoverReplicaChanges = Map.of("index1", 3, "index2", 4);

        // index1: 1->3 = 2 * 100MB = 200MB
        // index2: 2->4 = 2 * 200MB = 400MB => total 600MB
        long cacheNeeded = ReplicasScalerCacheBudget.getCacheNeededForInstantFailoverScaleUps(
            rankingContext,
            instantFailoverReplicaChanges
        );

        assertThat(cacheNeeded, equalTo(600_000_000L));
    }

    public void testGetCacheNeededForInstantFailoverScaleUps_ScaleDownIgnored() {
        Index index1 = new Index("index1", "uuid1");
        Index index2 = new Index("index2", "uuid2");
        ReplicaRankingContext rankingContext = new ReplicaRankingContext(
            Map.of(
                index1,
                new SearchMetricsService.IndexProperties(index1.getName(), 1, 2, false, false, 0),
                index2,
                new SearchMetricsService.IndexProperties(index2.getName(), 1, 4, false, false, 0)
            ),
            Map.of(new ShardId(index1, 0), shardMetricOf(100_000_000L), new ShardId(index2, 0), shardMetricOf(200_000_000L)),
            -1
        );

        // index1 scales up, index2 scales down
        Map<String, Integer> instantFailoverReplicaChanges = Map.of("index1", 5, "index2", 2);

        // Only index1 scale-up counts: 2->5 = 3 * 100MB = 300MB
        long cacheNeeded = ReplicasScalerCacheBudget.getCacheNeededForInstantFailoverScaleUps(
            rankingContext,
            instantFailoverReplicaChanges
        );

        assertThat(cacheNeeded, equalTo(300_000_000L));
    }

    public void testBudgetLoadBalancingScaleUps_AllFitWithinBudget() {
        Index index1 = new Index("index1", "uuid1");
        Index index2 = new Index("index2", "uuid2");
        Index index3 = new Index("index3", "uuid3");
        Index index4 = new Index("index4", "uuid4");
        ReplicaRankingContext rankingContext = new ReplicaRankingContext(
            Map.of(
                index1,
                new SearchMetricsService.IndexProperties(index1.getName(), 1, 1, false, false, 0),
                index2,
                new SearchMetricsService.IndexProperties(index2.getName(), 1, 1, false, false, 0),
                index3,
                new SearchMetricsService.IndexProperties(index3.getName(), 1, 4, false, false, 0),
                index4,
                new SearchMetricsService.IndexProperties(index4.getName(), 1, 3, false, false, 0)
            ),
            Map.of(
                new ShardId(index1, 0),
                shardMetricOf(100_000_000L),
                new ShardId(index2, 0),
                shardMetricOf(100_000_000L),
                new ShardId(index3, 0),
                shardMetricOf(50_000_000L),
                new ShardId(index4, 0),
                shardMetricOf(75_000_000L)
            ),
            -1
        );

        SortedMap<String, Integer> desiredReplicas = new TreeMap<>();
        desiredReplicas.put("index1", 3);  // scale up: 1->3
        desiredReplicas.put("index2", 2);  // scale up: 1->2
        desiredReplicas.put("index3", 2);  // scale down: 4->2
        desiredReplicas.put("index4", 1);  // scale down: 3->1

        ReplicasLoadBalancingScaler.ReplicasLoadBalancingResult initialResult = new ReplicasLoadBalancingScaler.ReplicasLoadBalancingResult(
            Map.of(),
            desiredReplicas,
            0
        );

        // Budget: 500MB, needed for scale-ups: 2*100MB + 1*100MB = 300MB
        ReplicasLoadBalancingScaler.ReplicasLoadBalancingResult budgetedResult = ReplicasScalerCacheBudget.budgetLoadBalancingScaleUps(
            rankingContext,
            Map.of(),
            initialResult,
            500_000_000L
        );

        assertSame(budgetedResult.immediateReplicaScaleDown(), initialResult.immediateReplicaScaleDown());
        assertThat(budgetedResult.desiredReplicasPerIndex().size(), equalTo(4));
        assertThat(budgetedResult.desiredReplicasPerIndex().get("index1"), equalTo(3));
        assertThat(budgetedResult.desiredReplicasPerIndex().get("index2"), equalTo(2));
        assertThat(budgetedResult.desiredReplicasPerIndex().get("index3"), equalTo(2)); // Scale-downs are always kept
        assertThat(budgetedResult.desiredReplicasPerIndex().get("index4"), equalTo(1)); // Scale-downs are always kept
        assertThat(budgetedResult.indicesBlockedFromScaleUp(), equalTo(0));
    }

    public void testBudgetLoadBalancingScaleUps_SomeExceedBudget() {
        Index index1 = new Index("index1", "uuid1");
        Index index2 = new Index("index2", "uuid2");
        Index index3 = new Index("index3", "uuid3");
        Index index4 = new Index("index4", "uuid4");
        ReplicaRankingContext rankingContext = new ReplicaRankingContext(
            Map.of(
                index1,
                new SearchMetricsService.IndexProperties(index1.getName(), 1, 1, false, false, 0),
                index2,
                new SearchMetricsService.IndexProperties(index2.getName(), 1, 1, false, false, 0),
                index3,
                new SearchMetricsService.IndexProperties(index3.getName(), 1, 4, false, false, 0),
                index4,
                new SearchMetricsService.IndexProperties(index4.getName(), 1, 3, false, false, 0)
            ),
            Map.of(
                new ShardId(index1, 0),
                shardMetricOf(200_000_000L),
                new ShardId(index2, 0),
                shardMetricOf(300_000_000L),
                new ShardId(index3, 0),
                shardMetricOf(50_000_000L),
                new ShardId(index4, 0),
                shardMetricOf(75_000_000L)
            ),
            -1
        );

        SortedMap<String, Integer> desiredReplicas = new TreeMap<>(Comparator.naturalOrder());
        desiredReplicas.put("index1", 3);  // scale up: 1->3
        desiredReplicas.put("index2", 2);  // scale up: 1->2
        desiredReplicas.put("index3", 2);  // scale down: 4->2
        desiredReplicas.put("index4", 1);  // scale down: 3->1

        ReplicasLoadBalancingScaler.ReplicasLoadBalancingResult initialResult = new ReplicasLoadBalancingScaler.ReplicasLoadBalancingResult(
            Map.of(),
            desiredReplicas,
            0
        );

        // Budget: 500MB, needed for scale-ups: 2*200MB = 400MB (fits) + 1*300MB (doesn't fit)
        ReplicasLoadBalancingScaler.ReplicasLoadBalancingResult budgetedResult = ReplicasScalerCacheBudget.budgetLoadBalancingScaleUps(
            rankingContext,
            Map.of(),
            initialResult,
            500_000_000L
        );

        assertSame(budgetedResult.immediateReplicaScaleDown(), initialResult.immediateReplicaScaleDown());
        assertThat(budgetedResult.desiredReplicasPerIndex().size(), equalTo(4));
        assertThat(budgetedResult.desiredReplicasPerIndex().get("index1"), equalTo(3));
        assertThat(budgetedResult.desiredReplicasPerIndex().get("index2"), equalTo(1));
        assertThat(budgetedResult.desiredReplicasPerIndex().get("index3"), equalTo(2)); // Scale-downs are always kept
        assertThat(budgetedResult.desiredReplicasPerIndex().get("index4"), equalTo(1)); // Scale-downs are always kept
        assertThat(budgetedResult.indicesBlockedFromScaleUp(), equalTo(1));
    }

    public void testBudgetLoadBalancingScaleUps_InstantFailoverIsNotRestricted() {
        Index index1 = new Index("index1", "uuid1");
        Index index2 = new Index("index2", "uuid2");
        Index index3 = new Index("index3", "uuid3");
        Index index4 = new Index("index4", "uuid4");
        ReplicaRankingContext rankingContext = new ReplicaRankingContext(
            Map.of(
                index1,
                new SearchMetricsService.IndexProperties(index1.getName(), 1, 1, false, false, 0),
                index2,
                new SearchMetricsService.IndexProperties(index2.getName(), 1, 1, false, false, 0),
                index3,
                new SearchMetricsService.IndexProperties(index3.getName(), 1, 4, false, false, 0),
                index4,
                new SearchMetricsService.IndexProperties(index4.getName(), 1, 3, false, false, 0)
            ),
            Map.of(
                new ShardId(index1, 0),
                shardMetricOf(200_000_000L),
                new ShardId(index2, 0),
                shardMetricOf(300_000_000L),
                new ShardId(index3, 0),
                shardMetricOf(50_000_000L),
                new ShardId(index4, 0),
                shardMetricOf(75_000_000L)
            ),
            -1
        );

        SortedMap<String, Integer> desiredReplicas = new TreeMap<>();
        desiredReplicas.put("index1", 3);  // scale up: 1->3
        desiredReplicas.put("index2", 2);  // scale up: 1->2
        desiredReplicas.put("index3", 2);  // scale down: 4->2
        desiredReplicas.put("index4", 1);  // scale down: 3->1

        ReplicasLoadBalancingScaler.ReplicasLoadBalancingResult initialResult = new ReplicasLoadBalancingScaler.ReplicasLoadBalancingResult(
            Map.of(),
            desiredReplicas,
            0
        );
        Map<String, Integer> instantFailoverReplicaChanges = Map.of("index2", 2);

        // Budget: 500MB, needed for scale-ups: 2*200MB = 400MB (fits) + 1*300MB (doesn't fit)
        // However instant failover also recommends index2 so it will not be removed
        ReplicasLoadBalancingScaler.ReplicasLoadBalancingResult budgetedResult = ReplicasScalerCacheBudget.budgetLoadBalancingScaleUps(
            rankingContext,
            instantFailoverReplicaChanges,
            initialResult,
            500_000_000L
        );

        assertSame(budgetedResult.immediateReplicaScaleDown(), initialResult.immediateReplicaScaleDown());
        assertThat(budgetedResult.desiredReplicasPerIndex().size(), equalTo(4));
        assertThat(budgetedResult.desiredReplicasPerIndex().get("index1"), equalTo(3));
        assertThat(budgetedResult.desiredReplicasPerIndex().get("index2"), equalTo(2));
        assertThat(budgetedResult.desiredReplicasPerIndex().get("index3"), equalTo(2)); // Scale-downs are always kept
        assertThat(budgetedResult.desiredReplicasPerIndex().get("index4"), equalTo(1)); // Scale-downs are always kept
        assertThat(budgetedResult.indicesBlockedFromScaleUp(), equalTo(0));
    }

    public void testApplyCacheBudgetConstraint_NoIndicesExceedBudget() {
        DesiredTopologyContext desiredTopologyContext = new DesiredTopologyContext(
            ClusterServiceUtils.createClusterService(testThreadPool)
        );
        desiredTopologyContext.updateDesiredClusterTopology(
            new DesiredClusterTopology(
                new DesiredClusterTopology.TierTopology(2, "5gb", 1.0f, 1.0f, 1.0f),
                DesiredClusterTopologyTestUtils.randomTierTopology()
            )
        );
        ReplicasScalerCacheBudget replicasScalerCacheBudget = new ReplicasScalerCacheBudget(
            Settings.builder().putList("node.roles", "search").build(),
            desiredTopologyContext,
            null,
            -1
        );
        replicasScalerCacheBudget.setCacheBudgetRatio(0.5);

        /*
        At this point, we have around 4.5gb of "cache budget."
        This is because:
        - 5gb memory * 1.0 storage ratio * 0.9 SHARED_CACHE_SIZE_SETTING ~= 4.5gb of cache per node
        - 2 nodes => 9gb of cache
        - 0.5 cache budget for scaling replicas => 4.5gb of cache budget
        Let's build 3 indices with 1 x 0.5gb shard each. (3 * 0.5gb = 1.5gb, well within cache budget).
        Then, we'll recommend all of them to scale up to 2 shards. (3 * 2 * 0.5gb = 3gb, still well within cache budget).
        All of these scale-ups should make it through the cache budgeting step.
         */
        Index index1 = new Index("index1", "uuid1");
        Index index2 = new Index("index2", "uuid2");
        Index systemIndex = new Index(".system", "uuid3");
        ReplicaRankingContext rankingContext = new ReplicaRankingContext(
            Map.of(
                index1,
                new SearchMetricsService.IndexProperties(index1.getName(), 1, 1, false, false, 0),
                index2,
                new SearchMetricsService.IndexProperties(index2.getName(), 1, 1, false, true, 0),
                systemIndex,
                new SearchMetricsService.IndexProperties(systemIndex.getName(), 1, 1, true, false, 0)
            ),
            Map.of(
                new ShardId(index1, 0),
                shardMetricOf(500_000_000L),
                new ShardId(index2, 0),
                shardMetricOf(500_000_000L),
                new ShardId(systemIndex, 0),
                shardMetricOf(500_000_000L)
            ),
            -1 // SPmin has no effect since we're calling applyCacheBudgetConstraint directly in this test
        );

        SortedMap<String, Integer> recommendedChanges = new TreeMap<>();
        recommendedChanges.put("index1", 2);
        recommendedChanges.put("index2", 2);
        recommendedChanges.put(".system", 2);
        int indicesBlockedFromScaleUp = 0;
        var initialResult = new ReplicasLoadBalancingScaler.ReplicasLoadBalancingResult(
            Map.of(),
            recommendedChanges,
            indicesBlockedFromScaleUp
        );

        var budgetedResult = replicasScalerCacheBudget.applyCacheBudgetConstraint(rankingContext, Map.of(), initialResult, false);

        assertSame(budgetedResult.immediateReplicaScaleDown(), initialResult.immediateReplicaScaleDown());
        assertThat(budgetedResult.desiredReplicasPerIndex().size(), equalTo(3));
        assertThat(budgetedResult.desiredReplicasPerIndex().get(".system"), equalTo(2));
        assertThat(budgetedResult.desiredReplicasPerIndex().get("index1"), equalTo(2));
        assertThat(budgetedResult.desiredReplicasPerIndex().get("index2"), equalTo(2));
        assertThat(budgetedResult.indicesBlockedFromScaleUp(), equalTo(indicesBlockedFromScaleUp));
    }

    public void testApplyCacheBudgetConstraint_OneIndexExceedsBudget() {
        DesiredTopologyContext desiredTopologyContext = new DesiredTopologyContext(
            ClusterServiceUtils.createClusterService(testThreadPool)
        );
        desiredTopologyContext.updateDesiredClusterTopology(
            new DesiredClusterTopology(
                new DesiredClusterTopology.TierTopology(4, "1.1gb", 1.0f, 1.0f, 1.0f),
                DesiredClusterTopologyTestUtils.randomTierTopology()
            )
        );
        ReplicasScalerCacheBudget replicasScalerCacheBudget = new ReplicasScalerCacheBudget(
            Settings.builder().putList("node.roles", "search").build(),
            desiredTopologyContext,
            null,
            -1
        );
        replicasScalerCacheBudget.setCacheBudgetRatio(0.25);

        /*
        At this point, we have around 1gb of "cache budget."
        This is because:
        - 1.1gb memory * 1.0 storage ratio * 0.9 SHARED_CACHE_SIZE_SETTING ~= 1gb of cache per node
        - 4 nodes => 4gb of cache
        - 0.25 cache budget for scaling replicas => 1gb of cache budget
        Let's build 3 indices with one 0.09gb shard and 3 replicas each. (3 * 3 * 0.1gb = 0.9gb, within cache budget).
        Then, we'll recommend 2 of those indices to scale up to 4 replicas, which would put us outside the cache budget. (additional
        2 * 0.1gb = 0.2gb needed. 0.9gb + 0.2gb = 1.1 gb).
        Only one will fit.
        */
        Index index1 = new Index("index1", "uuid1");
        Index index2 = new Index("index2", "uuid2");
        Index systemIndex = new Index(".system", "uuid3");
        ReplicaRankingContext rankingContext = new ReplicaRankingContext(
            Map.of(
                index1,
                new SearchMetricsService.IndexProperties(index1.getName(), 1, 3, false, false, 0),
                index2,
                new SearchMetricsService.IndexProperties(index2.getName(), 1, 3, false, true, 0),
                systemIndex,
                new SearchMetricsService.IndexProperties(systemIndex.getName(), 1, 3, true, false, 0)
            ),
            Map.of(
                new ShardId(index1, 0),
                shardMetricOf(100_000_000L),
                new ShardId(index2, 0),
                shardMetricOf(100_000_000L),
                new ShardId(systemIndex, 0),
                shardMetricOf(100_000_000L)
            ),
            -1 // SPmin has no effect since we're calling applyCacheBudgetConstraint directly in this test
        );

        SortedMap<String, Integer> recommendedChanges = new TreeMap<>(REVERSED_KEY_COMPARATOR);
        recommendedChanges.put("index1", 4);
        recommendedChanges.put("index2", 4);
        recommendedChanges.put(".system", 3);
        int indicesBlockedFromScaleUp = 0;
        var initialResult = new ReplicasLoadBalancingScaler.ReplicasLoadBalancingResult(
            Map.of(),
            recommendedChanges,
            indicesBlockedFromScaleUp
        );

        var budgetedResult = replicasScalerCacheBudget.applyCacheBudgetConstraint(rankingContext, Map.of(), initialResult, false);

        assertSame(budgetedResult.immediateReplicaScaleDown(), initialResult.immediateReplicaScaleDown());
        assertThat(budgetedResult.desiredReplicasPerIndex().size(), equalTo(3));
        assertThat(budgetedResult.desiredReplicasPerIndex().get("index2"), equalTo(4));
        assertThat(budgetedResult.desiredReplicasPerIndex().get("index1"), equalTo(3));
        assertThat(budgetedResult.desiredReplicasPerIndex().get(".system"), equalTo(3));
        assertThat(budgetedResult.indicesBlockedFromScaleUp(), equalTo(1));
    }

    // This test is almost identical to the above test, with the addition of a single scale down, which frees up room for *all* recommended
    // scale-ups to proceed.
    public void testApplyCacheBudgetConstraint_NoIndicesExceedBudgetDueToScaleDown() {
        DesiredTopologyContext desiredTopologyContext = new DesiredTopologyContext(
            ClusterServiceUtils.createClusterService(testThreadPool)
        );
        desiredTopologyContext.updateDesiredClusterTopology(
            new DesiredClusterTopology(
                new DesiredClusterTopology.TierTopology(4, "1.1gb", 1.0f, 1.0f, 1.0f),
                DesiredClusterTopologyTestUtils.randomTierTopology()
            )
        );
        ReplicasScalerCacheBudget replicasScalerCacheBudget = new ReplicasScalerCacheBudget(
            Settings.builder().putList("node.roles", "search").build(),
            desiredTopologyContext,
            new ReplicasScaleDownState(),
            -1
        );
        replicasScalerCacheBudget.setCacheBudgetRatio(0.25);

        /*
        At this point, we have around 1gb of "cache budget."
        This is because:
        - 1.1gb memory * 1.0 storage ratio * 0.9 SHARED_CACHE_SIZE_SETTING ~= 1gb of cache per node
        - 4 nodes => 4gb of cache
        - 0.25 cache budget for scaling replicas => 1gb of cache budget
        Let's build 3 indices with one 0.09gb shard and 3 replicas each. (3 * 3 * 0.1gb = 0.9gb, within cache budget).
        Then, we'll recommend 2 of those indices to scale up to 4 replicas, which would put us outside the cache budget. (additional
        2 * 0.1gb = 0.2gb needed. 0.9gb + 0.2gb = 1.1 gb).
        However, we'll simultaneously recommend the other index to scale down to 1 replica. (reduces cache needed by 2 * 0.1gb = 0.2gb).
        This should allow both scale-ups to pass, as the scale down will reduce the cache needed by just enough.
        */
        Index index1 = new Index("index1", "uuid1");
        Index index2 = new Index("index2", "uuid2");
        Index systemIndex = new Index(".system", "uuid3");
        ReplicaRankingContext rankingContext = new ReplicaRankingContext(
            Map.of(
                index1,
                new SearchMetricsService.IndexProperties(index1.getName(), 1, 3, false, false, 0),
                index2,
                new SearchMetricsService.IndexProperties(index2.getName(), 1, 3, false, true, 0),
                systemIndex,
                new SearchMetricsService.IndexProperties(systemIndex.getName(), 1, 3, true, false, 0)
            ),
            Map.of(
                new ShardId(index1, 0),
                shardMetricOf(100_000_000L),
                new ShardId(index2, 0),
                shardMetricOf(100_000_000L),
                new ShardId(systemIndex, 0),
                shardMetricOf(100_000_000L)
            ),
            -1 // SPmin has no effect since we're calling applyCacheBudgetConstraint directly in this test
        );

        SortedMap<String, Integer> recommendedReplicas = new TreeMap<>(REVERSED_KEY_COMPARATOR); // keeps .system last (fewer replicas)
        recommendedReplicas.put("index1", 4);
        recommendedReplicas.put("index2", 4);
        recommendedReplicas.put(".system", 1);
        int indicesBlockedFromScaleUp = 0;
        var initialResult = new ReplicasLoadBalancingScaler.ReplicasLoadBalancingResult(
            Map.of(),
            recommendedReplicas,
            indicesBlockedFromScaleUp
        );

        ReplicasLoadBalancingScaler.ReplicasLoadBalancingResult budgetedResult;
        if (randomBoolean()) {
            // One way to force scale-downs is with immediateScaleDown = true
            budgetedResult = replicasScalerCacheBudget.applyCacheBudgetConstraint(rankingContext, Map.of(), initialResult, true);
        } else {
            // The other way is to have enough scale-down signals (we'll use the default of 6)
            // So we must send 5 signals - the 6th is inherent in the call of applyCacheBudgetConstraint
            var replicasScaleDownState = new ReplicasScaleDownState();
            for (int i = 0; i < 5; i++) {
                replicasScaleDownState.updateMaxReplicasRecommended(".system", 1);
            }
            replicasScalerCacheBudget = new ReplicasScalerCacheBudget(
                Settings.builder().putList("node.roles", "search").build(),
                desiredTopologyContext,
                replicasScaleDownState,
                6
            );
            replicasScalerCacheBudget.setCacheBudgetRatio(0.25);
            budgetedResult = replicasScalerCacheBudget.applyCacheBudgetConstraint(rankingContext, Map.of(), initialResult, false);
        }

        assertSame(budgetedResult.immediateReplicaScaleDown(), initialResult.immediateReplicaScaleDown());
        assertThat(budgetedResult.desiredReplicasPerIndex().size(), equalTo(3));
        assertThat(budgetedResult.desiredReplicasPerIndex().get("index1"), equalTo(4));
        assertThat(budgetedResult.desiredReplicasPerIndex().get("index2"), equalTo(4));
        assertThat(budgetedResult.desiredReplicasPerIndex().get(".system"), equalTo(1));
        assertThat(budgetedResult.indicesBlockedFromScaleUp(), equalTo(indicesBlockedFromScaleUp));
    }

    public void testApplyCacheBudgetConstraint_AllIndicesExceedBudget() {
        DesiredTopologyContext desiredTopologyContext = new DesiredTopologyContext(
            ClusterServiceUtils.createClusterService(testThreadPool)
        );
        desiredTopologyContext.updateDesiredClusterTopology(
            new DesiredClusterTopology(
                new DesiredClusterTopology.TierTopology(2, "1.1gb", 1.0f, 1.0f, 1.0f),
                DesiredClusterTopologyTestUtils.randomTierTopology()
            )
        );
        ReplicasScalerCacheBudget replicasScalerCacheBudget = new ReplicasScalerCacheBudget(
            Settings.builder().putList("node.roles", "search").build(),
            desiredTopologyContext,
            null,
            -1
        );
        replicasScalerCacheBudget.setCacheBudgetRatio(0.5);

        /*
        At this point, we have around 1gb of "cache budget."
        This is because:
        - 1.1gb memory * 1.0 storage ratio * 0.9 SHARED_CACHE_SIZE_SETTING ~= 1gb of cache per node
        - 2 nodes => 2gb of cache
        - 0.5 cache budget for scaling replicas => 1gb of cache budget
        Let's build 3 indices with 1 x 0.19gb shard each. (3 * 0.3 = 0.9gb, barely within cache budget).
        Then, we'll recommend all of them to scale up to 2 shards.
        The cache budget shouldn't allow any scale ups because there is no room for another 0.3gb replica.
        */
        Index index1 = new Index("index1", "uuid1");
        Index index2 = new Index("index2", "uuid2");
        Index systemIndex = new Index(".system", "uuid3");
        ReplicaRankingContext rankingContext = new ReplicaRankingContext(
            Map.of(
                index1,
                new SearchMetricsService.IndexProperties(index1.getName(), 1, 1, false, false, 0),
                index2,
                new SearchMetricsService.IndexProperties(index2.getName(), 1, 1, false, true, 0),
                systemIndex,
                new SearchMetricsService.IndexProperties(systemIndex.getName(), 1, 1, true, false, 0)
            ),
            Map.of(
                new ShardId(index1, 0),
                shardMetricOf(300_000_000L),
                new ShardId(index2, 0),
                shardMetricOf(300_000_000L),
                new ShardId(systemIndex, 0),
                shardMetricOf(300_000_000L)
            ),
            -1 // SPmin has no effect since we're calling applyCacheBudgetConstraint directly in this test
        );

        SortedMap<String, Integer> recommendedReplicas = new TreeMap<>(REVERSED_KEY_COMPARATOR);
        recommendedReplicas.put("index1", 2);
        recommendedReplicas.put("index2", 2);
        recommendedReplicas.put(".system", 2);
        int indicesBlockedFromScaleUp = 0;
        var initialResult = new ReplicasLoadBalancingScaler.ReplicasLoadBalancingResult(
            Map.of(),
            recommendedReplicas,
            indicesBlockedFromScaleUp
        );

        var budgetedResult = replicasScalerCacheBudget.applyCacheBudgetConstraint(rankingContext, Map.of(), initialResult, false);

        assertSame(budgetedResult.immediateReplicaScaleDown(), initialResult.immediateReplicaScaleDown());
        assertThat(budgetedResult.desiredReplicasPerIndex().size(), equalTo(3));
        assertThat(budgetedResult.desiredReplicasPerIndex().get("index1"), equalTo(1));
        assertThat(budgetedResult.desiredReplicasPerIndex().get("index2"), equalTo(1));
        assertThat(budgetedResult.desiredReplicasPerIndex().get(".system"), equalTo(1));
        assertThat(budgetedResult.indicesBlockedFromScaleUp(), equalTo(indicesBlockedFromScaleUp + 3));
    }

    public void testApplyCacheBudgetConstraint_InstantFailoverExceedsLoadBalancingBudget() {
        DesiredTopologyContext desiredTopologyContext = new DesiredTopologyContext(
            ClusterServiceUtils.createClusterService(testThreadPool)
        );
        desiredTopologyContext.updateDesiredClusterTopology(
            new DesiredClusterTopology(
                new DesiredClusterTopology.TierTopology(2, "1.1gb", 1.0f, 1.0f, 1.0f),
                DesiredClusterTopologyTestUtils.randomTierTopology()
            )
        );
        ReplicasScalerCacheBudget replicasScalerCacheBudget = new ReplicasScalerCacheBudget(
            Settings.builder().putList("node.roles", "search").build(),
            desiredTopologyContext,
            null,
            -1
        );
        replicasScalerCacheBudget.setCacheBudgetRatio(0.5);

        /*
        At this point, we have around 1gb of "cache budget."
        This is because:
        - 1.1gb memory * 1.0 storage ratio * 0.9 SHARED_CACHE_SIZE_SETTING ~= 1gb of cache per node
        - 2 nodes => 2gb of cache
        - 0.5 cache budget for scaling replicas => 1gb of cache budget
        Let's build 3 indices with 1 x 0.19gb shard each. (3 * 0.3 = 0.9gb, barely within cache budget).
        Then, the instant failover system will recommend all of them to scale up to 2 shards.
        Since instant failover has now pushed us over cache budget, any load balancing scale-up recommendations should be ignored.
        */
        Index index1 = new Index("index1", "uuid1");
        Index index2 = new Index("index2", "uuid2");
        Index systemIndex = new Index(".system", "uuid3");
        ReplicaRankingContext rankingContext = new ReplicaRankingContext(
            Map.of(
                index1,
                new SearchMetricsService.IndexProperties(index1.getName(), 1, 1, false, false, 0),
                index2,
                new SearchMetricsService.IndexProperties(index2.getName(), 1, 1, false, true, 0),
                systemIndex,
                new SearchMetricsService.IndexProperties(systemIndex.getName(), 1, 1, true, false, 0)
            ),
            Map.of(
                new ShardId(index1, 0),
                shardMetricOf(300_000_000L),
                new ShardId(index2, 0),
                shardMetricOf(300_000_000L),
                new ShardId(systemIndex, 0),
                shardMetricOf(300_000_000L)
            ),
            -1 // SPmin has no effect since we're calling applyCacheBudgetConstraint directly in this test
        );

        Map<String, Integer> instantFailoverRecommendations = new HashMap<>();
        instantFailoverRecommendations.put("index1", 2);
        instantFailoverRecommendations.put("index2", 2);
        instantFailoverRecommendations.put(".system", 2);
        SortedMap<String, Integer> recommendedChanges = new TreeMap<>(REVERSED_KEY_COMPARATOR);
        recommendedChanges.put("index1", 3);
        recommendedChanges.put("index2", 3);
        recommendedChanges.put(".system", 3);
        int indicesBlockedFromScaleUp = 0;
        ReplicasLoadBalancingScaler.ReplicasLoadBalancingResult initialResult = new ReplicasLoadBalancingScaler.ReplicasLoadBalancingResult(
            Map.of(),
            recommendedChanges,
            indicesBlockedFromScaleUp
        );

        var budgetedResult = replicasScalerCacheBudget.applyCacheBudgetConstraint(
            rankingContext,
            instantFailoverRecommendations,
            initialResult,
            false
        );

        assertSame(budgetedResult.immediateReplicaScaleDown(), initialResult.immediateReplicaScaleDown());
        // budget is exceeded by instant failover alone, so the budgeting will
        // remove all load balancing scale ups, keeping all indices to the current
        // number of replicas
        assertThat(budgetedResult.desiredReplicasPerIndex().size(), equalTo(3));
        assertThat(budgetedResult.desiredReplicasPerIndex().get("index1"), equalTo(1));
        assertThat(budgetedResult.desiredReplicasPerIndex().get("index2"), equalTo(1));
        assertThat(budgetedResult.desiredReplicasPerIndex().get(".system"), equalTo(1));
        assertThat(budgetedResult.indicesBlockedFromScaleUp(), equalTo(indicesBlockedFromScaleUp + 3));
    }
}
