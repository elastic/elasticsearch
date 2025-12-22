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
import co.elastic.elasticsearch.stateless.api.ShardSizeStatsReader;
import co.elastic.elasticsearch.stateless.autoscaling.DesiredClusterTopology;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.search.stats.SearchStats;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpNodeClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mockito;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static co.elastic.elasticsearch.stateless.autoscaling.DesiredClusterTopologyTestUtils.randomTierTopology;
import static co.elastic.elasticsearch.stateless.autoscaling.search.ReplicasLoadBalancingScaler.getIndicesRelativeSearchLoads;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class ReplicasLoadBalancingScalerTests extends ESTestCase {

    private TestThreadPool testThreadPool;
    private ReplicasLoadBalancingScaler scaler;
    private MockClient client;

    @Before
    private void setup() {
        testThreadPool = new TestThreadPool(getTestName());
        ClusterService clusterService = ClusterServiceUtils.createClusterService(testThreadPool, createClusterSettings());
        client = new MockClient(testThreadPool);
        scaler = new ReplicasLoadBalancingScaler(clusterService, client);
        scaler.init();
    }

    @After
    public void cleanup() {
        testThreadPool.shutdownNow();
    }

    public void testGetIndicesRelativeSearchLoadsWithSearchLoad() {
        Map<Index, SearchMetricsService.IndexProperties> indicesMap = new HashMap<>();
        Map<ShardId, SearchMetricsService.ShardMetrics> shardMetrics = new HashMap<>();
        SearchMetricsService.ShardMetrics sizeRelatedShardMetrics = new SearchMetricsService.ShardMetrics();
        sizeRelatedShardMetrics.shardSize = new ShardSizeStatsReader.ShardSize(100, 150, 0, 0);

        Index index1 = new Index("index1", "uuid");
        indicesMap.put(index1, new SearchMetricsService.IndexProperties("index1", 1, 1, false, false, 0));
        shardMetrics.put(new ShardId(index1, 0), sizeRelatedShardMetrics);
        Index index2 = new Index("index2", "uuid2");
        indicesMap.put(index2, new SearchMetricsService.IndexProperties("index2", 1, 1, false, false, 0));
        shardMetrics.put(new ShardId(index2, 0), sizeRelatedShardMetrics);

        // Create stats for index1 with search load 100
        ShardStats index1PrimaryShard = createShardStats("index-node", new ShardId(index1, 0), true, 0.01);
        ShardStats index1SearchShard = createShardStats("search-node", new ShardId(index1, 0), false, 100.0);
        IndexStats index1Stats = new IndexStats(
            index1.getName(),
            index1.getUUID(),
            null,
            IndexMetadata.State.OPEN,
            new ShardStats[] { index1SearchShard, index1PrimaryShard }
        );

        // Create stats for index2 with search load 200
        ShardStats index2PrimaryShard = createShardStats("index-node", new ShardId(index2, 0), true, 0.01);
        ShardStats index2SearchShard = createShardStats("search-node", new ShardId(index2, 0), false, 200.0);
        IndexStats index2Stats = new IndexStats(
            index2.getName(),
            index2.getUUID(),
            null,
            IndexMetadata.State.OPEN,
            new ShardStats[] { index2SearchShard, index2PrimaryShard }
        );

        Map<String, IndexStats> indicesStatsMap = Map.of(index1.getName(), index1Stats, index2.getName(), index2Stats);

        ReplicaRankingContext context = new ReplicaRankingContext(indicesMap, shardMetrics, 100);
        Map<String, Double> result = getIndicesRelativeSearchLoads(context, indicesStatsMap::get);
        assertThat(result.size(), is(2));
        assertThat(result.get("index1"), closeTo(100.0 / 300.0, 0.01));
        assertThat(result.get("index2"), closeTo(200.0 / 300.0, 0.01));
    }

    public void testGetIndicesRelativeSearchLoadsWhenNoLoad() {
        Map<Index, SearchMetricsService.IndexProperties> indicesMap = new HashMap<>();
        Map<ShardId, SearchMetricsService.ShardMetrics> shardMetrics = new HashMap<>();
        SearchMetricsService.ShardMetrics sizeRelatedShardMetrics = new SearchMetricsService.ShardMetrics();
        sizeRelatedShardMetrics.shardSize = new ShardSizeStatsReader.ShardSize(100, 150, 0, 0);

        Index index1 = new Index("index1", "uuid");
        indicesMap.put(index1, new SearchMetricsService.IndexProperties("index1", 1, 1, false, false, 0));
        shardMetrics.put(new ShardId(index1, 0), sizeRelatedShardMetrics);
        Index index2 = new Index("index2", "uuid2");
        indicesMap.put(index2, new SearchMetricsService.IndexProperties("index2", 1, 1, false, false, 0));
        shardMetrics.put(new ShardId(index2, 0), sizeRelatedShardMetrics);

        ShardStats index1PrimaryShard = createShardStats("index-node", new ShardId(index1, 0), true, 0.01);
        ShardStats index1SearchShard = createShardStats("search-node", new ShardId(index1, 0), false, 0.0);
        IndexStats index1Stats = new IndexStats(
            index1.getName(),
            index1.getUUID(),
            null,
            IndexMetadata.State.OPEN,
            new ShardStats[] { index1SearchShard, index1PrimaryShard }
        );

        ShardStats index2PrimaryShard = createShardStats("index-node", new ShardId(index2, 0), true, 0.01);
        ShardStats index2SearchShard = createShardStats("search-node", new ShardId(index2, 0), false, 0.00001);
        IndexStats index2Stats = new IndexStats(
            index2.getName(),
            index2.getUUID(),
            null,
            IndexMetadata.State.OPEN,
            new ShardStats[] { index2SearchShard, index2PrimaryShard }
        );
        Map<String, IndexStats> indicesStatsMap = Map.of(index1.getName(), index1Stats, index2.getName(), index2Stats);
        ReplicaRankingContext context = new ReplicaRankingContext(indicesMap, shardMetrics, 100);

        Map<String, Double> result = getIndicesRelativeSearchLoads(context, indicesStatsMap::get);
        assertThat(result.size(), is(2));
        assertThat(result.get("index1"), closeTo(0.0, 0.01));
        assertThat(result.get("index2"), closeTo(1.0, 0.01));
    }

    public void testGetIndicesRelativeSearchLoadsFiltersPrimaryShardsOut() {
        Map<Index, SearchMetricsService.IndexProperties> indicesMap = new HashMap<>();
        Map<ShardId, SearchMetricsService.ShardMetrics> shardMetrics = new HashMap<>();
        SearchMetricsService.ShardMetrics sizeRelatedShardMetrics = new SearchMetricsService.ShardMetrics();
        sizeRelatedShardMetrics.shardSize = new ShardSizeStatsReader.ShardSize(100, 150, 0, 0);

        Index index1 = new Index("index1", "uuid");
        indicesMap.put(index1, new SearchMetricsService.IndexProperties("index1", 1, 1, false, false, 0));
        shardMetrics.put(new ShardId(index1, 0), sizeRelatedShardMetrics);
        Index index2 = new Index("index2", "uuid2");
        indicesMap.put(index2, new SearchMetricsService.IndexProperties("index2", 1, 1, false, false, 0));
        shardMetrics.put(new ShardId(index2, 0), sizeRelatedShardMetrics);

        ShardStats index1PrimaryShard = createShardStats("index-node", new ShardId(index1, 0), true, 400.0);
        ShardStats index1SearchShard = createShardStats("search-node", new ShardId(index1, 0), false, 100.0);
        IndexStats index1Stats = new IndexStats(
            index1.getName(),
            index1.getUUID(),
            null,
            IndexMetadata.State.OPEN,
            new ShardStats[] { index1SearchShard, index1PrimaryShard }
        );

        ShardStats index2PrimaryShard = createShardStats("index-node", new ShardId(index2, 0), true, 500.0);
        ShardStats index2SearchShard = createShardStats("search-node", new ShardId(index2, 0), false, 200.0);
        IndexStats index2Stats = new IndexStats(
            index2.getName(),
            index2.getUUID(),
            null,
            IndexMetadata.State.OPEN,
            new ShardStats[] { index2SearchShard, index2PrimaryShard }
        );

        Map<String, IndexStats> indicesStatsMap = Map.of(index1.getName(), index1Stats, index2.getName(), index2Stats);

        ReplicaRankingContext context = new ReplicaRankingContext(indicesMap, shardMetrics, 100);
        Map<String, Double> result = getIndicesRelativeSearchLoads(context, indicesStatsMap::get);
        assertThat(result.size(), is(2));
        assertThat(result.get("index1"), closeTo(100.0 / 300.0, 0.01));
        assertThat(result.get("index2"), closeTo(200.0 / 300.0, 0.01));
    }

    public void testRelativeSearchLoadsSumToOne() {
        Map<Index, SearchMetricsService.IndexProperties> indicesMap = new HashMap<>();
        Map<ShardId, SearchMetricsService.ShardMetrics> shardMetrics = new HashMap<>();
        SearchMetricsService.ShardMetrics sizeRelatedShardMetrics = new SearchMetricsService.ShardMetrics();
        sizeRelatedShardMetrics.shardSize = new ShardSizeStatsReader.ShardSize(100, 150, 0, 0);

        int numIndices = randomIntBetween(10, 100);
        Map<String, IndexStats> indicesStatsMap = new HashMap<>();
        for (int i = 0; i < numIndices; i++) {
            Index index = new Index("index" + i, "uuid" + i);
            indicesMap.put(index, new SearchMetricsService.IndexProperties(index.getName(), 1, 1, false, false, 0));
            shardMetrics.put(new ShardId(index, 0), sizeRelatedShardMetrics);

            double searchLoad = randomDoubleBetween(0.0, 1000.0, true);
            ShardStats primaryShard = createShardStats("index-node", new ShardId(index, 0), true, 0.01);
            ShardStats searchShard = createShardStats("search-node", new ShardId(index, 0), false, searchLoad);
            IndexStats indexStats = new IndexStats(
                index.getName(),
                index.getUUID(),
                null,
                IndexMetadata.State.OPEN,
                new ShardStats[] { searchShard, primaryShard }
            );
            indicesStatsMap.put(index.getName(), indexStats);
        }

        ReplicaRankingContext context = new ReplicaRankingContext(indicesMap, shardMetrics, 100);
        Map<String, Double> result = getIndicesRelativeSearchLoads(context, indicesStatsMap::get);
        double total = result.values().stream().mapToDouble(Double::doubleValue).sum();
        assertThat(total, closeTo(1.0, 0.0001));
    }

    public void testLimitReplicasToCurrentTopologyWhenDisabled() {
        scaler.updateEnableReplicasForLoadBalancing(false);

        Map<Index, SearchMetricsService.IndexProperties> indicesMap = new HashMap<>();
        Index index = new Index("index1", "uuid");
        indicesMap.put(index, new SearchMetricsService.IndexProperties("index1", 1, 5, false, false, 0));

        ReplicaRankingContext context = new ReplicaRankingContext(indicesMap, Map.of(), 100);
        DesiredClusterTopology topology = new DesiredClusterTopology(randomTierTopology(), randomTierTopology());

        Map<String, Integer> result = scaler.limitReplicasToCurrentTopology(context, topology, 8, randomIntBetween(2, 7));
        assertThat(result.isEmpty(), is(true));
    }

    public void testLimitReplicasToCurrentTopologyNoScaleDownNeeded() {
        Map<Index, SearchMetricsService.IndexProperties> indicesMap = new HashMap<>();
        Index index1 = new Index("index1", "uuid1");
        indicesMap.put(index1, new SearchMetricsService.IndexProperties("index1", 1, 3, false, false, 0));
        Index index2 = new Index("index2", "uuid2");
        indicesMap.put(index2, new SearchMetricsService.IndexProperties("index2", 1, 5, false, false, 0));

        ReplicaRankingContext context = new ReplicaRankingContext(indicesMap, Map.of(), 100);
        DesiredClusterTopology topology = new DesiredClusterTopology(
            new DesiredClusterTopology.TierTopology(10, "8Gi", randomFloat(), randomFloat(), randomFloat()),
            randomTierTopology()
        );

        Map<String, Integer> result = scaler.limitReplicasToCurrentTopology(context, topology, 10, 0);
        assertThat(result.isEmpty(), is(true));
    }

    public void testLimitReplicasToCurrentTopologyScaleDownToCurrentAliveNodes() {
        Map<Index, SearchMetricsService.IndexProperties> indicesMap = new HashMap<>();
        Index index1 = new Index("index1", "uuid1");
        indicesMap.put(index1, new SearchMetricsService.IndexProperties("index1", 1, 8, false, false, 0));

        ReplicaRankingContext context = new ReplicaRankingContext(indicesMap, Map.of(), 100);
        DesiredClusterTopology topology = new DesiredClusterTopology(
            new DesiredClusterTopology.TierTopology(5, "8Gi", randomFloat(), randomFloat(), randomFloat()),
            randomTierTopology()
        );

        Map<String, Integer> result = scaler.limitReplicasToCurrentTopology(context, topology, 8, 2);
        assertThat(result.size(), is(1));
        assertThat(result.get("index1"), is(6));
    }

    public void testLimitReplicasToCurrentTopologyMultipleIndices() {
        Map<Index, SearchMetricsService.IndexProperties> indicesMap = new HashMap<>();
        Index index1 = new Index("index1", "uuid1");
        indicesMap.put(index1, new SearchMetricsService.IndexProperties("index1", 1, 8, false, false, 0));
        Index index2 = new Index("index2", "uuid2");
        indicesMap.put(index2, new SearchMetricsService.IndexProperties("index2", 1, 12, false, false, 0));
        Index index3 = new Index("index3", "uuid3");
        indicesMap.put(index3, new SearchMetricsService.IndexProperties("index3", 1, 5, false, false, 0));

        ReplicaRankingContext context = new ReplicaRankingContext(indicesMap, Map.of(), 100);
        DesiredClusterTopology topology = new DesiredClusterTopology(
            new DesiredClusterTopology.TierTopology(6, "8Gi", randomFloat(), randomFloat(), randomFloat()),
            randomTierTopology()
        );

        Map<String, Integer> result = scaler.limitReplicasToCurrentTopology(context, topology, 10, 3);
        assertThat(result.size(), is(2));
        // index 1 and index 2 can remain on 7 replicas a bit longer as we have 7 alive nodes currentlya (despite the desired search
        // replicas being 6 - we're in the middle of a scale down event but we don't want to be too aggressive removing replicas)
        assertThat(result.get("index1"), is(7));
        assertThat(result.get("index2"), is(7));
        assertThat(result.containsKey("index3"), is(false));
    }

    public void testAllSearchNodesAreMarkedForShutdown() {
        Map<Index, SearchMetricsService.IndexProperties> indicesMap = new HashMap<>();
        Index index1 = new Index("index1", "uuid1");
        indicesMap.put(index1, new SearchMetricsService.IndexProperties("index1", 1, 5, false, false, 0));

        ReplicaRankingContext context = new ReplicaRankingContext(indicesMap, Map.of(), 100);
        DesiredClusterTopology topology = new DesiredClusterTopology(
            new DesiredClusterTopology.TierTopology(10, "8Gi", randomFloat(), randomFloat(), randomFloat()),
            randomTierTopology()
        );

        Map<String, Integer> result = scaler.limitReplicasToCurrentTopology(context, topology, 3, 3);
        // even though the all current nodes in the cluster are shutting down, the desired topology indicates we are on 10 search nodes so
        // nothing has to change for index1 that has 5 replicas (it's within the topology bounds already)
        assertThat(result.size(), is(0));
    }

    public void testCalculateDesiredReplicasBasicScenario() {
        Map<Index, SearchMetricsService.IndexProperties> indicesMap = new HashMap<>();
        Map<ShardId, SearchMetricsService.ShardMetrics> shardMetrics = new HashMap<>();
        SearchMetricsService.ShardMetrics sizeRelatedShardMetrics = new SearchMetricsService.ShardMetrics();
        sizeRelatedShardMetrics.shardSize = new ShardSizeStatsReader.ShardSize(100, 150, 0, 0);

        // index1 with high relative load (60%)
        Index index1 = new Index("index1", "uuid1");
        indicesMap.put(index1, new SearchMetricsService.IndexProperties("index1", 2, 3, false, false, 0));
        shardMetrics.put(new ShardId(index1, 0), sizeRelatedShardMetrics);

        // index2 with moderate relative load (30%)
        Index index2 = new Index("index2", "uuid2");
        indicesMap.put(index2, new SearchMetricsService.IndexProperties("index2", 1, 2, false, false, 0));
        shardMetrics.put(new ShardId(index2, 0), sizeRelatedShardMetrics);

        // index3 with low relative load (10%)
        Index index3 = new Index("index3", "uuid3");
        indicesMap.put(index3, new SearchMetricsService.IndexProperties("index3", 1, 1, false, false, 0));
        shardMetrics.put(new ShardId(index3, 0), sizeRelatedShardMetrics);

        ReplicaRankingContext context = new ReplicaRankingContext(indicesMap, shardMetrics, 100);
        Map<String, Double> relativeSearchLoads = Map.of("index1", 0.6, "index2", 0.3, "index3", 0.1);
        Map<String, Integer> immediateScaleDown = Map.of();
        DesiredClusterTopology topology = new DesiredClusterTopology(
            new DesiredClusterTopology.TierTopology(10, "8Gi", randomFloat(), randomFloat(), randomFloat()),
            randomTierTopology()
        );

        ReplicasLoadBalancingScaler.ReplicasLoadBalancingResult result = scaler.calculateDesiredReplicas(
            context,
            relativeSearchLoads,
            immediateScaleDown,
            topology,
            0
        );

        // with maxReplicaRelativeSearchLoad = 0.5:
        // index1: ceil(0.6 * 10 / (2 * 0.5)) = ceil(6.0) = 6 replicas (currently 3, should increase)
        // index2: ceil(0.3 * 10 / (1 * 0.5)) = ceil(6.0) = 6 replicas (currently 2, should increase)
        // index3: ceil(0.1 * 10 / (1 * 0.5)) = ceil(2.0) = 2 replicas (currently 1, should increase)
        assertThat(result.immediateReplicaScaleDown().isEmpty(), is(true));
        assertThat(result.desiredReplicasPerIndex().get("index1"), is(6));
        assertThat(result.desiredReplicasPerIndex().get("index2"), is(6));
        assertThat(result.desiredReplicasPerIndex().get("index3"), is(2));
    }

    public void testCalculateDesiredReplicasSkipsIndicesThatMustScaleDown() {
        Map<Index, SearchMetricsService.IndexProperties> indicesMap = new HashMap<>();
        Index index1 = new Index("index1", "uuid1");
        indicesMap.put(index1, new SearchMetricsService.IndexProperties("index1", 1, 5, false, false, 0));
        Index index2 = new Index("index2", "uuid2");
        indicesMap.put(index2, new SearchMetricsService.IndexProperties("index2", 1, 3, false, false, 0));

        ReplicaRankingContext context = new ReplicaRankingContext(indicesMap, Map.of(), 100);
        Map<String, Double> relativeSearchLoads = Map.of("index1", 0.7, "index2", 0.3);
        // even though index1 has high relative load and would normally receive more shards, it must scale down immediately
        Map<String, Integer> immediateScaleDown = Map.of("index1", 4);
        DesiredClusterTopology topology = new DesiredClusterTopology(
            new DesiredClusterTopology.TierTopology(10, "8Gi", randomFloat(), randomFloat(), randomFloat()),
            randomTierTopology()
        );

        ReplicasLoadBalancingScaler.ReplicasLoadBalancingResult result = scaler.calculateDesiredReplicas(
            context,
            relativeSearchLoads,
            immediateScaleDown,
            topology,
            0
        );

        // index1 should not appear in desiredReplicasPerIndex as it's in immediateScaleDown
        assertThat(result.desiredReplicasPerIndex().containsKey("index1"), is(false));
        assertThat(result.desiredReplicasPerIndex().containsKey("index2"), is(true));
    }

    public void testCalculateDesiredReplicasPreventsScaleUpDuringShutdown() {
        Map<Index, SearchMetricsService.IndexProperties> indicesMap = new HashMap<>();
        Index index1 = new Index("index1", "uuid1");
        indicesMap.put(index1, new SearchMetricsService.IndexProperties("index1", 1, 2, false, false, 0));

        ReplicaRankingContext context = new ReplicaRankingContext(indicesMap, Map.of(), 100);
        Map<String, Double> relativeSearchLoads = Map.of("index1", 0.8);
        DesiredClusterTopology topology = new DesiredClusterTopology(
            new DesiredClusterTopology.TierTopology(10, "8Gi", randomFloat(), randomFloat(), randomFloat()),
            randomTierTopology()
        );

        ReplicasLoadBalancingScaler.ReplicasLoadBalancingResult result = scaler.calculateDesiredReplicas(
            context,
            relativeSearchLoads,
            Map.of(),
            topology,
            2 // shutdowns in progress
        );

        // desired would be ceil(0.8 * 10 / (1 * 0.5)) = 16, but limited to 10 (desiredSearchNodes)
        // however, with shutdowns in progress and current replicas = 2, should stay at 2
        assertThat(result.desiredReplicasPerIndex().isEmpty(), is(true));
    }

    public void testCalculateDesiredReplicasAllowsScaleDownDuringShutdown() {
        Map<Index, SearchMetricsService.IndexProperties> indicesMap = new HashMap<>();
        Index index1 = new Index("index1", "uuid1");
        indicesMap.put(index1, new SearchMetricsService.IndexProperties("index1", 1, 8, false, false, 0));

        ReplicaRankingContext context = new ReplicaRankingContext(indicesMap, Map.of(), 100);
        Map<String, Double> relativeSearchLoads = Map.of("index1", 0.2);
        DesiredClusterTopology topology = new DesiredClusterTopology(
            new DesiredClusterTopology.TierTopology(10, "8Gi", randomFloat(), randomFloat(), randomFloat()),
            randomTierTopology()
        );

        ReplicasLoadBalancingScaler.ReplicasLoadBalancingResult result = scaler.calculateDesiredReplicas(
            context,
            relativeSearchLoads,
            Map.of(),
            topology,
            2 // shutdowns in progress
        );

        // desired: ceil(0.2 * 10 / (1 * 0.5)) = 4, current is 8, so scale down is allowed
        assertThat(result.desiredReplicasPerIndex().get("index1"), is(4));
    }

    public void testCalculateDesiredReplicasRespectsDesiredSearchNodesLimit() {
        Map<Index, SearchMetricsService.IndexProperties> indicesMap = new HashMap<>();
        Index index1 = new Index("index1", "uuid1");
        indicesMap.put(index1, new SearchMetricsService.IndexProperties("index1", 1, 3, false, false, 0));

        ReplicaRankingContext context = new ReplicaRankingContext(indicesMap, Map.of(), 100);
        Map<String, Double> relativeSearchLoads = Map.of("index1", 1.0);
        DesiredClusterTopology topology = new DesiredClusterTopology(
            new DesiredClusterTopology.TierTopology(5, "8Gi", randomFloat(), randomFloat(), randomFloat()),
            randomTierTopology()
        );

        ReplicasLoadBalancingScaler.ReplicasLoadBalancingResult result = scaler.calculateDesiredReplicas(
            context,
            relativeSearchLoads,
            Map.of(),
            topology,
            0
        );

        // formula gives: ceil(1.0 * 5 / (1 * 0.5)) = 10, but capped at desiredSearchNodes = 5
        assertThat(result.desiredReplicasPerIndex().get("index1"), is(5));
    }

    public void testCalculateDesiredReplicasNoChangeWhenMatching() {
        Map<Index, SearchMetricsService.IndexProperties> indicesMap = new HashMap<>();
        Index index1 = new Index("index1", "uuid1");
        indicesMap.put(index1, new SearchMetricsService.IndexProperties("index1", 1, 4, false, false, 0));

        ReplicaRankingContext context = new ReplicaRankingContext(indicesMap, Map.of(), 100);
        Map<String, Double> relativeSearchLoads = Map.of("index1", 0.2);
        DesiredClusterTopology topology = new DesiredClusterTopology(
            new DesiredClusterTopology.TierTopology(10, "8Gi", randomFloat(), randomFloat(), randomFloat()),
            randomTierTopology()
        );

        ReplicasLoadBalancingScaler.ReplicasLoadBalancingResult result = scaler.calculateDesiredReplicas(
            context,
            relativeSearchLoads,
            Map.of(),
            topology,
            0
        );

        // desired: ceil(0.2 * 10 / (1 * 0.5)) = 4, matches current, no change needed
        assertThat(result.desiredReplicasPerIndex().isEmpty(), is(true));
    }

    public void testGetRecommendedReplicasWhenServiceDisabled() {
        scaler.updateEnableReplicasForLoadBalancing(false);

        ClusterState state = ClusterServiceUtils.createClusterService(testThreadPool, createClusterSettings()).state();
        ReplicaRankingContext context = new ReplicaRankingContext(Map.of(), Map.of(), 100);
        DesiredClusterTopology topology = new DesiredClusterTopology(randomTierTopology(), randomTierTopology());

        PlainActionFuture<ReplicasLoadBalancingScaler.ReplicasLoadBalancingResult> future = new PlainActionFuture<>();
        scaler.getRecommendedReplicas(state, context, topology, randomBoolean(), future);

        ReplicasLoadBalancingScaler.ReplicasLoadBalancingResult result = future.actionGet();
        assertThat(result, is(ReplicasLoadBalancingScaler.EMPTY_RESULT));
        assertThat(client.executionCount.get(), is(0));
    }

    public void testGetRecommendedReplicasOnlyTopologyCheck() {
        Map<Index, SearchMetricsService.IndexProperties> indicesMap = new HashMap<>();
        Index index1 = new Index("index1", "uuid1");
        indicesMap.put(index1, new SearchMetricsService.IndexProperties("index1", 1, 8, false, false, 0));

        ClusterState state = ClusterServiceUtils.createClusterService(testThreadPool, createClusterSettings()).state();
        ReplicaRankingContext context = new ReplicaRankingContext(indicesMap, Map.of(), 100);
        DesiredClusterTopology topology = new DesiredClusterTopology(
            new DesiredClusterTopology.TierTopology(5, "8Gi", randomFloat(), randomFloat(), randomFloat()),
            randomTierTopology()
        );

        PlainActionFuture<ReplicasLoadBalancingScaler.ReplicasLoadBalancingResult> future = new PlainActionFuture<>();
        scaler.getRecommendedReplicas(state, context, topology, true, future);

        ReplicasLoadBalancingScaler.ReplicasLoadBalancingResult result = future.actionGet();
        // should have immediate scale down but no desired replicas calculation
        assertThat(result.immediateReplicaScaleDown().containsKey("index1"), is(true));
        assertThat(result.desiredReplicasPerIndex().isEmpty(), is(true));
        assertThat(client.executionCount.get(), is(0));
    }

    public void testGetRecommendedReplicasFullCalculation() {
        Map<Index, SearchMetricsService.IndexProperties> indicesMap = new HashMap<>();
        Map<ShardId, SearchMetricsService.ShardMetrics> shardMetrics = new HashMap<>();
        SearchMetricsService.ShardMetrics sizeRelatedShardMetrics = new SearchMetricsService.ShardMetrics();
        sizeRelatedShardMetrics.shardSize = new ShardSizeStatsReader.ShardSize(100, 150, 0, 0);

        Index index1 = new Index("index1", "uuid1");
        indicesMap.put(index1, new SearchMetricsService.IndexProperties("index1", 1, 3, false, false, 0));
        shardMetrics.put(new ShardId(index1, 0), sizeRelatedShardMetrics);

        Index index2 = new Index("index2", "uuid2");
        indicesMap.put(index2, new SearchMetricsService.IndexProperties("index2", 1, 2, false, false, 0));
        shardMetrics.put(new ShardId(index2, 0), sizeRelatedShardMetrics);

        // Create stats for indices
        ShardStats index1PrimaryShard = createShardStats("index-node", new ShardId(index1, 0), true, 0.01);
        ShardStats index1SearchShard = createShardStats("search-node", new ShardId(index1, 0), false, 300.0);
        IndexStats index1Stats = new IndexStats(
            index1.getName(),
            index1.getUUID(),
            null,
            IndexMetadata.State.OPEN,
            new ShardStats[] { index1SearchShard, index1PrimaryShard }
        );

        ShardStats index2PrimaryShard = createShardStats("index-node", new ShardId(index2, 0), true, 0.01);
        ShardStats index2SearchShard = createShardStats("search-node", new ShardId(index2, 0), false, 200.0);
        IndexStats index2Stats = new IndexStats(
            index2.getName(),
            index2.getUUID(),
            null,
            IndexMetadata.State.OPEN,
            new ShardStats[] { index2SearchShard, index2PrimaryShard }
        );

        Map<String, IndexStats> indicesStatsMap = Map.of(index1.getName(), index1Stats, index2.getName(), index2Stats);

        client.setIndicesStatsResponse(indicesStatsMap);

        ClusterState state = ClusterServiceUtils.createClusterService(testThreadPool, createClusterSettings()).state();
        ReplicaRankingContext context = new ReplicaRankingContext(indicesMap, shardMetrics, 100);
        DesiredClusterTopology topology = new DesiredClusterTopology(
            new DesiredClusterTopology.TierTopology(10, "8Gi", randomFloat(), randomFloat(), randomFloat()),
            randomTierTopology()
        );

        PlainActionFuture<ReplicasLoadBalancingScaler.ReplicasLoadBalancingResult> future = new PlainActionFuture<>();
        scaler.getRecommendedReplicas(state, context, topology, false, future);

        ReplicasLoadBalancingScaler.ReplicasLoadBalancingResult result = future.actionGet();
        assertThat(client.executionCount.get(), is(1));
        assertThat(result.immediateReplicaScaleDown().size(), is(0));
        // both indices should receive more than the initial replicas due to the high search load and availability in the search tier (10
        // nodes)
        Map<String, Integer> desiredReplicas = result.desiredReplicasPerIndex();
        assertThat(desiredReplicas.size(), is(2));
        assertThat(desiredReplicas.get("index1"), is(10)); // index1: ceil(0.6 * 10 / (1 * 0.5)) = 12 (capped at 10 due to topology)
        assertThat(desiredReplicas.get("index2"), is(8)); // index2: ceil(0.4 * 10 / (1 * 0.5)) = 8
    }

    public void testGetRecommendedReplicasWithStatsFailure() {
        Map<Index, SearchMetricsService.IndexProperties> indicesMap = new HashMap<>();
        Index index1 = new Index("index1", "uuid1");
        indicesMap.put(index1, new SearchMetricsService.IndexProperties("index1", 1, 8, false, false, 0));

        client.setFailure(new RuntimeException("stats failed"));

        ClusterState state = ClusterServiceUtils.createClusterService(testThreadPool, createClusterSettings()).state();
        ReplicaRankingContext context = new ReplicaRankingContext(indicesMap, Map.of(), 100);
        DesiredClusterTopology topology = new DesiredClusterTopology(
            new DesiredClusterTopology.TierTopology(5, "8Gi", randomFloat(), randomFloat(), randomFloat()),
            randomTierTopology()
        );

        PlainActionFuture<ReplicasLoadBalancingScaler.ReplicasLoadBalancingResult> future = new PlainActionFuture<>();
        scaler.getRecommendedReplicas(state, context, topology, false, future);

        ReplicasLoadBalancingScaler.ReplicasLoadBalancingResult result = future.actionGet();
        // should return immediate scale down but no desired replicas on failure (always return the immediate scale down stuff to make sure
        // we stay within the topology limits)
        assertThat(result.immediateReplicaScaleDown().containsKey("index1"), is(true));
        assertThat(result.desiredReplicasPerIndex().isEmpty(), is(true));
    }

    private static class MockClient extends NoOpNodeClient {
        final AtomicInteger executionCount = new AtomicInteger(0);
        private Map<String, IndexStats> indicesStatsResponse;
        private Exception failure;

        MockClient(ThreadPool threadPool) {
            super(threadPool);
        }

        void setIndicesStatsResponse(Map<String, IndexStats> response) {
            this.indicesStatsResponse = response;
            this.failure = null;
        }

        void setFailure(Exception failure) {
            this.failure = failure;
            this.indicesStatsResponse = null;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            assertThat(request, instanceOf(IndicesStatsRequest.class));
            IndicesStatsRequest statsRequest = (IndicesStatsRequest) request;
            CommonStatsFlags.Flag[] flags = statsRequest.flags().getFlags();
            assertThat(flags.length, is(1));
            assertThat(flags[0], is(CommonStatsFlags.Flag.Search));
            executionCount.incrementAndGet();

            if (failure != null) {
                listener.onFailure(failure);
            } else if (indicesStatsResponse != null) {
                // IndicesStatsResponse is packed neatly, which is great (final and package private constructors so we need to mock it)
                IndicesStatsResponse statsResponse = mock(IndicesStatsResponse.class);
                Mockito.doAnswer(invocation -> {
                    String indexName = invocation.getArgument(0);
                    return indicesStatsResponse.get(indexName);
                }).when(statsResponse).getIndex(Mockito.anyString());
                listener.onResponse((Response) statsResponse);
            }
            super.doExecute(action, request, listener);
        }
    }

    private static ShardStats createShardStats(String nodeId, ShardId shardId, boolean primary, double recentSearchLoad) {
        ShardRouting shardRouting = ShardRouting.newUnassigned(
            shardId,
            primary,
            primary ? RecoverySource.EmptyStoreRecoverySource.INSTANCE : RecoverySource.PeerRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "message"),
            primary ? ShardRouting.Role.INDEX_ONLY : ShardRouting.Role.SEARCH_ONLY
        );
        shardRouting = shardRouting.initialize(nodeId, null, shardRouting.getExpectedShardSize());
        return shardStat(shardRouting, recentSearchLoad);
    }

    private static ShardStats shardStat(ShardRouting routing, double recentSearchLoad) {
        Path fakePath = PathUtils.get("test/dir/" + routing.shardId().getIndex().getUUID() + "/" + routing.shardId().id());
        ShardPath fakeShardPath = new ShardPath(false, fakePath, fakePath, routing.shardId());
        CommonStats commonStats = new CommonStats(CommonStatsFlags.ALL);
        SearchStats.Stats stats = new SearchStats.Stats(
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            recentSearchLoad
        );
        Map<String, SearchStats.Stats> groupStats = new HashMap<>();
        groupStats.put("group", stats);

        SearchStats searchStats = new SearchStats(stats, randomNonNegativeLong(), groupStats);
        commonStats.getSearch().add(searchStats);
        return new ShardStats(routing, fakeShardPath, commonStats, null, null, null, false, 0);
    }

    private static ClusterSettings createClusterSettings() {
        Set<Setting<?>> defaultClusterSettings = Sets.addToCopy(
            ClusterSettings.BUILT_IN_CLUSTER_SETTINGS,
            ServerlessSharedSettings.ENABLE_REPLICAS_LOAD_BALANCING,
            ReplicasLoadBalancingScaler.MAX_REPLICA_RELATIVE_SEARCH_LOAD
        );
        return new ClusterSettings(
            Settings.builder().put(ServerlessSharedSettings.ENABLE_REPLICAS_FOR_INSTANT_FAILOVER.getKey(), true).build(),
            defaultClusterSettings
        );
    }
}
