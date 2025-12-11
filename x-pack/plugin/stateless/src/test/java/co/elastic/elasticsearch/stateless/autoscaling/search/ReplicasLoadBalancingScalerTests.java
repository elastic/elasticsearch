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

import co.elastic.elasticsearch.stateless.api.ShardSizeStatsReader;

import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.search.stats.SearchStats;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static co.elastic.elasticsearch.stateless.autoscaling.search.ReplicasLoadBalancingScaler.getIndicesRelativeSearchLoads;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.is;

public class ReplicasLoadBalancingScalerTests extends ESTestCase {

    public void testGetIndicesRelativeSearchLoadsWithSearchLoad() throws Exception {
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

    public void testGetIndicesRelativeSearchLoadsWhenNoLoad() throws Exception {
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

    public void testGetIndicesRelativeSearchLoadsFiltersPrimaryShardsOut() throws Exception {
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

    private static ShardStats createShardStats(String nodeId, ShardId shardId, boolean primary, double recentSearchLoad)
        throws IOException {
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
}
