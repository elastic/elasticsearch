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

import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

class ReplicaRankingContext {

    private final Map<String, IndexReplicationRanker.IndexRankingProperties> rankingProperties = new HashMap<>();

    private final long allIndicesInteractiveSize;

    private final int searchPowerMin;

    ReplicaRankingContext(
        Map<Index, SearchMetricsService.IndexProperties> indicesMap,
        Map<ShardId, SearchMetricsService.ShardMetrics> shardMetricsMap,
        int searchPowerMin
    ) {
        this.searchPowerMin = searchPowerMin;
        long allIndicesInteractiveSize = 0;
        for (Map.Entry<Index, SearchMetricsService.IndexProperties> entry : indicesMap.entrySet()) {
            Index index = entry.getKey();
            SearchMetricsService.IndexProperties indexProperties = entry.getValue();
            long totalIndexInteractiveSize = 0;
            for (int i = 0; i < indexProperties.shards(); i++) {
                SearchMetricsService.ShardMetrics shardMetrics = shardMetricsMap.get(new ShardId(index, i));
                if (shardMetrics == null) {
                    // continue with the next shard, this one might be removed or have 0 replicas but it
                    // can also be from a tiny index with skewed document distribution and some shards
                    // not having any documents in them
                    continue;
                }
                totalIndexInteractiveSize += shardMetrics.shardSize.interactiveSizeInBytes();
            }
            rankingProperties.put(
                index.getName(),
                new IndexReplicationRanker.IndexRankingProperties(indexProperties, totalIndexInteractiveSize)
            );
            allIndicesInteractiveSize += totalIndexInteractiveSize;

        }
        this.allIndicesInteractiveSize = allIndicesInteractiveSize;
    }

    Set<String> indices() {
        return rankingProperties.keySet();
    }

    Collection<IndexReplicationRanker.IndexRankingProperties> properties() {
        return rankingProperties.values();
    }

    long getThreshold() {
        return allIndicesInteractiveSize * Math.max(0, searchPowerMin - ReplicasUpdaterService.SEARCH_POWER_MIN_NO_REPLICATION)
            / (ReplicasUpdaterService.SEARCH_POWER_MIN_FULL_REPLICATION - ReplicasUpdaterService.SEARCH_POWER_MIN_NO_REPLICATION);
    }

    public int getSearchPowerMin() {
        return searchPowerMin;
    }

    /**
     * Calculates the average replica setting value across indices.
     * <p>
     * This provides insight into what amount of indices is scaled up currently and summarizes this in
     * a metric that can be compared and averaged over for multiple projects.
     *
     * @param onlyInteractive if set to true, only sum over all interactive indices, otherwise use all indices
     * @return The average number of replicas for all involved indices, e.g if half of them have only 1 replica and
     * the other half 2 we'll return 0.5.
     */
    public double calculateReplicationFactor(boolean onlyInteractive) {
        double ret = 0;
        int allReplicasSum = 0;
        int normalizer = 0;
        if (onlyInteractive) {
            allReplicasSum = rankingProperties.values()
                .stream()
                .filter(IndexReplicationRanker.IndexRankingProperties::isInteractive)
                .mapToInt(p -> p.indexProperties().replicas())
                .sum();
            normalizer = rankingProperties.values()
                .stream()
                .filter(IndexReplicationRanker.IndexRankingProperties::isInteractive)
                .collect(Collectors.counting())
                .intValue();
        } else {
            allReplicasSum = rankingProperties.values().stream().mapToInt(p -> p.indexProperties().replicas()).sum();
            normalizer = rankingProperties.size();
        }
        if (normalizer > 0) {
            ret = (double) allReplicasSum / normalizer;
        }
        return ret;
    }

    /**
     * <p>Calculates the over-/under-use of the allowed interactive size for indices with two replicas, given by the `threshold`
     * ({@see #getThreshold}).
     *
     * <p>This means:
     * <ul>
     * <li>For SP &lt;= 100, where under normal circumstances all indices only have one replica, this should be 0.0
     * <li>For SP >= 250 all interactive indices should have two replicas, but threshold and the interactive size across
     * all indices should be identical, so the expected value should also be 0.0
     * <li>For 100 &lt;= SP &lt; 250 we also should ideally have a usage factor of 0.0, because we never try
     * to fit in more cumulative index size than the threshold. In cases where we haven't scaled down
     * some indices yet (because of the intended delay in scaling down), we would see values in the range [0.0, 1.0] which
     * can be interpreted at the amount of memory we are over-using the intended extra memory for the second replias. In cases
     * where we are not able to use all the allowed space up to `threshold` we would see negative values in the range [-1.0, 0.0].
     * This can happen i.e. when indices that are next on the list of ranked indices are too large to fit into the remaining space.
     * </ul>
     */
    double calculateReplicationSizeOveruse() {
        double replicationSizeOveruse = 0.0;
        if (this.allIndicesInteractiveSize > 0) {
            long sumTwoReplicasInteractiveSize = rankingProperties.values()
                .stream()
                .filter(p -> p.indexProperties().replicas() == 2)
                .mapToLong(p -> p.interactiveSize())
                .sum();
            replicationSizeOveruse = ((double) sumTwoReplicasInteractiveSize - getThreshold()) / this.allIndicesInteractiveSize;
        }
        return replicationSizeOveruse;
    }

    public long getAllIndicesInteractiveSize() {
        return allIndicesInteractiveSize;
    }

    @Override
    public String toString() {
        return "rankingProperties: "
            + rankingProperties
            + ", allIndicesInteractiveSize: "
            + allIndicesInteractiveSize
            + ", SPmin: "
            + searchPowerMin;
    }
}
