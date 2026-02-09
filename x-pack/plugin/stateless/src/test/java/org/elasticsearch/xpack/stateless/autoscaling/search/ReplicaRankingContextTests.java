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

import co.elastic.elasticsearch.stateless.api.ShardSizeStatsReader.ShardSize;

import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;

public class ReplicaRankingContextTests extends ESTestCase {

    public void testRankingContext() {
        Map<Index, SearchMetricsService.IndexProperties> indicesMap = new HashMap<>();
        Map<ShardId, SearchMetricsService.ShardMetrics> shardMetrics = new HashMap<>();

        ReplicaRankingContext rankingContext = new ReplicaRankingContext(indicesMap, shardMetrics, 100);
        assertEquals(0.0, rankingContext.calculateReplicationFactor(false), Double.MIN_VALUE);
        assertEquals(0.0, rankingContext.calculateReplicationFactor(true), Double.MIN_VALUE);
        assertEquals(0.0, rankingContext.calculateReplicationSizeOveruse(), Double.MIN_VALUE);
        assertEquals(0, rankingContext.getThreshold());

        Index index1 = new Index("index1", "uuid");
        indicesMap.put(index1, new SearchMetricsService.IndexProperties("index1", 1, 1, false, false, 0));
        shardMetrics.put(new ShardId(index1, 0), shardMetricOf(1000));
        Index index2 = new Index("index2", "uuid");
        indicesMap.put(index2, new SearchMetricsService.IndexProperties("index2", 1, 1, false, false, 0));
        shardMetrics.put(new ShardId(index2, 0), shardMetricOf(1000));
        Index index3 = new Index("index3", "uuid");
        indicesMap.put(index3, new SearchMetricsService.IndexProperties("index2", 1, 1, false, false, 0));
        shardMetrics.put(new ShardId(index3, 0), shardMetricOf(0, 1000));

        rankingContext = new ReplicaRankingContext(indicesMap, shardMetrics, 100);
        assertEquals(1.0, rankingContext.calculateReplicationFactor(false), Double.MIN_VALUE);
        assertEquals(1.0, rankingContext.calculateReplicationFactor(true), Double.MIN_VALUE);
        assertEquals(0.0, rankingContext.calculateReplicationSizeOveruse(), Double.MIN_VALUE);
        assertEquals(0, rankingContext.getThreshold());

        rankingContext = new ReplicaRankingContext(indicesMap, shardMetrics, 175);
        assertEquals(1.0, rankingContext.calculateReplicationFactor(false), Double.MIN_VALUE);
        assertEquals(1.0, rankingContext.calculateReplicationFactor(true), Double.MIN_VALUE);
        assertEquals(-0.5, rankingContext.calculateReplicationSizeOveruse(), Double.MIN_VALUE);
        assertEquals(1000, rankingContext.getThreshold());

        rankingContext = new ReplicaRankingContext(indicesMap, shardMetrics, 250);
        assertEquals(1.0, rankingContext.calculateReplicationFactor(false), Double.MIN_VALUE);
        assertEquals(1.0, rankingContext.calculateReplicationFactor(true), Double.MIN_VALUE);
        assertEquals(-1.0, rankingContext.calculateReplicationSizeOveruse(), Double.MIN_VALUE);
        assertEquals(2000, rankingContext.getThreshold());

        indicesMap.put(index1, new SearchMetricsService.IndexProperties("index1", 1, 2, false, false, 0));
        rankingContext = new ReplicaRankingContext(indicesMap, shardMetrics, 175);
        assertEquals(4.0 / 3, rankingContext.calculateReplicationFactor(false), Double.MIN_VALUE);
        assertEquals(1.5, rankingContext.calculateReplicationFactor(true), Double.MIN_VALUE);
        assertEquals(0.0, rankingContext.calculateReplicationSizeOveruse(), Double.MIN_VALUE);
        assertEquals(1000, rankingContext.getThreshold());

        indicesMap.put(index2, new SearchMetricsService.IndexProperties("index2", 1, 2, false, false, 0));
        rankingContext = new ReplicaRankingContext(indicesMap, shardMetrics, 175);
        assertEquals(5.0 / 3, rankingContext.calculateReplicationFactor(false), Double.MIN_VALUE);
        assertEquals(2.0, rankingContext.calculateReplicationFactor(true), Double.MIN_VALUE);
        assertEquals(0.5, rankingContext.calculateReplicationSizeOveruse(), Double.MIN_VALUE);
        assertEquals(1000, rankingContext.getThreshold());

        rankingContext = new ReplicaRankingContext(indicesMap, shardMetrics, 250);
        assertEquals(5.0 / 3.0, rankingContext.calculateReplicationFactor(false), Double.MIN_VALUE);
        assertEquals(2.0, rankingContext.calculateReplicationFactor(true), Double.MIN_VALUE);
        assertEquals(0.0, rankingContext.calculateReplicationSizeOveruse(), Double.MIN_VALUE);
        assertEquals(2000, rankingContext.getThreshold());
    }

    public void testGetSumOfReplicasInteractiveSizes() {
        Map<Index, SearchMetricsService.IndexProperties> indicesMap = new HashMap<>();
        Map<ShardId, SearchMetricsService.ShardMetrics> shardMetrics = new HashMap<>();

        // Empty context
        ReplicaRankingContext rankingContext = new ReplicaRankingContext(indicesMap, shardMetrics, -1);
        assertEquals(0, rankingContext.getSumOfReplicasInteractiveSizes());

        // Single index with 1 replica
        Index index1 = new Index("index1", "uuid1");
        indicesMap.put(index1, new SearchMetricsService.IndexProperties("index1", 1, 1, false, false, 0));
        shardMetrics.put(new ShardId(index1, 0), shardMetricOf(1000));
        rankingContext = new ReplicaRankingContext(indicesMap, shardMetrics, -1);
        assertEquals(1, rankingContext.getTotalReplicas());
        assertEquals(1000, rankingContext.getSumOfReplicasInteractiveSizes());

        // Single index with 2 replicas
        indicesMap.put(index1, new SearchMetricsService.IndexProperties("index1", 1, 2, false, false, 0));
        rankingContext = new ReplicaRankingContext(indicesMap, shardMetrics, -1);
        assertEquals(2, rankingContext.getTotalReplicas());
        assertEquals(2000, rankingContext.getSumOfReplicasInteractiveSizes());

        // Multiple indices with different replica counts
        Index index2 = new Index("index2", "uuid2");
        indicesMap.put(index2, new SearchMetricsService.IndexProperties("index2", 1, 1, false, false, 0));
        shardMetrics.put(new ShardId(index2, 0), shardMetricOf(500));
        rankingContext = new ReplicaRankingContext(indicesMap, shardMetrics, -1);
        assertEquals(3, rankingContext.getTotalReplicas());
        assertEquals(2500, rankingContext.getSumOfReplicasInteractiveSizes());

        // Index with non-interactive size only (should contribute 0)
        Index index3 = new Index("index3", "uuid3");
        indicesMap.put(index3, new SearchMetricsService.IndexProperties("index3", 1, 2, false, false, 0));
        shardMetrics.put(new ShardId(index3, 0), shardMetricOf(0, 2000));
        rankingContext = new ReplicaRankingContext(indicesMap, shardMetrics, -1);
        assertEquals(5, rankingContext.getTotalReplicas());
        assertEquals(2500, rankingContext.getSumOfReplicasInteractiveSizes());

        // Multiple shards per index
        Index index4 = new Index("index4", "uuid4");
        indicesMap.put(index4, new SearchMetricsService.IndexProperties("index4", 3, 2, false, false, 0));
        shardMetrics.put(new ShardId(index4, 0), shardMetricOf(100));
        shardMetrics.put(new ShardId(index4, 1), shardMetricOf(200));
        shardMetrics.put(new ShardId(index4, 2), shardMetricOf(300));
        rankingContext = new ReplicaRankingContext(indicesMap, shardMetrics, -1);
        assertEquals(7, rankingContext.getTotalReplicas());
        assertEquals(3700, rankingContext.getSumOfReplicasInteractiveSizes());
    }

    private static SearchMetricsService.ShardMetrics shardMetricOf(long interactiveSize) {
        return shardMetricOf(interactiveSize, 0);
    }

    private static SearchMetricsService.ShardMetrics shardMetricOf(long interactiveSize, long nonInteractiveSize) {
        SearchMetricsService.ShardMetrics sm = new SearchMetricsService.ShardMetrics();
        sm.shardSize = new ShardSize(interactiveSize, nonInteractiveSize, 0, 0);
        return sm;
    }
}
