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

    private static SearchMetricsService.ShardMetrics shardMetricOf(long interactiveSize) {
        return shardMetricOf(interactiveSize, 0);
    }

    private static SearchMetricsService.ShardMetrics shardMetricOf(long interactiveSize, long nonInteractiveSize) {
        SearchMetricsService.ShardMetrics sm = new SearchMetricsService.ShardMetrics();
        sm.shardSize = new ShardSize(interactiveSize, nonInteractiveSize, 0, 0);
        return sm;
    }
}
