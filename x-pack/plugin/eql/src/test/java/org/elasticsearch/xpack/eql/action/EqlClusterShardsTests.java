/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.action;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.RemoteClusterAware;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

public class EqlClusterShardsTests extends ESTestCase {

    public void testLocalSearchUsesTopLevelCounts() {
        List<EqlClusterShards> shards = EqlClusterShards.from(4, 4, 1, 0, SearchResponse.Clusters.EMPTY);
        assertThat(shards, equalTo(List.of(new EqlClusterShards(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, 4, 3, 1, 0))));
    }

    public void testCrossClusterSearchUsesPerClusterCounts() {
        SearchResponse.Clusters clusters = new SearchResponse.Clusters(
            Map.of("", counted("", 2, 2, 0, 0), "remote", counted("remote", 5, 4, 1, 1))
        );
        List<EqlClusterShards> shards = EqlClusterShards.from(99, 99, 99, 99, clusters);
        assertThat(shards, containsInAnyOrder(new EqlClusterShards("", 2, 2, 0, 0), new EqlClusterShards("remote", 5, 3, 1, 1)));
    }

    public void testSearchFailuresAreCountedOncePerShard() {
        List<EqlClusterShards> pit = List.of(new EqlClusterShards("", 2, 2, 0, 0));
        SearchShardTarget shard = new SearchShardTarget("n1", new ShardId("logs", "_na_", 0), null);
        ShardSearchFailure first = new ShardSearchFailure(new IllegalStateException("lost"), shard);
        ShardSearchFailure again = new ShardSearchFailure(new IllegalStateException("lost again"), shard);
        List<EqlClusterShards> adjusted = EqlClusterShards.withSearchFailures(pit, List.of(first, again));
        assertThat(adjusted, equalTo(List.of(new EqlClusterShards("", 2, 1, 0, 1))));
    }

    public void testSuccessfulBelowSkippedFloorsAtZero() {
        List<EqlClusterShards> shards = EqlClusterShards.from(3, 1, 2, 0, SearchResponse.Clusters.EMPTY);
        assertThat(shards, equalTo(List.of(new EqlClusterShards(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, 3, 0, 2, 0))));
    }

    public void testClusterWithoutShardCountsIsOmitted() {
        SearchResponse.Cluster running = new SearchResponse.Cluster("", "idx", false, "(local)");
        SearchResponse.Clusters clusters = new SearchResponse.Clusters(Map.of("", running));
        List<EqlClusterShards> shards = EqlClusterShards.from(1, 1, 0, 0, clusters);
        assertThat(shards, equalTo(List.of()));
    }

    private static SearchResponse.Cluster counted(String alias, int total, int successful, int skipped, int failed) {
        String origin = alias.isEmpty() ? "(local)" : null;
        return new SearchResponse.Cluster(
            alias,
            "idx",
            false,
            SearchResponse.Cluster.Status.SUCCESSFUL,
            total,
            successful,
            skipped,
            failed,
            List.of(),
            TimeValue.timeValueMillis(1),
            false,
            origin
        );
    }
}
