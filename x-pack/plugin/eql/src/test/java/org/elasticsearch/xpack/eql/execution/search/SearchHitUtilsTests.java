/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.execution.search;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.eql.execution.payload.EventPayload;
import org.elasticsearch.xpack.eql.session.Results;
import org.elasticsearch.xpack.eql.util.SearchHitUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SearchHitUtilsTests extends ESTestCase {
    public void testSkippedProjectSurvivesEventPayloadAndResults() {
        Exception down = new IllegalStateException("link down after field resolution");
        SearchResponse.Cluster skipped = new SearchResponse.Cluster.Builder(new SearchResponse.Cluster("linked", "logs-*", true, null))
            .setStatus(SearchResponse.Cluster.Status.SKIPPED)
            .setFailures(List.of(new ShardSearchFailure(down)))
            .build();
        SearchResponse response = SearchResponseUtils.response()
            .clusters(new SearchResponse.Clusters(Map.of("linked", skipped), false))
            .build();
        try {
            assertTrue(response.getShardFailures().isEmpty());
            Results results = Results.fromPayload(new EventPayload(response, SearchHitUtils.shardFailures(response)));
            assertEquals(1, results.shardFailures().size());
            ShardSearchFailure failure = results.shardFailures().getFirst();
            assertEquals("linked", failure.shard().getClusterAlias());
            assertEquals(-1, failure.shardId());
            assertSame(down, failure.getCause().getCause());
            Map<String, ShardSearchFailure> sequenceFailures = new HashMap<>();
            SearchHitUtils.addShardFailures(sequenceFailures, SearchHitUtils.shardFailures(response));
            assertEquals(1, sequenceFailures.size());
        } finally {
            response.decRef();
        }
    }

    public void testSkippedProjectWithoutFailureStillReportsPartialResults() {
        SearchResponse.Cluster skipped = new SearchResponse.Cluster.Builder(new SearchResponse.Cluster("linked", "logs-*", true, null))
            .setStatus(SearchResponse.Cluster.Status.SKIPPED)
            .build();
        List<ShardSearchFailure> failures = SearchHitUtils.skippedClusterFailures(
            new SearchResponse.Clusters(Map.of("linked", skipped), false)
        );
        assertEquals(1, failures.size());
        assertEquals("linked", failures.getFirst().shard().getClusterAlias());
    }

    /** A skipped project can report the same real shard failure in both response metadata lists. */
    public void testReportedShardFailureIsNotReplacedOrDuplicated() {
        ShardSearchFailure reported = new ShardSearchFailure(
            new IllegalStateException("shard failed"),
            new SearchShardTarget("node", new ShardId("logs", "_na_", 3), "linked")
        );
        SearchResponse response = SearchResponseUtils.response()
            .shards(1, 0, 0)
            .shardFailures(reported)
            .clusters(new SearchResponse.Clusters(Map.of("linked", skippedCluster("linked", reported)), false))
            .build();
        try {
            assertSame(response.getShardFailures(), SearchHitUtils.shardFailures(response));
            assertSame(reported, SearchHitUtils.shardFailures(response).getFirst());
            assertEquals(3, SearchHitUtils.shardFailures(response).getFirst().shardId());
        } finally {
            response.decRef();
        }
    }

    /** Deduplication of one project's shard failure must not hide a different project's connection failure. */
    public void testOnlyUnrepresentedSkippedProjectsAreAdded() {
        ShardSearchFailure reported = new ShardSearchFailure(
            new IllegalStateException("shard failed"),
            new SearchShardTarget("node", new ShardId("logs", "_na_", 3), "linked")
        );
        SearchResponse response = SearchResponseUtils.response()
            .shards(1, 0, 0)
            .shardFailures(reported)
            .clusters(
                new SearchResponse.Clusters(
                    Map.of(
                        "linked",
                        skippedCluster("linked", reported),
                        "other",
                        skippedCluster("other", new ShardSearchFailure(new IllegalStateException("connection failed")))
                    ),
                    false
                )
            )
            .build();
        try {
            List<ShardSearchFailure> failures = SearchHitUtils.shardFailures(response);
            assertEquals(2, failures.size());
            assertSame(reported, failures.getFirst());
            assertEquals("other", failures.get(1).shard().getClusterAlias());
            assertEquals(-1, failures.get(1).shardId());
        } finally {
            response.decRef();
        }
    }

    private static SearchResponse.Cluster skippedCluster(String alias, ShardSearchFailure failure) {
        return new SearchResponse.Cluster.Builder(new SearchResponse.Cluster(alias, "logs-*", true, null)).setStatus(
            SearchResponse.Cluster.Status.SKIPPED
        ).setFailures(List.of(failure)).build();
    }

    public void testTimeoutSurvivesEventPayloadAndResults() {
        SearchResponse response = SearchResponseUtils.response().timedOut(true).build();
        try {
            assertTrue(Results.fromPayload(new EventPayload(response)).timedOut());
        } finally {
            response.decRef();
        }
    }
}
