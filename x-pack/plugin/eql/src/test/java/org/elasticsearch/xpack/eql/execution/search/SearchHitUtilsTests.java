/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.execution.search;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.search.SearchResponseUtils;
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
            Results results = Results.fromPayload(new EventPayload(response));
            assertEquals(1, results.shardFailures().size());
            ShardSearchFailure failure = results.shardFailures().getFirst();
            assertEquals("linked", failure.shard().getClusterAlias());
            assertEquals(-1, failure.shardId());
            assertSame(down, failure.getCause().getCause());
            Map<String, ShardSearchFailure> sequenceFailures = new HashMap<>();
            SearchHitUtils.addShardFailures(sequenceFailures, response);
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

    public void testTimeoutSurvivesEventPayloadAndResults() {
        SearchResponse response = SearchResponseUtils.response().timedOut(true).build();
        try {
            assertTrue(Results.fromPayload(new EventPayload(response)).timedOut());
        } finally {
            response.decRef();
        }
    }
}
