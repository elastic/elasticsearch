/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms.common;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class SourceAccessDiagnosticsTests extends ESTestCase {

    public void testNoFailures_returnsFallbackMessage() {
        SearchResponse response = createResponseWithNoFailures();
        try {
            String message = SourceAccessDiagnostics.diagnoseSourceAccessFailure(response);
            assertThat(message, equalTo(SourceAccessDiagnostics.SOURCE_INDICES_MISSING));
        } finally {
            response.decRef();
        }
    }

    public void testEmptyClusters_returnsFallbackMessage() {
        SearchResponse response = createResponseWithClustersAndShardFailures(SearchResponse.Clusters.EMPTY, ShardSearchFailure.EMPTY_ARRAY);
        try {
            String message = SourceAccessDiagnostics.diagnoseSourceAccessFailure(response);
            assertThat(message, equalTo(SourceAccessDiagnostics.SOURCE_INDICES_MISSING));
        } finally {
            response.decRef();
        }
    }

    public void testClusterSkippedDueToSecurityException_returnsClusterMessage() {
        ShardSearchFailure securityFailure = new ShardSearchFailure(
            new ElasticsearchSecurityException("action [indices:data/read/search] is unauthorized", RestStatus.FORBIDDEN)
        );
        SearchResponse.Cluster skippedCluster = new SearchResponse.Cluster(
            "my_remote_cluster",
            "remote_test_index",
            true,
            SearchResponse.Cluster.Status.SKIPPED,
            0,
            0,
            0,
            0,
            List.of(securityFailure),
            null,
            false,
            null
        );
        SearchResponse.Clusters clusters = new SearchResponse.Clusters(Map.of("my_remote_cluster", skippedCluster));

        SearchResponse response = createResponseWithClustersAndShardFailures(clusters, ShardSearchFailure.EMPTY_ARRAY);
        try {
            String message = SourceAccessDiagnostics.diagnoseSourceAccessFailure(response);
            assertThat(message, containsString("lacks the required permissions"));
            assertThat(message, containsString("my_remote_cluster"));
        } finally {
            response.decRef();
        }
    }

    public void testClusterFailedDueToSecurityException_returnsClusterMessage() {
        ShardSearchFailure securityFailure = new ShardSearchFailure(
            new ElasticsearchSecurityException("action [indices:data/read/search] is unauthorized", RestStatus.FORBIDDEN)
        );
        SearchResponse.Cluster failedCluster = new SearchResponse.Cluster(
            "my_remote_cluster",
            "remote_test_index",
            false,
            SearchResponse.Cluster.Status.FAILED,
            1,
            0,
            0,
            1,
            List.of(securityFailure),
            null,
            false,
            null
        );
        SearchResponse.Clusters clusters = new SearchResponse.Clusters(Map.of("my_remote_cluster", failedCluster));

        SearchResponse response = createResponseWithClustersAndShardFailures(clusters, ShardSearchFailure.EMPTY_ARRAY);
        try {
            String message = SourceAccessDiagnostics.diagnoseSourceAccessFailure(response);
            assertThat(message, containsString("lacks the required permissions"));
            assertThat(message, containsString("my_remote_cluster"));
        } finally {
            response.decRef();
        }
    }

    public void testShardFailureWithSecurityException_returnsIndexMessage() {
        SearchShardTarget shardTarget = new SearchShardTarget(
            "nodeId",
            new org.elasticsearch.index.shard.ShardId("my_index", "_na_", 0),
            null
        );
        ShardSearchFailure securityFailure = new ShardSearchFailure(
            new ElasticsearchSecurityException("action [indices:data/read/search] is unauthorized", RestStatus.FORBIDDEN),
            shardTarget
        );

        SearchResponse response = createResponseWithClustersAndShardFailures(
            SearchResponse.Clusters.EMPTY,
            new ShardSearchFailure[] { securityFailure }
        );
        try {
            String message = SourceAccessDiagnostics.diagnoseSourceAccessFailure(response);
            assertThat(message, containsString("lacks the required permissions"));
            assertThat(message, containsString("my_index"));
        } finally {
            response.decRef();
        }
    }

    public void testShardFailureWithForbiddenStatus_returnsIndexMessage() {
        SearchShardTarget shardTarget = new SearchShardTarget(
            "nodeId",
            new org.elasticsearch.index.shard.ShardId("my_index", "_na_", 0),
            null
        );
        ShardSearchFailure forbiddenFailure = new ShardSearchFailure(
            new ElasticsearchSecurityException("not authorized", RestStatus.FORBIDDEN),
            shardTarget
        );

        SearchResponse response = createResponseWithClustersAndShardFailures(
            SearchResponse.Clusters.EMPTY,
            new ShardSearchFailure[] { forbiddenFailure }
        );
        try {
            String message = SourceAccessDiagnostics.diagnoseSourceAccessFailure(response);
            assertThat(message, containsString("lacks the required permissions"));
        } finally {
            response.decRef();
        }
    }

    public void testShardFailureWithUnauthorizedStatus_returnsIndexMessage() {
        SearchShardTarget shardTarget = new SearchShardTarget(
            "nodeId",
            new org.elasticsearch.index.shard.ShardId("my_index", "_na_", 0),
            null
        );
        ShardSearchFailure unauthorizedFailure = new ShardSearchFailure(
            new ElasticsearchSecurityException("not authenticated", RestStatus.UNAUTHORIZED),
            shardTarget
        );

        SearchResponse response = createResponseWithClustersAndShardFailures(
            SearchResponse.Clusters.EMPTY,
            new ShardSearchFailure[] { unauthorizedFailure }
        );
        try {
            String message = SourceAccessDiagnostics.diagnoseSourceAccessFailure(response);
            assertThat(message, containsString("lacks the required permissions"));
            assertThat(message, containsString("my_index"));
        } finally {
            response.decRef();
        }
    }

    public void testShardFailureWithSecurityExceptionNoIndex_returnsGenericPermissionMessage() {
        ShardSearchFailure securityFailure = new ShardSearchFailure(
            new ElasticsearchSecurityException("unauthorized", RestStatus.FORBIDDEN)
        );

        SearchResponse response = createResponseWithClustersAndShardFailures(
            SearchResponse.Clusters.EMPTY,
            new ShardSearchFailure[] { securityFailure }
        );
        try {
            String message = SourceAccessDiagnostics.diagnoseSourceAccessFailure(response);
            assertThat(message, equalTo("User lacks the required permissions to read from the source indices."));
        } finally {
            response.decRef();
        }
    }

    public void testNonSecurityShardFailure_returnsFallbackMessage() {
        ShardSearchFailure nonSecurityFailure = new ShardSearchFailure(new IndexNotFoundException("missing_index"));

        SearchResponse response = createResponseWithClustersAndShardFailures(
            SearchResponse.Clusters.EMPTY,
            new ShardSearchFailure[] { nonSecurityFailure }
        );
        try {
            String message = SourceAccessDiagnostics.diagnoseSourceAccessFailure(response);
            assertThat(message, equalTo(SourceAccessDiagnostics.SOURCE_INDICES_MISSING));
        } finally {
            response.decRef();
        }
    }

    public void testClusterSuccessfulWithNonSecurityShardFailures_returnsFallbackMessage() {
        ShardSearchFailure nonSecurityFailure = new ShardSearchFailure(new IndexNotFoundException("missing_index"));

        SearchResponse.Cluster successfulCluster = new SearchResponse.Cluster(
            "my_remote_cluster",
            "remote_test_index",
            true,
            SearchResponse.Cluster.Status.SUCCESSFUL,
            1,
            1,
            0,
            0,
            List.of(nonSecurityFailure),
            null,
            false,
            null
        );
        SearchResponse.Clusters clusters = new SearchResponse.Clusters(Map.of("my_remote_cluster", successfulCluster));

        SearchResponse response = createResponseWithClustersAndShardFailures(clusters, ShardSearchFailure.EMPTY_ARRAY);
        try {
            String message = SourceAccessDiagnostics.diagnoseSourceAccessFailure(response);
            assertThat(message, equalTo(SourceAccessDiagnostics.SOURCE_INDICES_MISSING));
        } finally {
            response.decRef();
        }
    }

    private static SearchResponse createResponseWithNoFailures() {
        return createResponseWithClustersAndShardFailures(SearchResponse.Clusters.EMPTY, ShardSearchFailure.EMPTY_ARRAY);
    }

    private static SearchResponse createResponseWithClustersAndShardFailures(
        SearchResponse.Clusters clusters,
        ShardSearchFailure[] shardFailures
    ) {
        return new SearchResponse(
            SearchHits.unpooled(SearchHits.EMPTY, new TotalHits(0, TotalHits.Relation.EQUAL_TO), 0.0f),
            null, // null aggregations -- simulates the scenario we are diagnosing
            new Suggest(Collections.emptyList()),
            false,
            null,
            null,
            0,
            null,
            1,
            1,
            0,
            0,
            shardFailures,
            clusters
        );
    }
}
