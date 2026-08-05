/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DatafeedSearchProbeDiagnosticsTests extends ESTestCase {

    public void testNoFailuresShouldReturnNull() {
        SearchResponse response = mock(SearchResponse.class);
        when(response.getClusters()).thenReturn(SearchResponse.Clusters.EMPTY);
        when(response.getShardFailures()).thenReturn(ShardSearchFailure.EMPTY_ARRAY);

        assertThat(DatafeedSearchProbeDiagnostics.diagnoseSearchProbeFailure(response), nullValue());
    }

    public void testSkippedClusterWithSecurityExceptionShouldReturnDiagnosis() {
        ShardSearchFailure failure = new ShardSearchFailure(new ElasticsearchSecurityException("action denied", RestStatus.FORBIDDEN));
        SearchResponse.Cluster cluster = new SearchResponse.Cluster(
            "linked_project",
            "linked:logs",
            false,
            SearchResponse.Cluster.Status.SKIPPED,
            null,
            null,
            null,
            null,
            List.of(failure),
            null,
            false,
            null
        );
        SearchResponse.Clusters clusters = new SearchResponse.Clusters(Map.of("linked_project", cluster));
        SearchResponse response = mock(SearchResponse.class);
        when(response.getClusters()).thenReturn(clusters);
        when(response.getShardFailures()).thenReturn(ShardSearchFailure.EMPTY_ARRAY);

        String diagnosis = DatafeedSearchProbeDiagnostics.diagnoseSearchProbeFailure(response);
        assertThat(diagnosis, containsString("linked_project"));
        assertThat(diagnosis, containsString("permissions"));
    }

    public void testFailedClusterWithSecurityExceptionShouldReturnDiagnosis() {
        ShardSearchFailure failure = new ShardSearchFailure(new ElasticsearchSecurityException("action denied", RestStatus.FORBIDDEN));
        SearchResponse.Cluster cluster = new SearchResponse.Cluster(
            "linked_project",
            "linked:logs",
            false,
            SearchResponse.Cluster.Status.FAILED,
            null,
            null,
            null,
            null,
            List.of(failure),
            null,
            false,
            null
        );
        SearchResponse.Clusters clusters = new SearchResponse.Clusters(Map.of("linked_project", cluster));
        SearchResponse response = mock(SearchResponse.class);
        when(response.getClusters()).thenReturn(clusters);
        when(response.getShardFailures()).thenReturn(ShardSearchFailure.EMPTY_ARRAY);

        assertThat(DatafeedSearchProbeDiagnostics.diagnoseSearchProbeFailure(response), containsString("linked_project"));
    }

    public void testRunningClusterWithSecurityExceptionShouldReturnNull() {
        ShardSearchFailure failure = new ShardSearchFailure(new ElasticsearchSecurityException("action denied", RestStatus.FORBIDDEN));
        SearchResponse.Cluster cluster = new SearchResponse.Cluster(
            "linked_project",
            "linked:logs",
            false,
            SearchResponse.Cluster.Status.RUNNING,
            null,
            null,
            null,
            null,
            List.of(failure),
            null,
            false,
            null
        );
        SearchResponse.Clusters clusters = new SearchResponse.Clusters(Map.of("linked_project", cluster));
        SearchResponse response = mock(SearchResponse.class);
        when(response.getClusters()).thenReturn(clusters);
        when(response.getShardFailures()).thenReturn(ShardSearchFailure.EMPTY_ARRAY);

        assertThat(DatafeedSearchProbeDiagnostics.diagnoseSearchProbeFailure(response), nullValue());
    }

    public void testTopLevelShardSecurityFailureShouldReturnDiagnosisWithIndex() {
        SearchShardTarget shardTarget = new SearchShardTarget("node-1", new ShardId("priv-linked-logs", "uuid", 0), "linked_project");
        ShardSearchFailure failure = new ShardSearchFailure(
            new ElasticsearchSecurityException("action denied", RestStatus.FORBIDDEN),
            shardTarget
        );
        SearchResponse response = mock(SearchResponse.class);
        when(response.getClusters()).thenReturn(SearchResponse.Clusters.EMPTY);
        when(response.getShardFailures()).thenReturn(new ShardSearchFailure[] { failure });

        String diagnosis = DatafeedSearchProbeDiagnostics.diagnoseSearchProbeFailure(response);
        assertThat(diagnosis, containsString("linked_project:priv-linked-logs"));
    }

    public void testTopLevelShardSecurityFailureWithoutIndexShouldReturnGenericDiagnosis() {
        ShardSearchFailure failure = new ShardSearchFailure(new ElasticsearchSecurityException("action denied", RestStatus.FORBIDDEN));
        SearchResponse response = mock(SearchResponse.class);
        when(response.getClusters()).thenReturn(SearchResponse.Clusters.EMPTY);
        when(response.getShardFailures()).thenReturn(new ShardSearchFailure[] { failure });

        assertThat(
            DatafeedSearchProbeDiagnostics.diagnoseSearchProbeFailure(response),
            containsString("User lacks the required permissions to read from the datafeed indices")
        );
    }

    public void testTransportFailureWithoutSecurityCauseShouldReturnNull() {
        ShardSearchFailure failure = new ShardSearchFailure(new RuntimeException("connection reset"));
        SearchResponse.Cluster cluster = new SearchResponse.Cluster(
            "linked_project",
            "linked:logs",
            false,
            SearchResponse.Cluster.Status.SKIPPED,
            null,
            null,
            null,
            null,
            List.of(failure),
            null,
            false,
            null
        );
        SearchResponse.Clusters clusters = new SearchResponse.Clusters(Map.of("linked_project", cluster));
        SearchResponse response = mock(SearchResponse.class);
        when(response.getClusters()).thenReturn(clusters);
        when(response.getShardFailures()).thenReturn(ShardSearchFailure.EMPTY_ARRAY);

        assertThat(DatafeedSearchProbeDiagnostics.diagnoseSearchProbeFailure(response), nullValue());
    }
}
