/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed.extractor;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.datafeed.LinkedClusterState;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link DataExtractorUtils#extractLinkedClusterStates(SearchResponse)},
 * using mock or real {@link SearchResponse} and {@link SearchResponse.Clusters}.
 */
public class DataExtractorUtilsTests extends ESTestCase {

    public void testExtractLinkedClusterStates_returnsEmptyWhenClustersIsNull() {
        SearchResponse response = mock(SearchResponse.class);
        when(response.getClusters()).thenReturn(null);

        List<LinkedClusterState> states = DataExtractorUtils.extractLinkedClusterStates(response);

        assertThat(states, hasSize(0));
    }

    public void testExtractLinkedClusterStates_returnsEmptyWhenClustersIsEmpty() {
        SearchResponse response = createSearchResponse(SearchResponse.Clusters.EMPTY);

        try {
            List<LinkedClusterState> states = DataExtractorUtils.extractLinkedClusterStates(response);
            assertThat(states, hasSize(0));
        } finally {
            response.decRef();
        }
    }

    public void testExtractLinkedClusterStates_mapsSuccessfulToAvailable() {
        SearchResponse.Cluster cluster = new SearchResponse.Cluster(
            "remote_1",
            "remote_index",
            false,
            SearchResponse.Cluster.Status.SUCCESSFUL,
            1,
            1,
            0,
            0,
            List.of(),
            TimeValue.timeValueMillis(50L),
            false,
            null
        );
        SearchResponse.Clusters clusters = new SearchResponse.Clusters(Map.of("remote_1", cluster));
        SearchResponse response = createSearchResponse(clusters);

        try {
            List<LinkedClusterState> states = DataExtractorUtils.extractLinkedClusterStates(response);
            assertThat(states, hasSize(1));
            LinkedClusterState state = states.get(0);
            assertThat(state.alias(), equalTo("remote_1"));
            assertThat(state.status(), equalTo(LinkedClusterState.Status.AVAILABLE));
            assertThat(state.errorReason(), nullValue());
            assertThat(state.searchLatencyMs(), equalTo(50L));
        } finally {
            response.decRef();
        }
    }

    public void testExtractLinkedClusterStates_mapsRunningToAvailable() {
        SearchResponse.Cluster cluster = new SearchResponse.Cluster(
            "running_cluster",
            "remote_index",
            false,
            SearchResponse.Cluster.Status.RUNNING,
            1,
            0,
            0,
            0,
            List.of(),
            TimeValue.timeValueMillis(30L),
            false,
            null
        );
        SearchResponse.Clusters clusters = new SearchResponse.Clusters(Map.of("running_cluster", cluster));
        SearchResponse response = createSearchResponse(clusters);

        try {
            List<LinkedClusterState> states = DataExtractorUtils.extractLinkedClusterStates(response);
            assertThat(states, hasSize(1));
            assertThat(states.get(0).alias(), equalTo("running_cluster"));
            assertThat(states.get(0).status(), equalTo(LinkedClusterState.Status.AVAILABLE));
            assertThat(states.get(0).searchLatencyMs(), equalTo(30L));
        } finally {
            response.decRef();
        }
    }

    public void testExtractLinkedClusterStates_mapsSkippedToSkipped() {
        SearchResponse.Cluster cluster = new SearchResponse.Cluster(
            "skipped_cluster",
            "index",
            true,
            SearchResponse.Cluster.Status.SKIPPED,
            0,
            0,
            0,
            0,
            List.of(),
            null,
            false,
            null
        );
        SearchResponse.Clusters clusters = new SearchResponse.Clusters(Map.of("skipped_cluster", cluster));
        SearchResponse response = createSearchResponse(clusters);

        try {
            List<LinkedClusterState> states = DataExtractorUtils.extractLinkedClusterStates(response);
            assertThat(states, hasSize(1));
            assertThat(states.get(0).alias(), equalTo("skipped_cluster"));
            assertThat(states.get(0).status(), equalTo(LinkedClusterState.Status.SKIPPED));
            assertThat(states.get(0).errorReason(), nullValue());
            assertThat(states.get(0).searchLatencyMs(), equalTo(0L));
        } finally {
            response.decRef();
        }
    }

    public void testExtractLinkedClusterStates_mapsFailedToFailed() {
        SearchResponse.Cluster cluster = new SearchResponse.Cluster(
            "failed_cluster",
            "index",
            false,
            SearchResponse.Cluster.Status.FAILED,
            1,
            0,
            0,
            1,
            List.of(),
            null,
            false,
            null
        );
        SearchResponse.Clusters clusters = new SearchResponse.Clusters(Map.of("failed_cluster", cluster));
        SearchResponse response = createSearchResponse(clusters);

        try {
            List<LinkedClusterState> states = DataExtractorUtils.extractLinkedClusterStates(response);
            assertThat(states, hasSize(1));
            assertThat(states.get(0).alias(), equalTo("failed_cluster"));
            assertThat(states.get(0).status(), equalTo(LinkedClusterState.Status.FAILED));
        } finally {
            response.decRef();
        }
    }

    public void testExtractLinkedClusterStates_extractsErrorReasonFromFirstFailure() {
        ShardSearchFailure failure = new ShardSearchFailure(new RuntimeException("index_not_found_exception: no such index"));
        SearchResponse.Cluster cluster = new SearchResponse.Cluster(
            "failed_cluster",
            "index",
            false,
            SearchResponse.Cluster.Status.FAILED,
            1,
            0,
            0,
            1,
            List.of(failure),
            null,
            false,
            null
        );
        SearchResponse.Clusters clusters = new SearchResponse.Clusters(Map.of("failed_cluster", cluster));
        SearchResponse response = createSearchResponse(clusters);

        try {
            List<LinkedClusterState> states = DataExtractorUtils.extractLinkedClusterStates(response);
            assertThat(states, hasSize(1));
            assertThat(states.get(0).errorReason(), containsString("index_not_found_exception: no such index"));
        } finally {
            response.decRef();
        }
    }

    public void testExtractLinkedClusterStates_extractsSearchLatencyFromTook() {
        SearchResponse.Cluster cluster = new SearchResponse.Cluster(
            "remote_1",
            "idx",
            false,
            SearchResponse.Cluster.Status.SUCCESSFUL,
            2,
            2,
            0,
            0,
            List.of(),
            TimeValue.timeValueMillis(123L),
            false,
            null
        );
        SearchResponse.Clusters clusters = new SearchResponse.Clusters(Map.of("remote_1", cluster));
        SearchResponse response = createSearchResponse(clusters);

        try {
            List<LinkedClusterState> states = DataExtractorUtils.extractLinkedClusterStates(response);
            assertThat(states.get(0).searchLatencyMs(), equalTo(123L));
        } finally {
            response.decRef();
        }
    }

    public void testExtractLinkedClusterStates_mapsPartialToAvailable() {
        SearchResponse.Cluster cluster = new SearchResponse.Cluster(
            "partial_cluster",
            "idx",
            false,
            SearchResponse.Cluster.Status.PARTIAL,
            2,
            1,
            0,
            1,
            List.of(),
            TimeValue.timeValueMillis(10L),
            false,
            null
        );
        SearchResponse.Clusters clusters = new SearchResponse.Clusters(Map.of("partial_cluster", cluster));
        SearchResponse response = createSearchResponse(clusters);

        try {
            List<LinkedClusterState> states = DataExtractorUtils.extractLinkedClusterStates(response);
            assertThat(states, hasSize(1));
            assertThat(states.get(0).status(), equalTo(LinkedClusterState.Status.AVAILABLE));
        } finally {
            response.decRef();
        }
    }

    public void testExtractLinkedClusterStates_multipleClusters() {
        SearchResponse.Cluster local = new SearchResponse.Cluster(
            "",
            "local_idx",
            false,
            SearchResponse.Cluster.Status.SUCCESSFUL,
            1,
            1,
            0,
            0,
            List.of(),
            TimeValue.timeValueMillis(5L),
            false,
            "(local)"
        );
        SearchResponse.Cluster remote = new SearchResponse.Cluster(
            "remote_a",
            "remote_idx",
            false,
            SearchResponse.Cluster.Status.SUCCESSFUL,
            1,
            1,
            0,
            0,
            List.of(),
            TimeValue.timeValueMillis(20L),
            false,
            null
        );
        SearchResponse.Clusters clusters = new SearchResponse.Clusters(Map.of("", local, "remote_a", remote));
        SearchResponse response = createSearchResponse(clusters);

        try {
            List<LinkedClusterState> states = DataExtractorUtils.extractLinkedClusterStates(response);
            assertThat(states, hasSize(2));
            LinkedClusterState localState = states.stream().filter(s -> "(local)".equals(s.alias())).findFirst().orElseThrow();
            LinkedClusterState remoteState = states.stream().filter(s -> "remote_a".equals(s.alias())).findFirst().orElseThrow();
            assertThat(localState.status(), equalTo(LinkedClusterState.Status.AVAILABLE));
            assertThat(localState.searchLatencyMs(), equalTo(5L));
            assertThat(remoteState.status(), equalTo(LinkedClusterState.Status.AVAILABLE));
            assertThat(remoteState.searchLatencyMs(), equalTo(20L));
        } finally {
            response.decRef();
        }
    }

    private static SearchResponse createSearchResponse(SearchResponse.Clusters clusters) {
        return new SearchResponse(
            SearchHits.unpooled(SearchHits.EMPTY, new TotalHits(0, TotalHits.Relation.EQUAL_TO), 0.0f),
            null,
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
            ShardSearchFailure.EMPTY_ARRAY,
            clusters
        );
    }
}
