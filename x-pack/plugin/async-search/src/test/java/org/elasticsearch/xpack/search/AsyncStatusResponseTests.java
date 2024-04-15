/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.search;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.search.action.AsyncSearchResponse;
import org.elasticsearch.xpack.core.search.action.AsyncStatusResponse;

import java.io.IOException;
import java.util.Date;

import static org.elasticsearch.xpack.core.async.GetAsyncResultRequestTests.randomSearchId;

public class AsyncStatusResponseTests extends AbstractWireSerializingTestCase<AsyncStatusResponse> {

    @Override
    protected AsyncStatusResponse createTestInstance() {
        String id = randomSearchId();
        boolean isRunning = randomBoolean();
        boolean isPartial = isRunning ? randomBoolean() : false;
        long startTimeMillis = (new Date(randomLongBetween(0, 3000000000000L))).getTime();
        long expirationTimeMillis = startTimeMillis + 3600000L;
        int totalShards = randomIntBetween(10, 150);
        int successfulShards = randomIntBetween(0, totalShards - 5);
        int skippedShards = randomIntBetween(0, 5);
        int failedShards = totalShards - successfulShards - skippedShards;
        RestStatus completionStatus = isRunning ? null : randomBoolean() ? RestStatus.OK : RestStatus.SERVICE_UNAVAILABLE;
        Long completionTimeMillis = null;
        if (isRunning == false && completionStatus == RestStatus.OK) {
            completionTimeMillis = startTimeMillis + 25000;
        }
        SearchResponse.Clusters clusters = switch (randomIntBetween(0, 3)) {
            case 1 -> SearchResponse.Clusters.EMPTY;
            case 2 -> new SearchResponse.Clusters(1, 1, 0);
            case 3 -> AsyncSearchResponseTests.createCCSClusterObjects(4, 3, true);
            default -> null;  // case 0
        };
        return new AsyncStatusResponse(
            id,
            isRunning,
            isPartial,
            startTimeMillis,
            expirationTimeMillis,
            completionTimeMillis,
            totalShards,
            successfulShards,
            skippedShards,
            failedShards,
            completionStatus,
            clusters
        );
    }

    @Override
    protected Writeable.Reader<AsyncStatusResponse> instanceReader() {
        return AsyncStatusResponse::new;
    }

    @Override
    protected AsyncStatusResponse mutateInstance(AsyncStatusResponse instance) {
        // return a response with the opposite running status
        boolean isRunning = instance.isRunning() == false;
        boolean isPartial = isRunning ? randomBoolean() : false;
        RestStatus completionStatus = isRunning ? null : randomBoolean() ? RestStatus.OK : RestStatus.SERVICE_UNAVAILABLE;
        SearchResponse.Clusters clusters = switch (randomIntBetween(0, 3)) {
            case 1 -> SearchResponse.Clusters.EMPTY;
            case 2 -> new SearchResponse.Clusters(1, 1, 0);
            case 3 -> AsyncSearchResponseTests.createCCSClusterObjects(4, 3, true); // new SearchResponse.Clusters(4, 1, 0, 3, true);
            default -> null;  // case 0
        };
        return new AsyncStatusResponse(
            instance.getId(),
            isRunning,
            isPartial,
            instance.getStartTime(),
            instance.getExpirationTime(),
            isRunning ? null : instance.getStartTime() + 25000,
            instance.getTotalShards(),
            instance.getSuccessfulShards(),
            instance.getSkippedShards(),
            instance.getFailedShards(),
            completionStatus,
            clusters
        );
    }

    public void testToXContent() throws IOException {
        AsyncStatusResponse response = createTestInstance();
        String completionTimeEntry = "";
        if (response.isRunning() == false && response.getCompletionStatus() == RestStatus.OK) {
            completionTimeEntry = Strings.format("\"completion_time_in_millis\" : %s,", response.getCompletionTime());
        }
        try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
            String expectedJson;
            SearchResponse.Clusters clusters = response.getClusters();
            if (clusters == null || clusters.getTotal() == 0) {
                Object[] args = new Object[] {
                    response.getId(),
                    response.isRunning(),
                    response.isPartial(),
                    response.getStartTime(),
                    response.getExpirationTime(),
                    completionTimeEntry,
                    response.getTotalShards(),
                    response.getSuccessfulShards(),
                    response.getSkippedShards(),
                    response.getFailedShards(),
                    response.getCompletionStatus() == null ? "" : Strings.format("""
                        ,"completion_status" : %s""", response.getCompletionStatus().getStatus()) };

                expectedJson = Strings.format("""
                    {
                      "id" : "%s",
                      "is_running" : %s,
                      "is_partial" : %s,
                      "start_time_in_millis" : %s,
                      "expiration_time_in_millis" : %s,
                      %s
                      "_shards" : {
                        "total" : %s,
                        "successful" : %s,
                        "skipped" : %s,
                        "failed" : %s
                       }
                      %s
                    }
                    """, args);
            } else if (clusters.getTotal() == 1) {
                Object[] args = new Object[] {
                    response.getId(),
                    response.isRunning(),
                    response.isPartial(),
                    response.getStartTime(),
                    response.getExpirationTime(),
                    completionTimeEntry,
                    response.getTotalShards(),
                    response.getSuccessfulShards(),
                    response.getSkippedShards(),
                    response.getFailedShards(),
                    clusters.getTotal(),
                    clusters.getClusterStateCount(SearchResponse.Cluster.Status.SUCCESSFUL),
                    clusters.getClusterStateCount(SearchResponse.Cluster.Status.SKIPPED),
                    clusters.getClusterStateCount(SearchResponse.Cluster.Status.RUNNING),
                    clusters.getClusterStateCount(SearchResponse.Cluster.Status.PARTIAL),
                    clusters.getClusterStateCount(SearchResponse.Cluster.Status.FAILED),
                    response.getCompletionStatus() == null ? "" : Strings.format("""
                        ,"completion_status" : %s""", response.getCompletionStatus().getStatus()) };

                expectedJson = Strings.format("""
                    {
                      "id" : "%s",
                      "is_running" : %s,
                      "is_partial" : %s,
                      "start_time_in_millis" : %s,
                      "expiration_time_in_millis" : %s,
                      %s
                      "_shards" : {
                        "total" : %s,
                        "successful" : %s,
                        "skipped" : %s,
                        "failed" : %s
                      },
                      "_clusters": {
                       "total": %s,
                       "successful": %s,
                       "skipped": %s,
                       "running": %s,
                       "partial": %s,
                       "failed": %s
                      }
                      %s
                    }
                    """, args);
            } else {
                Object[] args = new Object[] {
                    response.getId(),
                    response.isRunning(),
                    response.isPartial(),
                    response.getStartTime(),
                    response.getExpirationTime(),
                    completionTimeEntry,
                    response.getTotalShards(),
                    response.getSuccessfulShards(),
                    response.getSkippedShards(),
                    response.getFailedShards(),
                    clusters.getTotal(),
                    clusters.getClusterStateCount(SearchResponse.Cluster.Status.SUCCESSFUL),
                    clusters.getClusterStateCount(SearchResponse.Cluster.Status.SKIPPED),
                    clusters.getClusterStateCount(SearchResponse.Cluster.Status.RUNNING),
                    clusters.getClusterStateCount(SearchResponse.Cluster.Status.PARTIAL),
                    clusters.getClusterStateCount(SearchResponse.Cluster.Status.FAILED),
                    response.getCompletionStatus() == null ? "" : Strings.format("""
                        ,"completion_status" : %s""", response.getCompletionStatus().getStatus()) };

                expectedJson = Strings.format("""
                    {
                      "id" : "%s",
                      "is_running" : %s,
                      "is_partial" : %s,
                      "start_time_in_millis" : %s,
                      "expiration_time_in_millis" : %s,
                      %s
                      "_shards" : {
                        "total" : %s,
                        "successful" : %s,
                        "skipped" : %s,
                        "failed" : %s
                      },
                      "_clusters": {
                       "total": %s,
                       "successful": %s,
                       "skipped": %s,
                       "running": %s,
                       "partial": %s,
                       "failed": %s,
                        "details": {
                          "(local)": {
                            "status": "running",
                            "indices": "foo,bar*",
                            "timed_out": false
                          },
                          "cluster_1": {
                            "status": "running",
                            "indices": "foo,bar*",
                            "timed_out": false
                          },
                          "cluster_2": {
                            "status": "running",
                            "indices": "foo,bar*",
                            "timed_out": false
                          },
                          "cluster_0": {
                            "status": "running",
                            "indices": "foo,bar*",
                            "timed_out": false
                          }
                        }
                      }
                      %s
                    }
                    """, args);

            }
            response.toXContent(builder, ToXContent.EMPTY_PARAMS);
            assertEquals(XContentHelper.stripWhitespace(expectedJson), Strings.toString(builder));
        }
    }

    public void testGetStatusFromStoredSearchRandomizedInputs() {
        boolean ccs = randomBoolean();
        String searchId = randomSearchId();
        SearchResponse searchResponse = AsyncSearchResponseTests.randomSearchResponse(ccs);
        AsyncSearchResponse asyncSearchResponse;
        try {
            asyncSearchResponse = AsyncSearchResponseTests.randomAsyncSearchResponse(searchId, searchResponse);
        } finally {
            searchResponse.decRef();
        }
        try {
            if (asyncSearchResponse.getSearchResponse() == null
                && asyncSearchResponse.getFailure() == null
                && asyncSearchResponse.isRunning() == false) {
                // if no longer running, the search should have recorded either a failure or a search response
                // if not an Exception should be thrown
                expectThrows(
                    IllegalStateException.class,
                    () -> AsyncStatusResponse.getStatusFromStoredSearch(asyncSearchResponse, 100, searchId)
                );
            } else {
                AsyncStatusResponse statusFromStoredSearch = AsyncStatusResponse.getStatusFromStoredSearch(
                    asyncSearchResponse,
                    100,
                    searchId
                );
                assertNotNull(statusFromStoredSearch);
                if (statusFromStoredSearch.isRunning()) {
                    assertNull(
                        "completion_status should only be present if search is no longer running",
                        statusFromStoredSearch.getCompletionStatus()
                    );
                } else {
                    assertNotNull(
                        "completion_status should be present if search is no longer running",
                        statusFromStoredSearch.getCompletionStatus()
                    );
                }
            }
        } finally {
            asyncSearchResponse.decRef();
        }
    }

    public void testGetStatusFromStoredSearchFailureScenario() {
        String searchId = randomSearchId();
        Exception error = new IllegalArgumentException("dummy");
        AsyncSearchResponse asyncSearchResponse = new AsyncSearchResponse(searchId, null, error, true, false, 100, 200);
        try {
            AsyncStatusResponse statusFromStoredSearch = AsyncStatusResponse.getStatusFromStoredSearch(asyncSearchResponse, 100, searchId);
            assertNotNull(statusFromStoredSearch);
            assertEquals(statusFromStoredSearch.getCompletionStatus(), RestStatus.BAD_REQUEST);
            assertTrue(statusFromStoredSearch.isPartial());
            assertNull(statusFromStoredSearch.getClusters());
            assertEquals(0, statusFromStoredSearch.getTotalShards());
            assertEquals(0, statusFromStoredSearch.getSuccessfulShards());
            assertEquals(0, statusFromStoredSearch.getSkippedShards());
        } finally {
            asyncSearchResponse.decRef();
        }
    }

    public void testGetStatusFromStoredSearchFailedShardsScenario() {
        String searchId = randomSearchId();

        long tookInMillis = randomNonNegativeLong();
        int totalShards = randomIntBetween(1, Integer.MAX_VALUE);
        int successfulShards = randomIntBetween(0, totalShards);
        int skippedShards = randomIntBetween(0, successfulShards);
        SearchResponse.Clusters clusters = new SearchResponse.Clusters(100, 99, 1);
        SearchResponse searchResponse = SearchResponseUtils.emptyWithTotalHits(
            null,
            totalShards,
            successfulShards,
            skippedShards,
            tookInMillis,
            new ShardSearchFailure[] { new ShardSearchFailure(new RuntimeException("foo")) },
            clusters
        );
        AsyncSearchResponse asyncSearchResponse;
        try {
            asyncSearchResponse = new AsyncSearchResponse(searchId, searchResponse, null, false, false, 100, 200);
        } finally {
            searchResponse.decRef();
        }
        try {
            AsyncStatusResponse statusFromStoredSearch = AsyncStatusResponse.getStatusFromStoredSearch(asyncSearchResponse, 100, searchId);
            assertNotNull(statusFromStoredSearch);
            assertEquals(1, statusFromStoredSearch.getFailedShards());
            assertEquals(statusFromStoredSearch.getCompletionStatus(), RestStatus.OK);
        } finally {
            asyncSearchResponse.decRef();
        }
    }

    public void testGetStatusFromStoredSearchWithEmptyClustersSuccessfullyCompleted() {
        String searchId = randomSearchId();

        long tookInMillis = randomNonNegativeLong();
        int totalShards = randomIntBetween(1, Integer.MAX_VALUE);
        int successfulShards = randomIntBetween(0, totalShards);
        int skippedShards = randomIntBetween(0, successfulShards);

        SearchResponse searchResponse = SearchResponseUtils.emptyWithTotalHits(
            null,
            totalShards,
            successfulShards,
            skippedShards,
            tookInMillis,
            ShardSearchFailure.EMPTY_ARRAY,
            SearchResponse.Clusters.EMPTY
        );
        AsyncSearchResponse asyncSearchResponse;
        try {
            asyncSearchResponse = new AsyncSearchResponse(searchId, searchResponse, null, false, false, 100, 200);
        } finally {
            searchResponse.decRef();
        }
        try {
            AsyncStatusResponse statusFromStoredSearch = AsyncStatusResponse.getStatusFromStoredSearch(asyncSearchResponse, 100, searchId);
            assertNotNull(statusFromStoredSearch);
            assertEquals(statusFromStoredSearch.getCompletionStatus(), RestStatus.OK);
            assertNull(statusFromStoredSearch.getClusters());
        } finally {
            asyncSearchResponse.decRef();
        }
    }

    public void testGetStatusFromStoredSearchWithNonEmptyClustersSuccessfullyCompleted() {
        String searchId = randomSearchId();

        long tookInMillis = randomNonNegativeLong();
        int totalShards = randomIntBetween(1, Integer.MAX_VALUE);
        int successfulShards = randomIntBetween(0, totalShards);
        int skippedShards = randomIntBetween(0, successfulShards);

        int totalClusters;
        int successfulClusters;
        int skippedClusters;
        SearchResponse.Clusters clusters;
        if (randomBoolean()) {
            // local search only
            totalClusters = 1;
            successfulClusters = 1;
            skippedClusters = 0;
            clusters = new SearchResponse.Clusters(totalClusters, successfulClusters, skippedClusters);
        } else {
            // CCS search
            totalClusters = 80;
            successfulClusters = randomInt(60);
            int partial = randomInt(20);
            skippedClusters = totalClusters - (successfulClusters + partial);
            clusters = AsyncSearchResponseTests.createCCSClusterObjects(80, 80, true, successfulClusters, skippedClusters, partial);
        }
        SearchResponse searchResponse = SearchResponseUtils.emptyWithTotalHits(
            null,
            totalShards,
            successfulShards,
            skippedShards,
            tookInMillis,
            ShardSearchFailure.EMPTY_ARRAY,
            clusters
        );
        AsyncSearchResponse asyncSearchResponse;
        try {
            asyncSearchResponse = new AsyncSearchResponse(searchId, searchResponse, null, false, false, 100, 200);
        } finally {
            searchResponse.decRef();
        }
        try {
            AsyncStatusResponse statusFromStoredSearch = AsyncStatusResponse.getStatusFromStoredSearch(asyncSearchResponse, 100, searchId);
            assertNotNull(statusFromStoredSearch);
            assertEquals(0, statusFromStoredSearch.getFailedShards());
            assertEquals(statusFromStoredSearch.getCompletionStatus(), RestStatus.OK);
            assertEquals(totalClusters, statusFromStoredSearch.getClusters().getTotal());
            assertEquals(skippedClusters, statusFromStoredSearch.getClusters().getClusterStateCount(SearchResponse.Cluster.Status.SKIPPED));
            assertEquals(
                successfulClusters,
                statusFromStoredSearch.getClusters().getClusterStateCount(SearchResponse.Cluster.Status.SUCCESSFUL)
            );
        } finally {
            asyncSearchResponse.decRef();
        }
    }

    public void testGetStatusFromStoredSearchWithNonEmptyClustersStillRunning() {
        String searchId = randomSearchId();

        long tookInMillis = randomNonNegativeLong();
        int totalShards = randomIntBetween(1, Integer.MAX_VALUE);
        int successfulShards = randomIntBetween(0, totalShards);
        int skippedShards = randomIntBetween(0, successfulShards);
        int successful = randomInt(10);
        int partial = randomInt(10);
        int skipped = randomInt(10);

        if (successful + partial + skipped == 0) {
            int val = randomIntBetween(1, 10);
            switch (randomInt(2)) {
                case 0 -> successful = val;
                case 1 -> partial = val;
                case 2 -> skipped = val;
                default -> throw new UnsupportedOperationException();
            }
        }
        SearchResponse.Clusters clusters = AsyncSearchResponseTests.createCCSClusterObjects(100, 99, true, successful, skipped, partial);

        SearchResponse searchResponse = SearchResponseUtils.emptyWithTotalHits(
            null,
            totalShards,
            successfulShards,
            skippedShards,
            tookInMillis,
            ShardSearchFailure.EMPTY_ARRAY,
            clusters
        );
        boolean isRunning = true;
        AsyncSearchResponse asyncSearchResponse;
        try {
            asyncSearchResponse = new AsyncSearchResponse(searchId, searchResponse, null, false, isRunning, 100, 200);
        } finally {
            searchResponse.decRef();
        }
        try {
            AsyncStatusResponse statusFromStoredSearch = AsyncStatusResponse.getStatusFromStoredSearch(asyncSearchResponse, 100, searchId);
            assertNotNull(statusFromStoredSearch);
            assertEquals(0, statusFromStoredSearch.getFailedShards());
            assertNull("completion_status should not be present if still running", statusFromStoredSearch.getCompletionStatus());
            assertEquals(100, statusFromStoredSearch.getClusters().getTotal());
            assertEquals(successful, statusFromStoredSearch.getClusters().getClusterStateCount(SearchResponse.Cluster.Status.SUCCESSFUL));
            assertEquals(partial, statusFromStoredSearch.getClusters().getClusterStateCount(SearchResponse.Cluster.Status.PARTIAL));
            assertEquals(skipped, statusFromStoredSearch.getClusters().getClusterStateCount(SearchResponse.Cluster.Status.SKIPPED));
        } finally {
            asyncSearchResponse.decRef();
        }
    }
}
