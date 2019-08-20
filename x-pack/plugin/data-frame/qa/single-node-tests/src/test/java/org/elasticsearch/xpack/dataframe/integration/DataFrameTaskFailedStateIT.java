/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.integration;

import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformStats;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.oneOf;

public class DataFrameTaskFailedStateIT extends DataFrameRestTestCase {

    private final List<String> failureTransforms = new ArrayList<>();
    @Before
    public void setClusterSettings() throws IOException {
        // Make sure we never retry on failure to speed up the test
        // Set logging level to trace
        // see: https://github.com/elastic/elasticsearch/issues/45562
        Request addFailureRetrySetting = new Request("PUT", "/_cluster/settings");
        addFailureRetrySetting.setJsonEntity(
            "{\"transient\": {\"xpack.data_frame.num_transform_failure_retries\": \"" + 0 + "\"," +
                "\"logger.org.elasticsearch.action.bulk\": \"info\"," + // reduces bulk failure spam
                "\"logger.org.elasticsearch.xpack.core.indexing.AsyncTwoPhaseIndexer\": \"trace\"," +
                "\"logger.org.elasticsearch.xpack.dataframe\": \"trace\"}}");
        client().performRequest(addFailureRetrySetting);
    }

    @After
    public void cleanUpPotentiallyFailedTransform() throws Exception {
        // If the tests failed in the middle, we should force stop it. This prevents other transform tests from failing due
        // to this left over transform
        for (String transformId : failureTransforms) {
            stopDataFrameTransform(transformId, true);
            deleteDataFrameTransform(transformId);
        }
    }

    public void testForceStopFailedTransform() throws Exception {
        String transformId = "test-force-stop-failed-transform";
        createReviewsIndex(REVIEWS_INDEX_NAME, 10);
        String dataFrameIndex = "failure_pivot_reviews";
        createDestinationIndexWithBadMapping(dataFrameIndex);
        createContinuousPivotReviewsTransform(transformId, dataFrameIndex, null);
        failureTransforms.add(transformId);
        startDataframeTransform(transformId, false);
        awaitState(transformId, DataFrameTransformStats.State.FAILED);
        Map<?, ?> fullState = getDataFrameState(transformId);
        final String failureReason = "task encountered more than 0 failures; latest failure: " +
            "Bulk index experienced failures. See the logs of the node running the transform for details.";
        // Verify we have failed for the expected reason
        assertThat(XContentMapValues.extractValue("reason", fullState), equalTo(failureReason));

        // verify that we cannot stop a failed transform
        ResponseException ex = expectThrows(ResponseException.class, () -> stopDataFrameTransform(transformId, false));
        assertThat(ex.getResponse().getStatusLine().getStatusCode(), equalTo(RestStatus.CONFLICT.getStatus()));
        assertThat(XContentMapValues.extractValue("error.reason", entityAsMap(ex.getResponse())),
            equalTo("Unable to stop data frame transform [test-force-stop-failed-transform] as it is in a failed state with reason [" +
                failureReason +
                "]. Use force stop to stop the data frame transform."));

        // Verify that we can force stop a failed transform
        stopDataFrameTransform(transformId, true);

        awaitState(transformId, DataFrameTransformStats.State.STOPPED);
        fullState = getDataFrameState(transformId);
        assertThat(XContentMapValues.extractValue("reason", fullState), is(nullValue()));
    }

    public void testForceStartFailedTransform() throws Exception {
        String transformId = "test-force-start-failed-transform";
        createReviewsIndex(REVIEWS_INDEX_NAME, 10);
        String dataFrameIndex = "failure_pivot_reviews";
        createDestinationIndexWithBadMapping(dataFrameIndex);
        createContinuousPivotReviewsTransform(transformId, dataFrameIndex, null);
        failureTransforms.add(transformId);
        startDataframeTransform(transformId, false);
        awaitState(transformId, DataFrameTransformStats.State.FAILED);
        Map<?, ?> fullState = getDataFrameState(transformId);
        final String failureReason = "task encountered more than 0 failures; latest failure: " +
            "Bulk index experienced failures. See the logs of the node running the transform for details.";
        // Verify we have failed for the expected reason
        assertThat(XContentMapValues.extractValue("reason", fullState), equalTo(failureReason));

        final String expectedFailure = "Unable to start data frame transform [test-force-start-failed-transform] " +
            "as it is in a failed state with failure: [" + failureReason +
            "]. Use force start to restart data frame transform once error is resolved.";
        // Verify that we cannot start the transform when the task is in a failed state
        assertBusy(() -> {
            ResponseException ex = expectThrows(ResponseException.class, () -> startDataframeTransform(transformId, false));
            assertThat(ex.getResponse().getStatusLine().getStatusCode(), equalTo(RestStatus.CONFLICT.getStatus()));
            assertThat(XContentMapValues.extractValue("error.reason", entityAsMap(ex.getResponse())),
                equalTo(expectedFailure));
        }, 60, TimeUnit.SECONDS);

        // Correct the failure by deleting the destination index
        deleteIndex(dataFrameIndex);
        // Force start the data frame to indicate failure correction
        startDataframeTransform(transformId, true);

        // Verify that we have started and that our reason is cleared
        fullState = getDataFrameState(transformId);
        assertThat(XContentMapValues.extractValue("reason", fullState), is(nullValue()));
        assertThat(XContentMapValues.extractValue("state", fullState), oneOf("started", "indexing"));
        assertThat((Integer)XContentMapValues.extractValue("stats.index_failures", fullState), greaterThanOrEqualTo(1));

        stopDataFrameTransform(transformId, true);
    }

    private void awaitState(String transformId, DataFrameTransformStats.State state) throws Exception {
        assertBusy(() -> {
            String currentState = getDataFrameTransformState(transformId);
            assertThat(currentState, equalTo(state.value()));
        }, 180, TimeUnit.SECONDS); // It should not take this long, but if the scheduler gets deferred, it could
    }

    private void createDestinationIndexWithBadMapping(String indexName) throws IOException {
        // create mapping
        try (XContentBuilder builder = jsonBuilder()) {
            builder.startObject();
            {
                builder.startObject("mappings")
                    .startObject("properties")
                    .startObject("reviewer")
                    .field("type", "long")
                    .endObject()
                    .endObject()
                    .endObject();
            }
            builder.endObject();
            final StringEntity entity = new StringEntity(Strings.toString(builder), ContentType.APPLICATION_JSON);
            Request req = new Request("PUT", indexName);
            req.setEntity(entity);
            client().performRequest(req);
        }
    }
}
