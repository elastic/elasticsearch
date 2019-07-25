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
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformTaskState;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.equalTo;

public class DataFrameTaskFailedStateIT extends DataFrameRestTestCase {

    private static final String TRANSFORM_ID = "failure_pivot_1";

    @Before
    public void setClusterSettings() throws IOException {
        // Make sure we never retry on failure to speed up the test
        Request addFailureRetrySetting = new Request("PUT", "/_cluster/settings");
        addFailureRetrySetting.setJsonEntity(
            "{\"persistent\": {\"xpack.data_frame.num_transform_failure_retries\": \"" + 0 + "\"}}");
        client().performRequest(addFailureRetrySetting);
    }

    @After
    public void cleanUpPotentiallyFailedTransform() throws Exception {
        // If the tests failed in the middle, we should force stop it. This prevents other transform tests from failing due
        // to this left over transform
        stopDataFrameTransform(TRANSFORM_ID, true);
        deleteDataFrameTransform(TRANSFORM_ID);
    }

    public void testForceStopFailedTransform() throws Exception {
        createReviewsIndex(REVIEWS_INDEX_NAME, 10);
        String dataFrameIndex = "failure_pivot_reviews";
        createDestinationIndexWithBadMapping(dataFrameIndex);
        createContinuousPivotReviewsTransform(TRANSFORM_ID, dataFrameIndex, null);
        startDataframeTransform(TRANSFORM_ID, false);
        awaitState(TRANSFORM_ID, DataFrameTransformTaskState.FAILED);
        Map<?, ?> fullState = getDataFrameState(TRANSFORM_ID);
        final String failureReason = "task encountered more than 0 failures; latest failure: " +
            "Bulk index experienced failures. See the logs of the node running the transform for details.";
        // Verify we have failed for the expected reason
        assertThat(XContentMapValues.extractValue("reason", fullState), equalTo(failureReason));

        // verify that we cannot stop a failed transform
        ResponseException ex = expectThrows(ResponseException.class, () -> stopDataFrameTransform(TRANSFORM_ID, false));
        assertThat(ex.getResponse().getStatusLine().getStatusCode(), equalTo(RestStatus.CONFLICT.getStatus()));
        assertThat(XContentMapValues.extractValue("error.reason", entityAsMap(ex.getResponse())),
            equalTo("Unable to stop data frame transform [failure_pivot_1] as it is in a failed state with reason [" +
                failureReason +
                "]. Use force stop to stop the data frame transform."));

        // Verify that we can force stop a failed transform
        stopDataFrameTransform(TRANSFORM_ID, true);

        awaitState(TRANSFORM_ID, DataFrameTransformTaskState.STOPPED);
        fullState = getDataFrameState(TRANSFORM_ID);
        // Verify we have failed for the expected reason
        assertThat(XContentMapValues.extractValue("reason", fullState), is(nullValue()));
    }

    public void testForceStartFailedTransform() throws Exception {
        createReviewsIndex(REVIEWS_INDEX_NAME, 10);
        String dataFrameIndex = "failure_pivot_reviews";
        createDestinationIndexWithBadMapping(dataFrameIndex);
        createContinuousPivotReviewsTransform(TRANSFORM_ID, dataFrameIndex, null);
        startDataframeTransform(TRANSFORM_ID, false);
        awaitState(TRANSFORM_ID, DataFrameTransformTaskState.FAILED);
        Map<?, ?> fullState = getDataFrameState(TRANSFORM_ID);
        final String failureReason = "task encountered more than 0 failures; latest failure: " +
            "Bulk index experienced failures. See the logs of the node running the transform for details.";
        // Verify we have failed for the expected reason
        assertThat(XContentMapValues.extractValue("reason", fullState), equalTo(failureReason));

        // Verify that we cannot start the transform when the task is in a failed state
        ResponseException ex = expectThrows(ResponseException.class, () -> startDataframeTransform(TRANSFORM_ID, false));
        assertThat(ex.getResponse().getStatusLine().getStatusCode(), equalTo(RestStatus.CONFLICT.getStatus()));
        assertThat(XContentMapValues.extractValue("error.reason", entityAsMap(ex.getResponse())),
            equalTo("Unable to start data frame transform [failure_pivot_1] as it is in a failed state with failure: [" +
                failureReason +
                "]. Use force start to restart data frame transform once error is resolved."));

        // Correct the failure by deleting the destination index
        deleteIndex(dataFrameIndex);
        // Force start the data frame to indicate failure correction
        startDataframeTransform(TRANSFORM_ID, true);

        // Verify that we have started and that our reason is cleared
        fullState = getDataFrameState(TRANSFORM_ID);
        assertThat(XContentMapValues.extractValue("reason", fullState), is(nullValue()));
        assertThat(XContentMapValues.extractValue("task_state", fullState), equalTo("started"));
        assertThat((int)XContentMapValues.extractValue("stats.index_failures", fullState), equalTo(1));

        stopDataFrameTransform(TRANSFORM_ID, true);
    }

    private void awaitState(String transformId, DataFrameTransformTaskState state) throws Exception {
        assertBusy(() -> {
            String currentState = getDataFrameTaskState(transformId);
            assertThat(currentState, equalTo(state.value()));
        }, 180, TimeUnit.SECONDS); // It should not take this long, but if the scheduler gets deferred, it could
    }

    private void assertOnePivotValue(String query, double expected) throws IOException {
        Map<String, Object> searchResult = getAsMap(query);

        assertEquals(1, XContentMapValues.extractValue("hits.total.value", searchResult));
        double actual = (Double) ((List<?>) XContentMapValues.extractValue("hits.hits._source.avg_rating", searchResult)).get(0);
        assertEquals(expected, actual, 0.000001);
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
