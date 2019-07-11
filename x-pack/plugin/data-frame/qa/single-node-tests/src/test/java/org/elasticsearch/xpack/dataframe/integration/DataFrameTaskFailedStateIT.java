/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.integration;

import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformTaskState;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.equalTo;

public class DataFrameTaskFailedStateIT extends DataFrameRestTestCase {

    public void testDummy() {
        // remove once the awaits fix below is resolved
    }

    @AwaitsFix( bugUrl = "https://github.com/elastic/elasticsearch/issues/40543")
    public void testFailureStateInteraction() throws Exception {
        createReviewsIndex();
        String transformId = "failure_pivot_1";
        String dataFrameIndex = "failure_pivot_reviews";
        createContinuousPivotReviewsTransform(transformId, dataFrameIndex, null);
        startDataframeTransform(transformId, false);
        // wait for our initial indexing to complete
        waitForDataFrameCheckpoint(transformId);

        // Deleting the only concrete index should make the checkpoint gathering fail
        deleteIndex(REVIEWS_INDEX_NAME); // trigger start failure due to index missing

        awaitState(transformId, DataFrameTransformTaskState.FAILED);
        Map<?, ?> fullState = getDataFrameState(transformId);

        // Verify we have failed for the expected reason
        assertThat(XContentMapValues.extractValue("state.reason", fullState),
            equalTo("task encountered irrecoverable failure: no such index [reviews]"));

        // Verify that we cannot stop or start the transform when the task is in a failed state
        ResponseException ex = expectThrows(ResponseException.class, () -> stopDataFrameTransform(transformId, false));
        assertThat(ex.getResponse().getStatusLine().getStatusCode(), equalTo(RestStatus.CONFLICT.getStatus()));
        assertThat(XContentMapValues.extractValue("error.reason", entityAsMap(ex.getResponse())),
            equalTo("Unable to stop data frame transform [failure_pivot_1] as it is in a failed state with reason [" +
                "task encountered irrecoverable failure: no such index [reviews]. " +
                "Use force stop to stop the data frame transform."));

        ex = expectThrows(ResponseException.class, () -> startDataframeTransform(transformId, false));
        assertThat(ex.getResponse().getStatusLine().getStatusCode(), equalTo(RestStatus.CONFLICT.getStatus()));
        assertThat(XContentMapValues.extractValue("error.reason", entityAsMap(ex.getResponse())),
            equalTo("Unable to start data frame transform [failure_pivot_1] as it is in a failed state with failure: [" +
                "task encountered irrecoverable failure: no such index [reviews]. " +
                "Use force start to restart data frame transform once error is resolved."));

        // Correct the failure by creating the reviews index again
        createReviewsIndex();
        // Force start the data frame to indicate failure correction
        startDataframeTransform(transformId, true);
        // Wait for data to be indexed appropriately and refresh for search
        awaitState(transformId, DataFrameTransformTaskState.STARTED);

        // Verify that we have started and that our reason is cleared
        fullState = getDataFrameState(transformId);
        assertThat(XContentMapValues.extractValue("state.reason", fullState), is(nullValue()));
        assertThat(XContentMapValues.extractValue("state.task_state", fullState), equalTo("started"));

        stopDataFrameTransform(transformId, true);
        deleteDataFrameTransform(transformId);
    }

    private void awaitState(String transformId, DataFrameTransformTaskState state) throws Exception {
        assertBusy(() -> {
            String currentState = getDataFrameTaskState(transformId);
            assertThat(state.value(), equalTo(currentState));
        }, 60, TimeUnit.SECONDS);
    }
}
