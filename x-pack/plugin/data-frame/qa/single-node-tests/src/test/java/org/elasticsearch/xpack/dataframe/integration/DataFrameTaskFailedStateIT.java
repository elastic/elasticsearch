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

import java.io.IOException;
import java.util.List;
import java.util.Map;

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
        createPivotReviewsTransform(transformId, dataFrameIndex, null);
        deleteIndex(REVIEWS_INDEX_NAME); // trigger start failure due to index missing
        startDataframeTransform(transformId, false);
        awaitState(transformId, DataFrameTransformTaskState.FAILED);
        Map<?, ?> fullState = getDataFrameState(transformId);

        // Verify we have failed for the expected reason
        assertThat(XContentMapValues.extractValue("state.reason", fullState),
            equalTo("task encountered irrecoverable failure: no such index [reviews]"));
        assertThat(XContentMapValues.extractValue("state.indexer_state", fullState), equalTo("started"));

        // Verify that we cannot stop or start the transform when the task is in a failed state
        ResponseException ex = expectThrows(ResponseException.class, () -> stopDataFrameTransform(transformId, false));
        assertThat(ex.getResponse().getStatusLine().getStatusCode(), equalTo(RestStatus.CONFLICT.getStatus()));
        assertThat(XContentMapValues.extractValue("error.reason", entityAsMap(ex.getResponse())),
            equalTo("Unable to stop data frame transform [failure_pivot_1] as it is in a failed state with reason: [" +
                "task encountered irrecoverable failure: no such index [reviews]]. Use force stop to stop the data frame transform."));

        ex = expectThrows(ResponseException.class, () -> startDataframeTransform(transformId, false));
        assertThat(ex.getResponse().getStatusLine().getStatusCode(), equalTo(RestStatus.CONFLICT.getStatus()));
        assertThat(XContentMapValues.extractValue("error.reason", entityAsMap(ex.getResponse())),
            equalTo("Unable to start data frame transform [failure_pivot_1] as it is in a failed state with failure: [" +
                "task encountered irrecoverable failure: no such index [reviews]]. " +
                "Use force start to restart data frame transform once error is resolved."));

        // Correct the failure by creating the reviews index again
        createReviewsIndex();
        // Force start the data frame to indicate failure correction
        startDataframeTransform(transformId, true);
        // Wait for data to be indexed appropriately and refresh for search
        waitForDataFrameCheckpoint(transformId);
        refreshIndex(dataFrameIndex);

        // Verify that we have started and that our reason is cleared
        fullState = getDataFrameState(transformId);
        assertThat(XContentMapValues.extractValue("state.reason", fullState), is(nullValue()));
        assertThat(XContentMapValues.extractValue("state.task_state", fullState), equalTo("started"));
        assertThat(XContentMapValues.extractValue("state.indexer_state", fullState), equalTo("started"));
        assertThat(XContentMapValues.extractValue("stats.search_failures", fullState), equalTo(1));

        // get and check some users to verify we restarted
        assertOnePivotValue(dataFrameIndex + "/_search?q=reviewer:user_0", 3.776978417);
        assertOnePivotValue(dataFrameIndex + "/_search?q=reviewer:user_5", 3.72);
        assertOnePivotValue(dataFrameIndex + "/_search?q=reviewer:user_11", 3.846153846);
        assertOnePivotValue(dataFrameIndex + "/_search?q=reviewer:user_20", 3.769230769);
        assertOnePivotValue(dataFrameIndex + "/_search?q=reviewer:user_26", 3.918918918);


        stopDataFrameTransform(transformId, true);
        deleteDataFrameTransform(transformId);
    }

    private void awaitState(String transformId, DataFrameTransformTaskState state) throws Exception {
        assertBusy(() -> {
            String currentState = getDataFrameTaskState(transformId);
            assertThat(state.value(), equalTo(currentState));
        });
    }

    private void assertOnePivotValue(String query, double expected) throws IOException {
        Map<String, Object> searchResult = getAsMap(query);

        assertEquals(1, XContentMapValues.extractValue("hits.total.value", searchResult));
        double actual = (Double) ((List<?>) XContentMapValues.extractValue("hits.hits._source.avg_rating", searchResult)).get(0);
        assertEquals(expected, actual, 0.000001);
    }
}
