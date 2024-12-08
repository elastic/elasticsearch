/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.integration;

import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.transform.transforms.TransformStats;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

public class TransformTaskFailedStateIT extends TransformRestTestCase {

    @Before
    public void setClusterSettings() throws IOException {
        // Make sure we never retry on failure to speed up the test
        // Set logging level to trace
        // see: https://github.com/elastic/elasticsearch/issues/45562
        Request addFailureRetrySetting = new Request("PUT", "/_cluster/settings");
        // reduces bulk failure spam
        addFailureRetrySetting.setJsonEntity("""
            {
              "persistent": {
                "xpack.transform.num_transform_failure_retries": "0",
                "logger.org.elasticsearch.action.bulk": "info",
                "logger.org.elasticsearch.xpack.core.indexing.AsyncTwoPhaseIndexer": "trace",
                "logger.org.elasticsearch.xpack.transform": "trace"
              }
            }""");
        client().performRequest(addFailureRetrySetting);
    }

    @After
    public void cleanUpPotentiallyFailedTransform() throws Exception {
        adminClient().performRequest(new Request("POST", "/_features/_reset"));
    }

    public void testForceStopFailedTransform() throws Exception {
        String transformId = "test-force-stop-failed-transform";
        createReviewsIndex(REVIEWS_INDEX_NAME, 10, 27, "date", false, -1, null);
        String transformIndex = "failure_pivot_reviews";
        createDestinationIndexWithBadMapping(transformIndex);
        createContinuousPivotReviewsTransform(transformId, transformIndex, null);

        assertThat(getTransformTasks(), is(empty()));
        assertThat(getTransformTasksFromClusterState(transformId), is(empty()));

        startTransform(transformId);
        awaitState(transformId, TransformStats.State.FAILED);
        Map<?, ?> fullState = getTransformStateAndStats(transformId);
        var failureReason = "Failed to index documents into destination index due to permanent error: "
            + "[org.elasticsearch.xpack.transform.transforms.BulkIndexingException: Bulk index experienced [7] "
            + "failures and at least 1 irrecoverable "
            + "[org.elasticsearch.xpack.transform.transforms.TransformException: Destination index mappings are "
            + "incompatible with the transform configuration.;";
        // Verify we have failed for the expected reason
        assertThat((String) XContentMapValues.extractValue("reason", fullState), startsWith(failureReason));

        assertThat(getTransformTasks(), hasSize(1));
        assertThat(getTransformTasksFromClusterState(transformId), hasSize(1));

        // verify that we cannot stop a failed transform
        ResponseException ex = expectThrows(ResponseException.class, () -> stopTransform(transformId, false));
        assertThat(ex.getResponse().getStatusLine().getStatusCode(), equalTo(RestStatus.CONFLICT.getStatus()));
        assertThat(
            (String) XContentMapValues.extractValue("error.reason", entityAsMap(ex.getResponse())),
            startsWith(
                "Unable to stop transform [test-force-stop-failed-transform] as it is in a failed state. "
                    + "Use force stop to stop the transform. More details: ["
                    + failureReason
            )
        );

        // Verify that we can force stop a failed transform
        stopTransform(transformId, true);

        awaitState(transformId, TransformStats.State.STOPPED);
        fullState = getTransformStateAndStats(transformId);
        assertThat(XContentMapValues.extractValue("reason", fullState), is(nullValue()));

        assertThat(getTransformTasks(), is(empty()));
        assertThat(getTransformTasksFromClusterState(transformId), is(empty()));
    }

    public void testForceResetFailedTransform() throws Exception {
        String transformId = "test-force-reset-failed-transform";
        createReviewsIndex(REVIEWS_INDEX_NAME, 10, 27, "date", false, -1, null);
        String transformIndex = "failure_pivot_reviews";
        createDestinationIndexWithBadMapping(transformIndex);
        createContinuousPivotReviewsTransform(transformId, transformIndex, null);

        assertThat(getTransformTasks(), is(empty()));
        assertThat(getTransformTasksFromClusterState(transformId), is(empty()));

        startTransform(transformId);
        awaitState(transformId, TransformStats.State.FAILED);
        Map<?, ?> fullState = getTransformStateAndStats(transformId);
        var failureReason = "Failed to index documents into destination index due to permanent error: "
            + "[org.elasticsearch.xpack.transform.transforms.BulkIndexingException: Bulk index experienced [7] "
            + "failures and at least 1 irrecoverable "
            + "[org.elasticsearch.xpack.transform.transforms.TransformException: Destination index mappings are "
            + "incompatible with the transform configuration.;";
        // Verify we have failed for the expected reason
        assertThat((String) XContentMapValues.extractValue("reason", fullState), startsWith(failureReason));

        assertThat(getTransformTasks(), hasSize(1));
        assertThat(getTransformTasksFromClusterState(transformId), hasSize(1));

        // verify that we cannot reset a failed transform
        ResponseException ex = expectThrows(ResponseException.class, () -> resetTransform(transformId, false));
        assertThat(ex.getResponse().getStatusLine().getStatusCode(), equalTo(RestStatus.CONFLICT.getStatus()));
        assertThat(
            (String) XContentMapValues.extractValue("error.reason", entityAsMap(ex.getResponse())),
            is(equalTo("Cannot reset transform [test-force-reset-failed-transform] as the task is running. Stop the task first"))
        );

        // Verify that we can force reset a failed transform
        resetTransform(transformId, true);

        assertThat(getTransformTasks(), is(empty()));
        assertThat(getTransformTasksFromClusterState(transformId), is(empty()));
    }

    public void testStartFailedTransform() throws Exception {
        String transformId = "test-force-start-failed-transform";
        createReviewsIndex(REVIEWS_INDEX_NAME, 10, 27, "date", false, -1, null);
        String transformIndex = "failure_pivot_reviews";
        createDestinationIndexWithBadMapping(transformIndex);
        createContinuousPivotReviewsTransform(transformId, transformIndex, null);

        assertThat(getTransformTasks(), is(empty()));
        assertThat(getTransformTasksFromClusterState(transformId), is(empty()));

        startTransform(transformId);
        awaitState(transformId, TransformStats.State.FAILED);
        Map<?, ?> fullState = getTransformStateAndStats(transformId);
        var failureReason = "Failed to index documents into destination index due to permanent error: "
            + "[org.elasticsearch.xpack.transform.transforms.BulkIndexingException: Bulk index experienced [7] "
            + "failures and at least 1 irrecoverable "
            + "[org.elasticsearch.xpack.transform.transforms.TransformException: Destination index mappings are "
            + "incompatible with the transform configuration.;";
        // Verify we have failed for the expected reason
        assertThat((String) XContentMapValues.extractValue("reason", fullState), startsWith(failureReason));

        assertThat(getTransformTasks(), hasSize(1));
        assertThat(getTransformTasksFromClusterState(transformId), hasSize(1));

        var expectedFailure = "Unable to start transform [test-force-start-failed-transform] "
            + "as it is in a failed state. Use force stop and then restart the transform once error is resolved. More details: ["
            + failureReason;
        // Verify that we cannot start the transform when the task is in a failed state
        assertBusy(() -> {
            ResponseException ex = expectThrows(ResponseException.class, () -> startTransform(transformId));
            assertThat(ex.getResponse().getStatusLine().getStatusCode(), equalTo(RestStatus.CONFLICT.getStatus()));
            assertThat((String) XContentMapValues.extractValue("error.reason", entityAsMap(ex.getResponse())), startsWith(expectedFailure));
        }, 60, TimeUnit.SECONDS);

        stopTransform(transformId, true);

        assertThat(getTransformTasks(), is(empty()));
        assertThat(getTransformTasksFromClusterState(transformId), is(empty()));
    }

    private void awaitState(String transformId, TransformStats.State state) throws Exception {
        assertBusy(() -> {
            String currentState = getTransformState(transformId);
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
