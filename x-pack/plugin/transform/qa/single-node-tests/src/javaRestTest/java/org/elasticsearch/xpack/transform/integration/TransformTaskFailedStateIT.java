/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform.integration;

import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.transform.transforms.TransformStats;
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
import static org.hamcrest.Matchers.matchesRegex;

public class TransformTaskFailedStateIT extends TransformRestTestCase {

    private final List<String> failureTransforms = new ArrayList<>();

    @Before
    public void setClusterSettings() throws IOException {
        // Make sure we never retry on failure to speed up the test
        // Set logging level to trace
        // see: https://github.com/elastic/elasticsearch/issues/45562
        Request addFailureRetrySetting = new Request("PUT", "/_cluster/settings");
        addFailureRetrySetting.setJsonEntity(
            "{\"transient\": {\"xpack.transform.num_transform_failure_retries\": \""
                + 0
                + "\","
                + "\"logger.org.elasticsearch.action.bulk\": \"info\","
                + // reduces bulk failure spam
                "\"logger.org.elasticsearch.xpack.core.indexing.AsyncTwoPhaseIndexer\": \"trace\","
                + "\"logger.org.elasticsearch.xpack.transform\": \"trace\"}}"
        );
        client().performRequest(addFailureRetrySetting);
    }

    @After
    public void cleanUpPotentiallyFailedTransform() throws Exception {
        // If the tests failed in the middle, we should force stop it. This prevents other transform tests from failing due
        // to this left over transform
        for (String transformId : failureTransforms) {
            stopTransform(transformId, true);
            deleteTransform(transformId);
        }
    }

    public void testForceStopFailedTransform() throws Exception {
        String transformId = "test-force-stop-failed-transform";
        createReviewsIndex(REVIEWS_INDEX_NAME, 10, "date", false, -1, null);
        String transformIndex = "failure_pivot_reviews";
        createDestinationIndexWithBadMapping(transformIndex);
        createContinuousPivotReviewsTransform(transformId, transformIndex, null);
        failureTransforms.add(transformId);
        startTransform(transformId);
        awaitState(transformId, TransformStats.State.FAILED);
        Map<?, ?> fullState = getTransformStateAndStats(transformId);
        final String failureReason = "Failed to index documents into destination index due to permanent error: "
            + "\\[org.elasticsearch.xpack.transform.transforms.BulkIndexingException: Bulk index experienced \\[7\\] "
            + "failures and at least 1 irrecoverable "
            + "\\[org.elasticsearch.xpack.transform.transforms.TransformException: Destination index mappings are "
            + "incompatible with the transform configuration.;.*";
        // Verify we have failed for the expected reason
        assertThat((String) XContentMapValues.extractValue("reason", fullState), matchesRegex(failureReason));

        // verify that we cannot stop a failed transform
        ResponseException ex = expectThrows(ResponseException.class, () -> stopTransform(transformId, false));
        assertThat(ex.getResponse().getStatusLine().getStatusCode(), equalTo(RestStatus.CONFLICT.getStatus()));
        assertThat(
            (String) XContentMapValues.extractValue("error.reason", entityAsMap(ex.getResponse())),
            matchesRegex(
                "Unable to stop transform \\[test-force-stop-failed-transform\\] as it is in a failed state with reason \\["
                    + failureReason
                    + "\\]. Use force stop to stop the transform."
            )
        );

        // Verify that we can force stop a failed transform
        stopTransform(transformId, true);

        awaitState(transformId, TransformStats.State.STOPPED);
        fullState = getTransformStateAndStats(transformId);
        assertThat(XContentMapValues.extractValue("reason", fullState), is(nullValue()));
    }

    public void testStartFailedTransform() throws Exception {
        String transformId = "test-force-start-failed-transform";
        createReviewsIndex(REVIEWS_INDEX_NAME, 10, "date", false, -1, null);
        String transformIndex = "failure_pivot_reviews";
        createDestinationIndexWithBadMapping(transformIndex);
        createContinuousPivotReviewsTransform(transformId, transformIndex, null);
        failureTransforms.add(transformId);
        startTransform(transformId);
        awaitState(transformId, TransformStats.State.FAILED);
        Map<?, ?> fullState = getTransformStateAndStats(transformId);
        final String failureReason = "Failed to index documents into destination index due to permanent error: "
            + "\\[org.elasticsearch.xpack.transform.transforms.BulkIndexingException: Bulk index experienced \\[7\\] "
            + "failures and at least 1 irrecoverable "
            + "\\[org.elasticsearch.xpack.transform.transforms.TransformException: Destination index mappings are "
            + "incompatible with the transform configuration.;.*";
        // Verify we have failed for the expected reason
        assertThat((String) XContentMapValues.extractValue("reason", fullState), matchesRegex(failureReason));

        final String expectedFailure = "Unable to start transform \\[test-force-start-failed-transform\\] "
            + "as it is in a failed state with failure: \\["
            + failureReason
            + "\\]. Use force stop and then restart the transform once error is resolved.";
        // Verify that we cannot start the transform when the task is in a failed state
        assertBusy(() -> {
            ResponseException ex = expectThrows(ResponseException.class, () -> startTransform(transformId));
            assertThat(ex.getResponse().getStatusLine().getStatusCode(), equalTo(RestStatus.CONFLICT.getStatus()));
            assertThat(
                (String) XContentMapValues.extractValue("error.reason", entityAsMap(ex.getResponse())),
                matchesRegex(expectedFailure)
            );
        }, 60, TimeUnit.SECONDS);

        stopTransform(transformId, true);
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
