/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform.integration;

import org.elasticsearch.client.Request;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.oneOf;

public class TransformGetAndGetStatsIT extends TransformRestTestCase {

    private static final String TEST_USER_NAME = "transform_user";
    private static final String BASIC_AUTH_VALUE_TRANSFORM_USER = basicAuthHeaderValue(TEST_USER_NAME, TEST_PASSWORD_SECURE_STRING);
    private static final String TEST_ADMIN_USER_NAME = "transform_admin";
    private static final String BASIC_AUTH_VALUE_TRANSFORM_ADMIN = basicAuthHeaderValue(TEST_ADMIN_USER_NAME, TEST_PASSWORD_SECURE_STRING);

    private static boolean indicesCreated = false;

    // preserve indices in order to reuse source indices in several test cases
    @Override
    protected boolean preserveIndicesUponCompletion() {
        return true;
    }

    @Before
    public void createIndexes() throws IOException {

        // it's not possible to run it as @BeforeClass as clients aren't initialized then, so we need this little hack
        if (indicesCreated) {
            return;
        }

        createReviewsIndex();
        indicesCreated = true;

        // at random test the old deprecated roles, to be removed in 9.0.0
        if (useDeprecatedEndpoints() && randomBoolean()) {
            setupUser(TEST_USER_NAME, Collections.singletonList("data_frame_transforms_user"));
            setupUser(TEST_ADMIN_USER_NAME, Collections.singletonList("data_frame_transforms_admin"));
        } else {
            setupUser(TEST_USER_NAME, Collections.singletonList("transform_user"));
            setupUser(TEST_ADMIN_USER_NAME, Collections.singletonList("transform_admin"));
        }
    }

    @After
    public void clearOutTransforms() throws Exception {
        wipeTransforms();
    }

    @SuppressWarnings("unchecked")
    public void testGetAndGetStats() throws Exception {
        createPivotReviewsTransform("pivot_1", "pivot_reviews_1", null);
        createPivotReviewsTransform("pivot_2", "pivot_reviews_2", null);
        createContinuousPivotReviewsTransform("pivot_continuous", "pivot_reviews_continuous", null);

        startAndWaitForTransform("pivot_1", "pivot_reviews_1");
        startAndWaitForTransform("pivot_2", "pivot_reviews_2");
        startAndWaitForContinuousTransform("pivot_continuous", "pivot_reviews_continuous", null);
        stopTransform("pivot_1", false);
        stopTransform("pivot_2", false);

        // Alternate testing between admin and lowly user, as both should be able to get the configs and stats
        String authHeader = randomFrom(BASIC_AUTH_VALUE_TRANSFORM_USER, BASIC_AUTH_VALUE_TRANSFORM_ADMIN);

        // check all the different ways to retrieve all stats
        Request getRequest = createRequestWithAuth("GET", getTransformEndpoint() + "_stats", authHeader);
        Map<String, Object> stats = entityAsMap(client().performRequest(getRequest));
        assertEquals(3, XContentMapValues.extractValue("count", stats));
        getRequest = createRequestWithAuth("GET", getTransformEndpoint() + "_all/_stats", authHeader);
        stats = entityAsMap(client().performRequest(getRequest));
        assertEquals(3, XContentMapValues.extractValue("count", stats));
        getRequest = createRequestWithAuth("GET", getTransformEndpoint() + "*/_stats", authHeader);
        stats = entityAsMap(client().performRequest(getRequest));
        assertEquals(3, XContentMapValues.extractValue("count", stats));
        getRequest = createRequestWithAuth("GET", getTransformEndpoint() + "pivot_1,pivot_2/_stats", authHeader);
        stats = entityAsMap(client().performRequest(getRequest));
        assertEquals(2, XContentMapValues.extractValue("count", stats));
        getRequest = createRequestWithAuth("GET", getTransformEndpoint() + "pivot_*/_stats", authHeader);
        stats = entityAsMap(client().performRequest(getRequest));
        assertEquals(3, XContentMapValues.extractValue("count", stats));

        List<Map<String, Object>> transformsStats = (List<Map<String, Object>>) XContentMapValues.extractValue("transforms", stats);
        // Verify that both transforms have valid stats
        for (Map<String, Object> transformStats : transformsStats) {
            Map<String, Object> stat = (Map<String, Object>) transformStats.get("stats");
            assertThat("documents_processed is not > 0.", ((Integer) stat.get("documents_processed")), greaterThan(0));
            assertThat("search_total is not > 0.", ((Integer) stat.get("search_total")), greaterThan(0));
            assertThat("pages_processed is not > 0.", ((Integer) stat.get("pages_processed")), greaterThan(0));
            /* TODO progress is now checkpoint progress and it may be that no checkpoint is in progress here
            Map<String, Object> progress =
                (Map<String, Object>)XContentMapValues.extractValue("checkpointing.next.checkpoint_progress", transformStats);
            assertThat("total_docs is not 1000", progress.get("total_docs"), equalTo(1000));
            assertThat("docs_remaining is not 0", progress.get("docs_remaining"), equalTo(0));
            assertThat("percent_complete is not 100.0", progress.get("percent_complete"), equalTo(100.0));
            */
        }

        // only pivot_1
        getRequest = createRequestWithAuth("GET", getTransformEndpoint() + "pivot_1/_stats", authHeader);
        stats = entityAsMap(client().performRequest(getRequest));
        assertEquals(1, XContentMapValues.extractValue("count", stats));

        transformsStats = (List<Map<String, Object>>) XContentMapValues.extractValue("transforms", stats);
        assertEquals(1, transformsStats.size());
        assertEquals("stopped", XContentMapValues.extractValue("state", transformsStats.get(0)));
        assertNull(XContentMapValues.extractValue("checkpointing.next.position", transformsStats.get(0)));
        assertEquals(1, XContentMapValues.extractValue("checkpointing.last.checkpoint", transformsStats.get(0)));

        // only continuous
        getRequest = createRequestWithAuth("GET", getTransformEndpoint() + "pivot_continuous/_stats", authHeader);
        stats = entityAsMap(client().performRequest(getRequest));
        assertEquals(1, XContentMapValues.extractValue("count", stats));

        transformsStats = (List<Map<String, Object>>) XContentMapValues.extractValue("transforms", stats);
        assertEquals(1, transformsStats.size());
        assertThat(XContentMapValues.extractValue("state", transformsStats.get(0)), oneOf("started", "indexing"));
        assertEquals(1, XContentMapValues.extractValue("checkpointing.last.checkpoint", transformsStats.get(0)));

        // check all the different ways to retrieve all transforms
        getRequest = createRequestWithAuth("GET", getTransformEndpoint(), authHeader);
        Map<String, Object> transforms = entityAsMap(client().performRequest(getRequest));
        assertEquals(3, XContentMapValues.extractValue("count", transforms));
        getRequest = createRequestWithAuth("GET", getTransformEndpoint() + "_all", authHeader);
        transforms = entityAsMap(client().performRequest(getRequest));
        assertEquals(3, XContentMapValues.extractValue("count", transforms));
        getRequest = createRequestWithAuth("GET", getTransformEndpoint() + "*", authHeader);
        transforms = entityAsMap(client().performRequest(getRequest));
        assertEquals(3, XContentMapValues.extractValue("count", transforms));

        // only pivot_1
        getRequest = createRequestWithAuth("GET", getTransformEndpoint() + "pivot_1", authHeader);
        transforms = entityAsMap(client().performRequest(getRequest));
        assertEquals(1, XContentMapValues.extractValue("count", transforms));

        stopTransform("pivot_continuous", false);
    }

    @SuppressWarnings("unchecked")
    public void testGetPersistedStatsWithoutTask() throws Exception {
        createPivotReviewsTransform("pivot_stats_1", "pivot_reviews_stats_1", null);
        startAndWaitForTransform("pivot_stats_1", "pivot_reviews_stats_1");
        stopTransform("pivot_stats_1", false);

        // Get rid of the first transform task, but keep the configuration
        client().performRequest(new Request("POST", "_tasks/_cancel?actions=" + TransformField.TASK_NAME + "*"));

        // Verify that the task is gone
        Map<String, Object> tasks = entityAsMap(
            client().performRequest(new Request("GET", "_tasks?actions=" + TransformField.TASK_NAME + "*"))
        );
        assertTrue(((Map<?, ?>) XContentMapValues.extractValue("nodes", tasks)).isEmpty());

        createPivotReviewsTransform("pivot_stats_2", "pivot_reviews_stats_2", null);
        startAndWaitForTransform("pivot_stats_2", "pivot_reviews_stats_2");

        Request getRequest = createRequestWithAuth("GET", getTransformEndpoint() + "_stats", BASIC_AUTH_VALUE_TRANSFORM_ADMIN);
        Map<String, Object> stats = entityAsMap(client().performRequest(getRequest));
        assertEquals(2, XContentMapValues.extractValue("count", stats));
        List<Map<String, Object>> transformsStats = (List<Map<String, Object>>) XContentMapValues.extractValue("transforms", stats);
        // Verify that both transforms, the one with the task and the one without have statistics
        for (Map<String, Object> transformStats : transformsStats) {
            Map<String, Object> stat = (Map<String, Object>) transformStats.get("stats");
            assertThat(((Integer) stat.get("documents_processed")), greaterThan(0));
            assertThat(((Integer) stat.get("search_total")), greaterThan(0));
            assertThat(((Integer) stat.get("pages_processed")), greaterThan(0));
        }
    }

    @SuppressWarnings("unchecked")
    public void testGetProgressStatsWithPivotQuery() throws Exception {
        String transformId = "simple_stats_pivot_with_query";
        String transformIndex = "pivot_stats_reviews_user_id_above_20";
        String query = "\"match\": {\"user_id\": \"user_26\"}";
        createPivotReviewsTransform(transformId, transformIndex, query);
        startAndWaitForTransform(transformId, transformIndex);

        // Alternate testing between admin and lowly user, as both should be able to get the configs and stats
        String authHeader = randomFrom(BASIC_AUTH_VALUE_TRANSFORM_USER, BASIC_AUTH_VALUE_TRANSFORM_ADMIN);

        Request getRequest = createRequestWithAuth("GET", getTransformEndpoint() + transformId + "/_stats", authHeader);
        Map<String, Object> stats = entityAsMap(client().performRequest(getRequest));
        assertEquals(1, XContentMapValues.extractValue("count", stats));
        List<Map<String, Object>> transformsStats = (List<Map<String, Object>>) XContentMapValues.extractValue("transforms", stats);
        // Verify that the transform has stats and the total docs process matches the expected
        for (Map<String, Object> transformStats : transformsStats) {
            Map<String, Object> stat = (Map<String, Object>) transformStats.get("stats");
            assertThat("documents_processed is not > 0.", ((Integer) stat.get("documents_processed")), greaterThan(0));
            assertThat("search_total is not > 0.", ((Integer) stat.get("search_total")), greaterThan(0));
            assertThat("pages_processed is not > 0.", ((Integer) stat.get("pages_processed")), greaterThan(0));
            /* TODO progress is now checkpoint progress and it may be that no checkpoint is in progress here
            Map<String, Object> progress =
                (Map<String, Object>)XContentMapValues.extractValue("checkpointing.next.checkpoint_progress", transformStats);
            assertThat("total_docs is not 37", progress.get("total_docs"), equalTo(37));
            assertThat("docs_remaining is not 0", progress.get("docs_remaining"), equalTo(0));
            assertThat("percent_complete is not 100.0", progress.get("percent_complete"), equalTo(100.0));
            */
        }
    }

    @SuppressWarnings("unchecked")
    public void testGetStatsWithContinuous() throws Exception {
        String transformId = "pivot_progress_continuous";
        String transformDest = transformId + "_idx";
        String transformSrc = "reviews_cont_pivot_test";
        createReviewsIndex(transformSrc);
        final Request createTransformRequest = createRequestWithAuth("PUT", getTransformEndpoint() + transformId, null);
        String config = "{ \"dest\": {\"index\":\""
            + transformDest
            + "\"},"
            + " \"source\": {\"index\":\""
            + transformSrc
            + "\"},"
            + " \"frequency\": \"1s\","
            + " \"sync\": {\"time\":{\"field\": \"timestamp\", \"delay\": \"1s\"}},"
            + " \"pivot\": {"
            + "   \"group_by\": {"
            + "     \"reviewer\": {"
            + "       \"terms\": {"
            + "         \"field\": \"user_id\""
            + " } } },"
            + "   \"aggregations\": {"
            + "     \"avg_rating\": {"
            + "       \"avg\": {"
            + "         \"field\": \"stars\""
            + " } } } }"
            + "}";

        createTransformRequest.setJsonEntity(config);

        Map<String, Object> createTransformResponse = entityAsMap(client().performRequest(createTransformRequest));
        assertThat(createTransformResponse.get("acknowledged"), equalTo(Boolean.TRUE));
        startAndWaitForContinuousTransform(transformId, transformDest, null);

        Request getRequest = createRequestWithAuth("GET", getTransformEndpoint() + transformId + "/_stats", null);
        Map<String, Object> stats = entityAsMap(client().performRequest(getRequest));
        List<Map<String, Object>> transformsStats = (List<Map<String, Object>>) XContentMapValues.extractValue("transforms", stats);
        assertEquals(1, transformsStats.size());
        // No continuous checkpoints have been seen and thus all exponential averages should be equal to the batch stats
        for (Map<String, Object> transformStats : transformsStats) {
            transformStats = (Map<String, Object>) transformStats.get("stats");
            assertThat(transformStats.get("documents_processed"), equalTo(1000));
            assertThat(transformStats.get("documents_indexed"), equalTo(27));
            assertThat(
                "exponential_avg_checkpoint_duration_ms is not 0.0",
                (Double) transformStats.get("exponential_avg_checkpoint_duration_ms"),
                greaterThan(0.0)
            );
            assertThat(
                "exponential_avg_documents_indexed does not match documents_indexed",
                (Double) transformStats.get("exponential_avg_documents_indexed"),
                equalTo(((Integer) transformStats.get("documents_indexed")).doubleValue())
            );
            assertThat(
                "exponential_avg_documents_processed does not match documents_processed",
                transformStats.get("exponential_avg_documents_processed"),
                equalTo(((Integer) transformStats.get("documents_processed")).doubleValue())
            );
        }

        int numDocs = 10;
        final StringBuilder bulk = new StringBuilder();
        long now = Instant.now().toEpochMilli() - 1_000;
        for (int i = 0; i < numDocs; i++) {
            bulk.append("{\"index\":{\"_index\":\"" + transformSrc + "\"}}\n")
                .append("{\"user_id\":\"")
                .append("user_")
                // Doing only new users so that there is a deterministic number of docs for progress
                .append(randomFrom(42, 47, 113))
                .append("\",\"business_id\":\"")
                .append("business_")
                .append(10)
                .append("\",\"stars\":")
                .append(5)
                .append(",\"timestamp\":")
                .append(now)
                .append("}\n");
        }
        bulk.append("\r\n");
        final Request bulkRequest = new Request("POST", "/_bulk");
        bulkRequest.addParameter("refresh", "true");
        bulkRequest.setJsonEntity(bulk.toString());
        client().performRequest(bulkRequest);

        waitForTransformCheckpoint(transformId, 2L);

        // We should now have exp avgs since we have processed a continuous checkpoint
        assertBusy(() -> {
            Map<String, Object> statsResponse = entityAsMap(client().performRequest(getRequest));
            List<Map<String, Object>> contStats = (List<Map<String, Object>>) XContentMapValues.extractValue("transforms", statsResponse);
            assertEquals(1, contStats.size());
            for (Map<String, Object> transformStats : contStats) {
                Map<String, Object> statsObj = (Map<String, Object>) transformStats.get("stats");
                assertThat(
                    "exponential_avg_checkpoint_duration_ms is 0",
                    (Double) statsObj.get("exponential_avg_checkpoint_duration_ms"),
                    greaterThan(0.0)
                );
                assertThat(
                    "exponential_avg_documents_indexed is 0",
                    (Double) statsObj.get("exponential_avg_documents_indexed"),
                    greaterThan(0.0)
                );
                assertThat(
                    "exponential_avg_documents_processed is 0",
                    (Double) statsObj.get("exponential_avg_documents_processed"),
                    greaterThan(0.0)
                );
                Map<String, Object> checkpointing = (Map<String, Object>) transformStats.get("checkpointing");
                assertThat("changes_last_detected_at is null", checkpointing.get("changes_last_detected_at"), is(notNullValue()));
            }
        }, 120, TimeUnit.SECONDS);
    }
}
