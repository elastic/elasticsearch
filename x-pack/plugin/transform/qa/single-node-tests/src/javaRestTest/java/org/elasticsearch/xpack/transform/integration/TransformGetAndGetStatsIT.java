/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.integration;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.transforms.persistence.TransformInternalIndexConstants;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.oneOf;

@SuppressWarnings("removal")
public class TransformGetAndGetStatsIT extends TransformRestTestCase {

    private static final String TEST_USER_NAME = "transform_user";
    private static final String BASIC_AUTH_VALUE_TRANSFORM_USER = basicAuthHeaderValue(TEST_USER_NAME, TEST_PASSWORD_SECURE_STRING);
    private static final String TEST_ADMIN_USER_NAME = "transform_admin";
    private static final String BASIC_AUTH_VALUE_TRANSFORM_ADMIN = basicAuthHeaderValue(TEST_ADMIN_USER_NAME, TEST_PASSWORD_SECURE_STRING);
    private static final String DANGLING_TASK_ERROR_MESSAGE =
        "Found task for transform [pivot_continuous], but no configuration for it. To delete this transform use DELETE with force=true.";

    private static boolean indicesCreated = false;

    // preserve indices in order to reuse source indices in several test cases
    @Override
    protected boolean preserveIndicesUponCompletion() {
        return true;
    }

    @Before
    public void createIndexes() throws IOException {
        setupUser(TEST_USER_NAME, singletonList("transform_user"));
        setupUser(TEST_ADMIN_USER_NAME, singletonList("transform_admin"));

        // it's not possible to run it as @BeforeClass as clients aren't initialized then, so we need this little hack
        if (indicesCreated) {
            return;
        }

        createReviewsIndex();
        indicesCreated = true;

    }

    @After
    public void clearOutTransforms() throws Exception {
        adminClient().performRequest(new Request("POST", "/_features/_reset"));
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

        // Check all the different ways to retrieve transform stats
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

        // Check all the different ways to retrieve transforms
        verifyGetResponse(getTransformEndpoint(), 3, null);
        verifyGetResponse(getTransformEndpoint() + "_all", 3, null);
        verifyGetResponse(getTransformEndpoint() + "*", 3, null);
        verifyGetResponse(getTransformEndpoint() + "pivot_*", 3, null);
        verifyGetResponse(getTransformEndpoint() + "pivot_1,pivot_2", 2, null);
        verifyGetResponse(getTransformEndpoint() + "pivot_1", 1, null);
        verifyGetResponse(getTransformEndpoint() + "pivot_2", 1, null);

        stopTransform("pivot_continuous", false);
    }

    @SuppressWarnings("unchecked")
    public void testGetAndGetStatsForTransformWithoutConfig() throws Exception {
        createPivotReviewsTransform("pivot_1", "pivot_reviews_1", null);
        createPivotReviewsTransform("pivot_2", "pivot_reviews_2", null);
        createContinuousPivotReviewsTransform("pivot_continuous", "pivot_reviews_continuous", null);

        startAndWaitForTransform("pivot_1", "pivot_reviews_1");
        startAndWaitForTransform("pivot_2", "pivot_reviews_2");
        startAndWaitForContinuousTransform("pivot_continuous", "pivot_reviews_continuous", null);
        stopTransform("pivot_1", false);
        stopTransform("pivot_2", false);

        {  // Make sure the config document is there
            Request getTransformConfigRequest = new Request(
                "GET",
                TransformInternalIndexConstants.LATEST_INDEX_NAME + "/_doc/data_frame_transform_config-pivot_continuous"
            );
            getTransformConfigRequest.setOptions(RequestOptions.DEFAULT.toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE));
            Response getTransformConfigResponse = client().performRequest(getTransformConfigRequest);
            assertThat(entityAsMap(getTransformConfigResponse), hasEntry("found", true));
        }
        {  // Delete the config document while the continuous transform is running
            Request deleteTransformConfigRequest = new Request(
                "DELETE",
                TransformInternalIndexConstants.LATEST_INDEX_NAME + "/_doc/data_frame_transform_config-pivot_continuous?refresh=true"
            );
            deleteTransformConfigRequest.setOptions(RequestOptions.DEFAULT.toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE));
            Response deleteTransformConfigResponse = client().performRequest(deleteTransformConfigRequest);
            assertThat(entityAsMap(deleteTransformConfigResponse), hasEntry("result", "deleted"));
        }

        // Check all the different ways to retrieve transforms
        List<Map<String, String>> expectedErrors = List.of(Map.of("type", "dangling_task", "reason", DANGLING_TASK_ERROR_MESSAGE));
        verifyGetResponse(getTransformEndpoint(), 2, expectedErrors);
        verifyGetResponse(getTransformEndpoint() + "_all", 2, expectedErrors);
        verifyGetResponse(getTransformEndpoint() + "*", 2, expectedErrors);
        verifyGetResponse(getTransformEndpoint() + "pivot_*", 2, expectedErrors);
        verifyGetResponse(getTransformEndpoint() + "pivot_1,pivot_2", 2, null);
        verifyGetResponse(getTransformEndpoint() + "pivot_1", 1, null);
        verifyGetResponse(getTransformEndpoint() + "pivot_2", 1, null);
        {
            Request getRequest = createRequestWithAuth("GET", getTransformEndpoint() + "pivot_continuous", BASIC_AUTH_VALUE_TRANSFORM_USER);
            ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(getRequest));
            assertThat(e.getResponse().getStatusLine().getStatusCode(), is(equalTo(404)));
        }

        // Check all the different ways to retrieve transform stats
        verifyGetStatsResponse(getTransformEndpoint() + "_stats", 2);
        verifyGetStatsResponse(getTransformEndpoint() + "_all/_stats", 2);
        verifyGetStatsResponse(getTransformEndpoint() + "*/_stats", 2);
        verifyGetStatsResponse(getTransformEndpoint() + "pivot_*/_stats", 2);
        verifyGetStatsResponse(getTransformEndpoint() + "pivot_1,pivot_2/_stats", 2);
        verifyGetStatsResponse(getTransformEndpoint() + "pivot_1/_stats", 1);
        verifyGetStatsResponse(getTransformEndpoint() + "pivot_2/_stats", 1);
        verifyGetStatsResponse(getTransformEndpoint() + "pivot_continuous/_stats", 0);

        // "force" is needed as we deleted the config document for this transform
        stopTransform("pivot_continuous", true);
    }

    @SuppressWarnings("unchecked")
    public void testGetAndGetStatsWhenTransformInternalIndexDisappears() throws Exception {
        createPivotReviewsTransform("pivot_1", "pivot_reviews_1", null);
        createPivotReviewsTransform("pivot_2", "pivot_reviews_2", null);
        createContinuousPivotReviewsTransform("pivot_continuous", "pivot_reviews_continuous", null);

        startAndWaitForTransform("pivot_1", "pivot_reviews_1");
        startAndWaitForTransform("pivot_2", "pivot_reviews_2");
        startAndWaitForContinuousTransform("pivot_continuous", "pivot_reviews_continuous", null);
        stopTransform("pivot_1", false);
        stopTransform("pivot_2", false);

        {  // Make sure the config document is there
            Request getTransformConfigRequest = new Request(
                "GET",
                TransformInternalIndexConstants.LATEST_INDEX_NAME + "/_doc/data_frame_transform_config-pivot_continuous"
            );
            getTransformConfigRequest.setOptions(RequestOptions.DEFAULT.toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE));
            Response getTransformConfigResponse = client().performRequest(getTransformConfigRequest);
            assertThat(entityAsMap(getTransformConfigResponse), hasEntry("found", true));
        }
        {  // Delete the transform internal index while the continuous transform is running
            Request deleteIndexRequest = new Request("DELETE", TransformInternalIndexConstants.LATEST_INDEX_NAME);
            deleteIndexRequest.setOptions(RequestOptions.DEFAULT.toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE));
            Response deleteIndexResponse = client().performRequest(deleteIndexRequest);
            assertThat(entityAsMap(deleteIndexResponse), hasEntry("acknowledged", true));
        }

        // Check all the different ways to retrieve transforms
        List<Map<String, String>> expectedErrors = List.of(Map.of("type", "dangling_task", "reason", DANGLING_TASK_ERROR_MESSAGE));
        verifyGetResponse(getTransformEndpoint(), 0, expectedErrors);
        verifyGetResponse(getTransformEndpoint() + "_all", 0, expectedErrors);
        verifyGetResponse(getTransformEndpoint() + "*", 0, expectedErrors);
        verifyGetResponse(getTransformEndpoint() + "pivot_*", 0, expectedErrors);
        {
            Request getRequest = createRequestWithAuth("GET", getTransformEndpoint() + "pivot_1,pivot_2", BASIC_AUTH_VALUE_TRANSFORM_USER);
            ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(getRequest));
            assertThat(e.getResponse().getStatusLine().getStatusCode(), is(equalTo(404)));
        }
        {
            Request getRequest = createRequestWithAuth("GET", getTransformEndpoint() + "pivot_1", BASIC_AUTH_VALUE_TRANSFORM_USER);
            ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(getRequest));
            assertThat(e.getResponse().getStatusLine().getStatusCode(), is(equalTo(404)));
        }
        {
            Request getRequest = createRequestWithAuth("GET", getTransformEndpoint() + "pivot_2", BASIC_AUTH_VALUE_TRANSFORM_USER);
            ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(getRequest));
            assertThat(e.getResponse().getStatusLine().getStatusCode(), is(equalTo(404)));
        }
        {
            Request getRequest = createRequestWithAuth("GET", getTransformEndpoint() + "pivot_continuous", BASIC_AUTH_VALUE_TRANSFORM_USER);
            ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(getRequest));
            assertThat(e.getResponse().getStatusLine().getStatusCode(), is(equalTo(404)));
        }

        // Check all the different ways to retrieve transform stats
        verifyGetStatsResponse(getTransformEndpoint() + "_stats", 0);
        verifyGetStatsResponse(getTransformEndpoint() + "_all/_stats", 0);
        verifyGetStatsResponse(getTransformEndpoint() + "*/_stats", 0);
        verifyGetStatsResponse(getTransformEndpoint() + "pivot_*/_stats", 0);
        verifyGetStatsResponse(getTransformEndpoint() + "pivot_1,pivot_2/_stats", 0);
        verifyGetStatsResponse(getTransformEndpoint() + "pivot_1/_stats", 0);
        verifyGetStatsResponse(getTransformEndpoint() + "pivot_2/_stats", 0);
        verifyGetStatsResponse(getTransformEndpoint() + "pivot_continuous/_stats", 0);

        // "force" is needed as we deleted the config document for this transform
        stopTransform("pivot_continuous", true);
    }

    private void verifyGetResponse(String path, int expectedCount, List<Map<String, String>> expectedErrors) throws IOException {
        // Alternate testing between admin and lowly user, as both should be able to get the configs and stats
        String authHeader = randomFrom(BASIC_AUTH_VALUE_TRANSFORM_USER, BASIC_AUTH_VALUE_TRANSFORM_ADMIN);

        Request request = createRequestWithAuth("GET", path, authHeader);
        request.setOptions(RequestOptions.DEFAULT.toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE));
        Response response = client().performRequest(request);
        Map<String, Object> transforms = entityAsMap(response);
        assertThat(XContentMapValues.extractValue("count", transforms), is(equalTo(expectedCount)));
        assertThat(XContentMapValues.extractValue("errors", transforms), is(equalTo(expectedErrors)));
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> verifyGetStatsResponse(String path, int expectedCount) throws IOException {
        // Alternate testing between admin and lowly user, as both should be able to get the configs and stats
        String authHeader = randomFrom(BASIC_AUTH_VALUE_TRANSFORM_USER, BASIC_AUTH_VALUE_TRANSFORM_ADMIN);

        Request request = createRequestWithAuth("GET", path, authHeader);
        request.setOptions(RequestOptions.DEFAULT.toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE));
        Response response = client().performRequest(request);
        Map<String, Object> stats = entityAsMap(response);
        assertThat(XContentMapValues.extractValue("count", stats), is(equalTo(expectedCount)));

        List<Map<String, Object>> transformsStats = (List<Map<String, Object>>) XContentMapValues.extractValue("transforms", stats);
        assertThat(transformsStats, hasSize(expectedCount));
        // Verify that all the transforms have valid stats
        for (Map<String, Object> transformStats : transformsStats) {
            assertThat(XContentMapValues.extractValue("state", transformStats), is(equalTo("stopped")));
            assertThat(XContentMapValues.extractValue("checkpointing.next.position", transformStats), is(nullValue()));
            assertThat(XContentMapValues.extractValue("checkpointing.last.checkpoint", transformStats), is(equalTo(1)));

            Map<String, Object> stat = (Map<String, Object>) transformStats.get("stats");
            assertThat("documents_processed is not > 0.", ((Integer) stat.get("documents_processed")), is(greaterThan(0)));
            assertThat("search_total is not > 0.", ((Integer) stat.get("search_total")), is(greaterThan(0)));
            assertThat("pages_processed is not > 0.", ((Integer) stat.get("pages_processed")), is(greaterThan(0)));
        }
        return transformsStats;
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

        verifyGetStatsResponse(getTransformEndpoint() + "_stats", 2);
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
        String config = """
            {
              "dest": {
                "index": "%s"
              },
              "source": {
                "index": "%s"
              },
              "frequency": "1s",
              "sync": {
                "time": {
                  "field": "timestamp",
                  "delay": "1s"
                }
              },
              "pivot": {
                "group_by": {
                  "reviewer": {
                    "terms": {
                      "field": "user_id"
                    }
                  }
                },
                "aggregations": {
                  "avg_rating": {
                    "avg": {
                      "field": "stars"
                    }
                  }
                }
              }
            }""".formatted(transformDest, transformSrc);

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
            // Doing only new users so that there is a deterministic number of docs for progress
            bulk.append("""
                {"index":{"_index":"%s"}}
                {"user_id":"user_%s","business_id":"business_%s","stars":%s,"timestamp":%s}
                """.formatted(transformSrc, randomFrom(42, 47, 113), 10, 5, now));
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

    @SuppressWarnings("unchecked")
    public void testManyTransforms() throws IOException {
        String config = transformConfig();

        int numberOfTransforms = randomIntBetween(1_500, 4_000);
        for (int i = 0; i < numberOfTransforms; ++i) {
            String transformId = String.format(Locale.ROOT, "t-%05d", i);
            final Request createTransformRequest = createRequestWithAuth("PUT", getTransformEndpoint() + transformId, null);
            createTransformRequest.setJsonEntity(config);
            assertOK(client().performRequest(createTransformRequest));
        }

        for (int i = 0; i < 3; ++i) {
            int from = randomIntBetween(0, numberOfTransforms - 1_000);
            int size = randomIntBetween(1, 1000);

            var transforms = getTransforms(from, size);
            var statsResponse = getTransformsStateAndStats(from, size);

            assertEquals(numberOfTransforms, transforms.get("count"));
            assertEquals(numberOfTransforms, statsResponse.get("count"));

            var configs = (List<Map<String, Object>>) transforms.get("transforms");
            var stats = (List<Map<String, Object>>) statsResponse.get("transforms");

            assertEquals(size, configs.size());
            assertEquals(size, stats.size());

            assertThat(configs.get(0).get("id"), equalTo(String.format(Locale.ROOT, "t-%05d", from)));
            assertThat(configs.get(configs.size() - 1).get("id"), equalTo(String.format(Locale.ROOT, "t-%05d", from + size - 1)));
            assertThat(stats.get(0).get("id"), equalTo(String.format(Locale.ROOT, "t-%05d", from)));
            assertThat(stats.get(stats.size() - 1).get("id"), equalTo(String.format(Locale.ROOT, "t-%05d", from + size - 1)));

            if (size > 2) {
                int randomElement = randomIntBetween(1, size - 1);
                assertThat(configs.get(randomElement).get("id"), equalTo(String.format(Locale.ROOT, "t-%05d", from + randomElement)));
                assertThat(stats.get(randomElement).get("id"), equalTo(String.format(Locale.ROOT, "t-%05d", from + randomElement)));
            }
        }

        for (int i = 0; i < numberOfTransforms; ++i) {
            deleteTransform(String.format(Locale.ROOT, "t-%05d", i));
        }
    }

    private static String transformConfig() {
        return """
            {
              "description": "Test 10000 transform configs",
              "source": {
                "index":""" + "\"" + REVIEWS_INDEX_NAME + "\"" + """
                  },
                  "pivot": {
                    "group_by": {
                      "by-user": {
                        "terms": {
                          "field": "user_id"
                        }
                      }
                    },
                    "aggregations": {
                      "review_score.avg": {
                        "avg": {
                          "field": "stars"
                        }
                      },
                      "timestamp.max": {
                        "max": {
                          "field": "timestamp"
                        }
                      }
                    }
                  },
                  "dest": {
                    "index":"dest"
                  },
                  "frequency": "10s"
                }
            """;
    }
}
