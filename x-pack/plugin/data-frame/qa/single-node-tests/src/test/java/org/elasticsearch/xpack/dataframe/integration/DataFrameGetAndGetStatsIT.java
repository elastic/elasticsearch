/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.integration;

import org.elasticsearch.client.Request;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.xpack.core.dataframe.DataFrameField;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.greaterThan;

public class DataFrameGetAndGetStatsIT extends DataFrameRestTestCase {

    private static final String TEST_USER_NAME = "df_user";
    private static final String BASIC_AUTH_VALUE_DATA_FRAME_USER =
        basicAuthHeaderValue(TEST_USER_NAME, TEST_PASSWORD_SECURE_STRING);
    private static final String TEST_ADMIN_USER_NAME = "df_admin";
    private static final String BASIC_AUTH_VALUE_DATA_FRAME_ADMIN =
        basicAuthHeaderValue(TEST_ADMIN_USER_NAME, TEST_PASSWORD_SECURE_STRING);

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
        setupUser(TEST_USER_NAME, Collections.singletonList("data_frame_transforms_user"));
        setupUser(TEST_ADMIN_USER_NAME, Collections.singletonList("data_frame_transforms_admin"));
    }

    @After
    public void clearOutTransforms() throws Exception {
        wipeDataFrameTransforms();
    }

    public void testGetAndGetStats() throws Exception {
        createPivotReviewsTransform("pivot_1", "pivot_reviews_1", null);
        createPivotReviewsTransform("pivot_2", "pivot_reviews_2", null);

        startAndWaitForTransform("pivot_1", "pivot_reviews_1");
        startAndWaitForTransform("pivot_2", "pivot_reviews_2");

        // Alternate testing between admin and lowly user, as both should be able to get the configs and stats
        String authHeader = randomFrom(BASIC_AUTH_VALUE_DATA_FRAME_USER, BASIC_AUTH_VALUE_DATA_FRAME_ADMIN);

        // check all the different ways to retrieve all stats
        Request getRequest = createRequestWithAuth("GET", DATAFRAME_ENDPOINT + "_stats", authHeader);
        Map<String, Object> stats = entityAsMap(client().performRequest(getRequest));
        assertEquals(2, XContentMapValues.extractValue("count", stats));
        getRequest = createRequestWithAuth("GET", DATAFRAME_ENDPOINT + "_all/_stats", authHeader);
        stats = entityAsMap(client().performRequest(getRequest));
        assertEquals(2, XContentMapValues.extractValue("count", stats));
        getRequest = createRequestWithAuth("GET", DATAFRAME_ENDPOINT + "*/_stats", authHeader);
        stats = entityAsMap(client().performRequest(getRequest));
        assertEquals(2, XContentMapValues.extractValue("count", stats));
        getRequest = createRequestWithAuth("GET", DATAFRAME_ENDPOINT + "pivot_1,pivot_2/_stats", authHeader);
        stats = entityAsMap(client().performRequest(getRequest));
        assertEquals(2, XContentMapValues.extractValue("count", stats));
        getRequest = createRequestWithAuth("GET", DATAFRAME_ENDPOINT + "pivot_*/_stats", authHeader);
        stats = entityAsMap(client().performRequest(getRequest));
        assertEquals(2, XContentMapValues.extractValue("count", stats));

        // only pivot_1
        getRequest = createRequestWithAuth("GET", DATAFRAME_ENDPOINT + "pivot_1/_stats", authHeader);
        stats = entityAsMap(client().performRequest(getRequest));
        assertEquals(1, XContentMapValues.extractValue("count", stats));

        // check all the different ways to retrieve all transforms
        getRequest = createRequestWithAuth("GET", DATAFRAME_ENDPOINT, authHeader);
        Map<String, Object> transforms = entityAsMap(client().performRequest(getRequest));
        assertEquals(2, XContentMapValues.extractValue("count", transforms));
        getRequest = createRequestWithAuth("GET", DATAFRAME_ENDPOINT + "_all", authHeader);
        transforms = entityAsMap(client().performRequest(getRequest));
        assertEquals(2, XContentMapValues.extractValue("count", transforms));
        getRequest = createRequestWithAuth("GET", DATAFRAME_ENDPOINT + "*", authHeader);
        transforms = entityAsMap(client().performRequest(getRequest));
        assertEquals(2, XContentMapValues.extractValue("count", transforms));

        // only pivot_1
        getRequest = createRequestWithAuth("GET", DATAFRAME_ENDPOINT + "pivot_1", authHeader);
        transforms = entityAsMap(client().performRequest(getRequest));
        assertEquals(1, XContentMapValues.extractValue("count", transforms));
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/40963")
    @SuppressWarnings("unchecked")
    public void testGetPersistedStatsWithoutTask() throws Exception {
        createPivotReviewsTransform("pivot_stats_1", "pivot_reviews_stats_1", null);
        startAndWaitForTransform("pivot_stats_1", "pivot_reviews_stats_1");
        stopDataFrameTransform("pivot_stats_1", false);

        // Get rid of the first transform task, but keep the configuration
        client().performRequest(new Request("POST", "_tasks/_cancel?actions="+DataFrameField.TASK_NAME+"*"));

        // Verify that the task is gone
        Map<String, Object> tasks =
            entityAsMap(client().performRequest(new Request("GET", "_tasks?actions="+DataFrameField.TASK_NAME+"*")));
        assertTrue(((Map<?, ?>)XContentMapValues.extractValue("nodes", tasks)).isEmpty());

        createPivotReviewsTransform("pivot_stats_2", "pivot_reviews_stats_2", null);
        startAndWaitForTransform("pivot_stats_2", "pivot_reviews_stats_2");

        Request getRequest = createRequestWithAuth("GET", DATAFRAME_ENDPOINT + "_stats", BASIC_AUTH_VALUE_DATA_FRAME_ADMIN);
        Map<String, Object> stats = entityAsMap(client().performRequest(getRequest));
        assertEquals(2, XContentMapValues.extractValue("count", stats));
        List<Map<String, Object>> transformsStats = (List<Map<String, Object>>)XContentMapValues.extractValue("transforms", stats);
        // Verify that both transforms, the one with the task and the one without have statistics
        for (Map<String, Object> transformStats : transformsStats) {
            Map<String, Object> stat = (Map<String, Object>)transformStats.get("stats");
            assertThat(((Integer)stat.get("documents_processed")), greaterThan(0));
            assertThat(((Integer)stat.get("search_time_in_ms")), greaterThan(0));
            assertThat(((Integer)stat.get("search_total")), greaterThan(0));
            assertThat(((Integer)stat.get("pages_processed")), greaterThan(0));
        }
    }
}
