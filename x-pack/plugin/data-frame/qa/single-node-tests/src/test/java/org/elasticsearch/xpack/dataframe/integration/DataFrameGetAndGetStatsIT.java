/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.integration;

import org.elasticsearch.client.Request;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;

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
}
