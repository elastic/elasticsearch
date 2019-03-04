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
    }

    public void testGetAndGetStats() throws Exception {
        createPivotReviewsTransform("pivot_1", "pivot_reviews_1", null);
        createPivotReviewsTransform("pivot_2", "pivot_reviews_2", null);

        startAndWaitForTransform("pivot_1", "pivot_reviews_1");
        startAndWaitForTransform("pivot_2", "pivot_reviews_2");

        // check all the different ways to retrieve all stats
        Request getRequest = new Request("GET", DATAFRAME_ENDPOINT + "_stats");
        addAuthHeaderToRequest(getRequest, BASIC_AUTH_VALUE_DATA_FRAME_USER);
        Map<String, Object> stats = entityAsMap(client().performRequest(getRequest));
        assertEquals(2, XContentMapValues.extractValue("count", stats));
        getRequest = new Request("GET", DATAFRAME_ENDPOINT + "_all/_stats");
        addAuthHeaderToRequest(getRequest, BASIC_AUTH_VALUE_DATA_FRAME_USER);
        stats = entityAsMap(client().performRequest(getRequest));
        assertEquals(2, XContentMapValues.extractValue("count", stats));
        getRequest = new Request("GET", DATAFRAME_ENDPOINT + "*/_stats");
        addAuthHeaderToRequest(getRequest, BASIC_AUTH_VALUE_DATA_FRAME_USER);
        stats = entityAsMap(client().performRequest(getRequest));
        assertEquals(2, XContentMapValues.extractValue("count", stats));

        // only pivot_1
        getRequest = new Request("GET", DATAFRAME_ENDPOINT + "pivot_1/_stats");
        addAuthHeaderToRequest(getRequest, BASIC_AUTH_VALUE_DATA_FRAME_USER);
        stats = entityAsMap(client().performRequest(getRequest));
        assertEquals(1, XContentMapValues.extractValue("count", stats));

        // check all the different ways to retrieve all transforms
        getRequest = new Request("GET", DATAFRAME_ENDPOINT);
        addAuthHeaderToRequest(getRequest, BASIC_AUTH_VALUE_DATA_FRAME_USER);
        Map<String, Object> transforms = entityAsMap(client().performRequest(getRequest));
        assertEquals(2, XContentMapValues.extractValue("count", transforms));
        getRequest = new Request("GET", DATAFRAME_ENDPOINT + "_all");
        addAuthHeaderToRequest(getRequest, BASIC_AUTH_VALUE_DATA_FRAME_USER);
        transforms = entityAsMap(client().performRequest(getRequest));
        assertEquals(2, XContentMapValues.extractValue("count", transforms));
        getRequest = new Request("GET", DATAFRAME_ENDPOINT + "*");
        addAuthHeaderToRequest(getRequest, BASIC_AUTH_VALUE_DATA_FRAME_USER);
        transforms = entityAsMap(client().performRequest(getRequest));
        assertEquals(2, XContentMapValues.extractValue("count", transforms));

        // only pivot_1
        getRequest = new Request("GET", DATAFRAME_ENDPOINT + "pivot_1");
        addAuthHeaderToRequest(getRequest, BASIC_AUTH_VALUE_DATA_FRAME_USER);
        transforms = entityAsMap(client().performRequest(getRequest));
        assertEquals(1, XContentMapValues.extractValue("count", transforms));
    }
}
