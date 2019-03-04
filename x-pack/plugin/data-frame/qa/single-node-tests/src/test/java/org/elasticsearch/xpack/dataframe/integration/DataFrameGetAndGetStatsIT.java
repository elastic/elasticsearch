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
import java.util.Map;

public class DataFrameGetAndGetStatsIT extends DataFrameRestTestCase {

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
    }

    public void testGetAndGetStats() throws Exception {
        createPivotReviewsTransform("pivot_1", "pivot_reviews_1", null);
        createPivotReviewsTransform("pivot_2", "pivot_reviews_2", null);

        startAndWaitForTransform("pivot_1", "pivot_reviews_1");
        startAndWaitForTransform("pivot_2", "pivot_reviews_2");

        // check all the different ways to retrieve all stats
        Map<String, Object> stats = entityAsMap(client().performRequest(new Request("GET", DATAFRAME_ENDPOINT + "_stats")));
        assertEquals(2, XContentMapValues.extractValue("count", stats));
        stats = entityAsMap(client().performRequest(new Request("GET", DATAFRAME_ENDPOINT + "_all/_stats")));
        assertEquals(2, XContentMapValues.extractValue("count", stats));
        stats = entityAsMap(client().performRequest(new Request("GET", DATAFRAME_ENDPOINT + "*/_stats")));
        assertEquals(2, XContentMapValues.extractValue("count", stats));

        // only pivot_1
        stats = entityAsMap(client().performRequest(new Request("GET", DATAFRAME_ENDPOINT + "pivot_1/_stats")));
        assertEquals(1, XContentMapValues.extractValue("count", stats));

        // check all the different ways to retrieve all transforms
        Map<String, Object> transforms = entityAsMap(client().performRequest(new Request("GET", DATAFRAME_ENDPOINT)));
        assertEquals(2, XContentMapValues.extractValue("count", transforms));
        transforms = entityAsMap(client().performRequest(new Request("GET", DATAFRAME_ENDPOINT + "_all")));
        assertEquals(2, XContentMapValues.extractValue("count", transforms));
        transforms = entityAsMap(client().performRequest(new Request("GET", DATAFRAME_ENDPOINT + "*")));
        assertEquals(2, XContentMapValues.extractValue("count", transforms));

        // only pivot_1
        transforms = entityAsMap(client().performRequest(new Request("GET", DATAFRAME_ENDPOINT + "pivot_1")));
        assertEquals(1, XContentMapValues.extractValue("count", transforms));
    }


}
