/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.integration;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;

public class DataFramePivotRestIT extends DataFrameRestTestCase {

    private boolean indicesCreated = false;

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

    public void testSimplePivot() throws Exception {
        String transformId = "simplePivot";
        String dataFrameIndex = "pivot_reviews";

        createPivotReviewsTransform(transformId, dataFrameIndex);

        // start the transform
        final Request startTransformRequest = new Request("POST", DATAFRAME_ENDPOINT + transformId + "/_start");
        Map<String, Object> startTransformResponse = entityAsMap(client().performRequest(startTransformRequest));
        assertThat(startTransformResponse.get("started"), equalTo(Boolean.TRUE));

        // wait until the dataframe has been created and all data is available
        waitForDataFrameGeneration(transformId);
        refreshIndex(dataFrameIndex);

        // we expect 27 documents as there shall be 27 user_id's
        Map<String, Object> indexStats = getAsMap(dataFrameIndex + "/_stats");
        assertEquals(27, XContentMapValues.extractValue("_all.total.docs.count", indexStats));

        // get and check some users
        assertOnePivotValue(dataFrameIndex + "/_search?q=reviewer:user_0", 3.776978417);
        assertOnePivotValue(dataFrameIndex + "/_search?q=reviewer:user_5", 3.72);
        assertOnePivotValue(dataFrameIndex + "/_search?q=reviewer:user_11", 3.846153846);
        assertOnePivotValue(dataFrameIndex + "/_search?q=reviewer:user_20", 3.769230769);
        assertOnePivotValue(dataFrameIndex + "/_search?q=reviewer:user_26", 3.918918918);
    }

    private void waitForDataFrameGeneration(String transformId) throws Exception {
        assertBusy(() -> {
            long generation = getDataFrameGeneration(transformId);
            assertEquals(1, generation);
        }, 30, TimeUnit.SECONDS);
    }

    private static int getDataFrameGeneration(String transformId) throws IOException {
        Response statsResponse = client().performRequest(new Request("GET", DATAFRAME_ENDPOINT + transformId + "/_stats"));

        Map<?, ?> transformStatsAsMap = (Map<?, ?>) ((List<?>) entityAsMap(statsResponse).get("transforms")).get(0);
        return (int) XContentMapValues.extractValue("state.generation", transformStatsAsMap);
    }

    private void refreshIndex(String index) throws IOException {
        assertOK(client().performRequest(new Request("POST", index + "/_refresh")));
    }

    private void assertOnePivotValue(String query, double expected) throws IOException {
        Map<String, Object> searchResult = getAsMap(query);

        assertEquals(1, XContentMapValues.extractValue("hits.total.value", searchResult));
        double actual = (double) ((List<?>) XContentMapValues.extractValue("hits.hits._source.avg_rating", searchResult)).get(0);
        assertEquals(expected, actual, 0.000001);
    }
}
