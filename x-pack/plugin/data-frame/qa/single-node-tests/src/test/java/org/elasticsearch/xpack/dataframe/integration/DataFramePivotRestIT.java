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

    public void testSimplePivot() throws Exception {
        String transformId = "simplePivot";
        String dataFrameIndex = "pivot_reviews";

        createPivotReviewsTransform(transformId, dataFrameIndex, null);

        startAndWaitForTransform(transformId, dataFrameIndex);

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

    public void testSimplePivotWithQuery() throws Exception {
        String transformId = "simplePivotWithQuery";
        String dataFrameIndex = "pivot_reviews_user_id_above_20";
        String query = "\"match\": {\"user_id\": \"user_26\"}";

        createPivotReviewsTransform(transformId, dataFrameIndex, query);

        startAndWaitForTransform(transformId, dataFrameIndex);

        // we expect only 1 document due to the query
        Map<String, Object> indexStats = getAsMap(dataFrameIndex + "/_stats");
        assertEquals(1, XContentMapValues.extractValue("_all.total.docs.count", indexStats));
        assertOnePivotValue(dataFrameIndex + "/_search?q=reviewer:user_26", 3.918918918);
    }

    public void testHistogramPivot() throws Exception {
        String transformId = "simpleHistogramPivot";
        String dataFrameIndex = "pivot_reviews_via_histogram";

        final Request createDataframeTransformRequest = new Request("PUT", DATAFRAME_ENDPOINT + transformId);

        String config = "{"
            + " \"source\": \"reviews\","
            + " \"dest\": \"" + dataFrameIndex + "\",";


        config += " \"pivot\": {"
            + "   \"group_by\": [ {"
            + "     \"every_2\": {"
            + "       \"histogram\": {"
            + "         \"interval\": 2,\"field\":\"stars\""
            + " } } } ],"
            + "   \"aggregations\": {"
            + "     \"avg_rating\": {"
            + "       \"avg\": {"
            + "         \"field\": \"stars\""
            + " } } } }"
            + "}";


        createDataframeTransformRequest.setJsonEntity(config);
        Map<String, Object> createDataframeTransformResponse = entityAsMap(client().performRequest(createDataframeTransformRequest));
        assertThat(createDataframeTransformResponse.get("acknowledged"), equalTo(Boolean.TRUE));
        assertTrue(indexExists(dataFrameIndex));

        startAndWaitForTransform(transformId, dataFrameIndex);

        // we expect 3 documents as there shall be 5 unique star values and we are bucketing every 2 starting at 0
        Map<String, Object> indexStats = getAsMap(dataFrameIndex + "/_stats");
        assertEquals(3, XContentMapValues.extractValue("_all.total.docs.count", indexStats));
        assertOnePivotValue(dataFrameIndex + "/_search?q=every_2:0.0", 1.0);
    }

    public void testBiggerPivot() throws Exception {
        String transformId = "biggerPivot";
        String dataFrameIndex = "bigger_pivot_reviews";

        final Request createDataframeTransformRequest = new Request("PUT", DATAFRAME_ENDPOINT + transformId);

        String config = "{"
                + " \"source\": \"reviews\","
                + " \"dest\": \"" + dataFrameIndex + "\",";


        config += " \"pivot\": {"
                + "   \"group_by\": [ {"
                + "     \"reviewer\": {"
                + "       \"terms\": {"
                + "         \"field\": \"user_id\""
                + " } } } ],"
                + "   \"aggregations\": {"
                + "     \"avg_rating\": {"
                + "       \"avg\": {"
                + "         \"field\": \"stars\""
                + " } },"
                + "     \"sum_rating\": {"
                + "       \"sum\": {"
                + "         \"field\": \"stars\""
                + " } },"
                + "     \"cardinality_business\": {"
                + "       \"cardinality\": {"
                + "         \"field\": \"business_id\""
                + " } },"
                + "     \"min_rating\": {"
                + "       \"min\": {"
                + "         \"field\": \"stars\""
                + " } },"
                + "     \"max_rating\": {"
                + "       \"max\": {"
                + "         \"field\": \"stars\""
                + " } },"
                + "     \"count\": {"
                + "       \"value_count\": {"
                + "         \"field\": \"business_id\""
                + " } }"
                + " } }"
                + "}";

        createDataframeTransformRequest.setJsonEntity(config);
        Map<String, Object> createDataframeTransformResponse = entityAsMap(client().performRequest(createDataframeTransformRequest));
        assertThat(createDataframeTransformResponse.get("acknowledged"), equalTo(Boolean.TRUE));
        assertTrue(indexExists(dataFrameIndex));

        startAndWaitForTransform(transformId, dataFrameIndex);

        // we expect 27 documents as there shall be 27 user_id's
        Map<String, Object> indexStats = getAsMap(dataFrameIndex + "/_stats");
        assertEquals(27, XContentMapValues.extractValue("_all.total.docs.count", indexStats));

        // get and check some users
        Map<String, Object> searchResult = getAsMap(dataFrameIndex + "/_search?q=reviewer:user_4");

        assertEquals(1, XContentMapValues.extractValue("hits.total.value", searchResult));
        Number actual = (Number) ((List<?>) XContentMapValues.extractValue("hits.hits._source.avg_rating", searchResult)).get(0);
        assertEquals(3.878048780, actual.doubleValue(), 0.000001);
        actual = (Number) ((List<?>) XContentMapValues.extractValue("hits.hits._source.sum_rating", searchResult)).get(0);
        assertEquals(159, actual.longValue());
        actual = (Number) ((List<?>) XContentMapValues.extractValue("hits.hits._source.cardinality_business", searchResult)).get(0);
        assertEquals(6, actual.longValue());
        actual = (Number) ((List<?>) XContentMapValues.extractValue("hits.hits._source.min_rating", searchResult)).get(0);
        assertEquals(1, actual.longValue());
        actual = (Number) ((List<?>) XContentMapValues.extractValue("hits.hits._source.max_rating", searchResult)).get(0);
        assertEquals(5, actual.longValue());
        actual = (Number) ((List<?>) XContentMapValues.extractValue("hits.hits._source.count", searchResult)).get(0);
        assertEquals(41, actual.longValue());
    }

    public void testDateHistogramPivot() throws Exception {
        String transformId = "simpleDateHistogramPivot";
        String dataFrameIndex = "pivot_reviews_via_date_histogram";

        final Request createDataframeTransformRequest = new Request("PUT", DATAFRAME_ENDPOINT + transformId);

        String config = "{"
            + " \"source\": \"reviews\","
            + " \"dest\": \"" + dataFrameIndex + "\",";


        config += " \"pivot\": {"
            + "   \"group_by\": [ {"
            + "     \"by_day\": {"
            + "       \"date_histogram\": {"
            + "         \"interval\": \"1d\",\"field\":\"timestamp\",\"format\":\"yyyy-MM-DD\""
            + " } } } ],"
            + "   \"aggregations\": {"
            + "     \"avg_rating\": {"
            + "       \"avg\": {"
            + "         \"field\": \"stars\""
            + " } } } }"
            + "}";

        createDataframeTransformRequest.setJsonEntity(config);
        Map<String, Object> createDataframeTransformResponse = entityAsMap(client().performRequest(createDataframeTransformRequest));
        assertThat(createDataframeTransformResponse.get("acknowledged"), equalTo(Boolean.TRUE));
        assertTrue(indexExists(dataFrameIndex));

        startAndWaitForTransform(transformId, dataFrameIndex);

        // we expect 17 documents as there shall be 21 days worth of docs
        Map<String, Object> indexStats = getAsMap(dataFrameIndex + "/_stats");
        assertEquals(21, XContentMapValues.extractValue("_all.total.docs.count", indexStats));
        assertOnePivotValue(dataFrameIndex + "/_search?q=by_day:2017-01-15", 3.82);
    }

    private void startAndWaitForTransform(String transformId, String dataFrameIndex) throws IOException, Exception {
        // start the transform
        final Request startTransformRequest = new Request("POST", DATAFRAME_ENDPOINT + transformId + "/_start");
        Map<String, Object> startTransformResponse = entityAsMap(client().performRequest(startTransformRequest));
        assertThat(startTransformResponse.get("started"), equalTo(Boolean.TRUE));

        // wait until the dataframe has been created and all data is available
        waitForDataFrameGeneration(transformId);
        refreshIndex(dataFrameIndex);
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
