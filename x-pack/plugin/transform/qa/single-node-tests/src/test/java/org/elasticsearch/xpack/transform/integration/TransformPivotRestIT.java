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
import org.junit.Before;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class TransformPivotRestIT extends TransformRestTestCase {

    private static final String TEST_USER_NAME = "transform_admin_plus_data";
    private static final String DATA_ACCESS_ROLE = "test_data_access";
    private static final String BASIC_AUTH_VALUE_TRANSFORM_ADMIN_WITH_SOME_DATA_ACCESS = basicAuthHeaderValue(
        TEST_USER_NAME,
        TEST_PASSWORD_SECURE_STRING
    );

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
        createReviewsIndexNano();
        indicesCreated = true;
        setupDataAccessRole(DATA_ACCESS_ROLE, REVIEWS_INDEX_NAME);

        // at random test the old deprecated roles, to be removed in 9.0.0
        if (randomBoolean()) {
            setupUser(TEST_USER_NAME, Arrays.asList("transform_admin", DATA_ACCESS_ROLE));
        } else {
            setupUser(TEST_USER_NAME, Arrays.asList("data_frame_transforms_admin", DATA_ACCESS_ROLE));
        }
    }

    public void testSimplePivot() throws Exception {
        String transformId = "simple-pivot";
        String transformIndex = "pivot_reviews";
        setupDataAccessRole(DATA_ACCESS_ROLE, REVIEWS_INDEX_NAME, transformIndex);

        createPivotReviewsTransform(transformId, transformIndex, null, null, BASIC_AUTH_VALUE_TRANSFORM_ADMIN_WITH_SOME_DATA_ACCESS);

        startAndWaitForTransform(transformId, transformIndex, BASIC_AUTH_VALUE_TRANSFORM_ADMIN_WITH_SOME_DATA_ACCESS);

        // we expect 27 documents as there shall be 27 user_id's
        Map<String, Object> indexStats = getAsMap(transformIndex + "/_stats");
        assertEquals(27, XContentMapValues.extractValue("_all.total.docs.count", indexStats));

        // get and check some users
        assertOnePivotValue(transformIndex + "/_search?q=reviewer:user_0", 3.776978417);
        assertOnePivotValue(transformIndex + "/_search?q=reviewer:user_5", 3.72);
        assertOnePivotValue(transformIndex + "/_search?q=reviewer:user_11", 3.846153846);
        assertOnePivotValue(transformIndex + "/_search?q=reviewer:user_20", 3.769230769);
        assertOnePivotValue(transformIndex + "/_search?q=reviewer:user_26", 3.918918918);
    }

    public void testSimplePivotWithQuery() throws Exception {
        String transformId = "simple_pivot_with_query";
        String transformIndex = "pivot_reviews_user_id_above_20";
        setupDataAccessRole(DATA_ACCESS_ROLE, REVIEWS_INDEX_NAME, transformIndex);
        String query = "\"match\": {\"user_id\": \"user_26\"}";

        createPivotReviewsTransform(transformId, transformIndex, query, null, BASIC_AUTH_VALUE_TRANSFORM_ADMIN_WITH_SOME_DATA_ACCESS);

        startAndWaitForTransform(transformId, transformIndex, BASIC_AUTH_VALUE_TRANSFORM_ADMIN_WITH_SOME_DATA_ACCESS);

        // we expect only 1 document due to the query
        Map<String, Object> indexStats = getAsMap(transformIndex + "/_stats");
        assertEquals(1, XContentMapValues.extractValue("_all.total.docs.count", indexStats));
        assertOnePivotValue(transformIndex + "/_search?q=reviewer:user_26", 3.918918918);
    }

    public void testSimplePivotWithScript() throws Exception {
        String transformId = "simple-pivot-script";
        String transformIndex = "pivot_reviews_script";
        setupDataAccessRole(DATA_ACCESS_ROLE, REVIEWS_INDEX_NAME, transformIndex);

        final Request createTransformRequest = createRequestWithAuth(
            "PUT",
            getTransformEndpoint() + transformId,
            BASIC_AUTH_VALUE_TRANSFORM_ADMIN_WITH_SOME_DATA_ACCESS
        );

        // same pivot as testSimplePivot, but we retrieve the grouping key using a script and add prefix
        String config = "{"
            + " \"dest\": {\"index\":\""
            + transformIndex
            + "\"},"
            + " \"source\": {\"index\":\""
            + REVIEWS_INDEX_NAME
            + "\"},"
            + " \"pivot\": {"
            + "   \"group_by\": {"
            + "     \"reviewer\": {"
            + "       \"terms\": {"
            + "         \"script\": {"
            + "           \"source\": \"'reviewer_' + doc['user_id'].value\""
            + " } } } },"
            + "   \"aggregations\": {"
            + "     \"avg_rating\": {"
            + "       \"avg\": {"
            + "         \"field\": \"stars\""
            + " } } } },"
            + "\"frequency\":\"1s\""
            + "}";
        createTransformRequest.setJsonEntity(config);

        Map<String, Object> createTransformResponse = entityAsMap(client().performRequest(createTransformRequest));
        assertThat(createTransformResponse.get("acknowledged"), equalTo(Boolean.TRUE));

        startAndWaitForTransform(transformId, transformIndex, BASIC_AUTH_VALUE_TRANSFORM_ADMIN_WITH_SOME_DATA_ACCESS);

        // we expect 27 documents as there shall be 27 user_id's
        Map<String, Object> indexStats = getAsMap(transformIndex + "/_stats");
        assertEquals(27, XContentMapValues.extractValue("_all.total.docs.count", indexStats));

        // get and check some users
        assertOnePivotValue(transformIndex + "/_search?q=reviewer:reviewer_user_0", 3.776978417);
        assertOnePivotValue(transformIndex + "/_search?q=reviewer:reviewer_user_5", 3.72);
        assertOnePivotValue(transformIndex + "/_search?q=reviewer:reviewer_user_11", 3.846153846);
        assertOnePivotValue(transformIndex + "/_search?q=reviewer:reviewer_user_20", 3.769230769);
        assertOnePivotValue(transformIndex + "/_search?q=reviewer:reviewer_user_26", 3.918918918);
    }

    public void testPivotWithPipeline() throws Exception {
        String transformId = "simple_pivot_with_pipeline";
        String transformIndex = "pivot_with_pipeline";
        String pipelineId = "my-pivot-pipeline";
        int pipelineValue = 42;
        Request pipelineRequest = new Request("PUT", "/_ingest/pipeline/" + pipelineId);
        pipelineRequest.setJsonEntity(
            "{\n"
                + "  \"description\" : \"my pivot pipeline\",\n"
                + "  \"processors\" : [\n"
                + "    {\n"
                + "      \"set\" : {\n"
                + "        \"field\": \"pipeline_field\",\n"
                + "        \"value\": "
                + pipelineValue
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}"
        );
        client().performRequest(pipelineRequest);

        setupDataAccessRole(DATA_ACCESS_ROLE, REVIEWS_INDEX_NAME, transformIndex);
        createPivotReviewsTransform(transformId, transformIndex, null, pipelineId, BASIC_AUTH_VALUE_TRANSFORM_ADMIN_WITH_SOME_DATA_ACCESS);

        startAndWaitForTransform(transformId, transformIndex, BASIC_AUTH_VALUE_TRANSFORM_ADMIN_WITH_SOME_DATA_ACCESS);

        // we expect 27 documents as there shall be 27 user_id's
        Map<String, Object> indexStats = getAsMap(transformIndex + "/_stats");
        assertEquals(27, XContentMapValues.extractValue("_all.total.docs.count", indexStats));

        // get and check some users
        assertOnePivotValue(transformIndex + "/_search?q=reviewer:user_0", 3.776978417);
        assertOnePivotValue(transformIndex + "/_search?q=reviewer:user_5", 3.72);
        assertOnePivotValue(transformIndex + "/_search?q=reviewer:user_11", 3.846153846);
        assertOnePivotValue(transformIndex + "/_search?q=reviewer:user_20", 3.769230769);
        assertOnePivotValue(transformIndex + "/_search?q=reviewer:user_26", 3.918918918);

        Map<String, Object> searchResult = getAsMap(transformIndex + "/_search?q=reviewer:user_0");
        Integer actual = (Integer) ((List<?>) XContentMapValues.extractValue("hits.hits._source.pipeline_field", searchResult)).get(0);
        assertThat(actual, equalTo(pipelineValue));
    }

    public void testBucketSelectorPivot() throws Exception {
        String transformId = "simple_bucket_selector_pivot";
        String transformIndex = "bucket_selector_idx";
        setupDataAccessRole(DATA_ACCESS_ROLE, REVIEWS_INDEX_NAME, transformIndex);
        final Request createTransformRequest = createRequestWithAuth(
            "PUT",
            getTransformEndpoint() + transformId,
            BASIC_AUTH_VALUE_TRANSFORM_ADMIN_WITH_SOME_DATA_ACCESS
        );
        String config = "{"
            + " \"source\": {\"index\":\""
            + REVIEWS_INDEX_NAME
            + "\"},"
            + " \"dest\": {\"index\":\""
            + transformIndex
            + "\"},"
            + " \"frequency\": \"1s\","
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
            + "    } },"
            + "     \"over_38\": {"
            + "         \"bucket_selector\" : {"
            + "            \"buckets_path\": {\"rating\":\"avg_rating\"}, "
            + "            \"script\": \"params.rating > 3.8\""
            + "         }"
            + "      } } }"
            + "}";
        createTransformRequest.setJsonEntity(config);
        Map<String, Object> createTransformResponse = entityAsMap(client().performRequest(createTransformRequest));
        assertThat(createTransformResponse.get("acknowledged"), equalTo(Boolean.TRUE));

        startAndWaitForTransform(transformId, transformIndex);
        assertTrue(indexExists(transformIndex));
        // get and check some users
        assertOnePivotValue(transformIndex + "/_search?q=reviewer:user_11", 3.846153846);
        assertOnePivotValue(transformIndex + "/_search?q=reviewer:user_26", 3.918918918);

        Map<String, Object> indexStats = getAsMap(transformIndex + "/_stats");
        // Should be less than the total number of users since we filtered every user who had an average review less than or equal to 3.8
        assertEquals(21, XContentMapValues.extractValue("_all.total.docs.count", indexStats));
    }

    public void testContinuousPivot() throws Exception {
        String indexName = "continuous_reviews";
        createReviewsIndex(indexName);
        String transformId = "simple_continuous_pivot";
        String transformIndex = "pivot_reviews_continuous";
        setupDataAccessRole(DATA_ACCESS_ROLE, indexName, transformIndex);
        final Request createTransformRequest = createRequestWithAuth(
            "PUT",
            getTransformEndpoint() + transformId,
            BASIC_AUTH_VALUE_TRANSFORM_ADMIN_WITH_SOME_DATA_ACCESS
        );
        String config = "{"
            + " \"source\": {\"index\":\""
            + indexName
            + "\"},"
            + " \"dest\": {\"index\":\""
            + transformIndex
            + "\"},"
            + " \"frequency\": \"1s\","
            + " \"sync\": {\"time\": {\"field\": \"timestamp\", \"delay\": \"1s\"}},"
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

        startAndWaitForContinuousTransform(transformId, transformIndex, null);
        assertTrue(indexExists(transformIndex));
        // get and check some users
        assertOnePivotValue(transformIndex + "/_search?q=reviewer:user_0", 3.776978417);
        assertOnePivotValue(transformIndex + "/_search?q=reviewer:user_5", 3.72);
        assertOnePivotValue(transformIndex + "/_search?q=reviewer:user_11", 3.846153846);
        assertOnePivotValue(transformIndex + "/_search?q=reviewer:user_20", 3.769230769);
        assertOnePivotValue(transformIndex + "/_search?q=reviewer:user_26", 3.918918918);

        Map<String, Object> indexStats = getAsMap(transformIndex + "/_stats");
        assertEquals(27, XContentMapValues.extractValue("_all.total.docs.count", indexStats));
        final StringBuilder bulk = new StringBuilder();
        long user = 42;
        long user26 = 26;

        long dateStamp = Instant.now().toEpochMilli() - 1_000;
        for (int i = 0; i < 25; i++) {
            bulk.append("{\"index\":{\"_index\":\"" + indexName + "\"}}\n");
            int stars = (i * 32) % 5;
            long business = (stars * user) % 13;
            String location = (user + 10) + "," + (user + 15);

            bulk.append("{\"user_id\":\"")
                .append("user_")
                .append(user)
                .append("\",\"business_id\":\"")
                .append("business_")
                .append(business)
                .append("\",\"stars\":")
                .append(stars)
                .append(",\"location\":\"")
                .append(location)
                .append("\",\"timestamp\":")
                .append(dateStamp)
                .append("}\n");

            stars = 5;
            business = 11;
            bulk.append("{\"index\":{\"_index\":\"" + indexName + "\"}}\n");
            bulk.append("{\"user_id\":\"")
                .append("user_")
                .append(user26)
                .append("\",\"business_id\":\"")
                .append("business_")
                .append(business)
                .append("\",\"stars\":")
                .append(stars)
                .append(",\"location\":\"")
                .append(location)
                .append("\",\"timestamp\":")
                .append(dateStamp)
                .append("}\n");
        }
        bulk.append("\r\n");

        final Request bulkRequest = new Request("POST", "/_bulk");
        bulkRequest.addParameter("refresh", "true");
        bulkRequest.setJsonEntity(bulk.toString());
        client().performRequest(bulkRequest);

        waitForTransformCheckpoint(transformId, 2);

        stopTransform(transformId, false);
        refreshIndex(transformIndex);

        // assert that other users are unchanged
        assertOnePivotValue(transformIndex + "/_search?q=reviewer:user_0", 3.776978417);
        assertOnePivotValue(transformIndex + "/_search?q=reviewer:user_5", 3.72);
        assertOnePivotValue(transformIndex + "/_search?q=reviewer:user_11", 3.846153846);
        assertOnePivotValue(transformIndex + "/_search?q=reviewer:user_20", 3.769230769);

        Map<String, Object> user26searchResult = getAsMap(transformIndex + "/_search?q=reviewer:user_26");
        assertEquals(1, XContentMapValues.extractValue("hits.total.value", user26searchResult));
        double actual = (Double) ((List<?>) XContentMapValues.extractValue("hits.hits._source.avg_rating", user26searchResult)).get(0);
        assertThat(actual, greaterThan(3.92));

        Map<String, Object> user42searchResult = getAsMap(transformIndex + "/_search?q=reviewer:user_42");
        assertEquals(1, XContentMapValues.extractValue("hits.total.value", user42searchResult));
        actual = (Double) ((List<?>) XContentMapValues.extractValue("hits.hits._source.avg_rating", user42searchResult)).get(0);
        assertThat(actual, greaterThan(0.0));
        assertThat(actual, lessThan(5.0));
    }

    public void testHistogramPivot() throws Exception {
        String transformId = "simple_histogram_pivot";
        String transformIndex = "pivot_reviews_via_histogram";
        setupDataAccessRole(DATA_ACCESS_ROLE, REVIEWS_INDEX_NAME, transformIndex);

        final Request createTransformRequest = createRequestWithAuth(
            "PUT",
            getTransformEndpoint() + transformId,
            BASIC_AUTH_VALUE_TRANSFORM_ADMIN_WITH_SOME_DATA_ACCESS
        );

        String config = "{"
            + " \"source\": {\"index\":\""
            + REVIEWS_INDEX_NAME
            + "\"},"
            + " \"dest\": {\"index\":\""
            + transformIndex
            + "\"},";

        config += " \"pivot\": {"
            + "   \"group_by\": {"
            + "     \"every_2\": {"
            + "       \"histogram\": {"
            + "         \"interval\": 2,\"field\":\"stars\""
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

        startAndWaitForTransform(transformId, transformIndex);
        assertTrue(indexExists(transformIndex));

        // we expect 3 documents as there shall be 5 unique star values and we are bucketing every 2 starting at 0
        Map<String, Object> indexStats = getAsMap(transformIndex + "/_stats");
        assertEquals(3, XContentMapValues.extractValue("_all.total.docs.count", indexStats));
        assertOnePivotValue(transformIndex + "/_search?q=every_2:0.0", 1.0);
    }

    public void testBiggerPivot() throws Exception {
        String transformId = "bigger_pivot";
        String transformIndex = "bigger_pivot_reviews";
        setupDataAccessRole(DATA_ACCESS_ROLE, REVIEWS_INDEX_NAME, transformIndex);

        final Request createTransformRequest = createRequestWithAuth(
            "PUT",
            getTransformEndpoint() + transformId,
            BASIC_AUTH_VALUE_TRANSFORM_ADMIN_WITH_SOME_DATA_ACCESS
        );

        String config = "{"
            + " \"source\": {\"index\":\""
            + REVIEWS_INDEX_NAME
            + "\"},"
            + " \"dest\": {\"index\":\""
            + transformIndex
            + "\"},";

        config += " \"pivot\": {"
            + "   \"group_by\": {"
            + "     \"reviewer\": {"
            + "       \"terms\": {"
            + "         \"field\": \"user_id\""
            + " } } },"
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

        createTransformRequest.setJsonEntity(config);
        Map<String, Object> createTransformResponse = entityAsMap(client().performRequest(createTransformRequest));
        assertThat(createTransformResponse.get("acknowledged"), equalTo(Boolean.TRUE));

        startAndWaitForTransform(transformId, transformIndex, BASIC_AUTH_VALUE_TRANSFORM_ADMIN_WITH_SOME_DATA_ACCESS);
        assertTrue(indexExists(transformIndex));

        // we expect 27 documents as there shall be 27 user_id's
        Map<String, Object> indexStats = getAsMap(transformIndex + "/_stats");
        assertEquals(27, XContentMapValues.extractValue("_all.total.docs.count", indexStats));

        // get and check some users
        Map<String, Object> searchResult = getAsMap(transformIndex + "/_search?q=reviewer:user_4");

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
        assertDateHistogramPivot(REVIEWS_INDEX_NAME);
    }

    public void testDateHistogramPivotNanos() throws Exception {
        assertDateHistogramPivot(REVIEWS_DATE_NANO_INDEX_NAME);
    }

    @SuppressWarnings("unchecked")
    public void testPivotWithTermsAgg() throws Exception {
        String transformId = "simple_terms_agg_pivot";
        String transformIndex = "pivot_reviews_via_histogram_with_terms_agg";
        setupDataAccessRole(DATA_ACCESS_ROLE, REVIEWS_INDEX_NAME, transformIndex);

        final Request createTransformRequest = createRequestWithAuth(
            "PUT",
            getTransformEndpoint() + transformId,
            BASIC_AUTH_VALUE_TRANSFORM_ADMIN_WITH_SOME_DATA_ACCESS
        );

        String config = "{"
            + " \"source\": {\"index\":\""
            + REVIEWS_INDEX_NAME
            + "\"},"
            + " \"dest\": {\"index\":\""
            + transformIndex
            + "\"},";

        config += " \"pivot\": {"
            + "   \"group_by\": {"
            + "     \"every_2\": {"
            + "       \"histogram\": {"
            + "         \"interval\": 2,\"field\":\"stars\""
            + " } } },"
            + "   \"aggregations\": {"
            + "     \"common_users\": {"
            + "       \"terms\": {"
            + "         \"field\": \"user_id\","
            + "         \"size\": 2"
            + "        },"
            + "        \"aggs\" : {"
            + "          \"common_businesses\": {"
            + "            \"terms\": {"
            + "              \"field\": \"business_id\","
            + "              \"size\": 2"
            + "         }}"
            + "        } "
            +"      },"
            + "     \"rare_users\": {"
            + "       \"rare_terms\": {"
            + "         \"field\": \"user_id\""
            + " } } } }"
            + "}";

        createTransformRequest.setJsonEntity(config);
        Map<String, Object> createTransformResponse = entityAsMap(client().performRequest(createTransformRequest));
        assertThat(createTransformResponse.get("acknowledged"), equalTo(Boolean.TRUE));

        startAndWaitForTransform(transformId, transformIndex);
        assertTrue(indexExists(transformIndex));

        // we expect 3 documents as there shall be 5 unique star values and we are bucketing every 2 starting at 0
        Map<String, Object> indexStats = getAsMap(transformIndex + "/_stats");
        assertEquals(3, XContentMapValues.extractValue("_all.total.docs.count", indexStats));

        // get and check some term results
        Map<String, Object> searchResult = getAsMap(transformIndex + "/_search?q=every_2:2.0");

        assertEquals(1, XContentMapValues.extractValue("hits.total.value", searchResult));
        Map<String, Integer> commonUsers = (Map<String, Integer>) ((List<?>) XContentMapValues.extractValue(
            "hits.hits._source.common_users",
            searchResult
        )).get(0);
        Map<String, Integer> rareUsers = (Map<String, Integer>) ((List<?>) XContentMapValues.extractValue(
            "hits.hits._source.rare_users",
            searchResult
        )).get(0);
        assertThat(commonUsers, is(not(nullValue())));
        assertThat(commonUsers, equalTo(new HashMap<>(){{
            put("user_10",
                Collections.singletonMap(
                    "common_businesses",
                    new HashMap<>(){{
                        put("business_12", 6);
                        put("business_9", 4);
            }}));
            put("user_0", Collections.singletonMap(
                "common_businesses",
                new HashMap<>(){{
                    put("business_0", 35);
            }}));
        }}));
        assertThat(rareUsers, is(not(nullValue())));
        assertThat(rareUsers, equalTo(new HashMap<>(){{
            put("user_5", 1);
            put("user_12", 1);
        }}));
    }

    private void assertDateHistogramPivot(String indexName) throws Exception {
        String transformId = "simple_date_histogram_pivot_" + indexName;
        String transformIndex = "pivot_reviews_via_date_histogram_" + indexName;
        setupDataAccessRole(DATA_ACCESS_ROLE, indexName, transformIndex);

        final Request createTransformRequest = createRequestWithAuth(
            "PUT",
            getTransformEndpoint() + transformId,
            BASIC_AUTH_VALUE_TRANSFORM_ADMIN_WITH_SOME_DATA_ACCESS
        );

        String config = "{" + " \"source\": {\"index\":\"" + indexName + "\"}," + " \"dest\": {\"index\":\"" + transformIndex + "\"},";

        config += " \"pivot\": {"
            + "   \"group_by\": {"
            + "     \"by_hr\": {"
            + "       \"date_histogram\": {"
            + "         \"fixed_interval\": \"1h\",\"field\":\"timestamp\""
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

        startAndWaitForTransform(transformId, transformIndex, BASIC_AUTH_VALUE_TRANSFORM_ADMIN_WITH_SOME_DATA_ACCESS);
        assertTrue(indexExists(transformIndex));

        Map<String, Object> indexStats = getAsMap(transformIndex + "/_stats");
        assertEquals(104, XContentMapValues.extractValue("_all.total.docs.count", indexStats));
        assertOnePivotValue(transformIndex + "/_search?q=by_hr:1484499600000", 4.0833333333);
    }

    @SuppressWarnings("unchecked")
    public void testPreviewTransform() throws Exception {
        setupDataAccessRole(DATA_ACCESS_ROLE, REVIEWS_INDEX_NAME);
        final Request createPreviewRequest = createRequestWithAuth(
            "POST",
            getTransformEndpoint() + "_preview",
            BASIC_AUTH_VALUE_TRANSFORM_ADMIN_WITH_SOME_DATA_ACCESS
        );

        String config = "{" + " \"source\": {\"index\":\"" + REVIEWS_INDEX_NAME + "\"}  ,";

        config += " \"pivot\": {"
            + "   \"group_by\": {"
            + "     \"user.id\": {\"terms\": { \"field\": \"user_id\" }},"
            + "     \"by_day\": {\"date_histogram\": {\"fixed_interval\": \"1d\",\"field\":\"timestamp\"}}},"
            + "   \"aggregations\": {"
            + "     \"user.avg_rating\": {"
            + "       \"avg\": {"
            + "         \"field\": \"stars\""
            + " } } } }"
            + "}";
        createPreviewRequest.setJsonEntity(config);

        Map<String, Object> previewTransformResponse = entityAsMap(client().performRequest(createPreviewRequest));
        List<Map<String, Object>> preview = (List<Map<String, Object>>) previewTransformResponse.get("preview");
        // preview is limited to 100
        assertThat(preview.size(), equalTo(100));
        Set<String> expectedTopLevelFields = new HashSet<>(Arrays.asList("user", "by_day"));
        Set<String> expectedNestedFields = new HashSet<>(Arrays.asList("id", "avg_rating"));
        preview.forEach(p -> {
            Set<String> keys = p.keySet();
            assertThat(keys, equalTo(expectedTopLevelFields));
            Map<String, Object> nestedObj = (Map<String, Object>) p.get("user");
            keys = nestedObj.keySet();
            assertThat(keys, equalTo(expectedNestedFields));
        });
    }

    @SuppressWarnings("unchecked")
    public void testPreviewTransformWithPipeline() throws Exception {
        String pipelineId = "my-preview-pivot-pipeline";
        int pipelineValue = 42;
        Request pipelineRequest = new Request("PUT", "/_ingest/pipeline/" + pipelineId);
        pipelineRequest.setJsonEntity(
            "{\n"
                + "  \"description\" : \"my pivot preview pipeline\",\n"
                + "  \"processors\" : [\n"
                + "    {\n"
                + "      \"set\" : {\n"
                + "        \"field\": \"pipeline_field\",\n"
                + "        \"value\": "
                + pipelineValue
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}"
        );
        client().performRequest(pipelineRequest);

        setupDataAccessRole(DATA_ACCESS_ROLE, REVIEWS_INDEX_NAME);
        final Request createPreviewRequest = createRequestWithAuth("POST", getTransformEndpoint() + "_preview", null);

        String config = "{ \"source\": {\"index\":\""
            + REVIEWS_INDEX_NAME
            + "\"} ,"
            + "\"dest\": {\"pipeline\": \""
            + pipelineId
            + "\"},"
            + " \"pivot\": {"
            + "   \"group_by\": {"
            + "     \"user.id\": {\"terms\": { \"field\": \"user_id\" }},"
            + "     \"by_day\": {\"date_histogram\": {\"fixed_interval\": \"1d\",\"field\":\"timestamp\"}}},"
            + "   \"aggregations\": {"
            + "     \"user.avg_rating\": {"
            + "       \"avg\": {"
            + "         \"field\": \"stars\""
            + " } } } }"
            + "}";
        createPreviewRequest.setJsonEntity(config);

        Map<String, Object> previewTransformResponse = entityAsMap(client().performRequest(createPreviewRequest));
        List<Map<String, Object>> preview = (List<Map<String, Object>>) previewTransformResponse.get("preview");
        // preview is limited to 100
        assertThat(preview.size(), equalTo(100));
        Set<String> expectedTopLevelFields = new HashSet<>(Arrays.asList("user", "by_day", "pipeline_field"));
        Set<String> expectedNestedFields = new HashSet<>(Arrays.asList("id", "avg_rating"));
        preview.forEach(p -> {
            Set<String> keys = p.keySet();
            assertThat(keys, equalTo(expectedTopLevelFields));
            assertThat(p.get("pipeline_field"), equalTo(pipelineValue));
            Map<String, Object> nestedObj = (Map<String, Object>) p.get("user");
            keys = nestedObj.keySet();
            assertThat(keys, equalTo(expectedNestedFields));
        });
    }

    public void testPivotWithMaxOnDateField() throws Exception {
        String transformId = "simple_date_histogram_pivot_with_max_time";
        String transformIndex = "pivot_reviews_via_date_histogram_with_max_time";
        setupDataAccessRole(DATA_ACCESS_ROLE, REVIEWS_INDEX_NAME, transformIndex);

        final Request createTransformRequest = createRequestWithAuth(
            "PUT",
            getTransformEndpoint() + transformId,
            BASIC_AUTH_VALUE_TRANSFORM_ADMIN_WITH_SOME_DATA_ACCESS
        );

        String config = "{"
            + " \"source\": {\"index\": \""
            + REVIEWS_INDEX_NAME
            + "\"},"
            + " \"dest\": {\"index\":\""
            + transformIndex
            + "\"},";

        config += "    \"pivot\": { \n"
            + "        \"group_by\": {\n"
            + "            \"by_day\": {\"date_histogram\": {\n"
            + "                \"fixed_interval\": \"1d\",\"field\":\"timestamp\"\n"
            + "            }}\n"
            + "        },\n"
            + "    \n"
            + "    \"aggs\" :{\n"
            + "        \"avg_rating\": {\n"
            + "            \"avg\": {\"field\": \"stars\"}\n"
            + "        },\n"
            + "        \"timestamp\": {\n"
            + "            \"max\": {\"field\": \"timestamp\"}\n"
            + "        }\n"
            + "    }}"
            + "}";

        createTransformRequest.setJsonEntity(config);

        Map<String, Object> createTransformResponse = entityAsMap(client().performRequest(createTransformRequest));
        assertThat(createTransformResponse.get("acknowledged"), equalTo(Boolean.TRUE));

        startAndWaitForTransform(transformId, transformIndex, BASIC_AUTH_VALUE_TRANSFORM_ADMIN_WITH_SOME_DATA_ACCESS);
        assertTrue(indexExists(transformIndex));

        // we expect 21 documents as there shall be 21 days worth of docs
        Map<String, Object> indexStats = getAsMap(transformIndex + "/_stats");
        assertEquals(21, XContentMapValues.extractValue("_all.total.docs.count", indexStats));
        assertOnePivotValue(transformIndex + "/_search?q=by_day:2017-01-15", 3.82);
        Map<String, Object> searchResult = getAsMap(transformIndex + "/_search?q=by_day:2017-01-15");
        String actual = (String) ((List<?>) XContentMapValues.extractValue("hits.hits._source.timestamp", searchResult)).get(0);
        // Do `containsString` as actual ending timestamp is indeterminate due to how data is generated
        assertThat(actual, containsString("2017-01-15T"));
    }

    public void testPivotWithScriptedMetricAgg() throws Exception {
        String transformId = "scripted_metric_pivot";
        String transformIndex = "scripted_metric_pivot_reviews";
        setupDataAccessRole(DATA_ACCESS_ROLE, REVIEWS_INDEX_NAME, transformIndex);

        final Request createTransformRequest = createRequestWithAuth(
            "PUT",
            getTransformEndpoint() + transformId,
            BASIC_AUTH_VALUE_TRANSFORM_ADMIN_WITH_SOME_DATA_ACCESS
        );

        String config = "{"
            + " \"source\": {\"index\":\""
            + REVIEWS_INDEX_NAME
            + "\"},"
            + " \"dest\": {\"index\":\""
            + transformIndex
            + "\"},";

        config += " \"pivot\": {"
            + "   \"group_by\": {"
            + "     \"reviewer\": {"
            + "       \"terms\": {"
            + "         \"field\": \"user_id\""
            + " } } },"
            + "   \"aggregations\": {"
            + "     \"avg_rating\": {"
            + "       \"avg\": {"
            + "         \"field\": \"stars\""
            + " } },"
            + "     \"squared_sum\": {"
            + "       \"scripted_metric\": {"
            + "         \"init_script\": \"state.reviews_sqrd = []\","
            + "         \"map_script\": \"state.reviews_sqrd.add(doc.stars.value * doc.stars.value)\","
            + "         \"combine_script\": \"state.reviews_sqrd\","
            + "         \"reduce_script\": \"def sum = 0.0; for(l in states){ for(a in l) { sum += a}} return sum\""
            + " } }"
            + " } }"
            + "}";

        createTransformRequest.setJsonEntity(config);
        Map<String, Object> createTransformResponse = entityAsMap(client().performRequest(createTransformRequest));
        assertThat(createTransformResponse.get("acknowledged"), equalTo(Boolean.TRUE));

        startAndWaitForTransform(transformId, transformIndex, BASIC_AUTH_VALUE_TRANSFORM_ADMIN_WITH_SOME_DATA_ACCESS);
        assertTrue(indexExists(transformIndex));

        // we expect 27 documents as there shall be 27 user_id's
        Map<String, Object> indexStats = getAsMap(transformIndex + "/_stats");
        assertEquals(27, XContentMapValues.extractValue("_all.total.docs.count", indexStats));

        // get and check some users
        Map<String, Object> searchResult = getAsMap(transformIndex + "/_search?q=reviewer:user_4");
        assertEquals(1, XContentMapValues.extractValue("hits.total.value", searchResult));
        Number actual = (Number) ((List<?>) XContentMapValues.extractValue("hits.hits._source.avg_rating", searchResult)).get(0);
        assertEquals(3.878048780, actual.doubleValue(), 0.000001);
        actual = (Number) ((List<?>) XContentMapValues.extractValue("hits.hits._source.squared_sum", searchResult)).get(0);
        assertEquals(711.0, actual.doubleValue(), 0.000001);
    }

    public void testPivotWithBucketScriptAgg() throws Exception {
        String transformId = "bucket_script_pivot";
        String transformIndex = "bucket_script_pivot_reviews";
        setupDataAccessRole(DATA_ACCESS_ROLE, REVIEWS_INDEX_NAME, transformIndex);

        final Request createTransformRequest = createRequestWithAuth(
            "PUT",
            getTransformEndpoint() + transformId,
            BASIC_AUTH_VALUE_TRANSFORM_ADMIN_WITH_SOME_DATA_ACCESS
        );

        String config = "{"
            + " \"source\": {\"index\":\""
            + REVIEWS_INDEX_NAME
            + "\"},"
            + " \"dest\": {\"index\":\""
            + transformIndex
            + "\"},";

        config += " \"pivot\": {"
            + "   \"group_by\": {"
            + "     \"reviewer\": {"
            + "       \"terms\": {"
            + "         \"field\": \"user_id\""
            + " } } },"
            + "   \"aggregations\": {"
            + "     \"avg_rating\": {"
            + "       \"avg\": {"
            + "         \"field\": \"stars\""
            + " } },"
            + "     \"avg_rating_again\": {"
            + "       \"bucket_script\": {"
            + "         \"buckets_path\": {\"param_1\": \"avg_rating\"},"
            + "         \"script\": \"return params.param_1\""
            + " } }"
            + " } }"
            + "}";

        createTransformRequest.setJsonEntity(config);
        Map<String, Object> createTransformResponse = entityAsMap(client().performRequest(createTransformRequest));
        assertThat(createTransformResponse.get("acknowledged"), equalTo(Boolean.TRUE));

        startAndWaitForTransform(transformId, transformIndex, BASIC_AUTH_VALUE_TRANSFORM_ADMIN_WITH_SOME_DATA_ACCESS);
        assertTrue(indexExists(transformIndex));

        // we expect 27 documents as there shall be 27 user_id's
        Map<String, Object> indexStats = getAsMap(transformIndex + "/_stats");
        assertEquals(27, XContentMapValues.extractValue("_all.total.docs.count", indexStats));

        // get and check some users
        Map<String, Object> searchResult = getAsMap(transformIndex + "/_search?q=reviewer:user_4");
        assertEquals(1, XContentMapValues.extractValue("hits.total.value", searchResult));
        Number actual = (Number) ((List<?>) XContentMapValues.extractValue("hits.hits._source.avg_rating", searchResult)).get(0);
        assertEquals(3.878048780, actual.doubleValue(), 0.000001);
        actual = (Number) ((List<?>) XContentMapValues.extractValue("hits.hits._source.avg_rating_again", searchResult)).get(0);
        assertEquals(3.878048780, actual.doubleValue(), 0.000001);
    }

    @SuppressWarnings("unchecked")
    public void testPivotWithGeoBoundsAgg() throws Exception {
        String transformId = "geo_bounds_pivot";
        String transformIndex = "geo_bounds_pivot_reviews";
        setupDataAccessRole(DATA_ACCESS_ROLE, REVIEWS_INDEX_NAME, transformIndex);

        final Request createTransformRequest = createRequestWithAuth(
            "PUT",
            getTransformEndpoint() + transformId,
            BASIC_AUTH_VALUE_TRANSFORM_ADMIN_WITH_SOME_DATA_ACCESS
        );

        String config = "{"
            + " \"source\": {\"index\":\""
            + REVIEWS_INDEX_NAME
            + "\"},"
            + " \"dest\": {\"index\":\""
            + transformIndex
            + "\"},";

        config += " \"pivot\": {"
            + "   \"group_by\": {"
            + "     \"reviewer\": {"
            + "       \"terms\": {"
            + "         \"field\": \"user_id\""
            + " } } },"
            + "   \"aggregations\": {"
            + "     \"avg_rating\": {"
            + "       \"avg\": {"
            + "         \"field\": \"stars\""
            + " } },"
            + "     \"boundary\": {"
            + "       \"geo_bounds\": {\"field\": \"location\"}"
            + " } } }"
            + "}";

        createTransformRequest.setJsonEntity(config);
        Map<String, Object> createTransformResponse = entityAsMap(client().performRequest(createTransformRequest));
        assertThat(createTransformResponse.get("acknowledged"), equalTo(Boolean.TRUE));

        startAndWaitForTransform(transformId, transformIndex, BASIC_AUTH_VALUE_TRANSFORM_ADMIN_WITH_SOME_DATA_ACCESS);
        assertTrue(indexExists(transformIndex));

        // we expect 27 documents as there shall be 27 user_id's
        Map<String, Object> indexStats = getAsMap(transformIndex + "/_stats");
        assertEquals(27, XContentMapValues.extractValue("_all.total.docs.count", indexStats));

        // get and check some users
        Map<String, Object> searchResult = getAsMap(transformIndex + "/_search?q=reviewer:user_4");
        assertEquals(1, XContentMapValues.extractValue("hits.total.value", searchResult));
        Number actual = (Number) ((List<?>) XContentMapValues.extractValue("hits.hits._source.avg_rating", searchResult)).get(0);
        assertEquals(3.878048780, actual.doubleValue(), 0.000001);
        Map<String, Object> actualObj = (Map<String, Object>) ((List<?>) XContentMapValues.extractValue(
            "hits.hits._source.boundary",
            searchResult
        )).get(0);
        assertThat(actualObj.get("type"), equalTo("point"));
        List<Double> coordinates = (List<Double>) actualObj.get("coordinates");
        assertEquals((4 + 10), coordinates.get(1), 0.000001);
        assertEquals((4 + 15), coordinates.get(0), 0.000001);
    }

    public void testPivotWithGeoCentroidAgg() throws Exception {
        String transformId = "geo_centroid_pivot";
        String transformIndex = "geo_centroid_pivot_reviews";
        setupDataAccessRole(DATA_ACCESS_ROLE, REVIEWS_INDEX_NAME, transformIndex);

        final Request createTransformRequest = createRequestWithAuth(
            "PUT",
            getTransformEndpoint() + transformId,
            BASIC_AUTH_VALUE_TRANSFORM_ADMIN_WITH_SOME_DATA_ACCESS
        );

        String config = "{"
            + " \"source\": {\"index\":\""
            + REVIEWS_INDEX_NAME
            + "\"},"
            + " \"dest\": {\"index\":\""
            + transformIndex
            + "\"},";

        config += " \"pivot\": {"
            + "   \"group_by\": {"
            + "     \"reviewer\": {"
            + "       \"terms\": {"
            + "         \"field\": \"user_id\""
            + " } } },"
            + "   \"aggregations\": {"
            + "     \"avg_rating\": {"
            + "       \"avg\": {"
            + "         \"field\": \"stars\""
            + " } },"
            + "     \"location\": {"
            + "       \"geo_centroid\": {\"field\": \"location\"}"
            + " } } }"
            + "}";

        createTransformRequest.setJsonEntity(config);
        Map<String, Object> createTransformResponse = entityAsMap(client().performRequest(createTransformRequest));
        assertThat(createTransformResponse.get("acknowledged"), equalTo(Boolean.TRUE));

        startAndWaitForTransform(transformId, transformIndex, BASIC_AUTH_VALUE_TRANSFORM_ADMIN_WITH_SOME_DATA_ACCESS);
        assertTrue(indexExists(transformIndex));

        // we expect 27 documents as there shall be 27 user_id's
        Map<String, Object> indexStats = getAsMap(transformIndex + "/_stats");
        assertEquals(27, XContentMapValues.extractValue("_all.total.docs.count", indexStats));

        // get and check some users
        Map<String, Object> searchResult = getAsMap(transformIndex + "/_search?q=reviewer:user_4");
        assertEquals(1, XContentMapValues.extractValue("hits.total.value", searchResult));
        Number actual = (Number) ((List<?>) XContentMapValues.extractValue("hits.hits._source.avg_rating", searchResult)).get(0);
        assertEquals(3.878048780, actual.doubleValue(), 0.000001);
        String actualString = (String) ((List<?>) XContentMapValues.extractValue("hits.hits._source.location", searchResult)).get(0);
        String[] latlon = actualString.split(",");
        assertEquals((4 + 10), Double.valueOf(latlon[0]), 0.000001);
        assertEquals((4 + 15), Double.valueOf(latlon[1]), 0.000001);
    }

    @SuppressWarnings("unchecked")
    public void testPivotWithGeotileGroupBy() throws Exception {
        String transformId = "geotile_grid_group_by";
        String transformIndex = "geotile_grid_pivot_reviews";
        setupDataAccessRole(DATA_ACCESS_ROLE, REVIEWS_INDEX_NAME, transformIndex);

        final Request createTransformRequest = createRequestWithAuth(
            "PUT",
            getTransformEndpoint() + transformId,
            BASIC_AUTH_VALUE_TRANSFORM_ADMIN_WITH_SOME_DATA_ACCESS
        );

        String config = "{"
            + " \"source\": {\"index\":\""
            + REVIEWS_INDEX_NAME
            + "\"},"
            + " \"dest\": {\"index\":\""
            + transformIndex
            + "\"},";

        config += " \"pivot\": {"
            + "   \"group_by\": {"
            + "     \"tile\": {"
            + "       \"geotile_grid\": {"
            + "         \"field\": \"location\","
            + "         \"precision\": 12"
            + " } } },"
            + "   \"aggregations\": {"
            + "     \"avg_rating\": {"
            + "       \"avg\": {"
            + "         \"field\": \"stars\""
            + " } },"
            + "     \"boundary\": {"
            + "       \"geo_bounds\": {\"field\": \"location\"}"
            + " } } }"
            + "}";

        createTransformRequest.setJsonEntity(config);
        Map<String, Object> createTransformResponse = entityAsMap(client().performRequest(createTransformRequest));
        assertThat(createTransformResponse.get("acknowledged"), equalTo(Boolean.TRUE));

        startAndWaitForTransform(transformId, transformIndex, BASIC_AUTH_VALUE_TRANSFORM_ADMIN_WITH_SOME_DATA_ACCESS);
        assertTrue(indexExists(transformIndex));

        // we expect 27 documents as there are that many tiles at this zoom
        Map<String, Object> indexStats = getAsMap(transformIndex + "/_stats");
        assertEquals(27, XContentMapValues.extractValue("_all.total.docs.count", indexStats));

        // Verify that the format is sane for the geo grid
        Map<String, Object> searchResult = getAsMap(transformIndex + "/_search?size=1");
        Number actual = (Number) ((List<?>) XContentMapValues.extractValue("hits.hits._source.avg_rating", searchResult)).get(0);
        assertThat(actual, is(not(nullValue())));
        Map<String, Object> actualObj = (Map<String, Object>) ((List<?>) XContentMapValues.extractValue(
            "hits.hits._source.tile",
            searchResult
        )).get(0);
        assertThat(actualObj.get("type"), equalTo("polygon"));
        List<List<Double>> coordinates = ((List<List<List<Double>>>) actualObj.get("coordinates")).get(0);
        assertThat(coordinates, is(not(nullValue())));
        assertThat(coordinates, hasSize(5));
        assertThat(coordinates.get(0), hasSize(2));
        assertThat(coordinates.get(1), hasSize(2));
        assertThat(coordinates.get(2), hasSize(2));
        assertThat(coordinates.get(3), hasSize(2));
        assertThat(coordinates.get(4), hasSize(2));
    }

    public void testPivotWithWeightedAvgAgg() throws Exception {
        String transformId = "weighted_avg_agg_transform";
        String transformIndex = "weighted_avg_pivot_reviews";
        setupDataAccessRole(DATA_ACCESS_ROLE, REVIEWS_INDEX_NAME, transformIndex);

        final Request createTransformRequest = createRequestWithAuth(
            "PUT",
            getTransformEndpoint() + transformId,
            BASIC_AUTH_VALUE_TRANSFORM_ADMIN_WITH_SOME_DATA_ACCESS
        );

        String config = "{"
            + " \"source\": {\"index\":\""
            + REVIEWS_INDEX_NAME
            + "\"},"
            + " \"dest\": {\"index\":\""
            + transformIndex
            + "\"},";

        config += " \"pivot\": {"
            + "   \"group_by\": {"
            + "     \"reviewer\": {"
            + "       \"terms\": {"
            + "         \"field\": \"user_id\""
            + " } } },"
            + "   \"aggregations\": {"
            + "     \"avg_rating\": {"
            + "       \"weighted_avg\": {"
            + "         \"value\": {\"field\": \"stars\"},"
            + "         \"weight\": {\"field\": \"stars\"}"
            + "} } } }"
            + "}";

        createTransformRequest.setJsonEntity(config);
        Map<String, Object> createTransformResponse = entityAsMap(client().performRequest(createTransformRequest));
        assertThat(createTransformResponse.get("acknowledged"), equalTo(Boolean.TRUE));

        startAndWaitForTransform(transformId, transformIndex, BASIC_AUTH_VALUE_TRANSFORM_ADMIN_WITH_SOME_DATA_ACCESS);
        assertTrue(indexExists(transformIndex));

        Map<String, Object> searchResult = getAsMap(transformIndex + "/_search?q=reviewer:user_4");
        assertEquals(1, XContentMapValues.extractValue("hits.total.value", searchResult));
        Number actual = (Number) ((List<?>) XContentMapValues.extractValue("hits.hits._source.avg_rating", searchResult)).get(0);
        assertEquals(4.47169811, actual.doubleValue(), 0.000001);
    }

    public void testManyBucketsWithSmallPageSize() throws Exception {
        String transformId = "test_with_many_buckets";
        String transformIndex = transformId + "-idx";
        setupDataAccessRole(DATA_ACCESS_ROLE, REVIEWS_INDEX_NAME, transformIndex);
        final Request createTransformRequest = createRequestWithAuth(
            "PUT",
            getTransformEndpoint() + transformId,
            BASIC_AUTH_VALUE_TRANSFORM_ADMIN_WITH_SOME_DATA_ACCESS
        );

        String config = "{"
            + " \"source\": {\"index\":\""
            + REVIEWS_INDEX_NAME
            + "\"},"
            + " \"dest\": {\"index\":\""
            + transformIndex
            + "\"},"
            + " \"pivot\": {"
            + "   \"group_by\": {"
            + "     \"user.id\": {\"terms\": { \"field\": \"user_id\" }},"
            + "     \"business.id\": {\"terms\": { \"field\": \"business_id\" }},"
            + "     \"every_star\": {\"histogram\": { \"field\": \"stars\", \"interval\": 1 }},"
            + "     \"every_two_star\": {\"histogram\": { \"field\": \"stars\", \"interval\": 2 }},"
            + "     \"by_second\": {\"date_histogram\": {\"fixed_interval\": \"1s\",\"field\":\"timestamp\"}},"
            + "     \"by_day\": {\"date_histogram\": {\"fixed_interval\": \"1d\",\"field\":\"timestamp\"}},"
            + "     \"by_minute\": {\"date_histogram\": {\"fixed_interval\": \"1m\",\"field\":\"timestamp\"}}},"
            + "   \"aggregations\": {"
            + "     \"user.avg_rating\": {"
            + "       \"avg\": {"
            + "         \"field\": \"stars\""
            + " } } } },"
            + " \"settings\": {"
            + "   \"max_page_search_size\": 10"
            + " }"
            + "}";
        createTransformRequest.setJsonEntity(config);
        Map<String, Object> createTransformResponse = entityAsMap(client().performRequest(createTransformRequest));
        assertThat(createTransformResponse.get("acknowledged"), equalTo(Boolean.TRUE));

        startAndWaitForTransform(transformId, transformIndex, BASIC_AUTH_VALUE_TRANSFORM_ADMIN_WITH_SOME_DATA_ACCESS);
        assertTrue(indexExists(transformIndex));

        Map<String, Object> stats = getAsMap(getTransformEndpoint() + transformId + "/_stats");
        assertEquals(101, ((List<?>) XContentMapValues.extractValue("transforms.stats.pages_processed", stats)).get(0));
    }

    public void testContinuousStopWaitForCheckpoint() throws Exception {
        Request updateLoggingLevels = new Request("PUT", "/_cluster/settings");
        updateLoggingLevels.setJsonEntity(
            "{\"transient\": {"
                + "\"logger.org.elasticsearch.xpack.core.indexing.AsyncTwoPhaseIndexer\": \"trace\","
                + "\"logger.org.elasticsearch.xpack.transform\": \"trace\"}}"
        );
        client().performRequest(updateLoggingLevels);
        String indexName = "continuous_reviews_wait_for_checkpoint";
        createReviewsIndex(indexName);
        String transformId = "simple_continuous_pivot_wait_for_checkpoint";
        String transformIndex = "pivot_reviews_continuous_wait_for_checkpoint";
        setupDataAccessRole(DATA_ACCESS_ROLE, indexName, transformIndex);
        final Request createTransformRequest = createRequestWithAuth(
            "PUT",
            getTransformEndpoint() + transformId,
            BASIC_AUTH_VALUE_TRANSFORM_ADMIN_WITH_SOME_DATA_ACCESS
        );
        String config = "{"
            + " \"source\": {\"index\":\""
            + indexName
            + "\"},"
            + " \"dest\": {\"index\":\""
            + transformIndex
            + "\"},"
            + " \"frequency\": \"1s\","
            + " \"sync\": {\"time\": {\"field\": \"timestamp\", \"delay\": \"1s\"}},"
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

        startAndWaitForContinuousTransform(transformId, transformIndex, null);
        assertTrue(indexExists(transformIndex));
        assertBusy(() -> {
            try {
                stopTransform(transformId, false, true);
            } catch (ResponseException e) {
                // We get a conflict sometimes depending on WHEN we try to write the state, should eventually pass though
                assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(200));
            }
        });

        // get and check some users
        assertOnePivotValue(transformIndex + "/_search?q=reviewer:user_0", 3.776978417);
        assertOnePivotValue(transformIndex + "/_search?q=reviewer:user_5", 3.72);
        assertOnePivotValue(transformIndex + "/_search?q=reviewer:user_11", 3.846153846);
        assertOnePivotValue(transformIndex + "/_search?q=reviewer:user_20", 3.769230769);
        assertOnePivotValue(transformIndex + "/_search?q=reviewer:user_26", 3.918918918);
        deleteIndex(indexName);
    }

    public void testContinuousDateNanos() throws Exception {
        String indexName = "nanos";
        createDateNanoIndex(indexName, 1000);
        String transformId = "nanos_continuous_pivot";
        String transformIndex = "pivot_nanos_continuous";
        setupDataAccessRole(DATA_ACCESS_ROLE, indexName, transformIndex);
        final Request createTransformRequest = createRequestWithAuth(
            "PUT",
            getTransformEndpoint() + transformId,
            BASIC_AUTH_VALUE_TRANSFORM_ADMIN_WITH_SOME_DATA_ACCESS
        );
        String config = "{"
            + " \"source\": {\"index\":\""
            + indexName
            + "\"},"
            + " \"dest\": {\"index\":\""
            + transformIndex
            + "\"},"
            + " \"frequency\": \"1s\","
            + " \"sync\": {\"time\": {\"field\": \"timestamp\", \"delay\": \"1s\"}},"
            + " \"pivot\": {"
            + "   \"group_by\": {"
            + "     \"id\": {"
            + "       \"terms\": {"
            + "         \"field\": \"id\""
            + " } } },"
            + "   \"aggregations\": {"
            + "     \"avg_rating\": {"
            + "       \"avg\": {"
            + "         \"field\": \"rating\""
            + " } } } }"
            + "}";
        createTransformRequest.setJsonEntity(config);
        Map<String, Object> createTransformResponse = entityAsMap(client().performRequest(createTransformRequest));
        assertThat(createTransformResponse.get("acknowledged"), equalTo(Boolean.TRUE));

        startAndWaitForContinuousTransform(transformId, transformIndex, null);
        assertTrue(indexExists(transformIndex));
        // get and check some ids
        assertOnePivotValue(transformIndex + "/_search?q=id:id_0", 2.97);
        assertOnePivotValue(transformIndex + "/_search?q=id:id_1", 2.99);
        assertOnePivotValue(transformIndex + "/_search?q=id:id_7", 2.97);
        assertOnePivotValue(transformIndex + "/_search?q=id:id_9", 3.01);

        String nanoResolutionTimeStamp = Instant.now().minusSeconds(1).plusNanos(randomIntBetween(1, 1000000)).toString();

        final StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < 20; i++) {
            bulk.append("{\"index\":{\"_index\":\"" + indexName + "\"}}\n");
            bulk.append("{\"id\":\"")
                .append("id_")
                .append(i % 5)
                .append("\",\"rating\":")
                .append(7)
                .append(",\"timestamp\":")
                .append("\"" + nanoResolutionTimeStamp + "\"")
                .append("}\n");
        }
        bulk.append("\r\n");

        final Request bulkRequest = new Request("POST", "/_bulk");
        bulkRequest.addParameter("refresh", "true");
        bulkRequest.setJsonEntity(bulk.toString());
        client().performRequest(bulkRequest);

        waitForTransformCheckpoint(transformId, 2);

        stopTransform(transformId, false);
        refreshIndex(transformIndex);

        // assert changes
        assertOnePivotValue(transformIndex + "/_search?q=id:id_0", 3.125);
        assertOnePivotValue(transformIndex + "/_search?q=id:id_1", 3.144230769);

        // assert unchanged
        assertOnePivotValue(transformIndex + "/_search?q=id:id_7", 2.97);
        assertOnePivotValue(transformIndex + "/_search?q=id:id_9", 3.01);

        deleteIndex(indexName);
    }

    public void testPivotWithPercentile() throws Exception {
        String transformId = "percentile_pivot";
        String transformIndex = "percentile_pivot_reviews";
        setupDataAccessRole(DATA_ACCESS_ROLE, REVIEWS_INDEX_NAME, transformIndex);
        final Request createTransformRequest = createRequestWithAuth(
            "PUT",
            getTransformEndpoint() + transformId,
            BASIC_AUTH_VALUE_TRANSFORM_ADMIN_WITH_SOME_DATA_ACCESS
        );
        String config = "{"
            + " \"source\": {\"index\":\""
            + REVIEWS_INDEX_NAME
            + "\"},"
            + " \"dest\": {\"index\":\""
            + transformIndex
            + "\"},"
            + " \"frequency\": \"1s\","
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
            + "    } },"
            + "     \"p\": {"
            + "         \"percentiles\" : {"
            + "            \"field\": \"stars\", "
            + "            \"percents\": [5, 50, 90, 99.9]"
            + "         }"
            + "      } } }"
            + "}";
        createTransformRequest.setJsonEntity(config);
        Map<String, Object> createTransformResponse = entityAsMap(client().performRequest(createTransformRequest));
        assertThat(createTransformResponse.get("acknowledged"), equalTo(Boolean.TRUE));

        startAndWaitForTransform(transformId, transformIndex);
        assertTrue(indexExists(transformIndex));
        // get and check some users
        assertOnePivotValue(transformIndex + "/_search?q=reviewer:user_11", 3.846153846);
        assertOnePivotValue(transformIndex + "/_search?q=reviewer:user_26", 3.918918918);

        Map<String, Object> searchResult = getAsMap(transformIndex + "/_search?q=reviewer:user_4");
        assertEquals(1, XContentMapValues.extractValue("hits.total.value", searchResult));
        Number actual = (Number) ((List<?>) XContentMapValues.extractValue("hits.hits._source.p.5", searchResult)).get(0);
        assertEquals(1, actual.longValue());
        actual = (Number) ((List<?>) XContentMapValues.extractValue("hits.hits._source.p.50", searchResult)).get(0);
        assertEquals(5, actual.longValue());
        actual = (Number) ((List<?>) XContentMapValues.extractValue("hits.hits._source.p.99_9", searchResult)).get(0);
        assertEquals(5, actual.longValue());

        searchResult = getAsMap(transformIndex + "/_search?q=reviewer:user_11");
        assertEquals(1, XContentMapValues.extractValue("hits.total.value", searchResult));
        actual = (Number) ((List<?>) XContentMapValues.extractValue("hits.hits._source.p.5", searchResult)).get(0);
        assertEquals(1, actual.longValue());
        actual = (Number) ((List<?>) XContentMapValues.extractValue("hits.hits._source.p.50", searchResult)).get(0);
        assertEquals(4, actual.longValue());
        actual = (Number) ((List<?>) XContentMapValues.extractValue("hits.hits._source.p.99_9", searchResult)).get(0);
        assertEquals(5, actual.longValue());
    }

    public void testPivotWithFilter() throws Exception {
        String transformId = "filter_pivot";
        String transformIndex = "filter_pivot_reviews";
        setupDataAccessRole(DATA_ACCESS_ROLE, REVIEWS_INDEX_NAME, transformIndex);
        final Request createTransformRequest = createRequestWithAuth(
            "PUT",
            getTransformEndpoint() + transformId,
            BASIC_AUTH_VALUE_TRANSFORM_ADMIN_WITH_SOME_DATA_ACCESS
        );
        String config = "{"
            + " \"source\": {\"index\":\""
            + REVIEWS_INDEX_NAME
            + "\"},"
            + " \"dest\": {\"index\":\""
            + transformIndex
            + "\"},"
            + " \"frequency\": \"1s\","
            + " \"pivot\": {"
            + "   \"group_by\": {"
            + "     \"reviewer\": {"
            + "       \"terms\": {"
            + "         \"field\": \"user_id\""
            + " } } },"
            + "   \"aggregations\": {"
            + "     \"top_ratings\": {"
            + "       \"filter\": {"
            + "         \"range\": {"
            + "            \"stars\": {"
            + "               \"gte\": 4 "
            + "         } } } },"
            + "     \"top_ratings_detail\": {"
            + "       \"filter\": {"
            + "         \"range\": {"
            + "            \"stars\": {"
            + "               \"gte\": 4"
            + "         } } },"
            + "         \"aggregations\": {"
            + "            \"unique_count\": {"
            + "               \"cardinality\": {"
            + "                  \"field\": \"business_id\""
            + "         } },"
            + "            \"max\": {"
            + "               \"max\": {"
            + "                  \"field\": \"stars\""
            + "         } },"
            + "            \"min\": {"
            + "               \"min\": {"
            + "                  \"field\": \"stars\""
            + "         } }"
            + " } } } }"
            + "}";

        createTransformRequest.setJsonEntity(config);
        Map<String, Object> createTransformResponse = entityAsMap(client().performRequest(createTransformRequest));
        assertThat(createTransformResponse.get("acknowledged"), equalTo(Boolean.TRUE));

        startAndWaitForTransform(transformId, transformIndex);
        assertTrue(indexExists(transformIndex));
        // get and check some users

        Map<String, Object> searchResult = getAsMap(transformIndex + "/_search?q=reviewer:user_4");
        assertEquals(1, XContentMapValues.extractValue("hits.total.value", searchResult));

        Number actual = (Number) ((List<?>) XContentMapValues.extractValue("hits.hits._source.top_ratings", searchResult)).get(0);
        assertEquals(29, actual.longValue());
        actual = (Number) ((List<?>) XContentMapValues.extractValue("hits.hits._source.top_ratings_detail.min", searchResult)).get(0);
        assertEquals(4, actual.longValue());
        actual = (Number) ((List<?>) XContentMapValues.extractValue("hits.hits._source.top_ratings_detail.max", searchResult)).get(0);
        assertEquals(5, actual.longValue());
        actual = (Number) ((List<?>) XContentMapValues.extractValue("hits.hits._source.top_ratings_detail.unique_count", searchResult)).get(
            0
        );
        assertEquals(4, actual.longValue());

        searchResult = getAsMap(transformIndex + "/_search?q=reviewer:user_2");
        assertEquals(1, XContentMapValues.extractValue("hits.total.value", searchResult));
        actual = (Number) ((List<?>) XContentMapValues.extractValue("hits.hits._source.top_ratings", searchResult)).get(0);
        assertEquals(19, actual.longValue());
        actual = (Number) ((List<?>) XContentMapValues.extractValue("hits.hits._source.top_ratings_detail.min", searchResult)).get(0);
        assertEquals(4, actual.longValue());
        actual = (Number) ((List<?>) XContentMapValues.extractValue("hits.hits._source.top_ratings_detail.max", searchResult)).get(0);
        assertEquals(5, actual.longValue());
        actual = (Number) ((List<?>) XContentMapValues.extractValue("hits.hits._source.top_ratings_detail.unique_count", searchResult)).get(
            0
        );
        assertEquals(3, actual.longValue());
    }

    private void createDateNanoIndex(String indexName, int numDocs) throws IOException {
        // create mapping
        try (XContentBuilder builder = jsonBuilder()) {
            builder.startObject();
            {
                builder.startObject("mappings")
                    .startObject("properties")
                    .startObject("timestamp")
                    .field("type", "date_nanos")
                    .field("format", "strict_date_optional_time_nanos")
                    .endObject()
                    .startObject("id")
                    .field("type", "keyword")
                    .endObject()
                    .startObject("rating")
                    .field("type", "integer")
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

        String randomNanos = "," + randomIntBetween(100000000, 999999999);
        final StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < numDocs; i++) {
            bulk.append("{\"index\":{\"_index\":\"" + indexName + "\"}}\n");
            bulk.append("{\"id\":\"")
                .append("id_")
                .append(i % 10)
                .append("\",\"rating\":")
                .append(i % 7)
                .append(",\"timestamp\":")
                .append("\"2020-01-27T01:59:00" + randomNanos + "Z\"")
                .append("}\n");

            if (i % 50 == 0) {
                bulk.append("\r\n");
                final Request bulkRequest = new Request("POST", "/_bulk");
                bulkRequest.addParameter("refresh", "true");
                bulkRequest.setJsonEntity(bulk.toString());
                client().performRequest(bulkRequest);
                // clear the builder
                bulk.setLength(0);
            }
        }
        bulk.append("\r\n");

        final Request bulkRequest = new Request("POST", "/_bulk");
        bulkRequest.addParameter("refresh", "true");
        bulkRequest.setJsonEntity(bulk.toString());
        client().performRequest(bulkRequest);
    }
}
