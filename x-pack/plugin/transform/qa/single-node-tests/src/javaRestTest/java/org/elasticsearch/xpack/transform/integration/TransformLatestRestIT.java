/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.integration;

import org.elasticsearch.client.Request;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.Strings;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class TransformLatestRestIT extends TransformRestTestCase {

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
        setupDataAccessRole(DATA_ACCESS_ROLE, REVIEWS_INDEX_NAME);
        setupUser(TEST_USER_NAME, Arrays.asList("transform_admin", DATA_ACCESS_ROLE));

        // it's not possible to run it as @BeforeClass as clients aren't initialized then, so we need this little hack
        if (indicesCreated) {
            return;
        }

        createReviewsIndex();
        indicesCreated = true;
    }

    public void testLatestWithAggregateMetricDouble() throws Exception {
        String transformId = "aggregate_metric_double_latest_transform";
        String transformIndex = "aggregate_metric_double_latest_reviews";
        setupDataAccessRole(DATA_ACCESS_ROLE, REVIEWS_INDEX_NAME, transformIndex);

        String config = Strings.format("""
            {
              "source": {
                "index": "%s"
              },
              "dest": {
                "index": "%s"
              },
              "latest": {
                "unique_key": [ "user_id" ],
                "sort": "@timestamp"
              }
            }""", REVIEWS_INDEX_NAME, transformIndex);

        final Request createPreviewRequest = createRequestWithAuth("POST", getTransformEndpoint() + "_preview", null);
        createPreviewRequest.setJsonEntity(config);
        Map<String, Object> previewTransformResponse = entityAsMap(client().performRequest(createPreviewRequest));
        assertThat(
            XContentMapValues.extractValue("generated_dest_index.mappings.properties", previewTransformResponse),
            is(equalTo(Map.of()))
        );

        final Request createTransformRequest = createRequestWithAuth(
            "PUT",
            getTransformEndpoint() + transformId,
            BASIC_AUTH_VALUE_TRANSFORM_ADMIN_WITH_SOME_DATA_ACCESS
        );
        createTransformRequest.setJsonEntity(config);
        Map<String, Object> createTransformResponse = entityAsMap(client().performRequest(createTransformRequest));
        assertThat(createTransformResponse.get("acknowledged"), equalTo(Boolean.TRUE));

        startAndWaitForTransform(transformId, transformIndex, BASIC_AUTH_VALUE_TRANSFORM_ADMIN_WITH_SOME_DATA_ACCESS);
        assertTrue(indexExists(transformIndex));

        Map<String, Object> searchResult = getAsMap(transformIndex + "/_search?q=user_id:user_4");
        assertEquals(1, XContentMapValues.extractValue("hits.total.value", searchResult));
        assertThat(((List<?>) XContentMapValues.extractValue("hits.hits._source.stars_stats.min", searchResult)).get(0), is(equalTo(4)));
        assertThat(((List<?>) XContentMapValues.extractValue("hits.hits._source.stars_stats.max", searchResult)).get(0), is(equalTo(6)));
        assertThat(((List<?>) XContentMapValues.extractValue("hits.hits._source.stars_stats.sum", searchResult)).get(0), is(equalTo(50)));

        searchResult = getAsMap(transformIndex + "/_search?q=user_id:user_1");
        assertEquals(1, XContentMapValues.extractValue("hits.total.value", searchResult));
        assertThat(((List<?>) XContentMapValues.extractValue("hits.hits._source.stars_stats.min", searchResult)).get(0), is(equalTo(1)));
        assertThat(((List<?>) XContentMapValues.extractValue("hits.hits._source.stars_stats.max", searchResult)).get(0), is(equalTo(3)));
        assertThat(((List<?>) XContentMapValues.extractValue("hits.hits._source.stars_stats.sum", searchResult)).get(0), is(equalTo(20)));
    }

    public void testLatestWithAggregateMetricDoubleAsUniqueKey() throws Exception {
        String transformId = "aggregate_metric_double_latest_transform";
        String transformIndex = "aggregate_metric_double_latest_reviews";
        String statsField = "stars_stats";
        setupDataAccessRole(DATA_ACCESS_ROLE, REVIEWS_INDEX_NAME, transformIndex);

        String config = Strings.format("""
            {
              "source": {
                "index": "%s"
              },
              "dest": {
                "index": "%s"
              },
              "latest": {
                "unique_key": [ "%s" ],
                "sort": "@timestamp"
              }
            }""", REVIEWS_INDEX_NAME, transformIndex, statsField);

        {
            final Request createPreviewRequest = createRequestWithAuth("POST", getTransformEndpoint() + "_preview", null);
            createPreviewRequest.setJsonEntity(config);
            Exception e = expectThrows(Exception.class, () -> client().performRequest(createPreviewRequest));
            assertThat(
                e.getMessage(),
                containsString("Field [stars_stats] of type [aggregate_metric_double] is not supported for aggregation [terms]")
            );
        }
        {
            final Request createTransformRequest = createRequestWithAuth(
                "PUT",
                getTransformEndpoint() + transformId,
                BASIC_AUTH_VALUE_TRANSFORM_ADMIN_WITH_SOME_DATA_ACCESS
            );
            createTransformRequest.setJsonEntity(config);
            Exception e = expectThrows(Exception.class, () -> client().performRequest(createTransformRequest));
            assertThat(
                e.getMessage(),
                containsString("Field [stars_stats] of type [aggregate_metric_double] is not supported for aggregation [terms]")
            );
        }
    }

    public void testContinuousLatestWithFrom_NoDocs() throws Exception {
        testContinuousLatestWithFrom("latest_from_no_docs", "reviews_from_no_docs", "2017-02-20", 0);
    }

    public void testContinuousLatestWithFrom_OneDoc() throws Exception {
        testContinuousLatestWithFrom("latest_from_one_doc", "reviews_from_one_doc", "2017-02-10", 1);
    }

    public void testContinuousLatestWithFrom_AllDocs_FromNull() throws Exception {
        testContinuousLatestWithFrom("latest_from_all_docs_from_null", "reviews_from_all_docs_from_null", null, 28);
    }

    public void testContinuousLatestWithFrom_AllDocs() throws Exception {
        testContinuousLatestWithFrom("latest_from_all_docs", "reviews_from_all_docs", "2017-01-01", 28);
    }

    private void testContinuousLatestWithFrom(String transformId, String indexName, String from, int expectedDestNumDocs) throws Exception {
        createReviewsIndex(indexName);
        String transformIndex = transformId + "-dest";
        setupDataAccessRole(DATA_ACCESS_ROLE, indexName, transformIndex);
        Request createTransformRequest = createRequestWithAuth(
            "PUT",
            getTransformEndpoint() + transformId,
            BASIC_AUTH_VALUE_TRANSFORM_ADMIN_WITH_SOME_DATA_ACCESS
        );
        String config = Strings.format("""
            {
              "source": {
                "index": "%s"
              },
              "dest": {
                "index": "%s"
              },
              "frequency": "1s",
              "sync": {
                "time": {
                  "field": "timestamp",
                  "delay": "1s"
                }
              },
              "latest": {
                "unique_key": [ "user_id" ],
                "sort": "timestamp"
              }
            }""", indexName, transformIndex);
        createTransformRequest.setJsonEntity(config);
        Map<String, Object> createTransformResponse = entityAsMap(client().performRequest(createTransformRequest));
        assertThat(createTransformResponse.get("acknowledged"), equalTo(Boolean.TRUE));

        assertSourceIndexContents(indexName, 1000, "2017-01-10T10:10:10.000Z", "2017-01-30T22:34:38.000Z");

        {
            StringBuilder bulk = new StringBuilder();
            bulk.append(Strings.format("""
                {"index":{"_index":"%s"}}
                {"user_id":"user_%s","business_id":"business_%s","stars":%s,"location":"%s","timestamp":%s}
                """, indexName, 666, 777, 7, 888, "\"2017-02-15\""));
            bulk.append("\r\n");

            Request bulkRequest = new Request("POST", "/_bulk");
            bulkRequest.addParameter("refresh", "true");
            bulkRequest.setJsonEntity(bulk.toString());
            Map<String, Object> bulkResponse = entityAsMap(client().performRequest(bulkRequest));
            assertThat(bulkResponse.get("errors"), equalTo(Boolean.FALSE));
        }

        assertSourceIndexContents(indexName, 1001, "2017-01-10T10:10:10.000Z", "2017-02-15T00:00:00.000Z");

        startAndWaitForContinuousTransform(transformId, transformIndex, null, from, 1L);
        assertTrue(indexExists(transformIndex));

        Map<String, Object> transformIndexStats = getAsMap(transformIndex + "/_stats");
        assertThat(
            "Stats were: " + transformIndexStats,
            XContentMapValues.extractValue("_all.total.docs.count", transformIndexStats),
            is(equalTo(expectedDestNumDocs))
        );

        stopTransform(transformId, false);
        deleteIndex(indexName);
    }

    private void assertSourceIndexContents(String indexName, int expectedNumDocs, String expectedMinTimestamp, String expectedMaxTimestamp)
        throws IOException {
        Request searchRequest = new Request("GET", indexName + "/_search");
        searchRequest.setJsonEntity("""
            {
              "size": 0,
              "aggregations": {
                "min_timestamp": {
                  "min": {
                    "field": "timestamp"
                  }
                },
                "max_timestamp": {
                  "max": {
                    "field": "timestamp"
                  }
                }
              }
            }""");
        Map<String, Object> searchResponse = entityAsMap(client().performRequest(searchRequest));
        assertThat(XContentMapValues.extractValue("hits.total.value", searchResponse), is(equalTo(expectedNumDocs)));
        assertThat(
            XContentMapValues.extractValue("aggregations.min_timestamp.value_as_string", searchResponse),
            is(equalTo(expectedMinTimestamp))
        );
        assertThat(
            XContentMapValues.extractValue("aggregations.max_timestamp.value_as_string", searchResponse),
            is(equalTo(expectedMaxTimestamp))
        );
    }
}
