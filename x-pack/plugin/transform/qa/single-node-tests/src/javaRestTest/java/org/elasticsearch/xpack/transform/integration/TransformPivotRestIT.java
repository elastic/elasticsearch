/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.integration;

import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.xcontent.XContentBuilder;
import org.junit.Before;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInRelativeOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class TransformPivotRestIT extends TransformRestTestCase {

    private static final String TEST_USER_NAME_NO_ACCESS = "no_authorization";
    private static final String TEST_USER_NAME = "transform_admin_plus_data";
    private static final String DATA_ACCESS_ROLE = "test_data_access";
    private static final String BASIC_AUTH_VALUE_TRANSFORM_ADMIN_WITH_SOME_DATA_ACCESS = basicAuthHeaderValue(
        TEST_USER_NAME,
        TEST_PASSWORD_SECURE_STRING
    );
    private static final String BASIC_AUTH_VALUE_NO_ACCESS = basicAuthHeaderValue(TEST_USER_NAME_NO_ACCESS, TEST_PASSWORD_SECURE_STRING);

    private static boolean indicesCreated = false;

    @Override
    protected RestClient buildClient(Settings settings, HttpHost[] hosts) throws IOException {
        RestClientBuilder builder = RestClient.builder(hosts);
        configureClient(builder, settings);
        builder.setStrictDeprecationMode(false);
        return builder.build();
    }

    // preserve indices in order to reuse source indices in several test cases
    @Override
    protected boolean preserveIndicesUponCompletion() {
        return true;
    }

    @Before
    public void createIndexes() throws IOException {
        setupDataAccessRole(DATA_ACCESS_ROLE, REVIEWS_INDEX_NAME);
        setupUser(TEST_USER_NAME, List.of("transform_admin", DATA_ACCESS_ROLE));

        // it's not possible to run it as @BeforeClass as clients aren't initialized then, so we need this little hack
        if (indicesCreated) {
            return;
        }

        createReviewsIndex();
        createReviewsIndexNano();
        indicesCreated = true;
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
        assertOneCount(transformIndex + "/_search?q=reviewer:user_0", "hits.hits._source.affiliate_missing", 0);
        assertOnePivotValue(transformIndex + "/_search?q=reviewer:user_5", 3.72);
        assertOneCount(transformIndex + "/_search?q=reviewer:user_5", "hits.hits._source.affiliate_missing", 25);
        assertOnePivotValue(transformIndex + "/_search?q=reviewer:user_11", 3.846153846);
        assertOnePivotValue(transformIndex + "/_search?q=reviewer:user_20", 3.769230769);
        assertOnePivotValue(transformIndex + "/_search?q=reviewer:user_26", 3.918918918);
        assertOneCount(transformIndex + "/_search?q=reviewer:user_26", "hits.hits._source.affiliate_missing", 0);
    }

    public void testSimplePivotWithSecondaryHeaders() throws Exception {
        setupUser(TEST_USER_NAME_NO_ACCESS, List.of("transform_admin"));
        String transformId = "simple-pivot";
        String transformIndex = "pivot_reviews";
        setupDataAccessRole(DATA_ACCESS_ROLE, REVIEWS_INDEX_NAME, transformIndex);
        createPivotReviewsTransform(
            transformId,
            transformIndex,
            null,
            null,
            null,
            null,
            BASIC_AUTH_VALUE_NO_ACCESS,
            BASIC_AUTH_VALUE_TRANSFORM_ADMIN_WITH_SOME_DATA_ACCESS,
            REVIEWS_INDEX_NAME
        );
        startAndWaitForTransform(
            transformId,
            transformIndex,
            BASIC_AUTH_VALUE_NO_ACCESS,
            BASIC_AUTH_VALUE_TRANSFORM_ADMIN_WITH_SOME_DATA_ACCESS,
            new String[0]
        );

        // we expect 27 documents as there shall be 27 user_id's
        // Just need to validate that things ran with secondary headers
        Map<String, Object> indexStats = getAsMap(transformIndex + "/_stats");
        assertEquals(27, XContentMapValues.extractValue("_all.total.docs.count", indexStats));
    }

    public void testSimpleDataStreamPivot() throws Exception {
        String indexName = "reviews_data_stream";
        createReviewsIndex(indexName, 1000, 27, "date", true, -1, null);
        String transformId = "simple_data_stream_pivot";
        String transformIndex = "pivot_reviews_data_stream";
        setupDataAccessRole(DATA_ACCESS_ROLE, indexName, transformIndex);
        createPivotReviewsTransform(
            transformId,
            transformIndex,
            null,
            null,
            null,
            null,
            null,
            BASIC_AUTH_VALUE_TRANSFORM_ADMIN_WITH_SOME_DATA_ACCESS,
            indexName
        );

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
        client().performRequest(new Request("DELETE", "/_data_stream/" + indexName));
    }

    public void testSimpleBooleanPivot() throws Exception {
        String transformId = "simple-boolean-pivot";
        String sourceIndex = "boolean_value";
        String transformIndex = "pivot_boolean_value";

        Request doc1 = new Request("POST", sourceIndex + "/_doc");
        doc1.setJsonEntity("{\"bool\": true, \"val\": 1.0}");
        client().performRequest(doc1);
        Request doc2 = new Request("POST", sourceIndex + "/_doc");
        doc2.setJsonEntity("{\"bool\": true, \"val\": 0.0}");
        client().performRequest(doc2);
        Request doc3 = new Request("POST", sourceIndex + "/_doc");
        doc3.setJsonEntity("{\"bool\": false, \"val\": 2.0}");
        client().performRequest(doc3);
        refreshIndex(sourceIndex);

        setupDataAccessRole(DATA_ACCESS_ROLE, sourceIndex, transformIndex);
        final Request createTransformRequest = createRequestWithAuth(
            "PUT",
            getTransformEndpoint() + transformId,
            BASIC_AUTH_VALUE_TRANSFORM_ADMIN_WITH_SOME_DATA_ACCESS
        );
        String config = """
            {
              "source": {
                "index": "boolean_value"
              },
              "dest": {
                "index": "pivot_boolean_value"
              },
              "pivot": {
                "group_by": {
                  "bool": {
                    "terms": {
                      "field": "bool"
                    }
                  }
                },
                "aggregations": {
                  "avg_rating": {
                    "avg": {
                      "field": "val"
                    }
                  }
                }
              }
            }""";

        createTransformRequest.setJsonEntity(config);
        Map<String, Object> createTransformResponse = entityAsMap(client().performRequest(createTransformRequest));
        assertThat(createTransformResponse.get("acknowledged"), equalTo(Boolean.TRUE));

        startAndWaitForTransform(transformId, transformIndex);
        assertTrue(indexExists(transformIndex));
        assertOnePivotValue(transformIndex + "/_search?q=bool:true", 0.5);
        assertOnePivotValue(transformIndex + "/_search?q=bool:false", 2.0);

        Map<String, Object> indexStats = getAsMap(transformIndex + "/_stats");
        assertEquals(2, XContentMapValues.extractValue("_all.total.docs.count", indexStats));
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
        String config = Strings.format("""
            {
              "dest": {
                "index": "%s"
              },
              "source": {
                "index": "%s"
              },
              "pivot": {
                "group_by": {
                  "reviewer": {
                    "terms": {
                      "script": {
                        "source": "'reviewer_' + doc['user_id'].value"
                      }
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
              },
              "frequency": "1s"
            }""", transformIndex, REVIEWS_INDEX_NAME);
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
        pipelineRequest.setJsonEntity(Strings.format("""
            {
               "description" : "my pivot pipeline",
               "processors" : [
                 {
                   "set" : {
                     "field": "pipeline_field",
                     "value": %s
                   }
                 }
               ]
            }""", pipelineValue));
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
        String config = Strings.format("""
            {
              "source": {
                "index": "%s"
              },
              "dest": {
                "index": "%s"
              },
              "frequency": "1s",
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
                  },
                  "over_38": {
                    "bucket_selector": {
                      "buckets_path": {
                        "rating": "avg_rating"
                      },
                      "script": "params.rating > 3.8"
                    }
                  }
                }
              }
            }""", REVIEWS_INDEX_NAME, transformIndex);
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
        createReviewsIndex(indexName, 1000, 27, "date", false, 5, "user_id");
        String transformId = "simple_continuous_pivot";
        String transformIndex = "pivot_reviews_continuous";
        setupDataAccessRole(DATA_ACCESS_ROLE, indexName, transformIndex);
        final Request createTransformRequest = createRequestWithAuth(
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
              "pivot": {
                "group_by": {
                  "reviewer": {
                    "terms": {
                      "field": "user_id",
                      "missing_bucket": true
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
            }""", indexName, transformIndex);
        createTransformRequest.setJsonEntity(config);
        Map<String, Object> createTransformResponse = entityAsMap(client().performRequest(createTransformRequest));
        assertThat(createTransformResponse.get("acknowledged"), equalTo(Boolean.TRUE));

        startAndWaitForContinuousTransform(transformId, transformIndex, null);
        assertTrue(indexExists(transformIndex));
        // get and check some users
        assertOnePivotValue(transformIndex + "/_search?q=reviewer:user_0", 3.776978417);

        // missing bucket check
        assertOnePivotValue(transformIndex + "/_search?q=!_exists_:reviewer", 3.72);

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
            int stars = (i * 32) % 5;
            long business = (stars * user) % 13;
            String location = (user + 10) + "," + (user + 15);
            bulk.append(Strings.format("""
                {"index":{"_index":"%s"}}
                {"user_id":"user_%s","business_id":"business_%s","stars":%s,"location":"%s","timestamp":%s}
                """, indexName, user, business, stars, location, dateStamp));
            stars = 5;
            business = 11;
            bulk.append(Strings.format("""
                {"index":{"_index":"%s"}}
                {"user_id":"user_%s","business_id":"business_%s","stars":%s,"location":"%s","timestamp":%s}
                """, indexName, user26, business, stars, location, dateStamp));
            bulk.append(Strings.format("""
                {"index":{"_index":"%s"}}
                {"business_id":"business_%s","stars":%s,"location":"%s","timestamp":%s}
                """, indexName, business, stars, location, dateStamp));
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
        assertOnePivotValue(transformIndex + "/_search?q=!_exists_:reviewer", 4.36);
        assertOnePivotValue(transformIndex + "/_search?q=reviewer:user_11", 3.846153846);
        assertOnePivotValue(transformIndex + "/_search?q=reviewer:user_20", 3.769230769);

        assertOnePivotValue(transformIndex + "/_search?q=reviewer:user_26", 4.354838709);
        assertOnePivotValue(transformIndex + "/_search?q=reviewer:user_42", 2.0);
    }

    public void testContinuousPivotFrom() throws Exception {
        String indexName = "continuous_reviews_from";
        createReviewsIndex(indexName);
        String transformId = "continuous_pivot_from";
        String transformIndex = "pivot_reviews_continuous_from";
        setupDataAccessRole(DATA_ACCESS_ROLE, indexName, transformIndex);
        final Request createTransformRequest = createRequestWithAuth(
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
              "pivot": {
                "group_by": {
                  "reviewer": {
                    "terms": {
                      "field": "user_id",
                      "missing_bucket": true
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
            }""", indexName, transformIndex);
        createTransformRequest.setJsonEntity(config);
        Map<String, Object> createTransformResponse = entityAsMap(client().performRequest(createTransformRequest));
        assertThat(createTransformResponse.get("acknowledged"), equalTo(Boolean.TRUE));

        final StringBuilder bulk = new StringBuilder();
        bulk.append(Strings.format("""
            {"index":{"_index":"%s"}}
            {"user_id":"user_%s","business_id":"business_%s","stars":%s,"location":"%s","timestamp":%s}
            """, indexName, 666, 777, 7, 888, "\"2017-01-20\""));
        bulk.append("\r\n");

        final Request bulkRequest = new Request("POST", "/_bulk");
        bulkRequest.addParameter("refresh", "true");
        bulkRequest.setJsonEntity(bulk.toString());
        Map<String, Object> bulkResponse = entityAsMap(client().performRequest(bulkRequest));
        assertThat(bulkResponse.get("errors"), equalTo(Boolean.FALSE));

        startAndWaitForContinuousTransform(transformId, transformIndex, null, "2017-01-23", 1L);
        assertTrue(indexExists(transformIndex));

        assertEquals(27, XContentMapValues.extractValue("_all.total.docs.count", getAsMap(transformIndex + "/_stats")));

        // get and check some users
        assertOnePivotValue(transformIndex + "/_search?q=reviewer:user_0", 3.776978417);
        assertOnePivotValue(transformIndex + "/_search?q=reviewer:user_11", 3.846153846);
        assertOnePivotValue(transformIndex + "/_search?q=reviewer:user_20", 3.769230769);
        assertOnePivotValue(transformIndex + "/_search?q=reviewer:user_26", 3.918918918);

        stopTransform(transformId, false);
        deleteIndex(indexName);
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

        String config = Strings.format("""
            {
              "source": {
                "index": "%s"
              },
              "dest": {
                "index": "%s"
              },
              "pivot": {
                "group_by": {
                  "every_2": {
                    "histogram": {
                      "interval": 2,
                      "field": "stars"
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
            }""", REVIEWS_INDEX_NAME, transformIndex);

        createTransformRequest.setJsonEntity(config);
        Map<String, Object> createTransformResponse = entityAsMap(client().performRequest(createTransformRequest));
        assertThat(createTransformResponse.get("acknowledged"), equalTo(Boolean.TRUE));

        startAndWaitForTransform(transformId, transformIndex);
        assertTrue(indexExists(transformIndex));

        // we expect 3 documents as there shall be 5 unique star values and we are bucketing every 2 starting at 0
        Map<String, Object> indexStats = getAsMap(transformIndex + "/_stats");
        assertEquals(3, XContentMapValues.extractValue("_all.total.docs.count", indexStats));
        assertOnePivotValue(transformIndex + "/_search?q=every_2:0", 1.0);
    }

    public void testContinuousPivotHistogram() throws Exception {
        String indexName = "continuous_reviews_histogram";
        createReviewsIndex(indexName);
        String transformId = "simple_continuous_pivot";
        String transformIndex = "pivot_reviews_continuous_histogram";
        setupDataAccessRole(DATA_ACCESS_ROLE, indexName, transformIndex);
        final Request createTransformRequest = createRequestWithAuth(
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
              "pivot": {
                "group_by": {
                  "every_2": {
                    "histogram": {
                      "interval": 2,
                      "field": "stars"
                    }
                  }
                },
                "aggregations": {
                  "user_dc": {
                    "cardinality": {
                      "field": "user_id"
                    }
                  }
                }
              }
            }""", indexName, transformIndex);
        createTransformRequest.setJsonEntity(config);
        Map<String, Object> createTransformResponse = entityAsMap(client().performRequest(createTransformRequest));
        assertThat(createTransformResponse.get("acknowledged"), equalTo(Boolean.TRUE));

        startAndWaitForContinuousTransform(transformId, transformIndex, null);
        assertTrue(indexExists(transformIndex));

        // we expect 3 documents as there shall be 5 unique star values and we are bucketing every 2 starting at 0
        Map<String, Object> indexStats = getAsMap(transformIndex + "/_stats");
        assertEquals(3, XContentMapValues.extractValue("_all.total.docs.count", indexStats));

        Map<String, Object> searchResult = getAsMap(transformIndex + "/_search?q=every_2:0");
        assertEquals(1, XContentMapValues.extractValue("hits.total.value", searchResult));
        assertThat(((List<?>) XContentMapValues.extractValue("hits.hits._source.user_dc", searchResult)).get(0), equalTo(19));

        searchResult = getAsMap(transformIndex + "/_search?q=every_2:2");
        assertEquals(1, XContentMapValues.extractValue("hits.total.value", searchResult));
        assertThat(((List<?>) XContentMapValues.extractValue("hits.hits._source.user_dc", searchResult)).get(0), equalTo(27));

        searchResult = getAsMap(transformIndex + "/_search?q=every_2:4");
        assertEquals(1, XContentMapValues.extractValue("hits.total.value", searchResult));
        assertThat(((List<?>) XContentMapValues.extractValue("hits.hits._source.user_dc", searchResult)).get(0), equalTo(27));

        final StringBuilder bulk = new StringBuilder();

        long dateStamp = Instant.now().toEpochMilli() - 1_000;

        // add 5 data points with 3 new users: 27, 28, 29
        for (int i = 25; i < 30; i++) {
            String location = (i + 10) + "," + (i + 15);
            bulk.append(Strings.format("""
                {"index":{"_index":"%s"}}
                {"user_id":"user_%s","business_id":"business_%s","stars":%s,"location":"%s","timestamp":%s}
                """, indexName, i, i, 3, location, dateStamp));
        }
        bulk.append("\r\n");

        final Request bulkRequest = new Request("POST", "/_bulk");
        bulkRequest.addParameter("refresh", "true");
        bulkRequest.setJsonEntity(bulk.toString());
        client().performRequest(bulkRequest);

        waitForTransformCheckpoint(transformId, 2);

        stopTransform(transformId, false);
        refreshIndex(transformIndex);

        // assert after changes
        // still 3 documents
        indexStats = getAsMap(transformIndex + "/_stats");
        assertEquals(3, XContentMapValues.extractValue("_all.total.docs.count", indexStats));

        searchResult = getAsMap(transformIndex + "/_search?q=every_2:0");
        assertEquals(1, XContentMapValues.extractValue("hits.total.value", searchResult));
        assertThat(((List<?>) XContentMapValues.extractValue("hits.hits._source.user_dc", searchResult)).get(0), equalTo(19));

        searchResult = getAsMap(transformIndex + "/_search?q=every_2:2");
        assertEquals(1, XContentMapValues.extractValue("hits.total.value", searchResult));
        assertThat(((List<?>) XContentMapValues.extractValue("hits.hits._source.user_dc", searchResult)).get(0), equalTo(30));

        searchResult = getAsMap(transformIndex + "/_search?q=every_2:4");
        assertEquals(1, XContentMapValues.extractValue("hits.total.value", searchResult));
        assertThat(((List<?>) XContentMapValues.extractValue("hits.hits._source.user_dc", searchResult)).get(0), equalTo(27));

        deleteTransform(transformId);
        deleteIndex(indexName);
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

        String config = Strings.format("""
            {
              "source": {
                "index": "%s"
              },
              "dest": {
                "index": "%s"
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
                  },
                  "variability_rating": {
                    "median_absolute_deviation": {
                      "field": "stars"
                    }
                  },
                  "sum_rating": {
                    "sum": {
                      "field": "stars"
                    }
                  },
                  "cardinality_business": {
                    "cardinality": {
                      "field": "business_id"
                    }
                  },
                  "min_rating": {
                    "min": {
                      "field": "stars"
                    }
                  },
                  "max_rating": {
                    "max": {
                      "field": "stars"
                    }
                  },
                  "count": {
                    "value_count": {
                      "field": "business_id"
                    }
                  }
                }
              }
            }""", REVIEWS_INDEX_NAME, transformIndex);

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
        actual = (Number) ((List<?>) XContentMapValues.extractValue("hits.hits._source.variability_rating", searchResult)).get(0);
        assertEquals(0.0, actual.doubleValue(), 0.000001);
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

        String config = Strings.format("""
            {
              "source": {
                "index": "%s"
              },
              "dest": {
                "index": "%s"
              },
              "pivot": {
                "group_by": {
                  "every_2": {
                    "histogram": {
                      "interval": 2,
                      "field": "stars"
                    }
                  }
                },
                "aggregations": {
                  "common_users": {
                    "terms": {
                      "field": "user_id",
                      "size": 2
                    },
                    "aggs": {
                      "common_businesses": {
                        "terms": {
                          "field": "business_id",
                          "size": 2
                        }
                      }
                    }
                  },
                  "common_users_desc": {
                    "terms": {
                      "field": "user_id",
                      "size": 3,
                      "order": {
                        "_key": "desc"
                      }
                    }
                  },
                  "rare_users": {
                    "rare_terms": {
                      "field": "user_id"
                    }
                  }
                }
              }
            }""", REVIEWS_INDEX_NAME, transformIndex);

        createTransformRequest.setJsonEntity(config);
        Map<String, Object> createTransformResponse = entityAsMap(client().performRequest(createTransformRequest));
        assertThat(createTransformResponse.get("acknowledged"), equalTo(Boolean.TRUE));

        startAndWaitForTransform(transformId, transformIndex);
        assertTrue(indexExists(transformIndex));

        // we expect 3 documents as there shall be 5 unique star values and we are bucketing every 2 starting at 0
        Map<String, Object> indexStats = getAsMap(transformIndex + "/_stats");
        assertEquals(3, XContentMapValues.extractValue("_all.total.docs.count", indexStats));

        // get and check some term results
        Map<String, Object> searchResult = getAsOrderedMap(transformIndex + "/_search?q=every_2:2.0");

        assertEquals(1, XContentMapValues.extractValue("hits.total.value", searchResult));
        Map<String, Integer> commonUsers = (Map<String, Integer>) ((List<?>) XContentMapValues.extractValue(
            "hits.hits._source.common_users",
            searchResult
        )).get(0);
        assertThat(commonUsers, is(not(nullValue())));
        assertThat(
            commonUsers,
            equalTo(
                Map.of(
                    "user_10",
                    Map.of("common_businesses", Map.of("business_12", 6, "business_9", 4)),
                    "user_0",
                    Map.of("common_businesses", Map.of("business_0", 35))
                )
            )
        );
        Map<String, Integer> commonUsersDesc = (Map<String, Integer>) ((List<?>) XContentMapValues.extractValue(
            "hits.hits._source.common_users_desc",
            searchResult
        )).get(0);
        assertThat(commonUsersDesc, is(not(nullValue())));
        // 3 user names latest in lexicographic order (user_9, user_8, user_7) are selected properly and their order is preserved.
        assertThat(commonUsersDesc.keySet(), containsInRelativeOrder("user_9", "user_8", "user_7"));
        Map<String, Integer> rareUsers = (Map<String, Integer>) ((List<?>) XContentMapValues.extractValue(
            "hits.hits._source.rare_users",
            searchResult
        )).get(0);
        assertThat(rareUsers, is(not(nullValue())));
        assertThat(rareUsers, is(equalTo(Map.of("user_5", 1, "user_12", 1))));
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

        String config = Strings.format("""
            {
              "source": {
                "index": "%s"
              },
              "dest": {
                "index": "%s"
              },
              "pivot": {
                "group_by": {
                  "by_hr": {
                    "date_histogram": {
                      "fixed_interval": "1h",
                      "field": "timestamp"
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
            }""", indexName, transformIndex);

        createTransformRequest.setJsonEntity(config);

        Map<String, Object> createTransformResponse = entityAsMap(client().performRequest(createTransformRequest));
        assertThat(createTransformResponse.get("acknowledged"), equalTo(Boolean.TRUE));

        startAndWaitForTransform(transformId, transformIndex, BASIC_AUTH_VALUE_TRANSFORM_ADMIN_WITH_SOME_DATA_ACCESS);
        assertTrue(indexExists(transformIndex));

        Map<String, Object> indexStats = getAsMap(transformIndex + "/_stats");
        assertEquals(104, XContentMapValues.extractValue("_all.total.docs.count", indexStats));
        assertOnePivotValue(transformIndex + "/_search?q=by_hr:1484499600000", 4.0833333333);

    }

    // test that docs in same date bucket with a later date than the updated doc are not ignored by the transform.
    @SuppressWarnings("unchecked")
    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/98377")
    public void testContinuousDateHistogramPivot() throws Exception {
        String indexName = "continuous_reviews_date_histogram";

        // ingest timestamp used to allow grouped on timestamp field to remain the same on update
        Request createIngestPipeLine = new Request("PUT", "/_ingest/pipeline/es-timeadd");
        createIngestPipeLine.setJsonEntity("""
            {
              "processors":[
                    {
                       "set":{
                          "field":"_source.es_timestamp",
                          "value":"{{_ingest.timestamp}}"
                       }
                    }
                 ]
            }""");
        client().performRequest(createIngestPipeLine);

        Request setDefaultPipeline = new Request("PUT", indexName);
        setDefaultPipeline.setJsonEntity("""
            {
               "settings":{
                  "index.default_pipeline":"es-timeadd"
               }
            }""");
        client().performRequest(setDefaultPipeline);

        Request createIndex = new Request("PUT", indexName + "/_doc/1");
        createIndex.setJsonEntity("""
            {
                "user_id" : "user_1",
                "timestamp": "2023-07-24T17:10:00.000Z",
                "stars": 5
            }""");
        client().performRequest(createIndex);

        var putRequest = new Request("PUT", indexName + "/_doc/2");
        putRequest.setJsonEntity("""
            {
                "user_id" : "user_2",
                "timestamp": "2023-07-24T17:55:00.000Z",
                "stars": 5
            }""");
        client().performRequest(putRequest);

        String transformId = "continuous_date_histogram_pivot";
        String transformIndex = "pivot_reviews_via_date_continuous_histogram";
        setupDataAccessRole(DATA_ACCESS_ROLE, indexName, transformIndex);

        final Request createTransformRequest = createRequestWithAuth(
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
                  "field": "es_timestamp",
                  "delay": "1s"
                }
              },
              "pivot": {
                "group_by": {
                  "by_hr": {
                    "date_histogram": {
                      "fixed_interval": "1h",
                      "field": "timestamp"
                    }
                  }
                },
                "aggregations": {
                  "total_rating": {
                    "sum": {
                      "field": "stars"
                    }
                  }
                }
              }
            }""", indexName, transformIndex);

        createTransformRequest.setJsonEntity(config);
        var createTransformResponse = entityAsMap(client().performRequest(createTransformRequest));
        assertThat(createTransformResponse.get("acknowledged"), equalTo(Boolean.TRUE));

        startAndWaitForContinuousTransform(transformId, transformIndex, null);
        assertTrue(indexExists(transformIndex));

        // update stars field in first doc
        Request updateDoc = new Request("PUT", indexName + "/_doc/1");
        updateDoc.setJsonEntity("""
            {
                "user_id" : "user_1",
                "timestamp": "2023-07-24T17:10:00.000Z",
                "stars": 6
            }""");
        updateDoc.addParameter("refresh", "true");
        client().performRequest(updateDoc);

        waitForTransformCheckpoint(transformId, 2);
        stopTransform(transformId, false);
        refreshIndex(transformIndex);

        var searchResponse = getAsMap(transformIndex + "/_search");
        var hits = ((List<Map<String, Object>>) XContentMapValues.extractValue("hits.hits", searchResponse)).get(0);
        var totalStars = (double) XContentMapValues.extractValue("_source.total_rating", hits);
        assertEquals(11, totalStars, 0);
    }

    public void testPreviewTransform() throws Exception {
        testPreviewTransform("");
    }

    public void testPreviewTransformWithQuery() throws Exception {
        testPreviewTransform("""
            ,
            "query": {
              "range": {
                "timestamp": {
                  "gte": 123456789
                }
              }
            }""");
    }

    @SuppressWarnings("unchecked")
    private void testPreviewTransform(String queryJson) throws Exception {
        setupDataAccessRole(DATA_ACCESS_ROLE, REVIEWS_INDEX_NAME);
        final Request createPreviewRequest = createRequestWithAuth(
            "POST",
            getTransformEndpoint() + "_preview",
            BASIC_AUTH_VALUE_TRANSFORM_ADMIN_WITH_SOME_DATA_ACCESS
        );

        String config = Strings.format("""
            {
              "source": {
                "index": "%s"
                %s
              },
              "pivot": {
                "group_by": {
                  "user.id": {
                    "terms": {
                      "field": "user_id"
                    }
                  },
                  "by_day": {
                    "date_histogram": {
                      "fixed_interval": "1d",
                      "field": "timestamp"
                    }
                  }
                },
                "aggregations": {
                  "user.avg_rating": {
                    "avg": {
                      "field": "stars"
                    }
                  }
                }
              }
            }""", REVIEWS_INDEX_NAME, queryJson);

        createPreviewRequest.setJsonEntity(config);

        Map<String, Object> previewTransformResponse = entityAsMap(client().performRequest(createPreviewRequest));
        List<Map<String, Object>> preview = (List<Map<String, Object>>) previewTransformResponse.get("preview");
        // preview is limited to 100
        assertThat(preview.size(), equalTo(100));
        Set<String> expectedTopLevelFields = Set.of("user", "by_day");
        Set<String> expectedNestedFields = Set.of("id", "avg_rating");
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
        pipelineRequest.setJsonEntity(Strings.format("""
            {
              "description": "my pivot preview pipeline",
              "processors": [
                {
                  "set": {
                    "field": "pipeline_field",
                    "value": %s
                  }
                }
              ]
            }
            """, pipelineValue));
        client().performRequest(pipelineRequest);

        setupDataAccessRole(DATA_ACCESS_ROLE, REVIEWS_INDEX_NAME);
        final Request createPreviewRequest = createRequestWithAuth("POST", getTransformEndpoint() + "_preview", null);

        String config = Strings.format("""
            {
              "source": {
                "index": "%s"
              },
              "dest": {
                "pipeline": "%s"
              },
              "pivot": {
                "group_by": {
                  "user.id": {
                    "terms": {
                      "field": "user_id"
                    }
                  },
                  "by_day": {
                    "date_histogram": {
                      "fixed_interval": "1d",
                      "field": "timestamp"
                    }
                  }
                },
                "aggregations": {
                  "user.avg_rating": {
                    "avg": {
                      "field": "stars"
                    }
                  }
                }
              }
            }""", REVIEWS_INDEX_NAME, pipelineId);
        createPreviewRequest.setJsonEntity(config);

        Map<String, Object> previewTransformResponse = entityAsMap(client().performRequest(createPreviewRequest));
        List<Map<String, Object>> preview = (List<Map<String, Object>>) previewTransformResponse.get("preview");
        // preview is limited to 100
        assertThat(preview.size(), equalTo(100));
        Set<String> expectedTopLevelFields = Set.of("user", "by_day", "pipeline_field");
        Set<String> expectedNestedFields = Set.of("id", "avg_rating");
        preview.forEach(p -> {
            Set<String> keys = p.keySet();
            assertThat(keys, equalTo(expectedTopLevelFields));
            assertThat(p.get("pipeline_field"), equalTo(pipelineValue));
            Map<String, Object> nestedObj = (Map<String, Object>) p.get("user");
            keys = nestedObj.keySet();
            assertThat(keys, equalTo(expectedNestedFields));
        });
    }

    @SuppressWarnings("unchecked")
    public void testPreviewTransformWithPipelineScript() throws Exception {
        String pipelineId = "my-preview-pivot-pipeline-script";
        Request pipelineRequest = new Request("PUT", "/_ingest/pipeline/" + pipelineId);
        pipelineRequest.setJsonEntity("""
            {
              "description": "my pivot preview pipeline",
              "processors": [
                {
                  "script": {
                    "lang": "painless",
                    "source": "ctx._id = ctx['non']['existing'];"
                  }
                }
              ]
            }
            """);
        client().performRequest(pipelineRequest);

        setupDataAccessRole(DATA_ACCESS_ROLE, REVIEWS_INDEX_NAME);
        final Request createPreviewRequest = createRequestWithAuth("POST", getTransformEndpoint() + "_preview", null);
        createPreviewRequest.setOptions(RequestOptions.DEFAULT.toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE));

        String config = Strings.format("""
            {
              "source": {
                "index": "%s"
              },
              "dest": {
                "pipeline": "%s"
              },
              "pivot": {
                "group_by": {
                  "user.id": {
                    "terms": {
                      "field": "user_id"
                    }
                  },
                  "by_day": {
                    "date_histogram": {
                      "fixed_interval": "1d",
                      "field": "timestamp"
                    }
                  }
                },
                "aggregations": {
                  "user.avg_rating": {
                    "avg": {
                      "field": "stars"
                    }
                  }
                }
              }
            }""", REVIEWS_INDEX_NAME, pipelineId);
        createPreviewRequest.setJsonEntity(config);

        Response createPreviewResponse = client().performRequest(createPreviewRequest);
        Map<String, Object> previewTransformResponse = entityAsMap(createPreviewResponse);
        List<Map<String, Object>> preview = (List<Map<String, Object>>) previewTransformResponse.get("preview");
        // Pipeline failed for all the docs so the preview is empty
        assertThat(preview, is(empty()));
        assertThat(createPreviewResponse.getWarnings(), hasSize(1));
        assertThat(
            createPreviewResponse.getWarnings().get(0),
            allOf(containsString("Pipeline returned 100 errors, first error:"), containsString("type=script_exception"))
        );
    }

    public void testPreviewTransformWithDateHistogramOffset() throws Exception {
        assertThat(
            previewWithOffset("+2d"),
            contains("2017-01-04T00:00:00.000Z", "2017-01-11T00:00:00.000Z", "2017-01-18T00:00:00.000Z", "2017-01-25T00:00:00.000Z")
        );
        assertThat(previewWithOffset("+1d"), contains("2017-01-10T00:00:00.000Z", "2017-01-17T00:00:00.000Z", "2017-01-24T00:00:00.000Z"));
        assertThat(
            previewWithOffset("0"),
            contains("2017-01-09T00:00:00.000Z", "2017-01-16T00:00:00.000Z", "2017-01-23T00:00:00.000Z", "2017-01-30T00:00:00.000Z")
        );
        assertThat(
            previewWithOffset("-1d"),
            contains("2017-01-08T00:00:00.000Z", "2017-01-15T00:00:00.000Z", "2017-01-22T00:00:00.000Z", "2017-01-29T00:00:00.000Z")
        );
        assertThat(
            previewWithOffset("-2d"),
            contains("2017-01-07T00:00:00.000Z", "2017-01-14T00:00:00.000Z", "2017-01-21T00:00:00.000Z", "2017-01-28T00:00:00.000Z")
        );
    }

    @SuppressWarnings("unchecked")
    private List<String> previewWithOffset(String offset) throws IOException {
        setupDataAccessRole(DATA_ACCESS_ROLE, REVIEWS_INDEX_NAME);
        final Request createPreviewRequest = createRequestWithAuth("POST", getTransformEndpoint() + "_preview", null);
        createPreviewRequest.setOptions(RequestOptions.DEFAULT.toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE));

        String config = Strings.format("""
            {
              "source": {
                "index": "%s"
              },
              "pivot": {
                "group_by": {
                  "by_week": {
                    "date_histogram": {
                      "calendar_interval": "1w",
                      "field": "timestamp",
                      "offset": "%s"
                    }
                  }
                },
                "aggregations": {
                  "user.avg_rating": {
                    "avg": {
                      "field": "stars"
                    }
                  }
                }
              },
              "settings": {
                "deduce_mappings": %s
              }
            }""", REVIEWS_INDEX_NAME, offset, randomBoolean());
        createPreviewRequest.setJsonEntity(config);

        Map<String, Object> previewTransformResponse = entityAsMap(client().performRequest(createPreviewRequest));
        List<Map<String, Object>> preview = (List<Map<String, Object>>) previewTransformResponse.get("preview");
        return preview.stream().map(p -> (String) p.get("by_week")).toList();
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

        String config = Strings.format("""
            {
              "source": {
                "index": "%s"
              },
              "dest": {
                "index": "%s"
              },
              "pivot": {
                "group_by": {
                  "by_day": {
                    "date_histogram": {
                      "fixed_interval": "1d",
                      "field": "timestamp"
                    }
                  }
                },
                "aggs": {
                  "avg_rating": {
                    "avg": {
                      "field": "stars"
                    }
                  },
                  "timestamp": {
                    "max": {
                      "field": "timestamp"
                    }
                  }
                }
              }
            }""", REVIEWS_INDEX_NAME, transformIndex);

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

        String config = Strings.format("""
            {
              "source": {
                "index": "%s"
              },
              "dest": {
                "index": "%s"
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
                  },
                  "squared_sum": {
                    "scripted_metric": {
                      "init_script": "state.reviews_sqrd = []",
                      "map_script": "state.reviews_sqrd.add(doc.stars.value * doc.stars.value)",
                      "combine_script": "state.reviews_sqrd",
                      "reduce_script": "def sum = 0.0; for(l in states){ for(a in l) { sum += a}} return sum"
                    }
                  }
                }
              }
            }""", REVIEWS_INDEX_NAME, transformIndex);

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

        String config = Strings.format("""
            {
              "source": {
                "index": "%s"
              },
              "dest": {
                "index": "%s"
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
                  },
                  "avg_rating_again": {
                    "bucket_script": {
                      "buckets_path": {
                        "param_1": "avg_rating"
                      },
                      "script": "return params.param_1"
                    }
                  }
                }
              }
            }""", REVIEWS_INDEX_NAME, transformIndex);

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
        String indexName = "reviews_geo_bounds";

        // gh#71874 regression test: create some sparse data
        createReviewsIndex(indexName, 1000, 27, "date", false, 5, "location");

        setupDataAccessRole(DATA_ACCESS_ROLE, indexName, transformIndex);

        final Request createTransformRequest = createRequestWithAuth(
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
                  },
                  "boundary": {
                    "geo_bounds": {
                      "field": "location"
                    }
                  }
                }
              }
            }
            """, indexName, transformIndex);

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
        assertEquals(-76.0, coordinates.get(1), 0.000001);
        assertEquals(-161.0, coordinates.get(0), 0.000001);
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

        String config = Strings.format("""
            {
              "source": {
                "index": "%s"
              },
              "dest": {
                "index": "%s"
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
                  },
                  "location": {
                    "geo_centroid": {
                      "field": "location"
                    }
                  }
                }
              }
            }""", REVIEWS_INDEX_NAME, transformIndex);

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
        assertEquals(-76.0, Double.valueOf(latlon[0]), 0.000001);
        assertEquals(-161.0, Double.valueOf(latlon[1]), 0.000001);
    }

    @SuppressWarnings("unchecked")
    public void testPivotWithGeoLineAgg() throws Exception {
        String transformId = "geo_line_pivot";
        String transformIndex = "geo_line_pivot_reviews";
        setupDataAccessRole(DATA_ACCESS_ROLE, REVIEWS_INDEX_NAME, transformIndex);

        final Request createTransformRequest = createRequestWithAuth(
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
                  },
                  "location": {
                    "geo_line": {
                      "point": {
                        "field": "location"
                      },
                      "sort": {
                        "field": "timestamp"
                      }
                    }
                  }
                }
              }
            }""", REVIEWS_INDEX_NAME, transformIndex);

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
        Map<String, Object> actualString = (Map<String, Object>) ((List<?>) XContentMapValues.extractValue(
            "hits.hits._source.location",
            searchResult
        )).get(0);
        assertThat(actualString, hasEntry("type", "LineString"));
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

        String config = Strings.format("""
            {
              "source": {
                "index": "%s"
              },
              "dest": {
                "index": "%s"
              },
              "pivot": {
                "group_by": {
                  "tile": {
                    "geotile_grid": {
                      "field": "location",
                      "precision": 12
                    }
                  }
                },
                "aggregations": {
                  "avg_rating": {
                    "avg": {
                      "field": "stars"
                    }
                  },
                  "boundary": {
                    "geo_bounds": {
                      "field": "location"
                    }
                  }
                }
              }
            }""", REVIEWS_INDEX_NAME, transformIndex);

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

        String config = Strings.format("""
            {
              "source": {
                "index": "%s"
              },
              "dest": {
                "index": "%s"
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
                    "weighted_avg": {
                      "value": {
                        "field": "stars"
                      },
                      "weight": {
                        "field": "stars"
                      }
                    }
                  }
                }
              }
            }""", REVIEWS_INDEX_NAME, transformIndex);

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

    public void testPivotWithTopMetrics() throws Exception {
        String transformId = "top_metrics_transform";
        String transformIndex = "top_metrics_pivot_reviews";
        setupDataAccessRole(DATA_ACCESS_ROLE, REVIEWS_INDEX_NAME, transformIndex);

        final Request createTransformRequest = createRequestWithAuth(
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
              "pivot": {
                "group_by": {
                  "reviewer": {
                    "terms": {
                      "field": "user_id"
                    }
                  }
                },
                "aggregations": {
                  "top_business": {
                    "top_metrics": {
                      "metrics": {
                        "field": "business_id"
                      },
                      "sort": {
                        "timestamp": "desc"
                      }
                    }
                  }
                }
              }
            }""", REVIEWS_INDEX_NAME, transformIndex);

        createTransformRequest.setJsonEntity(config);
        Map<String, Object> createTransformResponse = entityAsMap(client().performRequest(createTransformRequest));
        assertThat(createTransformResponse.get("acknowledged"), equalTo(Boolean.TRUE));

        startAndWaitForTransform(transformId, transformIndex, BASIC_AUTH_VALUE_TRANSFORM_ADMIN_WITH_SOME_DATA_ACCESS);
        assertTrue(indexExists(transformIndex));

        Map<String, Object> searchResult = getAsMap(transformIndex + "/_search?q=reviewer:user_4");
        assertEquals(1, XContentMapValues.extractValue("hits.total.value", searchResult));
        String actual = (String) ((List<?>) XContentMapValues.extractValue("hits.hits._source.top_business.business_id", searchResult)).get(
            0
        );
        assertEquals("business_9", actual);

        searchResult = getAsMap(transformIndex + "/_search?q=reviewer:user_1");
        assertEquals(1, XContentMapValues.extractValue("hits.total.value", searchResult));
        actual = (String) ((List<?>) XContentMapValues.extractValue("hits.hits._source.top_business.business_id", searchResult)).get(0);
        assertEquals("business_3", actual);
    }

    @SuppressWarnings(value = "unchecked")
    public void testPivotWithExtendedStats() throws Exception {
        var transformId = "extended_stats_transform";
        var transformIndex = "extended_stats_pivot_reviews";
        setupDataAccessRole(DATA_ACCESS_ROLE, REVIEWS_INDEX_NAME, transformIndex);

        var createTransformRequest = createRequestWithAuth(
            "PUT",
            getTransformEndpoint() + transformId,
            BASIC_AUTH_VALUE_TRANSFORM_ADMIN_WITH_SOME_DATA_ACCESS
        );

        var config = Strings.format("""
            {
              "source": {
                "index": "%s"
              },
              "dest": {
                "index": "%s"
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
                  "stars": {
                    "extended_stats": {
                      "field": "stars"
                    }
                  }
                }
              }
            }""", REVIEWS_INDEX_NAME, transformIndex);

        createTransformRequest.setJsonEntity(config);
        var createTransformResponse = entityAsMap(client().performRequest(createTransformRequest));
        assertThat(createTransformResponse.get("acknowledged"), equalTo(Boolean.TRUE));

        startAndWaitForTransform(transformId, transformIndex, BASIC_AUTH_VALUE_TRANSFORM_ADMIN_WITH_SOME_DATA_ACCESS);
        assertTrue(indexExists(transformIndex));

        var searchResult = getAsMap(transformIndex + "/_search?q=reviewer:user_4");
        assertEquals(1, XContentMapValues.extractValue("hits.total.value", searchResult));
        var stdDevMap = (Map<String, Object>) ((List<?>) XContentMapValues.extractValue("hits.hits._source.stars", searchResult)).get(0);
        assertThat(stdDevMap.get("count"), equalTo(41));
        assertThat(
            stdDevMap,
            allOf(
                hasEntry("sum", 159.0),
                hasEntry("min", 1.0),
                hasEntry("max", 5.0),
                hasEntry("avg", 3.8780487804878048),
                hasEntry("sum_of_squares", 711.0),
                hasEntry("variance", 2.3022010707911953),
                hasEntry("variance_population", 2.3022010707911953),
                hasEntry("variance_sampling", 2.3597560975609753),
                hasEntry("std_deviation", 1.5173005868288574),
                hasEntry("std_deviation_sampling", 1.5361497640402693),
                hasEntry("std_deviation_population", 1.5173005868288574)
            )
        );
        assertThat(
            (Map<String, ?>) stdDevMap.get("std_deviation_bounds"),
            allOf(
                hasEntry("upper", 6.91264995414552),
                hasEntry("lower", 0.84344760683009),
                hasEntry("upper_population", 6.91264995414552),
                hasEntry("lower_population", 0.84344760683009),
                hasEntry("upper_sampling", 6.950348308568343),
                hasEntry("lower_sampling", 0.8057492524072662)
            )
        );
    }

    public void testPivotWithBoxplot() throws Exception {
        String transformId = "boxplot_transform";
        String transformIndex = "boxplot_pivot_reviews";
        setupDataAccessRole(DATA_ACCESS_ROLE, REVIEWS_INDEX_NAME, transformIndex);

        final Request createTransformRequest = createRequestWithAuth(
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
              "pivot": {
                "group_by": {
                  "reviewer": {
                    "terms": {
                      "field": "user_id"
                    }
                  }
                },
                "aggregations": {
                  "stars_boxplot": {
                    "boxplot": {
                       "field": "stars"
                    }
                  }
                }
              }
            }""", REVIEWS_INDEX_NAME, transformIndex);

        createTransformRequest.setJsonEntity(config);
        Map<String, Object> createTransformResponse = entityAsMap(client().performRequest(createTransformRequest));
        assertThat(createTransformResponse.get("acknowledged"), equalTo(Boolean.TRUE));

        startAndWaitForTransform(transformId, transformIndex, BASIC_AUTH_VALUE_TRANSFORM_ADMIN_WITH_SOME_DATA_ACCESS);
        assertTrue(indexExists(transformIndex));

        Map<String, Object> searchResult = getAsMap(transformIndex + "/_search?q=reviewer:user_4");
        assertEquals(1, XContentMapValues.extractValue("hits.total.value", searchResult));
        assertThat(
            ((List<?>) XContentMapValues.extractValue("hits.hits._source.stars_boxplot.min", searchResult)).get(0),
            is(equalTo(1.0))
        );
        assertThat(
            ((List<?>) XContentMapValues.extractValue("hits.hits._source.stars_boxplot.max", searchResult)).get(0),
            is(equalTo(5.0))
        );
        assertThat(((List<?>) XContentMapValues.extractValue("hits.hits._source.stars_boxplot.q1", searchResult)).get(0), is(equalTo(3.0)));
        assertThat(((List<?>) XContentMapValues.extractValue("hits.hits._source.stars_boxplot.q2", searchResult)).get(0), is(equalTo(5.0)));
        assertThat(((List<?>) XContentMapValues.extractValue("hits.hits._source.stars_boxplot.q3", searchResult)).get(0), is(equalTo(5.0)));
        assertThat(
            ((List<?>) XContentMapValues.extractValue("hits.hits._source.stars_boxplot.lower", searchResult)).get(0),
            is(equalTo(1.0))
        );
        assertThat(
            ((List<?>) XContentMapValues.extractValue("hits.hits._source.stars_boxplot.upper", searchResult)).get(0),
            is(equalTo(5.0))
        );

        searchResult = getAsMap(transformIndex + "/_search?q=reviewer:user_1");
        assertEquals(1, XContentMapValues.extractValue("hits.total.value", searchResult));
        assertThat(
            ((List<?>) XContentMapValues.extractValue("hits.hits._source.stars_boxplot.min", searchResult)).get(0),
            is(equalTo(1.0))
        );
        assertThat(
            ((List<?>) XContentMapValues.extractValue("hits.hits._source.stars_boxplot.max", searchResult)).get(0),
            is(equalTo(5.0))
        );
        assertThat(((List<?>) XContentMapValues.extractValue("hits.hits._source.stars_boxplot.q1", searchResult)).get(0), is(equalTo(3.0)));
        assertThat(((List<?>) XContentMapValues.extractValue("hits.hits._source.stars_boxplot.q2", searchResult)).get(0), is(equalTo(5.0)));
        assertThat(((List<?>) XContentMapValues.extractValue("hits.hits._source.stars_boxplot.q3", searchResult)).get(0), is(equalTo(5.0)));
        assertThat(
            ((List<?>) XContentMapValues.extractValue("hits.hits._source.stars_boxplot.lower", searchResult)).get(0),
            is(equalTo(1.0))
        );
        assertThat(
            ((List<?>) XContentMapValues.extractValue("hits.hits._source.stars_boxplot.upper", searchResult)).get(0),
            is(equalTo(5.0))
        );

    }

    public void testPivotWithAggregateMetricDouble() throws Exception {
        String transformId = "aggregate_metric_double_transform";
        String transformIndex = "aggregate_metric_double_pivot_reviews";
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
              "pivot": {
                "group_by": {
                  "reviewer": {
                    "terms": {
                      "field": "user_id"
                    }
                  }
                },
                "aggregations": {
                  "stats_min": {
                    "min": {
                      "field": "%s"
                    }
                  },
                  "stats_max": {
                    "max": {
                      "field": "%s"
                    }
                  },
                  "stats_sum": {
                    "sum": {
                      "field": "%s"
                    }
                  }
                }
              }
            }""", REVIEWS_INDEX_NAME, transformIndex, statsField, statsField, statsField);

        final Request createPreviewRequest = createRequestWithAuth("POST", getTransformEndpoint() + "_preview", null);
        createPreviewRequest.setJsonEntity(config);
        Map<String, Object> previewTransformResponse = entityAsMap(client().performRequest(createPreviewRequest));
        @SuppressWarnings("unchecked")
        Map<String, Object> mappingsProperties = (Map<String, Object>) XContentMapValues.extractValue(
            "generated_dest_index.mappings.properties",
            previewTransformResponse
        );
        assertThat(XContentMapValues.extractValue("stats_min.type", mappingsProperties), is(equalTo("double")));
        assertThat(XContentMapValues.extractValue("stats_max.type", mappingsProperties), is(equalTo("double")));
        assertThat(XContentMapValues.extractValue("stats_sum.type", mappingsProperties), is(equalTo("double")));

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

        Map<String, Object> searchResult = getAsMap(transformIndex + "/_search?q=reviewer:user_4");
        assertEquals(1, XContentMapValues.extractValue("hits.total.value", searchResult));
        assertThat(((List<?>) XContentMapValues.extractValue("hits.hits._source.stats_min", searchResult)).get(0), is(equalTo(0.0)));
        assertThat(((List<?>) XContentMapValues.extractValue("hits.hits._source.stats_max", searchResult)).get(0), is(equalTo(6.0)));
        assertThat(((List<?>) XContentMapValues.extractValue("hits.hits._source.stats_sum", searchResult)).get(0), is(equalTo(1590.0)));

        searchResult = getAsMap(transformIndex + "/_search?q=reviewer:user_1");
        assertEquals(1, XContentMapValues.extractValue("hits.total.value", searchResult));
        assertThat(((List<?>) XContentMapValues.extractValue("hits.hits._source.stats_min", searchResult)).get(0), is(equalTo(0.0)));
        assertThat(((List<?>) XContentMapValues.extractValue("hits.hits._source.stats_max", searchResult)).get(0), is(equalTo(6.0)));
        assertThat(((List<?>) XContentMapValues.extractValue("hits.hits._source.stats_sum", searchResult)).get(0), is(equalTo(2020.0)));
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

        String config = Strings.format("""
            {
              "source": {
                "index": "%s"
              },
              "dest": {
                "index": "%s"
              },
              "pivot": {
                "group_by": {
                  "user.id": {
                    "terms": {
                      "field": "user_id"
                    }
                  },
                  "business.id": {
                    "terms": {
                      "field": "business_id"
                    }
                  },
                  "every_star": {
                    "histogram": {
                      "field": "stars",
                      "interval": 1
                    }
                  },
                  "every_two_star": {
                    "histogram": {
                      "field": "stars",
                      "interval": 2
                    }
                  },
                  "by_second": {
                    "date_histogram": {
                      "fixed_interval": "1s",
                      "field": "timestamp"
                    }
                  },
                  "by_day": {
                    "date_histogram": {
                      "fixed_interval": "1d",
                      "field": "timestamp"
                    }
                  },
                  "by_minute": {
                    "date_histogram": {
                      "fixed_interval": "1m",
                      "field": "timestamp"
                    }
                  }
                },
                "aggregations": {
                  "user.avg_rating": {
                    "avg": {
                      "field": "stars"
                    }
                  }
                }
              },
              "settings": {
                "max_page_search_size": 10
              }
            }""", REVIEWS_INDEX_NAME, transformIndex);
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
        updateLoggingLevels.setJsonEntity("""
            {
              "persistent": {
                "logger.org.elasticsearch.xpack.core.indexing.AsyncTwoPhaseIndexer": "trace",
                "logger.org.elasticsearch.xpack.transform": "trace"
              }
            }""");
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
            }""", indexName, transformIndex);
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
              "pivot": {
                "group_by": {
                  "id": {
                    "terms": {
                      "field": "id"
                    }
                  }
                },
                "aggregations": {
                  "avg_rating": {
                    "avg": {
                      "field": "rating"
                    }
                  }
                }
              }
            }""", indexName, transformIndex);
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
            bulk.append(Strings.format("""
                {"index":{"_index":"%s"}}
                {"id":"id_%s","rating":%s,"timestamp":"%s"}
                """, indexName, i % 5, 7, nanoResolutionTimeStamp));
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
        String config = Strings.format("""
            {
              "source": {
                "index": "%s"
              },
              "dest": {
                "index": "%s"
              },
              "frequency": "1s",
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
                  },
                  "p": {
                    "percentiles": {
                      "field": "stars",
                      "percents": [ 5, 50, 90, 99.9 ]
                    }
                  }
                }
              }
            }""", REVIEWS_INDEX_NAME, transformIndex);
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

    public void testPivotWithRanges() throws Exception {
        String transformId = "range_pivot";
        String transformIndex = "range_pivot_reviews";
        boolean keyed = randomBoolean();
        setupDataAccessRole(DATA_ACCESS_ROLE, REVIEWS_INDEX_NAME, transformIndex);
        final Request createTransformRequest = createRequestWithAuth(
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
                  },
                  "ranges": {
                    "range": {
                      "field": "stars",
                      "keyed": %s,
                      "ranges": [ { "to": 2 }, { "from": 2, "to": 3.99 }, { "from": 4 } ]
                    }
                  },
                  "ranges-avg": {
                    "range": {
                      "field": "stars",
                      "keyed": %s,
                      "ranges": [ { "to": 2 }, { "from": 2, "to": 3.99 }, { "from": 4 } ]
                    },
                    "aggs": { "avg_stars": { "avg": { "field": "stars" } } }
                  }
                }
              }
            }""", REVIEWS_INDEX_NAME, transformIndex, keyed, keyed);
        createTransformRequest.setJsonEntity(config);
        Map<String, Object> createTransformResponse = entityAsMap(client().performRequest(createTransformRequest));
        assertThat(createTransformResponse.get("acknowledged"), equalTo(Boolean.TRUE));

        startAndWaitForTransform(transformId, transformIndex);
        assertTrue(indexExists(transformIndex));

        // check destination index mappings
        Map<String, Object> mappingsResult = getAsMap(transformIndex + "/_mapping");
        assertThat(
            XContentMapValues.extractValue("range_pivot_reviews.mappings.properties.ranges.properties", mappingsResult),
            is(equalTo(Map.of("4-*", Map.of("type", "long"), "2-3_99", Map.of("type", "long"), "*-2", Map.of("type", "long"))))
        );
        assertThat(
            XContentMapValues.extractValue("range_pivot_reviews.mappings.properties.ranges-avg.properties", mappingsResult),
            is(
                equalTo(
                    Map.of(
                        "4-*",
                        Map.of("properties", Map.of("avg_stars", Map.of("type", "double"))),
                        "2-3_99",
                        Map.of("properties", Map.of("avg_stars", Map.of("type", "double"))),
                        "*-2",
                        Map.of("properties", Map.of("avg_stars", Map.of("type", "double")))
                    )
                )
            )
        );

        // get and check some users
        Map<String, Object> searchResult = getAsMap(transformIndex + "/_search?q=reviewer:user_11");
        assertEquals(1, XContentMapValues.extractValue("hits.total.value", searchResult));
        Number actual = (Number) ((List<?>) XContentMapValues.extractValue("hits.hits._source.ranges.*-2", searchResult)).get(0);
        assertEquals(5, actual.longValue());
        actual = (Number) ((List<?>) XContentMapValues.extractValue("hits.hits._source.ranges.2-3_99", searchResult)).get(0);
        assertEquals(2, actual.longValue());
        actual = (Number) ((List<?>) XContentMapValues.extractValue("hits.hits._source.ranges.4-*", searchResult)).get(0);
        assertEquals(19, actual.longValue());

        actual = (Number) ((List<?>) XContentMapValues.extractValue("hits.hits._source.ranges-avg.*-2.avg_stars", searchResult)).get(0);
        assertEquals(1.0, actual.doubleValue(), 1E-6);
        actual = (Number) ((List<?>) XContentMapValues.extractValue("hits.hits._source.ranges-avg.2-3_99.avg_stars", searchResult)).get(0);
        assertEquals(3.0, actual.doubleValue(), 1E-6);
        actual = (Number) ((List<?>) XContentMapValues.extractValue("hits.hits._source.ranges-avg.4-*.avg_stars", searchResult)).get(0);
        assertEquals(4.6842105, actual.doubleValue(), 1E-6);
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
        String config = Strings.format("""
            {
              "source": {
                "index": "%s"
              },
              "dest": {
                "index": "%s"
              },
              "frequency": "1s",
              "pivot": {
                "group_by": {
                  "reviewer": {
                    "terms": {
                      "field": "user_id"
                    }
                  }
                },
                "aggregations": {
                  "top_ratings": {
                    "filter": {
                      "range": {
                        "stars": {
                          "gte": 4
                        }
                      }
                    }
                  },
                  "top_ratings_detail": {
                    "filter": {
                      "range": {
                        "stars": {
                          "gte": 4
                        }
                      }
                    },
                    "aggregations": {
                      "unique_count": {
                        "cardinality": {
                          "field": "business_id"
                        }
                      },
                      "max": {
                        "max": {
                          "field": "stars"
                        }
                      },
                      "min": {
                        "min": {
                          "field": "stars"
                        }
                      }
                    }
                  }
                }
              }
            }""", REVIEWS_INDEX_NAME, transformIndex);

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

    @SuppressWarnings("unchecked")
    public void testExportAndImport() throws Exception {
        String transformId = "export-transform";
        String transformIndex = "export_reviews";
        setupDataAccessRole(DATA_ACCESS_ROLE, REVIEWS_INDEX_NAME, transformIndex);

        createPivotReviewsTransform(transformId, transformIndex, null, null, BASIC_AUTH_VALUE_TRANSFORM_ADMIN_WITH_SOME_DATA_ACCESS);

        Response response = adminClient().performRequest(
            new Request("GET", getTransformEndpoint() + transformId + "?exclude_generated=true")
        );
        Map<String, Object> storedConfig = ((List<Map<String, Object>>) XContentMapValues.extractValue("transforms", entityAsMap(response)))
            .get(0);
        storedConfig.remove("id");
        try (XContentBuilder builder = jsonBuilder()) {
            builder.map(storedConfig);
            Request putTransform = new Request("PUT", getTransformEndpoint() + transformId + "-import");
            putTransform.setJsonEntity(Strings.toString(builder));
            adminClient().performRequest(putTransform);
        }

        response = adminClient().performRequest(
            new Request("GET", getTransformEndpoint() + transformId + "-import" + "?exclude_generated=true")
        );
        Map<String, Object> importConfig = ((List<Map<String, Object>>) XContentMapValues.extractValue("transforms", entityAsMap(response)))
            .get(0);
        importConfig.remove("id");
        assertThat(storedConfig, equalTo(importConfig));
    }

    public void testPivotWithDeprecatedMaxPageSearchSize() throws Exception {
        String transformId = "deprecated_settings_pivot";
        String transformIndex = "deprecated_settings_pivot_index";
        setupDataAccessRole(DATA_ACCESS_ROLE, REVIEWS_INDEX_NAME, transformIndex);

        final Request createTransformRequest = createRequestWithAuth(
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
              "pivot": {
                "group_by": {
                  "every_2": {
                    "histogram": {
                      "interval": 2,
                      "field": "stars"
                    }
                  }
                },
                "aggregations": {
                  "avg_rating": {
                    "avg": {
                      "field": "stars"
                    }
                  }
                },
                "max_page_search_size": 1234
              }
            }""", REVIEWS_INDEX_NAME, transformIndex);

        createTransformRequest.setJsonEntity(config);
        Map<String, Object> createTransformResponse = entityAsMap(client().performRequest(createTransformRequest));
        assertThat(createTransformResponse.get("acknowledged"), equalTo(Boolean.TRUE));

        Map<String, Object> transform = getTransformConfig(transformId, BASIC_AUTH_VALUE_TRANSFORM_ADMIN_WITH_SOME_DATA_ACCESS);
        assertNull(XContentMapValues.extractValue("pivot.max_page_search_size", transform));
        assertThat(XContentMapValues.extractValue("settings.max_page_search_size", transform), equalTo(1234));
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
            bulk.append(Strings.format("""
                {"index":{"_index":"%s"}}
                {"id":"id_%s","rating":%s,"timestamp":"2020-01-27T01:59:00%sZ"}
                """, indexName, i % 10, i % 7, randomNanos));

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
