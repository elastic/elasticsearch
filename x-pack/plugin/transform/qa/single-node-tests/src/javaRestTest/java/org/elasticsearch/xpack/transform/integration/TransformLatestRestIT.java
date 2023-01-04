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
}
