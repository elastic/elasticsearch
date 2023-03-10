/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.integration;

import org.apache.http.HttpHost;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.common.settings.Settings;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class TransformDeleteIT extends TransformRestTestCase {

    private static final String TEST_USER_NAME = "transform_user";
    private static final String TEST_ADMIN_USER_NAME_1 = "transform_admin_1";
    private static final String BASIC_AUTH_VALUE_TRANSFORM_ADMIN_1 = basicAuthHeaderValue(
        TEST_ADMIN_USER_NAME_1,
        TEST_PASSWORD_SECURE_STRING
    );
    private static final String DATA_ACCESS_ROLE = "test_data_access";

    private static boolean indicesCreated = false;

    // preserve indices in order to reuse source indices in several test cases
    @Override
    protected boolean preserveIndicesUponCompletion() {
        return true;
    }

    @Override
    protected boolean enableWarningsCheck() {
        return false;
    }

    @Override
    protected RestClient buildClient(Settings settings, HttpHost[] hosts) throws IOException {
        RestClientBuilder builder = RestClient.builder(hosts);
        configureClient(builder, settings);
        builder.setStrictDeprecationMode(false);
        return builder.build();
    }

    @Before
    public void createIndexes() throws IOException {
        setupDataAccessRole(DATA_ACCESS_ROLE, REVIEWS_INDEX_NAME);
        setupUser(TEST_USER_NAME, Arrays.asList("transform_user", DATA_ACCESS_ROLE));
        setupUser(TEST_ADMIN_USER_NAME_1, Arrays.asList("transform_admin", DATA_ACCESS_ROLE));

        // it's not possible to run it as @BeforeClass as clients aren't initialized then, so we need this little hack
        if (indicesCreated) {
            return;
        }

        createReviewsIndex();
        indicesCreated = true;
    }

    public void testDeleteDoesNotDeleteDestinationIndexByDefault() throws Exception {
        String transformId = "transform-1";
        String transformDest = transformId + "_idx";
        setupDataAccessRole(DATA_ACCESS_ROLE, REVIEWS_INDEX_NAME, transformDest);

        createTransform(transformId, transformDest);
        assertFalse(indexExists(transformDest));

        startTransform(transformId);
        waitForTransformCheckpoint(transformId, 1);
        stopTransform(transformId, false);
        assertTrue(indexExists(transformDest));

        deleteTransform(transformId);
        assertTrue(indexExists(transformDest));
    }

    public void testDeleteWithParamDeletesAutoCreatedDestinationIndex() throws Exception {
        String transformId = "transform-2";
        String transformDest = transformId + "_idx";
        setupDataAccessRole(DATA_ACCESS_ROLE, REVIEWS_INDEX_NAME, transformDest);

        createTransform(transformId, transformDest);
        assertFalse(indexExists(transformDest));

        startTransform(transformId);
        waitForTransformCheckpoint(transformId, 1);

        stopTransform(transformId, false);
        assertTrue(indexExists(transformDest));

        deleteTransform(transformId, true);
        assertFalse(indexExists(transformDest));
    }

    public void testDeleteWithParamDeletesManuallyCreatedDestinationIndex() throws Exception {
        String transformId = "transform-3";
        String transformDest = transformId + "_idx";
        setupDataAccessRole(DATA_ACCESS_ROLE, REVIEWS_INDEX_NAME, transformDest);

        createIndex(transformDest);
        assertTrue(indexExists(transformDest));

        createTransform(transformId, transformDest);

        startTransform(transformId);
        waitForTransformCheckpoint(transformId, 1);

        stopTransform(transformId, false);
        assertTrue(indexExists(transformDest));

        deleteTransform(transformId, true);
        assertFalse(indexExists(transformDest));
    }

    public void testDeleteWithParamDoesNotDeleteAlias() throws Exception {
        String transformId = "transform-4";
        String transformDest = transformId + "_idx";
        String transformDestAlias = transformId + "_alias";
        setupDataAccessRole(DATA_ACCESS_ROLE, REVIEWS_INDEX_NAME, transformDest, transformDestAlias);

        createIndex(transformDest, null, null, "\"" + transformDestAlias + "\": { \"is_write_index\": true }");
        assertTrue(indexExists(transformDest));
        assertTrue(indexExists(transformDestAlias));

        createTransform(transformId, transformDestAlias);

        startTransform(transformId);
        waitForTransformCheckpoint(transformId, 1);

        stopTransform(transformId, false);
        assertTrue(indexExists(transformDest));

        ResponseException e = expectThrows(ResponseException.class, () -> deleteTransform(transformId, true));
        assertThat(
            e.getMessage(),
            containsString(
                String.format(
                    Locale.ROOT,
                    "The provided expression [%s] matches an alias, specify the corresponding concrete indices instead.",
                    transformDestAlias
                )
            )
        );
    }

    private void createTransform(String transformId, String destIndex) throws IOException {
        final Request createTransformRequest = createRequestWithAuth(
            "PUT",
            getTransformEndpoint() + transformId,
            BASIC_AUTH_VALUE_TRANSFORM_ADMIN_1
        );
        String config = String.format(Locale.ROOT, """
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
            }""", destIndex, REVIEWS_INDEX_NAME);
        createTransformRequest.setJsonEntity(config);
        Map<String, Object> createTransformResponse = entityAsMap(client().performRequest(createTransformRequest));
        assertThat(createTransformResponse.get("acknowledged"), equalTo(Boolean.TRUE));
    }
}
