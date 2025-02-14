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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
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
        String transformDestAlias = transformId + "_alias";
        setupDataAccessRole(DATA_ACCESS_ROLE, REVIEWS_INDEX_NAME, transformDest, transformDestAlias);

        createTransform(transformId, transformDest, transformDestAlias);
        assertFalse(indexExists(transformDest));
        assertFalse(aliasExists(transformDestAlias));

        startTransform(transformId);
        waitForTransformCheckpoint(transformId, 1);

        stopTransform(transformId, false);
        assertTrue(indexExists(transformDest));
        assertTrue(aliasExists(transformDestAlias));

        deleteTransform(transformId);
        assertTrue(indexExists(transformDest));
        assertTrue(aliasExists(transformDestAlias));
    }

    public void testDeleteWithParamDeletesAutoCreatedDestinationIndex() throws Exception {
        String transformId = "transform-2";
        String transformDest = transformId + "_idx";
        String transformDestAlias = transformId + "_alias";
        setupDataAccessRole(DATA_ACCESS_ROLE, REVIEWS_INDEX_NAME, transformDest, transformDestAlias);

        createTransform(transformId, transformDest, transformDestAlias);
        assertFalse(indexExists(transformDest));
        assertFalse(aliasExists(transformDestAlias));

        startTransform(transformId);
        waitForTransformCheckpoint(transformId, 1);

        stopTransform(transformId, false);
        assertTrue(indexExists(transformDest));
        assertTrue(aliasExists(transformDestAlias));

        deleteTransform(transformId, false, true);
        assertFalse(indexExists(transformDest));
        assertFalse(aliasExists(transformDestAlias));
    }

    public void testDeleteWithParamDeletesManuallyCreatedDestinationIndex() throws Exception {
        String transformId = "transform-3";
        String transformDest = transformId + "_idx";
        String transformDestAlias = transformId + "_alias";
        setupDataAccessRole(DATA_ACCESS_ROLE, REVIEWS_INDEX_NAME, transformDest, transformDestAlias);

        createIndex(transformDest);
        assertTrue(indexExists(transformDest));
        // The alias does not exist yet, it will be created when the transform starts
        assertFalse(aliasExists(transformDestAlias));

        createTransform(transformId, transformDest, transformDestAlias);
        assertFalse(aliasExists(transformDestAlias));

        startTransform(transformId);
        waitForTransformCheckpoint(transformId, 1);

        stopTransform(transformId, false);
        assertTrue(indexExists(transformDest));
        assertTrue(aliasExists(transformDestAlias));

        deleteTransform(transformId, false, true);
        assertFalse(indexExists(transformDest));
        assertFalse(aliasExists(transformDestAlias));
    }

    public void testDeleteWithManuallyCreatedIndexAndManuallyCreatedAlias() throws Exception {
        String transformId = "transform-4";
        String transformDest = transformId + "_idx";
        String transformDestAlias = transformId + "_alias";
        setupDataAccessRole(DATA_ACCESS_ROLE, REVIEWS_INDEX_NAME, transformDest, transformDestAlias);

        createIndex(transformDest, null, null, "\"" + transformDestAlias + "\": { \"is_write_index\": true }");
        assertTrue(indexExists(transformDest));
        assertTrue(aliasExists(transformDestAlias));

        createTransform(transformId, transformDestAlias, null);

        startTransform(transformId);
        waitForTransformCheckpoint(transformId, 1);

        stopTransform(transformId, false);
        assertTrue(indexExists(transformDest));
        assertTrue(aliasExists(transformDestAlias));

        deleteTransform(transformId, false, true);
        assertFalse(indexExists(transformDest));
        assertFalse(aliasExists(transformDestAlias));
    }

    public void testDeleteDestinationIndexIsNoOpWhenNoDestinationIndexExists() throws Exception {
        String transformId = "transform-5";
        String transformDest = transformId + "_idx";
        String transformDestAlias = transformId + "_alias";
        setupDataAccessRole(DATA_ACCESS_ROLE, REVIEWS_INDEX_NAME, transformDest, transformDestAlias);

        createTransform(transformId, transformDest, transformDestAlias);
        assertFalse(indexExists(transformDest));
        assertFalse(aliasExists(transformDestAlias));

        deleteTransform(transformId, false, true);
        assertFalse(indexExists(transformDest));
        assertFalse(aliasExists(transformDestAlias));
    }

    public void testDeleteWithAliasPointingToManyIndices() throws Exception {
        var transformId = "transform-6";
        var transformDest = transformId + "_idx";
        var otherIndex = "some-other-index-6";
        String transformDestAlias = transformId + "_alias";
        setupDataAccessRole(DATA_ACCESS_ROLE, REVIEWS_INDEX_NAME, transformDest, otherIndex, transformDestAlias);

        createIndex(transformDest, null, null, "\"" + transformDestAlias + "\": { \"is_write_index\": true }");
        createIndex(otherIndex, null, null, "\"" + transformDestAlias + "\": {}");

        assertTrue(indexExists(transformDest));
        assertTrue(indexExists(otherIndex));
        assertTrue(aliasExists(transformDestAlias));

        createTransform(transformId, transformDestAlias, null);

        startTransform(transformId);
        waitForTransformCheckpoint(transformId, 1);

        stopTransform(transformId, false);

        assertTrue(indexExists(transformDest));
        assertTrue(indexExists(otherIndex));
        assertTrue(aliasExists(transformDestAlias));

        deleteTransform(transformId, false, true);

        assertFalse(indexExists(transformDest));
        assertTrue(indexExists(otherIndex));
        assertTrue(aliasExists(transformDestAlias));
    }

    public void testDeleteWithNoWriteIndexThrowsException() throws Exception {
        var transformId = "transform-7";
        var transformDest = transformId + "_idx";
        var otherIndex = "some-other-index-7";
        String transformDestAlias = transformId + "_alias";
        setupDataAccessRole(DATA_ACCESS_ROLE, REVIEWS_INDEX_NAME, transformDest, otherIndex, transformDestAlias);

        createIndex(transformDest, null, null, "\"" + transformDestAlias + "\": {}");

        assertTrue(indexExists(transformDest));
        assertTrue(aliasExists(transformDestAlias));

        createTransform(transformId, transformDestAlias, null);

        createIndex(otherIndex, null, null, "\"" + transformDestAlias + "\": {}");
        assertTrue(indexExists(otherIndex));

        ResponseException e = expectThrows(ResponseException.class, () -> deleteTransform(transformId, false, true));
        assertThat(
            e.getMessage(),
            containsString(
                Strings.format(
                    "Cannot disambiguate destination index alias [%s]. Alias points to many indices with no clear write alias."
                        + " Retry with delete_dest_index=false and manually clean up destination index.",
                    transformDestAlias
                )
            )
        );
    }

    public void testDeleteWithAlreadyDeletedIndex() throws Exception {
        var transformId = "transform-8";
        var transformDest = transformId + "_idx";
        setupDataAccessRole(DATA_ACCESS_ROLE, REVIEWS_INDEX_NAME, transformDest);

        createIndex(transformDest);

        assertTrue(indexExists(transformDest));

        createTransform(transformId, transformDest, null);

        deleteIndex(transformDest);

        assertFalse(indexExists(transformDest));

        deleteTransform(transformId, false, true);

        assertFalse(indexExists(transformDest));
    }

    private void createTransform(String transformId, String destIndex, String destAlias) throws IOException {
        final Request createTransformRequest = createRequestWithAuth(
            "PUT",
            getTransformEndpoint() + transformId,
            BASIC_AUTH_VALUE_TRANSFORM_ADMIN_1
        );
        String destAliases = destAlias != null ? Strings.format("""
            , "aliases": [{"alias": "%s"}]
            """, destAlias) : "";
        String config = Strings.format("""
            {
              "dest": {
                "index": "%s"
                %s
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
            }""", destIndex, destAliases, REVIEWS_INDEX_NAME);
        createTransformRequest.setJsonEntity(config);
        Map<String, Object> createTransformResponse = entityAsMap(client().performRequest(createTransformRequest));
        assertThat(createTransformResponse.get("acknowledged"), equalTo(Boolean.TRUE));
    }
}
