/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai.embeddings;

import org.apache.http.HttpHeaders;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.googlevertexai.GoogleVertexAiSecretSettings;
import org.hamcrest.MatcherAssert;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.googlevertexai.embeddings.GoogleVertexAiEmbeddingsTaskSettingsTests.getTaskSettingsMap;
import static org.hamcrest.Matchers.is;

public class GoogleVertexAiEmbeddingsModelTests extends ESTestCase {

    private static final String TEST_INFERENCE_ID = "test-inference-id";
    private static final String TEST_SERVICE_NAME = "test-service-name";
    private static final String TEST_LOCATION = "test-location-id";
    private static final String TEST_PROJECT_ID = "test-project-id";
    private static final String TEST_MODEL_ID = "test-model-id";
    private static final InputType TEST_INPUT_TYPE = InputType.SEARCH;

    public void testBuildUri() throws URISyntaxException {
        URI uri = GoogleVertexAiEmbeddingsModel.buildUri(TEST_LOCATION, TEST_PROJECT_ID, TEST_MODEL_ID);

        assertThat(
            uri,
            is(
                new URI(
                    Strings.format(
                        "https://%s-aiplatform.googleapis.com/v1/projects/%s/locations/%s/publishers/google/models/%s:predict",
                        TEST_LOCATION,
                        TEST_PROJECT_ID,
                        TEST_LOCATION,
                        TEST_MODEL_ID
                    )
                )
            )
        );
    }

    public void testBuildUri_EmptyLocation_UsesGlobalHost() throws URISyntaxException {
        testBuildUri_UsesGlobalHost("");
    }

    public void testBuildUri_NullLocation_UsesGlobalHost() throws URISyntaxException {
        testBuildUri_UsesGlobalHost(null);
    }

    private static void testBuildUri_UsesGlobalHost(String location) throws URISyntaxException {
        var expectedUri = new URI(
            Strings.format(
                "https://aiplatform.googleapis.com/v1/projects/%s/locations/global/publishers/google/models/%s:predict",
                TEST_PROJECT_ID,
                TEST_MODEL_ID
            )
        );

        assertThat(GoogleVertexAiEmbeddingsModel.buildUri(location, TEST_PROJECT_ID, TEST_MODEL_ID), is(expectedUri));
    }

    public void testOverrideWith_DoesNotOverrideAndModelRemainsEqual_WhenSettingsAreEmpty() {
        var model = createModel(TEST_MODEL_ID, Boolean.FALSE, TEST_INPUT_TYPE);
        var overriddenModel = GoogleVertexAiEmbeddingsModel.of(model, Map.of());

        MatcherAssert.assertThat(overriddenModel, is(model));
    }

    public void testOverrideWith_DoesNotOverrideAndModelRemainsEqual_WhenSettingsAreNull() {
        var model = createModel(TEST_MODEL_ID, Boolean.FALSE, TEST_INPUT_TYPE);
        var overriddenModel = GoogleVertexAiEmbeddingsModel.of(model, null);

        MatcherAssert.assertThat(overriddenModel, is(model));
    }

    public void testOverrideWith_SetsInputType_FromRequestTaskSettings_IfValid_OverridingStoredTaskSettings() {
        var model = createModel(TEST_MODEL_ID, Boolean.FALSE, TEST_INPUT_TYPE);
        var newInputType = InputType.INGEST;
        var overriddenModel = GoogleVertexAiEmbeddingsModel.of(model, getTaskSettingsMap(null, newInputType));

        var expectedModel = createModel(TEST_MODEL_ID, Boolean.FALSE, newInputType);
        MatcherAssert.assertThat(overriddenModel, is(expectedModel));
    }

    public void testOverrideWith_DoesNotOverrideInputType_WhenRequestTaskSettingsIsNull() {
        var model = createModel(TEST_MODEL_ID, Boolean.FALSE, TEST_INPUT_TYPE);
        var overriddenModel = GoogleVertexAiEmbeddingsModel.of(model, getTaskSettingsMap(null, null));

        var expectedModel = createModel(TEST_MODEL_ID, Boolean.FALSE, TEST_INPUT_TYPE);
        MatcherAssert.assertThat(overriddenModel, is(expectedModel));
    }

    public void testOverrideWith_DoesNotOverrideModelUri() {
        var model = createModel(TEST_MODEL_ID, Boolean.FALSE, TEST_INPUT_TYPE);
        var overriddenModel = GoogleVertexAiEmbeddingsModel.of(model, Map.of());

        MatcherAssert.assertThat(overriddenModel.nonStreamingUri(), is(model.nonStreamingUri()));
    }

    public static GoogleVertexAiEmbeddingsModel createModel(
        String location,
        String projectId,
        String modelId,
        String uri,
        String serviceAccountJson,
        String authHeaderValue
    ) {
        return new GoogleVertexAiEmbeddingsModel(
            TEST_INFERENCE_ID,
            TaskType.TEXT_EMBEDDING,
            TEST_SERVICE_NAME,
            uri,
            new GoogleVertexAiEmbeddingsServiceSettings(location, projectId, modelId, false, null, null, null, null, null),
            new GoogleVertexAiEmbeddingsTaskSettings(Boolean.FALSE, null),
            new GoogleVertexAiSecretSettings(new SecureString(serviceAccountJson.toCharArray())),
            (httpPost, model) -> httpPost.setHeader(HttpHeaders.AUTHORIZATION, authHeaderValue)
        );
    }

    public static GoogleVertexAiEmbeddingsModel createModel(
        String modelId,
        @Nullable Boolean autoTruncate,
        SimilarityMeasure similarityMeasure
    ) {
        return new GoogleVertexAiEmbeddingsModel(
            TEST_INFERENCE_ID,
            TaskType.TEXT_EMBEDDING,
            TEST_SERVICE_NAME,
            new GoogleVertexAiEmbeddingsServiceSettings(
                randomAlphaOfLength(8),
                randomAlphaOfLength(8),
                modelId,
                false,
                null,
                null,
                null,
                similarityMeasure,
                null
            ),
            new GoogleVertexAiEmbeddingsTaskSettings(autoTruncate, randomFrom(InputType.INGEST, TEST_INPUT_TYPE)),
            null,
            new GoogleVertexAiSecretSettings(new SecureString(randomAlphaOfLength(8).toCharArray()))
        );
    }

    public static GoogleVertexAiEmbeddingsModel createModel(String modelId, @Nullable Boolean autoTruncate, @Nullable InputType inputType) {
        return new GoogleVertexAiEmbeddingsModel(
            TEST_INFERENCE_ID,
            TaskType.TEXT_EMBEDDING,
            TEST_SERVICE_NAME,
            new GoogleVertexAiEmbeddingsServiceSettings(
                TEST_LOCATION,
                TEST_PROJECT_ID,
                modelId,
                false,
                null,
                null,
                null,
                SimilarityMeasure.DOT_PRODUCT,
                null
            ),
            new GoogleVertexAiEmbeddingsTaskSettings(autoTruncate, inputType),
            null,
            new GoogleVertexAiSecretSettings(new SecureString("testString".toCharArray()))
        );
    }

    public static GoogleVertexAiEmbeddingsModel createRandomizedModel(
        String modelId,
        @Nullable Boolean autoTruncate,
        @Nullable InputType inputType
    ) {
        return new GoogleVertexAiEmbeddingsModel(
            TEST_INFERENCE_ID,
            TaskType.TEXT_EMBEDDING,
            TEST_SERVICE_NAME,
            new GoogleVertexAiEmbeddingsServiceSettings(
                randomAlphaOfLength(8),
                randomAlphaOfLength(8),
                modelId,
                false,
                null,
                null,
                null,
                SimilarityMeasure.DOT_PRODUCT,
                null
            ),
            new GoogleVertexAiEmbeddingsTaskSettings(autoTruncate, inputType),
            null,
            new GoogleVertexAiSecretSettings(new SecureString(randomAlphaOfLength(8).toCharArray()))
        );
    }
}
