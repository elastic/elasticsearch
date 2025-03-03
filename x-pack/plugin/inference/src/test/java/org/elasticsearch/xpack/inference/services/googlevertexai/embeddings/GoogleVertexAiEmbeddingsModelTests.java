/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai.embeddings;

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

    public void testBuildUri() throws URISyntaxException {
        var location = "location";
        var projectId = "project";
        var modelId = "model";

        URI uri = GoogleVertexAiEmbeddingsModel.buildUri(location, projectId, modelId);

        assertThat(
            uri,
            is(
                new URI(
                    Strings.format(
                        "https://%s-aiplatform.googleapis.com/v1/projects/%s/locations/%s/publishers/google/models/%s:predict",
                        location,
                        projectId,
                        location,
                        modelId
                    )
                )
            )
        );
    }

    public void testOverrideWith_DoesNotOverrideAndModelRemainsEqual_WhenSettingsAreEmpty_AndInputTypeIsInvalid() {
        var model = createModel("model", Boolean.FALSE, InputType.SEARCH);
        var overriddenModel = GoogleVertexAiEmbeddingsModel.of(model, Map.of(), InputType.UNSPECIFIED);

        MatcherAssert.assertThat(overriddenModel, is(model));
    }

    public void testOverrideWith_DoesNotOverrideAndModelRemainsEqual_WhenSettingsAreNull_AndInputTypeIsInvalid() {
        var model = createModel("model", Boolean.FALSE, InputType.SEARCH);
        var overriddenModel = GoogleVertexAiEmbeddingsModel.of(model, null, InputType.UNSPECIFIED);

        MatcherAssert.assertThat(overriddenModel, is(model));
    }

    public void testOverrideWith_SetsInputTypeToOverride_WhenFieldIsNullInModelTaskSettings_AndNullInRequestTaskSettings() {
        var model = createModel("model", Boolean.FALSE, (InputType) null);
        var overriddenModel = GoogleVertexAiEmbeddingsModel.of(model, getTaskSettingsMap(null, null), InputType.SEARCH);

        var expectedModel = createModel("model", Boolean.FALSE, InputType.SEARCH);
        MatcherAssert.assertThat(overriddenModel, is(expectedModel));
    }

    public void testOverrideWith_SetsInputType_FromRequest_IfValid_OverridingStoredTaskSettings() {
        var model = createModel("model", Boolean.FALSE, InputType.INGEST);
        var overriddenModel = GoogleVertexAiEmbeddingsModel.of(model, getTaskSettingsMap(null, null), InputType.SEARCH);

        var expectedModel = createModel("model", Boolean.FALSE, InputType.SEARCH);
        MatcherAssert.assertThat(overriddenModel, is(expectedModel));
    }

    public void testOverrideWith_SetsInputType_FromRequest_IfValid_OverridingRequestTaskSettings() {
        var model = createModel("model", Boolean.FALSE, (InputType) null);
        var overriddenModel = GoogleVertexAiEmbeddingsModel.of(model, getTaskSettingsMap(null, InputType.CLUSTERING), InputType.SEARCH);

        var expectedModel = createModel("model", Boolean.FALSE, InputType.SEARCH);
        MatcherAssert.assertThat(overriddenModel, is(expectedModel));
    }

    public void testOverrideWith_OverridesInputType_WithRequestTaskSettingsSearch_WhenRequestInputTypeIsInvalid() {
        var model = createModel("model", Boolean.FALSE, InputType.INGEST);
        var overriddenModel = GoogleVertexAiEmbeddingsModel.of(model, getTaskSettingsMap(null, InputType.SEARCH), InputType.UNSPECIFIED);

        var expectedModel = createModel("model", Boolean.FALSE, InputType.SEARCH);
        MatcherAssert.assertThat(overriddenModel, is(expectedModel));
    }

    public void testOverrideWith_DoesNotSetInputType_FromRequest_IfInputTypeIsInvalid() {
        var model = createModel("model", Boolean.FALSE, (InputType) null);
        var overriddenModel = GoogleVertexAiEmbeddingsModel.of(model, getTaskSettingsMap(null, null), InputType.UNSPECIFIED);

        var expectedModel = createModel("model", Boolean.FALSE, (InputType) null);
        MatcherAssert.assertThat(overriddenModel, is(expectedModel));
    }

    public void testOverrideWith_DoesNotSetInputType_WhenRequestTaskSettingsIsNull_AndRequestInputTypeIsInvalid() {
        var model = createModel("model", Boolean.FALSE, InputType.INGEST);
        var overriddenModel = GoogleVertexAiEmbeddingsModel.of(model, getTaskSettingsMap(null, null), InputType.UNSPECIFIED);

        var expectedModel = createModel("model", Boolean.FALSE, InputType.INGEST);
        MatcherAssert.assertThat(overriddenModel, is(expectedModel));
    }

    public void testOverrideWith_DoesNotOverrideModelUri() {
        var model = createModel("model", Boolean.FALSE, InputType.SEARCH);
        var overriddenModel = GoogleVertexAiEmbeddingsModel.of(model, Map.of(), null);

        MatcherAssert.assertThat(overriddenModel.uri(), is(model.uri()));
    }

    public static GoogleVertexAiEmbeddingsModel createModel(
        String location,
        String projectId,
        String modelId,
        String uri,
        String serviceAccountJson
    ) {
        return new GoogleVertexAiEmbeddingsModel(
            "id",
            TaskType.TEXT_EMBEDDING,
            "service",
            uri,
            new GoogleVertexAiEmbeddingsServiceSettings(location, projectId, modelId, false, null, null, null, null),
            new GoogleVertexAiEmbeddingsTaskSettings(Boolean.FALSE, null),
            new GoogleVertexAiSecretSettings(new SecureString(serviceAccountJson.toCharArray()))
        );
    }

    public static GoogleVertexAiEmbeddingsModel createModel(
        String modelId,
        @Nullable Boolean autoTruncate,
        SimilarityMeasure similarityMeasure
    ) {
        return new GoogleVertexAiEmbeddingsModel(
            "id",
            TaskType.TEXT_EMBEDDING,
            "service",
            new GoogleVertexAiEmbeddingsServiceSettings(
                randomAlphaOfLength(8),
                randomAlphaOfLength(8),
                modelId,
                false,
                null,
                null,
                similarityMeasure,
                null
            ),
            new GoogleVertexAiEmbeddingsTaskSettings(autoTruncate, randomFrom(InputType.INGEST, InputType.SEARCH)),
            null,
            new GoogleVertexAiSecretSettings(new SecureString(randomAlphaOfLength(8).toCharArray()))
        );
    }

    public static GoogleVertexAiEmbeddingsModel createModel(String modelId, @Nullable Boolean autoTruncate, @Nullable InputType inputType) {
        return new GoogleVertexAiEmbeddingsModel(
            "id",
            TaskType.TEXT_EMBEDDING,
            "service",
            new GoogleVertexAiEmbeddingsServiceSettings(
                "location",
                "projectId",
                modelId,
                false,
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
            "id",
            TaskType.TEXT_EMBEDDING,
            "service",
            new GoogleVertexAiEmbeddingsServiceSettings(
                randomAlphaOfLength(8),
                randomAlphaOfLength(8),
                modelId,
                false,
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
