/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai.embeddings;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.googlevertexai.GoogleVertexAiServiceFields;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;

import java.io.IOException;
import java.util.HashMap;

import static org.elasticsearch.xpack.inference.MatchersUtils.equalToIgnoringWhitespaceInJsonString;
import static org.hamcrest.Matchers.is;

public class GoogleVertexAiEmbeddingsServiceSettingsTests extends AbstractBWCWireSerializationTestCase<
    GoogleVertexAiEmbeddingsServiceSettings> {

    public void testFromMap_Request_CreatesSettingsCorrectly() {
        var location = randomAlphaOfLength(8);
        var projectId = randomAlphaOfLength(8);
        var model = randomAlphaOfLength(8);
        var dimensionsSetByUser = randomBoolean();
        var maxInputTokens = randomFrom(new Integer[] { null, randomNonNegativeInt() });
        var similarityMeasure = randomFrom(new SimilarityMeasure[] { null, randomFrom(SimilarityMeasure.values()) });
        var similarityMeasureString = similarityMeasure == null ? null : similarityMeasure.toString();
        var dims = randomFrom(new Integer[] { null, randomNonNegativeInt() });
        var configurationParseContext = ConfigurationParseContext.PERSISTENT;

        var serviceSettings = GoogleVertexAiEmbeddingsServiceSettings.fromMap(new HashMap<>() {
            {
                put(GoogleVertexAiServiceFields.LOCATION, location);
                put(GoogleVertexAiServiceFields.PROJECT_ID, projectId);
                put(ServiceFields.MODEL_ID, model);
                put(GoogleVertexAiEmbeddingsServiceSettings.DIMENSIONS_SET_BY_USER, dimensionsSetByUser);
                put(ServiceFields.MAX_INPUT_TOKENS, maxInputTokens);
                put(ServiceFields.SIMILARITY, similarityMeasureString);
                put(ServiceFields.DIMENSIONS, dims);
            }
        }, configurationParseContext);

        assertThat(
            serviceSettings,
            is(
                new GoogleVertexAiEmbeddingsServiceSettings(
                    location,
                    projectId,
                    model,
                    null, // TODO: Randomize this
                    null, // TODO: Randomize this
                    dimensionsSetByUser,
                    maxInputTokens,
                    dims,
                    similarityMeasure,
                    null
                )
            )
        );
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var entity = new GoogleVertexAiEmbeddingsServiceSettings(
            "location",
            "projectId",
            "modelId",
            null, // TODO: Set this value
            null, // TODO: Set this value
            true,
            10,
            10,
            SimilarityMeasure.DOT_PRODUCT,
            null
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, equalToIgnoringWhitespaceInJsonString("""
            {
                "location": "location",
                "project_id": "projectId",
                "model_id": "modelId",
                "max_input_tokens": 10,
                "dimensions": 10,
                "similarity": "dot_product",
                "rate_limit": {
                    "requests_per_minute": 30000
                },
                "dimensions_set_by_user": true
            }
            """));
    }

    public void testFilteredXContentObject_WritesAllValues_ExceptDimensionsSetByUser() throws IOException {
        var entity = new GoogleVertexAiEmbeddingsServiceSettings(
            "location",
            "projectId",
            "modelId",
            null, // TODO: Set this value
            null, // TODO: Set this value
            true,
            10,
            10,
            SimilarityMeasure.DOT_PRODUCT,
            null
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        var filteredXContent = entity.getFilteredXContentObject();
        filteredXContent.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, equalToIgnoringWhitespaceInJsonString("""
            {
                "location": "location",
                "project_id": "projectId",
                "model_id": "modelId",
                "max_input_tokens": 10,
                "dimensions": 10,
                "similarity": "dot_product",
                "rate_limit": {
                    "requests_per_minute": 30000
                }
            }
            """));
    }

    @Override
    protected Writeable.Reader<GoogleVertexAiEmbeddingsServiceSettings> instanceReader() {
        return GoogleVertexAiEmbeddingsServiceSettings::new;
    }

    @Override
    protected GoogleVertexAiEmbeddingsServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected GoogleVertexAiEmbeddingsServiceSettings mutateInstance(GoogleVertexAiEmbeddingsServiceSettings instance) throws IOException {
        return randomValueOtherThan(instance, GoogleVertexAiEmbeddingsServiceSettingsTests::createRandom);
    }

    @Override
    protected GoogleVertexAiEmbeddingsServiceSettings mutateInstanceForVersion(
        GoogleVertexAiEmbeddingsServiceSettings instance,
        TransportVersion version
    ) {
        return instance;
    }

    private static GoogleVertexAiEmbeddingsServiceSettings createRandom() {
        return new GoogleVertexAiEmbeddingsServiceSettings(
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            null, // TODO: Randomize this value
            null, // TODO: Randomize this value
            randomBoolean(),
            randomFrom(new Integer[] { null, randomNonNegativeInt() }),
            randomFrom(new Integer[] { null, randomNonNegativeInt() }),
            randomFrom(new SimilarityMeasure[] { null, randomFrom(SimilarityMeasure.values()) }),
            randomFrom(new RateLimitSettings[] { null, RateLimitSettingsTests.createRandom() })
        );
    }
}
