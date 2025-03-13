/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googleaistudio.embeddings;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.MatchersUtils.equalToIgnoringWhitespaceInJsonString;
import static org.elasticsearch.xpack.inference.Utils.randomSimilarityMeasure;
import static org.hamcrest.Matchers.is;

public class GoogleAiStudioEmbeddingsServiceSettingsTests extends AbstractWireSerializingTestCase<GoogleAiStudioEmbeddingsServiceSettings> {

    private static GoogleAiStudioEmbeddingsServiceSettings createRandom() {
        return new GoogleAiStudioEmbeddingsServiceSettings(
            randomAlphaOfLength(8),
            randomFrom(randomNonNegativeInt(), null),
            randomFrom(randomNonNegativeInt(), null),
            randomFrom(randomSimilarityMeasure(), null),
            randomFrom(RateLimitSettingsTests.createRandom(), null)
        );
    }

    public void testFromMap_Request_CreatesSettingsCorrectly() {
        var model = randomAlphaOfLength(8);
        var maxInputTokens = randomIntBetween(1, 1024);
        var dims = randomIntBetween(1, 10000);
        var similarity = randomSimilarityMeasure();

        var serviceSettings = GoogleAiStudioEmbeddingsServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
                    ServiceFields.MODEL_ID,
                    model,
                    ServiceFields.MAX_INPUT_TOKENS,
                    maxInputTokens,
                    ServiceFields.DIMENSIONS,
                    dims,
                    ServiceFields.SIMILARITY,
                    similarity.toString()
                )
            ),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(serviceSettings, is(new GoogleAiStudioEmbeddingsServiceSettings(model, maxInputTokens, dims, similarity, null)));
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var entity = new GoogleAiStudioEmbeddingsServiceSettings("model", 1024, 8, SimilarityMeasure.DOT_PRODUCT, null);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, equalToIgnoringWhitespaceInJsonString("""
            {
                "model_id":"model",
                "max_input_tokens": 1024,
                "dimensions": 8,
                "similarity": "dot_product",
                "rate_limit": {
                    "requests_per_minute":360
                }
            }"""));
    }

    @Override
    protected Writeable.Reader<GoogleAiStudioEmbeddingsServiceSettings> instanceReader() {
        return GoogleAiStudioEmbeddingsServiceSettings::new;
    }

    @Override
    protected GoogleAiStudioEmbeddingsServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected GoogleAiStudioEmbeddingsServiceSettings mutateInstance(GoogleAiStudioEmbeddingsServiceSettings instance) throws IOException {
        return randomValueOtherThan(instance, GoogleAiStudioEmbeddingsServiceSettingsTests::createRandom);
    }
}
