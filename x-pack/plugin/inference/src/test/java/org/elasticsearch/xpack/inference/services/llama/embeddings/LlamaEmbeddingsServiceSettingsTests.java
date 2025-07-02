/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.llama.embeddings;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;
import org.hamcrest.CoreMatchers;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class LlamaEmbeddingsServiceSettingsTests extends AbstractWireSerializingTestCase<LlamaEmbeddingsServiceSettings> {
    private static final String MODEL_ID = "some model";
    private static final String CORRECT_URL = "https://www.elastic.co";
    private static final int DIMENSIONS = 384;
    private static final SimilarityMeasure SIMILARITY_MEASURE = SimilarityMeasure.DOT_PRODUCT;
    private static final int MAX_INPUT_TOKENS = 128;
    private static final int RATE_LIMIT = 2;

    public void testFromMap_AllFields_Success() {
        var serviceSettings = LlamaEmbeddingsServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
                    ServiceFields.MODEL_ID,
                    MODEL_ID,
                    ServiceFields.URL,
                    CORRECT_URL,
                    ServiceFields.SIMILARITY,
                    SIMILARITY_MEASURE.toString(),
                    ServiceFields.DIMENSIONS,
                    DIMENSIONS,
                    ServiceFields.MAX_INPUT_TOKENS,
                    MAX_INPUT_TOKENS,
                    RateLimitSettings.FIELD_NAME,
                    new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, RATE_LIMIT))
                )
            ),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(
            serviceSettings,
            is(
                new LlamaEmbeddingsServiceSettings(
                    MODEL_ID,
                    CORRECT_URL,
                    DIMENSIONS,
                    SIMILARITY_MEASURE,
                    MAX_INPUT_TOKENS,
                    new RateLimitSettings(RATE_LIMIT)
                )
            )
        );
    }

    public void testFromMap_NoModelId_Failure() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> LlamaEmbeddingsServiceSettings.fromMap(
                new HashMap<>(
                    Map.of(
                        ServiceFields.URL,
                        CORRECT_URL,
                        ServiceFields.SIMILARITY,
                        SIMILARITY_MEASURE.toString(),
                        ServiceFields.DIMENSIONS,
                        DIMENSIONS,
                        ServiceFields.MAX_INPUT_TOKENS,
                        MAX_INPUT_TOKENS,
                        RateLimitSettings.FIELD_NAME,
                        new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, RATE_LIMIT))
                    )
                ),
                ConfigurationParseContext.PERSISTENT
            )
        );
        assertThat(
            thrownException.getMessage(),
            containsString(Strings.format("Validation Failed: 1: [service_settings] does not contain the required setting [model_id];", 2))
        );
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var entity = new LlamaEmbeddingsServiceSettings(
            MODEL_ID,
            CORRECT_URL,
            DIMENSIONS,
            SIMILARITY_MEASURE,
            MAX_INPUT_TOKENS,
            new RateLimitSettings(3)
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, CoreMatchers.is(XContentHelper.stripWhitespace("""
            {
                "model_id": "some model",
                "url": "https://www.elastic.co",
                "dimensions": 384,
                "similarity": "dot_product",
                "max_input_tokens": 128,
                "rate_limit": {
                    "requests_per_minute": 3
                }
            }
            """)));
    }

    public void testStreamInputAndOutput_WritesValuesCorrectly() throws IOException {
        var outputBuffer = new BytesStreamOutput();
        var settings = new LlamaEmbeddingsServiceSettings(
            MODEL_ID,
            CORRECT_URL,
            DIMENSIONS,
            SIMILARITY_MEASURE,
            MAX_INPUT_TOKENS,
            new RateLimitSettings(3)
        );
        settings.writeTo(outputBuffer);

        var outputBufferRef = outputBuffer.bytes();
        var inputBuffer = new ByteArrayStreamInput(outputBufferRef.array());

        var settingsFromBuffer = new LlamaEmbeddingsServiceSettings(inputBuffer);

        assertEquals(settings, settingsFromBuffer);
    }

    @Override
    protected Writeable.Reader<LlamaEmbeddingsServiceSettings> instanceReader() {
        return LlamaEmbeddingsServiceSettings::new;
    }

    @Override
    protected LlamaEmbeddingsServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected LlamaEmbeddingsServiceSettings mutateInstance(LlamaEmbeddingsServiceSettings instance) throws IOException {
        return randomValueOtherThan(instance, LlamaEmbeddingsServiceSettingsTests::createRandom);
    }

    private static LlamaEmbeddingsServiceSettings createRandom() {
        var modelId = randomAlphaOfLength(8);
        var url = randomAlphaOfLength(15);
        var similarityMeasure = randomFrom(SimilarityMeasure.values());
        var dimensions = randomIntBetween(32, 256);
        var maxInputTokens = randomIntBetween(128, 256);
        return new LlamaEmbeddingsServiceSettings(
            modelId,
            url,
            dimensions,
            similarityMeasure,
            maxInputTokens,
            RateLimitSettingsTests.createRandom()
        );
    }
}
