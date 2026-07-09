/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.tencentcloud.embeddings;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.tencentcloud.TencentCloudCommonServiceSettings;
import org.elasticsearch.xpack.inference.services.tencentcloud.TencentCloudCommonServiceSettingsTests;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class TencentCloudEmbeddingsServiceSettingsTests extends AbstractBWCWireSerializationTestCase<
    TencentCloudEmbeddingsServiceSettings> {

    private static final String TEST_MODEL_ID = "bge-m3";
    private static final int TEST_DIMENSIONS = 1024;
    private static final int TEST_MAX_INPUT_TOKENS = 512;

    public static TencentCloudEmbeddingsServiceSettings createRandom() {
        return new TencentCloudEmbeddingsServiceSettings(
            TencentCloudCommonServiceSettingsTests.createRandom(),
            randomBoolean() ? randomFrom(SimilarityMeasure.values()) : null,
            randomBoolean() ? randomIntBetween(32, TEST_DIMENSIONS) : null,
            randomBoolean() ? randomIntBetween(16, TEST_MAX_INPUT_TOKENS) : null
        );
    }

    public void testFromMap_MinimalConfig() {
        var settings = TencentCloudEmbeddingsServiceSettings.fromMap(
            new HashMap<>(Map.of(ServiceFields.MODEL_ID, TEST_MODEL_ID)),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(settings.modelId(), is(TEST_MODEL_ID));
        assertNull(settings.similarity());
        assertNull(settings.dimensions());
        assertNull(settings.maxInputTokens());
    }

    public void testFromMap_AllFields_Success() {
        var settings = TencentCloudEmbeddingsServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
                    ServiceFields.MODEL_ID,
                    TEST_MODEL_ID,
                    ServiceFields.SIMILARITY,
                    SimilarityMeasure.DOT_PRODUCT.toString(),
                    ServiceFields.DIMENSIONS,
                    TEST_DIMENSIONS,
                    ServiceFields.MAX_INPUT_TOKENS,
                    TEST_MAX_INPUT_TOKENS,
                    RateLimitSettings.FIELD_NAME,
                    new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, 100))
                )
            ),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(settings.modelId(), is(TEST_MODEL_ID));
        assertThat(settings.similarity(), is(SimilarityMeasure.DOT_PRODUCT));
        assertThat(settings.dimensions(), is(TEST_DIMENSIONS));
        assertThat(settings.maxInputTokens(), is(TEST_MAX_INPUT_TOKENS));
        assertThat(settings.rateLimitSettings(), is(new RateLimitSettings(100)));
    }

    public void testUpdateEmbeddingDetails_ReturnsCopyWithNewValues() {
        var commonSettings = new TencentCloudCommonServiceSettings(TEST_MODEL_ID, null, new RateLimitSettings(20));
        var settings = new TencentCloudEmbeddingsServiceSettings(commonSettings, null, null, null);

        var updated = settings.updateEmbeddingDetails(TEST_DIMENSIONS, SimilarityMeasure.COSINE);

        assertThat(updated.dimensions(), is(TEST_DIMENSIONS));
        assertThat(updated.similarity(), is(SimilarityMeasure.COSINE));
        assertThat(updated.getCommonSettings(), is(commonSettings));
    }

    @Override
    protected Writeable.Reader<TencentCloudEmbeddingsServiceSettings> instanceReader() {
        return TencentCloudEmbeddingsServiceSettings::new;
    }

    @Override
    protected TencentCloudEmbeddingsServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected TencentCloudEmbeddingsServiceSettings mutateInstance(TencentCloudEmbeddingsServiceSettings instance) throws IOException {
        var commonSettings = instance.getCommonSettings();
        var similarity = instance.similarity();
        var dimensions = instance.dimensions();
        var maxInputTokens = instance.maxInputTokens();

        switch (between(0, 3)) {
            case 0 -> commonSettings = randomValueOtherThan(commonSettings, TencentCloudCommonServiceSettingsTests::createRandom);
            case 1 -> similarity = randomValueOtherThan(similarity, () -> randomFrom(SimilarityMeasure.values()));
            case 2 -> dimensions = randomValueOtherThan(dimensions, () -> randomIntBetween(32, 4096));
            case 3 -> maxInputTokens = randomValueOtherThan(maxInputTokens, () -> randomIntBetween(16, 8192));
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new TencentCloudEmbeddingsServiceSettings(commonSettings, similarity, dimensions, maxInputTokens);
    }

    @Override
    protected TencentCloudEmbeddingsServiceSettings mutateInstanceForVersion(
        TencentCloudEmbeddingsServiceSettings instance,
        TransportVersion version
    ) {
        return instance;
    }
}
