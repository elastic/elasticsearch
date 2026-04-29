/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.voyageai.embeddings;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.voyageai.VoyageAICommonServiceSettings;
import org.elasticsearch.xpack.inference.services.voyageai.VoyageAICommonServiceSettingsTests;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.Utils.randomSimilarityMeasure;
import static org.elasticsearch.xpack.inference.services.ServiceFields.DIMENSIONS_SET_BY_USER;
import static org.elasticsearch.xpack.inference.services.voyageai.VoyageAICommonServiceSettingsTests.DEFAULT_RATE_LIMIT;
import static org.elasticsearch.xpack.inference.services.voyageai.VoyageAICommonServiceSettingsTests.INITIAL_TEST_MODEL_ID;
import static org.elasticsearch.xpack.inference.services.voyageai.VoyageAICommonServiceSettingsTests.INITIAL_TEST_RATE_LIMIT;
import static org.elasticsearch.xpack.inference.services.voyageai.VoyageAICommonServiceSettingsTests.TEST_MODEL_ID;
import static org.elasticsearch.xpack.inference.services.voyageai.VoyageAICommonServiceSettingsTests.TEST_RATE_LIMIT;
import static org.hamcrest.Matchers.is;

public class VoyageAIEmbeddingsServiceSettingsTests extends AbstractBWCWireSerializationTestCase<VoyageAIEmbeddingsServiceSettings> {

    private static final int TEST_DIMENSIONS = 1536;
    private static final int INITIAL_TEST_DIMENSIONS = 3072;

    private static final int TEST_MAX_INPUT_TOKENS = 512;
    private static final int INITIAL_TEST_MAX_INPUT_TOKENS = 1024;

    private static final SimilarityMeasure TEST_SIMILARITY_MEASURE = SimilarityMeasure.COSINE;
    private static final SimilarityMeasure INITIAL_TEST_SIMILARITY_MEASURE = SimilarityMeasure.DOT_PRODUCT;

    private static final VoyageAIEmbeddingType TEST_EMBEDDING_TYPE = VoyageAIEmbeddingType.INT8;
    private static final VoyageAIEmbeddingType INITIAL_TEST_EMBEDDING_TYPE = VoyageAIEmbeddingType.FLOAT;
    private static final VoyageAIEmbeddingType DEFAULT_EMBEDDING_TYPE = VoyageAIEmbeddingType.FLOAT;

    public static VoyageAIEmbeddingsServiceSettings createRandom() {
        return new VoyageAIEmbeddingsServiceSettings(
            VoyageAICommonServiceSettingsTests.createRandom(),
            randomFrom(VoyageAIEmbeddingType.values()),
            randomFrom(randomSimilarityMeasure(), null),
            randomFrom(randomIntBetween(128, 256), null),
            randomFrom(randomIntBetween(128, 2048), null),
            randomBoolean()
        );
    }

    public void testFromMap_Request_AllFields_CreatesSettingsCorrectly() {
        var serviceSettings = VoyageAIEmbeddingsServiceSettings.fromMap(
            buildServiceSettingsMap(
                TEST_MODEL_ID,
                TEST_EMBEDDING_TYPE.toString(),
                TEST_SIMILARITY_MEASURE.toString(),
                TEST_DIMENSIONS,
                TEST_MAX_INPUT_TOKENS,
                null,
                TEST_RATE_LIMIT
            ),
            ConfigurationParseContext.REQUEST
        );

        assertThat(
            serviceSettings,
            is(
                new VoyageAIEmbeddingsServiceSettings(
                    new VoyageAICommonServiceSettings(TEST_MODEL_ID, new RateLimitSettings(TEST_RATE_LIMIT)),
                    TEST_EMBEDDING_TYPE,
                    TEST_SIMILARITY_MEASURE,
                    TEST_DIMENSIONS,
                    TEST_MAX_INPUT_TOKENS,
                    true
                )
            )
        );
    }

    public void testFromMap_Persistent_AllFields_CreatesSettingsCorrectly() {
        var dimensionsSetByUser = randomBoolean();
        var serviceSettings = VoyageAIEmbeddingsServiceSettings.fromMap(
            buildServiceSettingsMap(
                TEST_MODEL_ID,
                TEST_EMBEDDING_TYPE.toString(),
                TEST_SIMILARITY_MEASURE.toString(),
                TEST_DIMENSIONS,
                TEST_MAX_INPUT_TOKENS,
                dimensionsSetByUser,
                TEST_RATE_LIMIT
            ),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(
            serviceSettings,
            is(
                new VoyageAIEmbeddingsServiceSettings(
                    new VoyageAICommonServiceSettings(TEST_MODEL_ID, new RateLimitSettings(TEST_RATE_LIMIT)),
                    TEST_EMBEDDING_TYPE,
                    TEST_SIMILARITY_MEASURE,
                    TEST_DIMENSIONS,
                    TEST_MAX_INPUT_TOKENS,
                    dimensionsSetByUser
                )
            )
        );
    }

    public void testFromMap_Request_OnlyMandatoryFields_CreatesSettingsCorrectly() {
        var serviceSettings = VoyageAIEmbeddingsServiceSettings.fromMap(
            buildServiceSettingsMap(TEST_MODEL_ID, null, null, null, null, null, null),
            ConfigurationParseContext.REQUEST
        );

        assertThat(
            serviceSettings,
            is(
                new VoyageAIEmbeddingsServiceSettings(
                    new VoyageAICommonServiceSettings(TEST_MODEL_ID, new RateLimitSettings(DEFAULT_RATE_LIMIT)),
                    DEFAULT_EMBEDDING_TYPE,
                    null,
                    null,
                    null,
                    false
                )
            )
        );
    }

    public void testFromMap_Persistent_OnlyMandatoryFields_CreatesSettingsCorrectly() {
        var dimensionsSetByUser = randomBoolean();
        var serviceSettings = VoyageAIEmbeddingsServiceSettings.fromMap(
            buildServiceSettingsMap(TEST_MODEL_ID, null, null, null, null, dimensionsSetByUser, null),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(
            serviceSettings,
            is(
                new VoyageAIEmbeddingsServiceSettings(
                    new VoyageAICommonServiceSettings(TEST_MODEL_ID, new RateLimitSettings(DEFAULT_RATE_LIMIT)),
                    DEFAULT_EMBEDDING_TYPE,
                    null,
                    null,
                    null,
                    dimensionsSetByUser
                )
            )
        );
    }

    public void testFromMap_Persistent_DimensionsSetByUserNotProvided_DefaultsToFalse() {
        var serviceSettings = VoyageAIEmbeddingsServiceSettings.fromMap(
            buildServiceSettingsMap(TEST_MODEL_ID, null, null, null, null, null, null),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(
            serviceSettings,
            is(
                new VoyageAIEmbeddingsServiceSettings(
                    new VoyageAICommonServiceSettings(TEST_MODEL_ID, new RateLimitSettings(DEFAULT_RATE_LIMIT)),
                    DEFAULT_EMBEDDING_TYPE,
                    null,
                    null,
                    null,
                    false
                )
            )
        );
    }

    public void testFromMap_Request_DimensionsSetByUser_ThrowsValidationError() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> VoyageAIEmbeddingsServiceSettings.fromMap(
                buildServiceSettingsMap(TEST_MODEL_ID, null, null, TEST_DIMENSIONS, null, randomBoolean(), null),
                ConfigurationParseContext.REQUEST
            )
        );

        assertThat(thrownException.validationErrors().size(), is(1));
        assertThat(
            thrownException.validationErrors().getFirst(),
            is(Strings.format("[service_settings] does not allow the setting [%s]", DIMENSIONS_SET_BY_USER))
        );
    }

    public void testFromMap_InvalidSimilarity_ThrowsValidationError() {
        var invalidSimilarity = "by_size";
        var thrownException = expectThrows(
            ValidationException.class,
            () -> VoyageAIEmbeddingsServiceSettings.fromMap(
                buildServiceSettingsMap(TEST_MODEL_ID, null, invalidSimilarity, null, null, null, null),
                randomFrom(ConfigurationParseContext.values())
            )
        );

        assertThat(thrownException.validationErrors().size(), is(1));
        assertThat(
            thrownException.validationErrors().getFirst(),
            is(
                Strings.format(
                    "[service_settings] Invalid value [%s] received. [similarity] must be one of [cosine, dot_product, l2_norm]",
                    invalidSimilarity
                )
            )
        );
    }

    public void testUpdateServiceSettings_AllFields_OnlyMutableFieldsAreUpdated() {
        var initialDimensionsSetByUser = randomBoolean();
        var originalServiceSettings = new VoyageAIEmbeddingsServiceSettings(
            new VoyageAICommonServiceSettings(INITIAL_TEST_MODEL_ID, new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)),
            INITIAL_TEST_EMBEDDING_TYPE,
            INITIAL_TEST_SIMILARITY_MEASURE,
            INITIAL_TEST_DIMENSIONS,
            INITIAL_TEST_MAX_INPUT_TOKENS,
            initialDimensionsSetByUser
        );

        var updatedServiceSettings = originalServiceSettings.updateServiceSettings(
            buildServiceSettingsMap(
                TEST_MODEL_ID,
                TEST_EMBEDDING_TYPE.toString(),
                TEST_SIMILARITY_MEASURE.toString(),
                TEST_DIMENSIONS,
                TEST_MAX_INPUT_TOKENS,
                initialDimensionsSetByUser == false,
                TEST_RATE_LIMIT
            )
        );

        assertThat(
            updatedServiceSettings,
            is(
                new VoyageAIEmbeddingsServiceSettings(
                    new VoyageAICommonServiceSettings(INITIAL_TEST_MODEL_ID, new RateLimitSettings(TEST_RATE_LIMIT)),
                    INITIAL_TEST_EMBEDDING_TYPE,
                    INITIAL_TEST_SIMILARITY_MEASURE,
                    INITIAL_TEST_DIMENSIONS,
                    TEST_MAX_INPUT_TOKENS,
                    initialDimensionsSetByUser
                )
            )
        );
    }

    public void testUpdateServiceSettings_EmptyMap_DoesNotChangeSettings() {
        var originalServiceSettings = new VoyageAIEmbeddingsServiceSettings(
            new VoyageAICommonServiceSettings(INITIAL_TEST_MODEL_ID, new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)),
            INITIAL_TEST_EMBEDDING_TYPE,
            INITIAL_TEST_SIMILARITY_MEASURE,
            INITIAL_TEST_DIMENSIONS,
            INITIAL_TEST_MAX_INPUT_TOKENS,
            randomBoolean()
        );

        assertThat(originalServiceSettings.updateServiceSettings(new HashMap<>()), is(originalServiceSettings));
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var serviceSettings = new VoyageAIEmbeddingsServiceSettings(
            new VoyageAICommonServiceSettings(TEST_MODEL_ID, new RateLimitSettings(TEST_RATE_LIMIT)),
            TEST_EMBEDDING_TYPE,
            TEST_SIMILARITY_MEASURE,
            TEST_DIMENSIONS,
            TEST_MAX_INPUT_TOKENS,
            randomBoolean()
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is(XContentHelper.stripWhitespace(Strings.format("""
            {
                "model_id": "%s",
                "rate_limit": {
                    "requests_per_minute": %d
                },
                "similarity": "%s",
                "dimensions": %d,
                "max_input_tokens": %d,
                "embedding_type": "%s"
            }
            """, TEST_MODEL_ID, TEST_RATE_LIMIT, TEST_SIMILARITY_MEASURE, TEST_DIMENSIONS, TEST_MAX_INPUT_TOKENS, TEST_EMBEDDING_TYPE))));
    }

    public void testToXContent_OnlyMandatoryFields_WritesOnlyMandatoryFieldsAndDefaults() throws IOException {
        var serviceSettings = new VoyageAIEmbeddingsServiceSettings(
            new VoyageAICommonServiceSettings(TEST_MODEL_ID, null),
            null,
            null,
            null,
            null,
            randomBoolean()
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is(XContentHelper.stripWhitespace(Strings.format("""
            {
                "model_id": "%s",
                "rate_limit": {
                    "requests_per_minute": %d
                }
            }
            """, TEST_MODEL_ID, DEFAULT_RATE_LIMIT))));
    }

    @Override
    protected Writeable.Reader<VoyageAIEmbeddingsServiceSettings> instanceReader() {
        return VoyageAIEmbeddingsServiceSettings::new;
    }

    @Override
    protected VoyageAIEmbeddingsServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected VoyageAIEmbeddingsServiceSettings mutateInstance(VoyageAIEmbeddingsServiceSettings instance) throws IOException {
        var commonSettings = instance.getCommonSettings();
        var embeddingType = instance.getEmbeddingType();
        var similarity = instance.similarity();
        var dimensions = instance.dimensions();
        var maxInputTokens = instance.maxInputTokens();
        var dimensionsSetByUser = instance.dimensionsSetByUser();
        switch (randomInt(5)) {
            case 0 -> commonSettings = randomValueOtherThan(commonSettings, VoyageAICommonServiceSettingsTests::createRandom);
            case 1 -> embeddingType = randomValueOtherThan(embeddingType, () -> randomFrom(VoyageAIEmbeddingType.values()));
            case 2 -> similarity = randomValueOtherThan(similarity, () -> randomFrom(randomSimilarityMeasure(), null));
            case 3 -> dimensions = randomValueOtherThan(dimensions, () -> randomFrom(randomIntBetween(1, 2048), null));
            case 4 -> maxInputTokens = randomValueOtherThan(maxInputTokens, () -> randomFrom(randomIntBetween(128, 2048), null));
            case 5 -> dimensionsSetByUser = dimensionsSetByUser == false;
            default -> throw new AssertionError("Illegal randomisation branch");
        }

        return new VoyageAIEmbeddingsServiceSettings(
            commonSettings,
            embeddingType,
            similarity,
            dimensions,
            maxInputTokens,
            dimensionsSetByUser
        );
    }

    public static Map<String, Object> buildServiceSettingsMap(String modelId) {
        return buildServiceSettingsMap(modelId, null, null, null, null, null, null);
    }

    private static Map<String, Object> buildServiceSettingsMap(
        @Nullable String modelId,
        @Nullable String embeddingType,
        @Nullable String similarity,
        @Nullable Integer dimensions,
        @Nullable Integer maxInputTokens,
        @Nullable Boolean dimensionsSetByUser,
        @Nullable Integer rateLimit
    ) {
        var map = new HashMap<String, Object>();
        if (modelId != null) {
            map.put(ServiceFields.MODEL_ID, modelId);
        }
        if (embeddingType != null) {
            map.put(ServiceFields.EMBEDDING_TYPE, embeddingType);
        }
        if (similarity != null) {
            map.put(ServiceFields.SIMILARITY, similarity);
        }
        if (dimensions != null) {
            map.put(ServiceFields.DIMENSIONS, dimensions);
        }
        if (maxInputTokens != null) {
            map.put(ServiceFields.MAX_INPUT_TOKENS, maxInputTokens);
        }
        if (dimensionsSetByUser != null) {
            map.put(DIMENSIONS_SET_BY_USER, dimensionsSetByUser);
        }
        if (rateLimit != null) {
            map.put(RateLimitSettings.FIELD_NAME, new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, rateLimit)));
        }
        return map;
    }

    @Override
    protected VoyageAIEmbeddingsServiceSettings mutateInstanceForVersion(
        VoyageAIEmbeddingsServiceSettings instance,
        TransportVersion version
    ) {
        return instance;
    }
}
