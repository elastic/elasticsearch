/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock.embeddings;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockProvider;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.Utils.randomSimilarityMeasure;
import static org.elasticsearch.xpack.inference.services.ServiceFields.DIMENSIONS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.DIMENSIONS_SET_BY_USER;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MAX_INPUT_TOKENS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.SIMILARITY;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockConstants.MODEL_FIELD;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockConstants.PROVIDER_FIELD;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockConstants.REGION_FIELD;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class AmazonBedrockEmbeddingsServiceSettingsTests extends AbstractBWCWireSerializationTestCase<
    AmazonBedrockEmbeddingsServiceSettings> {
    private static final String TEST_REGION = "test-region";
    private static final String INITIAL_TEST_REGION = "initial-test-region";
    private static final String TEST_MODEL_ID = "test-model-id";
    private static final String INITIAL_TEST_MODEL_ID = "initial-test-model-id";
    private static final AmazonBedrockProvider TEST_PROVIDER = AmazonBedrockProvider.AMAZONTITAN;
    private static final AmazonBedrockProvider INITIAL_TEST_PROVIDER = AmazonBedrockProvider.AI21LABS;
    private static final int TEST_DIMENSIONS = 1536;
    private static final int INITIAL_TEST_DIMENSIONS = 1536;
    private static final boolean TEST_DIMENSIONS_SET_BY_USER = true;
    private static final boolean INITIAL_TEST_DIMENSIONS_SET_BY_USER = false;
    private static final int TEST_MAX_INPUT_TOKENS = 512;
    private static final int INITIAL_TEST_MAX_INPUT_TOKENS = 1024;
    private static final SimilarityMeasure TEST_SIMILARITY = SimilarityMeasure.COSINE;
    private static final SimilarityMeasure INITIAL_TEST_SIMILARITY = SimilarityMeasure.DOT_PRODUCT;
    private static final int TEST_RATE_LIMIT = 20;
    private static final int INITIAL_TEST_RATE_LIMIT = 30;

    public void testUpdateServiceSettings_AllFields_Success() {
        var newSettingsMap = createEmbeddingsRequestSettingsMap(
            TEST_REGION,
            TEST_MODEL_ID,
            TEST_PROVIDER.toString(),
            TEST_DIMENSIONS,
            TEST_DIMENSIONS_SET_BY_USER,
            TEST_MAX_INPUT_TOKENS,
            TEST_SIMILARITY,
            TEST_RATE_LIMIT
        );
        var serviceSettings = new AmazonBedrockEmbeddingsServiceSettings(
            INITIAL_TEST_REGION,
            INITIAL_TEST_MODEL_ID,
            INITIAL_TEST_PROVIDER,
            INITIAL_TEST_DIMENSIONS,
            INITIAL_TEST_DIMENSIONS_SET_BY_USER,
            INITIAL_TEST_MAX_INPUT_TOKENS,
            INITIAL_TEST_SIMILARITY,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        ).updateServiceSettings(newSettingsMap, TaskType.TEXT_EMBEDDING);

        assertThat(
            serviceSettings,
            is(
                new AmazonBedrockEmbeddingsServiceSettings(
                    TEST_REGION,
                    TEST_MODEL_ID,
                    TEST_PROVIDER,
                    TEST_DIMENSIONS,
                    TEST_DIMENSIONS_SET_BY_USER,
                    TEST_MAX_INPUT_TOKENS,
                    TEST_SIMILARITY,
                    new RateLimitSettings(TEST_RATE_LIMIT)
                )
            )
        );
    }

    public void testUpdateServiceSettings_EmptyMap_Success() {
        var serviceSettings = new AmazonBedrockEmbeddingsServiceSettings(
            INITIAL_TEST_REGION,
            INITIAL_TEST_MODEL_ID,
            INITIAL_TEST_PROVIDER,
            INITIAL_TEST_DIMENSIONS,
            INITIAL_TEST_DIMENSIONS_SET_BY_USER,
            INITIAL_TEST_MAX_INPUT_TOKENS,
            INITIAL_TEST_SIMILARITY,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        ).updateServiceSettings(new HashMap<>(), TaskType.TEXT_EMBEDDING);

        assertThat(
            serviceSettings,
            is(
                new AmazonBedrockEmbeddingsServiceSettings(
                    INITIAL_TEST_REGION,
                    INITIAL_TEST_MODEL_ID,
                    INITIAL_TEST_PROVIDER,
                    INITIAL_TEST_DIMENSIONS,
                    INITIAL_TEST_DIMENSIONS_SET_BY_USER,
                    INITIAL_TEST_MAX_INPUT_TOKENS,
                    INITIAL_TEST_SIMILARITY,
                    new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
                )
            )
        );
    }

    public void testFromMap_Request_CreatesSettingsCorrectly() {
        var serviceSettings = AmazonBedrockEmbeddingsServiceSettings.fromMap(
            createEmbeddingsRequestSettingsMap(
                TEST_REGION,
                TEST_MODEL_ID,
                TEST_PROVIDER.toString(),
                null,
                null,
                TEST_MAX_INPUT_TOKENS,
                TEST_SIMILARITY
            ),
            ConfigurationParseContext.REQUEST
        );

        assertThat(
            serviceSettings,
            is(
                new AmazonBedrockEmbeddingsServiceSettings(
                    TEST_REGION,
                    TEST_MODEL_ID,
                    TEST_PROVIDER,
                    null,
                    false,
                    TEST_MAX_INPUT_TOKENS,
                    TEST_SIMILARITY,
                    null
                )
            )
        );
    }

    public void testFromMap_RequestWithRateLimit_CreatesSettingsCorrectly() {
        var settingsMap = createEmbeddingsRequestSettingsMap(
            TEST_REGION,
            TEST_MODEL_ID,
            TEST_PROVIDER.toString(),
            null,
            null,
            TEST_MAX_INPUT_TOKENS,
            TEST_SIMILARITY,
            TEST_RATE_LIMIT
        );

        var serviceSettings = AmazonBedrockEmbeddingsServiceSettings.fromMap(settingsMap, ConfigurationParseContext.REQUEST);

        assertThat(
            serviceSettings,
            is(
                new AmazonBedrockEmbeddingsServiceSettings(
                    TEST_REGION,
                    TEST_MODEL_ID,
                    TEST_PROVIDER,
                    null,
                    false,
                    TEST_MAX_INPUT_TOKENS,
                    TEST_SIMILARITY,
                    new RateLimitSettings(TEST_RATE_LIMIT)
                )
            )
        );
    }

    private static HashMap<String, Object> createEmbeddingsRequestSettingsMap(
        String region,
        String model,
        String provider,
        @Nullable Integer dimensions,
        @Nullable Boolean dimensionsSetByUser,
        @Nullable Integer maxTokens,
        @Nullable SimilarityMeasure similarityMeasure,
        int rateLimit
    ) {
        var settingsMap = createEmbeddingsRequestSettingsMap(
            region,
            model,
            provider,
            dimensions,
            dimensionsSetByUser,
            maxTokens,
            similarityMeasure
        );
        settingsMap.put(RateLimitSettings.FIELD_NAME, new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, rateLimit)));
        return settingsMap;
    }

    public void testFromMap_Request_DimensionsSetByUser_IsFalse_WhenDimensionsAreNotPresent() {
        var settingsMap = createEmbeddingsRequestSettingsMap(
            TEST_REGION,
            TEST_MODEL_ID,
            TEST_PROVIDER.toString(),
            null,
            null,
            TEST_MAX_INPUT_TOKENS,
            TEST_SIMILARITY
        );
        var serviceSettings = AmazonBedrockEmbeddingsServiceSettings.fromMap(settingsMap, ConfigurationParseContext.REQUEST);

        assertThat(
            serviceSettings,
            is(
                new AmazonBedrockEmbeddingsServiceSettings(
                    TEST_REGION,
                    TEST_MODEL_ID,
                    TEST_PROVIDER,
                    null,
                    false,
                    TEST_MAX_INPUT_TOKENS,
                    TEST_SIMILARITY,
                    null
                )
            )
        );
    }

    public void testFromMap_Request_DimensionsSetByUser_ShouldThrowWhenPresent() {
        var settingsMap = createEmbeddingsRequestSettingsMap(
            TEST_REGION,
            TEST_MODEL_ID,
            TEST_PROVIDER.toString(),
            null,
            true,
            TEST_MAX_INPUT_TOKENS,
            TEST_SIMILARITY
        );

        var thrownException = expectThrows(
            ValidationException.class,
            () -> AmazonBedrockEmbeddingsServiceSettings.fromMap(settingsMap, ConfigurationParseContext.REQUEST)
        );

        MatcherAssert.assertThat(
            thrownException.getMessage(),
            containsString(
                Strings.format("Validation Failed: 1: [service_settings] does not allow the setting [%s];", DIMENSIONS_SET_BY_USER)
            )
        );
    }

    public void testFromMap_Request_Dimensions_ShouldThrowWhenPresent() {
        var settingsMap = createEmbeddingsRequestSettingsMap(
            TEST_REGION,
            TEST_MODEL_ID,
            TEST_PROVIDER.toString(),
            TEST_DIMENSIONS,
            null,
            null,
            null,
            TEST_RATE_LIMIT
        );

        var thrownException = expectThrows(
            ValidationException.class,
            () -> AmazonBedrockEmbeddingsServiceSettings.fromMap(settingsMap, ConfigurationParseContext.REQUEST)
        );

        MatcherAssert.assertThat(
            thrownException.getMessage(),
            containsString(Strings.format("[service_settings] does not allow the setting [%s]", DIMENSIONS))
        );
    }

    public void testFromMap_Request_MaxTokensShouldBePositiveInteger() {
        var maxInputTokens = -128;

        var settingsMap = createEmbeddingsRequestSettingsMap(
            TEST_REGION,
            TEST_MODEL_ID,
            TEST_PROVIDER.toString(),
            null,
            null,
            maxInputTokens,
            null
        );

        var thrownException = expectThrows(
            ValidationException.class,
            () -> AmazonBedrockEmbeddingsServiceSettings.fromMap(settingsMap, ConfigurationParseContext.REQUEST)
        );

        MatcherAssert.assertThat(
            thrownException.getMessage(),
            containsString(Strings.format("[%s] must be a positive integer", MAX_INPUT_TOKENS))
        );
    }

    public void testFromMap_Persistent_CreatesSettingsCorrectly() {
        var settingsMap = createEmbeddingsRequestSettingsMap(
            TEST_REGION,
            TEST_MODEL_ID,
            TEST_PROVIDER.toString(),
            TEST_DIMENSIONS,
            false,
            TEST_MAX_INPUT_TOKENS,
            TEST_SIMILARITY
        );
        var serviceSettings = AmazonBedrockEmbeddingsServiceSettings.fromMap(settingsMap, ConfigurationParseContext.PERSISTENT);

        assertThat(
            serviceSettings,
            is(
                new AmazonBedrockEmbeddingsServiceSettings(
                    TEST_REGION,
                    TEST_MODEL_ID,
                    TEST_PROVIDER,
                    TEST_DIMENSIONS,
                    false,
                    TEST_MAX_INPUT_TOKENS,
                    TEST_SIMILARITY,
                    null
                )
            )
        );
    }

    public void testFromMap_PersistentContext_DoesNotThrowException_WhenDimensionsIsNull() {
        var settingsMap = createEmbeddingsRequestSettingsMap(TEST_REGION, TEST_MODEL_ID, TEST_PROVIDER.toString(), null, true, null, null);
        var serviceSettings = AmazonBedrockEmbeddingsServiceSettings.fromMap(settingsMap, ConfigurationParseContext.PERSISTENT);

        assertThat(
            serviceSettings,
            is(new AmazonBedrockEmbeddingsServiceSettings(TEST_REGION, TEST_MODEL_ID, TEST_PROVIDER, null, true, null, null, null))
        );
    }

    public void testFromMap_PersistentContext_DoesNotThrowException_WhenSimilarityIsPresent() {
        var settingsMap = createEmbeddingsRequestSettingsMap(
            TEST_REGION,
            TEST_MODEL_ID,
            TEST_PROVIDER.toString(),
            null,
            true,
            null,
            TEST_SIMILARITY
        );
        var serviceSettings = AmazonBedrockEmbeddingsServiceSettings.fromMap(settingsMap, ConfigurationParseContext.PERSISTENT);

        assertThat(
            serviceSettings,
            is(
                new AmazonBedrockEmbeddingsServiceSettings(
                    TEST_REGION,
                    TEST_MODEL_ID,
                    TEST_PROVIDER,
                    null,
                    true,
                    null,
                    TEST_SIMILARITY,
                    null
                )
            )
        );
    }

    public void testFromMap_PersistentContext_ThrowsException_WhenDimensionsSetByUserIsNull() {
        var settingsMap = createEmbeddingsRequestSettingsMap(
            TEST_REGION,
            TEST_MODEL_ID,
            TEST_PROVIDER.toString(),
            TEST_DIMENSIONS,
            null,
            null,
            null,
            TEST_RATE_LIMIT
        );

        var exception = expectThrows(
            ValidationException.class,
            () -> AmazonBedrockEmbeddingsServiceSettings.fromMap(settingsMap, ConfigurationParseContext.PERSISTENT)
        );

        assertThat(
            exception.getMessage(),
            containsString("Validation Failed: 1: [service_settings] does not contain the required setting [dimensions_set_by_user];")
        );
    }

    public void testToXContent_WritesDimensionsSetByUserTrue() throws IOException {
        var entity = new AmazonBedrockEmbeddingsServiceSettings(
            TEST_REGION,
            TEST_MODEL_ID,
            TEST_PROVIDER,
            null,
            TEST_DIMENSIONS_SET_BY_USER,
            null,
            null,
            new RateLimitSettings(TEST_RATE_LIMIT)
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(
            xContentResult,
            is(
                Strings.format(
                    """
                        {"region":"%s","model":"%s","provider":"%s",""" + """
                        "rate_limit":{"requests_per_minute":%d},"dimensions_set_by_user":%b}""",
                    TEST_REGION,
                    TEST_MODEL_ID,
                    TEST_PROVIDER.name(),
                    TEST_RATE_LIMIT,
                    TEST_DIMENSIONS_SET_BY_USER
                )
            )
        );
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var entity = new AmazonBedrockEmbeddingsServiceSettings(
            TEST_REGION,
            TEST_MODEL_ID,
            TEST_PROVIDER,
            TEST_DIMENSIONS,
            false,
            TEST_MAX_INPUT_TOKENS,
            null,
            new RateLimitSettings(TEST_RATE_LIMIT)
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(
            xContentResult,
            is(
                Strings.format(
                    """
                        {"region":"%s","model":"%s","provider":"%s","rate_limit":{"requests_per_minute":%d},\
                        "dimensions":%d,"max_input_tokens":%d,"dimensions_set_by_user":%b}""",
                    TEST_REGION,
                    TEST_MODEL_ID,
                    TEST_PROVIDER.name(),
                    TEST_RATE_LIMIT,
                    TEST_DIMENSIONS,
                    TEST_MAX_INPUT_TOKENS,
                    false
                )
            )
        );
    }

    public void testToFilteredXContent_WritesAllValues_ExceptDimensionsSetByUser() throws IOException {
        var entity = new AmazonBedrockEmbeddingsServiceSettings(
            TEST_REGION,
            TEST_MODEL_ID,
            TEST_PROVIDER,
            TEST_DIMENSIONS,
            false,
            TEST_MAX_INPUT_TOKENS,
            null,
            new RateLimitSettings(TEST_RATE_LIMIT)
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        var filteredXContent = entity.getFilteredXContentObject();
        filteredXContent.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(
            xContentResult,
            is(
                Strings.format(
                    """
                        {"region":"%s","model":"%s","provider":"%s","rate_limit":{"requests_per_minute":%d},\
                        "dimensions":%d,"max_input_tokens":%d}""",
                    TEST_REGION,
                    TEST_MODEL_ID,
                    TEST_PROVIDER.name(),
                    TEST_RATE_LIMIT,
                    TEST_DIMENSIONS,
                    TEST_MAX_INPUT_TOKENS
                )
            )
        );
    }

    public static HashMap<String, Object> createEmbeddingsRequestSettingsMap(
        String region,
        String model,
        String provider,
        @Nullable Integer dimensions,
        @Nullable Boolean dimensionsSetByUser,
        @Nullable Integer maxTokens,
        @Nullable SimilarityMeasure similarityMeasure
    ) {
        var map = new HashMap<String, Object>(Map.of(REGION_FIELD, region, MODEL_FIELD, model, PROVIDER_FIELD, provider));

        if (dimensions != null) {
            map.put(ServiceFields.DIMENSIONS, dimensions);
        }

        if (dimensionsSetByUser != null) {
            map.put(DIMENSIONS_SET_BY_USER, dimensionsSetByUser.equals(Boolean.TRUE));
        }

        if (maxTokens != null) {
            map.put(ServiceFields.MAX_INPUT_TOKENS, maxTokens);
        }

        if (similarityMeasure != null) {
            map.put(SIMILARITY, similarityMeasure.toString());
        }

        return map;
    }

    @Override
    protected AmazonBedrockEmbeddingsServiceSettings mutateInstanceForVersion(
        AmazonBedrockEmbeddingsServiceSettings instance,
        TransportVersion version
    ) {
        return instance;
    }

    @Override
    protected Writeable.Reader<AmazonBedrockEmbeddingsServiceSettings> instanceReader() {
        return AmazonBedrockEmbeddingsServiceSettings::new;
    }

    @Override
    protected AmazonBedrockEmbeddingsServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected AmazonBedrockEmbeddingsServiceSettings mutateInstance(AmazonBedrockEmbeddingsServiceSettings instance) throws IOException {
        var region = instance.region();
        var modelId = instance.modelId();
        var provider = instance.provider();
        var dimensions = instance.dimensions();
        var dimensionsSetByUser = instance.dimensionsSetByUser();
        var maxInputTokens = instance.maxInputTokens();
        var similarity = instance.similarity();
        var rateLimitSettings = instance.rateLimitSettings();
        switch (randomInt(7)) {
            case 0 -> region = randomValueOtherThan(region, () -> randomAlphaOfLength(10));
            case 1 -> modelId = randomValueOtherThan(modelId, () -> randomAlphaOfLength(10));
            case 2 -> provider = randomValueOtherThan(provider, () -> randomFrom(AmazonBedrockProvider.values()));
            case 3 -> dimensions = randomValueOtherThan(dimensions, ESTestCase::randomNonNegativeIntOrNull);
            case 4 -> dimensionsSetByUser = randomValueOtherThan(dimensionsSetByUser, ESTestCase::randomBoolean);
            case 5 -> maxInputTokens = randomValueOtherThan(maxInputTokens, ESTestCase::randomNonNegativeIntOrNull);
            case 6 -> similarity = randomValueOtherThan(similarity, AmazonBedrockEmbeddingsServiceSettingsTests::randomSimilarityOrNull);
            case 7 -> rateLimitSettings = randomValueOtherThan(rateLimitSettings, RateLimitSettingsTests::createRandom);
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new AmazonBedrockEmbeddingsServiceSettings(
            region,
            modelId,
            provider,
            dimensions,
            dimensionsSetByUser,
            maxInputTokens,
            similarity,
            rateLimitSettings
        );
    }

    private static AmazonBedrockEmbeddingsServiceSettings createRandom() {
        return new AmazonBedrockEmbeddingsServiceSettings(
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomFrom(AmazonBedrockProvider.values()),
            randomNonNegativeIntOrNull(),
            randomBoolean(),
            randomNonNegativeIntOrNull(),
            randomSimilarityOrNull(),
            RateLimitSettingsTests.createRandom()
        );
    }

    private static SimilarityMeasure randomSimilarityOrNull() {
        return randomFrom(new SimilarityMeasure[] { null, randomSimilarityMeasure() });
    }
}
