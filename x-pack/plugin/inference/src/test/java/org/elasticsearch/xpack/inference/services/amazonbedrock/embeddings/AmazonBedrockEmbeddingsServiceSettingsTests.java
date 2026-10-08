/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock.embeddings;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AbstractAmazonBedrockServiceSettingsTests;
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
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.is;

public class AmazonBedrockEmbeddingsServiceSettingsTests extends AbstractAmazonBedrockServiceSettingsTests<
    AmazonBedrockEmbeddingsServiceSettings> {
    public static final boolean TEST_DIMENSIONS_SET_BY_USER = false;
    public static final boolean INITIAL_TEST_DIMENSIONS_SET_BY_USER = true;
    private static final int TEST_DIMENSIONS = 1536;
    private static final int INITIAL_TEST_DIMENSIONS = 1536;
    private static final int TEST_MAX_INPUT_TOKENS = 512;
    private static final int INITIAL_TEST_MAX_INPUT_TOKENS = 1024;
    private static final SimilarityMeasure TEST_SIMILARITY = SimilarityMeasure.COSINE;
    private static final SimilarityMeasure INITIAL_TEST_SIMILARITY = SimilarityMeasure.DOT_PRODUCT;

    @Override
    protected AmazonBedrockEmbeddingsServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        return AmazonBedrockEmbeddingsServiceSettings.fromMap(map, context);
    }

    @Override
    protected Map<String, Object> buildCommonServiceSettingsMap(String region, String model, String provider, Integer rateLimit) {
        return buildServiceSettingsMap(region, model, provider, null, null, null, null, rateLimit);
    }

    @Override
    protected AmazonBedrockEmbeddingsServiceSettings createServiceSettings(
        String region,
        String model,
        AmazonBedrockProvider provider,
        RateLimitSettings rateLimitSettings
    ) {
        return new AmazonBedrockEmbeddingsServiceSettings(region, model, provider, null, false, null, null, rateLimitSettings);
    }

    @Override
    protected AmazonBedrockEmbeddingsServiceSettings doParseInstance(XContentParser parser) throws IOException {
        return AmazonBedrockEmbeddingsServiceSettings.createParser(true)
            .apply(parser, ConfigurationParseContext.PERSISTENT)
            .build(ConfigurationParseContext.PERSISTENT);
    }

    public void testUpdateServiceSettings_AllFields_OnlyMutableFieldsAreUpdated() {
        var settingsMap = new HashMap<String, Object>();
        settingsMap.put(ServiceFields.MAX_INPUT_TOKENS, TEST_MAX_INPUT_TOKENS);
        settingsMap.put(RateLimitSettings.FIELD_NAME, new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, TEST_RATE_LIMIT)));
        var originalServiceSettings = new AmazonBedrockEmbeddingsServiceSettings(
            INITIAL_TEST_REGION,
            INITIAL_TEST_MODEL_ID,
            INITIAL_TEST_PROVIDER,
            INITIAL_TEST_DIMENSIONS,
            INITIAL_TEST_DIMENSIONS_SET_BY_USER,
            INITIAL_TEST_MAX_INPUT_TOKENS,
            INITIAL_TEST_SIMILARITY,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );
        var updatedServiceSettings = originalServiceSettings.updateServiceSettings(settingsMap);

        assertThat(
            updatedServiceSettings,
            is(
                new AmazonBedrockEmbeddingsServiceSettings(
                    INITIAL_TEST_REGION,
                    INITIAL_TEST_MODEL_ID,
                    INITIAL_TEST_PROVIDER,
                    INITIAL_TEST_DIMENSIONS,
                    INITIAL_TEST_DIMENSIONS_SET_BY_USER,
                    TEST_MAX_INPUT_TOKENS,
                    INITIAL_TEST_SIMILARITY,
                    new RateLimitSettings(TEST_RATE_LIMIT)
                )
            )
        );
    }

    public void testUpdateServiceSettings_Request_Dimensions_ShouldThrowWhenPresent() {
        var settingsMap = new HashMap<String, Object>();
        settingsMap.put(DIMENSIONS, TEST_DIMENSIONS);
        var originalServiceSettings = new AmazonBedrockEmbeddingsServiceSettings(
            INITIAL_TEST_REGION,
            INITIAL_TEST_MODEL_ID,
            INITIAL_TEST_PROVIDER,
            INITIAL_TEST_DIMENSIONS,
            INITIAL_TEST_DIMENSIONS_SET_BY_USER,
            INITIAL_TEST_MAX_INPUT_TOKENS,
            INITIAL_TEST_SIMILARITY,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );

        var thrownException = expectThrows(
            IllegalArgumentException.class,
            () -> originalServiceSettings.updateServiceSettings(settingsMap)
        );

        assertThat(
            thrownException.getMessage(),
            endsWith(Strings.format("[%s] unknown field [%s]", ModelConfigurations.SERVICE_SETTINGS, DIMENSIONS))
        );
    }

    public void testUpdateServiceSettings_Request_DimensionsSetByUser_ShouldThrowWhenPresent() {
        var settingsMap = new HashMap<String, Object>();
        settingsMap.put(DIMENSIONS_SET_BY_USER, Boolean.TRUE);
        var originalServiceSettings = new AmazonBedrockEmbeddingsServiceSettings(
            INITIAL_TEST_REGION,
            INITIAL_TEST_MODEL_ID,
            INITIAL_TEST_PROVIDER,
            INITIAL_TEST_DIMENSIONS,
            INITIAL_TEST_DIMENSIONS_SET_BY_USER,
            INITIAL_TEST_MAX_INPUT_TOKENS,
            INITIAL_TEST_SIMILARITY,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );

        var thrownException = expectThrows(
            IllegalArgumentException.class,
            () -> originalServiceSettings.updateServiceSettings(settingsMap)
        );

        assertThat(
            thrownException.getMessage(),
            endsWith(Strings.format("[%s] unknown field [%s]", ModelConfigurations.SERVICE_SETTINGS, DIMENSIONS_SET_BY_USER))
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
            XContentParseException.class,
            () -> AmazonBedrockEmbeddingsServiceSettings.fromMap(settingsMap, ConfigurationParseContext.REQUEST)
        );

        MatcherAssert.assertThat(
            thrownException.getMessage(),
            containsString(Strings.format("[service_settings] unknown field [%s]", DIMENSIONS_SET_BY_USER))
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
            XContentParseException.class,
            () -> AmazonBedrockEmbeddingsServiceSettings.fromMap(settingsMap, ConfigurationParseContext.REQUEST)
        );

        MatcherAssert.assertThat(
            thrownException.getMessage(),
            containsString(Strings.format("[service_settings] unknown field [%s]", DIMENSIONS))
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
            IllegalArgumentException.class,
            () -> AmazonBedrockEmbeddingsServiceSettings.fromMap(settingsMap, ConfigurationParseContext.REQUEST)
        );

        assertThat(
            thrownException.getMessage(),
            endsWith(Strings.format("[%s] failed to parse field [%s]", ModelConfigurations.SERVICE_SETTINGS, MAX_INPUT_TOKENS))
        );
        assertThat(
            thrownException.getCause().getMessage(),
            is(
                Strings.format(
                    "[%s] Invalid value [%d]. [%s] must be a positive integer",
                    ModelConfigurations.SERVICE_SETTINGS,
                    maxInputTokens,
                    MAX_INPUT_TOKENS
                )
            )
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

    public void testFromMap_PersistentContext_DoesNotThrowException_WhenDimensionsSetByUserIsNull() {
        var settingsMap = createEmbeddingsRequestSettingsMap(
            TEST_REGION,
            TEST_MODEL_ID,
            TEST_PROVIDER.toString(),
            TEST_DIMENSIONS,
            null,
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

    public void testToXContent_WritesDimensionsSetByUserTrue() throws IOException {
        var entity = new AmazonBedrockEmbeddingsServiceSettings(
            TEST_REGION,
            TEST_MODEL_ID,
            TEST_PROVIDER,
            null,
            true,
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
                    true
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

    public static Map<String, Object> buildServiceSettingsMap(
        @Nullable String region,
        @Nullable String model,
        @Nullable String provider,
        @Nullable String dimensions,
        @Nullable Boolean dimensionsSetByUser,
        @Nullable Integer maxInputTokens,
        @Nullable String similarity,
        @Nullable Integer rateLimit
    ) {
        var map = new HashMap<String, Object>();
        if (region != null) {
            map.put(REGION_FIELD, region);
        }
        if (model != null) {
            map.put(MODEL_FIELD, model);
        }
        if (provider != null) {
            map.put(PROVIDER_FIELD, provider);
        }
        if (dimensions != null) {
            map.put(DIMENSIONS, dimensions);
        }
        if (dimensionsSetByUser != null) {
            map.put(DIMENSIONS_SET_BY_USER, dimensionsSetByUser);
        }
        if (maxInputTokens != null) {
            map.put(MAX_INPUT_TOKENS, maxInputTokens);
        }
        if (similarity != null) {
            map.put(SIMILARITY, similarity);
        }
        if (rateLimit != null) {
            map.put(RateLimitSettings.FIELD_NAME, new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, rateLimit)));
        }
        return map;
    }

}
