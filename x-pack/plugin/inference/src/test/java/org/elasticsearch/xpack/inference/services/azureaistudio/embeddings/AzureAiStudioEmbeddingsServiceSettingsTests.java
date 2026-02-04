/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureaistudio.embeddings;

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
import org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioConstants;
import org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioEndpointType;
import org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioProvider;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.Utils.randomSimilarityMeasure;
import static org.elasticsearch.xpack.inference.services.ServiceFields.SIMILARITY;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class AzureAiStudioEmbeddingsServiceSettingsTests extends AbstractBWCWireSerializationTestCase<
    AzureAiStudioEmbeddingsServiceSettings> {
    private static final String TEST_TARGET = "http://sometarget.local";
    private static final String INITIAL_TEST_TARGET = "http://initialtarget.local";
    private static final AzureAiStudioProvider TEST_PROVIDER = AzureAiStudioProvider.OPENAI;
    private static final AzureAiStudioProvider INITIAL_TEST_PROVIDER = AzureAiStudioProvider.MISTRAL;
    private static final AzureAiStudioEndpointType TEST_ENDPOINT_TYPE = AzureAiStudioEndpointType.TOKEN;
    private static final AzureAiStudioEndpointType INITIAL_TEST_ENDPOINT_TYPE = AzureAiStudioEndpointType.REALTIME;
    private static final int TEST_RATE_LIMIT = 20;
    private static final int INITIAL_TEST_RATE_LIMIT = 30;
    private static final int TEST_DIMENSIONS = 1536;
    private static final int INITIAL_TEST_DIMENSIONS = 3072;
    private static final int TEST_MAX_INPUT_TOKENS = 512;
    private static final int INITIAL_TEST_MAX_INPUT_TOKENS = 1024;
    private static final SimilarityMeasure TEST_SIMILARITY_MEASURE = SimilarityMeasure.COSINE;
    private static final SimilarityMeasure INITIAL_SIMILARITY_MEASURE = SimilarityMeasure.DOT_PRODUCT;

    public void testUpdateServiceSettings_AllFields_Success() {
        var settingsMap = createRequestSettingsMap(
            TEST_TARGET,
            TEST_PROVIDER.toString(),
            TEST_ENDPOINT_TYPE.toString(),
            TEST_DIMENSIONS,
            null,
            TEST_MAX_INPUT_TOKENS,
            TEST_SIMILARITY_MEASURE
        );
        settingsMap.put(RateLimitSettings.FIELD_NAME, new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, TEST_RATE_LIMIT)));
        var serviceSettings = new AzureAiStudioEmbeddingsServiceSettings(
            INITIAL_TEST_TARGET,
            INITIAL_TEST_PROVIDER,
            INITIAL_TEST_ENDPOINT_TYPE,
            INITIAL_TEST_DIMENSIONS,
            false,
            INITIAL_TEST_MAX_INPUT_TOKENS,
            INITIAL_SIMILARITY_MEASURE,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        ).updateServiceSettings(settingsMap, TaskType.TEXT_EMBEDDING);

        MatcherAssert.assertThat(
            serviceSettings,
            is(
                new AzureAiStudioEmbeddingsServiceSettings(
                    TEST_TARGET,
                    TEST_PROVIDER,
                    TEST_ENDPOINT_TYPE,
                    TEST_DIMENSIONS,
                    true,
                    TEST_MAX_INPUT_TOKENS,
                    TEST_SIMILARITY_MEASURE,
                    new RateLimitSettings(TEST_RATE_LIMIT)
                )
            )
        );
    }

    public void testUpdateServiceSettings_EmptyMap_DimensionsSetByUserTrue_Success() {
        testUpdateServiceSettings_EmptyMap_Success(false);
    }

    public void testUpdateServiceSettings_EmptyMap_DimensionsSetByUserFalse_Success() {
        testUpdateServiceSettings_EmptyMap_Success(true);
    }

    private static void testUpdateServiceSettings_EmptyMap_Success(boolean dimensionsSetByUser) {
        var serviceSettings = new AzureAiStudioEmbeddingsServiceSettings(
            INITIAL_TEST_TARGET,
            INITIAL_TEST_PROVIDER,
            INITIAL_TEST_ENDPOINT_TYPE,
            INITIAL_TEST_DIMENSIONS,
            dimensionsSetByUser,
            INITIAL_TEST_MAX_INPUT_TOKENS,
            INITIAL_SIMILARITY_MEASURE,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        ).updateServiceSettings(new HashMap<>(), TaskType.TEXT_EMBEDDING);

        MatcherAssert.assertThat(
            serviceSettings,
            is(
                new AzureAiStudioEmbeddingsServiceSettings(
                    INITIAL_TEST_TARGET,
                    INITIAL_TEST_PROVIDER,
                    INITIAL_TEST_ENDPOINT_TYPE,
                    INITIAL_TEST_DIMENSIONS,
                    dimensionsSetByUser,
                    INITIAL_TEST_MAX_INPUT_TOKENS,
                    INITIAL_SIMILARITY_MEASURE,
                    new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
                )
            )
        );
    }

    public void testFromMap_Request_CreatesSettingsCorrectly() {
        var serviceSettings = AzureAiStudioEmbeddingsServiceSettings.fromMap(
            createRequestSettingsMap(
                TEST_TARGET,
                TEST_PROVIDER.toString(),
                TEST_ENDPOINT_TYPE.toString(),
                TEST_DIMENSIONS,
                null,
                TEST_MAX_INPUT_TOKENS,
                TEST_SIMILARITY_MEASURE
            ),
            ConfigurationParseContext.REQUEST
        );

        assertThat(
            serviceSettings,
            is(
                new AzureAiStudioEmbeddingsServiceSettings(
                    TEST_TARGET,
                    TEST_PROVIDER,
                    TEST_ENDPOINT_TYPE,
                    TEST_DIMENSIONS,
                    true,
                    TEST_MAX_INPUT_TOKENS,
                    TEST_SIMILARITY_MEASURE,
                    null
                )
            )
        );
    }

    public void testFromMap_RequestWithRateLimit_CreatesSettingsCorrectly() {
        var settingsMap = createRequestSettingsMap(
            TEST_TARGET,
            TEST_PROVIDER.toString(),
            TEST_ENDPOINT_TYPE.toString(),
            TEST_DIMENSIONS,
            null,
            TEST_MAX_INPUT_TOKENS,
            TEST_SIMILARITY_MEASURE
        );
        settingsMap.put(RateLimitSettings.FIELD_NAME, new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, TEST_RATE_LIMIT)));

        var serviceSettings = AzureAiStudioEmbeddingsServiceSettings.fromMap(settingsMap, ConfigurationParseContext.REQUEST);

        assertThat(
            serviceSettings,
            is(
                new AzureAiStudioEmbeddingsServiceSettings(
                    TEST_TARGET,
                    TEST_PROVIDER,
                    TEST_ENDPOINT_TYPE,
                    TEST_DIMENSIONS,
                    true,
                    TEST_MAX_INPUT_TOKENS,
                    TEST_SIMILARITY_MEASURE,
                    new RateLimitSettings(TEST_RATE_LIMIT)
                )
            )
        );
    }

    public void testFromMap_Request_DimensionsSetByUser_IsFalse_WhenDimensionsAreNotPresent() {
        var settingsMap = createRequestSettingsMap(
            TEST_TARGET,
            TEST_PROVIDER.toString(),
            TEST_ENDPOINT_TYPE.toString(),
            null,
            null,
            TEST_MAX_INPUT_TOKENS,
            TEST_SIMILARITY_MEASURE
        );
        var serviceSettings = AzureAiStudioEmbeddingsServiceSettings.fromMap(settingsMap, ConfigurationParseContext.REQUEST);

        assertThat(
            serviceSettings,
            is(
                new AzureAiStudioEmbeddingsServiceSettings(
                    TEST_TARGET,
                    TEST_PROVIDER,
                    TEST_ENDPOINT_TYPE,
                    null,
                    false,
                    TEST_MAX_INPUT_TOKENS,
                    TEST_SIMILARITY_MEASURE,
                    null
                )
            )
        );
    }

    public void testFromMap_Request_DimensionsSetByUser_ShouldThrowWhenPresent() {
        var settingsMap = createRequestSettingsMap(
            TEST_TARGET,
            TEST_PROVIDER.toString(),
            TEST_ENDPOINT_TYPE.toString(),
            null,
            true,
            TEST_MAX_INPUT_TOKENS,
            TEST_SIMILARITY_MEASURE
        );

        var thrownException = expectThrows(
            ValidationException.class,
            () -> AzureAiStudioEmbeddingsServiceSettings.fromMap(settingsMap, ConfigurationParseContext.REQUEST)
        );

        MatcherAssert.assertThat(
            thrownException.getMessage(),
            containsString(
                Strings.format(
                    "Validation Failed: 1: [service_settings] does not allow the setting [%s];",
                    ServiceFields.DIMENSIONS_SET_BY_USER
                )
            )
        );
    }

    public void testFromMap_Persistent_CreatesSettingsCorrectly() {
        var settingsMap = createRequestSettingsMap(
            TEST_TARGET,
            TEST_PROVIDER.toString(),
            TEST_ENDPOINT_TYPE.toString(),
            TEST_DIMENSIONS,
            false,
            TEST_MAX_INPUT_TOKENS,
            TEST_SIMILARITY_MEASURE
        );
        var serviceSettings = AzureAiStudioEmbeddingsServiceSettings.fromMap(settingsMap, ConfigurationParseContext.PERSISTENT);

        assertThat(
            serviceSettings,
            is(
                new AzureAiStudioEmbeddingsServiceSettings(
                    TEST_TARGET,
                    TEST_PROVIDER,
                    TEST_ENDPOINT_TYPE,
                    TEST_DIMENSIONS,
                    false,
                    TEST_MAX_INPUT_TOKENS,
                    TEST_SIMILARITY_MEASURE,
                    null
                )
            )
        );
    }

    public void testFromMap_ThrowsException_WhenDimensionsAreZero() {
        var dimensions = 0;

        var settingsMap = createRequestSettingsMap(
            TEST_TARGET,
            TEST_PROVIDER.toString(),
            TEST_ENDPOINT_TYPE.toString(),
            dimensions,
            true,
            null,
            TEST_SIMILARITY_MEASURE
        );

        var thrownException = expectThrows(
            ValidationException.class,
            () -> AzureAiStudioEmbeddingsServiceSettings.fromMap(settingsMap, ConfigurationParseContext.REQUEST)
        );

        assertThat(
            thrownException.getMessage(),
            containsString("Validation Failed: 1: [service_settings] Invalid value [0]. [dimensions] must be a positive integer;")
        );
    }

    public void testFromMap_ThrowsException_WhenDimensionsAreNegative() {
        var dimensions = randomNegativeInt();

        var settingsMap = createRequestSettingsMap(
            TEST_TARGET,
            TEST_PROVIDER.toString(),
            TEST_ENDPOINT_TYPE.toString(),
            dimensions,
            true,
            null,
            TEST_SIMILARITY_MEASURE
        );

        var thrownException = expectThrows(
            ValidationException.class,
            () -> AzureAiStudioEmbeddingsServiceSettings.fromMap(settingsMap, ConfigurationParseContext.REQUEST)
        );

        assertThat(
            thrownException.getMessage(),
            containsString(
                Strings.format(
                    "Validation Failed: 1: [service_settings] Invalid value [%d]. [dimensions] must be a positive integer;",
                    dimensions
                )
            )
        );
    }

    public void testFromMap_ThrowsException_WhenMaxInputTokensAreZero() {
        var maxInputTokens = 0;

        var settingsMap = createRequestSettingsMap(
            TEST_TARGET,
            TEST_PROVIDER.toString(),
            TEST_ENDPOINT_TYPE.toString(),
            null,
            true,
            maxInputTokens,
            TEST_SIMILARITY_MEASURE
        );

        var thrownException = expectThrows(
            ValidationException.class,
            () -> AzureAiStudioEmbeddingsServiceSettings.fromMap(settingsMap, ConfigurationParseContext.REQUEST)
        );

        assertThat(
            thrownException.getMessage(),
            containsString("Validation Failed: 1: [service_settings] Invalid value [0]. [max_input_tokens] must be a positive integer;")
        );
    }

    public void testFromMap_ThrowsException_WhenMaxInputTokensAreNegative() {
        var maxInputTokens = randomNegativeInt();

        var settingsMap = createRequestSettingsMap(
            TEST_TARGET,
            TEST_PROVIDER.toString(),
            TEST_ENDPOINT_TYPE.toString(),
            null,
            true,
            maxInputTokens,
            TEST_SIMILARITY_MEASURE
        );

        var thrownException = expectThrows(
            ValidationException.class,
            () -> AzureAiStudioEmbeddingsServiceSettings.fromMap(settingsMap, ConfigurationParseContext.REQUEST)
        );

        assertThat(
            thrownException.getMessage(),
            containsString(
                Strings.format(
                    "Validation Failed: 1: [service_settings] Invalid value [%d]. [max_input_tokens] must be a positive integer;",
                    maxInputTokens
                )
            )
        );
    }

    public void testFromMap_PersistentContext_DoesNotThrowException_WhenDimensionsIsNull() {
        var settingsMap = createRequestSettingsMap(
            TEST_TARGET,
            TEST_PROVIDER.toString(),
            TEST_ENDPOINT_TYPE.toString(),
            null,
            true,
            null,
            null
        );
        var serviceSettings = AzureAiStudioEmbeddingsServiceSettings.fromMap(settingsMap, ConfigurationParseContext.PERSISTENT);

        assertThat(
            serviceSettings,
            is(new AzureAiStudioEmbeddingsServiceSettings(TEST_TARGET, TEST_PROVIDER, TEST_ENDPOINT_TYPE, null, true, null, null, null))
        );
    }

    public void testFromMap_PersistentContext_DoesNotThrowException_WhenSimilarityIsPresent() {
        var settingsMap = createRequestSettingsMap(
            TEST_TARGET,
            TEST_PROVIDER.toString(),
            TEST_ENDPOINT_TYPE.toString(),
            null,
            true,
            null,
            SimilarityMeasure.DOT_PRODUCT
        );
        var serviceSettings = AzureAiStudioEmbeddingsServiceSettings.fromMap(settingsMap, ConfigurationParseContext.PERSISTENT);

        assertThat(
            serviceSettings,
            is(
                new AzureAiStudioEmbeddingsServiceSettings(
                    TEST_TARGET,
                    TEST_PROVIDER,
                    TEST_ENDPOINT_TYPE,
                    null,
                    true,
                    null,
                    SimilarityMeasure.DOT_PRODUCT,
                    null
                )
            )
        );
    }

    public void testFromMap_PersistentContext_ThrowsException_WhenDimensionsSetByUserIsNull() {
        var settingsMap = createRequestSettingsMap(
            TEST_TARGET,
            TEST_PROVIDER.toString(),
            TEST_ENDPOINT_TYPE.toString(),
            TEST_DIMENSIONS,
            null,
            null,
            null
        );

        var exception = expectThrows(
            ValidationException.class,
            () -> AzureAiStudioEmbeddingsServiceSettings.fromMap(settingsMap, ConfigurationParseContext.PERSISTENT)
        );

        assertThat(
            exception.getMessage(),
            containsString("Validation Failed: 1: [service_settings] does not contain the required setting [dimensions_set_by_user];")
        );
    }

    public void testToXContent_WritesDimensionsSetByUserTrue() throws IOException {
        boolean dimensionsSetByUser = true;
        var entity = new AzureAiStudioEmbeddingsServiceSettings(
            TEST_TARGET,
            TEST_PROVIDER,
            TEST_ENDPOINT_TYPE,
            null,
            dimensionsSetByUser,
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
                        {"target":"%s","provider":"%s","endpoint_type":"%s","rate_limit":\
                        {"requests_per_minute":%d},"dimensions_set_by_user":%b}""",
                    TEST_TARGET,
                    TEST_PROVIDER,
                    TEST_ENDPOINT_TYPE,
                    TEST_RATE_LIMIT,
                    dimensionsSetByUser
                )
            )
        );
    }

    public void testToXContent_WritesAllValues() throws IOException {
        boolean dimensionsSetByUser = false;
        var entity = new AzureAiStudioEmbeddingsServiceSettings(
            TEST_TARGET,
            TEST_PROVIDER,
            TEST_ENDPOINT_TYPE,
            TEST_DIMENSIONS,
            dimensionsSetByUser,
            TEST_MAX_INPUT_TOKENS,
            TEST_SIMILARITY_MEASURE,
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
                        {"target":"%s","provider":"%s","endpoint_type":"%s","rate_limit":{"requests_per_minute":%d},\
                        "dimensions":%d,"max_input_tokens":%d,"similarity":"%s","dimensions_set_by_user":%b}""",
                    TEST_TARGET,
                    TEST_PROVIDER,
                    TEST_ENDPOINT_TYPE,
                    TEST_RATE_LIMIT,
                    TEST_DIMENSIONS,
                    TEST_MAX_INPUT_TOKENS,
                    TEST_SIMILARITY_MEASURE,
                    dimensionsSetByUser
                )
            )
        );
    }

    public void testToFilteredXContent_WritesAllValues_ExceptDimensionsSetByUser() throws IOException {
        var entity = new AzureAiStudioEmbeddingsServiceSettings(
            TEST_TARGET,
            TEST_PROVIDER,
            TEST_ENDPOINT_TYPE,
            TEST_DIMENSIONS,
            false,
            TEST_MAX_INPUT_TOKENS,
            TEST_SIMILARITY_MEASURE,
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
                        {"target":"%s","provider":"%s","endpoint_type":"%s",\
                        "rate_limit":{"requests_per_minute":%d},"dimensions":%d,"max_input_tokens":%d,"similarity":"%s"}""",
                    TEST_TARGET,
                    TEST_PROVIDER,
                    TEST_ENDPOINT_TYPE,
                    TEST_RATE_LIMIT,
                    TEST_DIMENSIONS,
                    TEST_MAX_INPUT_TOKENS,
                    TEST_SIMILARITY_MEASURE
                )
            )
        );
    }

    public static HashMap<String, Object> createRequestSettingsMap(
        String target,
        String provider,
        String endpointType,
        @Nullable Integer dimensions,
        @Nullable Boolean dimensionsSetByUser,
        @Nullable Integer maxTokens,
        @Nullable SimilarityMeasure similarityMeasure
    ) {
        var map = new HashMap<String, Object>(
            Map.of(
                AzureAiStudioConstants.TARGET_FIELD,
                target,
                AzureAiStudioConstants.PROVIDER_FIELD,
                provider,
                AzureAiStudioConstants.ENDPOINT_TYPE_FIELD,
                endpointType
            )
        );

        if (dimensions != null) {
            map.put(ServiceFields.DIMENSIONS, dimensions);
        }

        if (dimensionsSetByUser != null) {
            map.put(ServiceFields.DIMENSIONS_SET_BY_USER, dimensionsSetByUser.equals(Boolean.TRUE));
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
    protected Writeable.Reader<AzureAiStudioEmbeddingsServiceSettings> instanceReader() {
        return AzureAiStudioEmbeddingsServiceSettings::new;
    }

    @Override
    protected AzureAiStudioEmbeddingsServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected AzureAiStudioEmbeddingsServiceSettings mutateInstance(AzureAiStudioEmbeddingsServiceSettings instance) throws IOException {
        var target = instance.target();
        var provider = instance.provider();
        var endpointType = instance.endpointType();
        var dimensions = instance.dimensions();
        var dimensionsSetByUser = instance.dimensionsSetByUser();
        var maxInputTokens = instance.maxInputTokens();
        var similarity = instance.similarity();
        var rateLimitSettings = instance.rateLimitSettings();
        switch (randomInt(7)) {
            case 0 -> target = randomValueOtherThan(target, () -> randomAlphaOfLength(10));
            case 1 -> provider = randomValueOtherThan(provider, () -> randomFrom(AzureAiStudioProvider.values()));
            case 2 -> endpointType = randomValueOtherThan(endpointType, () -> randomFrom(AzureAiStudioEndpointType.values()));
            case 3 -> dimensions = randomValueOtherThan(dimensions, ESTestCase::randomNonNegativeIntOrNull);
            case 4 -> dimensionsSetByUser = randomValueOtherThan(dimensionsSetByUser, ESTestCase::randomBoolean);
            case 5 -> maxInputTokens = randomValueOtherThan(maxInputTokens, ESTestCase::randomNonNegativeIntOrNull);
            case 6 -> similarity = randomValueOtherThan(
                similarity,
                AzureAiStudioEmbeddingsServiceSettingsTests::randomSimilarityMeasureOrNull
            );
            case 7 -> rateLimitSettings = randomValueOtherThan(rateLimitSettings, RateLimitSettingsTests::createRandom);
            default -> throw new AssertionError("Illegal randomisation branch");
        }

        return new AzureAiStudioEmbeddingsServiceSettings(
            target,
            provider,
            endpointType,
            dimensions,
            dimensionsSetByUser,
            maxInputTokens,
            similarity,
            rateLimitSettings
        );
    }

    @Override
    protected AzureAiStudioEmbeddingsServiceSettings mutateInstanceForVersion(
        AzureAiStudioEmbeddingsServiceSettings instance,
        TransportVersion version
    ) {
        return instance;
    }

    private static AzureAiStudioEmbeddingsServiceSettings createRandom() {
        return new AzureAiStudioEmbeddingsServiceSettings(
            randomAlphaOfLength(10),
            randomFrom(AzureAiStudioProvider.values()),
            randomFrom(AzureAiStudioEndpointType.values()),
            randomNonNegativeIntOrNull(),
            randomBoolean(),
            randomNonNegativeIntOrNull(),
            randomSimilarityMeasureOrNull(),
            RateLimitSettingsTests.createRandom()
        );
    }

    private static SimilarityMeasure randomSimilarityMeasureOrNull() {
        return randomFrom(new SimilarityMeasure[] { null, randomSimilarityMeasure() });
    }
}
