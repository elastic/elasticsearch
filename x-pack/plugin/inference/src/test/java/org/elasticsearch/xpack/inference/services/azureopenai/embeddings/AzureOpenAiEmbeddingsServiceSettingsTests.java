/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai.embeddings;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiServiceFields;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.Utils.randomSimilarityMeasure;
import static org.elasticsearch.xpack.inference.services.ServiceFields.DIMENSIONS_SET_BY_USER;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class AzureOpenAiEmbeddingsServiceSettingsTests extends AbstractWireSerializingTestCase<AzureOpenAiEmbeddingsServiceSettings> {
    private static final String TEST_RESOURCE_NAME = "test-resource-name";
    private static final String INITIAL_TEST_RESOURCE_NAME = "initial-resource-name";
    private static final String TEST_DEPLOYMENT_ID = "test-deployment-id";
    private static final String INITIAL_TEST_DEPLOYMENT_ID = "initial-deployment-id";
    private static final String TEST_API_VERSION = "test-api-version";
    private static final String INITIAL_TEST_API_VERSION = "initial-api-version";
    private static final int TEST_RATE_LIMIT = 20;
    private static final int INITIAL_TEST_RATE_LIMIT = 30;
    private static final int TEST_DIMENSIONS = 1536;
    private static final int INITIAL_TEST_DIMENSIONS = 3072;
    private static final int TEST_MAX_INPUT_TOKENS = 512;
    private static final int INITIAL_TEST_MAX_INPUT_TOKENS = 1024;
    private static final SimilarityMeasure TEST_SIMILARITY_MEASURE = SimilarityMeasure.COSINE;
    private static final SimilarityMeasure INITIAL_SIMILARITY_MEASURE = SimilarityMeasure.DOT_PRODUCT;

    private static AzureOpenAiEmbeddingsServiceSettings createRandom() {
        var resourceName = randomAlphaOfLength(8);
        var deploymentId = randomAlphaOfLength(8);
        var apiVersion = randomAlphaOfLength(8);
        Integer dims = randomNonNegativeIntOrNull();
        Integer maxInputTokens = randomBoolean() ? null : randomIntBetween(128, 256);
        return new AzureOpenAiEmbeddingsServiceSettings(
            resourceName,
            deploymentId,
            apiVersion,
            dims,
            randomBoolean(),
            maxInputTokens,
            null,
            RateLimitSettingsTests.createRandom()
        );
    }

    public void testUpdateServiceSettings_AllFields_DimensionsSetByUserFalse_Success() {
        testUpdateServiceSettings_AllFields_Success(false);
    }

    public void testUpdateServiceSettings_AllFields_DimensionsSetByUserTrue_Success() {
        testUpdateServiceSettings_AllFields_Success(true);
    }

    private static void testUpdateServiceSettings_AllFields_Success(boolean dimensionsSetByUser) {
        var settingsMap = createEmbeddingsServiceSettingsMap(
            TEST_RESOURCE_NAME,
            TEST_DEPLOYMENT_ID,
            TEST_API_VERSION,
            TEST_DIMENSIONS,
            null,
            TEST_MAX_INPUT_TOKENS,
            TEST_SIMILARITY_MEASURE,
            TEST_RATE_LIMIT
        );
        var serviceSettings = new AzureOpenAiEmbeddingsServiceSettings(
            INITIAL_TEST_RESOURCE_NAME,
            INITIAL_TEST_DEPLOYMENT_ID,
            INITIAL_TEST_API_VERSION,
            INITIAL_TEST_DIMENSIONS,
            dimensionsSetByUser,
            INITIAL_TEST_MAX_INPUT_TOKENS,
            INITIAL_SIMILARITY_MEASURE,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        ).updateServiceSettings(settingsMap, TaskType.TEXT_EMBEDDING);

        MatcherAssert.assertThat(
            serviceSettings,
            is(
                new AzureOpenAiEmbeddingsServiceSettings(
                    TEST_RESOURCE_NAME,
                    TEST_DEPLOYMENT_ID,
                    TEST_API_VERSION,
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
        var serviceSettings = new AzureOpenAiEmbeddingsServiceSettings(
            INITIAL_TEST_RESOURCE_NAME,
            INITIAL_TEST_DEPLOYMENT_ID,
            INITIAL_TEST_API_VERSION,
            INITIAL_TEST_DIMENSIONS,
            dimensionsSetByUser,
            INITIAL_TEST_MAX_INPUT_TOKENS,
            INITIAL_SIMILARITY_MEASURE,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        ).updateServiceSettings(new HashMap<>(), TaskType.TEXT_EMBEDDING);

        MatcherAssert.assertThat(
            serviceSettings,
            is(
                new AzureOpenAiEmbeddingsServiceSettings(
                    INITIAL_TEST_RESOURCE_NAME,
                    INITIAL_TEST_DEPLOYMENT_ID,
                    INITIAL_TEST_API_VERSION,
                    INITIAL_TEST_DIMENSIONS,
                    dimensionsSetByUser,
                    INITIAL_TEST_MAX_INPUT_TOKENS,
                    INITIAL_SIMILARITY_MEASURE,
                    new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
                )
            )
        );
    }

    public void testFromMap_Request_AllFields_Success() {
        var serviceSettings = AzureOpenAiEmbeddingsServiceSettings.fromMap(
            createEmbeddingsServiceSettingsMap(
                TEST_RESOURCE_NAME,
                TEST_DEPLOYMENT_ID,
                TEST_API_VERSION,
                TEST_DIMENSIONS,
                null,
                TEST_MAX_INPUT_TOKENS,
                TEST_SIMILARITY_MEASURE,
                TEST_RATE_LIMIT
            ),
            ConfigurationParseContext.REQUEST
        );

        assertThat(
            serviceSettings,
            is(
                new AzureOpenAiEmbeddingsServiceSettings(
                    TEST_RESOURCE_NAME,
                    TEST_DEPLOYMENT_ID,
                    TEST_API_VERSION,
                    TEST_DIMENSIONS,
                    true,
                    TEST_MAX_INPUT_TOKENS,
                    TEST_SIMILARITY_MEASURE,
                    new RateLimitSettings(TEST_RATE_LIMIT)
                )
            )
        );
    }

    public void testFromMap_Request_OnlyMandatoryFields_Success() {
        var serviceSettings = AzureOpenAiEmbeddingsServiceSettings.fromMap(
            createEmbeddingsServiceSettingsMap(TEST_RESOURCE_NAME, TEST_DEPLOYMENT_ID, TEST_API_VERSION, null, null, null, null, null),
            ConfigurationParseContext.REQUEST
        );

        assertThat(
            serviceSettings,
            is(
                new AzureOpenAiEmbeddingsServiceSettings(
                    TEST_RESOURCE_NAME,
                    TEST_DEPLOYMENT_ID,
                    TEST_API_VERSION,
                    null,
                    false,
                    null,
                    null,
                    null
                )
            )
        );
    }

    public void testFromMap_Request_DimensionsSetByUserTrue_ThrowsException() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> AzureOpenAiEmbeddingsServiceSettings.fromMap(
                createEmbeddingsServiceSettingsMap(TEST_RESOURCE_NAME, TEST_DEPLOYMENT_ID, TEST_API_VERSION, null, true, null, null, null),
                ConfigurationParseContext.REQUEST
            )
        );

        MatcherAssert.assertThat(
            thrownException.getMessage(),
            is(Strings.format("Validation Failed: 1: [service_settings] does not allow the setting [%s];", DIMENSIONS_SET_BY_USER))
        );
    }

    public void testFromMap_Request_DimensionsSetByUserFalse_ThrowsException() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> AzureOpenAiEmbeddingsServiceSettings.fromMap(
                createEmbeddingsServiceSettingsMap(TEST_RESOURCE_NAME, TEST_DEPLOYMENT_ID, TEST_API_VERSION, null, false, null, null, null),
                ConfigurationParseContext.REQUEST
            )
        );

        MatcherAssert.assertThat(
            thrownException.getMessage(),
            is(Strings.format("Validation Failed: 1: [service_settings] does not allow the setting [%s];", DIMENSIONS_SET_BY_USER))
        );
    }

    public void testFromMap_Request_DimensionsZero_ThrowsException() {
        var dimensions = 0;

        var settingsMap = createEmbeddingsServiceSettingsMap(
            TEST_RESOURCE_NAME,
            TEST_DEPLOYMENT_ID,
            TEST_API_VERSION,
            dimensions,
            null,
            null,
            null,
            null
        );

        var thrownException = expectThrows(
            ValidationException.class,
            () -> AzureOpenAiEmbeddingsServiceSettings.fromMap(settingsMap, ConfigurationParseContext.REQUEST)
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

    public void testFromMap_Request_DimensionsNegative_ThrowsException() {
        var dimensions = randomNegativeInt();

        var settingsMap = createEmbeddingsServiceSettingsMap(
            TEST_RESOURCE_NAME,
            TEST_DEPLOYMENT_ID,
            TEST_API_VERSION,
            dimensions,
            null,
            null,
            null,
            null
        );

        var thrownException = expectThrows(
            ValidationException.class,
            () -> AzureOpenAiEmbeddingsServiceSettings.fromMap(settingsMap, ConfigurationParseContext.REQUEST)
        );

        assertThat(
            thrownException.getMessage(),
            is(
                Strings.format(
                    "Validation Failed: 1: [service_settings] Invalid value [%d]. [dimensions] must be a positive integer;",
                    dimensions
                )
            )
        );
    }

    public void testFromMap_MaxInputTokensZero_ThrowsException() {
        var maxInputTokens = 0;

        var settingsMap = createEmbeddingsServiceSettingsMap(
            TEST_RESOURCE_NAME,
            TEST_DEPLOYMENT_ID,
            TEST_API_VERSION,
            null,
            null,
            maxInputTokens,
            null,
            null
        );

        var thrownException = expectThrows(
            ValidationException.class,
            () -> AzureOpenAiEmbeddingsServiceSettings.fromMap(settingsMap, ConfigurationParseContext.REQUEST)
        );

        assertThat(
            thrownException.getMessage(),
            is(
                Strings.format(
                    "Validation Failed: 1: [service_settings] Invalid value [%d]. [max_input_tokens] must be a positive integer;",
                    maxInputTokens
                )
            )
        );
    }

    public void testFromMap_MaxInputTokensNegative_ThrowsException() {
        var maxInputTokens = randomNegativeInt();

        var settingsMap = createEmbeddingsServiceSettingsMap(
            TEST_RESOURCE_NAME,
            TEST_DEPLOYMENT_ID,
            TEST_API_VERSION,
            null,
            null,
            maxInputTokens,
            null,
            null
        );

        var thrownException = expectThrows(
            ValidationException.class,
            () -> AzureOpenAiEmbeddingsServiceSettings.fromMap(settingsMap, ConfigurationParseContext.REQUEST)
        );

        assertThat(
            thrownException.getMessage(),
            is(
                Strings.format(
                    "Validation Failed: 1: [service_settings] Invalid value [%d]. [max_input_tokens] must be a positive integer;",
                    maxInputTokens
                )
            )
        );
    }

    public void testFromMap_Persistent_AllFields_Success() {
        var serviceSettings = AzureOpenAiEmbeddingsServiceSettings.fromMap(
            createEmbeddingsServiceSettingsMap(
                TEST_RESOURCE_NAME,
                TEST_DEPLOYMENT_ID,
                TEST_API_VERSION,
                TEST_DIMENSIONS,
                false,
                TEST_MAX_INPUT_TOKENS,
                TEST_SIMILARITY_MEASURE,
                TEST_RATE_LIMIT
            ),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(
            serviceSettings,
            is(
                new AzureOpenAiEmbeddingsServiceSettings(
                    TEST_RESOURCE_NAME,
                    TEST_DEPLOYMENT_ID,
                    TEST_API_VERSION,
                    TEST_DIMENSIONS,
                    false,
                    TEST_MAX_INPUT_TOKENS,
                    TEST_SIMILARITY_MEASURE,
                    new RateLimitSettings(TEST_RATE_LIMIT)
                )
            )
        );
    }

    public void testFromMap_PersistentContext_DimensionsNull_Success() {
        var settings = AzureOpenAiEmbeddingsServiceSettings.fromMap(
            createEmbeddingsServiceSettingsMap(TEST_RESOURCE_NAME, TEST_DEPLOYMENT_ID, TEST_API_VERSION, null, false, null, null, null),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(
            settings,
            is(
                new AzureOpenAiEmbeddingsServiceSettings(
                    TEST_RESOURCE_NAME,
                    TEST_DEPLOYMENT_ID,
                    TEST_API_VERSION,
                    null,
                    false,
                    null,
                    null,
                    null
                )
            )
        );
    }

    public void testFromMap_PersistentContext_DimensionsNull_DimensionsSetByUserTrue_ThrowsException() {
        var exception = expectThrows(
            ValidationException.class,
            () -> AzureOpenAiEmbeddingsServiceSettings.fromMap(
                createEmbeddingsServiceSettingsMap(TEST_RESOURCE_NAME, TEST_DEPLOYMENT_ID, TEST_API_VERSION, null, true, null, null, null),
                ConfigurationParseContext.PERSISTENT
            )
        );

        assertThat(
            exception.getMessage(),
            is("Validation Failed: 1: [service_settings] does not contain the required setting [dimensions];")

        );
    }

    public void testFromMap_PersistentContext_DimensionsSetByUserNull_ThrowsException() {
        var exception = expectThrows(
            ValidationException.class,
            () -> AzureOpenAiEmbeddingsServiceSettings.fromMap(
                createEmbeddingsServiceSettingsMap(
                    TEST_RESOURCE_NAME,
                    TEST_DEPLOYMENT_ID,
                    TEST_API_VERSION,
                    TEST_DIMENSIONS,
                    null,
                    null,
                    null,
                    null
                ),
                ConfigurationParseContext.PERSISTENT
            )
        );

        assertThat(
            exception.getMessage(),
            containsString("Validation Failed: 1: [service_settings] does not contain the required setting [dimensions_set_by_user];")
        );
    }

    public void testToXContent_WritesDimensionsSetByUserTrue() throws IOException {
        boolean dimensionsSetByUser = true;
        var entity = new AzureOpenAiEmbeddingsServiceSettings(
            TEST_RESOURCE_NAME,
            TEST_DEPLOYMENT_ID,
            TEST_API_VERSION,
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
                        {"resource_name":"%s","deployment_id":"%s","api_version":"%s","rate_limit":\
                        {"requests_per_minute":%d},"dimensions_set_by_user":%b}""",
                    TEST_RESOURCE_NAME,
                    TEST_DEPLOYMENT_ID,
                    TEST_API_VERSION,
                    TEST_RATE_LIMIT,
                    dimensionsSetByUser
                )
            )
        );
    }

    public void testToXContent_WritesAllValues() throws IOException {
        boolean dimensionsSetByUser = false;
        var entity = new AzureOpenAiEmbeddingsServiceSettings(
            TEST_RESOURCE_NAME,
            TEST_DEPLOYMENT_ID,
            TEST_API_VERSION,
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
                        {"resource_name":"%s","deployment_id":"%s","api_version":"%s","dimensions":%d,\
                        "max_input_tokens":%d,"similarity":"%s","rate_limit":{"requests_per_minute":%d},"dimensions_set_by_user":%b}""",
                    TEST_RESOURCE_NAME,
                    TEST_DEPLOYMENT_ID,
                    TEST_API_VERSION,
                    TEST_DIMENSIONS,
                    TEST_MAX_INPUT_TOKENS,
                    TEST_SIMILARITY_MEASURE,
                    TEST_RATE_LIMIT,
                    dimensionsSetByUser
                )
            )
        );
    }

    public void testToFilteredXContent_WritesAllValues_ExceptDimensionsSetByUser() throws IOException {
        var entity = new AzureOpenAiEmbeddingsServiceSettings(
            TEST_RESOURCE_NAME,
            TEST_DEPLOYMENT_ID,
            TEST_API_VERSION,
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
                        {"resource_name":"%s","deployment_id":"%s","api_version":"%s","dimensions":%d,"max_input_tokens":%d,\
                        "similarity":"%s","rate_limit":{"requests_per_minute":%d}}""",
                    TEST_RESOURCE_NAME,
                    TEST_DEPLOYMENT_ID,
                    TEST_API_VERSION,
                    TEST_DIMENSIONS,
                    TEST_MAX_INPUT_TOKENS,
                    TEST_SIMILARITY_MEASURE,
                    TEST_RATE_LIMIT
                )
            )
        );
    }

    @Override
    protected Writeable.Reader<AzureOpenAiEmbeddingsServiceSettings> instanceReader() {
        return AzureOpenAiEmbeddingsServiceSettings::new;
    }

    @Override
    protected AzureOpenAiEmbeddingsServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected AzureOpenAiEmbeddingsServiceSettings mutateInstance(AzureOpenAiEmbeddingsServiceSettings instance) throws IOException {
        var resourceName = instance.resourceName();
        var deploymentId = instance.deploymentId();
        var apiVersion = instance.apiVersion();
        var dimensions = instance.dimensions();
        var dimensionsSetByUser = instance.dimensionsSetByUser();
        var maxInputTokens = instance.maxInputTokens();
        var similarity = instance.similarity();
        var rateLimitSettings = instance.rateLimitSettings();
        switch (randomInt(7)) {
            case 0 -> resourceName = randomValueOtherThan(resourceName, () -> randomAlphaOfLength(8));
            case 1 -> deploymentId = randomValueOtherThan(deploymentId, () -> randomAlphaOfLength(8));
            case 2 -> apiVersion = randomValueOtherThan(apiVersion, () -> randomAlphaOfLength(8));
            case 3 -> dimensions = randomValueOtherThan(dimensions, ESTestCase::randomNonNegativeIntOrNull);
            case 4 -> dimensionsSetByUser = dimensionsSetByUser == false;
            case 5 -> maxInputTokens = randomValueOtherThan(maxInputTokens, () -> randomFrom(randomIntBetween(128, 256), null));
            case 6 -> similarity = randomValueOtherThan(similarity, () -> randomFrom(randomSimilarityMeasure(), null));
            case 7 -> rateLimitSettings = randomValueOtherThan(rateLimitSettings, RateLimitSettingsTests::createRandom);
            default -> throw new AssertionError("Illegal randomisation branch");
        }

        return new AzureOpenAiEmbeddingsServiceSettings(
            resourceName,
            deploymentId,
            apiVersion,
            dimensions,
            dimensionsSetByUser,
            maxInputTokens,
            similarity,
            rateLimitSettings
        );
    }

    public static HashMap<String, Object> createEmbeddingsServiceSettingsMap(
        String resourceName,
        String deploymentId,
        String apiVersion,
        @Nullable Integer dimensions,
        @Nullable Boolean dimensionsSetByUser,
        @Nullable Integer maxTokens,
        @Nullable SimilarityMeasure similarityMeasure,
        @Nullable Integer rateLimitSettings
    ) {
        var map = new HashMap<String, Object>(
            Map.of(
                AzureOpenAiServiceFields.RESOURCE_NAME,
                resourceName,
                AzureOpenAiServiceFields.DEPLOYMENT_ID,
                deploymentId,
                AzureOpenAiServiceFields.API_VERSION,
                apiVersion
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
            map.put(ServiceFields.SIMILARITY, similarityMeasure.toString());
        }

        if (rateLimitSettings != null) {
            map.put(RateLimitSettings.FIELD_NAME, new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, rateLimitSettings)));
        }

        return map;
    }
}
