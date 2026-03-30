/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai.embeddings;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.common.JsonUtils;
import org.elasticsearch.xpack.inference.common.oauth2.OAuth2Settings;
import org.elasticsearch.xpack.inference.common.oauth2.OAuth2SettingsTests;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiOAuth2Settings;
import org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiOAuth2SettingsTests;
import org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiServiceFields;
import org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

import static org.elasticsearch.test.BWCVersions.DEFAULT_BWC_VERSIONS;
import static org.elasticsearch.xpack.inference.Utils.randomSimilarityMeasure;
import static org.elasticsearch.xpack.inference.services.ServiceFields.DIMENSIONS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.DIMENSIONS_SET_BY_USER;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MAX_INPUT_TOKENS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.SIMILARITY;
import static org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiOAuth2Settings.AZURE_OPENAI_OAUTH_SETTINGS;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class AzureOpenAiEmbeddingsServiceSettingsTests extends AzureOpenAiServiceSettingsTests<AzureOpenAiEmbeddingsServiceSettings> {

    public static final int TEST_DIMENSIONS = 1536;
    public static final int TEST_MAX_INPUT_TOKENS = 512;
    public static final SimilarityMeasure TEST_SIMILARITY = SimilarityMeasure.DOT_PRODUCT;

    /** Mirrors {@link AzureOpenAiEmbeddingsServiceSettings} default RPM when rate_limit is omitted. */
    private static final int DEFAULT_EMBEDDINGS_REQUESTS_PER_MINUTE = 1_440;

    private static final String ERROR_DIMENSIONS_SET_BY_USER_NOT_ALLOWED =
        "Validation Failed: 1: [service_settings] does not allow the setting [%s];";
    private static final String ERROR_DIMENSIONS =
        "Validation Failed: 1: [service_settings] Invalid value [%d]. [dimensions] must be a positive integer;";
    private static final String ERROR_MAX_INPUT_TOKENS_ZERO =
        "Validation Failed: 1: [service_settings] Invalid value [0]. [max_input_tokens] must be a positive integer;";
    private static final String ERROR_MAX_INPUT_TOKENS_NEGATIVE =
        "Validation Failed: 1: [service_settings] Invalid value [%d]. [max_input_tokens] must be a positive integer;";
    private static final String ERROR_DIMENSIONS_SET_BY_USER_REQUIRED =
        "Validation Failed: 1: [service_settings] does not contain the required setting [dimensions_set_by_user];";

    @Override
    protected AzureOpenAiEmbeddingsServiceSettings updateServiceSettings(
        AzureOpenAiEmbeddingsServiceSettings serviceSettings,
        Map<String, Object> serviceSettingsMap
    ) {
        return serviceSettings.updateServiceSettings(serviceSettingsMap);
    }

    @Override
    protected AzureOpenAiEmbeddingsServiceSettings fromMap(Map<String, Object> serviceSettingsMap, ConfigurationParseContext context) {
        return AzureOpenAiEmbeddingsServiceSettings.fromMap(serviceSettingsMap, context);
    }

    @Override
    protected void assertCommonImmutableFieldsUnchangedAfterUpdate(AzureOpenAiEmbeddingsServiceSettings updatedSettings) {
        assertThat(updatedSettings.resourceName(), is(TEST_RESOURCE_NAME));
        assertThat(updatedSettings.deploymentId(), is(TEST_DEPLOYMENT_ID));
        assertThat(updatedSettings.apiVersion(), is(TEST_API_VERSION));
    }

    /**
     * Embeddings task fields are not mutable via {@code updateServiceSettings}; same expectations as after parsing a full map for this fixture.
     */
    @Override
    protected void assertTaskSpecificAfterFullMap(AzureOpenAiEmbeddingsServiceSettings serviceSettings, ConfigurationParseContext context) {
        assertThat(serviceSettings.dimensions(), is(TEST_DIMENSIONS));
        assertThat(serviceSettings.maxInputTokens(), is(TEST_MAX_INPUT_TOKENS));
        assertThat(serviceSettings.similarity(), is(TEST_SIMILARITY));
        assertThat(serviceSettings.dimensionsSetByUser(), is(true));
    }

    @Override
    protected void assertTaskSpecificAfterRequiredOnlyMap(
        AzureOpenAiEmbeddingsServiceSettings serviceSettings,
        ConfigurationParseContext context
    ) {
        assertThat(serviceSettings.dimensions(), nullValue());
        assertThat(serviceSettings.dimensionsSetByUser(), is(false));
        assertThat(serviceSettings.maxInputTokens(), nullValue());
        assertThat(serviceSettings.similarity(), nullValue());
    }

    public void testFromMap_Request_DimensionsSetByUser_IsFalse_WhenDimensionsAreNotPresent() {
        var serviceSettings = AzureOpenAiEmbeddingsServiceSettings.fromMap(
            new HashMap<>(buildEmbeddingServiceSettingsMap(null, null, TEST_MAX_INPUT_TOKENS, TEST_SIMILARITY)),
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
                    TEST_MAX_INPUT_TOKENS,
                    TEST_SIMILARITY,
                    null
                )
            )
        );
    }

    public void testFromMap_Request_DimensionsSetByUser_ShouldThrowWhenPresent() {
        var map = buildEmbeddingServiceSettingsMap(TEST_DIMENSIONS, false, TEST_MAX_INPUT_TOKENS, TEST_SIMILARITY);
        var thrownException = expectThrows(
            ValidationException.class,
            () -> AzureOpenAiEmbeddingsServiceSettings.fromMap(map, ConfigurationParseContext.REQUEST)
        );

        assertThat(
            thrownException.getMessage(),
            containsString(Strings.format(ERROR_DIMENSIONS_SET_BY_USER_NOT_ALLOWED, DIMENSIONS_SET_BY_USER))
        );
    }

    public void testFromMap_ThrowsException_WhenDimensionsAreZero() {
        var dimensions = 0;

        var settingsMap = buildEmbeddingServiceSettingsMap(dimensions, null, null, null);

        var thrownException = expectThrows(
            ValidationException.class,
            () -> AzureOpenAiEmbeddingsServiceSettings.fromMap(settingsMap, ConfigurationParseContext.REQUEST)
        );

        assertThat(thrownException.getMessage(), containsString(Strings.format(ERROR_DIMENSIONS, dimensions)));
    }

    public void testFromMap_ThrowsException_WhenDimensionsAreNegative() {
        var dimensions = randomNegativeInt();

        var settingsMap = buildEmbeddingServiceSettingsMap(dimensions, null, null, null);

        var thrownException = expectThrows(
            ValidationException.class,
            () -> AzureOpenAiEmbeddingsServiceSettings.fromMap(settingsMap, ConfigurationParseContext.REQUEST)
        );

        assertThat(thrownException.getMessage(), containsString(Strings.format(ERROR_DIMENSIONS, dimensions)));
    }

    public void testFromMap_ThrowsException_WhenMaxInputTokensAreZero() {
        var maxInputTokens = 0;

        var settingsMap = buildEmbeddingServiceSettingsMap(null, null, maxInputTokens, null);

        var thrownException = expectThrows(
            ValidationException.class,
            () -> AzureOpenAiEmbeddingsServiceSettings.fromMap(settingsMap, ConfigurationParseContext.REQUEST)
        );

        assertThat(thrownException.getMessage(), containsString(ERROR_MAX_INPUT_TOKENS_ZERO));
    }

    public void testFromMap_ThrowsException_WhenMaxInputTokensAreNegative() {
        var maxInputTokens = randomNegativeInt();

        var settingsMap = buildEmbeddingServiceSettingsMap(null, null, maxInputTokens, null);

        var thrownException = expectThrows(
            ValidationException.class,
            () -> AzureOpenAiEmbeddingsServiceSettings.fromMap(settingsMap, ConfigurationParseContext.REQUEST)
        );

        assertThat(thrownException.getMessage(), containsString(Strings.format(ERROR_MAX_INPUT_TOKENS_NEGATIVE, maxInputTokens)));
    }

    public void testFromMap_Persistent_CreatesSettingsCorrectly() {
        var serviceSettings = AzureOpenAiEmbeddingsServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
                    AzureOpenAiServiceFields.RESOURCE_NAME,
                    TEST_RESOURCE_NAME,
                    AzureOpenAiServiceFields.DEPLOYMENT_ID,
                    TEST_DEPLOYMENT_ID,
                    AzureOpenAiServiceFields.API_VERSION,
                    TEST_API_VERSION,
                    DIMENSIONS,
                    TEST_DIMENSIONS,
                    DIMENSIONS_SET_BY_USER,
                    false,
                    MAX_INPUT_TOKENS,
                    TEST_MAX_INPUT_TOKENS,
                    SIMILARITY,
                    SimilarityMeasure.DOT_PRODUCT.toString()
                )
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
                    SimilarityMeasure.DOT_PRODUCT,
                    null
                )
            )
        );
    }

    public void testFromMap_PersistentContext_DoesNotThrowException_WhenDimensionsIsNull() {
        var settings = AzureOpenAiEmbeddingsServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
                    AzureOpenAiServiceFields.RESOURCE_NAME,
                    TEST_RESOURCE_NAME,
                    AzureOpenAiServiceFields.DEPLOYMENT_ID,
                    TEST_DEPLOYMENT_ID,
                    AzureOpenAiServiceFields.API_VERSION,
                    TEST_API_VERSION,
                    DIMENSIONS_SET_BY_USER,
                    true
                )
            ),
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
                    true,
                    null,
                    null,
                    null
                )
            )
        );
    }

    public void testFromMap_PersistentContext_DoesNotThrowException_WhenSimilarityIsPresent() {
        var settings = AzureOpenAiEmbeddingsServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
                    AzureOpenAiServiceFields.RESOURCE_NAME,
                    TEST_RESOURCE_NAME,
                    AzureOpenAiServiceFields.DEPLOYMENT_ID,
                    TEST_DEPLOYMENT_ID,
                    AzureOpenAiServiceFields.API_VERSION,
                    TEST_API_VERSION,
                    DIMENSIONS_SET_BY_USER,
                    true,
                    SIMILARITY,
                    SimilarityMeasure.COSINE.toString()
                )
            ),
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
                    true,
                    null,
                    SimilarityMeasure.COSINE,
                    null
                )
            )
        );
    }

    public void testFromMap_PersistentContext_ThrowsException_WhenDimensionsSetByUserIsNull() {
        var exception = expectThrows(
            ValidationException.class,
            () -> AzureOpenAiEmbeddingsServiceSettings.fromMap(
                new HashMap<>(
                    Map.of(
                        AzureOpenAiServiceFields.RESOURCE_NAME,
                        TEST_RESOURCE_NAME,
                        AzureOpenAiServiceFields.DEPLOYMENT_ID,
                        TEST_DEPLOYMENT_ID,
                        AzureOpenAiServiceFields.API_VERSION,
                        TEST_API_VERSION,
                        DIMENSIONS,
                        1
                    )
                ),
                ConfigurationParseContext.PERSISTENT
            )
        );

        assertThat(exception.getMessage(), containsString(ERROR_DIMENSIONS_SET_BY_USER_REQUIRED));
    }

    public void testToXContent_WritesDimensionsSetByUserTrue() throws IOException {
        var entity = new AzureOpenAiEmbeddingsServiceSettings(
            TEST_RESOURCE_NAME,
            TEST_DEPLOYMENT_ID,
            TEST_API_VERSION,
            null,
            true,
            null,
            null,
            null
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        var xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is(XContentHelper.stripWhitespace(Strings.format("""
            {
                "resource_name":"%s",
                "deployment_id":"%s",
                "api_version":"%s",
                "rate_limit":{
                    "requests_per_minute":%d
                },
                "dimensions_set_by_user":true
            }
            """, TEST_RESOURCE_NAME, TEST_DEPLOYMENT_ID, TEST_API_VERSION, DEFAULT_EMBEDDINGS_REQUESTS_PER_MINUTE))));
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var entity = new AzureOpenAiEmbeddingsServiceSettings(
            TEST_RESOURCE_NAME,
            TEST_DEPLOYMENT_ID,
            TEST_API_VERSION,
            TEST_DIMENSIONS,
            false,
            TEST_MAX_INPUT_TOKENS,
            null,
            new RateLimitSettings(TEST_RATE_LIMIT)
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        var xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is(XContentHelper.stripWhitespace(Strings.format("""
            {
                "resource_name":"%s",
                "deployment_id":"%s",
                "api_version":"%s",
                "rate_limit":{
                    "requests_per_minute":%d
                },
                "dimensions":%d,
                "max_input_tokens":%d,
                "dimensions_set_by_user":false
            }
            """, TEST_RESOURCE_NAME, TEST_DEPLOYMENT_ID, TEST_API_VERSION, TEST_RATE_LIMIT, TEST_DIMENSIONS, TEST_MAX_INPUT_TOKENS))));
    }

    public void testToFilteredXContent_WritesAllValues_Except_DimensionsSetByUser() throws IOException {
        var entity = new AzureOpenAiEmbeddingsServiceSettings(
            TEST_RESOURCE_NAME,
            TEST_DEPLOYMENT_ID,
            TEST_API_VERSION,
            TEST_DIMENSIONS,
            false,
            TEST_MAX_INPUT_TOKENS,
            null,
            new RateLimitSettings(TEST_RATE_LIMIT)
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        var filteredXContent = entity.getFilteredXContentObject();
        filteredXContent.toXContent(builder, null);
        var xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is(XContentHelper.stripWhitespace(Strings.format("""
            {
                "resource_name":"%s",
                "deployment_id":"%s",
                "api_version":"%s",
                "rate_limit":{
                    "requests_per_minute":%d
                },
                "dimensions":%d,
                "max_input_tokens":%d
            }
            """, TEST_RESOURCE_NAME, TEST_DEPLOYMENT_ID, TEST_API_VERSION, TEST_RATE_LIMIT, TEST_DIMENSIONS, TEST_MAX_INPUT_TOKENS))));
    }

    public void testToXContent_WritesAllValues_WithOAuth2() throws IOException {
        var entity = new AzureOpenAiEmbeddingsServiceSettings(
            TEST_RESOURCE_NAME,
            TEST_DEPLOYMENT_ID,
            TEST_API_VERSION,
            TEST_DIMENSIONS,
            false,
            TEST_MAX_INPUT_TOKENS,
            null,
            new RateLimitSettings(TEST_RATE_LIMIT),
            new AzureOpenAiOAuth2Settings(
                new OAuth2Settings(OAuth2SettingsTests.TEST_CLIENT_ID, OAuth2SettingsTests.TEST_SCOPES),
                AzureOpenAiOAuth2SettingsTests.INITIAL_TEST_TENANT_ID
            )
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        var xContentResult = Strings.toString(builder);

        assertThat(
            xContentResult,
            is(
                XContentHelper.stripWhitespace(
                    Strings.format(
                        """
                            {
                                "resource_name":"%s",
                                "deployment_id":"%s",
                                "api_version":"%s",
                                "rate_limit":{
                                    "requests_per_minute":%d
                                },
                                "client_id": "%s",
                                "scopes":%s,
                                "tenant_id": "%s",
                                "dimensions": %d,
                                "max_input_tokens": %d,
                                "dimensions_set_by_user": false
                            }
                            """,
                        TEST_RESOURCE_NAME,
                        TEST_DEPLOYMENT_ID,
                        TEST_API_VERSION,
                        TEST_RATE_LIMIT,
                        OAuth2SettingsTests.TEST_CLIENT_ID,
                        JsonUtils.toJson(OAuth2SettingsTests.TEST_SCOPES, ""),
                        AzureOpenAiOAuth2SettingsTests.INITIAL_TEST_TENANT_ID,
                        TEST_DIMENSIONS,
                        TEST_MAX_INPUT_TOKENS
                    )
                )
            )
        );
    }

    public static AzureOpenAiEmbeddingsServiceSettings createRandom() {
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
            RateLimitSettingsTests.createRandom(),
            randomFrom(AzureOpenAiOAuth2SettingsTests.createRandom(), null)
        );
    }

    @Override
    protected Map<String, Object> buildFullServiceSettingsMap(ConfigurationParseContext context) {
        final Map<String, Object> settingsMap = buildFullServiceSettingsMap(
            TEST_RESOURCE_NAME,
            TEST_DEPLOYMENT_ID,
            TEST_API_VERSION,
            TEST_RATE_LIMIT,
            OAuth2SettingsTests.TEST_CLIENT_ID,
            OAuth2SettingsTests.TEST_SCOPES,
            AzureOpenAiOAuth2SettingsTests.TEST_TENANT_ID
        );
        settingsMap.put(SIMILARITY, TEST_SIMILARITY.toString());
        settingsMap.put(MAX_INPUT_TOKENS, TEST_MAX_INPUT_TOKENS);
        settingsMap.put(DIMENSIONS, TEST_DIMENSIONS);
        return switch (context) {
            case REQUEST -> settingsMap;
            case PERSISTENT -> {
                settingsMap.put(DIMENSIONS_SET_BY_USER, true);
                yield settingsMap;
            }
        };
    }

    @Override
    protected Map<String, Object> buildRequiredServiceSettingsMap(ConfigurationParseContext context) {
        final Map<String, Object> settingsMap = buildRequiredServiceSettingsMap(TEST_RESOURCE_NAME, TEST_DEPLOYMENT_ID, TEST_API_VERSION);
        return switch (context) {
            case REQUEST -> settingsMap;
            case PERSISTENT -> {
                settingsMap.put(DIMENSIONS_SET_BY_USER, false);
                yield settingsMap;
            }
        };
    }

    @Override
    protected AzureOpenAiEmbeddingsServiceSettings createServiceSettings(@Nullable AzureOpenAiOAuth2Settings oAuth2Settings) {
        return new AzureOpenAiEmbeddingsServiceSettings(
            TEST_RESOURCE_NAME,
            TEST_DEPLOYMENT_ID,
            TEST_API_VERSION,
            TEST_DIMENSIONS,
            true,
            TEST_MAX_INPUT_TOKENS,
            TEST_SIMILARITY,
            new RateLimitSettings(TEST_RATE_LIMIT),
            oAuth2Settings
        );
    }

    public static Map<String, Object> buildEmbeddingServiceSettingsMap(
        @Nullable Integer dimensions,
        @Nullable Boolean dimensionsSetByUser,
        @Nullable Integer maxInputTokens,
        @Nullable SimilarityMeasure similarity
    ) {
        var map = buildRequiredServiceSettingsMap(TEST_RESOURCE_NAME, TEST_DEPLOYMENT_ID, TEST_API_VERSION);
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
            map.put(SIMILARITY, similarity.toString());
        }
        return map;
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
        var oAuth2Settings = instance.oAuth2Settings();
        switch (randomInt(8)) {
            case 0 -> resourceName = randomValueOtherThan(resourceName, () -> randomAlphaOfLength(8));
            case 1 -> deploymentId = randomValueOtherThan(deploymentId, () -> randomAlphaOfLength(8));
            case 2 -> apiVersion = randomValueOtherThan(apiVersion, () -> randomAlphaOfLength(8));
            case 3 -> dimensions = randomValueOtherThan(dimensions, ESTestCase::randomNonNegativeIntOrNull);
            case 4 -> dimensionsSetByUser = dimensionsSetByUser == false;
            case 5 -> maxInputTokens = randomValueOtherThan(maxInputTokens, () -> randomFrom(randomIntBetween(128, 256), null));
            case 6 -> similarity = randomValueOtherThan(similarity, () -> randomFrom(randomSimilarityMeasure(), null));
            case 7 -> rateLimitSettings = randomValueOtherThan(rateLimitSettings, RateLimitSettingsTests::createRandom);
            case 8 -> oAuth2Settings = randomValueOtherThan(
                oAuth2Settings,
                () -> randomFrom(AzureOpenAiOAuth2SettingsTests.createRandom(), null)
            );
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
            rateLimitSettings,
            oAuth2Settings
        );
    }

    @Override
    protected AzureOpenAiEmbeddingsServiceSettings mutateInstanceForVersion(
        AzureOpenAiEmbeddingsServiceSettings instance,
        TransportVersion version
    ) {
        if (version.supports(AZURE_OPENAI_OAUTH_SETTINGS)) {
            return instance;
        }

        return new AzureOpenAiEmbeddingsServiceSettings(
            instance.resourceName(),
            instance.deploymentId(),
            instance.apiVersion(),
            instance.dimensions(),
            instance.dimensionsSetByUser(),
            instance.maxInputTokens(),
            instance.similarity(),
            instance.rateLimitSettings()
        );
    }

    // Versions before AZURE_OPENAI_OAUTH_SETTINGS throw an exception when serializing non-null OAuth2 settings,
    // so we filter those out of the bwc versions to avoid test failures.
    // The logic is tested directly by testAzureOpenAiOAuth2Settings_AreNotBackwardsCompatible.
    @Override
    protected Collection<TransportVersion> bwcVersions() {
        return super.bwcVersions().stream().filter(version -> version.supports(AZURE_OPENAI_OAUTH_SETTINGS)).toList();
    }

    public void testAzureOpenAiOAuth2Settings_AreNotBackwardsCompatible() throws IOException {
        var unsupportedVersions = DEFAULT_BWC_VERSIONS.stream()
            .filter(Predicate.not(version -> version.supports(AZURE_OPENAI_OAUTH_SETTINGS)))
            .toList();
        for (int runs = 0; runs < NUMBER_OF_TEST_RUNS; runs++) {
            var testInstance = createTestInstance();
            for (var unsupportedVersion : unsupportedVersions) {
                if (testInstance.oAuth2Settings() != null) {
                    var statusException = assertThrows(
                        ElasticsearchStatusException.class,
                        () -> copyWriteable(testInstance, getNamedWriteableRegistry(), instanceReader(), unsupportedVersion)
                    );
                    assertThat(statusException.status(), is(RestStatus.BAD_REQUEST));
                    assertThat(
                        statusException.getMessage(),
                        is("Cannot send OAuth2 settings to an older node. Please wait until all nodes are upgraded before using OAuth2.")
                    );
                } else {
                    // If the instance doesn't contain OAuth2 settings, assert that it can still be serialized
                    assertBwcSerialization(testInstance, unsupportedVersion);
                }
            }
        }
    }
}
