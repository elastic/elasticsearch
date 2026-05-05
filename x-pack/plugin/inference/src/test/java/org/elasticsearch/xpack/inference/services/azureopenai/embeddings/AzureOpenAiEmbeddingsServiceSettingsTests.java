/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai.embeddings;

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
import org.elasticsearch.xpack.inference.common.JsonUtils;
import org.elasticsearch.xpack.inference.common.oauth2.OAuth2Settings;
import org.elasticsearch.xpack.inference.common.oauth2.OAuth2SettingsTests;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiOAuth2Settings;
import org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiOAuth2SettingsTests;
import org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

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
    private static final int INITIAL_TEST_DIMENSIONS = 1024;

    public static final int TEST_MAX_INPUT_TOKENS = 512;
    private static final int INITIAL_TEST_MAX_INPUT_TOKENS = 256;

    public static final SimilarityMeasure TEST_SIMILARITY = SimilarityMeasure.DOT_PRODUCT;
    private static final SimilarityMeasure INITIAL_TEST_SIMILARITY = SimilarityMeasure.COSINE;

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
    protected RateLimitSettings getDefaultRateLimitSettings() {
        return new RateLimitSettings(1_440);
    }

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
    protected void assertFieldsAfterUpdate(AzureOpenAiEmbeddingsServiceSettings updatedSettings) {
        super.assertFieldsAfterUpdate(updatedSettings);
        assertThat(updatedSettings.maxInputTokens(), is(TEST_MAX_INPUT_TOKENS));
        assertThat(updatedSettings.similarity(), is(INITIAL_TEST_SIMILARITY));
        assertThat(updatedSettings.dimensions(), is(INITIAL_TEST_DIMENSIONS));
        assertThat(updatedSettings.dimensionsSetByUser(), is(Boolean.TRUE));
    }

    @Override
    protected void assertFromMap_AllFields(AzureOpenAiEmbeddingsServiceSettings serviceSettings, ConfigurationParseContext context) {
        super.assertFromMap_AllFields(serviceSettings, context);
        assertThat(serviceSettings.maxInputTokens(), is(TEST_MAX_INPUT_TOKENS));
        assertThat(serviceSettings.similarity(), is(TEST_SIMILARITY));
        assertThat(serviceSettings.dimensions(), is(TEST_DIMENSIONS));
        assertThat(serviceSettings.dimensionsSetByUser(), is(Boolean.TRUE));
    }

    @Override
    protected void assertFromMap_RequiredFieldsOnly(
        AzureOpenAiEmbeddingsServiceSettings serviceSettings,
        ConfigurationParseContext context
    ) {
        super.assertFromMap_RequiredFieldsOnly(serviceSettings, context);
        assertThat(serviceSettings.dimensionsSetByUser(), is(Boolean.FALSE));
        assertThat(serviceSettings.maxInputTokens(), is(nullValue()));
        assertThat(serviceSettings.similarity(), is(nullValue()));
    }

    @Override
    protected Map<String, Object> buildAllFieldsServiceSettingsMap(ConfigurationParseContext context) {
        final Map<String, Object> settingsMap = buildAllFieldsServiceSettingsMap(
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
    protected Map<String, Object> buildRequiredFieldsServiceSettingsMap(ConfigurationParseContext context) {
        final Map<String, Object> settingsMap = buildRequiredFieldsServiceSettingsMap(
            TEST_RESOURCE_NAME,
            TEST_DEPLOYMENT_ID,
            TEST_API_VERSION
        );
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
            INITIAL_TEST_RESOURCE_NAME,
            INITIAL_TEST_DEPLOYMENT_ID,
            INITIAL_TEST_API_VERSION,
            INITIAL_TEST_DIMENSIONS,
            true,
            INITIAL_TEST_MAX_INPUT_TOKENS,
            INITIAL_TEST_SIMILARITY,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT),
            oAuth2Settings
        );
    }

    public static Map<String, Object> buildEmbeddingServiceSettingsMap(
        @Nullable Integer dimensions,
        @Nullable Boolean dimensionsSetByUser,
        @Nullable Integer maxInputTokens,
        @Nullable SimilarityMeasure similarity
    ) {
        var settingsMap = buildRequiredFieldsServiceSettingsMap(TEST_RESOURCE_NAME, TEST_DEPLOYMENT_ID, TEST_API_VERSION);
        if (dimensions != null) {
            settingsMap.put(DIMENSIONS, dimensions);
        }
        if (dimensionsSetByUser != null) {
            settingsMap.put(DIMENSIONS_SET_BY_USER, dimensionsSetByUser);
        }
        if (maxInputTokens != null) {
            settingsMap.put(MAX_INPUT_TOKENS, maxInputTokens);
        }
        if (similarity != null) {
            settingsMap.put(SIMILARITY, similarity.toString());
        }
        return settingsMap;
    }

    public void testFromMap_Request_NoDimensions_DimensionsSetByUserIsFalse() {
        var serviceSettings = AzureOpenAiEmbeddingsServiceSettings.fromMap(
            buildEmbeddingServiceSettingsMap(null, null, null, null),
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

    public void testFromMap_Request_DimensionsSetByUserPresent_ThrowsValidationException() {
        var map = buildEmbeddingServiceSettingsMap(null, false, null, null);
        var exception = expectThrows(
            ValidationException.class,
            () -> AzureOpenAiEmbeddingsServiceSettings.fromMap(map, ConfigurationParseContext.REQUEST)
        );

        assertThat(
            exception.getMessage(),
            containsString(Strings.format(ERROR_DIMENSIONS_SET_BY_USER_NOT_ALLOWED, DIMENSIONS_SET_BY_USER))
        );
    }

    public void testFromMap_Request_DimensionsAreZero_ThrowsValidationException() {
        var dimensions = 0;

        var settingsMap = buildEmbeddingServiceSettingsMap(dimensions, null, null, null);

        var exception = expectThrows(
            ValidationException.class,
            () -> AzureOpenAiEmbeddingsServiceSettings.fromMap(settingsMap, ConfigurationParseContext.REQUEST)
        );

        assertThat(exception.getMessage(), containsString(Strings.format(ERROR_DIMENSIONS, dimensions)));
    }

    public void testFromMap_Request_DimensionsAreNegative_ThrowsValidationException() {
        var dimensions = randomNegativeInt();

        var settingsMap = buildEmbeddingServiceSettingsMap(dimensions, null, null, null);

        var exception = expectThrows(
            ValidationException.class,
            () -> AzureOpenAiEmbeddingsServiceSettings.fromMap(settingsMap, ConfigurationParseContext.REQUEST)
        );

        assertThat(exception.getMessage(), containsString(Strings.format(ERROR_DIMENSIONS, dimensions)));
    }

    public void testFromMap_Request_MaxInputTokensAreZero_ThrowsValidationException() {
        var maxInputTokens = 0;

        var settingsMap = buildEmbeddingServiceSettingsMap(null, null, maxInputTokens, null);

        var exception = expectThrows(
            ValidationException.class,
            () -> AzureOpenAiEmbeddingsServiceSettings.fromMap(settingsMap, ConfigurationParseContext.REQUEST)
        );

        assertThat(exception.getMessage(), containsString(ERROR_MAX_INPUT_TOKENS_ZERO));
    }

    public void testFromMap_Request_MaxInputTokensAreNegative_ThrowsValidationException() {
        var maxInputTokens = randomNegativeInt();

        var settingsMap = buildEmbeddingServiceSettingsMap(null, null, maxInputTokens, null);

        var exception = expectThrows(
            ValidationException.class,
            () -> AzureOpenAiEmbeddingsServiceSettings.fromMap(settingsMap, ConfigurationParseContext.REQUEST)
        );

        assertThat(exception.getMessage(), containsString(Strings.format(ERROR_MAX_INPUT_TOKENS_NEGATIVE, maxInputTokens)));
    }

    public void testFromMap_Persistent_DimensionsAreNull_CreatesExpectedSettings() {
        var settings = AzureOpenAiEmbeddingsServiceSettings.fromMap(
            buildEmbeddingServiceSettingsMap(null, true, null, null),
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

    public void testFromMap_Persistent_DimensionsSetByUserIsNull_ThrowsValidationException() {
        var exception = expectThrows(
            ValidationException.class,
            () -> AzureOpenAiEmbeddingsServiceSettings.fromMap(
                buildEmbeddingServiceSettingsMap(null, null, null, null),
                ConfigurationParseContext.PERSISTENT
            )
        );

        assertThat(exception.getMessage(), containsString(ERROR_DIMENSIONS_SET_BY_USER_REQUIRED));
    }

    public void testToFilteredXContent_AllFieldsExceptDimensionsSetByUserAreWritten() throws IOException {
        var entity = new AzureOpenAiEmbeddingsServiceSettings(
            TEST_RESOURCE_NAME,
            TEST_DEPLOYMENT_ID,
            TEST_API_VERSION,
            TEST_DIMENSIONS,
            true,
            TEST_MAX_INPUT_TOKENS,
            TEST_SIMILARITY,
            new RateLimitSettings(TEST_RATE_LIMIT),
            new AzureOpenAiOAuth2Settings(
                new OAuth2Settings(OAuth2SettingsTests.TEST_CLIENT_ID, OAuth2SettingsTests.TEST_SCOPES),
                AzureOpenAiOAuth2SettingsTests.INITIAL_TEST_TENANT_ID
            )
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        var filteredXContent = entity.getFilteredXContentObject();
        filteredXContent.toXContent(builder, null);
        var xContentResult = Strings.toString(builder);

        assertThat(
            xContentResult,
            is(
                XContentHelper.stripWhitespace(
                    Strings.format(
                        """
                            {
                                "resource_name": "%s",
                                "deployment_id": "%s",
                                "api_version": "%s",
                                "rate_limit": {
                                    "requests_per_minute": %d
                                },
                                "client_id": "%s",
                                "scopes":%s,
                                "tenant_id": "%s",
                                "dimensions": %d,
                                "max_input_tokens": %d,
                                "similarity": "%s"
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
                        TEST_MAX_INPUT_TOKENS,
                        TEST_SIMILARITY
                    )
                )
            )
        );
    }

    public void testToXContent_AllFieldsAreWritten() throws IOException {
        boolean dimensionsSetByUser = true;
        var entity = new AzureOpenAiEmbeddingsServiceSettings(
            TEST_RESOURCE_NAME,
            TEST_DEPLOYMENT_ID,
            TEST_API_VERSION,
            TEST_DIMENSIONS,
            dimensionsSetByUser,
            TEST_MAX_INPUT_TOKENS,
            TEST_SIMILARITY,
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
                                "resource_name": "%s",
                                "deployment_id": "%s",
                                "api_version": "%s",
                                "rate_limit": {
                                    "requests_per_minute": %d
                                },
                                "client_id": "%s",
                                "scopes":%s,
                                "tenant_id": "%s",
                                "dimensions": %d,
                                "max_input_tokens": %d,
                                "similarity": "%s",
                                "dimensions_set_by_user": %b
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
                        TEST_MAX_INPUT_TOKENS,
                        TEST_SIMILARITY,
                        dimensionsSetByUser
                    )
                )
            )
        );
    }

    public void testToXContent_OnlyRequiredAndDefaultFieldsAreWritten() throws IOException {
        boolean dimensionsSetByUser = false;
        var entity = new AzureOpenAiEmbeddingsServiceSettings(
            TEST_RESOURCE_NAME,
            TEST_DEPLOYMENT_ID,
            TEST_API_VERSION,
            null,
            false,
            null,
            null,
            null,
            null
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
                                "resource_name": "%s",
                                "deployment_id": "%s",
                                "api_version": "%s",
                                "rate_limit": {
                                    "requests_per_minute": %d
                                },
                                "dimensions_set_by_user": %b
                            }
                            """,
                        TEST_RESOURCE_NAME,
                        TEST_DEPLOYMENT_ID,
                        TEST_API_VERSION,
                        getDefaultRateLimitSettings().requestsPerTimeUnit(),
                        dimensionsSetByUser
                    )
                )
            )
        );
    }

    public static AzureOpenAiEmbeddingsServiceSettings createRandom() {
        var resourceName = randomAlphaOfLength(8);
        var deploymentId = randomAlphaOfLength(8);
        var apiVersion = randomAlphaOfLength(8);
        var dimensions = randomBoolean() ? null : randomIntBetween(128, 4096);
        var dimensionsSetByUser = randomBoolean();
        var maxInputTokens = randomBoolean() ? null : randomIntBetween(128, 256);
        var similarity = randomFrom(randomSimilarityMeasure(), null);
        var rateLimitSettings = RateLimitSettingsTests.createRandom();
        var oAuth2Settings = randomFrom(AzureOpenAiOAuth2SettingsTests.createRandom(), null);

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
            case 3 -> dimensions = randomValueOtherThan(dimensions, () -> randomFrom(randomIntBetween(128, 4096), null));
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

    /**
     * Versions before {@link AzureOpenAiOAuth2Settings#AZURE_OPENAI_OAUTH_SETTINGS} throw an exception when serializing non-null OAuth2
     * settings, so we filter those out of the bwc versions to avoid test failures.
     * The logic is tested directly by {@link #testAzureOpenAiOAuth2Settings_AreNotBackwardsCompatible}
     */
    @Override
    protected Collection<TransportVersion> bwcVersions() {
        return super.bwcVersions().stream().filter(version -> version.supports(AZURE_OPENAI_OAUTH_SETTINGS)).toList();
    }

    public void testAzureOpenAiOAuth2Settings_AreNotBackwardsCompatible() throws IOException {
        testSerializationIsNotBackwardsCompatible(
            AZURE_OPENAI_OAUTH_SETTINGS,
            i -> i.oAuth2Settings() != null,
            "Cannot send OAuth2 settings to an older node. Please wait until all nodes are upgraded before using OAuth2."
        );
    }
}
