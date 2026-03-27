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
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiOAuth2Settings;
import org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiOAuth2SettingsTests;
import org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiServiceFields;
import org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static org.elasticsearch.test.BWCVersions.DEFAULT_BWC_VERSIONS;
import static org.elasticsearch.xpack.inference.Utils.randomSimilarityMeasure;
import static org.elasticsearch.xpack.inference.services.ServiceFields.DIMENSIONS_SET_BY_USER;
import static org.elasticsearch.xpack.inference.services.ServiceFields.SIMILARITY;
import static org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiOAuth2Settings.AZURE_OPENAI_OAUTH_SETTINGS;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class AzureOpenAiEmbeddingsServiceSettingsTests extends AzureOpenAiServiceSettingsTests<AzureOpenAiEmbeddingsServiceSettings> {

    private static final int DIMENSIONS_VALUE = 1536;
    private static final int MAX_INPUT_TOKENS_VALUE = 512;

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

    public static Map<String, Object> getPersistentAzureOpenAiServiceSettingsMap(
        String resourceName,
        String deploymentId,
        String apiVersion,
        @Nullable Integer dimensions,
        @Nullable Integer maxInputTokens
    ) {
        var map = new HashMap<String, Object>();

        map.put(AzureOpenAiServiceFields.RESOURCE_NAME, resourceName);
        map.put(AzureOpenAiServiceFields.DEPLOYMENT_ID, deploymentId);
        map.put(AzureOpenAiServiceFields.API_VERSION, apiVersion);

        if (dimensions != null) {
            map.put(ServiceFields.DIMENSIONS, dimensions);
            map.put(DIMENSIONS_SET_BY_USER, true);
        } else {
            map.put(DIMENSIONS_SET_BY_USER, false);
        }

        if (maxInputTokens != null) {
            map.put(ServiceFields.MAX_INPUT_TOKENS, maxInputTokens);
        }

        return map;
    }

    public static Map<String, Object> getPersistentAzureOpenAiServiceSettingsMap(
        String resourceName,
        String deploymentId,
        String apiVersion,
        @Nullable Integer dimensions,
        @Nullable Integer maxInputTokens,
        @Nullable String clientId,
        @Nullable List<String> scopes,
        @Nullable String tenantId
    ) {
        var map = getPersistentAzureOpenAiServiceSettingsMap(resourceName, deploymentId, apiVersion, dimensions, maxInputTokens);

        if (clientId != null) {
            map.put(OAuth2Settings.CLIENT_ID_FIELD, clientId);
        }

        if (scopes != null) {
            map.put(OAuth2Settings.SCOPES_FIELD, scopes);
        }

        if (tenantId != null) {
            map.put(AzureOpenAiOAuth2Settings.TENANT_ID_FIELD, tenantId);
        }

        return map;
    }

    public static Map<String, Object> getRequestAzureOpenAiServiceSettingsMap(
        String resourceName,
        String deploymentId,
        String apiVersion,
        @Nullable Integer dimensions,
        @Nullable Integer maxInputTokens
    ) {
        var map = new HashMap<String, Object>();

        map.put(AzureOpenAiServiceFields.RESOURCE_NAME, resourceName);
        map.put(AzureOpenAiServiceFields.DEPLOYMENT_ID, deploymentId);
        map.put(AzureOpenAiServiceFields.API_VERSION, apiVersion);

        if (dimensions != null) {
            map.put(ServiceFields.DIMENSIONS, dimensions);
        }

        if (maxInputTokens != null) {
            map.put(ServiceFields.MAX_INPUT_TOKENS, maxInputTokens);
        }

        return map;
    }

    @Override
    protected AzureOpenAiEmbeddingsServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        return AzureOpenAiEmbeddingsServiceSettings.fromMap(map, context);
    }

    @Override
    protected Map<String, Object> buildMinimalPersistentMapWithOAuth2() {
        return getPersistentAzureOpenAiServiceSettingsMap(
            AzureOpenAiServiceSettingsTests.RESOURCE_NAME_VALUE,
            AzureOpenAiServiceSettingsTests.DEPLOYMENT_ID_VALUE,
            AzureOpenAiServiceSettingsTests.API_VERSION_VALUE,
            null,
            null,
            OAuth2SettingsTests.CLIENT_ID_VALUE,
            OAuth2SettingsTests.SCOPES_VALUE,
            AzureOpenAiServiceSettingsTests.TENANT_ID_VALUE
        );
    }

    @Override
    protected AzureOpenAiEmbeddingsServiceSettings createSettingsWithOAuth2(@Nullable AzureOpenAiOAuth2Settings oAuth2Settings) {
        return new AzureOpenAiEmbeddingsServiceSettings(
            AzureOpenAiServiceSettingsTests.RESOURCE_NAME_VALUE,
            AzureOpenAiServiceSettingsTests.DEPLOYMENT_ID_VALUE,
            AzureOpenAiServiceSettingsTests.API_VERSION_VALUE,
            null,
            true,
            null,
            null,
            null,
            oAuth2Settings
        );
    }

    public void testFromMap_Request_CreatesSettingsCorrectly() {
        var serviceSettings = AzureOpenAiEmbeddingsServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
                    AzureOpenAiServiceFields.RESOURCE_NAME,
                    RESOURCE_NAME_VALUE,
                    AzureOpenAiServiceFields.DEPLOYMENT_ID,
                    DEPLOYMENT_ID_VALUE,
                    AzureOpenAiServiceFields.API_VERSION,
                    API_VERSION_VALUE,
                    ServiceFields.DIMENSIONS,
                    DIMENSIONS_VALUE,
                    ServiceFields.MAX_INPUT_TOKENS,
                    MAX_INPUT_TOKENS_VALUE,
                    SIMILARITY,
                    SimilarityMeasure.COSINE.toString()
                )
            ),
            ConfigurationParseContext.REQUEST
        );

        assertThat(
            serviceSettings,
            is(
                new AzureOpenAiEmbeddingsServiceSettings(
                    RESOURCE_NAME_VALUE,
                    DEPLOYMENT_ID_VALUE,
                    API_VERSION_VALUE,
                    DIMENSIONS_VALUE,
                    true,
                    MAX_INPUT_TOKENS_VALUE,
                    SimilarityMeasure.COSINE,
                    null
                )
            )
        );
    }

    public void testFromMap_RequestWithRateLimit_CreatesSettingsCorrectly() {
        var serviceSettings = AzureOpenAiEmbeddingsServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
                    AzureOpenAiServiceFields.RESOURCE_NAME,
                    RESOURCE_NAME_VALUE,
                    AzureOpenAiServiceFields.DEPLOYMENT_ID,
                    DEPLOYMENT_ID_VALUE,
                    AzureOpenAiServiceFields.API_VERSION,
                    API_VERSION_VALUE,
                    ServiceFields.DIMENSIONS,
                    DIMENSIONS_VALUE,
                    ServiceFields.MAX_INPUT_TOKENS,
                    MAX_INPUT_TOKENS_VALUE,
                    SIMILARITY,
                    SimilarityMeasure.COSINE.toString(),
                    RateLimitSettings.FIELD_NAME,
                    new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, RATE_LIMIT_REQUESTS_PER_MINUTE))
                )
            ),
            ConfigurationParseContext.REQUEST
        );

        assertThat(
            serviceSettings,
            is(
                new AzureOpenAiEmbeddingsServiceSettings(
                    RESOURCE_NAME_VALUE,
                    DEPLOYMENT_ID_VALUE,
                    API_VERSION_VALUE,
                    DIMENSIONS_VALUE,
                    true,
                    MAX_INPUT_TOKENS_VALUE,
                    SimilarityMeasure.COSINE,
                    new RateLimitSettings(RATE_LIMIT_REQUESTS_PER_MINUTE)
                )
            )
        );
    }

    public void testFromMap_Request_DimensionsSetByUser_IsFalse_WhenDimensionsAreNotPresent() {
        var serviceSettings = AzureOpenAiEmbeddingsServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
                    AzureOpenAiServiceFields.RESOURCE_NAME,
                    RESOURCE_NAME_VALUE,
                    AzureOpenAiServiceFields.DEPLOYMENT_ID,
                    DEPLOYMENT_ID_VALUE,
                    AzureOpenAiServiceFields.API_VERSION,
                    API_VERSION_VALUE,
                    ServiceFields.MAX_INPUT_TOKENS,
                    MAX_INPUT_TOKENS_VALUE
                )
            ),
            ConfigurationParseContext.REQUEST
        );

        assertThat(
            serviceSettings,
            is(
                new AzureOpenAiEmbeddingsServiceSettings(
                    RESOURCE_NAME_VALUE,
                    DEPLOYMENT_ID_VALUE,
                    API_VERSION_VALUE,
                    null,
                    false,
                    MAX_INPUT_TOKENS_VALUE,
                    null,
                    null
                )
            )
        );
    }

    public void testFromMap_Request_DimensionsSetByUser_ShouldThrowWhenPresent() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> AzureOpenAiEmbeddingsServiceSettings.fromMap(
                new HashMap<>(
                    Map.of(
                        AzureOpenAiServiceFields.RESOURCE_NAME,
                        RESOURCE_NAME_VALUE,
                        AzureOpenAiServiceFields.DEPLOYMENT_ID,
                        DEPLOYMENT_ID_VALUE,
                        AzureOpenAiServiceFields.API_VERSION,
                        API_VERSION_VALUE,
                        ServiceFields.MAX_INPUT_TOKENS,
                        MAX_INPUT_TOKENS_VALUE,
                        ServiceFields.DIMENSIONS,
                        DIMENSIONS_VALUE,
                        DIMENSIONS_SET_BY_USER,
                        false
                    )
                ),
                ConfigurationParseContext.REQUEST
            )
        );

        MatcherAssert.assertThat(
            thrownException.getMessage(),
            containsString(Strings.format(ERROR_DIMENSIONS_SET_BY_USER_NOT_ALLOWED, DIMENSIONS_SET_BY_USER))
        );
    }

    public void testFromMap_ThrowsException_WhenDimensionsAreZero() {
        var dimensions = 0;

        var settingsMap = getRequestAzureOpenAiServiceSettingsMap(
            RESOURCE_NAME_VALUE,
            DEPLOYMENT_ID_VALUE,
            API_VERSION_VALUE,
            dimensions,
            null
        );

        var thrownException = expectThrows(
            ValidationException.class,
            () -> AzureOpenAiEmbeddingsServiceSettings.fromMap(settingsMap, ConfigurationParseContext.REQUEST)
        );

        assertThat(thrownException.getMessage(), containsString(Strings.format(ERROR_DIMENSIONS, dimensions)));
    }

    public void testFromMap_ThrowsException_WhenDimensionsAreNegative() {
        var dimensions = randomNegativeInt();

        var settingsMap = getRequestAzureOpenAiServiceSettingsMap(
            RESOURCE_NAME_VALUE,
            DEPLOYMENT_ID_VALUE,
            API_VERSION_VALUE,
            dimensions,
            null
        );

        var thrownException = expectThrows(
            ValidationException.class,
            () -> AzureOpenAiEmbeddingsServiceSettings.fromMap(settingsMap, ConfigurationParseContext.REQUEST)
        );

        assertThat(thrownException.getMessage(), containsString(Strings.format(ERROR_DIMENSIONS, dimensions)));
    }

    public void testFromMap_ThrowsException_WhenMaxInputTokensAreZero() {
        var maxInputTokens = 0;

        var settingsMap = getRequestAzureOpenAiServiceSettingsMap(
            RESOURCE_NAME_VALUE,
            DEPLOYMENT_ID_VALUE,
            API_VERSION_VALUE,
            null,
            maxInputTokens
        );

        var thrownException = expectThrows(
            ValidationException.class,
            () -> AzureOpenAiEmbeddingsServiceSettings.fromMap(settingsMap, ConfigurationParseContext.REQUEST)
        );

        assertThat(thrownException.getMessage(), containsString(ERROR_MAX_INPUT_TOKENS_ZERO));
    }

    public void testFromMap_ThrowsException_WhenMaxInputTokensAreNegative() {
        var maxInputTokens = randomNegativeInt();

        var settingsMap = getRequestAzureOpenAiServiceSettingsMap(
            RESOURCE_NAME_VALUE,
            DEPLOYMENT_ID_VALUE,
            API_VERSION_VALUE,
            null,
            maxInputTokens
        );

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
                    RESOURCE_NAME_VALUE,
                    AzureOpenAiServiceFields.DEPLOYMENT_ID,
                    DEPLOYMENT_ID_VALUE,
                    AzureOpenAiServiceFields.API_VERSION,
                    API_VERSION_VALUE,
                    ServiceFields.DIMENSIONS,
                    DIMENSIONS_VALUE,
                    DIMENSIONS_SET_BY_USER,
                    false,
                    ServiceFields.MAX_INPUT_TOKENS,
                    MAX_INPUT_TOKENS_VALUE,
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
                    RESOURCE_NAME_VALUE,
                    DEPLOYMENT_ID_VALUE,
                    API_VERSION_VALUE,
                    DIMENSIONS_VALUE,
                    false,
                    MAX_INPUT_TOKENS_VALUE,
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
                    RESOURCE_NAME_VALUE,
                    AzureOpenAiServiceFields.DEPLOYMENT_ID,
                    DEPLOYMENT_ID_VALUE,
                    AzureOpenAiServiceFields.API_VERSION,
                    API_VERSION_VALUE,
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
                    RESOURCE_NAME_VALUE,
                    DEPLOYMENT_ID_VALUE,
                    API_VERSION_VALUE,
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
                    RESOURCE_NAME_VALUE,
                    AzureOpenAiServiceFields.DEPLOYMENT_ID,
                    DEPLOYMENT_ID_VALUE,
                    AzureOpenAiServiceFields.API_VERSION,
                    API_VERSION_VALUE,
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
                    RESOURCE_NAME_VALUE,
                    DEPLOYMENT_ID_VALUE,
                    API_VERSION_VALUE,
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
                        RESOURCE_NAME_VALUE,
                        AzureOpenAiServiceFields.DEPLOYMENT_ID,
                        DEPLOYMENT_ID_VALUE,
                        AzureOpenAiServiceFields.API_VERSION,
                        API_VERSION_VALUE,
                        ServiceFields.DIMENSIONS,
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
            RESOURCE_NAME_VALUE,
            DEPLOYMENT_ID_VALUE,
            API_VERSION_VALUE,
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
                    "requests_per_minute":1440
                },
                "dimensions_set_by_user":true
            }
            """, RESOURCE_NAME_VALUE, DEPLOYMENT_ID_VALUE, API_VERSION_VALUE))));
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var entity = new AzureOpenAiEmbeddingsServiceSettings(
            RESOURCE_NAME_VALUE,
            DEPLOYMENT_ID_VALUE,
            API_VERSION_VALUE,
            DIMENSIONS_VALUE,
            false,
            MAX_INPUT_TOKENS_VALUE,
            null,
            new RateLimitSettings(RATE_LIMIT_REQUESTS_PER_MINUTE)
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
                                "dimensions":%d,
                                "max_input_tokens":%d,
                                "dimensions_set_by_user":false
                            }
                            """,
                        RESOURCE_NAME_VALUE,
                        DEPLOYMENT_ID_VALUE,
                        API_VERSION_VALUE,
                        RATE_LIMIT_REQUESTS_PER_MINUTE,
                        DIMENSIONS_VALUE,
                        MAX_INPUT_TOKENS_VALUE
                    )
                )
            )
        );
    }

    public void testToFilteredXContent_WritesAllValues_Except_DimensionsSetByUser() throws IOException {
        var entity = new AzureOpenAiEmbeddingsServiceSettings(
            RESOURCE_NAME_VALUE,
            DEPLOYMENT_ID_VALUE,
            API_VERSION_VALUE,
            DIMENSIONS_VALUE,
            false,
            MAX_INPUT_TOKENS_VALUE,
            null,
            new RateLimitSettings(RATE_LIMIT_REQUESTS_PER_MINUTE)
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
                                "resource_name":"%s",
                                "deployment_id":"%s",
                                "api_version":"%s",
                                "rate_limit":{
                                    "requests_per_minute":%d
                                },
                                "dimensions":%d,
                                "max_input_tokens":%d
                            }
                            """,
                        RESOURCE_NAME_VALUE,
                        DEPLOYMENT_ID_VALUE,
                        API_VERSION_VALUE,
                        RATE_LIMIT_REQUESTS_PER_MINUTE,
                        DIMENSIONS_VALUE,
                        MAX_INPUT_TOKENS_VALUE
                    )
                )
            )
        );
    }

    public void testToXContent_WritesAllValues_WithOAuth2() throws IOException {
        var entity = new AzureOpenAiEmbeddingsServiceSettings(
            RESOURCE_NAME_VALUE,
            DEPLOYMENT_ID_VALUE,
            API_VERSION_VALUE,
            DIMENSIONS_VALUE,
            false,
            MAX_INPUT_TOKENS_VALUE,
            null,
            new RateLimitSettings(RATE_LIMIT_REQUESTS_PER_MINUTE),
            new AzureOpenAiOAuth2Settings(
                new OAuth2Settings(OAuth2SettingsTests.CLIENT_ID_VALUE, OAuth2SettingsTests.SCOPES_VALUE),
                TENANT_ID_VALUE
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
                                "client_id":"%s",
                                "scopes":%s,
                                "tenant_id":"%s",
                                "dimensions":%d,
                                "max_input_tokens":%d,
                                "dimensions_set_by_user":false
                            }
                            """,
                        RESOURCE_NAME_VALUE,
                        DEPLOYMENT_ID_VALUE,
                        API_VERSION_VALUE,
                        RATE_LIMIT_REQUESTS_PER_MINUTE,
                        OAuth2SettingsTests.CLIENT_ID_VALUE,
                        JsonUtils.toJson(OAuth2SettingsTests.SCOPES_VALUE, ""),
                        TENANT_ID_VALUE,
                        DIMENSIONS_VALUE,
                        MAX_INPUT_TOKENS_VALUE
                    )
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
