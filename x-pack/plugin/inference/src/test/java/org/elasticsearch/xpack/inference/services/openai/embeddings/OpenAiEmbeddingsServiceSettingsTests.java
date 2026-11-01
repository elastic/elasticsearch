/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai.embeddings;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.common.oauth2.OAuth2SettingsTests;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.openai.OpenAiOAuth2Settings;
import org.elasticsearch.xpack.inference.services.openai.OpenAiOAuth2SettingsTests;
import org.elasticsearch.xpack.inference.services.openai.OpenAiServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.Utils.randomSimilarityMeasure;
import static org.elasticsearch.xpack.inference.common.JsonUtils.toJson;
import static org.elasticsearch.xpack.inference.services.ServiceFields.DIMENSIONS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MAX_INPUT_TOKENS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MODEL_ID;
import static org.elasticsearch.xpack.inference.services.ServiceFields.SIMILARITY;
import static org.elasticsearch.xpack.inference.services.ServiceFields.URL;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createUri;
import static org.elasticsearch.xpack.inference.services.openai.OpenAiOAuth2Settings.OPENAI_OAUTH2_SETTINGS;
import static org.elasticsearch.xpack.inference.services.openai.OpenAiServiceFields.ORGANIZATION;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class OpenAiEmbeddingsServiceSettingsTests extends OpenAiServiceSettingsTests<OpenAiEmbeddingsServiceSettings> {

    private static final SimilarityMeasure TEST_SIMILARITY = SimilarityMeasure.DOT_PRODUCT;
    private static final SimilarityMeasure INITIAL_TEST_SIMILARITY = SimilarityMeasure.COSINE;

    private static final int TEST_DIMENSIONS = 1536;
    private static final int INITIAL_TEST_DIMENSIONS = 768;

    private static final int TEST_MAX_INPUT_TOKENS = 512;
    private static final int INITIAL_TEST_MAX_INPUT_TOKENS = 128;

    @Override
    protected RateLimitSettings getDefaultRateLimitSettings() {
        return OpenAiEmbeddingsServiceSettings.DEFAULT_RATE_LIMIT_SETTINGS;
    }

    @Override
    protected OpenAiEmbeddingsServiceSettings fromMap(Map<String, Object> serviceSettingsMap, ConfigurationParseContext context) {
        return OpenAiEmbeddingsServiceSettings.fromMap(serviceSettingsMap, context);
    }

    @Override
    protected OpenAiEmbeddingsServiceSettings updateServiceSettings(
        OpenAiEmbeddingsServiceSettings serviceSettings,
        Map<String, Object> serviceSettingsMap
    ) {
        return serviceSettings.updateServiceSettings(serviceSettingsMap);
    }

    @Override
    protected Map<String, Object> buildRequiredFieldsServiceSettingsMap(ConfigurationParseContext context) {
        var settingsMap = buildRequiredFieldsServiceSettingsMap(TEST_MODEL_ID);
        return switch (context) {
            case REQUEST -> settingsMap;
            case PERSISTENT -> {
                settingsMap.put(ServiceFields.DIMENSIONS_SET_BY_USER, false);
                yield settingsMap;
            }
        };
    }

    @Override
    protected Map<String, Object> buildAllFieldsServiceSettingsMap(ConfigurationParseContext context) {
        var settingsMap = buildAllFieldsServiceSettingsMap(
            TEST_MODEL_ID,
            TEST_URI.toString(),
            TEST_ORGANIZATION_ID,
            TEST_MAX_INPUT_TOKENS,
            TEST_RATE_LIMIT,
            OAuth2SettingsTests.TEST_CLIENT_ID,
            OAuth2SettingsTests.TEST_SCOPES,
            OpenAiOAuth2SettingsTests.TEST_TOKEN_URL
        );
        settingsMap.put(SIMILARITY, TEST_SIMILARITY.toString());
        settingsMap.put(DIMENSIONS, TEST_DIMENSIONS);
        return switch (context) {
            case REQUEST -> settingsMap;
            case PERSISTENT -> {
                settingsMap.put(ServiceFields.DIMENSIONS_SET_BY_USER, true);
                yield settingsMap;
            }
        };
    }

    @Override
    protected OpenAiEmbeddingsServiceSettings createServiceSettings(@Nullable OpenAiOAuth2Settings oAuth2Settings) {
        return new OpenAiEmbeddingsServiceSettings(
            INITIAL_TEST_MODEL_ID,
            INITIAL_TEST_URI,
            INITIAL_TEST_ORGANIZATION_ID,
            INITIAL_TEST_SIMILARITY,
            INITIAL_TEST_DIMENSIONS,
            INITIAL_TEST_MAX_INPUT_TOKENS,
            true,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT),
            oAuth2Settings
        );
    }

    @Override
    protected void assertFromMapAllFields(OpenAiEmbeddingsServiceSettings serviceSettings, ConfigurationParseContext context) {
        super.assertFromMapAllFields(serviceSettings, context);
        assertThat(serviceSettings.rateLimitSettings(), is(new RateLimitSettings(TEST_RATE_LIMIT)));
        assertThat(serviceSettings.modelId(), is(TEST_MODEL_ID));
        assertThat(serviceSettings.uri(), is(TEST_URI));
        assertThat(serviceSettings.organizationId(), is(TEST_ORGANIZATION_ID));
        assertThat(serviceSettings.similarity(), is(TEST_SIMILARITY));
        assertThat(serviceSettings.dimensions(), is(TEST_DIMENSIONS));
        assertThat(serviceSettings.maxInputTokens(), is(TEST_MAX_INPUT_TOKENS));
        assertThat(serviceSettings.dimensionsSetByUser(), is(Boolean.TRUE));
    }

    @Override
    protected void assertFromMapRequiredFieldsOnly(OpenAiEmbeddingsServiceSettings serviceSettings, ConfigurationParseContext context) {
        super.assertFromMapRequiredFieldsOnly(serviceSettings, context);
        assertThat(serviceSettings.rateLimitSettings(), is(getDefaultRateLimitSettings()));
        assertThat(serviceSettings.modelId(), is(TEST_MODEL_ID));
        assertThat(serviceSettings.dimensionsSetByUser(), is(Boolean.FALSE));
        assertThat(serviceSettings.similarity(), is(nullValue()));
        assertThat(serviceSettings.dimensions(), is(nullValue()));
        assertThat(serviceSettings.maxInputTokens(), is(nullValue()));
    }

    @Override
    protected void assertFieldsAfterUpdate(OpenAiEmbeddingsServiceSettings updatedSettings) {
        super.assertFieldsAfterUpdate(updatedSettings);
        assertThat(updatedSettings.rateLimitSettings(), is(new RateLimitSettings(TEST_RATE_LIMIT)));
        assertThat(updatedSettings.modelId(), is(INITIAL_TEST_MODEL_ID));
        assertThat(updatedSettings.uri(), is(INITIAL_TEST_URI));
        assertThat(updatedSettings.organizationId(), is(TEST_ORGANIZATION_ID));
        assertThat(updatedSettings.maxInputTokens(), is(TEST_MAX_INPUT_TOKENS));
        assertThat(updatedSettings.similarity(), is(INITIAL_TEST_SIMILARITY));
        assertThat(updatedSettings.dimensions(), is(INITIAL_TEST_DIMENSIONS));
        assertThat(updatedSettings.dimensionsSetByUser(), is(Boolean.TRUE));
    }

    /**
     * Versions before {@link OpenAiOAuth2Settings#OPENAI_OAUTH2_SETTINGS} throw an exception when serializing non-null OAuth2
     * settings, so we filter those out of the bwc versions to avoid test failures.
     * The logic is tested directly by {@link #testOpenAiOAuth2Settings_AreNotBackwardsCompatible}
     */
    @Override
    protected Collection<TransportVersion> bwcVersions() {
        return super.bwcVersions().stream().filter(version -> version.supports(OPENAI_OAUTH2_SETTINGS)).toList();
    }

    public static OpenAiEmbeddingsServiceSettings createRandomWithNonNullUrl() {
        return createRandom(randomAlphaOfLength(15));
    }

    public static OpenAiEmbeddingsServiceSettings createRandom() {
        return createRandom(randomAlphaOfLengthOrNull(15));
    }

    private static OpenAiEmbeddingsServiceSettings createRandom(String url) {
        var modelId = randomAlphaOfLength(8);
        var organizationId = randomAlphaOfLengthOrNull(15);
        var similarityMeasure = randomBoolean() ? randomSimilarityMeasure() : null;
        var dimensions = randomBoolean() ? randomIntBetween(1, 1000) : null;
        var maxInputTokens = randomBoolean() ? null : randomIntBetween(128, 256);
        var oAuth2Settings = randomFrom(OpenAiOAuth2SettingsTests.createRandom(), null);
        return new OpenAiEmbeddingsServiceSettings(
            modelId,
            createUri(url),
            organizationId,
            similarityMeasure,
            dimensions,
            maxInputTokens,
            randomBoolean(),
            RateLimitSettingsTests.createRandom(),
            oAuth2Settings
        );
    }

    public void testFromMap_Request_AllFields_CreatesSettingsCorrectly() {
        var serviceSettings = OpenAiEmbeddingsServiceSettings.fromMap(
            buildServiceSettingsMap(
                TEST_MODEL_ID,
                TEST_URI.toString(),
                TEST_ORGANIZATION_ID,
                TEST_SIMILARITY.toString(),
                TEST_DIMENSIONS,
                TEST_MAX_INPUT_TOKENS,
                TEST_RATE_LIMIT,
                null
            ),
            ConfigurationParseContext.REQUEST
        );

        assertThat(
            serviceSettings,
            is(
                new OpenAiEmbeddingsServiceSettings(
                    TEST_MODEL_ID,
                    TEST_URI,
                    TEST_ORGANIZATION_ID,
                    TEST_SIMILARITY,
                    TEST_DIMENSIONS,
                    TEST_MAX_INPUT_TOKENS,
                    true,
                    new RateLimitSettings(TEST_RATE_LIMIT)
                )
            )
        );
    }

    public void testFromMap_Request_OnlyMandatoryFields_CreatesSettingsCorrectly() {
        var serviceSettings = OpenAiEmbeddingsServiceSettings.fromMap(
            buildServiceSettingsMap(TEST_MODEL_ID, null, null, null, null, null, null, null),
            ConfigurationParseContext.REQUEST
        );

        assertThat(
            serviceSettings,
            is(new OpenAiEmbeddingsServiceSettings(TEST_MODEL_ID, (URI) null, null, null, null, null, false, null))
        );
    }

    public void testFromMap_Request_DimensionsSetByUser_IsFalse_WhenDimensionsAreNotPresent() {
        var serviceSettings = OpenAiEmbeddingsServiceSettings.fromMap(
            buildServiceSettingsMap(
                TEST_MODEL_ID,
                TEST_URI.toString(),
                TEST_ORGANIZATION_ID,
                TEST_SIMILARITY.toString(),
                null,
                TEST_MAX_INPUT_TOKENS,
                null,
                null
            ),
            ConfigurationParseContext.REQUEST
        );

        assertThat(
            serviceSettings,
            is(
                new OpenAiEmbeddingsServiceSettings(
                    TEST_MODEL_ID,
                    TEST_URI,
                    TEST_ORGANIZATION_ID,
                    TEST_SIMILARITY,
                    null,
                    TEST_MAX_INPUT_TOKENS,
                    false,
                    null
                )
            )
        );
    }

    public void testFromMap_Persistent_AllFields_CreatesSettingsCorrectly() {
        var serviceSettings = OpenAiEmbeddingsServiceSettings.fromMap(
            buildServiceSettingsMap(
                TEST_MODEL_ID,
                TEST_URI.toString(),
                TEST_ORGANIZATION_ID,
                TEST_SIMILARITY.toString(),
                TEST_DIMENSIONS,
                TEST_MAX_INPUT_TOKENS,
                TEST_RATE_LIMIT,
                false
            ),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(
            serviceSettings,
            is(
                new OpenAiEmbeddingsServiceSettings(
                    TEST_MODEL_ID,
                    TEST_URI,
                    TEST_ORGANIZATION_ID,
                    TEST_SIMILARITY,
                    TEST_DIMENSIONS,
                    TEST_MAX_INPUT_TOKENS,
                    false,
                    new RateLimitSettings(TEST_RATE_LIMIT)
                )
            )
        );
    }

    public void testFromMap_Persistent_DoesNotThrow_WhenDimensionsIsNull() {
        var settings = OpenAiEmbeddingsServiceSettings.fromMap(
            buildServiceSettingsMap(TEST_MODEL_ID, null, null, null, null, null, null, true),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(settings, is(new OpenAiEmbeddingsServiceSettings(TEST_MODEL_ID, (URI) null, null, null, null, null, true, null)));
    }

    public void testFromMap_Persistent_OnlyMandatoryFields_CreatesSettingsCorrectly() {
        var settings = OpenAiEmbeddingsServiceSettings.fromMap(
            buildServiceSettingsMap(TEST_MODEL_ID, null, null, null, null, null, null, null),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(settings, is(new OpenAiEmbeddingsServiceSettings(TEST_MODEL_ID, (URI) null, null, null, null, null, false, null)));
    }

    public void testFromMap_NoModelId_ThrowsValidationError() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> OpenAiEmbeddingsServiceSettings.fromMap(
                buildServiceSettingsMap(null, TEST_URI.toString(), TEST_ORGANIZATION_ID, null, null, null, null, null),
                ConfigurationParseContext.REQUEST
            )
        );

        assertThat(thrownException.validationErrors().size(), is(1));
        assertThat(
            thrownException.validationErrors().getFirst(),
            is(Strings.format("[service_settings] does not contain the required setting [%s]", MODEL_ID))
        );
    }

    public void testFromMap_DimensionsAreZero_ThrowsValidationError() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> OpenAiEmbeddingsServiceSettings.fromMap(
                buildServiceSettingsMap(TEST_MODEL_ID, TEST_URI.toString(), TEST_ORGANIZATION_ID, null, 0, null, null, null),
                ConfigurationParseContext.REQUEST
            )
        );

        assertThat(thrownException.validationErrors().size(), is(1));
        assertThat(
            thrownException.validationErrors().getFirst(),
            is(Strings.format("[service_settings] Invalid value [0]. [%s] must be a positive integer", DIMENSIONS))
        );
    }

    public void testFromMap_MaxInputTokensAreZero_ThrowsValidationError() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> OpenAiEmbeddingsServiceSettings.fromMap(
                buildServiceSettingsMap(TEST_MODEL_ID, TEST_URI.toString(), TEST_ORGANIZATION_ID, null, null, 0, null, null),
                ConfigurationParseContext.REQUEST
            )
        );

        assertThat(thrownException.validationErrors().size(), is(1));
        assertThat(
            thrownException.validationErrors().getFirst(),
            is(Strings.format("[service_settings] Invalid value [0]. [%s] must be a positive integer", MAX_INPUT_TOKENS))
        );
    }

    public void testFromMap_SimilarityIsInvalid_ThrowsValidationError() {
        var invalidSimilarity = "by_size";
        var thrownException = expectThrows(
            ValidationException.class,
            () -> OpenAiEmbeddingsServiceSettings.fromMap(
                buildServiceSettingsMap(TEST_MODEL_ID, null, null, invalidSimilarity, null, null, null, null),
                ConfigurationParseContext.REQUEST
            )
        );

        assertThat(thrownException.validationErrors().size(), is(1));
        assertThat(
            thrownException.validationErrors().getFirst(),
            is("[service_settings] Invalid value [by_size] received. [similarity] must be one of [cosine, dot_product, l2_norm]")
        );
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var entity = new OpenAiEmbeddingsServiceSettings(
            TEST_MODEL_ID,
            TEST_URI,
            TEST_ORGANIZATION_ID,
            TEST_SIMILARITY,
            TEST_DIMENSIONS,
            TEST_MAX_INPUT_TOKENS,
            false,
            new RateLimitSettings(TEST_RATE_LIMIT)
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
                                "model_id": "%s",
                                "url": "%s",
                                "organization_id": "%s",
                                "similarity": "%s",
                                "dimensions": %d,
                                "max_input_tokens": %d,
                                "rate_limit": {
                                    "requests_per_minute": %d
                                },
                                "dimensions_set_by_user": false
                            }
                            """,
                        TEST_MODEL_ID,
                        TEST_URI.toString(),
                        TEST_ORGANIZATION_ID,
                        TEST_SIMILARITY.toString(),
                        TEST_DIMENSIONS,
                        TEST_MAX_INPUT_TOKENS,
                        TEST_RATE_LIMIT
                    )
                )
            )
        );
    }

    public void testToXContent_WithOAuth2_WritesAllValues() throws IOException {
        var entity = new OpenAiEmbeddingsServiceSettings(
            TEST_MODEL_ID,
            TEST_URI,
            TEST_ORGANIZATION_ID,
            TEST_SIMILARITY,
            TEST_DIMENSIONS,
            TEST_MAX_INPUT_TOKENS,
            true,
            new RateLimitSettings(TEST_RATE_LIMIT),
            new OpenAiOAuth2Settings(
                OAuth2SettingsTests.TEST_CLIENT_ID,
                OAuth2SettingsTests.TEST_SCOPES,
                OpenAiOAuth2SettingsTests.TEST_TOKEN_URL
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
                                "client_id": "%s",
                                "scopes": %s,
                                "token_url": "%s",
                                "model_id": "%s",
                                "url": "%s",
                                "organization_id": "%s",
                                "similarity": "%s",
                                "dimensions": %d,
                                "max_input_tokens": %d,
                                "rate_limit": {
                                    "requests_per_minute": %d
                                },
                                "dimensions_set_by_user": true
                            }
                            """,
                        OAuth2SettingsTests.TEST_CLIENT_ID,
                        toJson(OAuth2SettingsTests.TEST_SCOPES, ""),
                        OpenAiOAuth2SettingsTests.TEST_TOKEN_URL,
                        TEST_MODEL_ID,
                        TEST_URI.toString(),
                        TEST_ORGANIZATION_ID,
                        TEST_SIMILARITY.toString(),
                        TEST_DIMENSIONS,
                        TEST_MAX_INPUT_TOKENS,
                        TEST_RATE_LIMIT
                    )
                )
            )
        );
    }

    public void testToXContent_OnlyWritesMandatoryFields_WhenOtherFieldsAreNull() throws IOException {
        var entity = new OpenAiEmbeddingsServiceSettings(TEST_MODEL_ID, (URI) null, null, null, null, null, true, null);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        var xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is(XContentHelper.stripWhitespace(Strings.format("""
            {
                "model_id": "%s",
                "rate_limit": {
                    "requests_per_minute": %d
                },
                "dimensions_set_by_user": true
            }
            """, TEST_MODEL_ID, OpenAiEmbeddingsServiceSettings.DEFAULT_RATE_LIMIT_SETTINGS.requestsPerTimeUnit()))));
    }

    public void testToFilteredXContent_WritesAllValues_ExceptDimensionsSetByUser() throws IOException {
        var entity = new OpenAiEmbeddingsServiceSettings(
            TEST_MODEL_ID,
            TEST_URI,
            TEST_ORGANIZATION_ID,
            TEST_SIMILARITY,
            TEST_DIMENSIONS,
            TEST_MAX_INPUT_TOKENS,
            false,
            new RateLimitSettings(TEST_RATE_LIMIT)
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
                                "model_id": "%s",
                                "url": "%s",
                                "organization_id": "%s",
                                "similarity": "%s",
                                "dimensions": %d,
                                "max_input_tokens": %d,
                                "rate_limit": {
                                    "requests_per_minute": %d
                                }
                            }
                            """,
                        TEST_MODEL_ID,
                        TEST_URI.toString(),
                        TEST_ORGANIZATION_ID,
                        TEST_SIMILARITY.toString(),
                        TEST_DIMENSIONS,
                        TEST_MAX_INPUT_TOKENS,
                        TEST_RATE_LIMIT
                    )
                )
            )
        );
    }

    @Override
    protected Writeable.Reader<OpenAiEmbeddingsServiceSettings> instanceReader() {
        return OpenAiEmbeddingsServiceSettings::new;
    }

    @Override
    protected OpenAiEmbeddingsServiceSettings createTestInstance() {
        return createRandomWithNonNullUrl();
    }

    @Override
    protected OpenAiEmbeddingsServiceSettings mutateInstance(OpenAiEmbeddingsServiceSettings instance) throws IOException {
        var modelId = instance.modelId();
        var uri = instance.uri();
        var organizationId = instance.organizationId();
        var similarity = instance.similarity();
        var dimensions = instance.dimensions();
        var maxInputTokens = instance.maxInputTokens();
        var dimensionsSetByUser = instance.dimensionsSetByUser();
        var rateLimitSettings = instance.rateLimitSettings();
        var oAuth2Settings = instance.oAuth2Settings();
        switch (randomInt(8)) {
            case 0 -> modelId = randomValueOtherThan(modelId, () -> randomAlphaOfLength(8));
            case 1 -> uri = randomValueOtherThan(uri, () -> randomFrom(createUri(randomAlphaOfLength(15)), null));
            case 2 -> organizationId = randomValueOtherThan(organizationId, () -> randomAlphaOfLengthOrNull(15));
            case 3 -> similarity = randomValueOtherThan(similarity, () -> randomFrom(randomSimilarityMeasure()));
            case 4 -> dimensions = randomValueOtherThan(dimensions, ESTestCase::randomNonNegativeIntOrNull);
            case 5 -> maxInputTokens = randomValueOtherThan(maxInputTokens, () -> randomFrom(randomIntBetween(128, 256), null));
            case 6 -> dimensionsSetByUser = dimensionsSetByUser == false;
            case 7 -> rateLimitSettings = randomValueOtherThan(rateLimitSettings, RateLimitSettingsTests::createRandom);
            case 8 -> oAuth2Settings = randomValueOtherThan(
                oAuth2Settings,
                () -> randomFrom(OpenAiOAuth2SettingsTests.createRandom(), null)
            );
            default -> throw new AssertionError("Illegal randomisation branch");
        }

        return new OpenAiEmbeddingsServiceSettings(
            modelId,
            uri,
            organizationId,
            similarity,
            dimensions,
            maxInputTokens,
            dimensionsSetByUser,
            rateLimitSettings,
            oAuth2Settings
        );
    }

    @Override
    protected OpenAiEmbeddingsServiceSettings mutateInstanceForVersion(OpenAiEmbeddingsServiceSettings instance, TransportVersion version) {
        if (version.supports(OPENAI_OAUTH2_SETTINGS)) {
            return instance;
        }

        return new OpenAiEmbeddingsServiceSettings(
            instance.modelId(),
            instance.uri(),
            instance.organizationId(),
            instance.similarity(),
            instance.dimensions(),
            instance.maxInputTokens(),
            instance.dimensionsSetByUser(),
            instance.rateLimitSettings()
        );
    }

    public static Map<String, Object> buildServiceSettingsMap(
        @Nullable String modelId,
        @Nullable String url,
        @Nullable String organizationId,
        @Nullable String similarity,
        @Nullable Integer dimensions,
        @Nullable Integer maxInputTokens,
        @Nullable Integer rateLimit,
        @Nullable Boolean dimensionsSetByUser
    ) {
        var map = new HashMap<String, Object>();
        if (modelId != null) {
            map.put(MODEL_ID, modelId);
        }
        if (url != null) {
            map.put(URL, url);
        }
        if (organizationId != null) {
            map.put(ORGANIZATION, organizationId);
        }
        if (similarity != null) {
            map.put(SIMILARITY, similarity);
        }
        if (dimensions != null) {
            map.put(DIMENSIONS, dimensions);
        }
        if (maxInputTokens != null) {
            map.put(MAX_INPUT_TOKENS, maxInputTokens);
        }
        if (rateLimit != null) {
            map.put(RateLimitSettings.FIELD_NAME, new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, rateLimit)));
        }
        if (dimensionsSetByUser != null) {
            map.put(ServiceFields.DIMENSIONS_SET_BY_USER, dimensionsSetByUser);
        }
        return map;
    }
}
