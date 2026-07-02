/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai.completion;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.common.oauth2.OAuth2SettingsTests;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.openai.OpenAiOAuth2Settings;
import org.elasticsearch.xpack.inference.services.openai.OpenAiOAuth2SettingsTests;
import org.elasticsearch.xpack.inference.services.openai.OpenAiServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

import static org.elasticsearch.xpack.inference.common.JsonUtils.toJson;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MODEL_ID;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createUri;
import static org.elasticsearch.xpack.inference.services.openai.OpenAiOAuth2Settings.OPENAI_OAUTH2_SETTINGS;
import static org.hamcrest.Matchers.is;

public class OpenAiChatCompletionServiceSettingsTests extends OpenAiServiceSettingsTests<OpenAiChatCompletionServiceSettings> {

    private static final int TEST_MAX_INPUT_TOKENS = 4096;
    private static final int INITIAL_TEST_MAX_INPUT_TOKENS = 1024;

    @Override
    protected RateLimitSettings getDefaultRateLimitSettings() {
        return OpenAiChatCompletionServiceSettings.DEFAULT_RATE_LIMIT_SETTINGS;
    }

    @Override
    protected OpenAiChatCompletionServiceSettings fromMap(Map<String, Object> serviceSettingsMap, ConfigurationParseContext context) {
        return OpenAiChatCompletionServiceSettings.fromMap(serviceSettingsMap, context);
    }

    @Override
    protected OpenAiChatCompletionServiceSettings updateServiceSettings(
        OpenAiChatCompletionServiceSettings serviceSettings,
        Map<String, Object> serviceSettingsMap
    ) {
        return serviceSettings.updateServiceSettings(serviceSettingsMap);
    }

    @Override
    protected Map<String, Object> buildRequiredFieldsServiceSettingsMap(ConfigurationParseContext context) {
        return buildRequiredFieldsServiceSettingsMap(TEST_MODEL_ID);
    }

    @Override
    protected Map<String, Object> buildAllFieldsServiceSettingsMap(ConfigurationParseContext context) {
        return buildAllFieldsServiceSettingsMap(
            TEST_MODEL_ID,
            TEST_URI.toString(),
            TEST_ORGANIZATION_ID,
            TEST_MAX_INPUT_TOKENS,
            TEST_RATE_LIMIT,
            OAuth2SettingsTests.TEST_CLIENT_ID,
            OAuth2SettingsTests.TEST_SCOPES,
            OpenAiOAuth2SettingsTests.TEST_TOKEN_URL
        );
    }

    @Override
    protected OpenAiChatCompletionServiceSettings createServiceSettings(@Nullable OpenAiOAuth2Settings oAuth2Settings) {
        return new OpenAiChatCompletionServiceSettings(
            INITIAL_TEST_MODEL_ID,
            INITIAL_TEST_URI,
            INITIAL_TEST_ORGANIZATION_ID,
            INITIAL_TEST_MAX_INPUT_TOKENS,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT),
            oAuth2Settings
        );
    }

    @Override
    protected void assertFromMapAllFields(OpenAiChatCompletionServiceSettings serviceSettings, ConfigurationParseContext context) {
        super.assertFromMapAllFields(serviceSettings, context);
        assertThat(serviceSettings.rateLimitSettings(), is(new RateLimitSettings(TEST_RATE_LIMIT)));
        assertThat(serviceSettings.modelId(), is(TEST_MODEL_ID));
        assertThat(serviceSettings.uri(), is(TEST_URI));
        assertThat(serviceSettings.organizationId(), is(TEST_ORGANIZATION_ID));
        assertThat(serviceSettings.maxInputTokens(), is(TEST_MAX_INPUT_TOKENS));
    }

    @Override
    protected void assertFromMapRequiredFieldsOnly(OpenAiChatCompletionServiceSettings serviceSettings, ConfigurationParseContext context) {
        super.assertFromMapRequiredFieldsOnly(serviceSettings, context);
        assertThat(serviceSettings.rateLimitSettings(), is(getDefaultRateLimitSettings()));
        assertThat(serviceSettings.modelId(), is(TEST_MODEL_ID));
    }

    @Override
    protected void assertFieldsAfterUpdate(OpenAiChatCompletionServiceSettings updatedSettings) {
        super.assertFieldsAfterUpdate(updatedSettings);
        assertThat(updatedSettings.rateLimitSettings(), is(new RateLimitSettings(TEST_RATE_LIMIT)));
        assertThat(updatedSettings.modelId(), is(INITIAL_TEST_MODEL_ID));
        assertThat(updatedSettings.uri(), is(INITIAL_TEST_URI));
        assertThat(updatedSettings.organizationId(), is(TEST_ORGANIZATION_ID));
        assertThat(updatedSettings.maxInputTokens(), is(TEST_MAX_INPUT_TOKENS));
    }

    public void testFromMap_AllFields_CreatesSettingsCorrectly() {
        var serviceSettings = OpenAiChatCompletionServiceSettings.fromMap(
            buildServiceSettingsMap(TEST_MODEL_ID, TEST_URI.toString(), TEST_ORGANIZATION_ID, TEST_MAX_INPUT_TOKENS, TEST_RATE_LIMIT),
            randomFrom(ConfigurationParseContext.values())
        );

        assertThat(
            serviceSettings,
            is(
                new OpenAiChatCompletionServiceSettings(
                    TEST_MODEL_ID,
                    TEST_URI,
                    TEST_ORGANIZATION_ID,
                    TEST_MAX_INPUT_TOKENS,
                    new RateLimitSettings(TEST_RATE_LIMIT)
                )
            )
        );
    }

    public void testFromMap_OnlyMandatoryFields_CreatesSettingsCorrectly() {
        var serviceSettings = OpenAiChatCompletionServiceSettings.fromMap(
            buildServiceSettingsMap(TEST_MODEL_ID, null, null, null, null),
            randomFrom(ConfigurationParseContext.values())
        );

        assertThat(serviceSettings, is(new OpenAiChatCompletionServiceSettings(TEST_MODEL_ID, (URI) null, null, null, null)));
    }

    public void testFromMap_NoModelId_ThrowsValidationError() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> OpenAiChatCompletionServiceSettings.fromMap(
                buildServiceSettingsMap(null, TEST_URI.toString(), TEST_ORGANIZATION_ID, TEST_MAX_INPUT_TOKENS, TEST_RATE_LIMIT),
                randomFrom(ConfigurationParseContext.values())
            )
        );

        assertThat(thrownException.validationErrors().size(), is(1));
        assertThat(
            thrownException.validationErrors().getFirst(),
            is(Strings.format("[service_settings] does not contain the required setting [%s]", MODEL_ID))
        );
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var serviceSettings = new OpenAiChatCompletionServiceSettings(
            TEST_MODEL_ID,
            TEST_URI,
            TEST_ORGANIZATION_ID,
            TEST_MAX_INPUT_TOKENS,
            new RateLimitSettings(TEST_RATE_LIMIT)
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
        var xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is(XContentHelper.stripWhitespace(Strings.format("""
            {
                "model_id": "%s",
                "url": "%s",
                "organization_id": "%s",
                "max_input_tokens": %d,
                "rate_limit": {
                    "requests_per_minute": %d
                }
            }
            """, TEST_MODEL_ID, TEST_URI.toString(), TEST_ORGANIZATION_ID, TEST_MAX_INPUT_TOKENS, TEST_RATE_LIMIT))));
    }

    public void testToXContent_WithOAuth2_WritesAllValues() throws IOException {
        var serviceSettings = new OpenAiChatCompletionServiceSettings(
            TEST_MODEL_ID,
            TEST_URI,
            TEST_ORGANIZATION_ID,
            TEST_MAX_INPUT_TOKENS,
            new RateLimitSettings(TEST_RATE_LIMIT),
            new OpenAiOAuth2Settings(
                OAuth2SettingsTests.TEST_CLIENT_ID,
                OAuth2SettingsTests.TEST_SCOPES,
                OpenAiOAuth2SettingsTests.TEST_TOKEN_URL
            )
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
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
                                "max_input_tokens": %d,
                                "rate_limit": {
                                    "requests_per_minute": %d
                                }
                            }
                            """,
                        OAuth2SettingsTests.TEST_CLIENT_ID,
                        toJson(OAuth2SettingsTests.TEST_SCOPES, ""),
                        OpenAiOAuth2SettingsTests.TEST_TOKEN_URL,
                        TEST_MODEL_ID,
                        TEST_URI.toString(),
                        TEST_ORGANIZATION_ID,
                        TEST_MAX_INPUT_TOKENS,
                        TEST_RATE_LIMIT
                    )
                )
            )
        );
    }

    public void testToXContent_DoesNotWriteOptionalValues() throws IOException {
        var serviceSettings = new OpenAiChatCompletionServiceSettings(TEST_MODEL_ID, (URI) null, null, null, null);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
        var xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is(XContentHelper.stripWhitespace(Strings.format("""
            {
                "model_id": "%s",
                "rate_limit": {
                    "requests_per_minute": %d
                }
            }
            """, TEST_MODEL_ID, OpenAiChatCompletionServiceSettings.DEFAULT_RATE_LIMIT_SETTINGS.requestsPerTimeUnit()))));
    }

    public static OpenAiChatCompletionServiceSettings createRandom() {
        var oAuth2Settings = randomFrom(OpenAiOAuth2SettingsTests.createRandom(), null);
        return createRandom(oAuth2Settings);
    }

    public static OpenAiChatCompletionServiceSettings createRandom(@Nullable OpenAiOAuth2Settings oAuth2Settings) {
        return new OpenAiChatCompletionServiceSettings(
            randomAlphaOfLength(8),
            createUri("https://" + randomAlphaOfLength(10) + ".example"),
            randomAlphaOfLengthOrNull(15),
            randomFrom(randomIntBetween(128, 4096), null),
            RateLimitSettingsTests.createRandom(),
            oAuth2Settings
        );
    }

    public static OpenAiChatCompletionServiceSettings createRandomWithoutOAuth2() {
        return createRandom(null);
    }

    @Override
    protected Writeable.Reader<OpenAiChatCompletionServiceSettings> instanceReader() {
        return OpenAiChatCompletionServiceSettings::new;
    }

    @Override
    protected OpenAiChatCompletionServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected OpenAiChatCompletionServiceSettings mutateInstance(OpenAiChatCompletionServiceSettings instance) throws IOException {
        var modelId = instance.modelId();
        var uri = instance.uri();
        var organizationId = instance.organizationId();
        var maxInputTokens = instance.maxInputTokens();
        var rateLimitSettings = instance.rateLimitSettings();
        var oAuth2Settings = instance.oAuth2Settings();
        switch (randomIntBetween(0, 5)) {
            case 0 -> modelId = randomValueOtherThan(modelId, () -> randomAlphaOfLength(8));
            case 1 -> uri = randomValueOtherThan(uri, () -> createUri("https://" + randomAlphaOfLength(10) + ".example"));
            case 2 -> organizationId = randomValueOtherThan(organizationId, () -> randomAlphaOfLengthOrNull(15));
            case 3 -> maxInputTokens = randomValueOtherThan(maxInputTokens, () -> randomFrom(randomIntBetween(128, 4096), null));
            case 4 -> rateLimitSettings = randomValueOtherThan(rateLimitSettings, RateLimitSettingsTests::createRandom);
            case 5 -> oAuth2Settings = randomValueOtherThan(
                oAuth2Settings,
                () -> randomFrom(OpenAiOAuth2SettingsTests.createRandom(), null)
            );
            default -> throw new AssertionError("Illegal randomisation branch");
        }

        return new OpenAiChatCompletionServiceSettings(modelId, uri, organizationId, maxInputTokens, rateLimitSettings, oAuth2Settings);
    }

    @Override
    protected OpenAiChatCompletionServiceSettings mutateInstanceForVersion(
        OpenAiChatCompletionServiceSettings instance,
        TransportVersion version
    ) {
        if (version.supports(OPENAI_OAUTH2_SETTINGS)) {
            return instance;
        }

        return new OpenAiChatCompletionServiceSettings(
            instance.modelId(),
            instance.uri(),
            instance.organizationId(),
            instance.maxInputTokens(),
            instance.rateLimitSettings()
        );
    }

    private static Map<String, Object> buildServiceSettingsMap(
        @Nullable String modelId,
        @Nullable String url,
        @Nullable String organizationId,
        @Nullable Integer maxInputTokens,
        @Nullable Integer rateLimit
    ) {
        return buildAllFieldsServiceSettingsMap(modelId, url, organizationId, maxInputTokens, rateLimit, null, null, null);
    }
}
