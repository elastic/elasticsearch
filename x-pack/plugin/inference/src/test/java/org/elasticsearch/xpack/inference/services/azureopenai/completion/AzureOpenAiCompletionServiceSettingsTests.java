/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai.completion;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.common.oauth2.OAuth2Settings;
import org.elasticsearch.xpack.inference.common.oauth2.OAuth2SettingsTests;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiOAuth2Settings;
import org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiOAuth2SettingsTests;
import org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.xpack.inference.common.JsonUtils.toJson;
import static org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiOAuth2Settings.AZURE_OPENAI_OAUTH_SETTINGS;
import static org.hamcrest.Matchers.is;

public class AzureOpenAiCompletionServiceSettingsTests extends AzureOpenAiServiceSettingsTests<AzureOpenAiCompletionServiceSettings> {

    @Override
    protected RateLimitSettings getDefaultRateLimitSettings() {
        return new RateLimitSettings(120);
    }

    @Override
    protected AzureOpenAiCompletionServiceSettings updateServiceSettings(
        AzureOpenAiCompletionServiceSettings serviceSettings,
        Map<String, Object> serviceSettingsMap
    ) {
        return serviceSettings.updateServiceSettings(serviceSettingsMap);
    }

    @Override
    protected AzureOpenAiCompletionServiceSettings fromMap(Map<String, Object> serviceSettingsMap, ConfigurationParseContext context) {
        return AzureOpenAiCompletionServiceSettings.fromMap(serviceSettingsMap, context);
    }

    @Override
    protected Map<String, Object> buildRequiredFieldsServiceSettingsMap(ConfigurationParseContext context) {
        return buildRequiredFieldsServiceSettingsMap(TEST_RESOURCE_NAME, TEST_DEPLOYMENT_ID, TEST_API_VERSION);
    }

    @Override
    protected Map<String, Object> buildAllFieldsServiceSettingsMap(ConfigurationParseContext context) {
        return buildAllFieldsServiceSettingsMap(
            TEST_RESOURCE_NAME,
            TEST_DEPLOYMENT_ID,
            TEST_API_VERSION,
            TEST_RATE_LIMIT,
            OAuth2SettingsTests.TEST_CLIENT_ID,
            OAuth2SettingsTests.TEST_SCOPES,
            AzureOpenAiOAuth2SettingsTests.TEST_TENANT_ID
        );
    }

    @Override
    protected AzureOpenAiCompletionServiceSettings createServiceSettings(@Nullable AzureOpenAiOAuth2Settings oAuth2Settings) {
        return new AzureOpenAiCompletionServiceSettings(
            INITIAL_TEST_RESOURCE_NAME,
            INITIAL_TEST_DEPLOYMENT_ID,
            INITIAL_TEST_API_VERSION,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT),
            oAuth2Settings
        );
    }

    public void testToXContent_AllFields_Written() throws IOException {
        var entity = new AzureOpenAiCompletionServiceSettings(
            TEST_RESOURCE_NAME,
            TEST_DEPLOYMENT_ID,
            TEST_API_VERSION,
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
                                "rate_limit":{
                                    "requests_per_minute": %d
                                },
                                "client_id": "%s",
                                "scopes": %s,
                                "tenant_id": "%s"
                            }
                            """,
                        TEST_RESOURCE_NAME,
                        TEST_DEPLOYMENT_ID,
                        TEST_API_VERSION,
                        TEST_RATE_LIMIT,
                        OAuth2SettingsTests.TEST_CLIENT_ID,
                        toJson(OAuth2SettingsTests.TEST_SCOPES, ""),
                        AzureOpenAiOAuth2SettingsTests.INITIAL_TEST_TENANT_ID
                    )
                )
            )
        );
    }

    public void testToXContent_MandatoryFieldsOnly_DefaultRateLimit_Written() throws IOException {
        var entity = new AzureOpenAiCompletionServiceSettings(TEST_RESOURCE_NAME, TEST_DEPLOYMENT_ID, TEST_API_VERSION, null);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        var xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is(XContentHelper.stripWhitespace(Strings.format("""
            {
                "resource_name": "%s",
                "deployment_id": "%s",
                "api_version": "%s",
                "rate_limit": {
                    "requests_per_minute": %d
                }
            }
            """, TEST_RESOURCE_NAME, TEST_DEPLOYMENT_ID, TEST_API_VERSION, getDefaultRateLimitSettings().requestsPerTimeUnit()))));
    }

    public static AzureOpenAiCompletionServiceSettings createRandom() {
        var oAuth2Settings = randomFrom(AzureOpenAiOAuth2SettingsTests.createRandom(), null);
        return createRandom(oAuth2Settings);
    }

    public static AzureOpenAiCompletionServiceSettings createRandom(@Nullable AzureOpenAiOAuth2Settings oAuth2Settings) {
        var resourceName = randomAlphaOfLength(8);
        var deploymentId = randomAlphaOfLength(8);
        var apiVersion = randomAlphaOfLength(8);
        var rateLimitSettings = RateLimitSettingsTests.createRandom();

        return new AzureOpenAiCompletionServiceSettings(resourceName, deploymentId, apiVersion, rateLimitSettings, oAuth2Settings);
    }

    public static AzureOpenAiCompletionServiceSettings createRandomWithoutOAuth2() {
        return createRandom(null);
    }

    @Override
    protected Writeable.Reader<AzureOpenAiCompletionServiceSettings> instanceReader() {
        return AzureOpenAiCompletionServiceSettings::new;
    }

    @Override
    protected AzureOpenAiCompletionServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected AzureOpenAiCompletionServiceSettings mutateInstance(AzureOpenAiCompletionServiceSettings instance) throws IOException {
        var resourceName = instance.resourceName();
        var deploymentId = instance.deploymentId();
        var apiVersion = instance.apiVersion();
        var rateLimitSettings = instance.rateLimitSettings();
        var oAuth2Settings = instance.oAuth2Settings();
        switch (randomIntBetween(0, 4)) {
            case 0 -> resourceName = randomValueOtherThan(resourceName, () -> randomAlphaOfLength(8));
            case 1 -> deploymentId = randomValueOtherThan(deploymentId, () -> randomAlphaOfLength(8));
            case 2 -> apiVersion = randomValueOtherThan(apiVersion, () -> randomAlphaOfLength(8));
            case 3 -> rateLimitSettings = randomValueOtherThan(rateLimitSettings, RateLimitSettingsTests::createRandom);
            case 4 -> oAuth2Settings = randomValueOtherThan(
                oAuth2Settings,
                () -> randomFrom(AzureOpenAiOAuth2SettingsTests.createRandom(), null)
            );
            default -> throw new AssertionError("Illegal randomisation branch");
        }

        return new AzureOpenAiCompletionServiceSettings(resourceName, deploymentId, apiVersion, rateLimitSettings, oAuth2Settings);
    }

    @Override
    protected AzureOpenAiCompletionServiceSettings mutateInstanceForVersion(
        AzureOpenAiCompletionServiceSettings instance,
        TransportVersion version
    ) {
        if (version.supports(AZURE_OPENAI_OAUTH_SETTINGS)) {
            return instance;
        }

        return new AzureOpenAiCompletionServiceSettings(
            instance.resourceName(),
            instance.deploymentId(),
            instance.apiVersion(),
            instance.rateLimitSettings()
        );
    }
}
