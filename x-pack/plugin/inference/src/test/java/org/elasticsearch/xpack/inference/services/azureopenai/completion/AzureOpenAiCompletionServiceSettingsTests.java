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
import org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiServiceFields;
import org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.common.JsonUtils.toJson;
import static org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiOAuth2Settings.AZURE_OPENAI_OAUTH_SETTINGS;
import static org.hamcrest.Matchers.is;

public class AzureOpenAiCompletionServiceSettingsTests extends AzureOpenAiServiceSettingsTests<AzureOpenAiCompletionServiceSettings> {

    private static final String NEW_TENANT_ID = "new-tenant";
    private static final String NEW_CLIENT_ID = "new-client";
    private static final List<String> NEW_SCOPES = List.of("new-scope1", "new-scope2");

    public static AzureOpenAiCompletionServiceSettings createRandom() {
        return createRandom(randomFrom(AzureOpenAiOAuth2SettingsTests.createRandom(), null));
    }

    public static AzureOpenAiCompletionServiceSettings createRandom(@Nullable AzureOpenAiOAuth2Settings oAuth2Settings) {
        var resourceName = randomAlphaOfLength(8);
        var deploymentId = randomAlphaOfLength(8);
        var apiVersion = randomAlphaOfLength(8);

        return new AzureOpenAiCompletionServiceSettings(
            resourceName,
            deploymentId,
            apiVersion,
            RateLimitSettingsTests.createRandom(),
            oAuth2Settings
        );
    }

    public static AzureOpenAiCompletionServiceSettings createRandomWithoutOAuth2() {
        return createRandom(null);
    }

    @Override
    protected AzureOpenAiCompletionServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        return AzureOpenAiCompletionServiceSettings.fromMap(map, context);
    }

    @Override
    protected Map<String, Object> buildMinimalPersistentMapWithOAuth2() {
        var map = new HashMap<String, Object>();
        map.put(AzureOpenAiServiceFields.RESOURCE_NAME, RESOURCE_NAME_VALUE);
        map.put(AzureOpenAiServiceFields.DEPLOYMENT_ID, DEPLOYMENT_ID_VALUE);
        map.put(AzureOpenAiServiceFields.API_VERSION, API_VERSION_VALUE);
        map.put(OAuth2Settings.CLIENT_ID_FIELD, OAuth2SettingsTests.CLIENT_ID_VALUE);
        map.put(OAuth2Settings.SCOPES_FIELD, OAuth2SettingsTests.SCOPES_VALUE);
        map.put(AzureOpenAiOAuth2Settings.TENANT_ID_FIELD, TENANT_ID_VALUE);
        return map;
    }

    @Override
    protected AzureOpenAiCompletionServiceSettings createSettingsWithOAuth2(@Nullable AzureOpenAiOAuth2Settings oAuth2Settings) {
        return new AzureOpenAiCompletionServiceSettings(RESOURCE_NAME_VALUE, DEPLOYMENT_ID_VALUE, API_VERSION_VALUE, null, oAuth2Settings);
    }

    public void testFromMap_Request_CreatesSettingsCorrectly() {
        var serviceSettings = AzureOpenAiCompletionServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
                    AzureOpenAiServiceFields.RESOURCE_NAME,
                    RESOURCE_NAME_VALUE,
                    AzureOpenAiServiceFields.DEPLOYMENT_ID,
                    DEPLOYMENT_ID_VALUE,
                    AzureOpenAiServiceFields.API_VERSION,
                    API_VERSION_VALUE
                )
            ),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(
            serviceSettings,
            is(new AzureOpenAiCompletionServiceSettings(RESOURCE_NAME_VALUE, DEPLOYMENT_ID_VALUE, API_VERSION_VALUE, null))
        );
    }

    public void testFromMap_RequestContext_CreatesSettingsCorrectly() {
        var serviceSettings = AzureOpenAiCompletionServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
                    AzureOpenAiServiceFields.RESOURCE_NAME,
                    RESOURCE_NAME_VALUE,
                    AzureOpenAiServiceFields.DEPLOYMENT_ID,
                    DEPLOYMENT_ID_VALUE,
                    AzureOpenAiServiceFields.API_VERSION,
                    API_VERSION_VALUE
                )
            ),
            ConfigurationParseContext.REQUEST
        );

        assertThat(
            serviceSettings,
            is(new AzureOpenAiCompletionServiceSettings(RESOURCE_NAME_VALUE, DEPLOYMENT_ID_VALUE, API_VERSION_VALUE, null))
        );
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var entity = new AzureOpenAiCompletionServiceSettings(
            RESOURCE_NAME_VALUE,
            DEPLOYMENT_ID_VALUE,
            API_VERSION_VALUE,
            null,
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
                                    "requests_per_minute":120
                                },
                                "client_id":"%s",
                                "scopes":%s,
                                "tenant_id":"%s"
                            }
                            """,
                        RESOURCE_NAME_VALUE,
                        DEPLOYMENT_ID_VALUE,
                        API_VERSION_VALUE,
                        OAuth2SettingsTests.CLIENT_ID_VALUE,
                        toJson(OAuth2SettingsTests.SCOPES_VALUE, ""),
                        TENANT_ID_VALUE
                    )
                )
            )
        );
    }

    public void testToXContent_WritesValues_WithoutOAuth2() throws IOException {
        var entity = new AzureOpenAiCompletionServiceSettings(RESOURCE_NAME_VALUE, DEPLOYMENT_ID_VALUE, API_VERSION_VALUE, null);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        var xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is(XContentHelper.stripWhitespace(Strings.format("""
            {
                "resource_name":"%s",
                "deployment_id":"%s",
                "api_version":"%s",
                "rate_limit":{
                    "requests_per_minute":120
                }
            }
            """, RESOURCE_NAME_VALUE, DEPLOYMENT_ID_VALUE, API_VERSION_VALUE))));
    }

    public void testUpdateServiceSettings_WhenOAuth2SettingsPresent_EmptyMap_ReturnsSettingsWithUnchangedOAuth2() {
        var oAuth2Settings = new AzureOpenAiOAuth2Settings(
            new OAuth2Settings(OAuth2SettingsTests.CLIENT_ID_VALUE, OAuth2SettingsTests.SCOPES_VALUE),
            TENANT_ID_VALUE
        );
        var settings = new AzureOpenAiCompletionServiceSettings(
            RESOURCE_NAME_VALUE,
            DEPLOYMENT_ID_VALUE,
            API_VERSION_VALUE,
            null,
            oAuth2Settings
        );

        var updated = (AzureOpenAiCompletionServiceSettings) settings.updateServiceSettings(new HashMap<>());

        assertThat(updated, is(settings));
    }

    public void testUpdateServiceSettings_WhenOAuth2SettingsPresent_UpdateTenantId_ReturnsSettingsWithUpdatedOAuth2() {
        var oAuth2Settings = new AzureOpenAiOAuth2Settings(
            new OAuth2Settings(OAuth2SettingsTests.CLIENT_ID_VALUE, OAuth2SettingsTests.SCOPES_VALUE),
            TENANT_ID_VALUE
        );
        var settings = new AzureOpenAiCompletionServiceSettings(
            RESOURCE_NAME_VALUE,
            DEPLOYMENT_ID_VALUE,
            API_VERSION_VALUE,
            null,
            oAuth2Settings
        );

        var updated = (AzureOpenAiCompletionServiceSettings) settings.updateServiceSettings(
            new HashMap<>(Map.of(AzureOpenAiOAuth2Settings.TENANT_ID_FIELD, NEW_TENANT_ID))
        );

        assertThat(updated.resourceName(), is(settings.resourceName()));
        assertThat(updated.oAuth2Settings().clientId(), is(OAuth2SettingsTests.CLIENT_ID_VALUE));
        assertThat(updated.oAuth2Settings().scopes(), is(OAuth2SettingsTests.SCOPES_VALUE));
        assertThat(updated.oAuth2Settings().tenantId(), is(NEW_TENANT_ID));
    }

    public void testUpdateServiceSettings_WhenOAuth2SettingsPresent_UpdateClientIdAndScopes_ReturnsSettingsWithUpdatedOAuth2() {
        var oAuth2Settings = new AzureOpenAiOAuth2Settings(
            new OAuth2Settings(OAuth2SettingsTests.CLIENT_ID_VALUE, OAuth2SettingsTests.SCOPES_VALUE),
            TENANT_ID_VALUE
        );
        var settings = new AzureOpenAiCompletionServiceSettings(
            RESOURCE_NAME_VALUE,
            DEPLOYMENT_ID_VALUE,
            API_VERSION_VALUE,
            null,
            oAuth2Settings
        );

        var updated = (AzureOpenAiCompletionServiceSettings) settings.updateServiceSettings(
            new HashMap<>(Map.of(OAuth2Settings.CLIENT_ID_FIELD, NEW_CLIENT_ID, OAuth2Settings.SCOPES_FIELD, NEW_SCOPES))
        );

        assertThat(updated.resourceName(), is(settings.resourceName()));
        assertThat(updated.oAuth2Settings().clientId(), is(NEW_CLIENT_ID));
        assertThat(updated.oAuth2Settings().scopes(), is(NEW_SCOPES));
        assertThat(updated.oAuth2Settings().tenantId(), is(TENANT_ID_VALUE));
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
