/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai.completion;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.common.oauth2.OAuth2Settings;
import org.elasticsearch.xpack.inference.common.oauth2.OAuth2SettingsTests;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiServiceFields;
import org.elasticsearch.xpack.inference.services.azureopenai.oauth2.AzureOpenAiOAuth2Settings;
import org.elasticsearch.xpack.inference.services.azureopenai.oauth2.AzureOpenAiOAuth2SettingsTests;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.azureopenai.oauth2.AzureOpenAiOAuth2Settings.AZURE_OPENAI_OAUTH_SETTINGS;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class AzureOpenAiCompletionServiceSettingsTests extends AbstractBWCWireSerializationTestCase<AzureOpenAiCompletionServiceSettings> {

    private static final String RESOURCE_NAME = "resource";
    private static final String DEPLOYMENT_ID = "deployment";
    private static final String API_VERSION = "2024";
    private static final String TENANT_ID = "tenant_id";

    private static final String NEW_TENANT_ID = "new-tenant";
    private static final String NEW_CLIENT_ID = "new-client";
    private static final List<String> NEW_SCOPES = List.of("new-scope1", "new-scope2");

    public static final String ERROR_RESOURCE_NAME_REQUIRED = "[service_settings] does not contain the required setting [resource_name]";
    public static final String ERROR_DEPLOYMENT_ID_REQUIRED = "[service_settings] does not contain the required setting [deployment_id]";
    public static final String ERROR_API_VERSION_REQUIRED = "[service_settings] does not contain the required setting [api_version]";

    public static AzureOpenAiCompletionServiceSettings createRandom() {
        return createRandom(randomFrom(AzureOpenAiOAuth2SettingsTests.createRandom(), null));
    }

    private static AzureOpenAiCompletionServiceSettings createRandom(@Nullable AzureOpenAiOAuth2Settings oAuth2Settings) {
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

    public void testFromMap_Request_CreatesSettingsCorrectly() {
        var serviceSettings = AzureOpenAiCompletionServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
                    AzureOpenAiServiceFields.RESOURCE_NAME,
                    RESOURCE_NAME,
                    AzureOpenAiServiceFields.DEPLOYMENT_ID,
                    DEPLOYMENT_ID,
                    AzureOpenAiServiceFields.API_VERSION,
                    API_VERSION
                )
            ),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(serviceSettings, is(new AzureOpenAiCompletionServiceSettings(RESOURCE_NAME, DEPLOYMENT_ID, API_VERSION, null)));
    }

    public void testFromMap_EmptyMap_ThrowsValidationException() {
        var exception = expectThrows(
            ValidationException.class,
            () -> AzureOpenAiCompletionServiceSettings.fromMap(new HashMap<>(), ConfigurationParseContext.PERSISTENT)
        );
        assertThat(exception.getMessage(), containsString(ERROR_RESOURCE_NAME_REQUIRED));
        assertThat(exception.getMessage(), containsString(ERROR_DEPLOYMENT_ID_REQUIRED));
        assertThat(exception.getMessage(), containsString(ERROR_API_VERSION_REQUIRED));
    }

    public void testFromMap_MissingResourceName_ThrowsValidationException() {
        Map<String, Object> map = new HashMap<>();
        map.put(AzureOpenAiServiceFields.DEPLOYMENT_ID, DEPLOYMENT_ID);
        map.put(AzureOpenAiServiceFields.API_VERSION, API_VERSION);
        var exception = expectThrows(
            ValidationException.class,
            () -> AzureOpenAiCompletionServiceSettings.fromMap(map, ConfigurationParseContext.PERSISTENT)
        );
        assertThat(exception.getMessage(), containsString(ERROR_RESOURCE_NAME_REQUIRED));
    }

    public void testFromMap_MissingDeploymentId_ThrowsValidationException() {
        Map<String, Object> map = new HashMap<>();
        map.put(AzureOpenAiServiceFields.RESOURCE_NAME, RESOURCE_NAME);
        map.put(AzureOpenAiServiceFields.API_VERSION, API_VERSION);
        var exception = expectThrows(
            ValidationException.class,
            () -> AzureOpenAiCompletionServiceSettings.fromMap(map, ConfigurationParseContext.PERSISTENT)
        );
        assertThat(exception.getMessage(), containsString(ERROR_DEPLOYMENT_ID_REQUIRED));
    }

    public void testFromMap_MissingApiVersion_ThrowsValidationException() {
        Map<String, Object> map = new HashMap<>();
        map.put(AzureOpenAiServiceFields.RESOURCE_NAME, RESOURCE_NAME);
        map.put(AzureOpenAiServiceFields.DEPLOYMENT_ID, DEPLOYMENT_ID);
        var exception = expectThrows(
            ValidationException.class,
            () -> AzureOpenAiCompletionServiceSettings.fromMap(map, ConfigurationParseContext.PERSISTENT)
        );
        assertThat(exception.getMessage(), containsString(ERROR_API_VERSION_REQUIRED));
    }

    public void testFromMap_WithRateLimit_CreatesSettingsCorrectly() {
        var requestsPerMinute = 99;
        Map<String, Object> map = new HashMap<>();
        map.put(AzureOpenAiServiceFields.RESOURCE_NAME, RESOURCE_NAME);
        map.put(AzureOpenAiServiceFields.DEPLOYMENT_ID, DEPLOYMENT_ID);
        map.put(AzureOpenAiServiceFields.API_VERSION, API_VERSION);
        map.put(RateLimitSettings.FIELD_NAME, new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, requestsPerMinute)));
        var serviceSettings = AzureOpenAiCompletionServiceSettings.fromMap(map, ConfigurationParseContext.PERSISTENT);

        assertThat(serviceSettings.resourceName(), is(RESOURCE_NAME));
        assertThat(serviceSettings.deploymentId(), is(DEPLOYMENT_ID));
        assertThat(serviceSettings.apiVersion(), is(API_VERSION));
        assertThat(serviceSettings.rateLimitSettings(), is(new RateLimitSettings(requestsPerMinute)));
    }

    public void testFromMap_WithOAuth2Settings_CreatesSettingsCorrectly() {
        Map<String, Object> map = new HashMap<>();
        map.put(AzureOpenAiServiceFields.RESOURCE_NAME, RESOURCE_NAME);
        map.put(AzureOpenAiServiceFields.DEPLOYMENT_ID, DEPLOYMENT_ID);
        map.put(AzureOpenAiServiceFields.API_VERSION, API_VERSION);
        map.put(OAuth2Settings.CLIENT_ID_FIELD, OAuth2SettingsTests.CLIENT_ID);
        map.put(OAuth2Settings.SCOPES_FIELD, OAuth2SettingsTests.SCOPES);
        map.put(AzureOpenAiOAuth2Settings.TENANT_ID_FIELD, TENANT_ID);
        var serviceSettings = AzureOpenAiCompletionServiceSettings.fromMap(map, ConfigurationParseContext.PERSISTENT);

        assertThat(serviceSettings.resourceName(), is(RESOURCE_NAME));
        assertThat(serviceSettings.deploymentId(), is(DEPLOYMENT_ID));
        assertThat(serviceSettings.apiVersion(), is(API_VERSION));
        assertNotNull(serviceSettings.oAuth2Settings());
        assertThat(serviceSettings.oAuth2Settings().getClientId(), is(OAuth2SettingsTests.CLIENT_ID));
        assertThat(serviceSettings.oAuth2Settings().getScopes(), is(OAuth2SettingsTests.SCOPES));
        assertThat(serviceSettings.oAuth2Settings().getTenantId(), is(TENANT_ID));
    }

    public void testFromMap_RequestContext_CreatesSettingsCorrectly() {
        var serviceSettings = AzureOpenAiCompletionServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
                    AzureOpenAiServiceFields.RESOURCE_NAME,
                    RESOURCE_NAME,
                    AzureOpenAiServiceFields.DEPLOYMENT_ID,
                    DEPLOYMENT_ID,
                    AzureOpenAiServiceFields.API_VERSION,
                    API_VERSION
                )
            ),
            ConfigurationParseContext.REQUEST
        );

        assertThat(serviceSettings, is(new AzureOpenAiCompletionServiceSettings(RESOURCE_NAME, DEPLOYMENT_ID, API_VERSION, null)));
    }

    public void testModelId_ReturnsNull() {
        var settings = createRandom();
        assertNull(settings.modelId());
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var entity = new AzureOpenAiCompletionServiceSettings(
            RESOURCE_NAME,
            DEPLOYMENT_ID,
            API_VERSION,
            null,
            new AzureOpenAiOAuth2Settings(new OAuth2Settings(OAuth2SettingsTests.CLIENT_ID, OAuth2SettingsTests.SCOPES), TENANT_ID)
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
                    "requests_per_minute":120
                },
                "client_id":"client_id",
                "scopes":[
                    "scope1",
                    "scope2"
                ],
                "tenant_id":"tenant_id"
            }
            """, RESOURCE_NAME, DEPLOYMENT_ID, API_VERSION))));
    }

    public void testToXContent_WritesValues_WithoutOAuth2() throws IOException {
        var entity = new AzureOpenAiCompletionServiceSettings(RESOURCE_NAME, DEPLOYMENT_ID, API_VERSION, null);

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
            """, RESOURCE_NAME, DEPLOYMENT_ID, API_VERSION))));
    }

    public void testUpdateServiceSettings_WhenOAuth2SettingsNull_ReturnsSameInstance() {
        var settings = new AzureOpenAiCompletionServiceSettings(RESOURCE_NAME, DEPLOYMENT_ID, API_VERSION, null);

        var updated = settings.updateServiceSettings(new HashMap<>(Map.of("some_key", "some_value")));

        assertThat(updated, is(settings));
    }

    public void testUpdateServiceSettings_WhenOAuth2SettingsPresent_EmptyMap_ReturnsSettingsWithUnchangedOAuth2() {
        var oAuth2Settings = new AzureOpenAiOAuth2Settings(
            new OAuth2Settings(OAuth2SettingsTests.CLIENT_ID, OAuth2SettingsTests.SCOPES),
            TENANT_ID
        );
        var settings = new AzureOpenAiCompletionServiceSettings(RESOURCE_NAME, DEPLOYMENT_ID, API_VERSION, null, oAuth2Settings);

        var updated = settings.updateServiceSettings(new HashMap<>());

        assertThat(updated.resourceName(), is(settings.resourceName()));
        assertThat(updated.deploymentId(), is(settings.deploymentId()));
        assertThat(updated.apiVersion(), is(settings.apiVersion()));
        assertThat(updated.oAuth2Settings().getClientId(), is(OAuth2SettingsTests.CLIENT_ID));
        assertThat(updated.oAuth2Settings().getScopes(), is(OAuth2SettingsTests.SCOPES));
        assertThat(updated.oAuth2Settings().getTenantId(), is(TENANT_ID));
    }

    public void testUpdateServiceSettings_WhenOAuth2SettingsPresent_UpdateTenantId_ReturnsSettingsWithUpdatedOAuth2() {
        var oAuth2Settings = new AzureOpenAiOAuth2Settings(
            new OAuth2Settings(OAuth2SettingsTests.CLIENT_ID, OAuth2SettingsTests.SCOPES),
            TENANT_ID
        );
        var settings = new AzureOpenAiCompletionServiceSettings(RESOURCE_NAME, DEPLOYMENT_ID, API_VERSION, null, oAuth2Settings);

        var updated = settings.updateServiceSettings(new HashMap<>(Map.of(AzureOpenAiOAuth2Settings.TENANT_ID_FIELD, NEW_TENANT_ID)));

        assertThat(updated.resourceName(), is(settings.resourceName()));
        assertThat(updated.oAuth2Settings().getClientId(), is(OAuth2SettingsTests.CLIENT_ID));
        assertThat(updated.oAuth2Settings().getScopes(), is(OAuth2SettingsTests.SCOPES));
        assertThat(updated.oAuth2Settings().getTenantId(), is(NEW_TENANT_ID));
    }

    public void testUpdateServiceSettings_WhenOAuth2SettingsPresent_UpdateClientIdAndScopes_ReturnsSettingsWithUpdatedOAuth2() {
        var oAuth2Settings = new AzureOpenAiOAuth2Settings(
            new OAuth2Settings(OAuth2SettingsTests.CLIENT_ID, OAuth2SettingsTests.SCOPES),
            TENANT_ID
        );
        var settings = new AzureOpenAiCompletionServiceSettings(RESOURCE_NAME, DEPLOYMENT_ID, API_VERSION, null, oAuth2Settings);

        var updated = settings.updateServiceSettings(
            new HashMap<>(Map.of(OAuth2Settings.CLIENT_ID_FIELD, NEW_CLIENT_ID, OAuth2Settings.SCOPES_FIELD, NEW_SCOPES))
        );

        assertThat(updated.resourceName(), is(settings.resourceName()));
        assertThat(updated.oAuth2Settings().getClientId(), is(NEW_CLIENT_ID));
        assertThat(updated.oAuth2Settings().getScopes(), is(NEW_SCOPES));
        assertThat(updated.oAuth2Settings().getTenantId(), is(TENANT_ID));
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
