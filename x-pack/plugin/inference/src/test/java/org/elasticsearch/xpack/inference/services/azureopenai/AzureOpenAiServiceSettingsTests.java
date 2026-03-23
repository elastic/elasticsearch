/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.common.oauth2.OAuth2Settings;
import org.elasticsearch.xpack.inference.common.oauth2.OAuth2SettingsTests;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public abstract class AzureOpenAiServiceSettingsTests<T extends AzureOpenAiServiceSettings> extends AbstractBWCWireSerializationTestCase<
    T> {

    protected static final String RESOURCE_NAME_VALUE = "some_resource";
    protected static final String DEPLOYMENT_ID_VALUE = "some_deployment";
    protected static final String API_VERSION_VALUE = "2024";
    protected static final String TENANT_ID_VALUE = "some_tenant_id";
    protected static final int RATE_LIMIT_REQUESTS_PER_MINUTE = 99;

    private static final String ERROR_API_VERSION_REQUIRED = "[service_settings] does not contain the required setting [api_version]";
    private static final Map<String, Object> UPDATE_REQUEST_MAP_WITHOUT_OAUTH2 = Map.of("some_key", "some_value");
    private static final String ERROR_RESOURCE_NAME_REQUIRED = "[service_settings] does not contain the required setting [resource_name]";
    private static final String ERROR_DEPLOYMENT_ID_REQUIRED = "[service_settings] does not contain the required setting [deployment_id]";

    protected abstract T fromMap(Map<String, Object> map, ConfigurationParseContext context);

    protected abstract Map<String, Object> buildMinimalPersistentMapWithOAuth2();

    protected abstract T createSettingsWithOAuth2(@Nullable AzureOpenAiOAuth2Settings oAuth2Settings);

    public void testFromMap_EmptyMap_ThrowsValidationException() {
        var exception = expectThrows(ValidationException.class, () -> fromMap(new HashMap<>(), ConfigurationParseContext.REQUEST));
        assertThat(exception.getMessage(), containsString(ERROR_RESOURCE_NAME_REQUIRED));
        assertThat(exception.getMessage(), containsString(ERROR_DEPLOYMENT_ID_REQUIRED));
        assertThat(exception.getMessage(), containsString(ERROR_API_VERSION_REQUIRED));
    }

    public void testFromMap_MissingResourceName_ThrowsValidationException() {
        Map<String, Object> map = new HashMap<>();
        map.put(AzureOpenAiServiceFields.DEPLOYMENT_ID, DEPLOYMENT_ID_VALUE);
        map.put(AzureOpenAiServiceFields.API_VERSION, API_VERSION_VALUE);
        var exception = expectThrows(ValidationException.class, () -> fromMap(map, ConfigurationParseContext.REQUEST));
        assertThat(exception.getMessage(), containsString(ERROR_RESOURCE_NAME_REQUIRED));
    }

    public void testFromMap_MissingDeploymentId_ThrowsValidationException() {
        Map<String, Object> map = new HashMap<>();
        map.put(AzureOpenAiServiceFields.RESOURCE_NAME, RESOURCE_NAME_VALUE);
        map.put(AzureOpenAiServiceFields.API_VERSION, API_VERSION_VALUE);
        var exception = expectThrows(ValidationException.class, () -> fromMap(map, ConfigurationParseContext.REQUEST));
        assertThat(exception.getMessage(), containsString(ERROR_DEPLOYMENT_ID_REQUIRED));
    }

    public void testFromMap_MissingApiVersion_ThrowsValidationException() {
        Map<String, Object> map = new HashMap<>();
        map.put(AzureOpenAiServiceFields.RESOURCE_NAME, RESOURCE_NAME_VALUE);
        map.put(AzureOpenAiServiceFields.DEPLOYMENT_ID, DEPLOYMENT_ID_VALUE);
        var exception = expectThrows(ValidationException.class, () -> fromMap(map, ConfigurationParseContext.REQUEST));
        assertThat(exception.getMessage(), containsString(ERROR_API_VERSION_REQUIRED));
    }

    public void testModelId_ReturnsNull() {
        assertNull(createTestInstance().modelId());
    }

    public void testFromMap_WithRateLimit_CreatesSettingsCorrectly() {
        Map<String, Object> map = new HashMap<>();
        map.put(AzureOpenAiServiceFields.RESOURCE_NAME, RESOURCE_NAME_VALUE);
        map.put(AzureOpenAiServiceFields.DEPLOYMENT_ID, DEPLOYMENT_ID_VALUE);
        map.put(AzureOpenAiServiceFields.API_VERSION, API_VERSION_VALUE);
        map.put(
            RateLimitSettings.FIELD_NAME,
            new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, RATE_LIMIT_REQUESTS_PER_MINUTE))
        );
        var serviceSettings = fromMap(map, ConfigurationParseContext.REQUEST);

        assertThat(serviceSettings.resourceName(), is(RESOURCE_NAME_VALUE));
        assertThat(serviceSettings.deploymentId(), is(DEPLOYMENT_ID_VALUE));
        assertThat(serviceSettings.apiVersion(), is(API_VERSION_VALUE));
        assertThat(serviceSettings.rateLimitSettings(), is(new RateLimitSettings(RATE_LIMIT_REQUESTS_PER_MINUTE)));
    }

    public void testFromMap_WithOAuth2Settings_CreatesSettingsCorrectly() {
        var map = buildMinimalPersistentMapWithOAuth2();
        var serviceSettings = fromMap(map, ConfigurationParseContext.PERSISTENT);

        assertThat(serviceSettings.resourceName(), is(RESOURCE_NAME_VALUE));
        assertThat(serviceSettings.deploymentId(), is(DEPLOYMENT_ID_VALUE));
        assertThat(serviceSettings.apiVersion(), is(API_VERSION_VALUE));
        assertNotNull(serviceSettings.oAuth2Settings());
        assertThat(serviceSettings.oAuth2Settings().clientId(), is(OAuth2SettingsTests.CLIENT_ID_VALUE));
        assertThat(serviceSettings.oAuth2Settings().scopes(), is(OAuth2SettingsTests.SCOPES_VALUE));
        assertThat(serviceSettings.oAuth2Settings().tenantId(), is(TENANT_ID_VALUE));
    }

    public void testUpdateServiceSettings_WhenOAuth2Null_ThrowsWhenNewSettingsContainsOAuth2Values() {
        var settings = createSettingsWithOAuth2(null);

        var exception = expectThrows(
            ValidationException.class,
            () -> settings.updateServiceSettings(
                new HashMap<>(
                    Map.of(
                        OAuth2Settings.CLIENT_ID_FIELD,
                        OAuth2SettingsTests.CLIENT_ID_VALUE,
                        OAuth2Settings.SCOPES_FIELD,
                        OAuth2SettingsTests.SCOPES_VALUE,
                        AzureOpenAiOAuth2Settings.TENANT_ID_FIELD,
                        TENANT_ID_VALUE
                    )
                )
            )
        );

        assertThat(
            exception.getMessage(),
            containsString("Cannot update OAuth2 fields as the service was not configured with OAuth2 settings.")
        );
    }

    public void testUpdateServiceSettings_WhenOAuth2Null_ReturnsSameInstance() {
        var settings = createSettingsWithOAuth2(null);

        var updated = settings.updateServiceSettings(new HashMap<>(UPDATE_REQUEST_MAP_WITHOUT_OAUTH2));

        assertThat(updated, sameInstance(settings));
    }

    public void testUpdateServiceSettings_WithOAuth2_ReturnsSameInstance() {
        var oAuth2Settings = new AzureOpenAiOAuth2Settings(
            new OAuth2Settings(OAuth2SettingsTests.CLIENT_ID_VALUE, OAuth2SettingsTests.SCOPES_VALUE),
            TENANT_ID_VALUE
        );

        var settings = createSettingsWithOAuth2(oAuth2Settings);

        var updated = settings.updateServiceSettings(new HashMap<>(UPDATE_REQUEST_MAP_WITHOUT_OAUTH2));

        assertThat(updated, sameInstance(settings));
    }

    public void testUpdateServiceSettings_UpdateTenantId_ReplacesTenantId_EvenWithUnmodifiableMap() {
        var oAuth2Settings = new AzureOpenAiOAuth2Settings(
            new OAuth2Settings(OAuth2SettingsTests.CLIENT_ID_VALUE, OAuth2SettingsTests.SCOPES_VALUE),
            TENANT_ID_VALUE
        );
        var newTenantId = "new-tenant";
        var unmodifiableMapSettings = Map.<String, Object>of(AzureOpenAiOAuth2Settings.TENANT_ID_FIELD, newTenantId);

        var settings = createSettingsWithOAuth2(oAuth2Settings);

        var updated = settings.updateServiceSettings(unmodifiableMapSettings);

        assertThat(updated, instanceOf(AzureOpenAiServiceSettings.class));
        var updatedOAuth2Settings = ((AzureOpenAiServiceSettings) updated).oAuth2Settings();
        assertThat(updatedOAuth2Settings.clientId(), is(OAuth2SettingsTests.CLIENT_ID_VALUE));
        assertThat(updatedOAuth2Settings.scopes(), is(OAuth2SettingsTests.SCOPES_VALUE));
        assertThat(updatedOAuth2Settings.tenantId(), is(newTenantId));
    }
}
