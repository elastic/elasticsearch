/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.common.oauth2.OAuth2Settings;
import org.elasticsearch.xpack.inference.common.oauth2.OAuth2SettingsTests;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public abstract class AzureOpenAiServiceSettingsTests<T extends AzureOpenAiServiceSettings> extends AbstractBWCWireSerializationTestCase<
    T> {

    protected static final String TEST_RESOURCE_NAME = "some_resource";
    protected static final String INITIAL_TEST_RESOURCE_NAME = "initial_resource";

    protected static final String TEST_DEPLOYMENT_ID = "some_deployment";
    protected static final String INITIAL_TEST_DEPLOYMENT_ID = "initial_some_deployment";

    protected static final String TEST_API_VERSION = "2024";
    protected static final String INITIAL_TEST_API_VERSION = "2023";

    protected static final int TEST_RATE_LIMIT = 200;
    protected static final int INITIAL_TEST_RATE_LIMIT = 100;

    private static final String ERROR_MISSING_FIELD_PATTERN = "[service_settings] does not contain the required setting [%s]";

    protected abstract T fromMap(Map<String, Object> serviceSettingsMap, ConfigurationParseContext context);

    protected abstract T updateServiceSettings(T serviceSettings, Map<String, Object> serviceSettingsMap);

    protected abstract Map<String, Object> buildAllFieldsServiceSettingsMap(ConfigurationParseContext context);

    protected abstract Map<String, Object> buildRequiredFieldsServiceSettingsMap(ConfigurationParseContext context);

    protected abstract T createServiceSettings(@Nullable AzureOpenAiOAuth2Settings oAuth2Settings);

    /**
     * Expected {@link RateLimitSettings} when {@link RateLimitSettings#FIELD_NAME} is omitted from the service settings map
     * (required-fields-only {@link #fromMap}).
     */
    protected abstract RateLimitSettings getDefaultRateLimitSettings();

    protected static AzureOpenAiOAuth2Settings createInitialOAuth2Settings() {
        return new AzureOpenAiOAuth2Settings(
            new OAuth2Settings(OAuth2SettingsTests.INITIAL_TEST_CLIENT_ID, OAuth2SettingsTests.INITIAL_TEST_SCOPES),
            AzureOpenAiOAuth2SettingsTests.INITIAL_TEST_TENANT_ID
        );
    }

    protected void assertFieldsAfterUpdate(T updatedSettings) {
        assertThat(updatedSettings.resourceName(), is(INITIAL_TEST_RESOURCE_NAME));
        assertThat(updatedSettings.deploymentId(), is(INITIAL_TEST_DEPLOYMENT_ID));
        assertThat(updatedSettings.apiVersion(), is(INITIAL_TEST_API_VERSION));

        assertThat(updatedSettings.rateLimitSettings(), is(new RateLimitSettings(TEST_RATE_LIMIT)));

        assertThat(updatedSettings.oAuth2Settings().clientId(), is(OAuth2SettingsTests.TEST_CLIENT_ID));
        assertThat(updatedSettings.oAuth2Settings().scopes(), is(OAuth2SettingsTests.TEST_SCOPES));
        assertThat(updatedSettings.oAuth2Settings().tenantId(), is(AzureOpenAiOAuth2SettingsTests.TEST_TENANT_ID));
    }

    public void testFromMap_Request_AllFields_CreatesSettings() {
        assertFromMap_AllFields(buildAllFieldsServiceSettingsMap(ConfigurationParseContext.REQUEST), ConfigurationParseContext.REQUEST);
    }

    public void testFromMap_Persistent_AllFields_CreatesSettings() {
        assertFromMap_AllFields(
            buildAllFieldsServiceSettingsMap(ConfigurationParseContext.PERSISTENT),
            ConfigurationParseContext.PERSISTENT
        );
    }

    public void testFromMap_Request_RequiredFieldsOnly_CreatesSettings() {
        assertFromMap_RequiredFieldsOnly(
            buildRequiredFieldsServiceSettingsMap(ConfigurationParseContext.REQUEST),
            ConfigurationParseContext.REQUEST
        );
    }

    public void testFromMap_Persistent_RequiredFieldsOnly_CreatesSettings() {
        assertFromMap_RequiredFieldsOnly(
            buildRequiredFieldsServiceSettingsMap(ConfigurationParseContext.PERSISTENT),
            ConfigurationParseContext.PERSISTENT
        );
    }

    private void assertFromMap_AllFields(Map<String, Object> serviceSettingsMap, ConfigurationParseContext context) {
        var serviceSettings = fromMap(serviceSettingsMap, context);
        assertFromMap_AllFields(serviceSettings, context);
    }

    protected void assertFromMap_AllFields(T serviceSettings, ConfigurationParseContext context) {
        assertThat(serviceSettings.resourceName(), is(TEST_RESOURCE_NAME));
        assertThat(serviceSettings.deploymentId(), is(TEST_DEPLOYMENT_ID));
        assertThat(serviceSettings.apiVersion(), is(TEST_API_VERSION));

        assertThat(serviceSettings.rateLimitSettings(), is(new RateLimitSettings(TEST_RATE_LIMIT)));
        assertThat(serviceSettings.oAuth2Settings(), is(notNullValue()));
        assertThat(serviceSettings.oAuth2Settings().clientId(), is(OAuth2SettingsTests.TEST_CLIENT_ID));
        assertThat(serviceSettings.oAuth2Settings().scopes(), is(OAuth2SettingsTests.TEST_SCOPES));
        assertThat(serviceSettings.oAuth2Settings().tenantId(), is(AzureOpenAiOAuth2SettingsTests.TEST_TENANT_ID));
    }

    private void assertFromMap_RequiredFieldsOnly(Map<String, Object> serviceSettingsMap, ConfigurationParseContext context) {
        var serviceSettings = fromMap(serviceSettingsMap, context);
        assertFromMap_RequiredFieldsOnly(serviceSettings, context);
    }

    protected void assertFromMap_RequiredFieldsOnly(T serviceSettings, ConfigurationParseContext context) {
        assertThat(serviceSettings.resourceName(), is(TEST_RESOURCE_NAME));
        assertThat(serviceSettings.deploymentId(), is(TEST_DEPLOYMENT_ID));
        assertThat(serviceSettings.apiVersion(), is(TEST_API_VERSION));

        assertThat(serviceSettings.rateLimitSettings(), is(getDefaultRateLimitSettings()));
        assertThat(serviceSettings.oAuth2Settings(), is(nullValue()));
    }

    public void testFromMap_Request_EmptyMap_ThrowsValidationException() {
        expectFromMap_MissingRequiredFields_ThrowsValidationException(
            new HashMap<>(),
            AzureOpenAiServiceFields.RESOURCE_NAME,
            AzureOpenAiServiceFields.DEPLOYMENT_ID,
            AzureOpenAiServiceFields.API_VERSION
        );
    }

    public void testFromMap_Request_MissingResourceName_ThrowsValidationException() {
        expectFromMap_MissingRequiredFields_ThrowsValidationException(
            buildRequiredFieldsServiceSettingsMap(null, TEST_DEPLOYMENT_ID, TEST_API_VERSION),
            AzureOpenAiServiceFields.RESOURCE_NAME
        );
    }

    public void testFromMap_Request_MissingDeploymentId_ThrowsValidationException() {
        expectFromMap_MissingRequiredFields_ThrowsValidationException(
            buildRequiredFieldsServiceSettingsMap(TEST_RESOURCE_NAME, null, TEST_API_VERSION),
            AzureOpenAiServiceFields.DEPLOYMENT_ID
        );
    }

    public void testFromMap_Request_MissingApiVersion_ThrowsValidationException() {
        expectFromMap_MissingRequiredFields_ThrowsValidationException(
            buildRequiredFieldsServiceSettingsMap(TEST_RESOURCE_NAME, TEST_DEPLOYMENT_ID, null),
            AzureOpenAiServiceFields.API_VERSION
        );
    }

    private void expectFromMap_MissingRequiredFields_ThrowsValidationException(Map<String, Object> map, String... missingFields) {
        var exception = expectThrows(ValidationException.class, () -> fromMap(map, ConfigurationParseContext.REQUEST));
        for (String expectedError : missingFields) {
            assertThat(exception.getMessage(), containsString(Strings.format(ERROR_MISSING_FIELD_PATTERN, expectedError)));
        }
    }

    public void testUpdateServiceSettings_AllFields_OnlyMutableFieldsAreUpdated() {
        var originalSettings = createServiceSettings(createInitialOAuth2Settings());

        var updatedSettings = updateServiceSettings(originalSettings, buildAllFieldsServiceSettingsMap(ConfigurationParseContext.REQUEST));

        assertFieldsAfterUpdate(updatedSettings);
    }

    public void testUpdateServiceSettings_EmptyMap_DoesNotChangeSettings() {
        var originalSettings = createServiceSettings(createInitialOAuth2Settings());

        var updatedSettings = updateServiceSettings(originalSettings, new HashMap<>());

        assertThat(updatedSettings, is(originalSettings));
    }

    public void testUpdateServiceSettings_AttemptToAddOAuth2Fields_ThrowsValidationException() {
        var originalSettings = createServiceSettings(null);

        var exception = expectThrows(
            ValidationException.class,
            () -> updateServiceSettings(originalSettings, buildAllFieldsServiceSettingsMap(ConfigurationParseContext.REQUEST))
        );

        assertThat(
            exception.getMessage(),
            containsString("Cannot update OAuth2 fields as the service was not configured with OAuth2 settings.")
        );
    }

    public void testModelId_ReturnsNull() {
        assertNull(createTestInstance().modelId());
    }

    /**
     * Builds a service-settings map for tests. Fields with a {@code null} value are omitted, simulating a missing entry in the body.
     */
    public static Map<String, Object> buildAllFieldsServiceSettingsMap(
        @Nullable String resourceName,
        @Nullable String deploymentId,
        @Nullable String apiVersion,
        @Nullable Integer rateLimit,
        @Nullable String clientId,
        @Nullable List<String> scopes,
        @Nullable String tenantId
    ) {
        Map<String, Object> map = buildRequiredFieldsServiceSettingsMap(resourceName, deploymentId, apiVersion);
        if (rateLimit != null) {
            map.put(RateLimitSettings.FIELD_NAME, new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, rateLimit)));
        }
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

    public static Map<String, Object> buildRequiredFieldsServiceSettingsMap(
        @Nullable String resourceName,
        @Nullable String deploymentId,
        @Nullable String apiVersion
    ) {
        Map<String, Object> map = new HashMap<>();
        if (resourceName != null) {
            map.put(AzureOpenAiServiceFields.RESOURCE_NAME, resourceName);
        }
        if (deploymentId != null) {
            map.put(AzureOpenAiServiceFields.DEPLOYMENT_ID, deploymentId);
        }
        if (apiVersion != null) {
            map.put(AzureOpenAiServiceFields.API_VERSION, apiVersion);
        }
        return map;
    }
}
