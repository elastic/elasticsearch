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

/**
 * Shared tests for Azure OpenAI service settings. Field categories:
 *
 * <p><b>Common</b> (all task types, {@link AzureOpenAiServiceSettings}):
 * <ul>
 *   <li><b>Required, immutable</b> — {@code resource_name}, {@code deployment_id}, {@code api_version}. Parsed from create/update maps;
 *       {@code updateServiceSettings} does not change stored values (see {@link #assertCommonImmutableFieldsUnchangedAfterUpdate}).
 *   <li><b>Optional, mutable</b> — {@code rate_limit}, OAuth2 ({@code client_id}, {@code scopes}, {@code tenant_id}). May be present in a
 *       full update map; same expectations as after parsing a full map ({@code assertCommonOptionalMutableFieldsMatchFullMap}).
 * </ul>
 *
 * <p><b>Task-specific</b> (e.g. embeddings dimensions) — asserted only in subclasses via
 * {@link #assertTaskSpecificAfterFullMap} and {@link #assertTaskSpecificAfterRequiredOnlyMap}.
 *
 * <p><b>Update</b> — two positive cases only: full map (mutable common fields updated, immutable common unchanged) and empty map
 * (instance unchanged). Subclasses verify task-specific behavior for the full-map case.
 */
public abstract class AzureOpenAiServiceSettingsTests<T extends AzureOpenAiServiceSettings> extends AbstractBWCWireSerializationTestCase<
    T> {

    protected static final String TEST_RESOURCE_NAME = "some_resource";
    protected static final String INITIAL_TEST_RESOURCE_NAME = "initial_resource";

    protected static final String TEST_DEPLOYMENT_ID = "some_deployment";
    protected static final String INITIAL_TEST_DEPLOYMENT_ID = "initial_some_deployment";

    protected static final String TEST_API_VERSION = "2024";
    protected static final String INITIAL_TEST_API_VERSION = "2023";

    protected static final int TEST_RATE_LIMIT = 99;
    protected static final int INITIAL_TEST_RATE_LIMIT = 100;

    private static final String ERROR_MISSING_FIELD_PATTERN = "[service_settings] does not contain the required setting [%s]";

    private enum FromMapAssertionLevel {
        FULL,
        REQUIRED_COMMON_ONLY
    }

    protected abstract T fromMap(Map<String, Object> serviceSettingsMap, ConfigurationParseContext context);

    protected abstract T updateServiceSettings(T serviceSettings, Map<String, Object> serviceSettingsMap);

    protected abstract Map<String, Object> buildFullServiceSettingsMap(ConfigurationParseContext context);

    protected abstract Map<String, Object> buildRequiredServiceSettingsMap(ConfigurationParseContext context);

    /**
     * Task-specific fields after parsing a <em>full</em> map, or after {@code updateServiceSettings} with a full map (task fields are not
     * mutable via update; expected state matches the fixture used in subclass {@link #createServiceSettings} / full map tests).
     */
    protected abstract void assertTaskSpecificAfterFullMap(T serviceSettings, ConfigurationParseContext context);

    /**
     * Task-specific fields after parsing a <em>minimal</em> map (required common only, plus any task keys the minimal map includes).
     */
    protected abstract void assertTaskSpecificAfterRequiredOnlyMap(T serviceSettings, ConfigurationParseContext context);

    protected abstract T createServiceSettings(@Nullable AzureOpenAiOAuth2Settings oAuth2Settings);

    /**
     * OAuth2 on instances built by {@link #createServiceSettings} for update tests — initial values of optional mutable common OAuth2.
     */
    protected static AzureOpenAiOAuth2Settings createInitialOAuth2Settings() {
        return new AzureOpenAiOAuth2Settings(
            new OAuth2Settings(OAuth2SettingsTests.INITIAL_TEST_CLIENT_ID, OAuth2SettingsTests.INITIAL_TEST_SCOPES),
            AzureOpenAiOAuth2SettingsTests.INITIAL_TEST_TENANT_ID
        );
    }

    /**
     * Required immutable common fields must be unchanged after {@code updateServiceSettings}. Default: values from {@link #createServiceSettings}
     * for completion ({@code INITIAL_*} constants).
     */
    protected void assertCommonImmutableFieldsUnchangedAfterUpdate(T updatedSettings) {
        assertThat(updatedSettings.resourceName(), is(INITIAL_TEST_RESOURCE_NAME));
        assertThat(updatedSettings.deploymentId(), is(INITIAL_TEST_DEPLOYMENT_ID));
        assertThat(updatedSettings.apiVersion(), is(INITIAL_TEST_API_VERSION));
    }

    public void testFromMap_Request_AllFields_ParsesExpectedSettings() {
        assertFromMapParses(
            buildFullServiceSettingsMap(ConfigurationParseContext.REQUEST),
            ConfigurationParseContext.REQUEST,
            FromMapAssertionLevel.FULL
        );
    }

    public void testFromMap_Persistent_AllFields_ParsesExpectedSettings() {
        assertFromMapParses(
            buildFullServiceSettingsMap(ConfigurationParseContext.PERSISTENT),
            ConfigurationParseContext.PERSISTENT,
            FromMapAssertionLevel.FULL
        );
    }

    public void testFromMap_Request_RequiredFieldsOnly_ParsesExpectedSettings() {
        assertFromMapParses(
            buildRequiredServiceSettingsMap(ConfigurationParseContext.REQUEST),
            ConfigurationParseContext.REQUEST,
            FromMapAssertionLevel.REQUIRED_COMMON_ONLY
        );
    }

    public void testFromMap_Persistent_RequiredFieldsOnly_ParsesExpectedSettings() {
        assertFromMapParses(
            buildRequiredServiceSettingsMap(ConfigurationParseContext.PERSISTENT),
            ConfigurationParseContext.PERSISTENT,
            FromMapAssertionLevel.REQUIRED_COMMON_ONLY
        );
    }

    private void assertFromMapParses(
        Map<String, Object> serviceSettingsMap,
        ConfigurationParseContext context,
        FromMapAssertionLevel level
    ) {
        var serviceSettings = fromMap(serviceSettingsMap, context);
        assertCommonRequiredFieldsFromMap(serviceSettings);
        if (level == FromMapAssertionLevel.FULL) {
            assertCommonOptionalMutableFieldsMatchFullMap(serviceSettings);
            assertTaskSpecificAfterFullMap(serviceSettings, context);
        } else {
            assertTaskSpecificAfterRequiredOnlyMap(serviceSettings, context);
        }
    }

    /** Required common fields present on the parsed object ({@code resource_name}, {@code deployment_id}, {@code api_version}). */
    private void assertCommonRequiredFieldsFromMap(T serviceSettings) {
        assertThat(serviceSettings.resourceName(), is(TEST_RESOURCE_NAME));
        assertThat(serviceSettings.deploymentId(), is(TEST_DEPLOYMENT_ID));
        assertThat(serviceSettings.apiVersion(), is(TEST_API_VERSION));
    }

    /** Optional mutable common fields after parsing a full map ({@code rate_limit}, OAuth2) match {@code TEST_*} fixture values. */
    private void assertCommonOptionalMutableFieldsMatchFullMap(T serviceSettings) {
        assertThat(serviceSettings.rateLimitSettings(), is(new RateLimitSettings(TEST_RATE_LIMIT)));
        assertThat(serviceSettings.oAuth2Settings(), is(notNullValue()));
        assertThat(serviceSettings.oAuth2Settings().clientId(), is(OAuth2SettingsTests.TEST_CLIENT_ID));
        assertThat(serviceSettings.oAuth2Settings().scopes(), is(OAuth2SettingsTests.TEST_SCOPES));
        assertThat(serviceSettings.oAuth2Settings().tenantId(), is(AzureOpenAiOAuth2SettingsTests.TEST_TENANT_ID));
    }

    public void testFromMap_Request_EmptyMap_ThrowsValidationException() {
        expectFromMapMissingRequiredFields(
            new HashMap<>(),
            AzureOpenAiServiceFields.RESOURCE_NAME,
            AzureOpenAiServiceFields.DEPLOYMENT_ID,
            AzureOpenAiServiceFields.API_VERSION
        );
    }

    public void testFromMap_Request_MissingResourceName_ThrowsException() {
        expectFromMapMissingRequiredFields(
            buildRequiredServiceSettingsMap(null, TEST_DEPLOYMENT_ID, TEST_API_VERSION),
            AzureOpenAiServiceFields.RESOURCE_NAME
        );
    }

    public void testFromMap_Request_MissingDeploymentId_ThrowsException() {
        expectFromMapMissingRequiredFields(
            buildRequiredServiceSettingsMap(TEST_RESOURCE_NAME, null, TEST_API_VERSION),
            AzureOpenAiServiceFields.DEPLOYMENT_ID
        );
    }

    public void testFromMap_Request_MissingApiVersion_ThrowsException() {
        expectFromMapMissingRequiredFields(
            buildRequiredServiceSettingsMap(TEST_RESOURCE_NAME, TEST_DEPLOYMENT_ID, null),
            AzureOpenAiServiceFields.API_VERSION
        );
    }

    private void expectFromMapMissingRequiredFields(Map<String, Object> map, String... missingFields) {
        var exception = expectThrows(ValidationException.class, () -> fromMap(map, ConfigurationParseContext.REQUEST));
        for (String expectedError : missingFields) {
            assertThat(exception.getMessage(), containsString(Strings.format(ERROR_MISSING_FIELD_PATTERN, expectedError)));
        }
    }

    public void testUpdateServiceSettings_AllFields_OnlyMutableFieldsAreUpdated() {
        var originalSettings = createServiceSettings(createInitialOAuth2Settings());

        var updatedSettings = updateServiceSettings(originalSettings, buildFullServiceSettingsMap(ConfigurationParseContext.REQUEST));

        assertCommonImmutableFieldsUnchangedAfterUpdate(updatedSettings);
        assertCommonOptionalMutableFieldsMatchFullMap(updatedSettings);
        assertTaskSpecificAfterFullMap(updatedSettings, ConfigurationParseContext.REQUEST);
    }

    public void testUpdateServiceSettings_EmptyMap_DoesNotChangeSettings() {
        var originalSettings = createServiceSettings(createInitialOAuth2Settings());

        var updatedSettings = updateServiceSettings(originalSettings, new HashMap<>());

        assertThat(updatedSettings, is(originalSettings));
    }

    public void testUpdateServiceSettings_WhenOAuth2NotConfigured_ThrowsException() {
        var originalSettings = createServiceSettings(null);

        var exception = expectThrows(
            ValidationException.class,
            () -> updateServiceSettings(originalSettings, buildFullServiceSettingsMap(ConfigurationParseContext.REQUEST))
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
    public static Map<String, Object> buildFullServiceSettingsMap(
        @Nullable String resourceName,
        @Nullable String deploymentId,
        @Nullable String apiVersion,
        @Nullable Integer rateLimit,
        @Nullable String clientId,
        @Nullable List<String> scopes,
        @Nullable String tenantId
    ) {
        Map<String, Object> map = buildRequiredServiceSettingsMap(resourceName, deploymentId, apiVersion);
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

    public static Map<String, Object> buildRequiredServiceSettingsMap(
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
