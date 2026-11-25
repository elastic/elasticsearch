/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.common.oauth2.OAuth2Settings;
import org.elasticsearch.xpack.inference.common.oauth2.OAuth2SettingsTests;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.ServiceFields.MODEL_ID;
import static org.elasticsearch.xpack.inference.services.ServiceFields.URL;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createUri;
import static org.elasticsearch.xpack.inference.services.openai.OpenAiOAuth2Settings.OPENAI_OAUTH2_SETTINGS;
import static org.elasticsearch.xpack.inference.services.openai.OpenAiServiceFields.ORGANIZATION;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public abstract class OpenAiServiceSettingsTests<T extends OpenAiServiceSettings> extends AbstractBWCWireSerializationTestCase<T> {

    protected static final String TEST_MODEL_ID = "test-model-id";
    protected static final String INITIAL_TEST_MODEL_ID = "initial-model-id";

    protected static final URI TEST_URI = createUri("https://www.test.com");
    protected static final URI INITIAL_TEST_URI = createUri("https://www.initial.com");

    protected static final String TEST_ORGANIZATION_ID = "test-organization";
    protected static final String INITIAL_TEST_ORGANIZATION_ID = "initial-organization";

    protected static final int TEST_RATE_LIMIT = 250;
    protected static final int INITIAL_TEST_RATE_LIMIT = 100;

    private static final String ERROR_MISSING_FIELD_PATTERN = "[service_settings] does not contain the required setting [%s]";

    protected abstract T fromMap(Map<String, Object> serviceSettingsMap, ConfigurationParseContext context);

    protected abstract T updateServiceSettings(T serviceSettings, Map<String, Object> serviceSettingsMap);

    protected abstract Map<String, Object> buildAllFieldsServiceSettingsMap(ConfigurationParseContext context);

    protected abstract Map<String, Object> buildRequiredFieldsServiceSettingsMap(ConfigurationParseContext context);

    protected abstract T createServiceSettings(@Nullable OpenAiOAuth2Settings oAuth2Settings);

    /**
     * Expected {@link RateLimitSettings} when {@link RateLimitSettings#FIELD_NAME} is omitted from the service settings map
     * (required-fields-only {@link #fromMap}).
     */
    protected abstract RateLimitSettings getDefaultRateLimitSettings();

    protected static OpenAiOAuth2Settings createInitialOAuth2Settings() {
        return new OpenAiOAuth2Settings(
            OAuth2SettingsTests.INITIAL_TEST_CLIENT_ID,
            OAuth2SettingsTests.INITIAL_TEST_SCOPES,
            OpenAiOAuth2SettingsTests.INITIAL_TEST_TOKEN_URL
        );
    }

    @Override
    protected Collection<TransportVersion> bwcVersions() {
        return super.bwcVersions().stream().filter(version -> version.supports(OPENAI_OAUTH2_SETTINGS)).toList();
    }

    protected void assertFieldsAfterUpdate(T updatedSettings) {
        assertThat(updatedSettings.oAuth2Settings().clientId(), is(OAuth2SettingsTests.TEST_CLIENT_ID));
        assertThat(updatedSettings.oAuth2Settings().scopes(), is(OAuth2SettingsTests.TEST_SCOPES));
        assertThat(updatedSettings.oAuth2Settings().tokenUrl(), is(OpenAiOAuth2SettingsTests.TEST_TOKEN_URL));
    }

    public void testFromMap_Request_AllFields_CreatesSettings() {
        assertFromMapAllFields(buildAllFieldsServiceSettingsMap(ConfigurationParseContext.REQUEST), ConfigurationParseContext.REQUEST);
    }

    public void testFromMap_Persistent_AllFields_CreatesSettings() {
        assertFromMapAllFields(
            buildAllFieldsServiceSettingsMap(ConfigurationParseContext.PERSISTENT),
            ConfigurationParseContext.PERSISTENT
        );
    }

    public void testFromMap_Request_RequiredFieldsOnly_CreatesSettings() {
        assertFromMapRequiredFieldsOnly(
            buildRequiredFieldsServiceSettingsMap(ConfigurationParseContext.REQUEST),
            ConfigurationParseContext.REQUEST
        );
    }

    public void testFromMap_Persistent_RequiredFieldsOnly_CreatesSettings() {
        assertFromMapRequiredFieldsOnly(
            buildRequiredFieldsServiceSettingsMap(ConfigurationParseContext.PERSISTENT),
            ConfigurationParseContext.PERSISTENT
        );
    }

    private void assertFromMapAllFields(Map<String, Object> serviceSettingsMap, ConfigurationParseContext context) {
        var serviceSettings = fromMap(serviceSettingsMap, context);
        assertFromMapAllFields(serviceSettings, context);
    }

    protected void assertFromMapAllFields(T serviceSettings, ConfigurationParseContext context) {
        assertThat(serviceSettings.oAuth2Settings(), is(notNullValue()));
        assertThat(serviceSettings.oAuth2Settings().clientId(), is(OAuth2SettingsTests.TEST_CLIENT_ID));
        assertThat(serviceSettings.oAuth2Settings().scopes(), is(OAuth2SettingsTests.TEST_SCOPES));
        assertThat(serviceSettings.oAuth2Settings().tokenUrl(), is(OpenAiOAuth2SettingsTests.TEST_TOKEN_URL));
    }

    private void assertFromMapRequiredFieldsOnly(Map<String, Object> serviceSettingsMap, ConfigurationParseContext context) {
        var serviceSettings = fromMap(serviceSettingsMap, context);
        assertFromMapRequiredFieldsOnly(serviceSettings, context);
    }

    protected void assertFromMapRequiredFieldsOnly(T serviceSettings, ConfigurationParseContext context) {
        assertThat(serviceSettings.oAuth2Settings(), is(nullValue()));
    }

    public void testFromMap_Request_EmptyMap_ThrowsValidationException() {
        expectFromMap_MissingRequiredFields_ThrowsValidationException(new HashMap<>(), MODEL_ID);
    }

    public void testFromMap_Request_MissingModelId_ThrowsValidationException() {
        expectFromMap_MissingRequiredFields_ThrowsValidationException(buildRequiredFieldsServiceSettingsMap((String) null), MODEL_ID);
    }

    private void expectFromMap_MissingRequiredFields_ThrowsValidationException(Map<String, Object> map, String... missingFields) {
        var exception = expectThrows(ValidationException.class, () -> fromMap(map, ConfigurationParseContext.REQUEST));
        for (var expectedError : missingFields) {
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

    public void testOpenAiOAuth2Settings_AreNotBackwardsCompatible() throws IOException {
        testSerializationIsNotBackwardsCompatible(
            OPENAI_OAUTH2_SETTINGS,
            i -> i.oAuth2Settings() != null,
            "Cannot send OAuth2 settings to an older node. Please wait until all nodes are upgraded before using OAuth2."
        );
    }

    /**
     * Builds a service-settings map for tests. Fields with a {@code null} value are omitted, simulating a missing entry in the body.
     */
    public static Map<String, Object> buildAllFieldsServiceSettingsMap(
        @Nullable String modelId,
        @Nullable String url,
        @Nullable String organizationId,
        @Nullable Integer maxInputTokens,
        @Nullable Integer rateLimit,
        @Nullable String clientId,
        @Nullable List<String> scopes,
        @Nullable URI tokenUrl
    ) {
        var map = buildRequiredFieldsServiceSettingsMap(modelId);
        if (url != null) {
            map.put(URL, url);
        }
        if (organizationId != null) {
            map.put(ORGANIZATION, organizationId);
        }
        if (maxInputTokens != null) {
            map.put(org.elasticsearch.xpack.inference.services.ServiceFields.MAX_INPUT_TOKENS, maxInputTokens);
        }
        if (rateLimit != null) {
            map.put(RateLimitSettings.FIELD_NAME, new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, rateLimit)));
        }
        if (clientId != null) {
            map.put(OAuth2Settings.CLIENT_ID_FIELD, clientId);
        }
        if (scopes != null) {
            map.put(OAuth2Settings.SCOPES_FIELD, scopes);
        }
        if (tokenUrl != null) {
            map.put(OpenAiServiceFields.TOKEN_URL, tokenUrl.toString());
        }
        return map;
    }

    public static Map<String, Object> buildRequiredFieldsServiceSettingsMap(@Nullable String modelId) {
        var map = new HashMap<String, Object>();
        if (modelId != null) {
            map.put(MODEL_ID, modelId);
        }
        return map;
    }
}
