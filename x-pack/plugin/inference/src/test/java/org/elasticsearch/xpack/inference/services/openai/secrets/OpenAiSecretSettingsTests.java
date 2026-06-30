/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai.secrets;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.common.oauth2.OAuth2Secrets.CLIENT_SECRET_FIELD;
import static org.elasticsearch.xpack.inference.common.oauth2.OAuth2SecretsTests.TEST_CLIENT_SECRET;
import static org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings.API_KEY;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class OpenAiSecretSettingsTests extends ESTestCase {

    private static final String TEST_API_KEY = "test-api-key";

    public void testFromMap_ApiKey_Only() {
        var result = OpenAiSecretSettings.fromMap(new HashMap<>(Map.of(API_KEY, TEST_API_KEY)));
        assertThat(result, is(new DefaultSecretSettings(new SecureString(TEST_API_KEY.toCharArray()))));
    }

    public void testFromMap_ClientSecret_Only() {
        var result = OpenAiSecretSettings.fromMap(new HashMap<>(Map.of(CLIENT_SECRET_FIELD, TEST_CLIENT_SECRET)));
        assertThat(result, instanceOf(OpenAiOAuth2SecretsSettings.class));
        assertThat(((OpenAiOAuth2SecretsSettings) result).clientSecret().toString(), is(TEST_CLIENT_SECRET));
    }

    public void testFromMap_ReturnsNull_WhenMapIsNull() {
        assertNull(OpenAiSecretSettings.fromMap(null));
    }

    public void testFromMap_MissingApiKeyAndClientSecret_ThrowsError() {
        var thrownException = expectThrows(ValidationException.class, () -> OpenAiSecretSettings.fromMap(new HashMap<>()));

        assertThat(thrownException.getMessage(), containsString(OpenAiSecretSettings.EXACTLY_ONE_SECRETS_FIELD_ERROR));
    }

    public void testFromMap_HasBothApiKeyAndClientSecret_ThrowsError() {
        var mapWithBothFields = getOpenAiSecretSettingsMap(TEST_API_KEY, TEST_CLIENT_SECRET);
        var thrownException = expectThrows(ValidationException.class, () -> OpenAiSecretSettings.fromMap(mapWithBothFields));

        assertThat(thrownException.getMessage(), containsString(OpenAiSecretSettings.EXACTLY_ONE_SECRETS_FIELD_ERROR));
    }

    public void testFromMap_EmptyApiKey_ThrowsError() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> OpenAiSecretSettings.fromMap(new HashMap<>(Map.of(API_KEY, "")))
        );

        assertThat(
            thrownException.getMessage(),
            containsString("[secret_settings] Invalid value empty string. [api_key] must be a non-empty string")
        );
    }

    public void testFromMap_EmptyClientSecret_ThrowsError() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> OpenAiSecretSettings.fromMap(new HashMap<>(Map.of(CLIENT_SECRET_FIELD, "")))
        );

        assertThat(
            thrownException.getMessage(),
            containsString("[secret_settings] Invalid value empty string. [client_secret] must be a non-empty string")
        );
    }

    public void testFromMap_IgnoresUnknownFields() {
        var map = new HashMap<String, Object>();
        map.put(API_KEY, TEST_API_KEY);
        map.put("some_unknown_field", "value");

        var result = OpenAiSecretSettings.fromMap(map);
        assertThat(result, is(new DefaultSecretSettings(new SecureString(TEST_API_KEY.toCharArray()))));
    }

    public void testNewSecretSettings_UpdatesClientSecret() {
        var newClientSecret = "new-client-secret";
        var original = new OpenAiOAuth2SecretsSettings(new SecureString("old".toCharArray()));
        var updated = original.newSecretSettings(new HashMap<>(Map.of(CLIENT_SECRET_FIELD, newClientSecret)));

        assertThat(updated, is(new OpenAiOAuth2SecretsSettings(new SecureString(newClientSecret.toCharArray()))));
    }

    public void testNewSecretSettings_ReturnsSameInstance_WhenNoSecretsProvided() {
        var original = new OpenAiOAuth2SecretsSettings(new SecureString(TEST_CLIENT_SECRET.toCharArray()));
        var result = original.newSecretSettings(new HashMap<>());

        assertSame(original, result);
    }

    public void testNewSecretSettings_ReturnsSameInstance_WhenClientSecretUnchanged() {
        var original = new OpenAiOAuth2SecretsSettings(new SecureString(TEST_CLIENT_SECRET.toCharArray()));
        var result = original.newSecretSettings(new HashMap<>(Map.of(CLIENT_SECRET_FIELD, TEST_CLIENT_SECRET)));

        assertSame(original, result);
    }

    public void testNewSecretSettings_ThrowsError_WhenApiKeyProvided() {
        var original = new OpenAiOAuth2SecretsSettings(new SecureString(TEST_CLIENT_SECRET.toCharArray()));
        var thrownException = expectThrows(
            ValidationException.class,
            () -> original.newSecretSettings(new HashMap<>(Map.of(API_KEY, TEST_API_KEY)))
        );

        assertThat(
            thrownException.getMessage(),
            containsString("[secret_settings] only the field [client_secret] can be updated for this secret, received: [api_key]")
        );
    }

    public static Map<String, Object> getOpenAiSecretSettingsMap(@Nullable String apiKey, @Nullable String clientSecret) {
        var map = new HashMap<String, Object>();
        if (apiKey != null) {
            map.put(API_KEY, apiKey);
        }
        if (clientSecret != null) {
            map.put(CLIENT_SECRET_FIELD, clientSecret);
        }
        return map;
    }
}
