/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai.secrets;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.common.oauth2.OAuth2Secrets.CLIENT_SECRET_FIELD;
import static org.elasticsearch.xpack.inference.common.oauth2.OAuth2SecretsTests.TEST_CLIENT_SECRET;
import static org.elasticsearch.xpack.inference.services.azureopenai.secrets.AzureOpenAiSecretSettings.API_KEY;
import static org.elasticsearch.xpack.inference.services.azureopenai.secrets.AzureOpenAiSecretSettings.ENTRA_ID;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class AzureOpenAiSecretSettingsTests extends ESTestCase {
    public static final String TEST_ENTRA_ID = "test-entra-id";
    public static final String TEST_API_KEY = "test-api-key";

    public void testFromMap_ApiKey_Only() {
        var result = AzureOpenAiSecretSettings.fromMap(new HashMap<>(Map.of(API_KEY, TEST_API_KEY)));
        assertThat(result, instanceOf(AzureOpenAiEntraIdApiKeySecrets.class));
        assertThat(result, is(new AzureOpenAiEntraIdApiKeySecrets(new SecureString(TEST_API_KEY.toCharArray()), null)));
    }

    public void testFromMap_EntraId_Only() {
        var result = AzureOpenAiSecretSettings.fromMap(new HashMap<>(Map.of(ENTRA_ID, TEST_ENTRA_ID)));
        assertThat(result, instanceOf(AzureOpenAiEntraIdApiKeySecrets.class));
        assertThat(result, is(new AzureOpenAiEntraIdApiKeySecrets(null, new SecureString(TEST_ENTRA_ID.toCharArray()))));
    }

    public void testFromMap_ClientSecret() {
        var result = AzureOpenAiSecretSettings.fromMap(new HashMap<>(Map.of(CLIENT_SECRET_FIELD, TEST_CLIENT_SECRET)));
        assertThat(result, instanceOf(AzureOpenAiOAuth2Secrets.class));
        assertThat(((AzureOpenAiOAuth2Secrets) result).getClientSecret().toString(), is(TEST_CLIENT_SECRET));
    }

    public void testFromMap_ReturnsNull_WhenMapIsNull() {
        assertNull(AzureOpenAiSecretSettings.fromMap(null));
    }

    public void testFromMap_MissingApiKeyAndEntraId_ThrowsError() {
        var thrownException = expectThrows(ValidationException.class, () -> AzureOpenAiSecretSettings.fromMap(new HashMap<>()));

        assertThat(
            thrownException.getMessage(),
            containsString(Strings.format("must have exactly one of [%s], [%s], or [%s] field set", API_KEY, ENTRA_ID, CLIENT_SECRET_FIELD))
        );
    }

    public void testFromMap_HasBothApiKeyAndEntraId_ThrowsError() {
        var mapValues = getAzureOpenAiSecretSettingsMap(TEST_API_KEY, TEST_ENTRA_ID, null);
        var thrownException = expectThrows(ValidationException.class, () -> AzureOpenAiSecretSettings.fromMap(mapValues));

        assertThat(
            thrownException.getMessage(),
            containsString(
                Strings.format(
                    "must have exactly one of [%1$s], [%2$s], or [%3$s] field set, received: [%1$s, %2$s]",
                    API_KEY,
                    ENTRA_ID,
                    CLIENT_SECRET_FIELD
                )
            )
        );
    }

    public void testFromMap_ApiKeyAndClientSecret_ThrowsError() {
        var mapWithApiKeyAndClientSecret = getAzureOpenAiSecretSettingsMap(TEST_API_KEY, null, "secret");
        var thrownException = expectThrows(
            ValidationException.class,
            () -> AzureOpenAiSecretSettings.fromMap(mapWithApiKeyAndClientSecret)
        );
        assertThat(
            thrownException.getMessage(),
            containsString(
                Strings.format(
                    "must have exactly one of [%1$s], [%2$s], or [%3$s] field set, received: [%1$s, %3$s]",
                    API_KEY,
                    ENTRA_ID,
                    CLIENT_SECRET_FIELD
                )
            )
        );
    }

    public void testFromMap_EntraAndClientSecret_ThrowsError() {
        var mapWithApiKeyAndClientSecret = getAzureOpenAiSecretSettingsMap(null, TEST_ENTRA_ID, "secret");
        var thrownException = expectThrows(
            ValidationException.class,
            () -> AzureOpenAiSecretSettings.fromMap(mapWithApiKeyAndClientSecret)
        );
        assertThat(
            thrownException.getMessage(),
            containsString(
                Strings.format(
                    "must have exactly one of [%1$s], [%2$s], or [%3$s] field set, received: [%2$s, %3$s]",
                    API_KEY,
                    ENTRA_ID,
                    CLIENT_SECRET_FIELD
                )
            )
        );
    }

    public void testFromMap_EmptyApiKey_ThrowsError() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> AzureOpenAiSecretSettings.fromMap(new HashMap<>(Map.of(API_KEY, "")))
        );

        assertThat(
            thrownException.getMessage(),
            containsString(Strings.format("[secret_settings] Invalid value empty string. [%s] must be a non-empty string", API_KEY))
        );
    }

    public void testFromMap_EmptyEntraId_ThrowsError() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> AzureOpenAiSecretSettings.fromMap(new HashMap<>(Map.of(ENTRA_ID, "")))
        );

        assertThat(
            thrownException.getMessage(),
            containsString(Strings.format("[secret_settings] Invalid value empty string. [%s] must be a non-empty string", ENTRA_ID))
        );
    }

    public static Map<String, Object> getAzureOpenAiSecretSettingsMap(@Nullable String apiKey, @Nullable String entraId) {
        return AzureOpenAiSecretSettingsTests.getAzureOpenAiSecretSettingsMap(apiKey, entraId, null);
    }

    public static Map<String, Object> getAzureOpenAiSecretSettingsMap(
        @Nullable String apiKey,
        @Nullable String entraId,
        @Nullable String clientSecret
    ) {
        var map = new HashMap<String, Object>();
        if (apiKey != null) {
            map.put(API_KEY, apiKey);
        }
        if (entraId != null) {
            map.put(ENTRA_ID, entraId);
        }
        if (clientSecret != null) {
            map.put(CLIENT_SECRET_FIELD, clientSecret);
        }
        return map;
    }
}
