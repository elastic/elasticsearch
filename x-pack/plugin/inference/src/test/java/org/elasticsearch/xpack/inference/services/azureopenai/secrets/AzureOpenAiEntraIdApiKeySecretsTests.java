/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai.secrets;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.common.oauth2.OAuth2Secrets.CLIENT_SECRET_FIELD;
import static org.elasticsearch.xpack.inference.services.azureopenai.secrets.AzureOpenAiSecretSettings.API_KEY;
import static org.elasticsearch.xpack.inference.services.azureopenai.secrets.AzureOpenAiSecretSettings.ENTRA_ID;
import static org.elasticsearch.xpack.inference.services.azureopenai.secrets.AzureOpenAiSecretSettingsTests.TEST_API_KEY;
import static org.elasticsearch.xpack.inference.services.azureopenai.secrets.AzureOpenAiSecretSettingsTests.TEST_ENTRA_ID;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class AzureOpenAiEntraIdApiKeySecretsTests extends AbstractBWCWireSerializationTestCase<AzureOpenAiEntraIdApiKeySecrets> {

    public static AzureOpenAiEntraIdApiKeySecrets createRandomEntraIdApiKeySecrets() {
        var useApiKey = randomBoolean();
        var secret = randomSecureStringOfLength(15);
        return useApiKey ? new AzureOpenAiEntraIdApiKeySecrets(secret, null) : new AzureOpenAiEntraIdApiKeySecrets(null, secret);
    }

    /**
     * Creates a {@link AzureOpenAiEntraIdApiKeySecrets} instance with either the API key or Entra ID set,
     * based on which parameter is non-null. API key takes precedence if both are provided,
     * but typically only one should be provided as they are mutually exclusive.
     */
    public static AzureOpenAiEntraIdApiKeySecrets createSecret(@Nullable String apiKey, @Nullable String entraId) {
        if (apiKey != null) {
            return new AzureOpenAiEntraIdApiKeySecrets(new SecureString(apiKey.toCharArray()), null);
        } else if (entraId != null) {
            return new AzureOpenAiEntraIdApiKeySecrets(null, new SecureString(entraId.toCharArray()));
        } else {
            throw new IllegalArgumentException("Either apiKey or entraId must be provided");
        }
    }

    public void testOnlyOneSecretFieldSet() {
        expectThrows(NullPointerException.class, () -> new AzureOpenAiEntraIdApiKeySecrets(null, null));

        var exception = expectThrows(
            IllegalArgumentException.class,
            () -> new AzureOpenAiEntraIdApiKeySecrets(new SecureString("apiKey".toCharArray()), new SecureString("entraId".toCharArray()))
        );
        assertThat(exception.getMessage(), is("Only one of apiKey or entraId can be set, but both were provided"));
    }

    public void testNewSecretSettings_EmptyMap_DoesNotChangeSettings() {
        var initialSettings = createRandomEntraIdApiKeySecrets();
        assertThat(initialSettings.newSecretSettings(new HashMap<>()), sameInstance(initialSettings));
    }

    public void testNewSecretSettings_ApiKeyMode_UpdatesApiKey() {
        var initialSettings = new AzureOpenAiEntraIdApiKeySecrets(randomSecureStringOfLength(15), null);
        var apiKey = randomValueOtherThan(initialSettings.apiKey(), () -> randomSecureStringOfLength(15));
        var newSettings = (AzureOpenAiEntraIdApiKeySecrets) initialSettings.newSecretSettings(
            new HashMap<>(Map.of(API_KEY, apiKey.toString()))
        );

        assertThat(newSettings, is(new AzureOpenAiEntraIdApiKeySecrets(apiKey, null)));
    }

    public void testNewSecretSettings_ApiKeyMode_SameApiKey_DoesNotChangeSettings() {
        var initialSettings = new AzureOpenAiEntraIdApiKeySecrets(randomSecureStringOfLength(15), null);
        assertThat(
            initialSettings.newSecretSettings(new HashMap<>(Map.of(API_KEY, initialSettings.apiKey().toString()))),
            sameInstance(initialSettings)
        );
    }

    public void testNewSecretSettings_EntraIdMode_UpdatesEntraId() {
        var initialSettings = new AzureOpenAiEntraIdApiKeySecrets(null, randomSecureStringOfLength(15));
        var entraId = randomValueOtherThan(initialSettings.entraId(), () -> randomSecureStringOfLength(15));
        var newSettings = (AzureOpenAiEntraIdApiKeySecrets) initialSettings.newSecretSettings(
            new HashMap<>(Map.of(ENTRA_ID, entraId.toString()))
        );

        assertThat(newSettings, is(new AzureOpenAiEntraIdApiKeySecrets(null, entraId)));
    }

    public void testNewSecretSettings_EntraIdMode_SameEntraId_DoesNotChangeSettings() {
        var initialSettings = new AzureOpenAiEntraIdApiKeySecrets(null, randomSecureStringOfLength(15));
        assertThat(
            initialSettings.newSecretSettings(new HashMap<>(Map.of(ENTRA_ID, initialSettings.entraId().toString()))),
            sameInstance(initialSettings)
        );
    }

    public void testNewSecretSettings_ApiKeyMode_RejectsOtherFields() {
        var initialSettings = new AzureOpenAiEntraIdApiKeySecrets(randomSecureStringOfLength(15), null);
        assertRejected(initialSettings, API_KEY, Map.of(ENTRA_ID, randomAlphaOfLength(10)), "[entra_id]");
        assertRejected(initialSettings, API_KEY, Map.of(CLIENT_SECRET_FIELD, randomAlphaOfLength(10)), "[client_secret]");
        assertRejected(initialSettings, API_KEY, Map.of(API_KEY, randomAlphaOfLength(10), ENTRA_ID, randomAlphaOfLength(10)), "[entra_id]");
    }

    public void testNewSecretSettings_EntraIdMode_RejectsOtherFields() {
        var initialSettings = new AzureOpenAiEntraIdApiKeySecrets(null, randomSecureStringOfLength(15));
        assertRejected(initialSettings, ENTRA_ID, Map.of(API_KEY, randomAlphaOfLength(10)), "[api_key]");
        assertRejected(initialSettings, ENTRA_ID, Map.of(CLIENT_SECRET_FIELD, randomAlphaOfLength(10)), "[client_secret]");
        assertRejected(initialSettings, ENTRA_ID, Map.of(API_KEY, randomAlphaOfLength(10), ENTRA_ID, randomAlphaOfLength(10)), "[api_key]");
    }

    public void testNewSecretSettings_EmptyApiKeyValue_ThrowsError() {
        var initialSettings = new AzureOpenAiEntraIdApiKeySecrets(randomSecureStringOfLength(15), null);
        var thrownException = expectThrows(
            ValidationException.class,
            () -> initialSettings.newSecretSettings(new HashMap<>(Map.of(API_KEY, "")))
        );
        assertThat(
            thrownException.getMessage(),
            containsString(
                Strings.format(
                    "[%s] Invalid value empty string. [%s] must be a non-empty string",
                    ModelConfigurations.SERVICE_SETTINGS,
                    API_KEY
                )
            )
        );
    }

    public void testNewSecretSettings_EmptyEntraIdValue_ThrowsError() {
        var initialSettings = new AzureOpenAiEntraIdApiKeySecrets(null, randomSecureStringOfLength(15));
        var thrownException = expectThrows(
            ValidationException.class,
            () -> initialSettings.newSecretSettings(new HashMap<>(Map.of(ENTRA_ID, "")))
        );
        assertThat(
            thrownException.getMessage(),
            containsString(
                Strings.format(
                    "[%s] Invalid value empty string. [%s] must be a non-empty string",
                    ModelConfigurations.SERVICE_SETTINGS,
                    ENTRA_ID
                )
            )
        );
    }

    public void testNewSecretSettings_IgnoresUnknownFields() {
        var initialSettings = new AzureOpenAiEntraIdApiKeySecrets(randomSecureStringOfLength(15), null);
        var newSecrets = new HashMap<String, Object>();
        newSecrets.put("some_unknown_field", "value");
        assertThat(initialSettings.newSecretSettings(newSecrets), sameInstance(initialSettings));
    }

    private static void assertRejected(
        AzureOpenAiEntraIdApiKeySecrets initialSettings,
        String allowedField,
        Map<String, Object> request,
        String expectedDisallowed
    ) {
        var thrownException = expectThrows(ValidationException.class, () -> initialSettings.newSecretSettings(new HashMap<>(request)));
        assertThat(
            thrownException.getMessage(),
            containsString(
                Strings.format(
                    "[%s] only the field [%s] can be updated for this secret, received: %s",
                    ModelConfigurations.SERVICE_SETTINGS,
                    allowedField,
                    expectedDisallowed
                )
            )
        );
    }

    public void testToXContext_WritesApiKeyOnlyWhenApiKeySet() throws IOException {
        var testSettings = new AzureOpenAiEntraIdApiKeySecrets(new SecureString(TEST_API_KEY.toCharArray()), null);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        testSettings.toXContent(builder, null);
        var xContentResult = Strings.toString(builder);

        var expectedResult = XContentHelper.stripWhitespace(Strings.format("""
            {
                "%s":"%s"
            }""", API_KEY, TEST_API_KEY));
        assertThat(xContentResult, is(expectedResult));
    }

    public void testToXContext_WritesEntraIdOnlyWhenEntraIdSet() throws IOException {
        var testSettings = new AzureOpenAiEntraIdApiKeySecrets(null, new SecureString(TEST_ENTRA_ID.toCharArray()));
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        testSettings.toXContent(builder, null);
        var xContentResult = Strings.toString(builder);

        var expectedResult = XContentHelper.stripWhitespace(Strings.format("""
            {
                "%s":"%s"
            }""", ENTRA_ID, TEST_ENTRA_ID));
        assertThat(xContentResult, is(expectedResult));
    }

    @Override
    protected Writeable.Reader<AzureOpenAiEntraIdApiKeySecrets> instanceReader() {
        return AzureOpenAiEntraIdApiKeySecrets::new;
    }

    @Override
    protected AzureOpenAiEntraIdApiKeySecrets createTestInstance() {
        return createRandomEntraIdApiKeySecrets();
    }

    @Override
    protected AzureOpenAiEntraIdApiKeySecrets mutateInstance(AzureOpenAiEntraIdApiKeySecrets instance) throws IOException {
        var apiKey = instance.apiKey();
        var entraId = instance.entraId();

        switch (randomInt(1)) {
            case 0 -> {
                if (apiKey == null) {
                    return new AzureOpenAiEntraIdApiKeySecrets(randomSecureStringOfLength(15), null);
                }

                if (randomBoolean()) {
                    return new AzureOpenAiEntraIdApiKeySecrets(
                        randomValueOtherThan(instance.apiKey(), () -> randomSecureStringOfLength(15)),
                        null
                    );
                } else {
                    return new AzureOpenAiEntraIdApiKeySecrets(null, randomSecureStringOfLength(15));
                }
            }
            case 1 -> {
                if (entraId == null) {
                    return new AzureOpenAiEntraIdApiKeySecrets(null, randomSecureStringOfLength(15));
                }

                if (randomBoolean()) {
                    return new AzureOpenAiEntraIdApiKeySecrets(
                        null,
                        randomValueOtherThan(instance.entraId(), () -> randomSecureStringOfLength(15))
                    );
                } else {
                    return new AzureOpenAiEntraIdApiKeySecrets(randomSecureStringOfLength(15), null);
                }
            }
            default -> throw new AssertionError("Illegal randomization branch");
        }
    }

    @Override
    protected AzureOpenAiEntraIdApiKeySecrets mutateInstanceForVersion(AzureOpenAiEntraIdApiKeySecrets instance, TransportVersion version) {
        return instance;
    }
}
