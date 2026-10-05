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
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.common.oauth2.OAuth2Secrets.CLIENT_SECRET_FIELD;
import static org.elasticsearch.xpack.inference.common.oauth2.OAuth2SecretsTests.TEST_CLIENT_SECRET;
import static org.elasticsearch.xpack.inference.services.azureopenai.secrets.AzureOpenAiSecretSettings.API_KEY;
import static org.elasticsearch.xpack.inference.services.azureopenai.secrets.AzureOpenAiSecretSettings.ENTRA_ID;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class AzureOpenAiOAuth2SecretsTests extends AbstractBWCWireSerializationTestCase<AzureOpenAiOAuth2Secrets> {

    public static AzureOpenAiOAuth2Secrets createRandom() {
        var clientSecret = randomSecureStringOfLength(15);
        return new AzureOpenAiOAuth2Secrets(clientSecret);
    }

    public void testNewSecretSettings_EmptyMap_DoesNotChangeSettings() {
        var initialSettings = createRandom();
        assertThat(initialSettings.newSecretSettings(new HashMap<>()), sameInstance(initialSettings));
    }

    public void testNewSecretSettings_ClientSecret() {
        var initialSettings = createRandom();
        var clientSecret = randomSecureStringOfLength(15);
        var expectedSettings = new AzureOpenAiOAuth2Secrets(clientSecret);
        var newSettings = (AzureOpenAiOAuth2Secrets) initialSettings.newSecretSettings(
            new HashMap<>(Map.of(CLIENT_SECRET_FIELD, clientSecret.toString()))
        );

        assertThat(newSettings, is(expectedSettings));
    }

    public void testNewSecretSettings_SameClientSecret_DoesNotChangeSettings() {
        var initialSettings = createRandom();
        assertThat(
            initialSettings.newSecretSettings(new HashMap<>(Map.of(CLIENT_SECRET_FIELD, initialSettings.getClientSecret().toString()))),
            sameInstance(initialSettings)
        );
    }

    public void testNewSecretSettings_RejectsOtherFields() {
        var initialSettings = createRandom();
        assertRejected(initialSettings, Map.of(API_KEY, randomAlphaOfLength(10)), "[api_key]");
        assertRejected(initialSettings, Map.of(ENTRA_ID, randomAlphaOfLength(10)), "[entra_id]");
        assertRejected(
            initialSettings,
            Map.of(CLIENT_SECRET_FIELD, randomAlphaOfLength(10), API_KEY, randomAlphaOfLength(10)),
            "[api_key]"
        );
    }

    public void testNewSecretSettings_EmptyClientSecretValue_ThrowsError() {
        var initialSettings = createRandom();
        var thrownException = expectThrows(
            ValidationException.class,
            () -> initialSettings.newSecretSettings(new HashMap<>(Map.of(CLIENT_SECRET_FIELD, "")))
        );
        assertThat(
            thrownException.getMessage(),
            containsString(
                Strings.format(
                    "[%s] Invalid value empty string. [%s] must be a non-empty string",
                    ModelConfigurations.SERVICE_SETTINGS,
                    CLIENT_SECRET_FIELD
                )
            )
        );
    }

    public void testNewSecretSettings_IgnoresUnknownFields() {
        var initialSettings = createRandom();
        var newSecrets = new HashMap<String, Object>();
        newSecrets.put("some_unknown_field", "value");
        assertThat(initialSettings.newSecretSettings(newSecrets), sameInstance(initialSettings));
    }

    private static void assertRejected(AzureOpenAiOAuth2Secrets initialSettings, Map<String, Object> request, String expectedDisallowed) {
        var thrownException = expectThrows(ValidationException.class, () -> initialSettings.newSecretSettings(new HashMap<>(request)));
        assertThat(
            thrownException.getMessage(),
            containsString(
                Strings.format(
                    "[%s] only the field [%s] can be updated for this secret, received: %s",
                    ModelConfigurations.SERVICE_SETTINGS,
                    CLIENT_SECRET_FIELD,
                    expectedDisallowed
                )
            )
        );
    }

    public void testToXContent_WritesClientSecretWhenSet() throws IOException {
        var testSettings = new AzureOpenAiOAuth2Secrets(new SecureString(TEST_CLIENT_SECRET.toCharArray()));
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        testSettings.toXContent(builder, null);
        var xContentResult = Strings.toString(builder);

        var expectedResult = XContentHelper.stripWhitespace(Strings.format("""
            {
                "%s": "%s"
            }
            """, CLIENT_SECRET_FIELD, TEST_CLIENT_SECRET));
        assertThat(xContentResult, is(expectedResult));
    }

    public void testFromMap_EmptyClientSecretValue_ThrowsError() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> AzureOpenAiSecretSettings.fromMap(new HashMap<>(Map.of(CLIENT_SECRET_FIELD, "")))
        );

        assertThat(
            thrownException.getMessage(),
            containsString(
                Strings.format(
                    "[%s] Invalid value empty string. [%s] must be a non-empty string",
                    ModelConfigurations.SERVICE_SETTINGS,
                    CLIENT_SECRET_FIELD
                )
            )
        );
    }

    @Override
    protected Writeable.Reader<AzureOpenAiOAuth2Secrets> instanceReader() {
        return AzureOpenAiOAuth2Secrets::new;
    }

    @Override
    protected AzureOpenAiOAuth2Secrets createTestInstance() {
        return createRandom();
    }

    @Override
    protected AzureOpenAiOAuth2Secrets mutateInstance(AzureOpenAiOAuth2Secrets instance) throws IOException {
        return new AzureOpenAiOAuth2Secrets(randomValueOtherThan(instance.getClientSecret(), () -> randomSecureStringOfLength(15)));
    }

    @Override
    protected AzureOpenAiOAuth2Secrets mutateInstanceForVersion(AzureOpenAiOAuth2Secrets instance, TransportVersion version) {
        return instance;
    }
}
