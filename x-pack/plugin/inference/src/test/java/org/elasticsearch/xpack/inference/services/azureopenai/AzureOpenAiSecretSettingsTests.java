/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.services.azureopenai.oauth2.AzureOpenAiOAuth2Secrets;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiSecretSettings.API_KEY;
import static org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiSecretSettings.ENTRA_ID;
import static org.elasticsearch.xpack.inference.services.azureopenai.oauth2.AzureOpenAiOAuth2Secrets.CLIENT_SECRET_FIELD;
import static org.elasticsearch.xpack.inference.services.azureopenai.oauth2.AzureOpenAiOAuth2Settings.AZURE_OPENAI_OAUTH_SETTINGS;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class AzureOpenAiSecretSettingsTests extends AbstractBWCWireSerializationTestCase<AzureOpenAiEntraIdApiKeySecrets> {

    private static final String TEST_INFERENCE_ID = "test-inference-id";

    public static AzureOpenAiEntraIdApiKeySecrets createRandomEntraIdApiKeySecrets() {
        if (randomBoolean()) {
            return new AzureOpenAiEntraIdApiKeySecrets(TEST_INFERENCE_ID, randomSecureStringOfLength(15), null);
        }
        return new AzureOpenAiEntraIdApiKeySecrets(TEST_INFERENCE_ID, null, randomSecureStringOfLength(15));
    }

    public void testNewSecretSettingsApiKey() {
        var initialSettings = createRandomEntraIdApiKeySecrets();
        var apiKey = randomSecureStringOfLength(15);
        var newSettings = new AzureOpenAiEntraIdApiKeySecrets(TEST_INFERENCE_ID, apiKey, null);
        var finalSettings = (AzureOpenAiEntraIdApiKeySecrets) initialSettings.newSecretSettings(Map.of(API_KEY, apiKey.toString()));

        assertEquals(newSettings, finalSettings);
    }

    public void testNewSecretSettingsEntraId() {
        var initialSettings = createRandomEntraIdApiKeySecrets();
        var entraId = randomSecureStringOfLength(15);
        var newSettings = new AzureOpenAiEntraIdApiKeySecrets(TEST_INFERENCE_ID, null, entraId);
        var finalSettings = (AzureOpenAiEntraIdApiKeySecrets) initialSettings.newSecretSettings(Map.of(ENTRA_ID, entraId.toString()));

        assertEquals(newSettings, finalSettings);
    }

    public void testFromMap_ApiKey_Only() {
        var result = AzureOpenAiSecretSettings.fromMap(new HashMap<>(Map.of(API_KEY, "abc")), TEST_INFERENCE_ID);
        assertThat(result, instanceOf(AzureOpenAiEntraIdApiKeySecrets.class));
        assertThat(result, is(new AzureOpenAiEntraIdApiKeySecrets(TEST_INFERENCE_ID, new SecureString("abc".toCharArray()), null)));
    }

    public void testFromMap_EntraId_Only() {
        var result = AzureOpenAiSecretSettings.fromMap(new HashMap<>(Map.of(ENTRA_ID, "xyz")), TEST_INFERENCE_ID);
        assertThat(result, instanceOf(AzureOpenAiEntraIdApiKeySecrets.class));
        assertThat(result, is(new AzureOpenAiEntraIdApiKeySecrets(TEST_INFERENCE_ID, null, new SecureString("xyz".toCharArray()))));
    }

    public void testFromMap_ClientSecret_Only() {
        var result = AzureOpenAiSecretSettings.fromMap(new HashMap<>(Map.of(CLIENT_SECRET_FIELD, "clientsecret")), TEST_INFERENCE_ID);
        assertThat(result, instanceOf(AzureOpenAiOAuth2Secrets.class));
        assertThat(((AzureOpenAiOAuth2Secrets) result).getClientSecret().toString(), is("clientsecret"));
    }

    public void testFromMap_ReturnsNull_WhenMapIsNull() {
        assertNull(AzureOpenAiSecretSettings.fromMap(null, TEST_INFERENCE_ID));
    }

    public void testFromMap_MissingApiKeyAndEntraId_ThrowsError() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> AzureOpenAiSecretSettings.fromMap(new HashMap<>(), TEST_INFERENCE_ID)
        );

        assertThat(thrownException.getMessage(), containsString("must have exactly one of"));
        assertThat(thrownException.getMessage(), containsString(API_KEY));
        assertThat(thrownException.getMessage(), containsString(ENTRA_ID));
        assertThat(thrownException.getMessage(), containsString(CLIENT_SECRET_FIELD));
    }

    public void testFromMap_HasBothApiKeyAndEntraId_ThrowsError() {
        var mapValues = getAzureOpenAiSecretSettingsMap("apikey", "entraid", null);
        var thrownException = expectThrows(
            ValidationException.class,
            () -> AzureOpenAiSecretSettings.fromMap(mapValues, TEST_INFERENCE_ID)
        );

        assertThat(thrownException.getMessage(), containsString("must have exactly one of"));
        assertThat(thrownException.getMessage(), containsString("received:"));
    }

    public void testFromMap_TwoOrMoreSecrets_ThrowsError() {
        var mapWithApiKeyAndClientSecret = getAzureOpenAiSecretSettingsMap("apikey", null, "clientsecret");
        var thrownException = expectThrows(
            ValidationException.class,
            () -> AzureOpenAiSecretSettings.fromMap(mapWithApiKeyAndClientSecret, TEST_INFERENCE_ID)
        );

        assertThat(thrownException.getMessage(), containsString("must have exactly one of"));
        assertThat(thrownException.getMessage(), containsString("received:"));
    }

    public void testFromMap_EmptyApiKey_ThrowsError() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> AzureOpenAiSecretSettings.fromMap(new HashMap<>(Map.of(API_KEY, "")), TEST_INFERENCE_ID)
        );

        assertThat(
            thrownException.getMessage(),
            containsString(Strings.format("[secret_settings] Invalid value empty string. [%s] must be a non-empty string", API_KEY))
        );
    }

    public void testFromMap_EmptyEntraId_ThrowsError() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> AzureOpenAiSecretSettings.fromMap(new HashMap<>(Map.of(ENTRA_ID, "")), TEST_INFERENCE_ID)
        );

        assertThat(
            thrownException.getMessage(),
            containsString(Strings.format("[secret_settings] Invalid value empty string. [%s] must be a non-empty string", ENTRA_ID))
        );
    }

    public void testToXContext_WritesApiKeyOnlyWhenApiKeySet() throws IOException {
        var testSettings = new AzureOpenAiEntraIdApiKeySecrets(TEST_INFERENCE_ID, new SecureString("apikey".toCharArray()), null);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        testSettings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        var expectedResult = Strings.format("{\"%s\":\"apikey\"}", API_KEY);
        assertThat(xContentResult, is(expectedResult));
    }

    public void testToXContext_WritesEntraIdOnlyWhenEntraIdSet() throws IOException {
        var testSettings = new AzureOpenAiEntraIdApiKeySecrets(TEST_INFERENCE_ID, null, new SecureString("entraid".toCharArray()));
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        testSettings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        var expectedResult = Strings.format("{\"%s\":\"entraid\"}", ENTRA_ID);
        assertThat(xContentResult, is(expectedResult));
    }

    public void testToXContent_WritesClientSecretOnlyWhenSet() throws IOException {
        var result = AzureOpenAiSecretSettings.fromMap(new HashMap<>(Map.of(CLIENT_SECRET_FIELD, "clientsecret")), TEST_INFERENCE_ID);
        assertThat(result, instanceOf(AzureOpenAiOAuth2Secrets.class));
        var testSettings = (AzureOpenAiOAuth2Secrets) result;
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        testSettings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        var expectedResult = Strings.format("{\"%s\":\"clientsecret\"}", CLIENT_SECRET_FIELD);
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
        if (instance.apiKey() != null) {
            return new AzureOpenAiEntraIdApiKeySecrets(
                TEST_INFERENCE_ID,
                randomValueOtherThan(instance.apiKey(), () -> randomSecureStringOfLength(15)),
                null
            );
        }
        return new AzureOpenAiEntraIdApiKeySecrets(
            TEST_INFERENCE_ID,
            null,
            randomValueOtherThan(instance.entraId(), () -> randomSecureStringOfLength(15))
        );
    }

    @Override
    protected AzureOpenAiEntraIdApiKeySecrets mutateInstanceForVersion(AzureOpenAiEntraIdApiKeySecrets instance, TransportVersion version) {
        if (version.supports(AZURE_OPENAI_OAUTH_SETTINGS)) {
            return instance;
        }

        return new AzureOpenAiEntraIdApiKeySecrets(null, instance.apiKey(), instance.entraId());
    }

    public static Map<String, Object> getAzureOpenAiSecretSettingsMap(@Nullable String apiKey, @Nullable String entraId) {
        return getAzureOpenAiSecretSettingsMap(apiKey, entraId, null);
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
