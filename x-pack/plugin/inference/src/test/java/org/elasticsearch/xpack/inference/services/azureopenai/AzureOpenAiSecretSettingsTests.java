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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiSecretSettings.API_KEY;
import static org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiSecretSettings.ENTRA_ID;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class AzureOpenAiSecretSettingsTests extends AbstractBWCWireSerializationTestCase<AzureOpenAiSecretSettings> {

    public static AzureOpenAiSecretSettings createRandom() {
        boolean isApiKeyNotEntraId = randomBoolean();
        return new AzureOpenAiSecretSettings(
            isApiKeyNotEntraId ? randomSecureStringOfLength(15) : null,
            isApiKeyNotEntraId == false ? randomSecureStringOfLength(15) : null
        );
    }

    public void testNewSecretSettingsApiKey() {
        AzureOpenAiSecretSettings initialSettings = createRandom();
        AzureOpenAiSecretSettings newSettings = new AzureOpenAiSecretSettings(randomSecureStringOfLength(15), null);
        AzureOpenAiSecretSettings finalSettings = (AzureOpenAiSecretSettings) initialSettings.newSecretSettings(
            Map.of(API_KEY, newSettings.apiKey().toString())
        );

        assertEquals(newSettings, finalSettings);
    }

    public void testNewSecretSettingsEntraId() {
        AzureOpenAiSecretSettings initialSettings = createRandom();
        AzureOpenAiSecretSettings newSettings = new AzureOpenAiSecretSettings(null, randomSecureStringOfLength(15));
        AzureOpenAiSecretSettings finalSettings = (AzureOpenAiSecretSettings) initialSettings.newSecretSettings(
            Map.of(ENTRA_ID, newSettings.entraId().toString())
        );

        assertEquals(newSettings, finalSettings);
    }

    public void testFromMap_ApiKey_Only() {
        var serviceSettings = AzureOpenAiSecretSettings.fromMap(new HashMap<>(Map.of(AzureOpenAiSecretSettings.API_KEY, "abc")));
        assertThat(new AzureOpenAiSecretSettings(new SecureString("abc".toCharArray()), null), is(serviceSettings));
    }

    public void testFromMap_EntraId_Only() {
        var serviceSettings = AzureOpenAiSecretSettings.fromMap(new HashMap<>(Map.of(ENTRA_ID, "xyz")));
        assertThat(new AzureOpenAiSecretSettings(null, new SecureString("xyz".toCharArray())), is(serviceSettings));
    }

    public void testFromMap_ReturnsNull_WhenMapIsNull() {
        assertNull(AzureOpenAiSecretSettings.fromMap(null));
    }

    public void testFromMap_MissingApiKeyAndEntraId_ThrowsError() {
        var thrownException = expectThrows(ValidationException.class, () -> AzureOpenAiSecretSettings.fromMap(new HashMap<>()));

        assertThat(
            thrownException.getMessage(),
            containsString(
                Strings.format(
                    "[secret_settings] must have either the [%s] or the [%s] key set",
                    AzureOpenAiSecretSettings.API_KEY,
                    ENTRA_ID
                )
            )
        );
    }

    public void testFromMap_HasBothApiKeyAndEntraId_ThrowsError() {
        var mapValues = getAzureOpenAiSecretSettingsMap("apikey", "entraid");
        var thrownException = expectThrows(ValidationException.class, () -> AzureOpenAiSecretSettings.fromMap(mapValues));

        assertThat(
            thrownException.getMessage(),
            containsString(
                Strings.format(
                    "[secret_settings] must have only one of the [%s] or the [%s] key set",
                    AzureOpenAiSecretSettings.API_KEY,
                    ENTRA_ID
                )
            )
        );
    }

    public void testFromMap_EmptyApiKey_ThrowsError() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> AzureOpenAiSecretSettings.fromMap(new HashMap<>(Map.of(AzureOpenAiSecretSettings.API_KEY, "")))
        );

        assertThat(
            thrownException.getMessage(),
            containsString(
                Strings.format(
                    "[secret_settings] Invalid value empty string. [%s] must be a non-empty string",
                    AzureOpenAiSecretSettings.API_KEY
                )
            )
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

    // test toXContent
    public void testToXContext_WritesApiKeyOnlyWhenEntraIdIsNull() throws IOException {
        var testSettings = new AzureOpenAiSecretSettings(new SecureString("apikey"), null);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        testSettings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        var expectedResult = Strings.format("{\"%s\":\"apikey\"}", API_KEY);
        assertThat(xContentResult, is(expectedResult));
    }

    public void testToXContext_WritesEntraIdOnlyWhenApiKeyIsNull() throws IOException {
        var testSettings = new AzureOpenAiSecretSettings(null, new SecureString("entraid"));
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        testSettings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        var expectedResult = Strings.format("{\"%s\":\"entraid\"}", ENTRA_ID);
        assertThat(xContentResult, is(expectedResult));
    }

    @Override
    protected Writeable.Reader<AzureOpenAiSecretSettings> instanceReader() {
        return AzureOpenAiSecretSettings::new;
    }

    @Override
    protected AzureOpenAiSecretSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected AzureOpenAiSecretSettings mutateInstance(AzureOpenAiSecretSettings instance) throws IOException {
        return randomValueOtherThan(instance, AzureOpenAiSecretSettingsTests::createRandom);
    }

    @Override
    protected AzureOpenAiSecretSettings mutateInstanceForVersion(AzureOpenAiSecretSettings instance, TransportVersion version) {
        return instance;
    }

    public static Map<String, Object> getAzureOpenAiSecretSettingsMap(@Nullable String apiKey, @Nullable String entraId) {
        var map = new HashMap<String, Object>();
        if (apiKey != null) {
            map.put(AzureOpenAiSecretSettings.API_KEY, apiKey);
        }
        if (entraId != null) {
            map.put(ENTRA_ID, entraId);
        }
        return map;
    }

}
