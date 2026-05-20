/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class GoogleVertexAiSecretSettingsTests extends AbstractBWCWireSerializationTestCase<GoogleVertexAiSecretSettings> {

    private static final String TEST_SERVICE_ACCOUNT_JSON = "some_secret_service_account_json";

    public static GoogleVertexAiSecretSettings createRandom() {
        return new GoogleVertexAiSecretSettings(randomSecureStringOfLength(30));
    }

    public void testNewSecretSettings_UpdatesServiceAccountJson() {
        var initialSettings = createRandom();
        var updatedServiceAccountJson = randomValueOtherThan(
            initialSettings.serviceAccountJson().toString(),
            () -> randomAlphaOfLength(30)
        );
        assertThat(
            initialSettings.newSecretSettings(secretSettingsMap(updatedServiceAccountJson)),
            is(settingsWithServiceAccountJson(updatedServiceAccountJson))
        );
    }

    public void testNewSecretSettings_EmptyMap_DoesNotChangeSettings() {
        var initialSettings = createRandom();
        assertThat(initialSettings.newSecretSettings(new HashMap<>()), sameInstance(initialSettings));
    }

    public void testNewSecretSettings_SameServiceAccountJson_DoesNotChangeSettings() {
        var initialSettings = createRandom();
        assertThat(
            initialSettings.newSecretSettings(secretSettingsMap(initialSettings.serviceAccountJson().toString())),
            sameInstance(initialSettings)
        );
    }

    public void testNewSecretSettings_EmptyServiceAccountJson_ThrowsError() {
        var initialSettings = createRandom();
        var exception = expectThrows(ValidationException.class, () -> initialSettings.newSecretSettings(secretSettingsMap("")));
        assertValidationError(exception, emptyServiceAccountJsonError());
    }

    public void testFromMap_ReturnsNull_WhenMapIsNUll() {
        assertNull(GoogleVertexAiSecretSettings.fromMap(null));
    }

    public void testFromMap_ThrowsError_IfServiceAccountJsonIsMissing() {
        var exception = expectThrows(ValidationException.class, () -> GoogleVertexAiSecretSettings.fromMap(new HashMap<>()));
        assertValidationError(exception, missingServiceAccountJsonError());
    }

    public void testFromMap_ThrowsError_IfServiceAccountJsonIsEmpty() {
        var exception = expectThrows(ValidationException.class, () -> GoogleVertexAiSecretSettings.fromMap(secretSettingsMap("")));
        assertValidationError(exception, emptyServiceAccountJsonError());
    }

    public void testToXContent_WritesServiceAccountJson() throws IOException {
        var secretSettings = settingsWithServiceAccountJson(TEST_SERVICE_ACCOUNT_JSON);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        secretSettings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is(XContentHelper.stripWhitespace(Strings.format("""
            {
                "service_account_json": "%s"
            }
            """, TEST_SERVICE_ACCOUNT_JSON))));
    }

    @Override
    protected Writeable.Reader<GoogleVertexAiSecretSettings> instanceReader() {
        return GoogleVertexAiSecretSettings::new;
    }

    @Override
    protected GoogleVertexAiSecretSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected GoogleVertexAiSecretSettings mutateInstance(GoogleVertexAiSecretSettings instance) throws IOException {
        SecureString serviceAccountJson = randomValueOtherThan(instance.serviceAccountJson(), () -> randomSecureStringOfLength(30));
        return new GoogleVertexAiSecretSettings(serviceAccountJson);
    }

    @Override
    protected GoogleVertexAiSecretSettings mutateInstanceForVersion(GoogleVertexAiSecretSettings instance, TransportVersion version) {
        return instance;
    }

    private static Map<String, Object> secretSettingsMap(String serviceAccountJson) {
        return new HashMap<>(Map.of(GoogleVertexAiSecretSettings.SERVICE_ACCOUNT_JSON, serviceAccountJson));
    }

    private static GoogleVertexAiSecretSettings settingsWithServiceAccountJson(String serviceAccountJson) {
        return new GoogleVertexAiSecretSettings(new SecureString(serviceAccountJson.toCharArray()));
    }

    private static void assertValidationError(ValidationException exception, String expectedError) {
        assertThat(exception.validationErrors().size(), is(1));
        assertThat(exception.validationErrors().getFirst(), is(expectedError));
    }

    private static String missingServiceAccountJsonError() {
        return Strings.format(
            "[%s] does not contain the required setting [%s]",
            ModelSecrets.SECRET_SETTINGS,
            GoogleVertexAiSecretSettings.SERVICE_ACCOUNT_JSON
        );
    }

    private static String emptyServiceAccountJsonError() {
        return Strings.format(
            "[%s] Invalid value empty string. [%s] must be a non-empty string",
            ModelSecrets.SECRET_SETTINGS,
            GoogleVertexAiSecretSettings.SERVICE_ACCOUNT_JSON
        );
    }
}
