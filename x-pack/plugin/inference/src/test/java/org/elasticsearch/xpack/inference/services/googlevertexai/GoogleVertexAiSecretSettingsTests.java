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
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class GoogleVertexAiSecretSettingsTests extends AbstractBWCWireSerializationTestCase<GoogleVertexAiSecretSettings> {

    public static GoogleVertexAiSecretSettings createRandom() {
        return new GoogleVertexAiSecretSettings(randomSecureStringOfLength(30));
    }

    public void testNewSecretSettings() {
        GoogleVertexAiSecretSettings initialSettings = createRandom();
        GoogleVertexAiSecretSettings newSettings = createRandom();
        GoogleVertexAiSecretSettings newGoogleVertexAiSecretSettings = (GoogleVertexAiSecretSettings) initialSettings.newSecretSettings(
            Map.of(GoogleVertexAiSecretSettings.SERVICE_ACCOUNT_JSON, newSettings.serviceAccountJson.toString())
        );
        assertEquals(newSettings, newGoogleVertexAiSecretSettings);
    }

    public void testFromMap_ReturnsNull_WhenMapIsNUll() {
        assertNull(GoogleVertexAiSecretSettings.fromMap(null));
    }

    public void testFromMap_ThrowsError_IfServiceAccountJsonIsMissing() {
        expectThrows(ValidationException.class, () -> GoogleVertexAiSecretSettings.fromMap(new HashMap<>()));
    }

    public void testFromMap_ThrowsError_IfServiceAccountJsonIsEmpty() {
        expectThrows(
            ValidationException.class,
            () -> GoogleVertexAiSecretSettings.fromMap(new HashMap<>(Map.of(GoogleVertexAiSecretSettings.SERVICE_ACCOUNT_JSON, "")))
        );
    }

    public void testToXContent_WritesServiceAccountJson() throws IOException {
        var secretSettings = new GoogleVertexAiSecretSettings(new SecureString("json"));

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        secretSettings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is("""
            {"service_account_json":"json"}"""));
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
        return randomValueOtherThan(instance, GoogleVertexAiSecretSettingsTests::createRandom);
    }

    @Override
    protected GoogleVertexAiSecretSettings mutateInstanceForVersion(GoogleVertexAiSecretSettings instance, TransportVersion version) {
        return instance;
    }
}
