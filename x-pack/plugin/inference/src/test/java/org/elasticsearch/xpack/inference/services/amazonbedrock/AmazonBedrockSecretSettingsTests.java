/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.hamcrest.CoreMatchers;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockConstants.ACCESS_KEY_FIELD;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockConstants.SECRET_KEY_FIELD;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class AmazonBedrockSecretSettingsTests extends AbstractBWCWireSerializationTestCase<AmazonBedrockSecretSettings> {

    public void testNewSecretSettings() {
        AmazonBedrockSecretSettings initialSettings = createRandom();
        AmazonBedrockSecretSettings newSettings = createRandom();

        AmazonBedrockSecretSettings finalSettings = (AmazonBedrockSecretSettings) initialSettings.newSecretSettings(
            Map.of(ACCESS_KEY_FIELD, newSettings.accessKey.toString(), SECRET_KEY_FIELD, newSettings.secretKey.toString())
        );

        assertEquals(newSettings, finalSettings);
    }

    public void testIt_CreatesSettings_ReturnsNullFromMap_null() {
        var secrets = AmazonBedrockSecretSettings.fromMap(null);
        assertNull(secrets);
    }

    public void testIt_CreatesSettings_FromMap_WithValues() {
        var secrets = AmazonBedrockSecretSettings.fromMap(
            new HashMap<>(Map.of(ACCESS_KEY_FIELD, "accesstest", SECRET_KEY_FIELD, "secrettest"))
        );
        assertThat(
            secrets,
            is(new AmazonBedrockSecretSettings(new SecureString("accesstest".toCharArray()), new SecureString("secrettest".toCharArray())))
        );
    }

    public void testIt_CreatesSettings_FromMap_IgnoresExtraKeys() {
        var secrets = AmazonBedrockSecretSettings.fromMap(
            new HashMap<>(Map.of(ACCESS_KEY_FIELD, "accesstest", SECRET_KEY_FIELD, "secrettest", "extrakey", "extravalue"))
        );
        assertThat(
            secrets,
            is(new AmazonBedrockSecretSettings(new SecureString("accesstest".toCharArray()), new SecureString("secrettest".toCharArray())))
        );
    }

    public void testIt_FromMap_ThrowsValidationException_AccessKeyMissing() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> AmazonBedrockSecretSettings.fromMap(new HashMap<>(Map.of(SECRET_KEY_FIELD, "secrettest")))
        );

        assertThat(
            thrownException.getMessage(),
            containsString(Strings.format("[secret_settings] does not contain the required setting [%s]", ACCESS_KEY_FIELD))
        );
    }

    public void testIt_FromMap_ThrowsValidationException_SecretKeyMissing() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> AmazonBedrockSecretSettings.fromMap(new HashMap<>(Map.of(ACCESS_KEY_FIELD, "accesstest")))
        );

        assertThat(
            thrownException.getMessage(),
            containsString(Strings.format("[secret_settings] does not contain the required setting [%s]", SECRET_KEY_FIELD))
        );
    }

    public void testToXContent_CreatesProperContent() throws IOException {
        var secrets = AmazonBedrockSecretSettings.fromMap(
            new HashMap<>(Map.of(ACCESS_KEY_FIELD, "accesstest", SECRET_KEY_FIELD, "secrettest"))
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        secrets.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);
        assertThat(xContentResult, CoreMatchers.is("""
            {"access_key":"accesstest","secret_key":"secrettest"}"""));
    }

    public static Map<String, Object> getAmazonBedrockSecretSettingsMap(String accessKey, String secretKey) {
        return new HashMap<String, Object>(Map.of(ACCESS_KEY_FIELD, accessKey, SECRET_KEY_FIELD, secretKey));
    }

    @Override
    protected AmazonBedrockSecretSettings mutateInstanceForVersion(AmazonBedrockSecretSettings instance, TransportVersion version) {
        return instance;
    }

    @Override
    protected Writeable.Reader<AmazonBedrockSecretSettings> instanceReader() {
        return AmazonBedrockSecretSettings::new;
    }

    @Override
    protected AmazonBedrockSecretSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected AmazonBedrockSecretSettings mutateInstance(AmazonBedrockSecretSettings instance) throws IOException {
        return randomValueOtherThan(instance, AmazonBedrockSecretSettingsTests::createRandom);
    }

    private static AmazonBedrockSecretSettings createRandom() {
        return new AmazonBedrockSecretSettings(new SecureString(randomAlphaOfLength(10)), new SecureString(randomAlphaOfLength(10)));
    }
}
