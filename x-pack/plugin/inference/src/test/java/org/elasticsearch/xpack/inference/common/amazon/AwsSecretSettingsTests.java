/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common.amazon;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockConstants.ACCESS_KEY_FIELD;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockConstants.SECRET_KEY_FIELD;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class AwsSecretSettingsTests extends AbstractBWCWireSerializationTestCase<AwsSecretSettings> {

    public void testNewSecretSettings_UpdatesAllFields() {
        var initialSettings = createRandom();
        var updatedAccessKey = randomValueOtherThan(initialSettings.accessKey().toString(), () -> randomAlphaOfLength(10));
        var updatedSecretKey = randomValueOtherThan(initialSettings.secretKey().toString(), () -> randomAlphaOfLength(10));

        var finalSettings = initialSettings.newSecretSettings(secretSettingsMap(updatedAccessKey, updatedSecretKey));

        assertThat(
            finalSettings,
            is(new AwsSecretSettings(new SecureString(updatedAccessKey.toCharArray()), new SecureString(updatedSecretKey.toCharArray())))
        );
    }

    public void testNewSecretSettings_EmptyMap_DoesNotChangeSettings() {
        var initialSettings = createRandom();
        assertThat(initialSettings.newSecretSettings(new HashMap<>()), sameInstance(initialSettings));
    }

    public void testNewSecretSettings_SameAccessKeyAndSecretKey_DoesNotChangeSettings() {
        var initialSettings = createRandom();
        assertThat(
            initialSettings.newSecretSettings(
                secretSettingsMap(initialSettings.accessKey().toString(), initialSettings.secretKey().toString())
            ),
            sameInstance(initialSettings)
        );
    }

    public void testNewSecretSettings_OnlyAccessKey_ThrowsError() {
        var initialSettings = createRandom();
        var exception = expectThrows(
            ValidationException.class,
            () -> initialSettings.newSecretSettings(new HashMap<>(Map.of(ACCESS_KEY_FIELD, randomAlphaOfLength(10))))
        );

        assertValidationError(exception, mustBeUpdatedTogetherError(SECRET_KEY_FIELD));
    }

    public void testNewSecretSettings_OnlySecretKey_ThrowsError() {
        var initialSettings = createRandom();
        var exception = expectThrows(
            ValidationException.class,
            () -> initialSettings.newSecretSettings(new HashMap<>(Map.of(SECRET_KEY_FIELD, randomAlphaOfLength(10))))
        );

        assertValidationError(exception, mustBeUpdatedTogetherError(ACCESS_KEY_FIELD));
    }

    public void testFromMap_NullMap_ReturnsNull() {
        assertThat(AwsSecretSettings.fromMap(null), is(nullValue()));
    }

    public void testFromMap_CreatesSettings_FromMap_WithValues() {
        var accessKey = randomAlphaOfLength(10);
        var secretKey = randomAlphaOfLength(10);
        assertThat(
            AwsSecretSettings.fromMap(secretSettingsMap(accessKey, secretKey)),
            is(new AwsSecretSettings(new SecureString(accessKey.toCharArray()), new SecureString(secretKey.toCharArray())))
        );
    }

    public void testFromMap_CreatesSettings_FromMap_IgnoresExtraKeys() {
        var accessKey = randomAlphaOfLength(10);
        var secretKey = randomAlphaOfLength(10);
        assertThat(
            AwsSecretSettings.fromMap(
                new HashMap<>(Map.of(ACCESS_KEY_FIELD, accessKey, SECRET_KEY_FIELD, secretKey, "extrakey", "extravalue"))
            ),
            is(new AwsSecretSettings(new SecureString(accessKey.toCharArray()), new SecureString(secretKey.toCharArray())))
        );
    }

    public void testFromMap_ThrowsValidationException_AccessKeyMissing() {
        var exception = expectThrows(
            ValidationException.class,
            () -> AwsSecretSettings.fromMap(new HashMap<>(Map.of(SECRET_KEY_FIELD, randomAlphaOfLength(10))))
        );

        assertValidationError(exception, missingRequiredSettingError(ACCESS_KEY_FIELD));
    }

    public void testFromMap_ThrowsValidationException_SecretKeyMissing() {
        var exception = expectThrows(
            ValidationException.class,
            () -> AwsSecretSettings.fromMap(new HashMap<>(Map.of(ACCESS_KEY_FIELD, randomAlphaOfLength(10))))
        );

        assertValidationError(exception, missingRequiredSettingError(SECRET_KEY_FIELD));
    }

    public void testToXContent_CreatesProperContent() throws IOException {
        var accessKey = randomAlphaOfLength(10);
        var secretKey = randomAlphaOfLength(10);
        var secrets = AwsSecretSettings.fromMap(secretSettingsMap(accessKey, secretKey));

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        secrets.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is(XContentHelper.stripWhitespace(Strings.format("""
            {
                "access_key": "%s",
                "secret_key": "%s"
            }
            """, accessKey, secretKey))));
    }

    public static Map<String, Object> getAmazonBedrockSecretSettingsMap(String accessKey, String secretKey) {
        return new HashMap<>(Map.of(ACCESS_KEY_FIELD, accessKey, SECRET_KEY_FIELD, secretKey));
    }

    @Override
    protected AwsSecretSettings mutateInstanceForVersion(AwsSecretSettings instance, TransportVersion version) {
        return instance;
    }

    @Override
    protected Writeable.Reader<AwsSecretSettings> instanceReader() {
        return AwsSecretSettings::new;
    }

    @Override
    protected AwsSecretSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected AwsSecretSettings mutateInstance(AwsSecretSettings instance) throws IOException {
        if (randomBoolean()) {
            var accessKey = randomValueOtherThan(instance.accessKey().toString(), () -> randomAlphaOfLength(10));
            return new AwsSecretSettings(new SecureString(accessKey.toCharArray()), instance.secretKey());
        } else {
            var secretKey = randomValueOtherThan(instance.secretKey().toString(), () -> randomAlphaOfLength(10));
            return new AwsSecretSettings(instance.accessKey(), new SecureString(secretKey.toCharArray()));
        }
    }

    private static AwsSecretSettings createRandom() {
        return new AwsSecretSettings(
            new SecureString(randomAlphaOfLength(10).toCharArray()),
            new SecureString(randomAlphaOfLength(10).toCharArray())
        );
    }

    private static Map<String, Object> secretSettingsMap(String accessKey, String secretKey) {
        return new HashMap<>(Map.of(ACCESS_KEY_FIELD, accessKey, SECRET_KEY_FIELD, secretKey));
    }

    private static void assertValidationError(ValidationException exception, String expectedError) {
        assertThat(exception.validationErrors().size(), is(1));
        assertThat(exception.validationErrors().getFirst(), is(expectedError));
    }

    private static String missingRequiredSettingError(String fieldName) {
        return Strings.format("[service_settings] does not contain the required setting [%s]", fieldName);
    }

    private static String mustBeUpdatedTogetherError(String missingField) {
        return Strings.format(
            "[service_settings] [%s] and [%s] must be updated together; missing: [%s]",
            ACCESS_KEY_FIELD,
            SECRET_KEY_FIELD,
            missingField
        );
    }
}
