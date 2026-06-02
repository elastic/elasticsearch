/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.custom;

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

import static org.elasticsearch.core.Tuple.tuple;
import static org.elasticsearch.xpack.inference.Utils.modifiableMap;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class CustomSecretSettingsTests extends AbstractBWCWireSerializationTestCase<CustomSecretSettings> {
    public static CustomSecretSettings createRandom() {
        return new CustomSecretSettings(createRandomSecretParameters());
    }

    private static Map<String, SecureString> createRandomSecretParameters() {
        return randomMap(0, 5, () -> tuple(randomAlphaOfLength(5), new SecureString(randomAlphaOfLength(5).toCharArray())));
    }

    public void testNewSecretSettings_UpdatesSecretParameters() {
        var updatedKey = "updated_key";
        var updatedValue = "updated_value";
        var initialSettings = new CustomSecretSettings(Map.of("initial_key", new SecureString("initial_value".toCharArray())));
        var updatedSettings = (CustomSecretSettings) initialSettings.newSecretSettings(secretSettingsMap(Map.of(updatedKey, updatedValue)));

        assertThat(updatedSettings, is(new CustomSecretSettings(Map.of(updatedKey, new SecureString(updatedValue.toCharArray())))));
    }

    public void testNewSecretSettings_EmptyMap_DoesNotChangeSettings() {
        var initialSettings = createRandom();
        assertThat(initialSettings.newSecretSettings(new HashMap<>()), sameInstance(initialSettings));
    }

    public void testNewSecretSettings_EmptySecretParameters_DoesNotChangeSettings() {
        var initialSettings = new CustomSecretSettings(Map.of("key", new SecureString("value".toCharArray())));
        assertThat(initialSettings.newSecretSettings(secretSettingsMap(Map.of())), sameInstance(initialSettings));
    }

    public void testNewSecretSettings_SameSecretParameters_DoesNotChangeSettings() {
        var initialKey = "key";
        var initialValue = "value";
        var initialSettings = new CustomSecretSettings(Map.of(initialKey, new SecureString(initialValue.toCharArray())));
        assertThat(initialSettings.newSecretSettings(secretSettingsMap(Map.of(initialKey, initialValue))), sameInstance(initialSettings));
    }

    public void testNewSecretSettings_InvalidValue_ThrowsError() {
        var initialSettings = createRandom();
        var exception = expectThrows(
            ValidationException.class,
            () -> initialSettings.newSecretSettings(secretSettingsMap(Map.of("key", Map.of("nested_key", "nested_value"))))
        );

        assertValidationError(exception, invalidValueTypeError());
    }

    public void testFromMap() {
        Map<String, Object> secretParameters = new HashMap<>(
            Map.of(CustomSecretSettings.SECRET_PARAMETERS, new HashMap<>(Map.of("test_key", "test_value")))
        );

        assertThat(
            CustomSecretSettings.fromMap(secretParameters),
            is(new CustomSecretSettings(Map.of("test_key", new SecureString("test_value".toCharArray()))))
        );
    }

    public void testFromMap_PassedNull_ReturnsNull() {
        assertNull(CustomSecretSettings.fromMap(null));
    }

    public void testFromMap_RemovesNullValues() {
        var mapWithNulls = new HashMap<String, Object>();
        mapWithNulls.put("value", "abc");
        mapWithNulls.put("null", null);

        assertThat(
            CustomSecretSettings.fromMap(modifiableMap(Map.of(CustomSecretSettings.SECRET_PARAMETERS, mapWithNulls))),
            is(new CustomSecretSettings(Map.of("value", new SecureString("abc".toCharArray()))))
        );
    }

    public void testFromMap_Throws_IfValueIsInvalid() {
        var exception = expectThrows(
            ValidationException.class,
            () -> CustomSecretSettings.fromMap(
                modifiableMap(Map.of(CustomSecretSettings.SECRET_PARAMETERS, modifiableMap(Map.of("key", Map.of("another_key", "value")))))
            )
        );

        assertValidationError(exception, invalidValueTypeError());
    }

    public void testFromMap_DefaultsToEmptyMap_WhenSecretParametersField_DoesNotExist() {
        var map = new HashMap<String, Object>(Map.of("key", new HashMap<>(Map.of("test_key", "test_value"))));

        assertThat(CustomSecretSettings.fromMap(map), is(new CustomSecretSettings(Map.of())));
    }

    public void testXContent() throws IOException {
        var entity = new CustomSecretSettings(Map.of("test_key", new SecureString("test_value".toCharArray())));

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        var expected = XContentHelper.stripWhitespace("""
            {
                "secret_parameters": {
                    "test_key": "test_value"
                }
            }
            """);

        assertThat(xContentResult, is(expected));
    }

    public void testXContent_EmptyParameters() throws IOException {
        var entity = new CustomSecretSettings(Map.of());

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        var expected = XContentHelper.stripWhitespace("""
            {
            }
            """);

        assertThat(xContentResult, is(expected));
    }

    @Override
    protected Writeable.Reader<CustomSecretSettings> instanceReader() {
        return CustomSecretSettings::new;
    }

    @Override
    protected CustomSecretSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected CustomSecretSettings mutateInstance(CustomSecretSettings instance) {
        return new CustomSecretSettings(
            randomValueOtherThan(instance.getSecretParameters(), CustomSecretSettingsTests::createRandomSecretParameters)
        );
    }

    @Override
    protected CustomSecretSettings mutateInstanceForVersion(CustomSecretSettings instance, TransportVersion version) {
        return instance;
    }

    private static Map<String, Object> secretSettingsMap(Map<String, ?> secretParameters) {
        return new HashMap<>(Map.of(CustomSecretSettings.SECRET_PARAMETERS, new HashMap<>(secretParameters)));
    }

    private static void assertValidationError(ValidationException exception, String expectedError) {
        assertThat(exception.validationErrors().size(), is(1));
        assertThat(exception.validationErrors().getFirst(), is(expectedError));
    }

    private static String invalidValueTypeError() {
        return Strings.format(
            "Map field [%s] has an entry that is not valid. Value type is not one of [String].",
            CustomSecretSettings.SECRET_PARAMETERS
        );
    }
}
