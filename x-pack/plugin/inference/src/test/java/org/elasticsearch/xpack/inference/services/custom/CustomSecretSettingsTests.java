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
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.services.settings.SerializableSecureString;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.core.Tuple.tuple;
import static org.elasticsearch.xpack.inference.Utils.modifiableMap;
import static org.hamcrest.Matchers.is;

public class CustomSecretSettingsTests extends AbstractBWCWireSerializationTestCase<CustomSecretSettings> {
    public static CustomSecretSettings createRandom() {
        Map<String, SerializableSecureString> secretParameters = randomMap(
            0,
            5,
            () -> tuple(randomAlphaOfLength(5), new SerializableSecureString(randomAlphaOfLength(5)))
        );

        return new CustomSecretSettings(secretParameters);
    }

    public void testFromMap() {
        Map<String, Object> secretParameters = new HashMap<>(
            Map.of(CustomSecretSettings.SECRET_PARAMETERS, new HashMap<>(Map.of("test_key", "test_value")))
        );

        assertThat(
            CustomSecretSettings.fromMap(secretParameters),
            is(new CustomSecretSettings(Map.of("test_key", new SerializableSecureString("test_value"))))
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
            is(new CustomSecretSettings(Map.of("value", new SerializableSecureString("abc"))))
        );
    }

    public void testFromMap_Throws_IfValueIsInvalid() {
        var exception = expectThrows(
            ValidationException.class,
            () -> CustomSecretSettings.fromMap(
                modifiableMap(Map.of(CustomSecretSettings.SECRET_PARAMETERS, modifiableMap(Map.of("key", Map.of("another_key", "value")))))
            )
        );

        assertThat(
            exception.getMessage(),
            is(
                "Validation Failed: 1: Map field [secret_parameters] has an entry that is not valid. "
                    + "Value type is not one of [String].;"
            )
        );
    }

    public void testFromMap_DefaultsToEmptyMap_WhenSecretParametersField_DoesNotExist() {
        var map = new HashMap<String, Object>(Map.of("key", new HashMap<>(Map.of("test_key", "test_value"))));

        assertThat(CustomSecretSettings.fromMap(map), is(new CustomSecretSettings(Map.of())));
    }

    public void testXContent() throws IOException {
        var entity = new CustomSecretSettings(Map.of("test_key", new SerializableSecureString("test_value")));

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
        return randomValueOtherThan(instance, CustomSecretSettingsTests::createRandom);
    }

    @Override
    protected CustomSecretSettings mutateInstanceForVersion(CustomSecretSettings instance, TransportVersion version) {
        return instance;
    }
}
