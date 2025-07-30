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
import org.elasticsearch.core.Nullable;
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

public class CustomTaskSettingsTests extends AbstractBWCWireSerializationTestCase<CustomTaskSettings> {
    public static CustomTaskSettings createRandom() {
        Map<String, Object> parameters = randomBoolean()
            ? randomMap(0, 5, () -> tuple(randomAlphaOfLength(5), (Object) randomAlphaOfLength(5)))
            : Map.of();
        return new CustomTaskSettings(parameters);
    }

    public void testFromMap() {
        var taskSettingsMap = new HashMap<String, Object>(
            Map.of(CustomTaskSettings.PARAMETERS, new HashMap<>(Map.of("test_key", "test_value")))
        );

        assertThat(CustomTaskSettings.fromMap(taskSettingsMap), is(new CustomTaskSettings(Map.of("test_key", "test_value"))));
    }

    public void testFromMap_Null_EmptyMap_Returns_EmptySettings() {
        assertThat(CustomTaskSettings.fromMap(Map.of()), is(CustomTaskSettings.EMPTY_SETTINGS));
        assertThat(CustomTaskSettings.fromMap(null), is(CustomTaskSettings.EMPTY_SETTINGS));
    }

    public void testFromMap_RemovesNullValues() {
        var mapWithNulls = new HashMap<String, Object>();
        mapWithNulls.put("value", "abc");
        mapWithNulls.put("null", null);

        assertThat(
            CustomTaskSettings.fromMap(modifiableMap(Map.of(CustomTaskSettings.PARAMETERS, mapWithNulls))),
            is(new CustomTaskSettings(Map.of("value", "abc")))
        );
    }

    public void testFromMap_Throws_IfValueIsInvalid() {
        var exception = expectThrows(
            ValidationException.class,
            () -> CustomTaskSettings.fromMap(
                modifiableMap(Map.of(CustomTaskSettings.PARAMETERS, modifiableMap(Map.of("key", Map.of("another_key", "value")))))
            )
        );

        assertThat(
            exception.getMessage(),
            is(
                "Validation Failed: 1: Map field [parameters] has an entry that is not valid, [key => {another_key=value}]. "
                    + "Value type of [{another_key=value}] is not one of [Boolean, Double, Float, Integer, String].;"
            )
        );
    }

    public void testFromMap_DefaultsToEmptyMap_WhenParametersField_DoesNotExist() {
        var taskSettingsMap = new HashMap<String, Object>(Map.of("key", new HashMap<>(Map.of("test_key", "test_value"))));

        assertThat(CustomTaskSettings.fromMap(taskSettingsMap), is(new CustomTaskSettings(Map.of())));
    }

    public void testOf_PrefersSettingsFromRequest() {
        assertThat(
            CustomTaskSettings.of(
                new CustomTaskSettings(Map.of("a", "a_value", "b", "b_value")),
                new CustomTaskSettings(Map.of("b", "b_value_overwritten"))
            ),
            is(new CustomTaskSettings(Map.of("a", "a_value", "b", "b_value_overwritten")))
        );
    }

    public void testXContent() throws IOException {
        var entity = new CustomTaskSettings(Map.of("test_key", "test_value"));

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        var expected = XContentHelper.stripWhitespace("""
            {
                "parameters": {
                    "test_key": "test_value"
                }
            }
            """);

        assertThat(xContentResult, is(expected));
    }

    public void testXContent_EmptyParameters() throws IOException {
        var entity = new CustomTaskSettings(Map.of());

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
    protected Writeable.Reader<CustomTaskSettings> instanceReader() {
        return CustomTaskSettings::new;
    }

    @Override
    protected CustomTaskSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected CustomTaskSettings mutateInstance(CustomTaskSettings instance) throws IOException {
        return randomValueOtherThan(instance, CustomTaskSettingsTests::createRandom);
    }

    public static Map<String, Object> getTaskSettingsMap(@Nullable Map<String, Object> parameters) {
        var map = new HashMap<String, Object>();
        if (parameters != null) {
            map.put(CustomTaskSettings.PARAMETERS, parameters);
        }

        return map;
    }

    @Override
    protected CustomTaskSettings mutateInstanceForVersion(CustomTaskSettings instance, TransportVersion version) {
        return instance;
    }
}
