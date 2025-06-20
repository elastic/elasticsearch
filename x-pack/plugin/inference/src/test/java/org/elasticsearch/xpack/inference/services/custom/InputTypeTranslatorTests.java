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
import org.elasticsearch.inference.InputType;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.core.Tuple.tuple;
import static org.hamcrest.Matchers.is;

public class InputTypeTranslatorTests extends AbstractBWCWireSerializationTestCase<InputTypeTranslator> {
    public static InputTypeTranslator createRandom() {
        Map<InputType, String> translation = randomBoolean()
            ? randomMap(
                0,
                5,
                () -> tuple(
                    randomFrom(List.of(InputType.CLASSIFICATION, InputType.CLUSTERING, InputType.INGEST, InputType.SEARCH)),
                    randomAlphaOfLength(5)
                )
            )
            : Map.of();
        return new InputTypeTranslator(translation, randomAlphaOfLength(5));
    }

    public void testFromMap() {
        var settings = new HashMap<String, Object>(
            Map.of(
                InputTypeTranslator.INPUT_TYPE_TRANSLATOR,
                new HashMap<>(
                    Map.of(
                        InputTypeTranslator.TRANSLATION,
                        new HashMap<>(
                            Map.of(
                                "CLASSIFICATION",
                                "test_value",
                                "CLUSTERING",
                                "test_value_2",
                                "INGEST",
                                "test_value_3",
                                "SEARCH",
                                "test_value_4"
                            )
                        ),
                        InputTypeTranslator.DEFAULT,
                        "default_value"
                    )
                )
            )
        );

        assertThat(
            InputTypeTranslator.fromMap(settings, new ValidationException(), "name"),
            is(
                new InputTypeTranslator(
                    Map.of(
                        InputType.CLASSIFICATION,
                        "test_value",
                        InputType.CLUSTERING,
                        "test_value_2",
                        InputType.INGEST,
                        "test_value_3",
                        InputType.SEARCH,
                        "test_value_4"
                    ),
                    "default_value"
                )
            )
        );
    }

    public void testFromMap_Null_EmptyMap_Returns_EmptySettings() {
        assertThat(InputTypeTranslator.fromMap(null, null, null), is(InputTypeTranslator.EMPTY_TRANSLATOR));
        assertThat(InputTypeTranslator.fromMap(Map.of(), null, null), is(InputTypeTranslator.EMPTY_TRANSLATOR));
    }

    public void testFromMap_Throws_IfValueIsNotAString() {
        var settings = new HashMap<String, Object>(
            Map.of(
                InputTypeTranslator.INPUT_TYPE_TRANSLATOR,
                new HashMap<>(Map.of(InputTypeTranslator.TRANSLATION, new HashMap<>(Map.of("CLASSIFICATION", 12345))))
            )
        );

        var exception = expectThrows(
            ValidationException.class,
            () -> InputTypeTranslator.fromMap(settings, new ValidationException(), "name")
        );

        assertThat(
            exception.getMessage(),
            is(
                "Validation Failed: 1: Input type translation value for key [CLASSIFICATION] "
                    + "must be a String that is not null and not empty, received: [12345], type: [Integer].;"
            )
        );
    }

    public void testFromMap_Throws_IfValueIsEmptyString() {
        var settings = new HashMap<String, Object>(
            Map.of(
                InputTypeTranslator.INPUT_TYPE_TRANSLATOR,
                new HashMap<>(Map.of(InputTypeTranslator.TRANSLATION, new HashMap<>(Map.of("CLASSIFICATION", ""))))
            )
        );

        var exception = expectThrows(
            ValidationException.class,
            () -> InputTypeTranslator.fromMap(settings, new ValidationException(), "name")
        );

        assertThat(
            exception.getMessage(),
            is(
                "Validation Failed: 1: Input type translation value for key [CLASSIFICATION] "
                    + "must be a String that is not null and not empty, received: [], type: [String].;"
            )
        );
    }

    public void testFromMap_DoesNotThrow_ForAnEmptyDefaultValue() {
        var settings = new HashMap<String, Object>(
            Map.of(
                InputTypeTranslator.INPUT_TYPE_TRANSLATOR,
                new HashMap<>(
                    Map.of(
                        InputTypeTranslator.TRANSLATION,
                        new HashMap<>(Map.of("CLASSIFICATION", "value")),
                        InputTypeTranslator.DEFAULT,
                        ""
                    )
                )
            )
        );

        var translator = InputTypeTranslator.fromMap(settings, new ValidationException(), "name");

        assertThat(translator, is(new InputTypeTranslator(Map.of(InputType.CLASSIFICATION, "value"), "")));
    }

    public void testFromMap_Throws_IfKeyIsInvalid() {
        var settings = new HashMap<String, Object>(
            Map.of(
                InputTypeTranslator.INPUT_TYPE_TRANSLATOR,
                new HashMap<>(
                    Map.of(
                        InputTypeTranslator.TRANSLATION,
                        new HashMap<>(Map.of("CLASSIFICATION", "test_value", "invalid_key", "another_value"))
                    )
                )
            )
        );

        var exception = expectThrows(
            ValidationException.class,
            () -> InputTypeTranslator.fromMap(settings, new ValidationException(), "name")
        );

        assertThat(
            exception.getMessage(),
            is(
                "Validation Failed: 1: Invalid input type translation for key: [invalid_key]"
                    + ", is not a valid value. Must be one of [ingest, search, classification, clustering];"
            )
        );
    }

    public void testFromMap_DefaultsToEmptyMap_WhenField_DoesNotExist() {
        var map = new HashMap<String, Object>(Map.of("key", new HashMap<>(Map.of("test_key", "test_value"))));

        assertThat(InputTypeTranslator.fromMap(map, new ValidationException(), "name"), is(new InputTypeTranslator(Map.of(), null)));
    }

    public void testXContent() throws IOException {
        var entity = new InputTypeTranslator(Map.of(InputType.CLASSIFICATION, "test_value"), "default");

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);

        builder.startObject();
        entity.toXContent(builder, null);
        builder.endObject();

        String xContentResult = Strings.toString(builder);

        var expected = XContentHelper.stripWhitespace("""
            {
                "input_type": {
                    "translation": {
                        "classification": "test_value"
                    },
                    "default": "default"
                }
            }
            """);

        assertThat(xContentResult, is(expected));
    }

    public void testXContent_EmptyTranslator() throws IOException {
        var entity = new InputTypeTranslator(Map.of(), null);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);

        builder.startObject();
        entity.toXContent(builder, null);
        builder.endObject();

        String xContentResult = Strings.toString(builder);

        var expected = XContentHelper.stripWhitespace("""
            {
                "input_type": {
                    "translation": {},
                    "default": ""
                }
            }
            """);

        assertThat(xContentResult, is(expected));
    }

    @Override
    protected Writeable.Reader<InputTypeTranslator> instanceReader() {
        return InputTypeTranslator::new;
    }

    @Override
    protected InputTypeTranslator createTestInstance() {
        return createRandom();
    }

    @Override
    protected InputTypeTranslator mutateInstance(InputTypeTranslator instance) throws IOException {
        return randomValueOtherThan(instance, InputTypeTranslatorTests::createRandom);
    }

    public static Map<String, Object> getTaskSettingsMap(@Nullable Map<String, Object> parameters) {
        var map = new HashMap<String, Object>();
        if (parameters != null) {
            map.put(CustomTaskSettings.PARAMETERS, parameters);
        }

        return map;
    }

    @Override
    protected InputTypeTranslator mutateInstanceForVersion(InputTypeTranslator instance, TransportVersion version) {
        return instance;
    }
}
