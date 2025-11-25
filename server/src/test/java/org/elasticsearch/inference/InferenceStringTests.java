/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.inference.InferenceString.DataType;
import org.elasticsearch.test.AbstractBWCSerializationTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.inference.InferenceString.DataType.IMAGE_BASE64;
import static org.elasticsearch.inference.InferenceString.DataType.TEXT;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class InferenceStringTests extends AbstractBWCSerializationTestCase<InferenceString> {
    public void testParserWithText() throws IOException {
        var requestJson = """
            {
                "type": "text",
                "value": "some text input"
            }
            """;
        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            var request = InferenceString.PARSER.apply(parser, null);
            assertThat(request.dataType(), is(TEXT));
            assertThat(request.value(), is("some text input"));
        }
    }

    public void testParserWithBase64Image() throws IOException {
        var requestJson = """
            {
                "type": "image_base64",
                "value": "some image data"
            }
            """;
        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            var request = InferenceString.PARSER.apply(parser, null);
            assertThat(request.dataType(), is(IMAGE_BASE64));
            assertThat(request.value(), is("some image data"));
        }
    }

    public void testParserWithNoType_throwsException() throws IOException {
        var requestJson = """
            {
                "value": "some image data"
            }
            """;
        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            IllegalArgumentException exception = expectThrows(
                IllegalArgumentException.class,
                () -> InferenceString.PARSER.apply(parser, null)
            );
            assertThat(exception.getMessage(), is("Required [type]"));
        }
    }

    public void testParserWithNoValue_throwsException() throws IOException {
        var requestJson = """
            {
                "type": "text"
            }
            """;
        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            IllegalArgumentException exception = expectThrows(
                IllegalArgumentException.class,
                () -> InferenceString.PARSER.apply(parser, null)
            );
            assertThat(exception.getMessage(), is("Required [value]"));
        }
    }

    public void testParserWithUnknownField_throwsException() throws IOException {
        var requestJson = """
            {
                "type": "image_base64",
                "value": "some image data",
                "extra": "should throw"
            }
            """;
        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            IllegalArgumentException exception = expectThrows(
                IllegalArgumentException.class,
                () -> InferenceString.PARSER.apply(parser, null)
            );
            assertThat(exception.getMessage(), containsString("unknown field [extra]"));
        }
    }

    public void testParserWithUnknownType_throwsException() throws IOException {
        var invalidType = "not a real type";
        var requestJson = Strings.format("""
            {
                "type": "%s",
                "value": "some image data"
            }
            """, invalidType);
        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            IllegalArgumentException exception = expectThrows(
                IllegalArgumentException.class,
                () -> InferenceString.PARSER.apply(parser, null)
            );
            assertThat(exception.getMessage(), containsString("[InferenceString] failed to parse field [type]"));
            assertThat(
                exception.getCause().getMessage(),
                is(Strings.format("Unrecognized type [%s], must be one of [text, image_base64]", invalidType))
            );
        }
    }

    public void testToStringList_withAllTextInferenceStrings() {
        var rawStrings = List.of("one", "two", "three", "four");
        var inferenceStrings = rawStrings.stream().map(s -> new InferenceString(TEXT, s)).toList();
        assertThat(InferenceString.toStringList(inferenceStrings), is(rawStrings));
    }

    public void testToStringList_throwsAssertionError_whenAnyInferenceStringIsNotText() {
        var rawStrings = List.of("one", "two", "three", "four");
        var inferenceStrings = rawStrings.stream().map(s -> new InferenceString(TEXT, s)).collect(Collectors.toList());
        // Add a non-text InferenceString randomly in the list
        inferenceStrings.add(randomInt(inferenceStrings.size()), new InferenceString(IMAGE_BASE64, "image data"));
        AssertionError assertionError = expectThrows(AssertionError.class, () -> InferenceString.toStringList(inferenceStrings));
        assertThat(assertionError.getMessage(), is("Non-text input returned from InferenceString.textValue"));
    }

    @Override
    protected Writeable.Reader<InferenceString> instanceReader() {
        return InferenceString::new;
    }

    @Override
    protected InferenceString createTestInstance() {
        return createRandom();
    }

    public static InferenceString createRandom() {
        return new InferenceString(randomFrom(DataType.values()), randomAlphanumericOfLength(10));
    }

    @Override
    protected InferenceString mutateInstance(InferenceString instance) throws IOException {
        if (randomBoolean()) {
            DataType dataType = instance.dataType();
            return new InferenceString(randomValueOtherThan(dataType, () -> randomFrom(DataType.values())), instance.value());
        } else {
            String value = instance.value();
            return new InferenceString(instance.dataType(), randomValueOtherThan(value, () -> randomAlphanumericOfLength(10)));
        }
    }

    @Override
    protected InferenceString mutateInstanceForVersion(InferenceString instance, TransportVersion version) {
        return instance;
    }

    @Override
    protected InferenceString doParseInstance(XContentParser parser) throws IOException {
        return InferenceString.PARSER.parse(parser, null);
    }
}
