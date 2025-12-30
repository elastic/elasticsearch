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
import org.elasticsearch.inference.InferenceString.DataFormat;
import org.elasticsearch.inference.InferenceString.DataType;
import org.elasticsearch.test.AbstractBWCSerializationTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.inference.InferenceString.supportedFormatsForType;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class InferenceStringTests extends AbstractBWCSerializationTestCase<InferenceString> {
    public void testConstructorWithNoFormat_usesDefault() {
        assertThat(new InferenceString(DataType.TEXT, "value").dataFormat(), is(DataFormat.TEXT));
        assertThat(new InferenceString(DataType.IMAGE, "value").dataFormat(), is(DataFormat.BASE64));
    }

    public void testSupportedFormatsForType() {
        assertThat(supportedFormatsForType(DataType.TEXT), is(EnumSet.of(DataFormat.TEXT)));
        assertThat(supportedFormatsForType(DataType.IMAGE), is(EnumSet.of(DataFormat.BASE64)));
    }

    public void testParserWithText() throws IOException {
        var requestJson = """
            {
                "type": "text",
                "format": "text",
                "value": "some text input"
            }
            """;
        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            var request = InferenceString.PARSER.apply(parser, null);
            assertThat(request.dataType(), is(DataType.TEXT));
            assertThat(request.dataFormat(), is(DataFormat.TEXT));
            assertThat(request.value(), is("some text input"));
        }
    }

    public void testParserWithBase64Image() throws IOException {
        var requestJson = """
            {
                "type": "image",
                "format": "base64",
                "value": "some image data"
            }
            """;
        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            var request = InferenceString.PARSER.apply(parser, null);
            assertThat(request.dataType(), is(DataType.IMAGE));
            assertThat(request.dataFormat(), is(DataFormat.BASE64));
            assertThat(request.value(), is("some image data"));
        }
    }

    public void testParserWithDefaultTextFormat() throws IOException {
        var requestJson = """
            {
                "type": "text",
                "value": "some text input"
            }
            """;
        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            var request = InferenceString.PARSER.apply(parser, null);
            assertThat(request.dataType(), is(DataType.TEXT));
            assertThat(request.dataFormat(), is(DataFormat.TEXT));
            assertThat(request.value(), is("some text input"));
        }
    }

    public void testParserWithDefaultImageFormat() throws IOException {
        var requestJson = """
            {
                "type": "image",
                "value": "some image data"
            }
            """;
        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            var request = InferenceString.PARSER.apply(parser, null);
            assertThat(request.dataType(), is(DataType.IMAGE));
            assertThat(request.dataFormat(), is(DataFormat.BASE64));
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
                "type": "image",
                "format": "base64",
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
                "format": "text",
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
                is(Strings.format("Unrecognized type [%s], must be one of [text, image]", invalidType))
            );
        }
    }

    public void testParserWithUnknownFormat_throwsException() throws IOException {
        var invalidFormat = "not a real format";
        var requestJson = Strings.format("""
            {
                "type": "text",
                "format": "%s"
                "value": "some image data"
            }
            """, invalidFormat);
        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            IllegalArgumentException exception = expectThrows(
                IllegalArgumentException.class,
                () -> InferenceString.PARSER.apply(parser, null)
            );
            assertThat(exception.getMessage(), containsString("[InferenceString] failed to parse field [format]"));
            assertThat(
                exception.getCause().getMessage(),
                is(Strings.format("Unrecognized format [%s], must be one of [text, base64]", invalidFormat))
            );
        }
    }

    public void testParserWithInvalidTypeAndFormatCombination_throwsException() throws IOException {
        var type = randomFrom(DataType.values());
        var unsupportedDataFormats = EnumSet.allOf(DataFormat.class);
        unsupportedDataFormats.removeAll(supportedFormatsForType(type));
        var invalidFormat = randomFrom(unsupportedDataFormats);
        var requestJson = Strings.format("""
            {
                "type": "%s",
                "format": "%s",
                "value": "some data"
            }
            """, type, invalidFormat);
        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            IllegalArgumentException exception = expectThrows(
                IllegalArgumentException.class,
                () -> InferenceString.PARSER.apply(parser, null)
            );
            assertThat(exception.getMessage(), containsString("[InferenceString] failed to parse field [value]"));
            Throwable cause = exception.getCause();
            assertThat(cause.getMessage(), is("Failed to build [InferenceString] after last required field arrived"));
            assertThat(
                cause.getCause().getMessage(),
                is(
                    Strings.format(
                        "Data type [%s] does not support data format [%s], supported formats are %s",
                        type,
                        invalidFormat,
                        supportedFormatsForType(type)
                    )
                )
            );
        }
    }

    public void testToStringList_withAllTextInferenceStrings() {
        var rawStrings = List.of("one", "two", "three", "four");
        var inferenceStrings = rawStrings.stream().map(s -> new InferenceString(DataType.TEXT, s)).toList();
        assertThat(InferenceString.toStringList(inferenceStrings), is(rawStrings));
    }

    public void testToStringList_throwsAssertionError_whenAnyInferenceStringIsNotText() {
        var rawStrings = List.of("one", "two", "three", "four");
        var inferenceStrings = rawStrings.stream().map(s -> new InferenceString(DataType.TEXT, s)).collect(Collectors.toList());
        // Add a non-text InferenceString randomly in the list
        inferenceStrings.add(randomInt(inferenceStrings.size()), new InferenceString(DataType.IMAGE, "image data"));
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
        DataType dataType = randomFrom(DataType.values());
        DataFormat format = randomFrom(supportedFormatsForType(dataType));
        return new InferenceString(dataType, format, randomAlphanumericOfLength(10));
    }

    @Override
    protected InferenceString mutateInstance(InferenceString instance) throws IOException {
        if (randomBoolean()) {
            DataType newDataType = randomValueOtherThan(instance.dataType(), () -> randomFrom(DataType.values()));
            DataFormat format = randomFrom(supportedFormatsForType(newDataType));
            return new InferenceString(newDataType, format, instance.value());
        } else {
            String value = instance.value();
            return new InferenceString(
                instance.dataType(),
                instance.dataFormat(),
                randomValueOtherThan(value, () -> randomAlphanumericOfLength(10))
            );
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
