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
import org.elasticsearch.test.AbstractBWCSerializationTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.inference.InferenceString.FORMAT_FIELD;
import static org.elasticsearch.inference.InferenceString.TYPE_FIELD;
import static org.elasticsearch.inference.InferenceString.VALUE_FIELD;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class InferenceStringTests extends AbstractBWCSerializationTestCase<InferenceString> {
    public static final String TEST_IMAGE_DATA_URI = "data:image/jpeg;base64,abcd";

    public void testConstructorWithNoFormat_usesDefault() {
        assertThat(new InferenceString(DataType.TEXT, "value").dataFormat(), is(DataFormat.TEXT));
        assertThat(new InferenceString(DataType.IMAGE, TEST_IMAGE_DATA_URI).dataFormat(), is(DataFormat.BASE64));
    }

    public void testSupportedFormatsForType() {
        assertThat(DataType.TEXT.getSupportedFormats(), is(EnumSet.of(DataFormat.TEXT)));
        assertThat(DataType.IMAGE.getSupportedFormats(), is(EnumSet.of(DataFormat.BASE64)));
    }

    public void testConstructorWithInvalidDataURI_throws() {
        var invalidDataURIs = List.of(
            "notADataURI",
            "image/jpeg;base64,abcd", // missing "data:"
            "data:image/jpeg;base64abcd", // missing final ","
            "data:;base64,abcd", // missing MIME type
            "data:image/jpeg;abcd", // missing "base64,"
            "Xdata:image/jpeg;base64,abcd", // extra character at start
            "data;image/jpeg;base64,abcd", // doesn't start with "data:"
            "data:image/jpeg;base63,abcd", // doesn't contain "base64,"
            "data:invalid;base64,abcd" // invalid MIME type format
        );
        invalidDataURIs.forEach(value -> {
            var exception = assertThrows(
                IllegalArgumentException.class,
                () -> new InferenceString(DataType.IMAGE, DataFormat.BASE64, value)
            );
            assertThat(
                exception.getMessage(),
                is("base64 inputs must be specified as data URIs with the format [data:{MIME-type};base64,...]")
            );
        });
    }

    public void testConstructorWithValidDataURIFormat() {
        var value = Strings.format(
            "data:%s/%s;base64,%s",
            randomAlphanumericOfLength(10),
            randomAlphanumericOfLength(10),
            randomAlphanumericOfLength(10)
        );
        new InferenceString(DataType.IMAGE, DataFormat.BASE64, value);
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
        var requestJson = Strings.format("""
            {
                "type": "image",
                "format": "base64",
                "value": "%s"
            }
            """, TEST_IMAGE_DATA_URI);
        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            var request = InferenceString.PARSER.apply(parser, null);
            assertThat(request.dataType(), is(DataType.IMAGE));
            assertThat(request.dataFormat(), is(DataFormat.BASE64));
            assertThat(request.value(), is(TEST_IMAGE_DATA_URI));
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
        var requestJson = Strings.format("""
            {
                "type": "image",
                "value": "%s"
            }
            """, TEST_IMAGE_DATA_URI);
        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            var request = InferenceString.PARSER.apply(parser, null);
            assertThat(request.dataType(), is(DataType.IMAGE));
            assertThat(request.dataFormat(), is(DataFormat.BASE64));
            assertThat(request.value(), is(TEST_IMAGE_DATA_URI));
        }
    }

    public void testParserWithNoType_throwsException() throws IOException {
        var requestJson = Strings.format("""
            {
                "value": "%s"
            }
            """, TEST_IMAGE_DATA_URI);
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
        var requestJson = Strings.format("""
            {
                "type": "image",
                "format": "base64",
                "value": "%s",
                "extra": "should throw"
            }
            """, TEST_IMAGE_DATA_URI);
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
                "value": "%s"
            }
            """, invalidFormat, TEST_IMAGE_DATA_URI);
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
        unsupportedDataFormats.removeAll(type.getSupportedFormats());
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
                        type.getSupportedFormats()
                    )
                )
            );
        }
    }

    public void testParserWithInvalidDataURI_throwsException() throws IOException {
        var requestJson = """
            {
                "type": "image",
                "format": "base64",
                "value": "not_a_data_uri"
            }
            """;
        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            var exception = expectThrows(IllegalArgumentException.class, () -> InferenceString.PARSER.apply(parser, null));
            assertThat(exception.getMessage(), containsString("[InferenceString] failed to parse field [value]"));
            Throwable cause = exception.getCause();
            assertThat(cause.getMessage(), containsString("Failed to build [InferenceString] after last required field arrived"));
            assertThat(
                cause.getCause().getMessage(),
                containsString("base64 inputs must be specified as data URIs with the format [data:{MIME-type};base64,...]")
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
        inferenceStrings.add(randomInt(inferenceStrings.size()), new InferenceString(DataType.IMAGE, TEST_IMAGE_DATA_URI));
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
        DataFormat format = randomBoolean() ? randomFrom(dataType.getSupportedFormats()) : null;
        var value = convertToDataURIIfNeeded(dataType, format, randomAlphanumericOfLength(10));
        return new InferenceString(dataType, format, value);
    }

    // Ensure we create a valid data URI format value if the format is base64
    public static String convertToDataURIIfNeeded(DataType dataType, DataFormat format, String value) {
        var formatToUse = format == null ? dataType.getDefaultFormat() : format;
        if (formatToUse == DataFormat.BASE64) {
            return "data:image/jpeg;base64," + value;
        }
        return value;
    }

    @Override
    protected InferenceString mutateInstance(InferenceString instance) throws IOException {
        if (randomBoolean()) {
            DataType newDataType = randomValueOtherThan(instance.dataType(), () -> randomFrom(DataType.values()));
            DataFormat format = randomFrom(newDataType.getSupportedFormats());
            return new InferenceString(newDataType, format, convertToDataURIIfNeeded(newDataType, format, instance.value()));
        } else {
            String value = instance.value();
            return new InferenceString(
                instance.dataType(),
                instance.dataFormat(),
                randomValueOtherThan(
                    value,
                    () -> convertToDataURIIfNeeded(instance.dataType(), instance.dataFormat(), randomAlphanumericOfLength(10))
                )
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

    /**
     * Converts the given {@link InferenceString} to a map matching what is sent in the request body. Equivalent to converting the
     * input to XContent, then parsing the XContent to a map.
     */
    public static Map<String, Object> toRequestMap(InferenceString input) {
        return Map.of(TYPE_FIELD, input.dataType().toString(), FORMAT_FIELD, input.dataFormat().toString(), VALUE_FIELD, input.value());
    }
}
