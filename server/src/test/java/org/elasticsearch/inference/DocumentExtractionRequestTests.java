/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.inference.DocumentExtractionRequest.SUPPORTED_DOCUMENT_EXTRACTION_DATA_TYPES;
import static org.elasticsearch.inference.InferenceStringTests.TEST_DATA_URI;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

/**
 * Tests wire serialization and parsing of {@link DocumentExtractionRequest}. This test does not extend a BWC serialization test case
 * because the request is only carried by the document extraction action, which was introduced together with this class, so the request
 * is never (de)serialized by a node on an older version.
 */
public class DocumentExtractionRequestTests extends AbstractWireSerializingTestCase<DocumentExtractionRequest> {

    public void testParser_WithSingleContentInput() throws IOException {
        var requestJson = Strings.format("""
            {
                "input": [
                    {
                        "content": {"type": "pdf", "format": "base64", "value": "%s"}
                    }
                ]
            }
            """, TEST_DATA_URI);
        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            var request = DocumentExtractionRequest.PARSER.apply(parser, null);
            assertThat(request.inputs(), is(List.of(new InferenceString(DataType.PDF, DataFormat.BASE64, TEST_DATA_URI))));
            assertThat(request.taskSettings(), anEmptyMap());
        }
    }

    public void testParser_WithMultipleContentInputs() throws IOException {
        var requestJson = Strings.format("""
            {
                "input": [
                    {
                        "content": {"type": "pdf", "format": "base64", "value": "%s"}
                    },
                    {
                        "content": {"type": "image", "format": "base64", "value": "%s"}
                    }
                ]
            }
            """, TEST_DATA_URI, TEST_DATA_URI);
        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            var request = DocumentExtractionRequest.PARSER.apply(parser, null);
            assertThat(
                request.inputs(),
                is(
                    List.of(
                        new InferenceString(DataType.PDF, DataFormat.BASE64, TEST_DATA_URI),
                        new InferenceString(DataType.IMAGE, DataFormat.BASE64, TEST_DATA_URI)
                    )
                )
            );
            assertThat(request.taskSettings(), anEmptyMap());
        }
    }

    public void testParser_WithUnspecifiedFormat_UsesDefault() throws IOException {
        var requestJson = Strings.format("""
            {
                "input": [
                    {
                        "content": {"type": "pdf", "value": "%s"}
                    }
                ]
            }
            """, TEST_DATA_URI);
        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            var request = DocumentExtractionRequest.PARSER.apply(parser, null);
            assertThat(request.inputs(), is(List.of(new InferenceString(DataType.PDF, DataFormat.BASE64, TEST_DATA_URI))));
        }
    }

    public void testParser_WithTaskSettings() throws IOException {
        var requestJson = Strings.format("""
            {
                "input": [
                    {
                        "content": {"type": "pdf", "format": "base64", "value": "%s"}
                    }
                ],
                "task_settings": {
                    "output_format": "markdown"
                }
            }
            """, TEST_DATA_URI);
        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            var request = DocumentExtractionRequest.PARSER.apply(parser, null);
            assertThat(request.inputs(), is(List.of(new InferenceString(DataType.PDF, DataFormat.BASE64, TEST_DATA_URI))));
            assertThat(request.taskSettings(), is(Map.of("output_format", "markdown")));
        }
    }

    public void testParser_WithEmptyTaskSettings() throws IOException {
        var requestJson = Strings.format("""
            {
                "input": [
                    {
                        "content": {"type": "pdf", "format": "base64", "value": "%s"}
                    }
                ],
                "task_settings": {}
            }
            """, TEST_DATA_URI);
        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            var request = DocumentExtractionRequest.PARSER.apply(parser, null);
            assertThat(request.taskSettings(), anEmptyMap());
        }
    }

    public void testParser_WithUnsupportedDataType_Throws() throws IOException {
        var unsupportedDataType = randomFrom(EnumSet.complementOf(SUPPORTED_DOCUMENT_EXTRACTION_DATA_TYPES));
        var value = InferenceStringTests.convertToDataURIIfNeeded(unsupportedDataType, null, randomAlphanumericOfLength(10));
        var requestJson = Strings.format("""
            {
                "input": [
                    {
                        "content": {"type": "%s", "value": "%s"}
                    }
                ]
            }
            """, unsupportedDataType, value);
        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            var exception = expectThrows(XContentParseException.class, () -> DocumentExtractionRequest.PARSER.apply(parser, null));
            assertThat(exception.getMessage(), containsString("failed to parse field [input]"));
            Throwable rootCause = exception;
            while (rootCause.getCause() != null) {
                rootCause = rootCause.getCause();
            }
            assertThat(
                rootCause.getMessage(),
                containsString(
                    Strings.format(
                        "Field [content] contains unsupported [type] value [%s]. Supported values are [image, pdf]",
                        unsupportedDataType
                    )
                )
            );
        }
    }

    public void testParser_WithMissingContentField_Throws() throws IOException {
        var requestJson = """
            {
                "input": [
                    {}
                ]
            }
            """;
        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            var exception = expectThrows(XContentParseException.class, () -> DocumentExtractionRequest.PARSER.apply(parser, null));
            assertThat(exception.getMessage(), containsString("failed to parse field [input]"));
        }
    }

    public void testParser_WithStringInput_Throws() throws IOException {
        var requestJson = """
            {
                "input": ["some text input"]
            }
            """;
        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            expectThrows(XContentParseException.class, () -> DocumentExtractionRequest.PARSER.apply(parser, null));
        }
    }

    @Override
    protected Writeable.Reader<DocumentExtractionRequest> instanceReader() {
        return DocumentExtractionRequest::new;
    }

    @Override
    protected DocumentExtractionRequest createTestInstance() {
        return createRandom();
    }

    public static DocumentExtractionRequest createRandom() {
        return new DocumentExtractionRequest(randomInputs(), Map.of(randomAlphanumericOfLength(8), randomAlphanumericOfLength(8)));
    }

    private static List<InferenceString> randomInputs() {
        var contents = new ArrayList<InferenceString>();
        for (int i = 0; i < randomIntBetween(1, 5); ++i) {
            contents.add(getRandomSupportedInferenceString());
        }
        return contents;
    }

    public static InferenceString getRandomSupportedInferenceString() {
        return InferenceStringTests.createRandomUsingDataTypes(SUPPORTED_DOCUMENT_EXTRACTION_DATA_TYPES);
    }

    @Override
    protected DocumentExtractionRequest mutateInstance(DocumentExtractionRequest instance) throws IOException {
        var inputs = instance.inputs();
        var taskSettings = instance.taskSettings();
        switch (randomInt(1)) {
            case 0 -> inputs = randomValueOtherThan(inputs, DocumentExtractionRequestTests::randomInputs);
            case 1 -> taskSettings = randomValueOtherThan(
                taskSettings,
                () -> Map.of(randomAlphanumericOfLength(8), randomAlphanumericOfLength(8))
            );
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new DocumentExtractionRequest(inputs, taskSettings);
    }
}
