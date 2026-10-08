/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.AbstractBWCSerializationTestCase;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.inference.DocumentExtractionRequest.SUPPORTED_DOCUMENT_EXTRACTION_DATA_TYPES;
import static org.elasticsearch.inference.InferenceString.EMBEDDING_AUDIO_VIDEO_PDF_INPUT_SUPPORT_ADDED;
import static org.elasticsearch.inference.InferenceString.URL_INPUT_FORMAT_SUPPORT_ADDED;
import static org.elasticsearch.inference.InferenceStringTests.TEST_DATA_URI;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class DocumentExtractionRequestTests extends AbstractBWCSerializationTestCase<DocumentExtractionRequest> {

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

    public void testParser_WithUnsupportedDataType_Throws() throws IOException {
        var unsupportedDataType = randomFrom(EnumSet.complementOf(EnumSet.copyOf(SUPPORTED_DOCUMENT_EXTRACTION_DATA_TYPES)));
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
            Throwable rootCause = exception;
            while (rootCause.getCause() != null) {
                rootCause = rootCause.getCause();
            }
            assertThat(rootCause, instanceOf(IllegalArgumentException.class));
            assertThat(rootCause.getMessage(), is(unsupportedDataTypeMessage(unsupportedDataType, 0)));
        }
    }

    public void testConstructor_WithNullInputs_Throws() {
        expectThrows(NullPointerException.class, () -> new DocumentExtractionRequest(null, Map.of()));
    }

    public void testConstructor_WithUnsupportedDataType_Throws() {
        var unsupportedDataType = randomFrom(EnumSet.complementOf(EnumSet.copyOf(SUPPORTED_DOCUMENT_EXTRACTION_DATA_TYPES)));
        var unsupportedInput = InferenceStringTests.createRandomUsingDataTypes(EnumSet.of(unsupportedDataType));
        var inputs = List.of(getRandomSupportedInferenceString(), unsupportedInput);

        var exception = expectThrows(IllegalArgumentException.class, () -> new DocumentExtractionRequest(inputs, Map.of()));
        assertThat(exception.getMessage(), is(unsupportedDataTypeMessage(unsupportedDataType, 1)));
    }

    private static String unsupportedDataTypeMessage(DataType dataType, int index) {
        return Strings.format(
            "Field [input] contains unsupported [type] value [%s] at index [%d]. Supported values are [image, pdf]",
            dataType,
            index
        );
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

    /**
     * Versions before {@link InferenceString#EMBEDDING_AUDIO_VIDEO_PDF_INPUT_SUPPORT_ADDED} throw an exception when serializing pdf
     * inputs, which every document extraction request may carry, and versions before
     * {@link InferenceString#URL_INPUT_FORMAT_SUPPORT_ADDED} throw an exception when serializing URL-format inputs, so we filter those
     * out of the bwc versions to avoid test failures. The URL-format guard is tested directly by
     * {@link #testUrlFormatIsNotBackwardsCompatible}.
     */
    @Override
    protected Collection<TransportVersion> bwcVersions() {
        return super.bwcVersions().stream()
            .filter(version -> version.supports(EMBEDDING_AUDIO_VIDEO_PDF_INPUT_SUPPORT_ADDED))
            .filter(version -> version.supports(URL_INPUT_FORMAT_SUPPORT_ADDED))
            .toList();
    }

    /**
     * Verifies that URL-format inputs cannot be sent to nodes that do not support {@link InferenceString#URL_INPUT_FORMAT_SUPPORT_ADDED}.
     * An {@link DataType#IMAGE} input is used since it pre-dates the audio/video/pdf gate, so the URL-specific error is always the one
     * raised on any pre-URL node.
     */
    public void testUrlFormatIsNotBackwardsCompatible() throws IOException {
        var urlRequest = DocumentExtractionRequest.of(
            List.of(new InferenceString(DataType.IMAGE, DataFormat.URL, "https://example.com/document.png"))
        );
        var preUrlVersions = super.bwcVersions().stream().filter(v -> v.supports(URL_INPUT_FORMAT_SUPPORT_ADDED) == false).toList();

        assertRequestNotBackwardsCompatible(preUrlVersions, urlRequest);
    }

    @Override
    protected DocumentExtractionRequest mutateInstanceForVersion(DocumentExtractionRequest instance, TransportVersion version) {
        return instance;
    }

    @Override
    protected DocumentExtractionRequest doParseInstance(XContentParser parser) throws IOException {
        return DocumentExtractionRequest.PARSER.parse(parser, null);
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
        return new DocumentExtractionRequest(randomInputs(), randomTaskSettings());
    }

    private static Map<String, Object> randomTaskSettings() {
        return randomMap(0, 3, () -> new Tuple<String, Object>(randomAlphanumericOfLength(8), randomAlphanumericOfLength(8)));
    }

    private void assertRequestNotBackwardsCompatible(List<TransportVersion> preUrlVersions, DocumentExtractionRequest urlRequest) {
        for (var version : preUrlVersions) {
            var ex = expectThrows(
                ElasticsearchStatusException.class,
                () -> copyWriteable(urlRequest, getNamedWriteableRegistry(), instanceReader(), version)
            );
            assertThat(ex.status(), is(RestStatus.BAD_REQUEST));
            assertThat(
                ex.getMessage(),
                is(
                    "Cannot send an inference request with URL format inputs to an older node. "
                        + "Please wait until all nodes are upgraded before using URL format inputs"
                )
            );
        }
    }

    private static List<InferenceString> randomInputs() {
        var contents = new ArrayList<InferenceString>();
        for (int i = 0; i < randomIntBetween(1, 5); ++i) {
            contents.add(getRandomSupportedInferenceString());
        }
        return contents;
    }

    public static InferenceString getRandomSupportedInferenceString() {
        return InferenceStringTests.createRandomUsingDataTypes(EnumSet.copyOf(SUPPORTED_DOCUMENT_EXTRACTION_DATA_TYPES));
    }

    @Override
    protected DocumentExtractionRequest mutateInstance(DocumentExtractionRequest instance) throws IOException {
        var inputs = instance.inputs();
        var taskSettings = instance.taskSettings();
        switch (randomInt(1)) {
            case 0 -> inputs = randomValueOtherThan(inputs, DocumentExtractionRequestTests::randomInputs);
            case 1 -> taskSettings = randomValueOtherThan(taskSettings, DocumentExtractionRequestTests::randomTaskSettings);
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new DocumentExtractionRequest(inputs, taskSettings);
    }
}
