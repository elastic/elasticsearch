/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.DataFormat;
import org.elasticsearch.inference.DataType;
import org.elasticsearch.inference.DocumentExtractionRequest;
import org.elasticsearch.inference.DocumentExtractionRequestTests;
import org.elasticsearch.inference.InferenceString;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.inference.InferenceContext;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.inference.InferenceString.EMBEDDING_AUDIO_VIDEO_PDF_INPUT_SUPPORT_ADDED;
import static org.elasticsearch.inference.InferenceString.URL_INPUT_FORMAT_SUPPORT_ADDED;
import static org.elasticsearch.inference.InferenceStringTests.TEST_DATA_URI;
import static org.elasticsearch.xpack.core.inference.action.BaseInferenceActionRequest.TIMEOUT_NOT_DETERMINED;
import static org.elasticsearch.xpack.core.inference.action.DocumentExtractionAction.Request.parseRequest;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class DocumentExtractionActionRequestTests extends AbstractBWCWireSerializationTestCase<DocumentExtractionAction.Request> {

    public void testConstructor_WithNullTimeout_UsesPlaceholder() {
        var request = new DocumentExtractionAction.Request(randomAlphanumericOfLength(8), randomDocumentExtractionRequest(), null);
        assertThat(request.getTimeout(), is(TIMEOUT_NOT_DETERMINED));
    }

    public void testConstructor_WithNonNullTimeout_UsesTimeout() {
        var timeout = randomTimeValue();
        var request = new DocumentExtractionAction.Request(randomAlphanumericOfLength(8), randomDocumentExtractionRequest(), timeout);
        assertThat(request.getTimeout(), is(timeout));
    }

    public void testGetTaskType_ReturnsDocumentExtraction() {
        assertThat(createRandom().getTaskType(), is(TaskType.DOCUMENT_EXTRACTION));
    }

    public void testParseRequest() throws IOException {
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
            var inferenceId = randomAlphanumericOfLength(8);
            var context = new InferenceContext(randomAlphaOfLength(10));
            var timeout = TimeValue.timeValueMillis(randomLongBetween(1, 2048));

            var expectedRequest = new DocumentExtractionAction.Request(
                inferenceId,
                new DocumentExtractionRequest(
                    List.of(new InferenceString(DataType.PDF, DataFormat.BASE64, TEST_DATA_URI)),
                    Map.of("output_format", "markdown")
                ),
                context,
                timeout
            );

            var parsedRequest = parseRequest(inferenceId, timeout, context, parser);

            assertThat(parsedRequest, is(expectedRequest));
        }
    }

    public void testIsStreaming_returnsFalse() {
        assertThat(createRandom().isStreaming(), is(false));
    }

    public void testValidate_withEmptyInputs_returnsValidationException() {
        var request = new DocumentExtractionAction.Request(
            randomAlphanumericOfLength(8),
            new DocumentExtractionRequest(List.of(), Map.of()),
            new InferenceContext(randomAlphaOfLength(10)),
            TimeValue.timeValueMillis(randomLongBetween(1, 2048))
        );

        var validationException = request.validate();
        assertThat(validationException.validationErrors(), hasSize(1));
        assertThat(validationException.validationErrors().getFirst(), is("Field [input] cannot be an empty array"));
    }

    public void testValidate_withValidRequest_returnsNull() {
        assertThat(createRandom().validate(), is(nullValue()));
    }

    /**
     * Versions before {@link InferenceString#EMBEDDING_AUDIO_VIDEO_PDF_INPUT_SUPPORT_ADDED} throw an exception when serializing pdf
     * inputs, which every document extraction request may carry, and versions before
     * {@link InferenceString#URL_INPUT_FORMAT_SUPPORT_ADDED} throw an exception when serializing URL-format inputs, so we filter those
     * out of the bwc versions to avoid test failures. Both guards live in {@link InferenceString} and are tested directly by
     * {@link DocumentExtractionRequestTests}.
     */
    @Override
    protected Collection<TransportVersion> bwcVersions() {
        return super.bwcVersions().stream()
            .filter(version -> version.supports(EMBEDDING_AUDIO_VIDEO_PDF_INPUT_SUPPORT_ADDED))
            .filter(version -> version.supports(URL_INPUT_FORMAT_SUPPORT_ADDED))
            .toList();
    }

    @Override
    protected DocumentExtractionAction.Request mutateInstanceForVersion(
        DocumentExtractionAction.Request instance,
        TransportVersion version
    ) {
        return instance;
    }

    @Override
    protected Writeable.Reader<DocumentExtractionAction.Request> instanceReader() {
        return DocumentExtractionAction.Request::new;
    }

    @Override
    protected DocumentExtractionAction.Request createTestInstance() {
        return createRandom();
    }

    public static DocumentExtractionAction.Request createRandom() {
        var inferenceId = randomAlphanumericOfLength(8);
        var documentExtractionRequest = randomDocumentExtractionRequest();
        var context = new InferenceContext(randomAlphaOfLength(10));
        var timeout = randomFrom(randomTimeValue(), null);
        return new DocumentExtractionAction.Request(inferenceId, documentExtractionRequest, context, timeout);
    }

    private static DocumentExtractionRequest randomDocumentExtractionRequest() {
        return DocumentExtractionRequestTests.createRandom();
    }

    @Override
    protected DocumentExtractionAction.Request mutateInstance(DocumentExtractionAction.Request instance) throws IOException {
        var inferenceId = instance.getInferenceEntityId();
        var documentExtractionRequest = instance.getDocumentExtractionRequest();
        var context = instance.getContext();
        var timeout = instance.getTimeout();
        switch (between(0, 3)) {
            case 0 -> inferenceId = randomValueOtherThan(inferenceId, () -> randomAlphaOfLength(8));
            case 1 -> documentExtractionRequest = randomValueOtherThan(
                documentExtractionRequest,
                DocumentExtractionActionRequestTests::randomDocumentExtractionRequest
            );
            case 2 -> context = randomValueOtherThan(context, () -> new InferenceContext(randomAlphaOfLength(10)));
            case 3 -> {
                if (timeout.equals(TIMEOUT_NOT_DETERMINED)) {
                    // Using null as timeout will translate it internally to TIMEOUT_NOT_DETERMINED, which would not mutate the instance
                    timeout = randomValueOtherThan(timeout, ESTestCase::randomTimeValue);
                } else {
                    timeout = randomValueOtherThan(timeout, () -> randomFrom(randomTimeValue(), null));
                }
            }
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new DocumentExtractionAction.Request(inferenceId, documentExtractionRequest, context, timeout);
    }
}
