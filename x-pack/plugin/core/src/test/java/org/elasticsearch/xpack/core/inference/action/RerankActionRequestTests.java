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
import org.elasticsearch.inference.DataType;
import org.elasticsearch.inference.InferenceString;
import org.elasticsearch.inference.InferenceStringTests;
import org.elasticsearch.inference.RerankRequest;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.inference.InferenceContext;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.inference.RerankRequest.SUPPORTED_RERANK_DATA_TYPES;
import static org.elasticsearch.xpack.core.inference.action.BaseInferenceActionRequest.TIMEOUT_NOT_DETERMINED;
import static org.elasticsearch.xpack.core.inference.action.RerankAction.Request.parseRequest;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class RerankActionRequestTests extends AbstractBWCWireSerializationTestCase<RerankAction.Request> {
    private static final TransportVersion INFERENCE_CONTEXT = TransportVersion.fromName("inference_context");

    public void testConstructor_WithNullTimeout_UsesPlaceholder() {
        var request = new RerankAction.Request(randomAlphanumericOfLength(8), randomRerankRequest(), null);
        assertThat(request.getTimeout(), is(TIMEOUT_NOT_DETERMINED));
    }

    public void testConstructor_WithNonNullTimeout_UsesTimeout() {
        var timeout = randomTimeValue();
        var request = new RerankAction.Request(randomAlphanumericOfLength(8), randomRerankRequest(), timeout);
        assertThat(request.getTimeout(), is(timeout));
    }

    public void testParseRequest() throws IOException {
        var firstInputText = randomAlphanumericOfLength(8);
        var secondInputText = randomAlphanumericOfLength(8);
        var queryText = randomAlphanumericOfLength(8);
        var topN = randomInt();
        var returnDocuments = randomBoolean();
        var requestJson = Strings.format("""
            {
                "input": [
                    {"type": "text", "format": "text", "value": "%s"},
                    {"type": "text", "format": "text", "value": "%s"}
                ],
                "query": {"type": "text", "format": "text", "value": "%s"},
                "top_n": %d,
                "return_documents": %b,
                "task_settings": {
                  "field": "value"
                }
            }
            """, firstInputText, secondInputText, queryText, topN, returnDocuments);
        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            var inferenceId = randomAlphanumericOfLength(8);
            var context = new InferenceContext(randomAlphaOfLength(10));
            var timeout = TimeValue.timeValueMillis(randomLongBetween(1, 2048));

            var expectedRequest = new RerankAction.Request(
                inferenceId,
                new RerankRequest(
                    List.of(new InferenceString(DataType.TEXT, firstInputText), new InferenceString(DataType.TEXT, secondInputText)),
                    new InferenceString(DataType.TEXT, queryText),
                    topN,
                    returnDocuments,
                    Map.of("field", "value")
                ),
                context,
                timeout
            );

            var parsedRequest = parseRequest(inferenceId, timeout, context, parser);

            assertThat(parsedRequest, is(expectedRequest));
        }
    }

    public void testIsStreaming_ReturnsFalse() {
        assertThat(createRandom().isStreaming(), is(false));
    }

    public void testValidate_WithNullInputs_ReturnsValidationException() {
        var request = new RerankAction.Request(
            randomAlphanumericOfLength(8),
            new RerankRequest(null, randomInferenceString(), null, null, Map.of()),
            new InferenceContext(randomAlphaOfLength(10)),
            TimeValue.timeValueMillis(randomLongBetween(1, 2048))
        );

        var validationException = request.validate();
        assertThat(validationException.validationErrors(), hasSize(1));
        assertThat(validationException.validationErrors().getFirst(), is("Field [input] cannot be null"));
    }

    public void testValidate_WithEmptyInputs_ReturnsValidationException() {
        var request = new RerankAction.Request(
            randomAlphanumericOfLength(8),
            new RerankRequest(List.of(), randomInferenceString(), null, null, Map.of()),
            new InferenceContext(randomAlphaOfLength(10)),
            TimeValue.timeValueMillis(randomLongBetween(1, 2048))
        );

        var validationException = request.validate();
        assertThat(validationException.validationErrors(), hasSize(1));
        assertThat(validationException.validationErrors().getFirst(), is("Field [input] cannot be an empty array"));
    }

    public void testValidate_WithUnsupportedInputDataType_ReturnsValidationException() {
        var unsupportedDataTypes = EnumSet.complementOf(SUPPORTED_RERANK_DATA_TYPES);
        var inputWithUnsupportedType = InferenceStringTests.createRandomUsingDataTypes(unsupportedDataTypes);
        var request = new RerankAction.Request(
            randomAlphanumericOfLength(8),
            new RerankRequest(List.of(randomInferenceString(), inputWithUnsupportedType), randomInferenceString(), null, null, Map.of()),
            new InferenceContext(randomAlphaOfLength(10)),
            TimeValue.timeValueMillis(randomLongBetween(1, 2048))
        );

        var validationException = request.validate();
        assertThat(validationException.validationErrors(), hasSize(1));
        assertThat(
            validationException.validationErrors().getFirst(),
            is(Strings.format("Field [input] contains unsupported [type] value %s at index 1", inputWithUnsupportedType.dataType()))
        );
    }

    public void testValidate_WithNullQuery_ReturnsValidationException() {
        var request = new RerankAction.Request(
            randomAlphanumericOfLength(8),
            new RerankRequest(List.of(randomInferenceString()), null, null, null, Map.of()),
            new InferenceContext(randomAlphaOfLength(10)),
            TimeValue.timeValueMillis(randomLongBetween(1, 2048))
        );

        var validationException = request.validate();
        assertThat(validationException.validationErrors(), hasSize(1));
        assertThat(validationException.validationErrors().getFirst(), is("Field [query] cannot be null"));
    }

    public void testValidate_WithEmptyQuery_ReturnsValidationException() {
        var request = new RerankAction.Request(
            randomAlphanumericOfLength(8),
            new RerankRequest(List.of(randomInferenceString()), new InferenceString(DataType.TEXT, ""), null, null, Map.of()),
            new InferenceContext(randomAlphaOfLength(10)),
            TimeValue.timeValueMillis(randomLongBetween(1, 2048))
        );

        var validationException = request.validate();
        assertThat(validationException.validationErrors(), hasSize(1));
        assertThat(validationException.validationErrors().getFirst(), is("Field [query] cannot be empty"));
    }

    public void testValidate_WithUnsupportedQueryDataType_ReturnsValidationException() {
        var unsupportedDataTypes = EnumSet.complementOf(SUPPORTED_RERANK_DATA_TYPES);
        var inputWithUnsupportedType = InferenceStringTests.createRandomUsingDataTypes(unsupportedDataTypes);
        var request = new RerankAction.Request(
            randomAlphanumericOfLength(8),
            new RerankRequest(List.of(randomInferenceString()), inputWithUnsupportedType, null, null, Map.of()),
            new InferenceContext(randomAlphaOfLength(10)),
            TimeValue.timeValueMillis(randomLongBetween(1, 2048))
        );

        var validationException = request.validate();
        assertThat(validationException.validationErrors(), hasSize(1));
        assertThat(
            validationException.validationErrors().getFirst(),
            is(Strings.format("Field [query] contains unsupported [type] value %s", inputWithUnsupportedType.dataType()))
        );
    }

    public void testValidate_WithInvalidTopN_ReturnsValidationException() {
        var invalidTopN = randomFrom(Integer.MIN_VALUE, -1, 0);
        var request = new RerankAction.Request(
            randomAlphanumericOfLength(8),
            new RerankRequest(List.of(randomInferenceString()), randomInferenceString(), invalidTopN, null, Map.of()),
            new InferenceContext(randomAlphaOfLength(10)),
            TimeValue.timeValueMillis(randomLongBetween(1, 2048))
        );

        var validationException = request.validate();
        assertThat(validationException.validationErrors(), hasSize(1));
        assertThat(validationException.validationErrors().getFirst(), is("Field [top_n] must be greater than or equal to 1"));
    }

    public void testValidate_WithMultipleValidationErrors_ReturnsAll() {
        // Create a request with empty inputs, null query and invalid top_n
        var invalidTopN = randomFrom(Integer.MIN_VALUE, -1, 0);
        var request = new RerankAction.Request(
            randomAlphanumericOfLength(8),
            new RerankRequest(List.of(), null, invalidTopN, null, Map.of()),
            new InferenceContext(randomAlphaOfLength(10)),
            TimeValue.timeValueMillis(randomLongBetween(1, 2048))
        );

        var validationException = request.validate();
        assertThat(
            validationException.validationErrors(),
            containsInAnyOrder(
                "Field [input] cannot be an empty array",
                "Field [query] cannot be null",
                "Field [top_n] must be greater than or equal to 1"
            )
        );
    }

    /**
     * Returns a random {@link InferenceString} with a {@link DataType} that is supported by the {@link TaskType#RERANK} task
     */
    private static InferenceString randomInferenceString() {
        return InferenceStringTests.createRandomUsingDataTypes(SUPPORTED_RERANK_DATA_TYPES);
    }

    @Override
    protected RerankAction.Request mutateInstanceForVersion(RerankAction.Request instance, TransportVersion version) {
        var context = instance.getContext();
        if (version.supports(INFERENCE_CONTEXT) == false) {
            context = InferenceContext.EMPTY_INSTANCE;
        }
        return new RerankAction.Request(instance.getInferenceEntityId(), instance.getRerankRequest(), context, instance.getTimeout());
    }

    @Override
    protected Writeable.Reader<RerankAction.Request> instanceReader() {
        return RerankAction.Request::new;
    }

    @Override
    protected RerankAction.Request createTestInstance() {
        return createRandom();
    }

    public static RerankAction.Request createRandom() {
        var inferenceId = randomAlphanumericOfLength(8);
        var embeddingRequest = randomRerankRequest();
        var context = new InferenceContext(randomAlphaOfLength(10));
        var timeout = randomTimeValue();
        return new RerankAction.Request(inferenceId, embeddingRequest, context, timeout);
    }

    private static RerankRequest randomRerankRequest() {
        return new RerankRequest(
            List.of(new InferenceString(DataType.TEXT, randomAlphanumericOfLength(8))),
            new InferenceString(DataType.TEXT, randomAlphanumericOfLength(8)),
            randomInt(),
            randomBoolean(),
            Map.of(randomAlphanumericOfLength(8), randomAlphanumericOfLength(8))
        );
    }

    @Override
    protected RerankAction.Request mutateInstance(RerankAction.Request instance) throws IOException {
        var inferenceId = instance.getInferenceEntityId();
        var rerankRequest = instance.getRerankRequest();
        var context = instance.getContext();
        var timeout = instance.getTimeout();
        switch (between(0, 3)) {
            case 0 -> inferenceId = randomValueOtherThan(inferenceId, () -> randomAlphaOfLength(8));
            case 1 -> rerankRequest = randomValueOtherThan(rerankRequest, RerankActionRequestTests::randomRerankRequest);
            case 2 -> context = randomValueOtherThan(context, () -> new InferenceContext(randomAlphaOfLength(10)));
            case 3 -> timeout = randomValueOtherThan(timeout, ESTestCase::randomTimeValue);
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new RerankAction.Request(inferenceId, rerankRequest, context, timeout);
    }
}
