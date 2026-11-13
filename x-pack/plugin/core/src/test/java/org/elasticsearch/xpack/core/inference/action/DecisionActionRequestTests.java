/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.DecisionRequest;
import org.elasticsearch.inference.DecisionRequestTests;
import org.elasticsearch.inference.InferenceString;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.inference.InferenceContext;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import static org.elasticsearch.inference.InferenceString.URL_INPUT_FORMAT_SUPPORT_ADDED;
import static org.elasticsearch.xpack.core.inference.action.BaseInferenceActionRequest.TIMEOUT_NOT_DETERMINED;
import static org.elasticsearch.xpack.core.inference.action.DecisionAction.Request.parseRequest;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class DecisionActionRequestTests extends AbstractBWCWireSerializationTestCase<DecisionAction.Request> {

    public void testConstructor_WithNullTimeout_UsesPlaceholder() {
        var request = new DecisionAction.Request(randomAlphanumericOfLength(8), randomDecisionRequest(), null);
        assertThat(request.getTimeout(), is(TIMEOUT_NOT_DETERMINED));
    }

    public void testConstructor_WithNonNullTimeout_UsesTimeout() {
        var timeout = randomTimeValue();
        var request = new DecisionAction.Request(randomAlphanumericOfLength(8), randomDecisionRequest(), timeout);
        assertThat(request.getTimeout(), is(timeout));
    }

    public void testGetTaskType_ReturnsDecision() {
        assertThat(createRandom().getTaskType(), is(TaskType.DECISION));
    }

    public void testParseRequest() throws IOException {
        var requestJson = """
            {
                "input": "I was charged twice for my order.",
                "task_settings": {
                    "questions": {
                        "is_urgent": {"type": "predicate", "instructions": "Does this need a response within the hour?"}
                    }
                }
            }
            """;
        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            var inferenceId = randomAlphanumericOfLength(8);
            var context = new InferenceContext(randomAlphaOfLength(10));
            var timeout = TimeValue.timeValueMillis(randomLongBetween(1, 2048));

            var expectedRequest = new DecisionAction.Request(
                inferenceId,
                new DecisionRequest(
                    InferenceString.ofText("I was charged twice for my order."),
                    Map.of(
                        "questions",
                        Map.of("is_urgent", Map.of("type", "predicate", "instructions", "Does this need a response within the hour?"))
                    )
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

    public void testValidate_WithBlankInput_ReturnsValidationException() {
        var request = new DecisionAction.Request(
            randomAlphanumericOfLength(8),
            DecisionRequest.of(randomFrom("", " ")),
            new InferenceContext(randomAlphaOfLength(10)),
            TimeValue.timeValueMillis(randomLongBetween(1, 2048))
        );

        var validationException = request.validate();
        assertThat(validationException.validationErrors(), hasSize(1));
        assertThat(validationException.validationErrors().getFirst(), is("Field [input] cannot be empty"));
    }

    public void testValidate_WithValidRequest_ReturnsNull() {
        assertThat(createRandom().validate(), is(nullValue()));
    }

    /**
     * Versions before {@link InferenceString#URL_INPUT_FORMAT_SUPPORT_ADDED} throw an exception when serializing URL-format inputs, so
     * we filter those out of the bwc versions to avoid test failures. The guard lives in {@link InferenceString} and is tested there.
     */
    @Override
    protected Collection<TransportVersion> bwcVersions() {
        return super.bwcVersions().stream().filter(version -> version.supports(URL_INPUT_FORMAT_SUPPORT_ADDED)).toList();
    }

    @Override
    protected DecisionAction.Request mutateInstanceForVersion(DecisionAction.Request instance, TransportVersion version) {
        return instance;
    }

    @Override
    protected Writeable.Reader<DecisionAction.Request> instanceReader() {
        return DecisionAction.Request::new;
    }

    @Override
    protected DecisionAction.Request createTestInstance() {
        return createRandom();
    }

    public static DecisionAction.Request createRandom() {
        var inferenceId = randomAlphanumericOfLength(8);
        var context = new InferenceContext(randomAlphaOfLength(10));
        var timeout = randomFrom(randomTimeValue(), null);
        return new DecisionAction.Request(inferenceId, randomDecisionRequest(), context, timeout);
    }

    private static DecisionRequest randomDecisionRequest() {
        return DecisionRequestTests.createRandom();
    }

    @Override
    protected DecisionAction.Request mutateInstance(DecisionAction.Request instance) throws IOException {
        var inferenceId = instance.getInferenceEntityId();
        var decisionRequest = instance.getDecisionRequest();
        var context = instance.getContext();
        var timeout = instance.getTimeout();
        switch (between(0, 3)) {
            case 0 -> inferenceId = randomValueOtherThan(inferenceId, () -> randomAlphaOfLength(8));
            case 1 -> decisionRequest = randomValueOtherThan(decisionRequest, DecisionActionRequestTests::randomDecisionRequest);
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
        return new DecisionAction.Request(inferenceId, decisionRequest, context, timeout);
    }
}
