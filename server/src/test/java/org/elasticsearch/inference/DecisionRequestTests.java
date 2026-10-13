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
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.AbstractBWCSerializationTestCase;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Map;

import static org.elasticsearch.inference.DecisionRequest.SUPPORTED_DECISION_DATA_TYPES;
import static org.elasticsearch.inference.InferenceString.URL_INPUT_FORMAT_SUPPORT_ADDED;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class DecisionRequestTests extends AbstractBWCSerializationTestCase<DecisionRequest> {

    public void testParser_WithStringInput_UsesTextInput() throws IOException {
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
            var request = DecisionRequest.PARSER.apply(parser, null);
            assertThat(request.input(), is(InferenceString.ofText("I was charged twice for my order.")));
            assertThat(
                request.taskSettings(),
                is(
                    Map.of(
                        "questions",
                        Map.of("is_urgent", Map.of("type", "predicate", "instructions", "Does this need a response within the hour?"))
                    )
                )
            );
        }
    }

    public void testParser_WithObjectInput_UsesInferenceString() throws IOException {
        var requestJson = """
            {
                "input": {"type": "image", "format": "base64", "value": "data:image/png;base64,abcd"}
            }
            """;
        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            var request = DecisionRequest.PARSER.apply(parser, null);
            assertThat(request.input(), is(new InferenceString(DataType.IMAGE, DataFormat.BASE64, "data:image/png;base64,abcd")));
            assertThat(request.taskSettings(), is(Map.of()));
        }
    }

    public void testParser_WithUnsupportedDataType_Throws() throws IOException {
        var unsupportedDataType = randomFrom(EnumSet.complementOf(EnumSet.copyOf(SUPPORTED_DECISION_DATA_TYPES)));
        var value = InferenceStringTests.convertToDataURIIfNeeded(unsupportedDataType, null, randomAlphanumericOfLength(10));
        var requestJson = Strings.format("""
            {
                "input": {"type": "%s", "value": "%s"}
            }
            """, unsupportedDataType, value);
        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            var exception = expectThrows(XContentParseException.class, () -> DecisionRequest.PARSER.apply(parser, null));
            Throwable rootCause = exception;
            while (rootCause.getCause() != null) {
                rootCause = rootCause.getCause();
            }
            assertThat(rootCause, instanceOf(IllegalArgumentException.class));
            assertThat(rootCause.getMessage(), is(unsupportedDataTypeMessage(unsupportedDataType)));
        }
    }

    public void testParser_WithArrayInput_Throws() throws IOException {
        var requestJson = """
            {
                "input": ["first input", "second input"]
            }
            """;
        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            expectThrows(XContentParseException.class, () -> DecisionRequest.PARSER.apply(parser, null));
        }
    }

    public void testParser_WithMissingInput_Throws() throws IOException {
        var requestJson = """
            {
                "task_settings": {}
            }
            """;
        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            var exception = expectThrows(IllegalArgumentException.class, () -> DecisionRequest.PARSER.apply(parser, null));
            assertThat(exception.getMessage(), containsString("Required [input]"));
        }
    }

    public void testConstructor_WithNullInput_Throws() {
        expectThrows(NullPointerException.class, () -> new DecisionRequest(null, Map.of()));
    }

    public void testConstructor_WithUnsupportedDataType_Throws() {
        var unsupportedDataType = randomFrom(EnumSet.complementOf(EnumSet.copyOf(SUPPORTED_DECISION_DATA_TYPES)));
        var unsupportedInput = InferenceStringTests.createRandomUsingDataTypes(EnumSet.of(unsupportedDataType));

        var exception = expectThrows(IllegalArgumentException.class, () -> new DecisionRequest(unsupportedInput, Map.of()));
        assertThat(exception.getMessage(), is(unsupportedDataTypeMessage(unsupportedDataType)));
    }

    public void testConstructor_WithNullTaskSettings_UsesMutableEmptyMap() {
        var request = DecisionRequest.of(randomAlphanumericOfLength(8));
        assertThat(request.taskSettings(), is(Map.of()));
        request.taskSettings().put("key", "value");
    }

    private static String unsupportedDataTypeMessage(DataType dataType) {
        return Strings.format("Field [input] contains unsupported [type] value [%s]. Supported values are [text, image]", dataType);
    }

    /**
     * Versions before {@link InferenceString#URL_INPUT_FORMAT_SUPPORT_ADDED} throw an exception when serializing URL-format inputs, so
     * we filter those out of the bwc versions to avoid test failures. The URL-format guard lives in {@link InferenceString} and is tested
     * by {@link InferenceStringTests}.
     */
    @Override
    protected Collection<TransportVersion> bwcVersions() {
        return super.bwcVersions().stream().filter(version -> version.supports(URL_INPUT_FORMAT_SUPPORT_ADDED)).toList();
    }

    @Override
    protected DecisionRequest mutateInstanceForVersion(DecisionRequest instance, TransportVersion version) {
        return instance;
    }

    @Override
    protected DecisionRequest doParseInstance(XContentParser parser) throws IOException {
        return DecisionRequest.PARSER.parse(parser, null);
    }

    @Override
    protected Writeable.Reader<DecisionRequest> instanceReader() {
        return DecisionRequest::new;
    }

    @Override
    protected DecisionRequest createTestInstance() {
        return createRandom();
    }

    public static DecisionRequest createRandom() {
        return new DecisionRequest(randomInput(), randomTaskSettings());
    }

    private static InferenceString randomInput() {
        return InferenceStringTests.createRandomUsingDataTypes(EnumSet.copyOf(SUPPORTED_DECISION_DATA_TYPES));
    }

    private static Map<String, Object> randomTaskSettings() {
        return randomMap(0, 3, () -> new Tuple<String, Object>(randomAlphanumericOfLength(8), randomAlphanumericOfLength(8)));
    }

    @Override
    protected DecisionRequest mutateInstance(DecisionRequest instance) throws IOException {
        var input = instance.input();
        var taskSettings = instance.taskSettings();
        switch (randomInt(1)) {
            case 0 -> input = randomValueOtherThan(input, DecisionRequestTests::randomInput);
            case 1 -> taskSettings = randomValueOtherThan(taskSettings, DecisionRequestTests::randomTaskSettings);
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new DecisionRequest(input, taskSettings);
    }
}
