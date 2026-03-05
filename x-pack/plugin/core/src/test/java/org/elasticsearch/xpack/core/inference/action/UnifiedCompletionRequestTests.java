/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static org.elasticsearch.inference.UnifiedCompletionRequest.MULTIMODAL_CHAT_COMPLETION_SUPPORT_ADDED;
import static org.elasticsearch.test.BWCVersions.DEFAULT_BWC_VERSIONS;
import static org.hamcrest.Matchers.is;

public class UnifiedCompletionRequestTests extends AbstractBWCWireSerializationTestCase<UnifiedCompletionRequest> {

    public void testParseAllFields() throws IOException {
        String requestJson = """
            {
                "messages": [
                  {
                    "content": [
                        {
                          "text": "some text",
                          "type": "text"
                        },
                        {
                          "image_url": {
                            "url": "image url value",
                            "detail": "auto"
                          },
                          "type": "image_url"
                        },
                        {
                          "file": {
                            "file_data": "file data value",
                            "filename": "file name value"
                          },
                          "type": "file"
                        }
                    ],
                    "role": "user",
                    "tool_call_id": "100",
                    "tool_calls": [
                        {
                            "id": "call_62136354",
                            "function": {
                                "arguments": "{'order_id': 'order_12345'}",
                                "name": "get_delivery_date"
                            },
                            "type": "function"
                        }
                    ]
                  }
                ],
                "stop": ["stop"],
                "temperature": 0.1,
                "tool_choice": {
                  "type": "function",
                  "function": {
                    "name": "some function"
                  }
                },
                "tools": [
                  {
                    "type": "function",
                    "function": {
                      "description": "Get the current weather in a given location",
                      "name": "get_current_weather",
                      "parameters": {
                        "type": "object"
                      }
                    }
                  }
                ],
                "top_p": 0.2,
                "max_completion_tokens": 100,
                "model": "gpt-4o"
            }
            """;

        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            var request = UnifiedCompletionRequest.PARSER.apply(parser, null);
            var expected = new UnifiedCompletionRequest(
                List.of(
                    new UnifiedCompletionRequest.Message(
                        new UnifiedCompletionRequest.ContentObjects(
                            List.of(
                                new UnifiedCompletionRequest.ContentObjectText("some text"),
                                new UnifiedCompletionRequest.ContentObjectImage(
                                    new UnifiedCompletionRequest.ContentObjectImageUrl(
                                        "image url value",
                                        UnifiedCompletionRequest.ImageUrlDetail.AUTO
                                    )
                                ),
                                new UnifiedCompletionRequest.ContentObjectFile(
                                    new UnifiedCompletionRequest.ContentObjectFileFields("file data value", null, "file name value")
                                )
                            )
                        ),
                        "user",
                        "100",
                        List.of(
                            new UnifiedCompletionRequest.ToolCall(
                                "call_62136354",
                                new UnifiedCompletionRequest.ToolCall.FunctionField("{'order_id': 'order_12345'}", "get_delivery_date"),
                                "function"
                            )
                        )
                    )
                ),
                "gpt-4o",
                100L,
                List.of("stop"),
                0.1F,
                new UnifiedCompletionRequest.ToolChoiceObject(
                    "function",
                    new UnifiedCompletionRequest.ToolChoiceObject.FunctionField("some function")
                ),
                List.of(
                    new UnifiedCompletionRequest.Tool(
                        "function",
                        new UnifiedCompletionRequest.Tool.FunctionField(
                            "Get the current weather in a given location",
                            "get_current_weather",
                            Map.of("type", "object"),
                            null
                        )
                    )
                ),
                0.2F
            );

            assertThat(request, is(expected));

            assertThat(
                Strings.toString(request, UnifiedCompletionRequest.withMaxCompletionTokens("gpt-4o", ToXContent.EMPTY_PARAMS)),
                is(XContentHelper.stripWhitespace(requestJson))
            );
        }
    }

    public void testParsing() throws IOException {
        String requestJson = """
            {
                "model": "gpt-4o",
                "messages": [
                  {
                    "role": "user",
                    "content": "What is the weather like in Boston today?"
                  }
                ],
                "stop": "none",
                "tools": [
                  {
                    "type": "function",
                    "function": {
                      "name": "get_current_weather",
                      "description": "Get the current weather in a given location",
                      "parameters": {
                        "type": "object"
                      }
                    }
                  }
                ],
                "tool_choice": "auto"
            }
            """;

        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            var request = UnifiedCompletionRequest.PARSER.apply(parser, null);
            var expected = new UnifiedCompletionRequest(
                List.of(
                    new UnifiedCompletionRequest.Message(
                        new UnifiedCompletionRequest.ContentString("What is the weather like in Boston today?"),
                        "user",
                        null,
                        null
                    )
                ),
                "gpt-4o",
                null,
                List.of("none"),
                null,
                new UnifiedCompletionRequest.ToolChoiceString("auto"),
                List.of(
                    new UnifiedCompletionRequest.Tool(
                        "function",
                        new UnifiedCompletionRequest.Tool.FunctionField(
                            "Get the current weather in a given location",
                            "get_current_weather",
                            Map.of("type", "object"),
                            null
                        )
                    )
                ),
                null
            );

            assertThat(request, is(expected));
        }
    }

    public void testUnsupportedContentType() throws IOException {
        String requestJson = """
            {
                "messages": [
                  {
                    "content": [
                        {
                          "text": "input text",
                          "type": "unknown type"
                        }
                    ],
                    "role": "user"
                  }
                ]
            }
            """;

        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            var exception = assertThrows(XContentParseException.class, () -> UnifiedCompletionRequest.PARSER.apply(parser, null));
            ElasticsearchStatusException rootCause = (ElasticsearchStatusException) ExceptionsHelper.unwrap(
                exception,
                ElasticsearchStatusException.class
            );
            assertThat(
                rootCause.getMessage(),
                is("Unrecognized type [unknown type] in object [content], must be one of [text, image_url, file]")
            );
            assertThat(rootCause.status(), is(RestStatus.BAD_REQUEST));
        }
    }

    public void testUnknownContentField() throws IOException {
        String requestJson = """
            {
                "messages": [
                  {
                    "content": [
                        {
                          "text": "input text",
                          "type": "text",
                          "unknown field": "value"
                        }
                    ],
                    "role": "user"
                  }
                ]
            }
            """;

        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            var exception = assertThrows(XContentParseException.class, () -> UnifiedCompletionRequest.PARSER.apply(parser, null));
            ElasticsearchStatusException rootCause = (ElasticsearchStatusException) ExceptionsHelper.unwrap(
                exception,
                ElasticsearchStatusException.class
            );
            assertThat(rootCause.getMessage(), is("[content] contains unknown fields [unknown field]"));
            assertThat(rootCause.status(), is(RestStatus.BAD_REQUEST));
        }
    }

    public void testUnknownImageContentField() throws IOException {
        String requestJson = """
            {
                "messages": [
                  {
                    "content": [
                        {
                          "image_url": {
                            "url": "input image",
                            "unknown field": "value"
                          },
                          "type": "image_url"
                        }
                    ],
                    "role": "user"
                  }
                ]
            }
            """;

        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            var exception = assertThrows(XContentParseException.class, () -> UnifiedCompletionRequest.PARSER.apply(parser, null));
            ElasticsearchStatusException rootCause = (ElasticsearchStatusException) ExceptionsHelper.unwrap(
                exception,
                ElasticsearchStatusException.class
            );
            assertThat(rootCause.getMessage(), is("[image_url] contains unknown fields [unknown field]"));
            assertThat(rootCause.status(), is(RestStatus.BAD_REQUEST));
        }
    }

    public void testUnknownFileContentField() throws IOException {
        String requestJson = """
            {
                "messages": [
                  {
                    "content": [
                        {
                          "file": {
                            "file_data": "input file",
                            "filename": "filename value",
                            "unknown field": "value"
                          },
                          "type": "file"
                        }
                    ],
                    "role": "user"
                  }
                ]
            }
            """;

        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            var exception = assertThrows(XContentParseException.class, () -> UnifiedCompletionRequest.PARSER.apply(parser, null));
            ElasticsearchStatusException rootCause = (ElasticsearchStatusException) ExceptionsHelper.unwrap(
                exception,
                ElasticsearchStatusException.class
            );
            assertThat(rootCause.getMessage(), is("[file] contains unknown fields [unknown field]"));
            assertThat(rootCause.status(), is(RestStatus.BAD_REQUEST));
        }
    }

    public void testFileContentFileDataRequired() throws IOException {
        String requestJson = """
            {
                "messages": [
                  {
                    "content": [
                        {
                          "file": {
                            "filename": "filename value"
                          },
                          "type": "file"
                        }
                    ],
                    "role": "user"
                  }
                ]
            }
            """;

        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            var exception = assertThrows(XContentParseException.class, () -> UnifiedCompletionRequest.PARSER.apply(parser, null));
            ElasticsearchStatusException rootCause = (ElasticsearchStatusException) ExceptionsHelper.unwrap(
                exception,
                ElasticsearchStatusException.class
            );
            assertThat(rootCause.getMessage(), is("Field [file_data] in object [file] is required but was not found"));
            assertThat(rootCause.status(), is(RestStatus.BAD_REQUEST));
        }
    }

    public void testFileContentFilenameRequired() throws IOException {
        String requestJson = """
            {
                "messages": [
                  {
                    "content": [
                        {
                          "file": {
                            "file_data": "file data value"
                          },
                          "type": "file"
                        }
                    ],
                    "role": "user"
                  }
                ]
            }
            """;

        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            var exception = assertThrows(XContentParseException.class, () -> UnifiedCompletionRequest.PARSER.apply(parser, null));
            ElasticsearchStatusException rootCause = (ElasticsearchStatusException) ExceptionsHelper.unwrap(
                exception,
                ElasticsearchStatusException.class
            );
            assertThat(rootCause.getMessage(), is("Field [filename] in object [file] is required but was not found"));
            assertThat(rootCause.status(), is(RestStatus.BAD_REQUEST));
        }
    }

    public void testFileIdFieldIsNotSupported() throws IOException {
        String requestJson = """
            {
                "messages": [
                  {
                    "content": [
                        {
                          "file": {
                            "file_data": "file data value",
                            "file_id": "file id value",
                            "filename": "file name value"
                          },
                          "type": "file"
                        }
                    ],
                    "role": "user"
                  }
                ]
            }
            """;

        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            var exception = assertThrows(XContentParseException.class, () -> UnifiedCompletionRequest.PARSER.apply(parser, null));
            ElasticsearchStatusException rootCause = (ElasticsearchStatusException) ExceptionsHelper.unwrap(
                exception,
                ElasticsearchStatusException.class
            );
            assertThat(rootCause.getMessage(), is("Field [file_id] is not supported for content of type [file]"));
            assertThat(rootCause.status(), is(RestStatus.BAD_REQUEST));
        }
    }

    // Versions before MULTIMODAL_CHAT_COMPLETION_SUPPORT_ADDED throw an exception when serializing non-text content
    // Those are tested in testMultimodalContentIsNotBackwardsCompatible
    @Override
    protected Collection<TransportVersion> bwcVersions() {
        return super.bwcVersions().stream().filter(version -> version.supports(MULTIMODAL_CHAT_COMPLETION_SUPPORT_ADDED)).toList();
    }

    public void testMultimodalContentIsNotBackwardsCompatible() throws IOException {
        var unsupportedVersions = DEFAULT_BWC_VERSIONS.stream()
            .filter(Predicate.not(version -> version.supports(MULTIMODAL_CHAT_COMPLETION_SUPPORT_ADDED)))
            .toList();
        for (int runs = 0; runs < NUMBER_OF_TEST_RUNS; runs++) {
            var testInstance = createTestInstance();
            for (var unsupportedVersion : unsupportedVersions) {
                if (testInstance.containsMultimodalContent()) {
                    var statusException = assertThrows(
                        ElasticsearchStatusException.class,
                        () -> copyWriteable(testInstance, getNamedWriteableRegistry(), instanceReader(), unsupportedVersion)
                    );
                    assertThat(statusException.status(), is(RestStatus.BAD_REQUEST));
                    assertThat(
                        statusException.getMessage(),
                        is(
                            "Cannot send a multimodal chat completion request to an older node. "
                                + "Please wait until all nodes are upgraded before using multimodal chat completion inputs"
                        )
                    );
                } else {
                    // If the instance doesn't contain multimodal content, assert that it can still be serialized
                    assertBwcSerialization(testInstance, unsupportedVersion);
                }
            }
        }
    }

    public static UnifiedCompletionRequest randomUnifiedCompletionRequest() {
        return new UnifiedCompletionRequest(
            randomList(5, UnifiedCompletionRequestTests::randomMessage),
            randomAlphaOfLengthOrNull(10),
            randomNonNegativeLongOrNull(),
            randomStopOrNull(),
            randomFloatOrNull(),
            randomToolChoiceOrNull(),
            randomToolListOrNull(),
            randomFloatOrNull()
        );
    }

    public static UnifiedCompletionRequest randomTextInputOnlyUnifiedCompletionRequest() {
        return new UnifiedCompletionRequest(
            randomList(5, () -> randomMessage(false)),
            randomAlphaOfLengthOrNull(10),
            randomNonNegativeLongOrNull(),
            randomStopOrNull(),
            randomFloatOrNull(),
            randomToolChoiceOrNull(),
            randomToolListOrNull(),
            randomFloatOrNull()
        );
    }

    public static UnifiedCompletionRequest.Message randomMessage() {
        return randomMessage(true);
    }

    public static UnifiedCompletionRequest.Message randomMessage(boolean allowMultimodal) {
        return new UnifiedCompletionRequest.Message(
            randomContent(allowMultimodal),
            randomAlphaOfLength(10),
            randomAlphaOfLengthOrNull(10),
            randomToolCallListOrNull()
        );
    }

    public static UnifiedCompletionRequest.Content randomContent() {
        return randomContent(true);
    }

    public static UnifiedCompletionRequest.Content randomContent(boolean allowMultimodal) {
        return randomBoolean()
            ? new UnifiedCompletionRequest.ContentString(randomAlphaOfLength(10))
            : new UnifiedCompletionRequest.ContentObjects(randomList(10, () -> randomContentObject(allowMultimodal)));
    }

    public static UnifiedCompletionRequest.ContentObject randomContentObject(boolean allowMultimodal) {
        if (allowMultimodal == false) {
            return randomContentObjectText();
        } else {
            return switch (randomFrom(UnifiedCompletionRequest.ContentObject.ContentObjectType.values())) {
                case TEXT -> randomContentObjectText();
                case IMAGE_URL -> randomContentObjectImage();
                case FILE -> randomContentObjectFile();
            };
        }
    }

    public static UnifiedCompletionRequest.ContentObjectText randomContentObjectText() {
        return new UnifiedCompletionRequest.ContentObjectText(randomAlphaOfLength(10));
    }

    public static UnifiedCompletionRequest.ContentObjectImage randomContentObjectImage() {
        return new UnifiedCompletionRequest.ContentObjectImage(
            new UnifiedCompletionRequest.ContentObjectImageUrl(
                randomAlphaOfLength(10),
                randomFrom(UnifiedCompletionRequest.ImageUrlDetail.values())
            )
        );
    }

    public static UnifiedCompletionRequest.ContentObjectFile randomContentObjectFile() {
        return new UnifiedCompletionRequest.ContentObjectFile(
            new UnifiedCompletionRequest.ContentObjectFileFields(randomAlphaOfLength(10), randomAlphaOfLength(10), randomAlphaOfLength(10))
        );
    }

    public static List<UnifiedCompletionRequest.ToolCall> randomToolCallListOrNull() {
        return randomBoolean() ? randomList(10, UnifiedCompletionRequestTests::randomToolCall) : null;
    }

    public static UnifiedCompletionRequest.ToolCall randomToolCall() {
        return new UnifiedCompletionRequest.ToolCall(randomAlphaOfLength(10), randomToolCallFunctionField(), randomAlphaOfLength(10));
    }

    public static UnifiedCompletionRequest.ToolCall.FunctionField randomToolCallFunctionField() {
        return new UnifiedCompletionRequest.ToolCall.FunctionField(randomAlphaOfLength(10), randomAlphaOfLength(10));
    }

    public static List<String> randomStopOrNull() {
        return randomBoolean() ? randomStop() : null;
    }

    public static List<String> randomStop() {
        return randomList(5, () -> randomAlphaOfLength(10));
    }

    public static UnifiedCompletionRequest.ToolChoice randomToolChoiceOrNull() {
        return randomBoolean() ? randomToolChoice() : null;
    }

    public static UnifiedCompletionRequest.ToolChoice randomToolChoice() {
        return randomBoolean()
            ? new UnifiedCompletionRequest.ToolChoiceString(randomAlphaOfLength(10))
            : new UnifiedCompletionRequest.ToolChoiceObject(randomAlphaOfLength(10), randomToolChoiceObjectFunctionField());
    }

    public static UnifiedCompletionRequest.ToolChoiceObject.FunctionField randomToolChoiceObjectFunctionField() {
        return new UnifiedCompletionRequest.ToolChoiceObject.FunctionField(randomAlphaOfLength(10));
    }

    public static List<UnifiedCompletionRequest.Tool> randomToolListOrNull() {
        return randomBoolean() ? randomList(10, UnifiedCompletionRequestTests::randomTool) : null;
    }

    public static UnifiedCompletionRequest.Tool randomTool() {
        return new UnifiedCompletionRequest.Tool(randomAlphaOfLength(10), randomToolFunctionField());
    }

    public static UnifiedCompletionRequest.Tool.FunctionField randomToolFunctionField() {
        return new UnifiedCompletionRequest.Tool.FunctionField(
            randomAlphaOfLengthOrNull(10),
            randomAlphaOfLength(10),
            null,
            randomOptionalBoolean()
        );
    }

    @Override
    protected UnifiedCompletionRequest mutateInstanceForVersion(UnifiedCompletionRequest instance, TransportVersion version) {
        // No need to mutate the instance for backwards compatibility, because unsupported content types cause an exception to be thrown
        // when serializing to older nodes
        return instance;
    }

    @Override
    protected Writeable.Reader<UnifiedCompletionRequest> instanceReader() {
        return UnifiedCompletionRequest::new;
    }

    @Override
    protected UnifiedCompletionRequest createTestInstance() {
        return randomUnifiedCompletionRequest();
    }

    @Override
    protected UnifiedCompletionRequest mutateInstance(UnifiedCompletionRequest instance) throws IOException {
        List<UnifiedCompletionRequest.Message> messages = instance.messages();
        String model = instance.model();
        Long maxCompletionTokens = instance.maxCompletionTokens();
        List<String> stop = instance.stop();
        Float temperature = instance.temperature();
        UnifiedCompletionRequest.ToolChoice toolChoice = instance.toolChoice();
        List<UnifiedCompletionRequest.Tool> tools = instance.tools();
        Float topP = instance.topP();
        switch (between(0, 7)) {
            case 0 -> messages = randomValueOtherThan(messages, () -> randomList(5, UnifiedCompletionRequestTests::randomMessage));
            case 1 -> model = randomValueOtherThan(model, () -> randomAlphaOfLength(10));
            case 2 -> maxCompletionTokens = randomValueOtherThan(maxCompletionTokens, ESTestCase::randomNonNegativeLongOrNull);
            case 3 -> stop = randomValueOtherThan(stop, UnifiedCompletionRequestTests::randomStopOrNull);
            case 4 -> temperature = randomValueOtherThan(temperature, ESTestCase::randomFloatOrNull);
            case 5 -> toolChoice = randomValueOtherThan(toolChoice, UnifiedCompletionRequestTests::randomToolChoiceOrNull);
            case 6 -> tools = randomValueOtherThan(tools, UnifiedCompletionRequestTests::randomToolListOrNull);
            case 7 -> topP = randomValueOtherThan(topP, ESTestCase::randomFloatOrNull);
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new UnifiedCompletionRequest(messages, model, maxCompletionTokens, stop, temperature, toolChoice, tools, topP);
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(UnifiedCompletionRequest.getNamedWriteables());
    }
}
