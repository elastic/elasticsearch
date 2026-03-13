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
import org.elasticsearch.inference.completion.Content;
import org.elasticsearch.inference.completion.ContentObject;
import org.elasticsearch.inference.completion.ContentObject.ContentObjectFile;
import org.elasticsearch.inference.completion.ContentObject.ContentObjectFile.ContentObjectFileFields;
import org.elasticsearch.inference.completion.ContentObject.ContentObjectImage;
import org.elasticsearch.inference.completion.ContentObject.ContentObjectImage.ContentObjectImageUrl;
import org.elasticsearch.inference.completion.ContentObject.ContentObjectImage.ContentObjectImageUrl.ImageUrlDetail;
import org.elasticsearch.inference.completion.ContentObject.ContentObjectText;
import org.elasticsearch.inference.completion.ContentObjects;
import org.elasticsearch.inference.completion.ContentString;
import org.elasticsearch.inference.completion.Message;
import org.elasticsearch.inference.completion.Reasoning;
import org.elasticsearch.inference.completion.ReasoningDetail;
import org.elasticsearch.inference.completion.ReasoningDetailTests;
import org.elasticsearch.inference.completion.ReasoningTests;
import org.elasticsearch.inference.completion.Tool;
import org.elasticsearch.inference.completion.ToolCall;
import org.elasticsearch.inference.completion.ToolChoice;
import org.elasticsearch.inference.completion.ToolChoice.ToolChoiceObject;
import org.elasticsearch.inference.completion.ToolChoice.ToolChoiceString;
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

import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.CHAT_COMPLETION_REASONING_SUPPORT_ADDED;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.MULTIMODAL_CHAT_COMPLETION_SUPPORT_ADDED;
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
                    ],
                    "reasoning": "some reasoning",
                    "reasoning_details": [
                        {
                            "type": "reasoning.encrypted",
                            "format": "some encrypted reasoning detail format",
                            "id": "some id 0",
                            "index": 0,
                            "data": "some encrypted data"
                        },
                        {
                            "type": "reasoning.summary",
                            "format": "some summary reasoning detail format",
                            "id": "some id 1",
                            "index": 1,
                            "summary": "some summary"
                        },
                        {
                            "type": "reasoning.text",
                            "format": "some text reasoning detail format",
                            "id": "some id 2",
                            "index": 2,
                            "text": "some text",
                            "signature": "some signature"
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
                "model": "gpt-4o",
                "reasoning": {
                    "effort": "medium",
                    "summary": "detailed",
                    "exclude": false,
                    "enabled": false
                }
            }
            """;

        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            var request = UnifiedCompletionRequest.PARSER.apply(parser, null);
            var expected = new UnifiedCompletionRequest(
                List.of(
                    new Message(
                        new ContentObjects(
                            List.of(
                                new ContentObjectText("some text"),
                                new ContentObjectImage(new ContentObjectImageUrl("image url value", ImageUrlDetail.AUTO)),
                                new ContentObjectFile(new ContentObjectFileFields("file data value", null, "file name value"))
                            )
                        ),
                        "user",
                        "100",
                        List.of(
                            new ToolCall(
                                "call_62136354",
                                new ToolCall.FunctionField("{'order_id': 'order_12345'}", "get_delivery_date"),
                                "function"
                            )
                        ),
                        "some reasoning",
                        List.of(
                            new ReasoningDetail.EncryptedReasoningDetail(
                                "some encrypted reasoning detail format",
                                "some id 0",
                                0L,
                                "some encrypted data"
                            ),
                            new ReasoningDetail.SummaryReasoningDetail(
                                "some summary reasoning detail format",
                                "some id 1",
                                1L,
                                "some summary"
                            ),
                            new ReasoningDetail.TextReasoningDetail(
                                "some text reasoning detail format",
                                "some id 2",
                                2L,
                                "some text",
                                "some signature"
                            )
                        )
                    )
                ),
                "gpt-4o",
                100L,
                List.of("stop"),
                0.1F,
                new ToolChoiceObject("function", new ToolChoiceObject.FunctionField("some function")),
                List.of(
                    new Tool(
                        "function",
                        new Tool.FunctionField(
                            "Get the current weather in a given location",
                            "get_current_weather",
                            Map.of("type", "object"),
                            null
                        )
                    )
                ),
                0.2F,
                new Reasoning(Reasoning.ReasoningEffort.MEDIUM, null, Reasoning.ReasoningSummary.DETAILED, false, false)
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
                List.of(new Message(new ContentString("What is the weather like in Boston today?"), "user", null, null)),
                "gpt-4o",
                null,
                List.of("none"),
                null,
                new ToolChoiceString("auto"),
                List.of(
                    new Tool(
                        "function",
                        new Tool.FunctionField(
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
            randomFloatOrNull(),
            randomReasoningOrNull()
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
            randomFloatOrNull(),
            randomReasoningOrNull()
        );
    }

    public static Message randomMessage() {
        return randomMessage(true);
    }

    public static Message randomMessage(boolean allowMultimodal) {
        return new Message(
            randomContent(allowMultimodal),
            randomAlphaOfLength(10),
            randomAlphaOfLengthOrNull(10),
            randomToolCallListOrNull(),
            randomAlphaOfLengthOrNull(10),
            randomReasoningDetailListOrNull()
        );
    }

    public static Content randomContent() {
        return randomContent(true);
    }

    public static Content randomContent(boolean allowMultimodal) {
        return randomBoolean()
            ? new ContentString(randomAlphaOfLength(10))
            : new ContentObjects(randomList(10, () -> randomContentObject(allowMultimodal)));
    }

    public static ContentObject randomContentObject(boolean allowMultimodal) {
        if (allowMultimodal == false) {
            return randomContentObjectText();
        } else {
            return switch (randomFrom(ContentObject.ContentObjectType.values())) {
                case TEXT -> randomContentObjectText();
                case IMAGE_URL -> randomContentObjectImage();
                case FILE -> randomContentObjectFile();
            };
        }
    }

    public static ContentObjectText randomContentObjectText() {
        return new ContentObjectText(randomAlphaOfLength(10));
    }

    public static ContentObjectImage randomContentObjectImage() {
        return new ContentObjectImage(new ContentObjectImageUrl(randomAlphaOfLength(10), randomFrom(ImageUrlDetail.values())));
    }

    public static ContentObjectFile randomContentObjectFile() {
        return new ContentObjectFile(
            new ContentObjectFileFields(randomAlphaOfLength(10), randomAlphaOfLength(10), randomAlphaOfLength(10))
        );
    }

    public static List<ToolCall> randomToolCallListOrNull() {
        return randomBoolean() ? randomList(10, UnifiedCompletionRequestTests::randomToolCall) : null;
    }

    public static ToolCall randomToolCall() {
        return new ToolCall(randomAlphaOfLength(10), randomToolCallFunctionField(), randomAlphaOfLength(10));
    }

    public static ToolCall.FunctionField randomToolCallFunctionField() {
        return new ToolCall.FunctionField(randomAlphaOfLength(10), randomAlphaOfLength(10));
    }

    public static List<ReasoningDetail> randomReasoningDetailListOrNull() {
        return randomBoolean() ? randomList(10, ReasoningDetailTests::randomReasoningDetail) : null;
    }

    public static List<String> randomStopOrNull() {
        return randomBoolean() ? randomStop() : null;
    }

    public static List<String> randomStop() {
        return randomList(5, () -> randomAlphaOfLength(10));
    }

    public static ToolChoice randomToolChoiceOrNull() {
        return randomBoolean() ? randomToolChoice() : null;
    }

    public static ToolChoice randomToolChoice() {
        return randomBoolean()
            ? new ToolChoiceString(randomAlphaOfLength(10))
            : new ToolChoiceObject(randomAlphaOfLength(10), randomToolChoiceObjectFunctionField());
    }

    public static ToolChoiceObject.FunctionField randomToolChoiceObjectFunctionField() {
        return new ToolChoiceObject.FunctionField(randomAlphaOfLength(10));
    }

    public static List<Tool> randomToolListOrNull() {
        return randomBoolean() ? randomList(10, UnifiedCompletionRequestTests::randomTool) : null;
    }

    public static Tool randomTool() {
        return new Tool(randomAlphaOfLength(10), randomToolFunctionField());
    }

    public static Tool.FunctionField randomToolFunctionField() {
        return new Tool.FunctionField(randomAlphaOfLengthOrNull(10), randomAlphaOfLength(10), null, randomOptionalBoolean());
    }

    public static Reasoning randomReasoningOrNull() {
        return randomBoolean() ? ReasoningTests.randomReasoning() : null;
    }

    @Override
    protected UnifiedCompletionRequest mutateInstanceForVersion(UnifiedCompletionRequest instance, TransportVersion version) {
        return mutateInstanceForTransportVersion(instance, version);
    }

    public static UnifiedCompletionRequest mutateInstanceForTransportVersion(UnifiedCompletionRequest instance, TransportVersion version) {
        // Unsupported content types cause an exception to be thrown when serializing to older nodes,
        // so there is no need to mutate the instance for backwards compatibility for versions that don't support multimodal content.
        // This is tested in testMultimodalContentIsNotBackwardsCompatible

        if (version.supports(CHAT_COMPLETION_REASONING_SUPPORT_ADDED) == false) {
            instance = new UnifiedCompletionRequest(
                instance.messages()
                    .stream()
                    .map(message -> new Message(message.content(), message.role(), message.toolCallId(), message.toolCalls()))
                    .toList(),
                instance.model(),
                instance.maxCompletionTokens(),
                instance.stop(),
                instance.temperature(),
                instance.toolChoice(),
                instance.tools(),
                instance.topP()
            );
        }
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
        List<Message> messages = instance.messages();
        String model = instance.model();
        Long maxCompletionTokens = instance.maxCompletionTokens();
        List<String> stop = instance.stop();
        Float temperature = instance.temperature();
        ToolChoice toolChoice = instance.toolChoice();
        List<Tool> tools = instance.tools();
        Float topP = instance.topP();
        Reasoning reasoning = instance.reasoning();
        switch (between(0, 8)) {
            case 0 -> messages = randomValueOtherThan(messages, () -> randomList(5, UnifiedCompletionRequestTests::randomMessage));
            case 1 -> model = randomValueOtherThan(model, () -> randomAlphaOfLength(10));
            case 2 -> maxCompletionTokens = randomValueOtherThan(maxCompletionTokens, ESTestCase::randomNonNegativeLongOrNull);
            case 3 -> stop = randomValueOtherThan(stop, UnifiedCompletionRequestTests::randomStopOrNull);
            case 4 -> temperature = randomValueOtherThan(temperature, ESTestCase::randomFloatOrNull);
            case 5 -> toolChoice = randomValueOtherThan(toolChoice, UnifiedCompletionRequestTests::randomToolChoiceOrNull);
            case 6 -> tools = randomValueOtherThan(tools, UnifiedCompletionRequestTests::randomToolListOrNull);
            case 7 -> topP = randomValueOtherThan(topP, ESTestCase::randomFloatOrNull);
            case 8 -> reasoning = randomValueOtherThan(reasoning, UnifiedCompletionRequestTests::randomReasoningOrNull);
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new UnifiedCompletionRequest(messages, model, maxCompletionTokens, stop, temperature, toolChoice, tools, topP, reasoning);
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(UnifiedCompletionRequest.getNamedWriteables());
    }
}
