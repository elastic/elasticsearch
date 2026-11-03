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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.UnifiedCompletionRequestBody;
import org.elasticsearch.inference.completion.CacheControl;
import org.elasticsearch.inference.completion.CacheControlTests;
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
import org.elasticsearch.inference.completion.Template;
import org.elasticsearch.inference.completion.TemplateTests;
import org.elasticsearch.inference.completion.Tool;
import org.elasticsearch.inference.completion.ToolCall;
import org.elasticsearch.inference.completion.ToolChoice;
import org.elasticsearch.inference.completion.ToolChoice.ToolChoiceObject;
import org.elasticsearch.inference.completion.ToolChoice.ToolChoiceString;
import org.elasticsearch.inference.completion.UnifiedCompletionUtils;
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

import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.CHAT_COMPLETION_CACHE_CONTROL_AND_SESSION_ID_ADDED;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.CHAT_COMPLETION_REASONING_SUPPORT_ADDED;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.CHAT_COMPLETION_TEMPLATE_ADDED;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.MULTIMODAL_CHAT_COMPLETION_SUPPORT_ADDED;
import static org.elasticsearch.test.BWCVersions.DEFAULT_BWC_VERSIONS;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class UnifiedCompletionRequestBodyTests extends AbstractBWCWireSerializationTestCase<UnifiedCompletionRequestBody> {

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
                },
                "cache_control": {
                    "type": "ephemeral"
                },
                "session_id": "test_session_123"
            }
            """;

        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            var request = UnifiedCompletionRequestBody.PARSER.apply(parser, null);
            var expected = new UnifiedCompletionRequestBody(
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
                new Reasoning(Reasoning.ReasoningEffort.MEDIUM, Reasoning.ReasoningSummary.DETAILED, false, false),
                new CacheControl("ephemeral", null),
                "test_session_123"
            );

            assertThat(request, is(expected));

            assertThat(
                Strings.toString(request, UnifiedCompletionRequestBody.withMaxCompletionTokens("gpt-4o", ToXContent.EMPTY_PARAMS)),
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
            var request = UnifiedCompletionRequestBody.PARSER.apply(parser, null);
            var expected = new UnifiedCompletionRequestBody(
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
            var exception = assertThrows(XContentParseException.class, () -> UnifiedCompletionRequestBody.PARSER.apply(parser, null));
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
            var exception = assertThrows(XContentParseException.class, () -> UnifiedCompletionRequestBody.PARSER.apply(parser, null));
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
            var exception = assertThrows(XContentParseException.class, () -> UnifiedCompletionRequestBody.PARSER.apply(parser, null));
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
            var exception = assertThrows(XContentParseException.class, () -> UnifiedCompletionRequestBody.PARSER.apply(parser, null));
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
            var exception = assertThrows(XContentParseException.class, () -> UnifiedCompletionRequestBody.PARSER.apply(parser, null));
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
            var exception = assertThrows(XContentParseException.class, () -> UnifiedCompletionRequestBody.PARSER.apply(parser, null));
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
            var exception = assertThrows(XContentParseException.class, () -> UnifiedCompletionRequestBody.PARSER.apply(parser, null));
            ElasticsearchStatusException rootCause = (ElasticsearchStatusException) ExceptionsHelper.unwrap(
                exception,
                ElasticsearchStatusException.class
            );
            assertThat(rootCause.getMessage(), is("Field [file_id] is not supported for content of type [file]"));
            assertThat(rootCause.status(), is(RestStatus.BAD_REQUEST));
        }
    }

    /**
     * Versions before {@link UnifiedCompletionUtils#MULTIMODAL_CHAT_COMPLETION_SUPPORT_ADDED} refuse to serialize non-text content
     * and versions before {@link UnifiedCompletionUtils#CHAT_COMPLETION_TEMPLATE_ADDED} refuse to serialize a template, so the
     * generic BWC test only covers versions that accept every random instance. Older versions, including the ones in between,
     * are covered by {@link #testUnsupportedFieldsAreNotBackwardsCompatible}.
     */
    @Override
    protected Collection<TransportVersion> bwcVersions() {
        return super.bwcVersions().stream().filter(version -> version.supports(CHAT_COMPLETION_TEMPLATE_ADDED)).toList();
    }

    /**
     * A body carries either messages or a template, so at most one refusal applies to an instance at a given version. Instances
     * that an older version does accept must still round trip through it, which keeps the reasoning, cache control and session id
     * down-casting covered for the versions {@link #bwcVersions()} leaves out.
     */
    public void testUnsupportedFieldsAreNotBackwardsCompatible() throws IOException {
        for (int runs = 0; runs < NUMBER_OF_TEST_RUNS; runs++) {
            var instance = createTestInstance();
            for (var version : DEFAULT_BWC_VERSIONS) {
                var expectedError = expectedSerializationError(instance, version);
                if (expectedError == null) {
                    assertBwcSerialization(instance, version);
                } else {
                    var e = expectThrows(
                        ElasticsearchStatusException.class,
                        () -> copyWriteable(instance, getNamedWriteableRegistry(), instanceReader(), version)
                    );
                    assertThat(e.status(), is(RestStatus.BAD_REQUEST));
                    assertThat(e.getMessage(), is(expectedError));
                }
            }
        }
    }

    /**
     * The error {@link UnifiedCompletionRequestBody#writeTo} raises for the body at the given version, or {@code null} when the
     * version accepts it. Shared with the tests of the requests that wrap the body.
     */
    @Nullable
    public static String expectedSerializationError(UnifiedCompletionRequestBody body, TransportVersion version) {
        if (body.containsTemplate() && version.supports(CHAT_COMPLETION_TEMPLATE_ADDED) == false) {
            return "Cannot send a chat completion request with a template to an older node. "
                + "Please wait until all nodes are upgraded before using templates";
        }
        if (body.containsMultimodalContent() && version.supports(MULTIMODAL_CHAT_COMPLETION_SUPPORT_ADDED) == false) {
            return "Cannot send a multimodal chat completion request to an older node. "
                + "Please wait until all nodes are upgraded before using multimodal chat completion inputs";
        }
        return null;
    }

    public void testParsing_TemplateOnly() throws IOException {
        String requestJson = """
            {
                "template": {
                    "id": ".attack-discovery",
                    "version": 1,
                    "variables": { "time_range": "24h", "alerts": [] }
                },
                "temperature": 0.1
            }
            """;

        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            var request = UnifiedCompletionRequestBody.PARSER.apply(parser, null);
            var expected = new UnifiedCompletionRequestBody(
                null,
                null,
                null,
                null,
                0.1f,
                null,
                null,
                null,
                null,
                null,
                null,
                new Template(".attack-discovery", 1, Map.of("time_range", "24h", "alerts", List.of()))
            );

            assertThat(request, is(expected));
            assertNull(request.messages());
            assertTrue(request.containsTemplate());
        }
    }

    /**
     * The parser accepts both fields; exclusivity is enforced by {@code UnifiedCompletionAction.Request#validate()}.
     */
    public void testParsing_MessagesAndTemplate_IsAcceptedByParser() throws IOException {
        String requestJson = """
            {
                "messages": [ { "role": "user", "content": "hello" } ],
                "template": { "id": ".summarize", "version": 2, "variables": {} }
            }
            """;

        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            var request = UnifiedCompletionRequestBody.PARSER.apply(parser, null);
            assertThat(request.messages(), is(List.of(new Message(new ContentString("hello"), "user", null, null))));
            assertThat(request.template(), is(new Template(".summarize", 2, Map.of())));
        }
    }

    public void testParsing_TemplateWithUnknownField_ThrowsException() throws IOException {
        String requestJson = """
            {
                "template": { "id": ".summarize", "version": 2, "variables": {}, "messages": [] }
            }
            """;

        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            var e = expectThrows(XContentParseException.class, () -> UnifiedCompletionRequestBody.PARSER.apply(parser, null));
            assertThat(e.getMessage(), containsString("failed to parse field [template]"));
            assertThat(e.getCause().getMessage(), containsString("unknown field [messages]"));
        }
    }

    public void testToXContent_TemplateOnly_OmitsMessages() {
        var body = new UnifiedCompletionRequestBody(
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            "session",
            new Template(".summarize", 2, Map.of("k", "v"))
        );

        assertThat(Strings.toString(body), is("""
            {"session_id":"session","template":{"id":".summarize","version":2,"variables":{"k":"v"}}}"""));
    }

    public void testContainsMultimodalContentWithNullContent() {
        var messageWithNullContent = new Message(null, "user", null, null, null, null);
        var request = UnifiedCompletionRequestBody.of(List.of(messageWithNullContent));
        assertFalse(request.containsMultimodalContent());
    }

    public void testContainsMultimodalContentWithMixedNullAndMultimodalContent() {
        var messageWithNullContent = new Message(null, "assistant", "call_1", null, null, null);
        var messageWithMultimodalContent = new Message(
            new ContentObjects(List.of(randomContentObjectImage())),
            "user",
            null,
            null,
            null,
            null
        );
        var request = UnifiedCompletionRequestBody.of(List.of(messageWithNullContent, messageWithMultimodalContent));
        assertTrue(request.containsMultimodalContent());
    }

    /**
     * A body with either messages or a template, never both, as {@code validate()} requires.
     */
    public static UnifiedCompletionRequestBody randomUnifiedCompletionRequest() {
        return randomBoolean() ? randomUnifiedCompletionRequestWithMessages() : randomUnifiedCompletionRequestWithTemplate();
    }

    /**
     * A template-only body. Tools are left out because a template rejects them.
     */
    public static UnifiedCompletionRequestBody randomUnifiedCompletionRequestWithTemplate() {
        return new UnifiedCompletionRequestBody(
            null,
            randomAlphaOfLengthOrNull(10),
            randomNonNegativeLongOrNull(),
            randomStopOrNull(),
            randomFloatOrNull(),
            null,
            null,
            randomFloatOrNull(),
            randomReasoningOrNull(),
            randomCacheControlOrNull(),
            randomAlphaOfLengthOrNull(10),
            TemplateTests.randomTemplate()
        );
    }

    public static UnifiedCompletionRequestBody randomUnifiedCompletionRequestWithMessages() {
        return new UnifiedCompletionRequestBody(
            randomList(5, UnifiedCompletionRequestBodyTests::randomMessage),
            randomAlphaOfLengthOrNull(10),
            randomNonNegativeLongOrNull(),
            randomStopOrNull(),
            randomFloatOrNull(),
            randomToolChoiceOrNull(),
            randomToolListOrNull(),
            randomFloatOrNull(),
            randomReasoningOrNull(),
            randomCacheControlOrNull(),
            randomAlphaOfLengthOrNull(10)
        );
    }

    public static UnifiedCompletionRequestBody randomTextInputOnlyUnifiedCompletionRequest() {
        return new UnifiedCompletionRequestBody(
            randomList(5, () -> randomMessage(false)),
            randomAlphaOfLengthOrNull(10),
            randomNonNegativeLongOrNull(),
            randomStopOrNull(),
            randomFloatOrNull(),
            randomToolChoiceOrNull(),
            randomToolListOrNull(),
            randomFloatOrNull(),
            randomReasoningOrNull(),
            randomCacheControlOrNull(),
            randomAlphaOfLengthOrNull(10)
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
        return randomBoolean() ? randomList(10, UnifiedCompletionRequestBodyTests::randomToolCall) : null;
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
        return randomBoolean() ? randomList(10, UnifiedCompletionRequestBodyTests::randomTool) : null;
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

    public static CacheControl randomCacheControlOrNull() {
        return randomBoolean() ? CacheControlTests.randomCacheControl() : null;
    }

    @Override
    protected UnifiedCompletionRequestBody mutateInstanceForVersion(UnifiedCompletionRequestBody instance, TransportVersion version) {
        return mutateInstanceForTransportVersion(instance, version);
    }

    public static UnifiedCompletionRequestBody mutateInstanceForTransportVersion(
        UnifiedCompletionRequestBody instance,
        TransportVersion version
    ) {
        // Multimodal content and templates cause an exception to be thrown when serializing to versions that don't support them,
        // so there is no need to mutate the instance for those versions. This is tested in
        // testUnsupportedFieldsAreNotBackwardsCompatible; the template is simply carried through here.
        if (version.supports(CHAT_COMPLETION_REASONING_SUPPORT_ADDED) == false) {
            instance = new UnifiedCompletionRequestBody(
                instance.messages() == null
                    ? null
                    : instance.messages()
                        .stream()
                        .map(message -> new Message(message.content(), message.role(), message.toolCallId(), message.toolCalls()))
                        .toList(),
                instance.model(),
                instance.maxCompletionTokens(),
                instance.stop(),
                instance.temperature(),
                instance.toolChoice(),
                instance.tools(),
                instance.topP(),
                null,
                instance.cacheControl(),
                instance.sessionId(),
                instance.template()
            );
        }
        if (version.supports(CHAT_COMPLETION_CACHE_CONTROL_AND_SESSION_ID_ADDED) == false) {
            instance = new UnifiedCompletionRequestBody(
                instance.messages(),
                instance.model(),
                instance.maxCompletionTokens(),
                instance.stop(),
                instance.temperature(),
                instance.toolChoice(),
                instance.tools(),
                instance.topP(),
                instance.reasoning(),
                null,
                null,
                instance.template()
            );
        }
        return instance;
    }

    @Override
    protected Writeable.Reader<UnifiedCompletionRequestBody> instanceReader() {
        return UnifiedCompletionRequestBody::new;
    }

    @Override
    protected UnifiedCompletionRequestBody createTestInstance() {
        return randomUnifiedCompletionRequest();
    }

    @Override
    protected UnifiedCompletionRequestBody mutateInstance(UnifiedCompletionRequestBody instance) throws IOException {
        List<Message> messages = instance.messages();
        String model = instance.model();
        Long maxCompletionTokens = instance.maxCompletionTokens();
        List<String> stop = instance.stop();
        Float temperature = instance.temperature();
        ToolChoice toolChoice = instance.toolChoice();
        List<Tool> tools = instance.tools();
        Float topP = instance.topP();
        Reasoning reasoning = instance.reasoning();
        CacheControl cacheControl = instance.cacheControl();
        String sessionId = instance.sessionId();
        Template template = instance.template();
        // A mutation may leave the body with both messages and a template, or neither. The record carries whatever it is
        // given; only the action request's validate() enforces exactly one, and that is tested separately.
        switch (between(0, 11)) {
            case 0 -> messages = randomValueOtherThan(messages, () -> randomList(5, UnifiedCompletionRequestBodyTests::randomMessage));
            case 1 -> model = randomValueOtherThan(model, () -> randomAlphaOfLength(10));
            case 2 -> maxCompletionTokens = randomValueOtherThan(maxCompletionTokens, ESTestCase::randomNonNegativeLongOrNull);
            case 3 -> stop = randomValueOtherThan(stop, UnifiedCompletionRequestBodyTests::randomStopOrNull);
            case 4 -> temperature = randomValueOtherThan(temperature, ESTestCase::randomFloatOrNull);
            case 5 -> toolChoice = randomValueOtherThan(toolChoice, UnifiedCompletionRequestBodyTests::randomToolChoiceOrNull);
            case 6 -> tools = randomValueOtherThan(tools, UnifiedCompletionRequestBodyTests::randomToolListOrNull);
            case 7 -> topP = randomValueOtherThan(topP, ESTestCase::randomFloatOrNull);
            case 8 -> reasoning = randomValueOtherThan(reasoning, UnifiedCompletionRequestBodyTests::randomReasoningOrNull);
            case 9 -> cacheControl = randomValueOtherThan(cacheControl, UnifiedCompletionRequestBodyTests::randomCacheControlOrNull);
            case 10 -> sessionId = randomValueOtherThan(sessionId, () -> randomBoolean() ? randomAlphaOfLength(10) : null);
            case 11 -> template = randomValueOtherThan(template, () -> randomBoolean() ? TemplateTests.randomTemplate() : null);
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new UnifiedCompletionRequestBody(
            messages,
            model,
            maxCompletionTokens,
            stop,
            temperature,
            toolChoice,
            tools,
            topP,
            reasoning,
            cacheControl,
            sessionId,
            template
        );
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(UnifiedCompletionRequestBody.getNamedWriteables());
    }
}
