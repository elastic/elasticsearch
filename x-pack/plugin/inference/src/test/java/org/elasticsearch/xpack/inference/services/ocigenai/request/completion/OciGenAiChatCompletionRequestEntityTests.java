/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ocigenai.request.completion;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.inference.UnifiedCompletionRequestBody;
import org.elasticsearch.inference.completion.ContentObject;
import org.elasticsearch.inference.completion.ContentObjects;
import org.elasticsearch.inference.completion.ContentString;
import org.elasticsearch.inference.completion.Message;
import org.elasticsearch.inference.completion.Tool;
import org.elasticsearch.inference.completion.ToolCall;
import org.elasticsearch.inference.completion.ToolChoice;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.services.ocigenai.completion.OciGenAiChatCompletionModelTests;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiTestUtils.COMPARTMENT_ID;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class OciGenAiChatCompletionRequestEntityTests extends ESTestCase {

    private static final String LLAMA = "meta.llama-3.3-70b-instruct";
    private static final String COMMAND = "cohere.command-a-03-2025";

    public void testGeneric_MessagesAndSamplingParameters() throws IOException {
        var body = new UnifiedCompletionRequestBody(
            List.of(
                message("system", "You are terse."),
                message("user", "Say hello."),
                new Message(
                    null,
                    "assistant",
                    null,
                    List.of(new ToolCall("call_1", new ToolCall.FunctionField("{\"city\":\"Paris\"}", "get_weather"), "function")),
                    null,
                    null
                ),
                new Message(new ContentString("sunny"), "tool", "call_1", null, null, null)
            ),
            LLAMA,
            50L,
            List.of("END"),
            0.2F,
            null,
            null,
            0.9F,
            null,
            null,
            null
        );

        var requestMap = toMap(body, LLAMA, true);

        assertThat(requestMap.get("compartmentId"), is(COMPARTMENT_ID));
        assertThat(requestMap.get("servingMode"), is(Map.of("servingType", "ON_DEMAND", "modelId", LLAMA)));

        @SuppressWarnings("unchecked")
        var chatRequest = (Map<String, Object>) requestMap.get("chatRequest");
        assertThat(chatRequest.get("apiFormat"), is("GENERIC"));
        assertThat(chatRequest.get("isStream"), is(true));
        assertThat(chatRequest.get("maxTokens"), is(50));
        assertThat(chatRequest.get("temperature"), is(0.2));
        assertThat(chatRequest.get("topP"), is(0.9));
        assertThat(chatRequest.get("stop"), is(List.of("END")));
        assertThat(
            chatRequest.get("messages"),
            is(
                List.of(
                    Map.of("role", "SYSTEM", "content", List.of(Map.of("type", "TEXT", "text", "You are terse."))),
                    Map.of("role", "USER", "content", List.of(Map.of("type", "TEXT", "text", "Say hello."))),
                    Map.of(
                        "role",
                        "ASSISTANT",
                        "toolCalls",
                        List.of(Map.of("type", "FUNCTION", "id", "call_1", "name", "get_weather", "arguments", "{\"city\":\"Paris\"}"))
                    ),
                    Map.of("role", "TOOL", "toolCallId", "call_1", "content", List.of(Map.of("type", "TEXT", "text", "sunny")))
                )
            )
        );
        assertFalse(chatRequest.containsKey("tools"));
        assertFalse(chatRequest.containsKey("toolChoice"));
    }

    public void testGeneric_ToolsAndToolChoice() throws IOException {
        var tool = new Tool(
            "function",
            new Tool.FunctionField("Get weather for a city", "get_weather", Map.of("type", "object", "properties", Map.of()), null)
        );
        var body = new UnifiedCompletionRequestBody(
            List.of(message("user", "Weather in Paris?")),
            null,
            null,
            null,
            null,
            new ToolChoice.ToolChoiceObject("function", new ToolChoice.ToolChoiceObject.FunctionField("get_weather")),
            List.of(tool),
            null,
            null,
            null,
            null
        );

        @SuppressWarnings("unchecked")
        var chatRequest = (Map<String, Object>) toMap(body, LLAMA, false).get("chatRequest");

        assertThat(chatRequest.get("isStream"), is(false));
        assertThat(
            chatRequest.get("tools"),
            is(
                List.of(
                    Map.of(
                        "type",
                        "FUNCTION",
                        "name",
                        "get_weather",
                        "description",
                        "Get weather for a city",
                        "parameters",
                        Map.of("type", "object", "properties", Map.of())
                    )
                )
            )
        );
        assertThat(chatRequest.get("toolChoice"), is(Map.of("type", "FUNCTION", "name", "get_weather")));
    }

    public void testGeneric_StringToolChoice() throws IOException {
        var body = new UnifiedCompletionRequestBody(
            List.of(message("user", "hi")),
            null,
            null,
            null,
            null,
            new ToolChoice.ToolChoiceString("required"),
            null,
            null,
            null,
            null,
            null
        );

        @SuppressWarnings("unchecked")
        var chatRequest = (Map<String, Object>) toMap(body, LLAMA, false).get("chatRequest");

        assertThat(chatRequest.get("toolChoice"), is(Map.of("type", "REQUIRED")));
    }

    public void testGeneric_ImageContent() throws IOException {
        var content = new ContentObjects(
            List.of(
                new ContentObject.ContentObjectText("describe"),
                new ContentObject.ContentObjectImage(
                    new ContentObject.ContentObjectImage.ContentObjectImageUrl("https://example.com/a.png", null)
                )
            )
        );
        var body = UnifiedCompletionRequestBody.of(List.of(new Message(content, "user", null, null, null, null)));

        @SuppressWarnings("unchecked")
        var chatRequest = (Map<String, Object>) toMap(body, LLAMA, false).get("chatRequest");

        assertThat(
            chatRequest.get("messages"),
            is(
                List.of(
                    Map.of(
                        "role",
                        "USER",
                        "content",
                        List.of(
                            Map.of("type", "TEXT", "text", "describe"),
                            Map.of("type", "IMAGE", "imageUrl", Map.of("url", "https://example.com/a.png"))
                        )
                    )
                )
            )
        );
    }

    public void testGeneric_UnsupportedRoleThrows() {
        var body = UnifiedCompletionRequestBody.of(List.of(message("robot", "hi")));

        var exception = expectThrows(ElasticsearchStatusException.class, () -> toMap(body, LLAMA, false));

        assertThat(exception.status(), is(RestStatus.BAD_REQUEST));
        assertThat(exception.getMessage(), containsString("Role [robot] is not supported"));
    }

    public void testCohere_MessageAndChatHistory() throws IOException {
        var body = new UnifiedCompletionRequestBody(
            List.of(
                message("system", "You are terse."),
                message("user", "hi"),
                message("assistant", "hello"),
                message("user", "Say hello.")
            ),
            null,
            40L,
            List.of("STOP"),
            0F,
            null,
            null,
            null,
            null,
            null,
            null
        );

        var requestMap = toMap(body, COMMAND, false);

        @SuppressWarnings("unchecked")
        var chatRequest = (Map<String, Object>) requestMap.get("chatRequest");
        assertThat(chatRequest.get("apiFormat"), is("COHERE"));
        assertThat(chatRequest.get("message"), is("Say hello."));
        assertThat(
            chatRequest.get("chatHistory"),
            is(
                List.of(
                    Map.of("role", "SYSTEM", "message", "You are terse."),
                    Map.of("role", "USER", "message", "hi"),
                    Map.of("role", "CHATBOT", "message", "hello")
                )
            )
        );
        assertThat(chatRequest.get("maxTokens"), is(40));
        assertThat(chatRequest.get("temperature"), is(0.0));
        assertThat(chatRequest.get("stopSequences"), is(List.of("STOP")));
        assertFalse(chatRequest.containsKey("messages"));
    }

    public void testCohere_SingleMessageHasNoChatHistory() throws IOException {
        var body = UnifiedCompletionRequestBody.of(List.of(message("user", "hi")));

        @SuppressWarnings("unchecked")
        var chatRequest = (Map<String, Object>) toMap(body, COMMAND, false).get("chatRequest");

        assertThat(chatRequest.get("message"), is("hi"));
        assertFalse(chatRequest.containsKey("chatHistory"));
    }

    public void testCohere_LastMessageMustBeFromTheUser() {
        var body = UnifiedCompletionRequestBody.of(List.of(message("user", "hi"), message("assistant", "hello")));

        var exception = expectThrows(ElasticsearchStatusException.class, () -> toMap(body, COMMAND, false));

        assertThat(exception.status(), is(RestStatus.BAD_REQUEST));
        assertThat(exception.getMessage(), containsString("The last message must have the [user] role"));
    }

    public void testCohere_ToolsAreRejected() {
        var body = new UnifiedCompletionRequestBody(
            List.of(message("user", "hi")),
            null,
            null,
            null,
            null,
            null,
            List.of(new Tool("function", new Tool.FunctionField(null, "f", null, null))),
            null,
            null,
            null,
            null
        );

        var exception = expectThrows(ElasticsearchStatusException.class, () -> toMap(body, COMMAND, false));

        assertThat(exception.getMessage(), containsString("Tools are not supported"));
    }

    private static Message message(String role, String text) {
        return new Message(new ContentString(text), role, null, null, null, null);
    }

    private static Map<String, Object> toMap(UnifiedCompletionRequestBody body, String modelId, boolean stream) throws IOException {
        var model = OciGenAiChatCompletionModelTests.createChatCompletionModel(null, modelId);
        var entity = new OciGenAiChatCompletionRequestEntity(new UnifiedChatInput(body, stream), model);
        return entityAsMap(Strings.toString(entity));
    }
}
