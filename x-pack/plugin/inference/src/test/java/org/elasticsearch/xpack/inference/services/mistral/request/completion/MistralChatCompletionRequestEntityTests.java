/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mistral.request.completion;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.inference.UnifiedCompletionRequestBody;
import org.elasticsearch.inference.completion.ContentString;
import org.elasticsearch.inference.completion.Message;
import org.elasticsearch.inference.completion.Reasoning;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class MistralChatCompletionRequestEntityTests extends ESTestCase {

    private static final String ROLE = "user";

    public void testSerializationWithModelIdStreaming() throws IOException {
        testSerialization("test-endpoint", true, """
            {
                "messages": [
                    {
                        "content": "Hello, world!",
                        "role": "user"
                    }
                ],
                "model": "test-endpoint",
                "n": 1,
                "stream": true
            }
            """);
    }

    public void testSerializationWithModelIdNonStreaming() throws IOException {
        testSerialization("test-endpoint", false, """
            {
                "messages": [
                    {
                        "content": "Hello, world!",
                        "role": "user"
                    }
                ],
                "model": "test-endpoint",
                "n": 1,
                "stream": false
            }
            """);
    }

    public void testSerializationWithoutModelIdStreaming() throws IOException {
        testSerialization(null, true, """
            {
                "messages": [
                    {
                        "content": "Hello, world!",
                        "role": "user"
                    }
                ],
                "n": 1,
                "stream": true
            }
            """);
    }

    public void testSerializationWithoutModelIdNonStreaming() throws IOException {
        testSerialization(null, false, """
            {
                "messages": [
                    {
                        "content": "Hello, world!",
                        "role": "user"
                    }
                ],
                "n": 1,
                "stream": false
            }
            """);
    }

    public void testReasoningEffortFromRequestIsForwardedUntouched() throws IOException {
        // `low` is not supported by Mistral reasoning models; it must reach Mistral so that Mistral can reject it.
        var request = body(List.of(user("hi")), null, null, new Reasoning(Reasoning.ReasoningEffort.LOW, null, null, null));

        var json = serialize(request, "service-default", false);

        assertThat(json.get("reasoning_effort"), is("low"));
        assertFalse("the nested unified object must not be forwarded", json.containsKey("reasoning"));
    }

    public void testReasoningEffortNoneFromRequestIsForwarded() throws IOException {
        var request = body(List.of(user("hi")), null, null, new Reasoning(Reasoning.ReasoningEffort.NONE, null, null, null));

        assertThat(serialize(request, null, false).get("reasoning_effort"), is("none"));
    }

    public void testReasoningEffortFallsBackToServiceSetting() throws IOException {
        var request = body(List.of(user("hi")), null, null, null);

        assertThat(serialize(request, "high", false).get("reasoning_effort"), is("high"));
    }

    public void testRequestReasoningEffortWinsOverServiceSetting() throws IOException {
        var request = body(List.of(user("hi")), null, null, new Reasoning(Reasoning.ReasoningEffort.NONE, null, null, null));

        assertThat(serialize(request, "high", false).get("reasoning_effort"), is("none"));
    }

    public void testNoReasoningEffortWhenNeitherIsSet() throws IOException {
        assertFalse(serialize(body(List.of(user("hi")), null, null, null), null, false).containsKey("reasoning_effort"));
    }

    public void testAssistantReasoningIsReplayedAsThinkingChunk() throws IOException {
        var assistant = new Message(new ContentString("the answer"), "assistant", null, null, "let me think", null);

        var messages = messages(serialize(body(List.of(user("q"), assistant), null, null, null), null, false));

        assertThat(messages.get(0).get("content"), is("q"));
        assertThat(
            messages.get(1).get("content"),
            is(
                List.of(
                    Map.of("type", "thinking", "thinking", List.of(Map.of("type", "text", "text", "let me think"))),
                    Map.of("type", "text", "text", "the answer")
                )
            )
        );
        assertFalse("reasoning must not be sent as a message field", messages.get(1).containsKey("reasoning"));
    }

    public void testAssistantReasoningIsReplayedWithToolCalls() throws IOException {
        var assistant = new Message(null, "assistant", null, null, "plan the call", null);

        var messages = messages(serialize(body(List.of(user("q"), assistant), null, null, null), null, false));

        assertThat(
            messages.get(1).get("content"),
            is(List.of(Map.of("type", "thinking", "thinking", List.of(Map.of("type", "text", "text", "plan the call")))))
        );
    }

    public void testAssistantWithoutReasoningIsSentAsBefore() throws IOException {
        var assistant = new Message(new ContentString("the answer"), "assistant", null, null);

        var messages = messages(serialize(body(List.of(user("q"), assistant), null, null, null), null, false));

        assertThat(messages.get(1), is(Map.of("content", "the answer", "role", "assistant")));
    }

    public void testReasoningOnNonAssistantMessageIsNotReplayed() throws IOException {
        var userWithReasoning = new Message(new ContentString("q"), "user", null, null, "ignored", null);

        var messages = messages(serialize(body(List.of(userWithReasoning), null, null, null), null, false));

        assertThat(messages.get(0), is(Map.of("content", "q", "role", "user")));
    }

    public void testTemperatureZeroWithoutTopPSetsTopPToOne() throws IOException {
        var json = serialize(body(List.of(user("hi")), 0f, null, null), null, false);

        assertThat(json.get("temperature"), is(0.0));
        assertThat(json.get("top_p"), is(1.0));
    }

    public void testTemperatureZeroKeepsCallerTopP() throws IOException {
        var json = serialize(body(List.of(user("hi")), 0f, 0.5f, null), null, false);

        assertThat(json.get("top_p"), is(0.5));
    }

    public void testNonZeroTemperatureDoesNotSetTopP() throws IOException {
        var json = serialize(body(List.of(user("hi")), 0.7f, null, null), null, false);

        assertFalse(json.containsKey("top_p"));
    }

    public void testDeveloperRoleIsSentAsSystem() throws IOException {
        var developer = new Message(new ContentString("be brief"), "developer", null, null);

        var messages = messages(serialize(body(List.of(developer, user("hi")), null, null, null), null, false));

        assertThat(messages.get(0).get("role"), is("system"));
        assertThat(messages.get(1).get("role"), is("user"));
    }

    public void testTopLevelUserFieldIsNeverSent() throws IOException {
        var request = new UnifiedCompletionRequestBody(
            List.of(user("hi")),
            "m",
            10L,
            List.of("stop"),
            0.2f,
            null,
            null,
            0.9f,
            null,
            null,
            "s"
        );

        var json = serialize(request, null, true);

        assertFalse(json.containsKey("user"));
        assertFalse(json.containsKey("session_id"));
        assertFalse(json.containsKey("cache_control"));
    }

    public void testMaxCompletionTokensIsSentAsMaxTokens() throws IOException {
        var request = new UnifiedCompletionRequestBody(List.of(user("hi")), null, 42L, null, null, null, null, null, null, null, null);

        var json = serialize(request, null, false);

        assertThat(json.get("max_tokens"), is(42));
        assertFalse(json.containsKey("max_completion_tokens"));
    }

    public void testStreamingDoesNotSendStreamOptions() throws IOException {
        var json = serialize(body(List.of(user("hi")), null, null, null), null, true);

        assertThat(json.get("stream"), is(true));
        assertFalse(json.containsKey("stream_options"));
    }

    private static Message user(String text) {
        return new Message(new ContentString(text), "user", null, null);
    }

    private static UnifiedCompletionRequestBody body(List<Message> messages, Float temperature, Float topP, Reasoning reasoning) {
        return new UnifiedCompletionRequestBody(messages, null, null, null, temperature, null, null, topP, reasoning, null, null);
    }

    private static Map<String, Object> serialize(UnifiedCompletionRequestBody request, String defaultEffort, boolean stream)
        throws IOException {
        var entity = new MistralChatCompletionRequestEntity(new UnifiedChatInput(request, stream), "mistral-reasoning-test", defaultEffort);
        XContentBuilder builder = JsonXContent.contentBuilder();
        entity.toXContent(builder, ToXContent.EMPTY_PARAMS);
        return XContentHelper.convertToMap(JsonXContent.jsonXContent, Strings.toString(builder), false);
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> messages(Map<String, Object> json) {
        return (List<Map<String, Object>>) json.get("messages");
    }

    private static void testSerialization(String modelId, boolean isStreaming, String expectedJson) throws IOException {
        Message message = new Message(new ContentString("Hello, world!"), ROLE, null, null);
        var messageList = new ArrayList<Message>();
        messageList.add(message);

        var unifiedRequest = UnifiedCompletionRequestBody.of(messageList);

        UnifiedChatInput unifiedChatInput = new UnifiedChatInput(unifiedRequest, isStreaming);

        MistralChatCompletionRequestEntity entity = new MistralChatCompletionRequestEntity(unifiedChatInput, modelId);

        XContentBuilder builder = JsonXContent.contentBuilder();
        entity.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertEquals(XContentHelper.stripWhitespace(expectedJson), Strings.toString(builder));
    }
}
