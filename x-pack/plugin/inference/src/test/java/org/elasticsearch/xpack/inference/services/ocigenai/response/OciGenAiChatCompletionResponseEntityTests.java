/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ocigenai.response;

import org.apache.http.HttpResponse;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.results.CompletionResults;
import org.elasticsearch.xpack.core.inference.results.completion.ChatCompletionChunkResponse;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.request.OutboundRequest;
import org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiChatApiFormat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;

public class OciGenAiChatCompletionResponseEntityTests extends ESTestCase {

    private static final String GENERIC_RESPONSE = """
        {
          "chatResponse": {
            "apiFormat": "GENERIC",
            "choices": [ {
              "finishReason": "stop",
              "index": 0,
              "logprobs": {},
              "message": {
                "content": [ { "text": "Hello, how are you today?", "type": "TEXT" } ],
                "role": "ASSISTANT",
                "toolCalls": []
              }
            } ],
            "timeCreated": "2026-09-18T19:22:24.776Z",
            "usage": { "completionTokens": 8, "promptTokens": 45, "totalTokens": 53 }
          },
          "modelId": "meta.llama-3.3-70b-instruct",
          "modelVersion": "1.0.0"
        }
        """;

    private static final String GENERIC_TOOL_CALL_RESPONSE = """
        {
          "chatResponse": {
            "apiFormat": "GENERIC",
            "choices": [ {
              "finishReason": "tool_calls",
              "index": 0,
              "message": {
                "role": "ASSISTANT",
                "toolCalls": [ {
              "arguments": "{\\"city\\": \\"Paris\\"}", "id": "chatcmpl-tool-a52781794a76726d", "name": "get_weather", "type": "FUNCTION"
            } ]
              }
            } ],
            "usage": { "completionTokens": 23, "promptTokens": 245, "totalTokens": 268 }
          },
          "modelId": "meta.llama-3.3-70b-instruct",
          "modelVersion": "1.0.0"
        }
        """;

    private static final String COHERE_RESPONSE = """
        {
          "chatResponse": {
            "apiFormat": "COHERE",
            "chatHistory": [
          { "message": "Say hello in five words.", "role": "USER" },
          { "message": "Hello, how are you?", "role": "CHATBOT" }
        ],
            "finishReason": "COMPLETE",
            "text": "Hello, how are you?",
            "usage": { "completionTokens": 6, "promptTokens": 13, "totalTokens": 19 }
          },
          "modelId": "cohere.command-a-03-2025",
          "modelVersion": "1.0"
        }
        """;

    public void testFromResponse_Generic() throws IOException {
        var chunk = OciGenAiChatCompletionResponseEntity.fromResponse(GENERIC_RESPONSE.getBytes(StandardCharsets.UTF_8), "id-1");

        assertThat(chunk.id(), is("id-1"));
        assertThat(chunk.model(), is("meta.llama-3.3-70b-instruct"));
        assertThat(chunk.object(), is("chat.completion"));
        assertThat(chunk.choices(), hasSize(1));
        var choice = chunk.choices().getFirst();
        assertThat(choice.index(), is(0));
        assertThat(choice.finishReason(), is("stop"));
        assertThat(choice.message().role(), is("assistant"));
        assertThat(choice.message().content(), is("Hello, how are you today?"));
        assertThat(choice.message().toolCalls(), nullValue());
        assertThat(chunk.usage(), notNullValue());
        assertThat(chunk.usage().completionTokens(), is(8));
        assertThat(chunk.usage().promptTokens(), is(45));
        assertThat(chunk.usage().totalTokens(), is(53));
    }

    public void testFromResponse_GenericToolCalls() throws IOException {
        var chunk = OciGenAiChatCompletionResponseEntity.fromResponse(GENERIC_TOOL_CALL_RESPONSE.getBytes(StandardCharsets.UTF_8), "id-1");

        var choice = chunk.choices().getFirst();
        assertThat(choice.finishReason(), is("tool_calls"));
        assertThat(choice.message().content(), nullValue());
        assertThat(choice.message().toolCalls(), hasSize(1));
        var toolCall = choice.message().toolCalls().getFirst();
        assertThat(toolCall.index(), is(0));
        assertThat(toolCall.id(), is("chatcmpl-tool-a52781794a76726d"));
        assertThat(toolCall.type(), is("function"));
        assertThat(toolCall.function().name(), is("get_weather"));
        assertThat(toolCall.function().arguments(), is("{\"city\": \"Paris\"}"));
    }

    public void testFromResponse_Cohere() throws IOException {
        var chunk = OciGenAiChatCompletionResponseEntity.fromResponse(COHERE_RESPONSE.getBytes(StandardCharsets.UTF_8), "id-2");

        assertThat(chunk.model(), is("cohere.command-a-03-2025"));
        assertThat(chunk.choices(), hasSize(1));
        var choice = chunk.choices().getFirst();
        assertThat(choice.finishReason(), is("stop"));
        assertThat(choice.message().role(), is("assistant"));
        assertThat(choice.message().content(), is("Hello, how are you?"));
        assertThat(chunk.usage().totalTokens(), is(19));
    }

    public void testFromResponse_ViaHttpResult_GeneratesAnId() throws IOException {
        var chunk = OciGenAiChatCompletionResponseEntity.fromResponse(mock(OutboundRequest.class), httpResult(GENERIC_RESPONSE));

        assertThat(chunk.id(), notNullValue());
        assertThat(chunk.choices().getFirst().message().content(), is("Hello, how are you today?"));
    }

    public void testFromResponseAsCompletion() throws IOException {
        var results = OciGenAiChatCompletionResponseEntity.fromResponseAsCompletion(
            mock(OutboundRequest.class),
            httpResult(GENERIC_RESPONSE)
        );

        assertThat(results.getResults(), is(List.of(new CompletionResults.Result("Hello, how are you today?"))));

        var cohereResults = OciGenAiChatCompletionResponseEntity.fromResponseAsCompletion(
            mock(OutboundRequest.class),
            httpResult(COHERE_RESPONSE)
        );
        assertThat(cohereResults.getResults(), is(List.of(new CompletionResults.Result("Hello, how are you?"))));
    }

    public void testFromResponse_FailsWithoutChatResponse() {
        var exception = expectThrows(
            IllegalStateException.class,
            () -> OciGenAiChatCompletionResponseEntity.fromResponse("{ \"modelId\": \"x\" }".getBytes(StandardCharsets.UTF_8), "id")
        );

        assertThat(exception.getMessage(), is("Failed to find required field [chatResponse] in OCI Generative AI chat response"));
    }

    public void testParseStreamingEvent_GenericContentDelta() throws IOException {
        var chunk = parseEvent("""
            {"index":0,"message":{"role":"ASSISTANT","content":[{"type":"TEXT","text":"Hello"}]},"pad":"aaaaa"}
            """);

        assertThat(chunk, notNullValue());
        assertThat(chunk.id(), is("stream-id"));
        assertThat(chunk.model(), is("meta.llama-3.3-70b-instruct"));
        assertThat(chunk.object(), is("chat.completion.chunk"));
        assertThat(chunk.usage(), nullValue());
        var choice = chunk.choices().getFirst();
        assertThat(choice.index(), is(0));
        assertThat(choice.finishReason(), nullValue());
        assertThat(choice.message().role(), is("assistant"));
        assertThat(choice.message().content(), is("Hello"));
    }

    public void testParseStreamingEvent_GenericFinalEvent() throws IOException {
        var chunk = parseEvent("""
            {"message":{"role":"ASSISTANT","content":[{"type":"TEXT","text":""}]},"finishReason":"stop","pad":"aaa"}
            """);

        assertThat(chunk, notNullValue());
        var choice = chunk.choices().getFirst();
        assertThat(choice.index(), is(0));
        assertThat(choice.finishReason(), is("stop"));
        assertThat(choice.message().content(), nullValue());
    }

    public void testParseStreamingEvent_GenericToolCallDeltas() throws IOException {
        var first = parseEvent("""
            {"index":0,"message":{"role":"ASSISTANT","toolCalls":[{"type":"FUNCTION","id":"call_1","name":"get_weather"}]},"pad":"a"}
            """);
        var second = parseEvent("""
            {"index":0,"message":{"role":"ASSISTANT","toolCalls":[{"type":"FUNCTION","arguments":"{\\"city\\": \\""}]},"pad":"aaa"}
            """);

        var firstToolCall = first.choices().getFirst().message().toolCalls().getFirst();
        assertThat(firstToolCall.id(), is("call_1"));
        assertThat(firstToolCall.function().name(), is("get_weather"));
        assertThat(firstToolCall.function().arguments(), nullValue());

        var secondToolCall = second.choices().getFirst().message().toolCalls().getFirst();
        assertThat(secondToolCall.index(), is(0));
        assertThat(secondToolCall.id(), nullValue());
        assertThat(secondToolCall.function().arguments(), is("{\"city\": \""));
    }

    public void testParseStreamingEvent_CohereDelta() throws IOException {
        var chunk = parseEvent("""
            {"apiFormat":"COHERE","text":"Hello","pad":"aaaaa"}
            """);

        assertThat(chunk, notNullValue());
        var choice = chunk.choices().getFirst();
        assertThat(choice.message().content(), is("Hello"));
        assertThat(choice.message().role(), is("assistant"));
        assertThat(choice.finishReason(), nullValue());
    }

    public void testParseStreamingEvent_CohereFinalEvent_DoesNotRepeatTheText() throws IOException {
        var chunk = parseEvent("""
            {"apiFormat":"COHERE","text":"Hello","chatHistory":[],"finishReason":"COMPLETE","pad":"a"}
            """);

        assertThat(chunk, notNullValue());
        var choice = chunk.choices().getFirst();
        assertThat(choice.message().content(), nullValue());
        assertThat(choice.finishReason(), is("stop"));
    }

    public void testParseStreamingEvent_ReturnsNullForEmptyEvent() throws IOException {
        assertThat(parseEvent("{\"pad\":\"aaa\"}"), nullValue());
        assertThat(parseEvent("{\"apiFormat\":\"COHERE\",\"text\":\"\",\"pad\":\"aaa\"}"), nullValue());
    }

    public void testToOpenAiFinishReason() {
        assertThat(OciGenAiChatCompletionResponseEntity.toOpenAiFinishReason(OciGenAiChatApiFormat.COHERE, "COMPLETE"), is("stop"));
        assertThat(OciGenAiChatCompletionResponseEntity.toOpenAiFinishReason(OciGenAiChatApiFormat.COHERE, "MAX_TOKENS"), is("length"));
        assertThat(
            OciGenAiChatCompletionResponseEntity.toOpenAiFinishReason(OciGenAiChatApiFormat.COHERE, "ERROR_TOXIC"),
            is("error_toxic")
        );
        assertThat(
            OciGenAiChatCompletionResponseEntity.toOpenAiFinishReason(OciGenAiChatApiFormat.GENERIC, "tool_calls"),
            is("tool_calls")
        );
        assertThat(OciGenAiChatCompletionResponseEntity.toOpenAiFinishReason(OciGenAiChatApiFormat.COHERE, null), nullValue());
    }

    private static ChatCompletionChunkResponse parseEvent(String json) throws IOException {
        var parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);
        try (XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, json)) {
            return OciGenAiChatCompletionResponseEntity.parseStreamingEvent(parser, "stream-id", "meta.llama-3.3-70b-instruct");
        }
    }

    private static HttpResult httpResult(String body) {
        return new HttpResult(mock(HttpResponse.class), body.getBytes(StandardCharsets.UTF_8));
    }
}
