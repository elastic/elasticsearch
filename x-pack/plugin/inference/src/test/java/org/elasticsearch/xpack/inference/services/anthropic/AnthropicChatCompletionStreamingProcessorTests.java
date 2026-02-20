/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.anthropic;

import org.apache.commons.lang3.tuple.Pair;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.inference.results.StreamingUnifiedChatCompletionResults;
import org.elasticsearch.xpack.inference.external.response.streaming.ServerSentEvent;
import org.hamcrest.Matchers;

import java.util.ArrayDeque;
import java.util.List;
import java.util.concurrent.Flow;

import static org.elasticsearch.xpack.inference.common.DelegatingProcessorTests.onError;
import static org.elasticsearch.xpack.inference.common.DelegatingProcessorTests.onNext;
import static org.elasticsearch.xpack.inference.external.response.streaming.StreamingInferenceTestUtils.events;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class AnthropicChatCompletionStreamingProcessorTests extends ESTestCase {

    public void testParseSuccess() {
        var item = events(
            List.of(
                Pair.of("message_start", """
                    {
                         "type": "message_start",
                         "message": {
                             "model": "claude-3-5-haiku-20241022",
                             "id": "msg_vrtx_01F9nngkx9PojtBCkhj9xP2v",
                             "type": "message",
                             "role": "assistant",
                             "content": [],
                             "stop_reason": null,
                             "stop_sequence": null,
                             "usage": {
                                 "input_tokens": 393,
                                 "cache_creation_input_tokens": 0,
                                 "cache_read_input_tokens": 0,
                                 "output_tokens": 1
                             }
                         }
                    }
                    """),
                Pair.of("content_block_start", """
                    {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}"""),
                Pair.of("ping", """
                    {"type": "ping"}"""),
                Pair.of("content_block_delta", """
                    {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Hello"}}"""),
                Pair.of("content_block_delta", """
                    {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"World"}}"""),
                Pair.of("content_block_stop", """
                    {"type":"content_block_stop","index":0}"""),
                Pair.of("content_block_start", """
                    {
                        "type": "content_block_start",
                        "index": 1,
                        "content_block": {
                            "type": "tool_use",
                            "id": "toolu_vrtx_01GooUb1exnL7s8QrUgAQvQj",
                            "name": "get_weather",
                            "input": {}
                        }
                    }
                    """),
                Pair.of("content_block_delta", """
                    {"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"Hello"}}"""),
                Pair.of("content_block_delta", """
                    {"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"World"}}"""),
                Pair.of("content_block_stop", """
                    {"type":"content_block_stop","index":1}"""),
                Pair.of("message_delta", """
                    {
                        "type": "message_delta",
                        "delta": {
                            "stop_reason": "tool_use",
                            "stop_sequence": null
                        },
                        "usage": {
                            "output_tokens": 99
                        }
                    }
                    """),
                Pair.of("message_stop", """
                    {"type":"message_stop"}""")
            )
        );

        var response = onNext(new AnthropicChatCompletionStreamingProcessor((noOp1, noOp2) -> {
            fail("This should not be called");
            return null;
        }), item);
        assertThat(response.chunks().size(), equalTo(8));
        {
            assertMessageStartBlock(response);
        }
        {
            assertContent(response, "");
        }
        {
            assertContent(response, "Hello");
        }
        {
            assertContent(response, "World");
        }
        {
            assertToolUseContentStartBlock(response);
        }
        {
            assertToolUseArguments(response, "Hello");
        }
        {
            assertToolUseArguments(response, "World");
        }
        {
            assertMessageDeltaBlock(response);
        }
    }

    public void testParseAlternateFieldOrder() {
        var item = events(List.of(Pair.of("message_start", """
            {
                "message": {
                    "content": [],
                    "id": "msg_vrtx_01F9nngkx9PojtBCkhj9xP2v",
                    "model": "claude-3-5-haiku-20241022",
                    "role": "assistant",
                    "stop_reason": null,
                    "stop_sequence": null,
                    "type": "message",
                    "usage": {
                        "cache_creation_input_tokens": 0,
                        "cache_read_input_tokens": 0,
                        "input_tokens": 393,
                        "output_tokens": 1
                    }
                },
                "type": "message_start"
            }
            """)));

        var response = onNext(new AnthropicChatCompletionStreamingProcessor((noOp1, noOp2) -> {
            fail("This should not be called");
            return null;
        }), item);
        assertThat(response.chunks().size(), equalTo(1));
        {
            assertMessageStartBlock(response);
        }
    }

    private static void assertMessageDeltaBlock(StreamingUnifiedChatCompletionResults.Results response) {
        var chatCompletionChunk = response.chunks().remove();
        var choices = chatCompletionChunk.choices();
        assertThat(choices.size(), is(1));
        assertThat(choices.getFirst().index(), is(0));
        assertNull(choices.getFirst().delta().toolCalls());
        assertNull(choices.getFirst().delta().content());
        assertThat(choices.getFirst().finishReason(), is("tool_use"));
        assertUsage(chatCompletionChunk.usage(), 99, 0, 99);
    }

    private static void assertMessageStartBlock(StreamingUnifiedChatCompletionResults.Results response) {
        var chatCompletionChunk = response.chunks().remove();
        assertThat(chatCompletionChunk.id(), is("msg_vrtx_01F9nngkx9PojtBCkhj9xP2v"));
        assertThat(chatCompletionChunk.model(), is("claude-3-5-haiku-20241022"));
        assertUsage(chatCompletionChunk.usage(), 1, 393, 394);
        assertThat(chatCompletionChunk.choices().size(), is(1));
        var choice = chatCompletionChunk.choices().getFirst();
        assertThat(choice.index(), is(0));
        assertThat(choice.delta().role(), is("assistant"));
    }

    private static void assertToolUseContentStartBlock(StreamingUnifiedChatCompletionResults.Results response) {
        var choices = response.chunks().remove().choices();
        assertThat(choices.size(), is(1));
        assertThat(choices.getFirst().index(), is(1));
        var toolCalls = choices.getFirst().delta().toolCalls();
        assertThat(toolCalls.size(), is(1));
        assertThat(toolCalls.getFirst().index(), is(0));
        assertThat(toolCalls.getFirst().id(), is("toolu_vrtx_01GooUb1exnL7s8QrUgAQvQj"));
        var function = toolCalls.getFirst().function();
        assertThat(function.arguments(), is("{}"));
        assertThat(function.name(), is("get_weather"));
    }

    private static void assertToolUseArguments(StreamingUnifiedChatCompletionResults.Results response, String arguments) {
        var choices = response.chunks().remove().choices();
        assertThat(choices.size(), is(1));
        assertThat(choices.getFirst().index(), is(1));
        var toolCalls = choices.getFirst().delta().toolCalls();
        assertThat(toolCalls.size(), is(1));
        assertThat(toolCalls.getFirst().index(), is(0));
        assertNull(toolCalls.getFirst().id());
        var function = toolCalls.getFirst().function();
        assertThat(function.arguments(), Matchers.is(arguments));
        assertNull(function.name());
    }

    private static void assertContent(StreamingUnifiedChatCompletionResults.Results response, String content) {
        var choices = response.chunks().remove().choices();
        assertThat(choices.size(), is(1));
        assertThat(choices.getFirst().index(), is(0));
        assertThat(choices.getFirst().delta().content(), Matchers.is(content));
    }

    private static void assertUsage(
        StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Usage usage,
        int completion,
        int prompt,
        int total
    ) {
        assertThat(usage.completionTokens(), is(completion));
        assertThat(usage.promptTokens(), is(prompt));
        assertThat(usage.totalTokens(), is(total));
    }

    public void testEmptyResultsRequestsMoreData() throws Exception {
        var emptyDeque = new ArrayDeque<ServerSentEvent>();

        var processor = new AnthropicChatCompletionStreamingProcessor((noOp1, noOp2) -> {
            fail("This should not be called");
            return null;
        });

        Flow.Subscriber<ChunkedToXContent> downstream = mock();
        processor.subscribe(downstream);

        Flow.Subscription upstream = mock();
        processor.onSubscribe(upstream);

        processor.next(emptyDeque);

        verify(upstream, times(1)).request(1);
        verify(downstream, times(0)).onNext(any());
    }

    public void testOnError() {
        var expectedException = new RuntimeException("hello");

        var processor = new AnthropicChatCompletionStreamingProcessor((noOp1, noOp2) -> { throw expectedException; });

        assertThat(onError(processor, events(List.of(Pair.of("error", "error")))), sameInstance(expectedException));
    }

    public void testMissingRequiredModelField() {
        var item = events(List.of(Pair.of("message_start", """
            {
                 "type": "message_start",
                 "message": {
                     "id": "msg_vrtx_01F9nngkx9PojtBCkhj9xP2v",
                     "type": "message",
                     "role": "assistant",
                     "content": [],
                     "stop_reason": null,
                     "stop_sequence": null,
                     "usage": {
                         "input_tokens": 393,
                         "cache_creation_input_tokens": 0,
                         "cache_read_input_tokens": 0,
                         "output_tokens": 1
                     }
                 }
            }
            """)));
        Throwable actual = onError(new AnthropicChatCompletionStreamingProcessor((noOp1, noOp2) -> noOp2), item);
        assertThat(actual, is(instanceOf(IllegalStateException.class)));
        assertThat(actual.getMessage(), is("Failed to find required field [model] in Anthropic chat completions response"));
    }

    public void testInvalidTypeModelField() {
        var item = events(List.of(Pair.of("message_start", """
            {
                 "type": "message_start",
                 "message": {
                     "model": 2,
                     "id": "msg_vrtx_01F9nngkx9PojtBCkhj9xP2v",
                     "type": "message",
                     "role": "assistant",
                     "content": [],
                     "stop_reason": null,
                     "stop_sequence": null,
                     "usage": {
                         "input_tokens": 393,
                         "cache_creation_input_tokens": 0,
                         "cache_read_input_tokens": 0,
                         "output_tokens": 1
                     }
                 }
            }
            """)));
        Throwable actual = onError(new AnthropicChatCompletionStreamingProcessor((noOp1, noOp2) -> noOp2), item);
        assertThat(actual, is(instanceOf(IllegalStateException.class)));
        assertThat(
            actual.getMessage(),
            is("Field [model] in Anthropic chat completions response is of unexpected type [Integer]. Expected type is [String].")
        );
    }
}
