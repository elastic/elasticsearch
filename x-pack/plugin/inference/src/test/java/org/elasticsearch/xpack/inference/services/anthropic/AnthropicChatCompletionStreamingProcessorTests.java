/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.anthropic;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.Level;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.inference.completion.ReasoningDetail;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.xpack.core.inference.results.StreamingUnifiedChatCompletionResults;
import org.elasticsearch.xpack.core.inference.results.completion.ChatCompletionUsageResponse;
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

    private static final String MSG_ID = "msg_vrtx_01F9nngkx9PojtBCkhj9xP2v";
    private static final String MODEL = "claude-3-5-haiku-20241022";
    private static final String TOOL_ID = "toolu_vrtx_01GooUb1exnL7s8QrUgAQvQj";
    private static final String FUNCTION_TYPE = "function";
    private static final String ANTHROPIC_FORMAT = "anthropic-claude-v1";
    private static final String CHUNK_OBJECT = "chat.completion.chunk";

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

        var response = onNext(createProcessor(), item);
        assertThat(response.chunks().size(), equalTo(9));
        assertMessageStartBlock(response);
        assertContent(response, "");
        assertContent(response, "Hello");
        assertContent(response, "World");
        assertToolUseContentStartBlock(response, 0, TOOL_ID, "get_weather");
        assertToolUseArguments(response, 0, "Hello");
        assertToolUseArguments(response, 0, "World");
        assertMessageDeltaBlock(response);
        assertMessageStopUsageBlock(response, 99, 393, 492, null, null);
    }

    private static AnthropicChatCompletionStreamingProcessor createProcessor() {
        return createProcessor(false);
    }

    private static AnthropicChatCompletionStreamingProcessor createProcessor(boolean excludeReasoning) {
        return new AnthropicChatCompletionStreamingProcessor((noOp1, noOp2) -> {
            fail("Error parser should not be called");
            return null;
        }, excludeReasoning);
    }

    public void testParseSuccess_MultipleToolCalls() {
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
                            "stop_reason": null,
                            "usage": {"input_tokens": 10, "output_tokens": 1,
                                      "cache_creation_input_tokens": 0, "cache_read_input_tokens": 0}
                        }
                    }
                    """),
                Pair.of("content_block_start", """
                    {"type":"content_block_start","index":0,
                     "content_block":{"type":"tool_use","id":"tool_id_0","name":"tool_a","input":{}}}"""),
                Pair.of("content_block_start", """
                    {"type":"content_block_start","index":1,
                     "content_block":{"type":"tool_use","id":"tool_id_1","name":"tool_b","input":{}}}"""),
                Pair.of("content_block_delta", """
                    {"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"arg_a"}}"""),
                Pair.of("content_block_delta", """
                    {"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"arg_b"}}"""),
                Pair.of("message_delta", """
                    {"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":20}}"""),
                Pair.of("message_stop", """
                    {"type":"message_stop"}""")
            )
        );

        var response = onNext(createProcessor(), item);

        // message_start + 2 tool_use starts + 2 input_json_deltas + message_delta + message_stop
        assertThat(response.chunks().size(), equalTo(7));

        // message_start: role chunk
        var startChunk = response.chunks().remove();
        assertThat(startChunk.choices().getFirst().message().role(), is("assistant"));

        // first tool_use: index 0
        var firstToolStart = response.chunks().remove();
        assertThat(firstToolStart.choices().size(), is(1));
        assertThat(firstToolStart.choices().getFirst().index(), is(0));
        var firstToolCall = firstToolStart.choices().getFirst().message().toolCalls().getFirst();
        assertThat(firstToolCall.index(), is(0));
        assertThat(firstToolCall.id(), is("tool_id_0"));
        assertThat(firstToolCall.function().name(), is("tool_a"));
        assertThat(firstToolCall.function().arguments(), is(""));
        assertThat(firstToolCall.type(), is(FUNCTION_TYPE));

        // second tool_use: index 1
        var secondToolStart = response.chunks().remove();
        assertThat(secondToolStart.choices().size(), is(1));
        assertThat(secondToolStart.choices().getFirst().index(), is(0));
        var secondToolCall = secondToolStart.choices().getFirst().message().toolCalls().getFirst();
        assertThat(secondToolCall.index(), is(1));
        assertThat(secondToolCall.id(), is("tool_id_1"));
        assertThat(secondToolCall.function().name(), is("tool_b"));

        // input_json_delta for block 0 -> tool index 0
        var deltaForToolA = response.chunks().remove();
        var toolCallDeltaA = deltaForToolA.choices().getFirst().message().toolCalls().getFirst();
        assertThat(toolCallDeltaA.index(), is(0));
        assertNull(toolCallDeltaA.id());
        assertThat(toolCallDeltaA.function().arguments(), is("arg_a"));

        // input_json_delta for block 1 -> tool index 1
        var deltaForToolB = response.chunks().remove();
        var toolCallDeltaB = deltaForToolB.choices().getFirst().message().toolCalls().getFirst();
        assertThat(toolCallDeltaB.index(), is(1));
        assertNull(toolCallDeltaB.id());
        assertThat(toolCallDeltaB.function().arguments(), is("arg_b"));

        // message_delta: finish reason tool_calls
        var messageDelta = response.chunks().remove();
        assertThat(messageDelta.choices().getFirst().finishReason(), is("tool_calls"));
        assertNull(messageDelta.usage());

        // message_stop: usage
        assertMessageStopUsageBlock(response, 20, 10, 30, null, null);
    }

    public void testParseSuccess_ThinkingBlock() {
        var item = events(List.of(Pair.of("message_start", """
            {
                "type": "message_start",
                "message": {
                    "model": "claude-3-5-haiku-20241022",
                    "id": "msg_vrtx_01F9nngkx9PojtBCkhj9xP2v",
                    "role": "assistant",
                    "stop_reason": null,
                    "usage": {"input_tokens": 5, "output_tokens": 1,
                              "cache_creation_input_tokens": 0, "cache_read_input_tokens": 0}
                }
            }
            """), Pair.of("content_block_start", """
            {"type":"content_block_start","index":0,
             "content_block":{"type":"thinking","thinking":"initial"}}"""), Pair.of("content_block_delta", """
            {"type":"content_block_delta","index":0,
             "delta":{"type":"thinking_delta","thinking":"more"}}"""), Pair.of("content_block_delta", """
            {"type":"content_block_delta","index":0,
             "delta":{"type":"signature_delta","signature":"sig123"}}"""), Pair.of("message_delta", """
            {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":10}}"""), Pair.of("message_stop", """
            {"type":"message_stop"}""")));

        var response = onNext(createProcessor(), item);

        // message_start + thinking_start + thinking_delta + signature_delta + message_delta + message_stop
        assertThat(response.chunks().size(), equalTo(6));

        response.chunks().remove(); // message_start

        // thinking block start: reasoning field + TextReasoningDetail with initial text
        var thinkingStart = response.chunks().remove();
        assertThat(thinkingStart.choices().getFirst().index(), is(0));
        var startDelta = thinkingStart.choices().getFirst().message();
        assertThat(startDelta.reasoning(), is("initial"));
        assertThat(startDelta.reasoningDetails().size(), is(1));
        var startDetail = (ReasoningDetail.TextReasoningDetail) startDelta.reasoningDetails().getFirst();
        assertThat(startDetail.format(), is(ANTHROPIC_FORMAT));
        assertThat(startDetail.index(), is(0L));
        assertThat(startDetail.text(), is("initial"));
        assertNull(startDetail.signature());

        // thinking_delta: reasoning + TextReasoningDetail with text
        var thinkingDelta = response.chunks().remove();
        var thinkingDeltaDelta = thinkingDelta.choices().getFirst().message();
        assertThat(thinkingDeltaDelta.reasoning(), is("more"));
        var thinkingDeltaDetail = (ReasoningDetail.TextReasoningDetail) thinkingDeltaDelta.reasoningDetails().getFirst();
        assertThat(thinkingDeltaDetail.format(), is(ANTHROPIC_FORMAT));
        assertThat(thinkingDeltaDetail.index(), is(0L));
        assertThat(thinkingDeltaDetail.text(), is("more"));
        assertNull(thinkingDeltaDetail.signature());

        // signature_delta: TextReasoningDetail with signature only
        var sigDelta = response.chunks().remove();
        var sigDeltaDelta = sigDelta.choices().getFirst().message();
        assertNull(sigDeltaDelta.reasoning());
        var sigDetail = (ReasoningDetail.TextReasoningDetail) sigDeltaDelta.reasoningDetails().getFirst();
        assertThat(sigDetail.index(), is(0L));
        assertNull(sigDetail.text());
        assertThat(sigDetail.signature(), is("sig123"));

        // message_delta: finish reason stop (end_turn maps to stop)
        var messageDelta = response.chunks().remove();
        assertThat(messageDelta.choices().getFirst().finishReason(), is("stop"));

        // message_stop: usage
        assertMessageStopUsageBlock(response, 10, 5, 15, null, null);
    }

    public void testParseSuccess_RedactedThinkingBlock() {
        var item = events(List.of(Pair.of("message_start", """
            {
                "type": "message_start",
                "message": {
                    "model": "claude-3-5-haiku-20241022",
                    "id": "msg_vrtx_01F9nngkx9PojtBCkhj9xP2v",
                    "role": "assistant",
                    "stop_reason": null,
                    "usage": {"input_tokens": 5, "output_tokens": 1,
                              "cache_creation_input_tokens": 0, "cache_read_input_tokens": 0}
                }
            }
            """), Pair.of("content_block_start", """
            {"type":"content_block_start","index":0,
             "content_block":{"type":"redacted_thinking","data":"encrypted_payload"}}"""), Pair.of("message_delta", """
            {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":5}}"""), Pair.of("message_stop", """
            {"type":"message_stop"}""")));

        var response = onNext(createProcessor(), item);

        assertThat(response.chunks().size(), equalTo(4));

        response.chunks().remove(); // message_start

        var redactedStart = response.chunks().remove();
        assertThat(redactedStart.choices().getFirst().index(), is(0));
        var redactedDelta = redactedStart.choices().getFirst().message();
        assertNull(redactedDelta.reasoning());
        assertThat(redactedDelta.reasoningDetails().size(), is(1));
        var encryptedDetail = (ReasoningDetail.EncryptedReasoningDetail) redactedDelta.reasoningDetails().getFirst();
        assertThat(encryptedDetail.format(), is(ANTHROPIC_FORMAT));
        assertThat(encryptedDetail.index(), is(0L));
        assertThat(encryptedDetail.data(), is("encrypted_payload"));

        response.chunks().remove(); // message_delta

        assertMessageStopUsageBlock(response, 5, 5, 10, null, null);
    }

    public void testParseSuccess_CacheTokensIncludedInPromptTokens() {
        var item = events(List.of(Pair.of("message_start", """
            {
                "type": "message_start",
                "message": {
                    "model": "claude-3-5-haiku-20241022",
                    "id": "msg_vrtx_01F9nngkx9PojtBCkhj9xP2v",
                    "role": "assistant",
                    "stop_reason": null,
                    "usage": {
                        "input_tokens": 100,
                        "output_tokens": 1,
                        "cache_creation_input_tokens": 50,
                        "cache_read_input_tokens": 200
                    }
                }
            }
            """), Pair.of("message_delta", """
            {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":30}}"""), Pair.of("message_stop", """
            {"type":"message_stop"}""")));

        var response = onNext(createProcessor(), item);

        assertThat(response.chunks().size(), equalTo(3));

        // message_start: no usage
        var startChunk = response.chunks().remove();
        assertNull(startChunk.usage());

        // message_delta: no usage, just finish reason
        var deltaChunk = response.chunks().remove();
        assertNull(deltaChunk.usage());

        // message_stop: prompt = 100 + 50 + 200 = 350; total = 350 + 30 = 380; cachedTokens = 200, cacheWriteTokens = 50
        assertMessageStopUsageBlock(response, 30, 350, 380, 200, 50);
    }

    public void testIdModelObjectPropagatedOnEveryChunk() {
        var item = events(List.of(Pair.of("message_start", """
            {
                "type": "message_start",
                "message": {
                    "model": "claude-3-5-haiku-20241022",
                    "id": "msg_vrtx_01F9nngkx9PojtBCkhj9xP2v",
                    "role": "assistant",
                    "stop_reason": null,
                    "usage": {"input_tokens": 5, "output_tokens": 1,
                              "cache_creation_input_tokens": 0, "cache_read_input_tokens": 0}
                }
            }
            """), Pair.of("content_block_delta", """
            {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"hi"}}"""), Pair.of("message_delta", """
            {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":2}}"""), Pair.of("message_stop", """
            {"type":"message_stop"}""")));

        var response = onNext(createProcessor(), item);

        // message_start + text_delta + message_delta + message_stop
        assertThat(response.chunks().size(), equalTo(4));
        for (var chunk : response.chunks()) {
            assertThat(chunk.id(), is(MSG_ID));
            assertThat(chunk.model(), is(MODEL));
            assertThat(chunk.object(), is(CHUNK_OBJECT));
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

        var response = onNext(createProcessor(), item);
        assertThat(response.chunks().size(), equalTo(1));
        assertMessageStartBlock(response);
    }

    public void testParseParallelToolCallsKeepsDistinctIndices() {
        // Two tool_use blocks (Anthropic content block indices 1 and 2, after a text block at 0) must stream as tool calls
        // with distinct monotonically increasing indices, so a client accumulating arguments by index does not merge them.
        var item = events(List.of(Pair.of("content_block_start", """
            {
                "type": "content_block_start",
                "index": 1,
                "content_block": {"type": "tool_use", "id": "toolu_01", "name": "get_weather", "input": {}}
            }
            """), Pair.of("content_block_delta", """
            {
                "type": "content_block_delta",
                "index": 1,
                "delta": {"type": "input_json_delta", "partial_json": "{\\"location\\": \\"San Francisco\\"}"}
            }
            """), Pair.of("content_block_start", """
            {
                "type": "content_block_start",
                "index": 2,
                "content_block": {"type": "tool_use", "id": "toolu_02", "name": "get_time", "input": {}}
            }
            """), Pair.of("content_block_delta", """
            {
                "type": "content_block_delta",
                "index": 2,
                "delta": {"type": "input_json_delta", "partial_json": "{\\"timezone\\": \\"PST\\"}"}
            }
            """)));

        var response = onNext(createProcessor(), item);
        assertThat(response.chunks().size(), equalTo(4));
        {
            assertToolUseContentStartBlock(response, 0, "toolu_01", "get_weather");
        }
        {
            assertToolUseArguments(response, 0, "{\"location\": \"San Francisco\"}");
        }
        {
            assertToolUseContentStartBlock(response, 1, "toolu_02", "get_time");
        }
        {
            assertToolUseArguments(response, 1, "{\"timezone\": \"PST\"}");
        }
    }

    public void testInputJsonDeltaForUnknownContentBlockIsSkipped() throws Exception {
        // An input_json_delta whose content block index was never announced by a tool_use content_block_start cannot be
        // attributed to a tool call, so it is dropped and more data is requested instead of emitting a chunk.
        var item = events(List.of(Pair.of("content_block_delta", """
            {
                "type": "content_block_delta",
                "index": 5,
                "delta": {"type": "input_json_delta", "partial_json": "{\\"location\\": \\"San Francisco\\"}"}
            }
            """)));

        var processor = createProcessor();

        Flow.Subscriber<ChunkedToXContent> downstream = mock();
        processor.subscribe(downstream);

        Flow.Subscription upstream = mock();
        processor.onSubscribe(upstream);

        processor.next(item);

        verify(upstream, times(1)).request(1);
        verify(downstream, times(0)).onNext(any());
    }

    public void testStopReasonConvertedToOpenAiFinishReason() {
        assertFinishReasonForStopReason("end_turn", "stop");
        assertFinishReasonForStopReason("stop_sequence", "stop");
        assertFinishReasonForStopReason("pause_turn", "stop");
        assertFinishReasonForStopReason("max_tokens", "length");
        assertFinishReasonForStopReason("tool_use", "tool_calls");
        assertFinishReasonForStopReason("refusal", "content_filter");
        assertFinishReasonForStopReason("some_unknown_stop_reason", "stop");
    }

    private static void assertFinishReasonForStopReason(String stopReason, String expectedFinishReason) {
        var item = events(List.of(Pair.of("message_delta", Strings.format("""
            {
                "type": "message_delta",
                "delta": {
                    "stop_reason": "%s",
                    "stop_sequence": null
                },
                "usage": {
                    "output_tokens": 10
                }
            }
            """, stopReason))));

        var response = onNext(createProcessor(), item);
        assertThat(response.chunks().size(), equalTo(1));
        var choices = response.chunks().remove().choices();
        assertThat(choices.size(), is(1));
        assertThat(choices.getFirst().finishReason(), is(expectedFinishReason));
    }

    public void testEmptyResultsRequestsMoreData() throws Exception {
        var emptyDeque = new ArrayDeque<ServerSentEvent>();

        var processor = createProcessor();

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

        var processor = new AnthropicChatCompletionStreamingProcessor((noOp1, noOp2) -> { throw expectedException; }, false);

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
        Throwable actual = onError(new AnthropicChatCompletionStreamingProcessor((noOp1, noOp2) -> noOp2, false), item);
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
        Throwable actual = onError(new AnthropicChatCompletionStreamingProcessor((noOp1, noOp2) -> noOp2, false), item);
        assertThat(actual, is(instanceOf(IllegalStateException.class)));
        assertThat(
            actual.getMessage(),
            is("Field [model] in Anthropic chat completions response is of unexpected type [Integer]. Expected type is [String].")
        );
    }

    public void testMultipleJsonObjectsInSingleEventAreParsed() {
        var firstDelta = """
            {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Hello"}}\
            """;
        var secondDelta = """
            {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"World"}}\
            """;
        var item = events(List.of(Pair.of("content_block_delta", firstDelta + "\n" + secondDelta)));

        var response = onNext(createProcessor(), item);
        assertThat(response.chunks().size(), is(2));
        assertContent(response, "Hello");
        assertContent(response, "World");
    }

    public void testThinkingContentBlockStart_IncludesInitialTextInReasoningDetail() {
        // The content_block_start for a thinking block carries an initial thinking fragment. That fragment must be passed
        // as the text field of the TextReasoningDetail — leaving both text and signature null trips validateTextOrSignaturePresent
        // when the detail is echoed back on a subsequent request turn.
        var item = events(List.of(Pair.of("content_block_start", """
            {"type":"content_block_start","index":0,
             "content_block":{"type":"thinking","thinking":"initial thought"}}""")));

        var response = onNext(createProcessor(), item);
        assertThat(response.chunks().size(), equalTo(1));
        var delta = response.chunks().remove().choices().getFirst().message();
        assertThat(delta.reasoning(), is("initial thought"));
        var detail = (ReasoningDetail.TextReasoningDetail) delta.reasoningDetails().getFirst();
        assertThat(detail.format(), is(ANTHROPIC_FORMAT));
        assertThat(detail.index(), is(0L));
        assertThat(detail.text(), is("initial thought"));
        assertNull(detail.signature());
    }

    public void testThinkingDeltaForUnknownContentBlockIsSkipped() throws Exception {
        // A thinking_delta whose content block index was never announced by a thinking content_block_start cannot be
        // attributed to a reasoning block and must be dropped silently, rather than producing a negative index.
        var item = events(List.of(Pair.of("content_block_delta", """
            {"type":"content_block_delta","index":5,
             "delta":{"type":"thinking_delta","thinking":"orphaned"}}""")));

        var processor = createProcessor();

        Flow.Subscriber<ChunkedToXContent> downstream = mock();
        processor.subscribe(downstream);

        Flow.Subscription upstream = mock();
        processor.onSubscribe(upstream);

        processor.next(item);

        verify(upstream, times(1)).request(1);
        verify(downstream, times(0)).onNext(any());
    }

    public void testInterleavedThinkingBlocksKeepDistinctIndices() {
        // Two thinking blocks (Anthropic content block indices 0 and 2, with a text block at 1) must stream as reasoning details
        // with monotonically increasing reasoning indices so a client accumulating details by index does not merge them.
        var item = events(List.of(Pair.of("content_block_start", """
            {"type":"content_block_start","index":0,
            "content_block":{"type":"thinking","thinking":"first"}}"""), Pair.of("content_block_start", """
            {"type":"content_block_start","index":1,
             "content_block":{"type":"text","text":""}}"""), Pair.of("content_block_start", """
            {"type":"content_block_start","index":2,
             "content_block":{"type":"thinking","thinking":"second"}}"""), Pair.of("content_block_delta", """
            {"type":"content_block_delta","index":0,
             "delta":{"type":"thinking_delta","thinking":"first-more"}}"""), Pair.of("content_block_delta", """
            {"type":"content_block_delta","index":2,
             "delta":{"type":"signature_delta","signature":"sig-for-second"}}""")));

        var response = onNext(createProcessor(), item);
        // 2 thinking_starts + 1 text_start + 1 thinking_delta + 1 signature_delta
        assertThat(response.chunks().size(), equalTo(5));

        // thinking block at content index 0 → reasoning index 0
        var first = (ReasoningDetail.TextReasoningDetail) response.chunks()
            .remove()
            .choices()
            .getFirst()
            .message()
            .reasoningDetails()
            .getFirst();
        assertThat(first.index(), is(0L));
        assertThat(first.text(), is("first"));

        response.chunks().remove(); // text block start

        // thinking block at content index 2 → reasoning index 1
        var second = (ReasoningDetail.TextReasoningDetail) response.chunks()
            .remove()
            .choices()
            .getFirst()
            .message()
            .reasoningDetails()
            .getFirst();
        assertThat(second.index(), is(1L));
        assertThat(second.text(), is("second"));

        // thinking_delta for content block index 0 → reasoning index 0
        var firstDelta = (ReasoningDetail.TextReasoningDetail) response.chunks()
            .remove()
            .choices()
            .getFirst()
            .message()
            .reasoningDetails()
            .getFirst();
        assertThat(firstDelta.index(), is(0L));
        assertThat(firstDelta.text(), is("first-more"));

        // signature_delta for content block index 2 → reasoning index 1
        var sigDelta = (ReasoningDetail.TextReasoningDetail) response.chunks()
            .remove()
            .choices()
            .getFirst()
            .message()
            .reasoningDetails()
            .getFirst();
        assertThat(sigDelta.index(), is(1L));
        assertThat(sigDelta.signature(), is("sig-for-second"));
    }

    public void testExcludeReasoning_DropsThinkingBlocksAndDeltas() {
        var item = events(List.of(Pair.of("message_start", Strings.format("""
            {"type":"message_start","message":{"model":"%s","id":"%s","role":"assistant",
             "stop_reason":null,"usage":{"input_tokens":5,"output_tokens":1,
             "cache_creation_input_tokens":0,"cache_read_input_tokens":0}}}
            """, MODEL, MSG_ID)), Pair.of("content_block_start", """
            {"type":"content_block_start","index":0,
             "content_block":{"type":"thinking","thinking":"secret"}}"""), Pair.of("content_block_delta", """
            {"type":"content_block_delta","index":0,
             "delta":{"type":"thinking_delta","thinking":"more secret"}}"""), Pair.of("content_block_delta", """
            {"type":"content_block_delta","index":0,
             "delta":{"type":"signature_delta","signature":"sig"}}"""), Pair.of("content_block_start", """
            {"type":"content_block_start","index":1,
             "content_block":{"type":"redacted_thinking","data":"opaque"}}"""), Pair.of("content_block_start", """
            {"type":"content_block_start","index":2,
             "content_block":{"type":"text","text":"hello"}}"""), Pair.of("message_delta", """
            {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":3}}"""), Pair.of("message_stop", """
            {"type":"message_stop"}""")));

        var response = onNext(createProcessor(true), item);
        // message_start + text_block_start + message_delta + message_stop — all thinking-related events dropped
        assertThat(response.chunks().size(), equalTo(4));

        for (var chunk : response.chunks()) {
            if (chunk.choices() != null && chunk.choices().isEmpty() == false) {
                var delta = chunk.choices().getFirst().message();
                assertNull(delta.reasoning());
                assertNull(delta.reasoningDetails());
            }
        }
    }

    public void testExcludeReasoning_DoesNotWarnForSkippedThinkingDeltas() {
        // When excludeReasoning is true the thinking_delta guard fires before the map lookup,
        // so no "unknown content block index" warning is emitted even though the block was never registered.
        // A message_start is included to ensure the batch produces non-null output (required by onNext helper).
        var item = events(List.of(Pair.of("message_start", """
            {"type":"message_start","message":{"id":"msg_01","type":"message","role":"assistant","model":"claude",
             "stop_reason":null,"usage":{"input_tokens":5,"output_tokens":1}}}"""), Pair.of("content_block_delta", """
            {"type":"content_block_delta","index":0,
             "delta":{"type":"thinking_delta","thinking":"orphaned"}}""")));

        MockLog.assertThatLogger(
            () -> onNext(createProcessor(true), item),
            AnthropicChatCompletionStreamingProcessor.class,
            new MockLog.UnseenEventExpectation(
                "no warn for excluded thinking delta",
                AnthropicChatCompletionStreamingProcessor.class.getName(),
                Level.DEBUG,
                "*unknown content block index*"
            )
        );
    }

    private static void assertMessageStartBlock(StreamingUnifiedChatCompletionResults.Results response) {
        var chunk = response.chunks().remove();
        assertThat(chunk.id(), is(MSG_ID));
        assertThat(chunk.model(), is(MODEL));
        assertThat(chunk.object(), is(CHUNK_OBJECT));
        assertNull(chunk.usage());
        assertThat(chunk.choices().size(), is(1));
        var choice = chunk.choices().getFirst();
        assertThat(choice.index(), is(0));
        assertThat(choice.message().role(), is("assistant"));
    }

    private static void assertToolUseContentStartBlock(
        StreamingUnifiedChatCompletionResults.Results response,
        int toolCallIndex,
        String id,
        String name
    ) {
        var choices = response.chunks().remove().choices();
        assertThat(choices.size(), is(1));
        assertThat(choices.getFirst().index(), is(0));
        var toolCalls = choices.getFirst().message().toolCalls();
        assertThat(toolCalls.size(), is(1));
        assertThat(toolCalls.getFirst().index(), is(toolCallIndex));
        assertThat(toolCalls.getFirst().id(), is(id));
        assertThat(toolCalls.getFirst().type(), is(FUNCTION_TYPE));
        var function = toolCalls.getFirst().function();
        assertThat(function.arguments(), is(""));
        assertThat(function.name(), is(name));
    }

    private static void assertToolUseArguments(
        StreamingUnifiedChatCompletionResults.Results response,
        int toolCallIndex,
        String arguments
    ) {
        var choices = response.chunks().remove().choices();
        assertThat(choices.size(), is(1));
        assertThat(choices.getFirst().index(), is(0));
        var toolCalls = choices.getFirst().message().toolCalls();
        assertThat(toolCalls.size(), is(1));
        assertThat(toolCalls.getFirst().index(), is(toolCallIndex));
        assertNull(toolCalls.getFirst().id());
        var function = toolCalls.getFirst().function();
        assertThat(function.arguments(), Matchers.is(arguments));
        assertNull(function.name());
    }

    private static void assertMessageDeltaBlock(StreamingUnifiedChatCompletionResults.Results response) {
        var chunk = response.chunks().remove();
        var choices = chunk.choices();
        assertThat(choices.size(), is(1));
        assertThat(choices.getFirst().index(), is(0));
        assertNull(choices.getFirst().message().toolCalls());
        assertNull(choices.getFirst().message().content());
        assertThat(choices.getFirst().finishReason(), is("tool_calls"));
        assertNull(chunk.usage());
    }

    private static void assertMessageStopUsageBlock(
        StreamingUnifiedChatCompletionResults.Results response,
        int expectedCompletion,
        int expectedPrompt,
        int expectedTotal,
        Integer expectedCachedTokens,
        Integer expectedCacheWriteTokens
    ) {
        var chunk = response.chunks().remove();
        assertThat(chunk.choices().size(), is(0));
        var usage = chunk.usage();
        assertThat(usage.completionTokens(), is(expectedCompletion));
        assertThat(usage.promptTokens(), is(expectedPrompt));
        assertThat(usage.totalTokens(), is(expectedTotal));
        assertThat(
            usage.promptTokensDetails(),
            is(ChatCompletionUsageResponse.PromptTokensDetails.ofNullable(expectedCachedTokens, expectedCacheWriteTokens))
        );
    }

    private static void assertContent(StreamingUnifiedChatCompletionResults.Results response, String content) {
        var choices = response.chunks().remove().choices();
        assertThat(choices.size(), is(1));
        assertThat(choices.getFirst().index(), is(0));
        assertThat(choices.getFirst().message().content(), Matchers.is(content));
    }
}
