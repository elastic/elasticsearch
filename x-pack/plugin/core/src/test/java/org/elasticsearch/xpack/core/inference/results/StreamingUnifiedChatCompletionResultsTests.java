/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 *
 * this file was contributed to by a generative AI
 */

package org.elasticsearch.xpack.core.inference.results;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.inference.completion.ReasoningDetail;
import org.elasticsearch.inference.completion.ReasoningDetailTests;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class StreamingUnifiedChatCompletionResultsTests extends AbstractWireSerializingTestCase<
    StreamingUnifiedChatCompletionResults.Results> {

    public void testResults_toXContentChunked_WithCachedTokens() throws IOException {
        testResults_toXContentChunked(true, false);
    }

    public void testResults_toXContentChunked_NoCachedTokens_NoReasoning() throws IOException {
        testResults_toXContentChunked(false, false);
    }

    public void testResults_toXContentChunked_NoCachedTokens_WithReasoning() throws IOException {
        testResults_toXContentChunked(false, true);
    }

    private void testResults_toXContentChunked(boolean includeCachedTokens, boolean includeReasoning) throws IOException {
        String reasoningPart = includeReasoning ? """
            "reasoning": "some_reasoning",
            """ : "";
        String reasoningDetailsPart = includeReasoning ? """
            ,
            "reasoning_details": [
              {
                "type": "reasoning.encrypted",
                "format": "some_encrypted_reasoning_detail_format",
                "id": "some_id_0",
                "index": 0,
                "data": "some_encrypted_data"
              },
              {
                "type": "reasoning.summary",
                "format": "some_summary_reasoning_detail_format",
                "id": "some_id_1",
                "index": 1,
                "summary": "some_summary"
              },
              {
                "type": "reasoning.text",
                "format": "some_text_reasoning_detail_format",
                "id": "some_id_2",
                "index": 2,
                "text": "some_text",
                "signature": "some_signature"
              }
            ]""" : "";
        String cachedTokensPart = includeCachedTokens ? """
            ,
            "prompt_tokens_details": {
              "cached_tokens": 20
            }""" : "";
        String reasoningUsagePart = includeReasoning ? """
            ,
            "completion_tokens_details": {
              "reasoning_tokens": 25
            }""" : "";

        String expected = Strings.format("""
            {
              "id": "chunk1",
              "choices": [
                {
                  "delta": {
                    "content": "example_content",
                    "refusal": "example_refusal",
                    "role": "assistant",
                    %s
                    "tool_calls": [
                      {
                        "index": 1,
                        "id": "tool1",
                        "function": {
                          "arguments": "example_arguments",
                          "name": "example_function"
                        },
                        "type": "function"
                      }
                    ]
                    %s
                  },
                  "finish_reason": "example_reason",
                  "index": 0
                }
              ],
              "model": "example_model",
              "object": "example_object",
              "usage": {
                "completion_tokens": 10,
                "prompt_tokens": 5,
                "total_tokens": 15
                %s
                %s
              }
            }
            """, reasoningPart, reasoningDetailsPart, cachedTokensPart, reasoningUsagePart);

        StreamingUnifiedChatCompletionResults.ChatCompletionChunk chunk = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk(
            "chunk1",
            List.of(
                new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice(
                    new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta(
                        "example_content",
                        "example_refusal",
                        "assistant",
                        List.of(
                            new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta.ToolCall(
                                1,
                                "tool1",
                                new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta.ToolCall.Function(
                                    "example_arguments",
                                    "example_function"
                                ),
                                "function"
                            )
                        ),
                        includeReasoning ? "some_reasoning" : null,
                        includeReasoning
                            ? List.of(
                                new ReasoningDetail.EncryptedReasoningDetail(
                                    "some_encrypted_reasoning_detail_format",
                                    "some_id_0",
                                    0L,
                                    "some_encrypted_data"
                                ),
                                new ReasoningDetail.SummaryReasoningDetail(
                                    "some_summary_reasoning_detail_format",
                                    "some_id_1",
                                    1L,
                                    "some_summary"
                                ),
                                new ReasoningDetail.TextReasoningDetail(
                                    "some_text_reasoning_detail_format",
                                    "some_id_2",
                                    2L,
                                    "some_text",
                                    "some_signature"
                                )
                            )
                            : null
                    ),
                    "example_reason",
                    0
                )
            ),
            "example_model",
            "example_object",
            new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Usage(
                10,
                5,
                15,
                includeCachedTokens ? 20 : null,
                includeReasoning ? new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Usage.CompletionTokenDetails(25) : null
            )
        );

        Deque<StreamingUnifiedChatCompletionResults.ChatCompletionChunk> deque = new ArrayDeque<>();
        deque.add(chunk);
        StreamingUnifiedChatCompletionResults.Results results = new StreamingUnifiedChatCompletionResults.Results(deque);
        XContentBuilder builder = JsonXContent.contentBuilder();
        results.toXContentChunked(null).forEachRemaining(xContent -> {
            try {
                xContent.toXContent(builder, null);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        assertEquals(expected.replaceAll("\\s+", ""), Strings.toString(builder.prettyPrint()).trim());
    }

    public void testChoiceToXContentChunked() throws IOException {
        String expected = """
            {
              "delta": {
                "content": "example_content",
                "refusal": "example_refusal",
                "role": "assistant",
                "reasoning": "some_reasoning",
                "tool_calls": [
                  {
                    "index": 1,
                    "id": "tool1",
                    "function": {
                      "arguments": "example_arguments",
                      "name": "example_function"
                    },
                    "type": "function"
                  }
                ],
                "reasoning_details": [
                  {
                    "type": "reasoning.encrypted",
                      "format": "some_encrypted_reasoning_detail_format",
                      "id": "some_id_0",
                      "index": 0,
                      "data": "some_encrypted_data"
                  },
                  {
                    "type": "reasoning.summary",
                    "format": "some_summary_reasoning_detail_format",
                    "id": "some_id_1",
                    "index": 1,
                    "summary": "some_summary"
                  },
                  {
                    "type": "reasoning.text",
                    "format": "some_text_reasoning_detail_format",
                    "id": "some_id_2",
                    "index": 2,
                    "text": "some_text",
                    "signature": "some_signature"
                  }
                ]
              },
              "finish_reason": "example_reason",
              "index": 0
            }
            """;

        StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice choice =
            new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice(
                new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta(
                    "example_content",
                    "example_refusal",
                    "assistant",
                    List.of(
                        new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta.ToolCall(
                            1,
                            "tool1",
                            new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta.ToolCall.Function(
                                "example_arguments",
                                "example_function"
                            ),
                            "function"
                        )
                    ),
                    "some_reasoning",
                    List.of(
                        new ReasoningDetail.EncryptedReasoningDetail(
                            "some_encrypted_reasoning_detail_format",
                            "some_id_0",
                            0L,
                            "some_encrypted_data"
                        ),
                        new ReasoningDetail.SummaryReasoningDetail("some_summary_reasoning_detail_format", "some_id_1", 1L, "some_summary"),
                        new ReasoningDetail.TextReasoningDetail(
                            "some_text_reasoning_detail_format",
                            "some_id_2",
                            2L,
                            "some_text",
                            "some_signature"
                        )
                    )
                ),
                "example_reason",
                0
            );

        XContentBuilder builder = JsonXContent.contentBuilder();
        choice.toXContentChunked(null).forEachRemaining(xContent -> {
            try {
                xContent.toXContent(builder, null);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        assertEquals(expected.replaceAll("\\s+", ""), Strings.toString(builder.prettyPrint()).trim());
    }

    public void testToolCallToXContentChunked() throws IOException {
        String expected = """
            {
              "index": 1,
              "id": "tool1",
              "function": {
                "arguments": "example_arguments",
                "name": "example_function"
              },
              "type": "function"
            }
            """;

        StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta.ToolCall toolCall =
            new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta.ToolCall(
                1,
                "tool1",
                new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta.ToolCall.Function(
                    "example_arguments",
                    "example_function"
                ),
                "function"
            );

        XContentBuilder builder = JsonXContent.contentBuilder();
        toolCall.toXContentChunked(null).forEachRemaining(xContent -> {
            try {
                xContent.toXContent(builder, null);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        assertEquals(expected.replaceAll("\\s+", ""), Strings.toString(builder.prettyPrint()).trim());
    }

    public void testBufferedPublishing() {
        var results = new ArrayDeque<StreamingUnifiedChatCompletionResults.ChatCompletionChunk>();
        results.offer(randomChatCompletionChunk());
        results.offer(randomChatCompletionChunk());
        var completed = new AtomicBoolean();
        var streamingResults = new StreamingUnifiedChatCompletionResults(downstream -> {
            downstream.onSubscribe(new Flow.Subscription() {
                @Override
                public void request(long n) {
                    if (completed.compareAndSet(false, true)) {
                        downstream.onNext(new StreamingUnifiedChatCompletionResults.Results(results));
                    } else {
                        downstream.onComplete();
                    }
                }

                @Override
                public void cancel() {
                    fail("Cancel should never be called.");
                }
            });
        });

        AtomicInteger counter = new AtomicInteger(0);
        AtomicReference<Flow.Subscription> upstream = new AtomicReference<>(null);
        Flow.Subscriber<StreamingUnifiedChatCompletionResults.Results> subscriber = spy(
            new Flow.Subscriber<StreamingUnifiedChatCompletionResults.Results>() {
                @Override
                public void onSubscribe(Flow.Subscription subscription) {
                    if (upstream.compareAndSet(null, subscription) == false) {
                        fail("Upstream already set?!");
                    }
                    subscription.request(1);
                }

                @Override
                public void onNext(StreamingUnifiedChatCompletionResults.Results item) {
                    assertNotNull(item);
                    counter.incrementAndGet();
                    var sub = upstream.get();
                    if (sub != null) {
                        sub.request(1);
                    } else {
                        fail("Upstream not yet set?!");
                    }
                }

                @Override
                public void onError(Throwable throwable) {
                    fail(throwable);
                }

                @Override
                public void onComplete() {}
            }
        );
        streamingResults.publisher().subscribe(subscriber);
        verify(subscriber, times(2)).onNext(any());
    }

    @Override
    protected Writeable.Reader<StreamingUnifiedChatCompletionResults.Results> instanceReader() {
        return StreamingUnifiedChatCompletionResults.Results::new;
    }

    @Override
    protected StreamingUnifiedChatCompletionResults.Results createTestInstance() {
        var results = new ArrayDeque<StreamingUnifiedChatCompletionResults.ChatCompletionChunk>();
        for (int i = 0; i < randomIntBetween(1, 3); i++) {
            results.offer(randomChatCompletionChunk());
        }
        return new StreamingUnifiedChatCompletionResults.Results(results);
    }

    private static StreamingUnifiedChatCompletionResults.ChatCompletionChunk randomChatCompletionChunk() {
        Supplier<String> randomOptionalString = () -> randomBoolean() ? null : randomAlphanumericOfLength(5);
        return new StreamingUnifiedChatCompletionResults.ChatCompletionChunk(
            randomAlphanumericOfLength(5),
            randomBoolean()
                ? null
                : randomList(
                    randomInt(5),
                    () -> new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice(
                        new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta(
                            randomOptionalString.get(),
                            randomOptionalString.get(),
                            randomOptionalString.get(),
                            randomBoolean() ? null : randomList(randomInt(5), () -> {
                                return new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta.ToolCall(
                                    randomInt(5),
                                    randomOptionalString.get(),
                                    randomBoolean()
                                        ? null
                                        : new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta.ToolCall.Function(
                                            randomOptionalString.get(),
                                            randomOptionalString.get()
                                        ),
                                    randomOptionalString.get()
                                );
                            }),
                            randomOptionalString.get(),
                            randomBoolean() ? null : randomList(randomInt(5), ReasoningDetailTests::randomReasoningDetail)
                        ),
                        randomOptionalString.get(),
                        randomInt(5)
                    )
                ),
            randomAlphanumericOfLength(5),
            randomAlphanumericOfLength(5),
            randomBoolean()
                ? null
                : new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Usage(
                    randomInt(5),
                    randomInt(5),
                    randomInt(5),
                    randomInt(5),
                    randomBoolean()
                        ? new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Usage.CompletionTokenDetails(
                            randomNonNegativeIntOrNull()
                        )
                        : null
                )
        );
    }

    @Override
    protected StreamingUnifiedChatCompletionResults.Results mutateInstance(StreamingUnifiedChatCompletionResults.Results instance)
        throws IOException {
        var results = new ArrayDeque<>(instance.chunks());
        if (randomBoolean()) {
            results.pop();
        } else {
            results.add(randomChatCompletionChunk());
        }
        return new StreamingUnifiedChatCompletionResults.Results(results); // immutable
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(UnifiedCompletionRequest.getNamedWriteables());
    }
}
