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
import org.elasticsearch.common.io.stream.Writeable;
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

    public void testResults_toXContentChunked() throws IOException {
        String expected = """
                        {
                          "id": "chunk1",
                          "choices": [
                            {
                              "delta": {
                                "content": "example_content",
                                "refusal": "example_refusal",
                                "role": "assistant",
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
                          }
                        }
            """;

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
                        )
                    ),
                    "example_reason",
                    0
                )
            ),
            "example_model",
            "example_object",
            new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Usage(10, 5, 15)
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
            randomBoolean() ? null : randomList(randomInt(5), () -> {
                return new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice(
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
                        })
                    ),
                    randomOptionalString.get(),
                    randomInt(5)
                );
            }),
            randomAlphanumericOfLength(5),
            randomAlphanumericOfLength(5),
            randomBoolean()
                ? null
                : new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Usage(randomInt(5), randomInt(5), randomInt(5))
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
}
