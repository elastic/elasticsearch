/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.results.completion;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.is;

/**
 * Wire-serialization tests for {@link ChatCompletionChunk} (the merged streaming/non-streaming payload).
 *
 * <p>Two public statics are exposed so that {@code StreamingUnifiedChatCompletionResultsTests} can
 * delegate to them, keeping both test classes in sync without duplicating the random-instance builders.
 */
public class ChatCompletionChunkTests extends AbstractBWCWireSerializationTestCase<ChatCompletionChunk> {
    public static ChatCompletionChunk randomChatCompletionChunk() {
        return new ChatCompletionChunk(
            randomAlphanumericOfLength(5),
            randomBoolean() ? null : randomList(randomInt(5), ChatCompletionChoiceResponseTests::randomChatCompletionChoiceResponse),
            randomAlphanumericOfLength(5),
            randomAlphanumericOfLength(5),
            randomBoolean() ? null : ChatCompletionUsageTests.randomChatCompletionUsage()
        );
    }

    /**
     * Truncates fields that would not survive serialization to an older transport version.
     * Delegates the per-record truncation rules to {@link ChatCompletionChoiceResponseTests#downgrade} and
     * {@link ChatCompletionUsageTests#downgrade}.
     * Exposed so that {@code StreamingUnifiedChatCompletionResultsTests.mutateInstanceForVersion} can delegate.
     */
    public static ChatCompletionChunk downgrade(ChatCompletionChunk instance, TransportVersion version) {
        var choices = instance.choices() == null
            ? null
            : instance.choices().stream().map(c -> ChatCompletionChoiceResponseTests.downgrade(c, version)).toList();
        var usage = instance.usage() == null ? null : ChatCompletionUsageTests.downgrade(instance.usage(), version);
        return new ChatCompletionChunk(instance.id(), choices, instance.model(), instance.object(), usage);
    }

    @Override
    protected Writeable.Reader<ChatCompletionChunk> instanceReader() {
        return ChatCompletionChunk::new;
    }

    @Override
    protected ChatCompletionChunk createTestInstance() {
        return randomChatCompletionChunk();
    }

    @Override
    protected ChatCompletionChunk mutateInstance(ChatCompletionChunk instance) {
        return switch (randomIntBetween(0, 3)) {
            case 0 -> new ChatCompletionChunk(
                instance.id() + "x",
                instance.choices(),
                instance.model(),
                instance.object(),
                instance.usage()
            );
            case 1 -> new ChatCompletionChunk(
                instance.id(),
                randomList(
                    1,
                    3,
                    () -> new ChatCompletionChoiceResponse(
                        new ChatCompletionMessage(randomAlphanumericOfLength(5), null, null, null),
                        null,
                        0
                    )
                ),
                instance.model(),
                instance.object(),
                instance.usage()
            );
            case 2 -> new ChatCompletionChunk(
                instance.id(),
                instance.choices(),
                instance.model() + "x",
                instance.object(),
                instance.usage()
            );
            case 3 -> new ChatCompletionChunk(
                instance.id(),
                instance.choices(),
                instance.model(),
                instance.object(),
                instance.usage() == null ? new ChatCompletionUsage(1, 2, 3) : null
            );
            default -> throw new AssertionError("unexpected case");
        };
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(UnifiedCompletionRequest.getNamedWriteables());
    }

    @Override
    protected ChatCompletionChunk mutateInstanceForVersion(ChatCompletionChunk instance, TransportVersion version) {
        return downgrade(instance, version);
    }

    public void testToXContentChunked_FullResponse() throws IOException {
        var completion = new ChatCompletionChunk(
            "chatcmpl-123",
            List.of(
                new ChatCompletionChoiceResponse(
                    new ChatCompletionMessage(
                        "Hello!",
                        null,
                        "assistant",
                        List.of(
                            new ChatCompletionToolCall(0, "call_abc", new ChatCompletionToolCall.Function("{}", "get_weather"), "function")
                        )
                    ),
                    "tool_calls",
                    0
                )
            ),
            "gpt-4o",
            "chat.completion",
            new ChatCompletionUsage(12, 9, 21)
        );

        assertThat(toXContentNonStreaming(completion), is(XContentHelper.stripWhitespace("""
            {
              "id": "chatcmpl-123",
              "choices": [
                {
                  "message": {
                    "content": "Hello!",
                    "role": "assistant",
                    "tool_calls": [
                      {
                        "index": 0,
                        "id": "call_abc",
                        "function": {
                          "arguments": "{}",
                          "name": "get_weather"
                        },
                        "type": "function"
                      }
                    ]
                  },
                  "finish_reason": "tool_calls",
                  "index": 0
                }
              ],
              "model": "gpt-4o",
              "object": "chat.completion",
              "usage": {
                "completion_tokens": 12,
                "prompt_tokens": 9,
                "total_tokens": 21
              }
            }
            """)));
    }

    public void testToXContentChunked_MinimalResponse() throws IOException {
        var completion = new ChatCompletionChunk(
            "chatcmpl-456",
            List.of(new ChatCompletionChoiceResponse(new ChatCompletionMessage("Hi", null, "assistant", null), "stop", 0)),
            "gpt-4o-mini",
            "chat.completion",
            null
        );

        assertThat(toXContentNonStreaming(completion), is(XContentHelper.stripWhitespace("""
            {
              "id": "chatcmpl-456",
              "choices": [
                {
                  "message": {
                    "content": "Hi",
                    "role": "assistant"
                  },
                  "finish_reason": "stop",
                  "index": 0
                }
              ],
              "model": "gpt-4o-mini",
              "object": "chat.completion"
            }
            """)));
    }

    /**
     * Wraps the non-streaming form in a top-level object to match the outer object supplied by
     * {@code InferenceAction.Response.toXContentChunked()} in production.
     */
    private static String toXContentNonStreaming(ChatCompletionChunk completion) throws IOException {
        var builder = JsonXContent.contentBuilder();
        builder.startObject();
        completion.toXContentChunked(null).forEachRemaining(xContent -> {
            try {
                xContent.toXContent(builder, null);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        builder.endObject();
        return Strings.toString(builder);
    }
}
