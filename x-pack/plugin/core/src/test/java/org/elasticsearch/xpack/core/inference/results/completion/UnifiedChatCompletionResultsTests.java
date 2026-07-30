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
import org.elasticsearch.inference.completion.ReasoningDetailTests;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.CHAT_COMPLETION_CACHE_WRITE_TOKENS_SUPPORT_ADDED;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.CHAT_COMPLETION_REASONING_SUPPORT_ADDED;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.INFERENCE_CACHED_TOKENS;
import static org.hamcrest.Matchers.is;

/**
 * Wire-serialization tests for {@link UnifiedChatCompletionResults} (the merged streaming/non-streaming payload).
 *
 * <p>Two public statics are exposed so that {@code StreamingUnifiedChatCompletionResultsTests} can
 * delegate to them, keeping both test classes in sync without duplicating the random-instance builders.
 */
public class UnifiedChatCompletionResultsTests extends AbstractBWCWireSerializationTestCase<UnifiedChatCompletionResults> {
    public static UnifiedChatCompletionResults randomUnifiedChatCompletionResults() {
        var randomOptionalString = new java.util.function.Supplier<String>() {
            @Override
            public String get() {
                return randomBoolean() ? null : randomAlphanumericOfLength(5);
            }
        };
        return new UnifiedChatCompletionResults(
            randomAlphanumericOfLength(5),
            randomBoolean()
                ? null
                : randomList(
                    randomInt(5),
                    () -> new Choice(
                        new Message(
                            randomOptionalString.get(),
                            randomOptionalString.get(),
                            randomOptionalString.get(),
                            randomBoolean()
                                ? null
                                : randomList(
                                    randomInt(5),
                                    () -> new ToolCall(
                                        randomInt(5),
                                        randomOptionalString.get(),
                                        randomBoolean()
                                            ? null
                                            : new ToolCall.Function(randomOptionalString.get(), randomOptionalString.get()),
                                        randomOptionalString.get()
                                    )
                                ),
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
                : new Usage(
                    randomInt(5),
                    randomInt(5),
                    randomInt(5),
                    randomBoolean() ? new Usage.PromptTokensDetails(randomInt(5), randomInt(5)) : null,
                    randomBoolean() ? new Usage.CompletionTokenDetails(randomNonNegativeIntOrNull()) : null
                )
        );
    }

    /**
     * Truncates fields that would not survive serialization to an older transport version.
     * Mirrors the gating in {@link Usage#writeTo} and {@link Message#writeTo}.
     * Exposed so that {@code StreamingUnifiedChatCompletionResultsTests.mutateInstanceForVersion} can delegate.
     */
    public static UnifiedChatCompletionResults downgrade(UnifiedChatCompletionResults instance, TransportVersion version) {
        var choices = instance.choices();
        var usage = instance.usage();

        if (version.supports(CHAT_COMPLETION_REASONING_SUPPORT_ADDED) == false && choices != null) {
            choices = choices.stream()
                .map(
                    choice -> new Choice(
                        new Message(
                            choice.message().content(),
                            choice.message().refusal(),
                            choice.message().role(),
                            choice.message().toolCalls()
                        ),
                        choice.finishReason(),
                        choice.index()
                    )
                )
                .toList();
        }

        if (usage != null) {
            var promptTokensDetails = usage.promptTokensDetails();
            var completionTokenDetails = usage.completionTokenDetails();

            if (version.supports(CHAT_COMPLETION_CACHE_WRITE_TOKENS_SUPPORT_ADDED) == false && promptTokensDetails != null) {
                // the old wire format only carries cachedTokens; without it the whole details object collapses to null
                promptTokensDetails = promptTokensDetails.cachedTokens() == null
                    ? null
                    : new Usage.PromptTokensDetails(promptTokensDetails.cachedTokens(), null);
            }

            if (version.supports(INFERENCE_CACHED_TOKENS) == false) {
                promptTokensDetails = null;
            }

            if (version.supports(CHAT_COMPLETION_REASONING_SUPPORT_ADDED) == false) {
                completionTokenDetails = null;
            }

            usage = new Usage(
                usage.completionTokens(),
                usage.promptTokens(),
                usage.totalTokens(),
                promptTokensDetails,
                completionTokenDetails
            );
        }

        return new UnifiedChatCompletionResults(instance.id(), choices, instance.model(), instance.object(), usage);
    }

    @Override
    protected Writeable.Reader<UnifiedChatCompletionResults> instanceReader() {
        return UnifiedChatCompletionResults::new;
    }

    @Override
    protected UnifiedChatCompletionResults createTestInstance() {
        return randomUnifiedChatCompletionResults();
    }

    @Override
    protected UnifiedChatCompletionResults mutateInstance(UnifiedChatCompletionResults instance) {
        return switch (randomIntBetween(0, 3)) {
            case 0 -> new UnifiedChatCompletionResults(
                instance.id() + "x",
                instance.choices(),
                instance.model(),
                instance.object(),
                instance.usage()
            );
            case 1 -> new UnifiedChatCompletionResults(
                instance.id(),
                randomList(1, 3, () -> new Choice(new Message(randomAlphanumericOfLength(5), null, null, null), null, 0)),
                instance.model(),
                instance.object(),
                instance.usage()
            );
            case 2 -> new UnifiedChatCompletionResults(
                instance.id(),
                instance.choices(),
                instance.model() + "x",
                instance.object(),
                instance.usage()
            );
            case 3 -> new UnifiedChatCompletionResults(
                instance.id(),
                instance.choices(),
                instance.model(),
                instance.object(),
                instance.usage() == null ? new Usage(1, 2, 3) : null
            );
            default -> throw new AssertionError("unexpected case");
        };
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(UnifiedCompletionRequest.getNamedWriteables());
    }

    @Override
    protected UnifiedChatCompletionResults mutateInstanceForVersion(UnifiedChatCompletionResults instance, TransportVersion version) {
        return downgrade(instance, version);
    }

    public void testToXContentChunked_FullResponse() throws IOException {
        var completion = new UnifiedChatCompletionResults(
            "chatcmpl-123",
            List.of(
                new Choice(
                    new Message(
                        "Hello!",
                        null,
                        "assistant",
                        List.of(new ToolCall(0, "call_abc", new ToolCall.Function("{}", "get_weather"), "function"))
                    ),
                    "tool_calls",
                    0
                )
            ),
            "gpt-4o",
            "chat.completion",
            new Usage(12, 9, 21)
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
        var completion = new UnifiedChatCompletionResults(
            "chatcmpl-456",
            List.of(new Choice(new Message("Hi", null, "assistant", null), "stop", 0)),
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
    private static String toXContentNonStreaming(UnifiedChatCompletionResults completion) throws IOException {
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
