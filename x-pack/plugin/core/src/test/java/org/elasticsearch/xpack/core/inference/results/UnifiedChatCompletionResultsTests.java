/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.results;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.is;

public class UnifiedChatCompletionResultsTests extends AbstractWireSerializingTestCase<UnifiedChatCompletionResults> {

    @Override
    protected Writeable.Reader<UnifiedChatCompletionResults> instanceReader() {
        return UnifiedChatCompletionResults::new;
    }

    @Override
    protected UnifiedChatCompletionResults createTestInstance() {
        return randomInstance();
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
                randomChoices(),
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
                instance.usage() == null ? new UnifiedChatCompletionResults.Usage(1, 2, 3) : null
            );
            default -> throw new AssertionError("unexpected case");
        };
    }

    public void testToXContentChunked_FullResponse() throws IOException {
        var results = new UnifiedChatCompletionResults(
            "chatcmpl-123",
            List.of(
                new UnifiedChatCompletionResults.Choice(
                    0,
                    new UnifiedChatCompletionResults.Message(
                        "assistant",
                        "Hello!",
                        List.of(
                            new UnifiedChatCompletionResults.ToolCall(
                                "call_abc",
                                "function",
                                new UnifiedChatCompletionResults.ToolCall.Function("get_weather", "{}")
                            )
                        ),
                        null
                    ),
                    "tool_calls"
                )
            ),
            "gpt-4o",
            "chat.completion",
            new UnifiedChatCompletionResults.Usage(12, 9, 21)
        );

        assertThat(toXContent(results), is(XContentHelper.stripWhitespace("""
            {
              "id": "chatcmpl-123",
              "choices": [
                {
                  "index": 0,
                  "message": {
                    "role": "assistant",
                    "content": "Hello!",
                    "tool_calls": [
                      {
                        "id": "call_abc",
                        "type": "function",
                        "function": {
                          "name": "get_weather",
                          "arguments": "{}"
                        }
                      }
                    ]
                  },
                  "finish_reason": "tool_calls"
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
        var results = new UnifiedChatCompletionResults(
            "chatcmpl-456",
            List.of(
                new UnifiedChatCompletionResults.Choice(0, new UnifiedChatCompletionResults.Message("assistant", "Hi", null, null), "stop")
            ),
            "gpt-4o-mini",
            "chat.completion",
            null
        );

        assertThat(toXContent(results), is(XContentHelper.stripWhitespace("""
            {
              "id": "chatcmpl-456",
              "choices": [
                {
                  "index": 0,
                  "message": {
                    "role": "assistant",
                    "content": "Hi"
                  },
                  "finish_reason": "stop"
                }
              ],
              "model": "gpt-4o-mini",
              "object": "chat.completion"
            }
            """)));
    }

    private static String toXContent(UnifiedChatCompletionResults results) throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        results.toXContentChunked(null).forEachRemaining(xContent -> {
            try {
                xContent.toXContent(builder, null);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        builder.endObject();
        return Strings.toString(builder);
    }

    private static UnifiedChatCompletionResults randomInstance() {
        return new UnifiedChatCompletionResults(
            randomAlphanumericOfLength(10),
            randomChoices(),
            randomAlphanumericOfLength(5),
            "chat.completion",
            randomBoolean()
                ? null
                : new UnifiedChatCompletionResults.Usage(randomIntBetween(0, 100), randomIntBetween(0, 100), randomIntBetween(0, 200))
        );
    }

    private static List<UnifiedChatCompletionResults.Choice> randomChoices() {
        return randomList(
            1,
            3,
            () -> new UnifiedChatCompletionResults.Choice(
                randomIntBetween(0, 5),
                randomMessage(),
                randomBoolean() ? null : randomAlphanumericOfLength(5)
            )
        );
    }

    private static UnifiedChatCompletionResults.Message randomMessage() {
        return new UnifiedChatCompletionResults.Message(
            randomBoolean() ? null : randomAlphanumericOfLength(5),
            randomBoolean() ? null : randomAlphanumericOfLength(10),
            randomBoolean()
                ? null
                : randomList(
                    0,
                    3,
                    () -> new UnifiedChatCompletionResults.ToolCall(
                        randomAlphanumericOfLength(5),
                        "function",
                        new UnifiedChatCompletionResults.ToolCall.Function(randomAlphanumericOfLength(5), randomAlphanumericOfLength(10))
                    )
                ),
            randomBoolean() ? null : randomAlphanumericOfLength(5)
        );
    }
}
