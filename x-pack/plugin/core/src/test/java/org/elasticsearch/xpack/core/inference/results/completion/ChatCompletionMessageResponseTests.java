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

import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.CHAT_COMPLETION_REASONING_SUPPORT_ADDED;
import static org.hamcrest.Matchers.is;

public class ChatCompletionMessageResponseTests extends AbstractBWCWireSerializationTestCase<ChatCompletionMessageResponse> {

    private static final int FIELD_LENGTH = 5;

    public static ChatCompletionMessageResponse randomChatCompletionMessageResponse() {
        return new ChatCompletionMessageResponse(
            randomAlphaOfLengthOrNull(FIELD_LENGTH),
            randomAlphaOfLengthOrNull(FIELD_LENGTH),
            randomAlphaOfLengthOrNull(FIELD_LENGTH),
            randomBoolean() ? null : randomList(randomInt(5), ChatCompletionToolCallTests::randomChatCompletionToolCall),
            randomAlphaOfLengthOrNull(FIELD_LENGTH),
            randomBoolean() ? null : randomList(randomInt(5), ReasoningDetailTests::randomReasoningDetail)
        );
    }

    public static ChatCompletionMessageResponse downgrade(ChatCompletionMessageResponse instance, TransportVersion version) {
        if (version.supports(CHAT_COMPLETION_REASONING_SUPPORT_ADDED) == false) {
            return new ChatCompletionMessageResponse(instance.content(), instance.refusal(), instance.role(), instance.toolCalls());
        }
        return instance;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(UnifiedCompletionRequest.getNamedWriteables());
    }

    @Override
    protected Writeable.Reader<ChatCompletionMessageResponse> instanceReader() {
        return ChatCompletionMessageResponse::new;
    }

    @Override
    protected ChatCompletionMessageResponse createTestInstance() {
        return randomChatCompletionMessageResponse();
    }

    @Override
    protected ChatCompletionMessageResponse mutateInstanceForVersion(ChatCompletionMessageResponse instance, TransportVersion version) {
        return downgrade(instance, version);
    }

    @Override
    protected ChatCompletionMessageResponse mutateInstance(ChatCompletionMessageResponse instance) {
        var content = instance.content();
        var refusal = instance.refusal();
        var role = instance.role();
        var toolCalls = instance.toolCalls();
        var reasoning = instance.reasoning();
        var reasoningDetails = instance.reasoningDetails();

        switch (between(0, 5)) {
            case 0 -> content = randomValueOtherThan(content, () -> randomAlphaOfLengthOrNull(FIELD_LENGTH));
            case 1 -> refusal = randomValueOtherThan(refusal, () -> randomAlphaOfLengthOrNull(FIELD_LENGTH));
            case 2 -> role = randomValueOtherThan(role, () -> randomAlphaOfLengthOrNull(FIELD_LENGTH));
            case 3 -> toolCalls = randomValueOtherThan(
                toolCalls,
                () -> randomBoolean() ? null : randomList(1, 3, ChatCompletionToolCallTests::randomChatCompletionToolCall)
            );
            case 4 -> reasoning = randomValueOtherThan(reasoning, () -> randomAlphaOfLengthOrNull(FIELD_LENGTH));
            case 5 -> reasoningDetails = randomValueOtherThan(
                reasoningDetails,
                () -> randomBoolean() ? null : randomList(1, 3, ReasoningDetailTests::randomReasoningDetail)
            );
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new ChatCompletionMessageResponse(content, refusal, role, toolCalls, reasoning, reasoningDetails);
    }

    public void testToXContentChunked_AllFields_DeltaWrapper() throws IOException {
        var message = new ChatCompletionMessageResponse(
            "Hello!",
            null,
            "assistant",
            List.of(new ChatCompletionToolCall(0, "call_abc", new ChatCompletionToolCall.Function("{}", "get_weather"), "function")),
            null,
            null
        );

        assertThat(toXContent(message, "delta"), is(XContentHelper.stripWhitespace("""
            {
              "delta": {
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
              }
            }
            """)));
    }

    public void testToXContentChunked_MessageWrapper() throws IOException {
        var message = new ChatCompletionMessageResponse("Hi", null, "user", null, null, null);

        assertThat(toXContent(message, "message"), is(XContentHelper.stripWhitespace("""
            {
              "message": {
                "content": "Hi",
                "role": "user"
              }
            }
            """)));
    }

    public void testToXContentChunked_EmptyToolCalls_ToolCallsOmitted() throws IOException {
        // tool_calls is only emitted when the list is non-null AND non-empty
        var message = new ChatCompletionMessageResponse("Hi", null, null, List.of(), null, null);

        assertThat(toXContent(message, "delta"), is(XContentHelper.stripWhitespace("""
            {
              "delta": {
                "content": "Hi"
              }
            }
            """)));
    }

    public void testToXContentChunked_EmptyReasoningDetails_ReasoningDetailsOmitted() throws IOException {
        // reasoning_details is only emitted when the list is non-null AND non-empty
        var message = new ChatCompletionMessageResponse("Hi", null, null, null, "thinking...", List.of());

        assertThat(toXContent(message, "delta"), is(XContentHelper.stripWhitespace("""
            {
              "delta": {
                "content": "Hi",
                "reasoning": "thinking..."
              }
            }
            """)));
    }

    static String toXContent(ChatCompletionMessageResponse message, String messageFieldName) throws IOException {
        var builder = JsonXContent.contentBuilder();
        builder.startObject();
        message.toXContentChunked(null, messageFieldName).forEachRemaining(chunk -> {
            try {
                chunk.toXContent(builder, null);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        builder.endObject();
        return Strings.toString(builder);
    }
}
