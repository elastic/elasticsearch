/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.results.completion;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.is;

public class ChatCompletionToolCallTests extends AbstractBWCWireSerializationTestCase<ChatCompletionToolCall> {

    private static final int FIELD_LENGTH = 5;

    public static ChatCompletionToolCall randomChatCompletionToolCall() {
        return new ChatCompletionToolCall(
            randomInt(5),
            randomAlphaOfLengthOrNull(FIELD_LENGTH),
            randomBoolean()
                ? null
                : new ChatCompletionToolCall.Function(randomAlphaOfLengthOrNull(FIELD_LENGTH), randomAlphaOfLengthOrNull(FIELD_LENGTH)),
            randomAlphaOfLengthOrNull(FIELD_LENGTH)
        );
    }

    @Override
    protected Writeable.Reader<ChatCompletionToolCall> instanceReader() {
        return ChatCompletionToolCall::new;
    }

    @Override
    protected ChatCompletionToolCall createTestInstance() {
        return randomChatCompletionToolCall();
    }

    @Override
    protected ChatCompletionToolCall mutateInstanceForVersion(ChatCompletionToolCall instance, TransportVersion version) {
        return instance;
    }

    @Override
    protected ChatCompletionToolCall mutateInstance(ChatCompletionToolCall instance) {
        var index = instance.index();
        var id = instance.id();
        var function = instance.function();
        var type = instance.type();

        switch (between(0, 3)) {
            case 0 -> index = randomValueOtherThan(index, () -> randomInt(5));
            case 1 -> id = randomValueOtherThan(id, () -> randomAlphaOfLengthOrNull(FIELD_LENGTH));
            case 2 -> function = randomValueOtherThan(
                function,
                () -> randomBoolean()
                    ? null
                    : new ChatCompletionToolCall.Function(randomAlphaOfLengthOrNull(FIELD_LENGTH), randomAlphaOfLengthOrNull(FIELD_LENGTH))
            );
            case 3 -> type = randomValueOtherThan(type, () -> randomAlphaOfLengthOrNull(FIELD_LENGTH));
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new ChatCompletionToolCall(index, id, function, type);
    }

    public void testToXContentChunked_WithFunction() throws IOException {
        var toolCall = new ChatCompletionToolCall(0, "call_abc", new ChatCompletionToolCall.Function("{}", "get_weather"), "function");

        assertThat(toXContent(toolCall), is(XContentHelper.stripWhitespace("""
            {
              "index": 0,
              "id": "call_abc",
              "function": {
                "arguments": "{}",
                "name": "get_weather"
              },
              "type": "function"
            }
            """)));
    }

    public void testToXContentChunked_NullFunction_FunctionObjectOmitted() throws IOException {
        var toolCall = new ChatCompletionToolCall(1, "call_xyz", null, "function");

        assertThat(toXContent(toolCall), is(XContentHelper.stripWhitespace("""
            {
              "index": 1,
              "id": "call_xyz",
              "type": "function"
            }
            """)));
    }

    public void testToXContentChunked_NullType_TypeEmittedAsNull() throws IOException {
        // type uses chunk() rather than chunkNullable(), so null is always emitted explicitly
        var toolCall = new ChatCompletionToolCall(0, null, null, null);

        assertThat(toXContent(toolCall), is(XContentHelper.stripWhitespace("""
            {
              "index": 0,
              "type": null
            }
            """)));
    }

    static String toXContent(ChatCompletionToolCall toolCall) throws IOException {
        var builder = JsonXContent.contentBuilder();
        toolCall.toXContentChunked(null).forEachRemaining(chunk -> {
            try {
                chunk.toXContent(builder, null);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        return Strings.toString(builder);
    }
}
