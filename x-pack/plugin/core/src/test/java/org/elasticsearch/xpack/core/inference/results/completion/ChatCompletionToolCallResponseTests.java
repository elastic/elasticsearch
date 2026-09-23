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

public class ChatCompletionToolCallResponseTests extends AbstractBWCWireSerializationTestCase<ChatCompletionToolCallResponse> {

    private static final int FIELD_LENGTH = 5;

    public static ChatCompletionToolCallResponse randomChatCompletionToolCallResponse() {
        return new ChatCompletionToolCallResponse(
            randomInt(5),
            randomAlphaOfLengthOrNull(FIELD_LENGTH),
            randomBoolean()
                ? null
                : new ChatCompletionToolCallResponse.Function(
                    randomAlphaOfLengthOrNull(FIELD_LENGTH),
                    randomAlphaOfLengthOrNull(FIELD_LENGTH)
                ),
            randomAlphaOfLengthOrNull(FIELD_LENGTH)
        );
    }

    @Override
    protected Writeable.Reader<ChatCompletionToolCallResponse> instanceReader() {
        return ChatCompletionToolCallResponse::new;
    }

    @Override
    protected ChatCompletionToolCallResponse createTestInstance() {
        return randomChatCompletionToolCallResponse();
    }

    @Override
    protected ChatCompletionToolCallResponse mutateInstanceForVersion(ChatCompletionToolCallResponse instance, TransportVersion version) {
        return instance;
    }

    @Override
    protected ChatCompletionToolCallResponse mutateInstance(ChatCompletionToolCallResponse instance) {
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
                    : new ChatCompletionToolCallResponse.Function(
                        randomAlphaOfLengthOrNull(FIELD_LENGTH),
                        randomAlphaOfLengthOrNull(FIELD_LENGTH)
                    )
            );
            case 3 -> type = randomValueOtherThan(type, () -> randomAlphaOfLengthOrNull(FIELD_LENGTH));
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new ChatCompletionToolCallResponse(index, id, function, type);
    }

    public void testToXContentChunked_WithFunction() throws IOException {
        var toolCall = new ChatCompletionToolCallResponse(
            0,
            "call_abc",
            new ChatCompletionToolCallResponse.Function("{}", "get_weather"),
            "function"
        );

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
        var toolCall = new ChatCompletionToolCallResponse(1, "call_xyz", null, "function");

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
        var toolCall = new ChatCompletionToolCallResponse(0, null, null, null);

        assertThat(toXContent(toolCall), is(XContentHelper.stripWhitespace("""
            {
              "index": 0,
              "type": null
            }
            """)));
    }

    static String toXContent(ChatCompletionToolCallResponse toolCall) throws IOException {
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
