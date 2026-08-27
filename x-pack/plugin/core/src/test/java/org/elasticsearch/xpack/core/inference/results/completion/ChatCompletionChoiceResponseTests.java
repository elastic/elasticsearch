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

import static org.hamcrest.Matchers.is;

public class ChatCompletionChoiceResponseTests extends AbstractBWCWireSerializationTestCase<ChatCompletionChoiceResponse> {

    private static final int FIELD_LENGTH = 5;

    public static ChatCompletionChoiceResponse randomChatCompletionChoiceResponse() {
        return new ChatCompletionChoiceResponse(
            ChatCompletionMessageResponseTests.randomChatCompletionMessageResponse(),
            randomAlphaOfLengthOrNull(FIELD_LENGTH),
            randomInt(5)
        );
    }

    public static ChatCompletionChoiceResponse downgrade(ChatCompletionChoiceResponse instance, TransportVersion version) {
        return new ChatCompletionChoiceResponse(
            ChatCompletionMessageResponseTests.downgrade(instance.message(), version),
            instance.finishReason(),
            instance.index()
        );
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(UnifiedCompletionRequest.getNamedWriteables());
    }

    @Override
    protected Writeable.Reader<ChatCompletionChoiceResponse> instanceReader() {
        return ChatCompletionChoiceResponse::new;
    }

    @Override
    protected ChatCompletionChoiceResponse createTestInstance() {
        return randomChatCompletionChoiceResponse();
    }

    @Override
    protected ChatCompletionChoiceResponse mutateInstanceForVersion(ChatCompletionChoiceResponse instance, TransportVersion version) {
        return downgrade(instance, version);
    }

    @Override
    protected ChatCompletionChoiceResponse mutateInstance(ChatCompletionChoiceResponse instance) {
        var message = instance.message();
        var finishReason = instance.finishReason();
        var index = instance.index();

        switch (between(0, 2)) {
            case 0 -> message = randomValueOtherThan(message, ChatCompletionMessageResponseTests::randomChatCompletionMessageResponse);
            case 1 -> finishReason = randomValueOtherThan(finishReason, () -> randomAlphaOfLengthOrNull(FIELD_LENGTH));
            case 2 -> index = randomValueOtherThan(index, () -> randomInt(5));
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new ChatCompletionChoiceResponse(message, finishReason, index);
    }

    public void testToXContentChunked_WithFinishReason_DeltaWrapper() throws IOException {
        var choice = new ChatCompletionChoiceResponse(new ChatCompletionMessageResponse("Hello!", null, "assistant", null, null, null), "stop", 0);

        assertThat(toXContent(choice, "delta"), is(XContentHelper.stripWhitespace("""
            {
              "delta": {
                "content": "Hello!",
                "role": "assistant"
              },
              "finish_reason": "stop",
              "index": 0
            }
            """)));
    }

    public void testToXContentChunked_NullFinishReason_FinishReasonOmitted() throws IOException {
        var choice = new ChatCompletionChoiceResponse(new ChatCompletionMessageResponse("Hi", null, null, null, null, null), null, 2);

        assertThat(toXContent(choice, "delta"), is(XContentHelper.stripWhitespace("""
            {
              "delta": {
                "content": "Hi"
              },
              "index": 2
            }
            """)));
    }

    public void testToXContentChunked_MessageWrapper() throws IOException {
        var choice = new ChatCompletionChoiceResponse(new ChatCompletionMessageResponse("Hi", null, "user", null, null, null), "stop", 1);

        assertThat(toXContent(choice, "message"), is(XContentHelper.stripWhitespace("""
            {
              "message": {
                "content": "Hi",
                "role": "user"
              },
              "finish_reason": "stop",
              "index": 1
            }
            """)));
    }

    static String toXContent(ChatCompletionChoiceResponse choice, String messageFieldName) throws IOException {
        var builder = JsonXContent.contentBuilder();
        choice.toXContentChunked(null, messageFieldName).forEachRemaining(chunk -> {
            try {
                chunk.toXContent(builder, null);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        return Strings.toString(builder);
    }
}
