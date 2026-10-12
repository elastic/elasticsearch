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

import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.CHAT_COMPLETION_CACHE_WRITE_TOKENS_SUPPORT_ADDED;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.CHAT_COMPLETION_REASONING_SUPPORT_ADDED;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.INFERENCE_CACHED_TOKENS;
import static org.hamcrest.Matchers.is;

public class ChatCompletionUsageResponseTests extends AbstractBWCWireSerializationTestCase<ChatCompletionUsageResponse> {

    public static ChatCompletionUsageResponse randomChatCompletionUsageResponse() {
        return new ChatCompletionUsageResponse(
            randomInt(100),
            randomInt(100),
            randomInt(100),
            randomBoolean()
                ? null
                : new ChatCompletionUsageResponse.PromptTokensDetails(randomNonNegativeIntOrNull(), randomNonNegativeIntOrNull()),
            randomBoolean() ? null : new ChatCompletionUsageResponse.CompletionTokenDetails(randomNonNegativeIntOrNull())
        );
    }

    public static ChatCompletionUsageResponse downgrade(ChatCompletionUsageResponse instance, TransportVersion version) {
        var promptTokensDetails = instance.promptTokensDetails();
        var completionTokenDetails = instance.completionTokenDetails();

        if (version.supports(CHAT_COMPLETION_CACHE_WRITE_TOKENS_SUPPORT_ADDED) == false && promptTokensDetails != null) {
            promptTokensDetails = promptTokensDetails.cachedTokens() == null
                ? null
                : new ChatCompletionUsageResponse.PromptTokensDetails(promptTokensDetails.cachedTokens(), null);
        }

        if (version.supports(INFERENCE_CACHED_TOKENS) == false) {
            promptTokensDetails = null;
        }

        if (version.supports(CHAT_COMPLETION_REASONING_SUPPORT_ADDED) == false) {
            completionTokenDetails = null;
        }

        return new ChatCompletionUsageResponse(
            instance.completionTokens(),
            instance.promptTokens(),
            instance.totalTokens(),
            promptTokensDetails,
            completionTokenDetails
        );
    }

    @Override
    protected Writeable.Reader<ChatCompletionUsageResponse> instanceReader() {
        return ChatCompletionUsageResponse::new;
    }

    @Override
    protected ChatCompletionUsageResponse createTestInstance() {
        return randomChatCompletionUsageResponse();
    }

    @Override
    protected ChatCompletionUsageResponse mutateInstanceForVersion(ChatCompletionUsageResponse instance, TransportVersion version) {
        return downgrade(instance, version);
    }

    @Override
    protected ChatCompletionUsageResponse mutateInstance(ChatCompletionUsageResponse instance) {
        var completionTokens = instance.completionTokens();
        var promptTokens = instance.promptTokens();
        var totalTokens = instance.totalTokens();
        var promptTokensDetails = instance.promptTokensDetails();
        var completionTokenDetails = instance.completionTokenDetails();

        switch (between(0, 4)) {
            case 0 -> completionTokens = randomValueOtherThan(completionTokens, () -> randomInt(100));
            case 1 -> promptTokens = randomValueOtherThan(promptTokens, () -> randomInt(100));
            case 2 -> totalTokens = randomValueOtherThan(totalTokens, () -> randomInt(100));
            case 3 -> promptTokensDetails = randomValueOtherThan(
                promptTokensDetails,
                () -> randomBoolean()
                    ? null
                    : new ChatCompletionUsageResponse.PromptTokensDetails(randomNonNegativeInt(), randomNonNegativeInt())
            );
            case 4 -> completionTokenDetails = randomValueOtherThan(
                completionTokenDetails,
                () -> randomBoolean() ? null : new ChatCompletionUsageResponse.CompletionTokenDetails(randomNonNegativeIntOrNull())
            );
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new ChatCompletionUsageResponse(completionTokens, promptTokens, totalTokens, promptTokensDetails, completionTokenDetails);
    }

    public void testToXContentChunked_AllFields() throws IOException {
        var usage = new ChatCompletionUsageResponse(
            12,
            9,
            21,
            new ChatCompletionUsageResponse.PromptTokensDetails(5, 3),
            new ChatCompletionUsageResponse.CompletionTokenDetails(7)
        );

        assertThat(toXContent(usage), is(XContentHelper.stripWhitespace("""
            {
              "completion_tokens": 12,
              "prompt_tokens": 9,
              "total_tokens": 21,
              "prompt_tokens_details": {
                "cached_tokens": 5,
                "cache_write_tokens": 3
              },
              "completion_tokens_details": {
                "reasoning_tokens": 7
              }
            }
            """)));
    }

    public void testToXContentChunked_NoDetails() throws IOException {
        var usage = new ChatCompletionUsageResponse(12, 9, 21);

        assertThat(toXContent(usage), is(XContentHelper.stripWhitespace("""
            {
              "completion_tokens": 12,
              "prompt_tokens": 9,
              "total_tokens": 21
            }
            """)));
    }

    public void testToXContentChunked_NullReasoningTokens_CompletionTokenDetailsOmitted() throws IOException {
        // completionTokenDetails is only emitted when reasoningTokens is non-null
        var usage = new ChatCompletionUsageResponse(
            12,
            9,
            21,
            null,
            new ChatCompletionUsageResponse.CompletionTokenDetails((Integer) null)
        );

        assertThat(toXContent(usage), is(XContentHelper.stripWhitespace("""
            {
              "completion_tokens": 12,
              "prompt_tokens": 9,
              "total_tokens": 21
            }
            """)));
    }

    public void testToXContentChunked_PartialPromptTokensDetails() throws IOException {
        var usage = new ChatCompletionUsageResponse(5, 3, 8, new ChatCompletionUsageResponse.PromptTokensDetails(2, null), null);

        assertThat(toXContent(usage), is(XContentHelper.stripWhitespace("""
            {
              "completion_tokens": 5,
              "prompt_tokens": 3,
              "total_tokens": 8,
              "prompt_tokens_details": {
                "cached_tokens": 2
              }
            }
            """)));
    }

    public void testToXContentChunked_EmptyPromptTokensDetails_OmitsField() throws IOException {
        // A non-null instance with both fields null must not produce a dangling field name with no value.
        var usage = new ChatCompletionUsageResponse(5, 3, 8, new ChatCompletionUsageResponse.PromptTokensDetails(null, null), null);

        assertThat(toXContent(usage), is(XContentHelper.stripWhitespace("""
            {
              "completion_tokens": 5,
              "prompt_tokens": 3,
              "total_tokens": 8
            }
            """)));
    }

    public void testToXContentChunked_EmptyCompletionTokenDetails_OmitsField() throws IOException {
        // A non-null instance with null reasoning tokens must omit the field entirely.
        var usage = new ChatCompletionUsageResponse(5, 3, 8, null, new ChatCompletionUsageResponse.CompletionTokenDetails((Integer) null));

        assertThat(toXContent(usage), is(XContentHelper.stripWhitespace("""
            {
              "completion_tokens": 5,
              "prompt_tokens": 3,
              "total_tokens": 8
            }
            """)));
    }

    public void testOfNullable_BothNull_ReturnsNull() {
        assertNull(ChatCompletionUsageResponse.PromptTokensDetails.ofNullable(null, null));
        assertNull(ChatCompletionUsageResponse.CompletionTokenDetails.ofNullable(null));
    }

    public void testOfNullable_AtLeastOneNonNull_ReturnsInstance() {
        assertNotNull(ChatCompletionUsageResponse.PromptTokensDetails.ofNullable(5, null));
        assertNotNull(ChatCompletionUsageResponse.PromptTokensDetails.ofNullable(null, 3));
        assertNotNull(ChatCompletionUsageResponse.PromptTokensDetails.ofNullable(5, 3));
        assertNotNull(ChatCompletionUsageResponse.CompletionTokenDetails.ofNullable(7));
    }

    static String toXContent(ChatCompletionUsageResponse usage) throws IOException {
        var builder = JsonXContent.contentBuilder();
        usage.toXContentChunked(null).forEachRemaining(chunk -> {
            try {
                chunk.toXContent(builder, null);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        return Strings.toString(builder);
    }
}
