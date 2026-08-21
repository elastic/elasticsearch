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

public class ChatCompletionUsageTests extends AbstractBWCWireSerializationTestCase<ChatCompletionUsage> {

    public static ChatCompletionUsage randomChatCompletionUsage() {
        return new ChatCompletionUsage(
            randomInt(100),
            randomInt(100),
            randomInt(100),
            randomBoolean()
                ? null
                : new ChatCompletionUsage.PromptTokensDetails(randomNonNegativeIntOrNull(), randomNonNegativeIntOrNull()),
            randomBoolean() ? null : new ChatCompletionUsage.CompletionTokenDetails(randomNonNegativeIntOrNull())
        );
    }

    public static ChatCompletionUsage downgrade(ChatCompletionUsage instance, TransportVersion version) {
        var promptTokensDetails = instance.promptTokensDetails();
        var completionTokenDetails = instance.completionTokenDetails();

        if (version.supports(CHAT_COMPLETION_CACHE_WRITE_TOKENS_SUPPORT_ADDED) == false && promptTokensDetails != null) {
            promptTokensDetails = promptTokensDetails.cachedTokens() == null
                ? null
                : new ChatCompletionUsage.PromptTokensDetails(promptTokensDetails.cachedTokens(), null);
        }

        if (version.supports(INFERENCE_CACHED_TOKENS) == false) {
            promptTokensDetails = null;
        }

        if (version.supports(CHAT_COMPLETION_REASONING_SUPPORT_ADDED) == false) {
            completionTokenDetails = null;
        }

        return new ChatCompletionUsage(
            instance.completionTokens(),
            instance.promptTokens(),
            instance.totalTokens(),
            promptTokensDetails,
            completionTokenDetails
        );
    }

    @Override
    protected Writeable.Reader<ChatCompletionUsage> instanceReader() {
        return ChatCompletionUsage::new;
    }

    @Override
    protected ChatCompletionUsage createTestInstance() {
        return randomChatCompletionUsage();
    }

    @Override
    protected ChatCompletionUsage mutateInstanceForVersion(ChatCompletionUsage instance, TransportVersion version) {
        return downgrade(instance, version);
    }

    @Override
    protected ChatCompletionUsage mutateInstance(ChatCompletionUsage instance) {
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
                () -> randomBoolean() ? null : new ChatCompletionUsage.PromptTokensDetails(randomNonNegativeInt(), randomNonNegativeInt())
            );
            case 4 -> completionTokenDetails = randomValueOtherThan(
                completionTokenDetails,
                () -> randomBoolean() ? null : new ChatCompletionUsage.CompletionTokenDetails(randomNonNegativeIntOrNull())
            );
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new ChatCompletionUsage(completionTokens, promptTokens, totalTokens, promptTokensDetails, completionTokenDetails);
    }

    public void testToXContentChunked_AllFields() throws IOException {
        var usage = new ChatCompletionUsage(
            12,
            9,
            21,
            new ChatCompletionUsage.PromptTokensDetails(5, 3),
            new ChatCompletionUsage.CompletionTokenDetails(7)
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
        var usage = new ChatCompletionUsage(12, 9, 21);

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
        var usage = new ChatCompletionUsage(12, 9, 21, null, new ChatCompletionUsage.CompletionTokenDetails((Integer) null));

        assertThat(toXContent(usage), is(XContentHelper.stripWhitespace("""
            {
              "completion_tokens": 12,
              "prompt_tokens": 9,
              "total_tokens": 21
            }
            """)));
    }

    public void testToXContentChunked_PartialPromptTokensDetails() throws IOException {
        var usage = new ChatCompletionUsage(5, 3, 8, new ChatCompletionUsage.PromptTokensDetails(2, null), null);

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
        var usage = new ChatCompletionUsage(5, 3, 8, new ChatCompletionUsage.PromptTokensDetails(null, null), null);

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
        var usage = new ChatCompletionUsage(5, 3, 8, null, new ChatCompletionUsage.CompletionTokenDetails((Integer) null));

        assertThat(toXContent(usage), is(XContentHelper.stripWhitespace("""
            {
              "completion_tokens": 5,
              "prompt_tokens": 3,
              "total_tokens": 8
            }
            """)));
    }

    public void testOfNullable_BothNull_ReturnsNull() {
        assertNull(ChatCompletionUsage.PromptTokensDetails.ofNullable(null, null));
        assertNull(ChatCompletionUsage.CompletionTokenDetails.ofNullable(null));
    }

    public void testOfNullable_AtLeastOneNonNull_ReturnsInstance() {
        assertNotNull(ChatCompletionUsage.PromptTokensDetails.ofNullable(5, null));
        assertNotNull(ChatCompletionUsage.PromptTokensDetails.ofNullable(null, 3));
        assertNotNull(ChatCompletionUsage.PromptTokensDetails.ofNullable(5, 3));
        assertNotNull(ChatCompletionUsage.CompletionTokenDetails.ofNullable(7));
    }

    static String toXContent(ChatCompletionUsage usage) throws IOException {
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
