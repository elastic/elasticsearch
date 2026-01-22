/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock.response.completion;

import software.amazon.awssdk.services.bedrockruntime.model.ContentBlock;
import software.amazon.awssdk.services.bedrockruntime.model.ConverseResponse;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xpack.core.inference.results.ChatCompletionResults;
import org.elasticsearch.xpack.inference.services.amazonbedrock.request.AmazonBedrockRequest;
import org.elasticsearch.xpack.inference.services.amazonbedrock.request.completion.AmazonBedrockChatCompletionRequest;
import org.elasticsearch.xpack.inference.services.amazonbedrock.response.AmazonBedrockResponse;

public class AmazonBedrockChatCompletionResponse extends AmazonBedrockResponse {

    private final ConverseResponse result;

    public AmazonBedrockChatCompletionResponse(ConverseResponse responseResult) {
        this.result = responseResult;
    }

    @Override
    public InferenceServiceResults accept(AmazonBedrockRequest request) {
        if (request instanceof AmazonBedrockChatCompletionRequest asChatCompletionRequest) {
            return fromResponse(result);
        }

        throw new ElasticsearchException("unexpected request type [" + request.getClass() + "]");
    }

    public static ChatCompletionResults fromResponse(ConverseResponse response) {
        var resultTexts = response.output()
            .message()
            .content()
            .stream()
            .map(ContentBlock::text)
            .map(ChatCompletionResults.Result::new)
            .toList();

        return new ChatCompletionResults(resultTexts);
    }
}
