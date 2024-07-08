/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.response.amazonbedrock.completion;

import com.amazonaws.services.bedrockruntime.model.ConverseResult;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xpack.core.inference.results.ChatCompletionResults;
import org.elasticsearch.xpack.inference.external.request.amazonbedrock.AmazonBedrockRequest;
import org.elasticsearch.xpack.inference.external.request.amazonbedrock.completion.AmazonBedrockChatCompletionRequest;
import org.elasticsearch.xpack.inference.external.response.amazonbedrock.AmazonBedrockResponse;

import java.util.ArrayList;

public class AmazonBedrockChatCompletionResponse extends AmazonBedrockResponse {

    private final ConverseResult result;

    public AmazonBedrockChatCompletionResponse(ConverseResult responseResult) {
        this.result = responseResult;
    }

    @Override
    public InferenceServiceResults accept(AmazonBedrockRequest request) {
        if (request instanceof AmazonBedrockChatCompletionRequest asChatCompletionRequest) {
            return fromResponse(result);
        }

        throw new ElasticsearchException("unexpected request type [" + request.getClass() + "]");
    }

    public static ChatCompletionResults fromResponse(ConverseResult response) {
        var responseMessage = response.getOutput().getMessage();

        var messageContents = responseMessage.getContent();
        var resultTexts = new ArrayList<ChatCompletionResults.Result>();
        for (var messageContent : messageContents) {
            resultTexts.add(new ChatCompletionResults.Result(messageContent.getText()));
        }

        return new ChatCompletionResults(resultTexts);
    }
}
