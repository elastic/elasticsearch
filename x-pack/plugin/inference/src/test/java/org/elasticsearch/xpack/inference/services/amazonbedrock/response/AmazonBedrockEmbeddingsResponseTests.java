/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock.response;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.bedrockruntime.model.InvokeModelResponse;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockProvider;
import org.elasticsearch.xpack.inference.services.amazonbedrock.request.completion.AmazonBedrockChatCompletionRequest;
import org.elasticsearch.xpack.inference.services.amazonbedrock.request.embeddings.AmazonBedrockEmbeddingsRequest;
import org.elasticsearch.xpack.inference.services.amazonbedrock.response.embeddings.AmazonBedrockEmbeddingsResponse;
import org.elasticsearch.xpack.inference.services.cohere.response.CohereResponseTests;

import java.nio.charset.StandardCharsets;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AmazonBedrockEmbeddingsResponseTests extends CohereResponseTests {
    private static final String titanResponse = """
        {
            "inputTextTokenCount": 10,
            "embedding": [
                    -0.008675309,
                    0.0405
            ]
        }
        """;

    @Override
    protected InferenceServiceResults parseResponse(String responseJson) {
        var response = response(responseJson);

        var request = request(AmazonBedrockProvider.COHERE);
        return new AmazonBedrockEmbeddingsResponse(response).accept(request);
    }

    private static InvokeModelResponse response(String body) {
        return InvokeModelResponse.builder().body(SdkBytes.fromString(body, StandardCharsets.UTF_8)).build();
    }

    private static AmazonBedrockEmbeddingsRequest request(AmazonBedrockProvider provider) {
        var request = mock(AmazonBedrockEmbeddingsRequest.class);
        when(request.provider()).thenReturn(provider);
        return request;
    }

    public void testNonEmbeddingsRequestThrowsException() {
        var response = InvokeModelResponse.builder().build();
        var request = mock(AmazonBedrockChatCompletionRequest.class);
        var e = assertThrows(ElasticsearchException.class, () -> new AmazonBedrockEmbeddingsResponse(response).accept(request));
        assertThat(e.getMessage(), equalTo("unexpected request type [" + request.getClass() + "]"));
    }

    public void testNonTitanOrCohereModelThrowsException() {
        var response = response(titanResponse);
        var request = request(AmazonBedrockProvider.AI21LABS);
        var e = assertThrows(ElasticsearchException.class, () -> new AmazonBedrockEmbeddingsResponse(response).accept(request));
        assertThat(e.getMessage(), equalTo("java.io.IOException: Unsupported provider [ai21labs]"));
    }

    public void testTitan() {
        var response = response(titanResponse);
        var request = request(AmazonBedrockProvider.AMAZONTITAN);
        var inferenceServiceResults = new AmazonBedrockEmbeddingsResponse(response).accept(request);
        assertFloatEmbeddings(inferenceServiceResults, new float[] { -0.008675309F, 0.0405F });
    }
}
