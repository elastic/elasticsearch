/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.amazonbedrock;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.bedrockruntime.model.ContentBlock;
import software.amazon.awssdk.services.bedrockruntime.model.ConverseOutput;
import software.amazon.awssdk.services.bedrockruntime.model.ConverseResponse;
import software.amazon.awssdk.services.bedrockruntime.model.InvokeModelResponse;
import software.amazon.awssdk.services.bedrockruntime.model.Message;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.external.request.amazonbedrock.completion.AmazonBedrockChatCompletionRequest;
import org.elasticsearch.xpack.inference.external.request.amazonbedrock.completion.AmazonBedrockConverseRequestEntity;
import org.elasticsearch.xpack.inference.external.request.amazonbedrock.embeddings.AmazonBedrockEmbeddingsRequest;
import org.elasticsearch.xpack.inference.external.request.amazonbedrock.embeddings.AmazonBedrockTitanEmbeddingsRequestEntity;
import org.elasticsearch.xpack.inference.external.response.amazonbedrock.completion.AmazonBedrockChatCompletionResponseHandler;
import org.elasticsearch.xpack.inference.external.response.amazonbedrock.embeddings.AmazonBedrockEmbeddingsResponseHandler;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockProvider;
import org.elasticsearch.xpack.inference.services.amazonbedrock.completion.AmazonBedrockChatCompletionModelTests;
import org.elasticsearch.xpack.inference.services.amazonbedrock.embeddings.AmazonBedrockEmbeddingsModelTests;

import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.elasticsearch.xpack.inference.common.TruncatorTests.createTruncator;
import static org.elasticsearch.xpack.inference.results.ChatCompletionResultsTests.buildExpectationCompletion;
import static org.elasticsearch.xpack.inference.results.TextEmbeddingResultsTests.buildExpectationFloat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class AmazonBedrockExecutorTests extends ESTestCase {
    public void testExecute_EmbeddingsRequest_ForAmazonTitan() throws CharacterCodingException {
        var model = AmazonBedrockEmbeddingsModelTests.createModel(
            "id",
            "region",
            "model",
            AmazonBedrockProvider.AMAZONTITAN,
            "accesskey",
            "secretkey"
        );
        var truncator = createTruncator();
        var truncatedInput = truncator.truncate(List.of("abc"));
        var requestEntity = new AmazonBedrockTitanEmbeddingsRequestEntity("abc");
        var request = new AmazonBedrockEmbeddingsRequest(truncator, truncatedInput, model, requestEntity, null);
        var responseHandler = new AmazonBedrockEmbeddingsResponseHandler();

        var clientCache = new AmazonBedrockMockClientCache(null, getTestInvokeResult(TEST_AMAZON_TITAN_EMBEDDINGS_RESULT), null);
        var listener = new PlainActionFuture<InferenceServiceResults>();

        var executor = new AmazonBedrockEmbeddingsExecutor(request, responseHandler, logger, () -> false, listener, clientCache);
        executor.run();
        var result = listener.actionGet(new TimeValue(30000));
        assertNotNull(result);
        assertThat(result.asMap(), is(buildExpectationFloat(List.of(new float[] { 0.123F, 0.456F, 0.678F, 0.789F }))));
    }

    public void testExecute_EmbeddingsRequest_ForCohere() throws CharacterCodingException {
        var model = AmazonBedrockEmbeddingsModelTests.createModel(
            "id",
            "region",
            "model",
            AmazonBedrockProvider.COHERE,
            "accesskey",
            "secretkey"
        );
        var requestEntity = new AmazonBedrockTitanEmbeddingsRequestEntity("abc");
        var truncator = createTruncator();
        var truncatedInput = truncator.truncate(List.of("abc"));
        var request = new AmazonBedrockEmbeddingsRequest(truncator, truncatedInput, model, requestEntity, null);
        var responseHandler = new AmazonBedrockEmbeddingsResponseHandler();

        var clientCache = new AmazonBedrockMockClientCache(null, getTestInvokeResult(TEST_COHERE_EMBEDDINGS_RESULT), null);
        var listener = new PlainActionFuture<InferenceServiceResults>();

        var executor = new AmazonBedrockEmbeddingsExecutor(request, responseHandler, logger, () -> false, listener, clientCache);
        executor.run();
        var result = listener.actionGet(new TimeValue(30000));
        assertNotNull(result);
        assertThat(result.asMap(), is(buildExpectationFloat(List.of(new float[] { 0.123F, 0.456F, 0.678F, 0.789F }))));
    }

    public void testExecute_ChatCompletionRequest() throws CharacterCodingException {
        var model = AmazonBedrockChatCompletionModelTests.createModel(
            "id",
            "region",
            "model",
            AmazonBedrockProvider.AMAZONTITAN,
            "accesskey",
            "secretkey"
        );

        var requestEntity = new AmazonBedrockConverseRequestEntity(List.of("abc"), null, null, 512);
        var request = new AmazonBedrockChatCompletionRequest(model, requestEntity, null, false);
        var responseHandler = new AmazonBedrockChatCompletionResponseHandler();

        var clientCache = new AmazonBedrockMockClientCache(getTestConverseResult("converse result"), null, null);
        var listener = new PlainActionFuture<InferenceServiceResults>();

        var executor = new AmazonBedrockChatCompletionExecutor(request, responseHandler, logger, () -> false, listener, clientCache);
        executor.run();
        var result = listener.actionGet(new TimeValue(30000));
        assertNotNull(result);
        assertThat(result.asMap(), is(buildExpectationCompletion(List.of("converse result"))));
    }

    public void testExecute_FailsProperly_WithElasticsearchException() {
        var model = AmazonBedrockChatCompletionModelTests.createModel(
            "id",
            "region",
            "model",
            AmazonBedrockProvider.AMAZONTITAN,
            "accesskey",
            "secretkey"
        );

        var requestEntity = new AmazonBedrockConverseRequestEntity(List.of("abc"), null, null, 512);
        var request = new AmazonBedrockChatCompletionRequest(model, requestEntity, null, false);
        var responseHandler = new AmazonBedrockChatCompletionResponseHandler();

        var clientCache = new AmazonBedrockMockClientCache(null, null, new ElasticsearchException("test exception"));
        var listener = new PlainActionFuture<InferenceServiceResults>();

        var executor = new AmazonBedrockChatCompletionExecutor(request, responseHandler, logger, () -> false, listener, clientCache);
        executor.run();

        var exceptionThrown = assertThrows(ElasticsearchException.class, () -> listener.actionGet(new TimeValue(30000)));
        assertThat(exceptionThrown.getMessage(), containsString("Failed to send request from inference entity id [id]"));
        assertThat(exceptionThrown.getCause().getMessage(), containsString("test exception"));
    }

    public static ConverseResponse getTestConverseResult(String resultText) {
        return ConverseResponse.builder()
            .output(
                ConverseOutput.builder().message(Message.builder().content(ContentBlock.builder().text(resultText).build()).build()).build()
            )
            .build();
    }

    public static InvokeModelResponse getTestInvokeResult(String resultJson) {
        return InvokeModelResponse.builder()
            .contentType("application/json")
            .body(SdkBytes.fromString(resultJson, StandardCharsets.UTF_8))
            .build();
    }

    public static final String TEST_AMAZON_TITAN_EMBEDDINGS_RESULT = """
        {
            "embedding": [0.123, 0.456, 0.678, 0.789],
            "inputTextTokenCount": int
        }""";

    public static final String TEST_COHERE_EMBEDDINGS_RESULT = """
        {
            "embeddings": [
                [0.123, 0.456, 0.678, 0.789]
            ],
            "id": string,
            "response_type" : "embeddings_floats",
            "texts": [string]
        }
        """;
}
