/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.ChatCompletionResults;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingFloatResults;
import org.elasticsearch.xpack.inference.InputTypeTests;
import org.elasticsearch.xpack.inference.external.http.sender.ChatCompletionInput;
import org.elasticsearch.xpack.inference.external.http.sender.EmbeddingsInput;
import org.elasticsearch.xpack.inference.services.ServiceComponentsTests;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockProvider;
import org.elasticsearch.xpack.inference.services.amazonbedrock.client.AmazonBedrockMockRequestSender;
import org.elasticsearch.xpack.inference.services.amazonbedrock.completion.AmazonBedrockChatCompletionModelTests;
import org.elasticsearch.xpack.inference.services.amazonbedrock.embeddings.AmazonBedrockEmbeddingsModelTests;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.core.inference.results.ChatCompletionResultsTests.buildExpectationCompletion;
import static org.elasticsearch.xpack.core.inference.results.TextEmbeddingFloatResultsTests.buildExpectationFloat;
import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityPool;
import static org.hamcrest.Matchers.is;

public class AmazonBedrockActionCreatorTests extends ESTestCase {
    private static final TimeValue TIMEOUT = new TimeValue(30, TimeUnit.SECONDS);
    private ThreadPool threadPool;

    @Before
    public void init() throws Exception {
        threadPool = createThreadPool(inferenceUtilityPool());
    }

    @After
    public void shutdown() throws IOException {
        terminate(threadPool);
    }

    public void testEmbeddingsRequestAction_Titan() throws IOException {
        var serviceComponents = ServiceComponentsTests.createWithEmptySettings(threadPool);
        var mockedFloatResults = List.of(new TextEmbeddingFloatResults.Embedding(new float[] { 0.0123F, -0.0123F }));
        var mockedResult = new TextEmbeddingFloatResults(mockedFloatResults);
        try (var sender = new AmazonBedrockMockRequestSender()) {
            sender.enqueue(mockedResult);
            var creator = new AmazonBedrockActionCreator(sender, serviceComponents, TIMEOUT);
            var model = AmazonBedrockEmbeddingsModelTests.createModel(
                "test_id",
                "test_region",
                "test_model",
                AmazonBedrockProvider.AMAZONTITAN,
                null,
                false,
                null,
                null,
                null,
                "accesskey",
                "secretkey"
            );
            var action = creator.create(model, Map.of());
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(
                new EmbeddingsInput(List.of("abc"), null, InputTypeTests.randomWithNull()),
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );
            var result = listener.actionGet(TIMEOUT);

            assertThat(result.asMap(), is(buildExpectationFloat(List.of(new float[] { 0.0123F, -0.0123F }))));

            assertThat(sender.sendCount(), is(1));
            var sentInputs = sender.getInputs();
            assertThat(sentInputs.size(), is(1));
            assertThat(sentInputs.get(0), is("abc"));
        }
    }

    public void testEmbeddingsRequestAction_Cohere() throws IOException {
        var serviceComponents = ServiceComponentsTests.createWithEmptySettings(threadPool);
        var mockedFloatResults = List.of(new TextEmbeddingFloatResults.Embedding(new float[] { 0.0123F, -0.0123F }));
        var mockedResult = new TextEmbeddingFloatResults(mockedFloatResults);
        try (var sender = new AmazonBedrockMockRequestSender()) {
            sender.enqueue(mockedResult);
            var creator = new AmazonBedrockActionCreator(sender, serviceComponents, TIMEOUT);
            var model = AmazonBedrockEmbeddingsModelTests.createModel(
                "test_id",
                "test_region",
                "test_model",
                AmazonBedrockProvider.COHERE,
                null,
                false,
                null,
                null,
                null,
                "accesskey",
                "secretkey"
            );
            var action = creator.create(model, Map.of());
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            var inputType = InputTypeTests.randomWithNull();
            action.execute(new EmbeddingsInput(List.of("abc"), null, inputType), InferenceAction.Request.DEFAULT_TIMEOUT, listener);
            var result = listener.actionGet(TIMEOUT);

            assertThat(result.asMap(), is(buildExpectationFloat(List.of(new float[] { 0.0123F, -0.0123F }))));

            assertThat(sender.sendCount(), is(1));
            var sentInputs = sender.getInputs();
            assertThat(sentInputs.size(), is(1));
            assertThat(sentInputs.get(0), is("abc"));

            if (inputType != null) {
                var sentInputType = sender.getInputType();
                assertThat(sentInputType, is(inputType));
            }
        }
    }

    public void testEmbeddingsRequestAction_HandlesException() throws IOException {
        var serviceComponents = ServiceComponentsTests.createWithEmptySettings(threadPool);
        var mockedResult = new ElasticsearchException("mock exception");
        try (var sender = new AmazonBedrockMockRequestSender()) {
            sender.enqueue(mockedResult);
            var creator = new AmazonBedrockActionCreator(sender, serviceComponents, TIMEOUT);
            var model = AmazonBedrockEmbeddingsModelTests.createModel(
                "test_id",
                "test_region",
                "test_model",
                AmazonBedrockProvider.AMAZONTITAN,
                "accesskey",
                "secretkey"
            );
            var action = creator.create(model, Map.of());
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(new EmbeddingsInput(List.of("abc"), null, null), InferenceAction.Request.DEFAULT_TIMEOUT, listener);
            var thrownException = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));

            assertThat(sender.sendCount(), is(1));
            assertThat(sender.getInputs().size(), is(1));
            assertThat(thrownException.getMessage(), is("mock exception"));
        }
    }

    public void testCompletionRequestAction() throws IOException {
        var serviceComponents = ServiceComponentsTests.createWithEmptySettings(threadPool);
        var mockedChatCompletionResults = List.of(new ChatCompletionResults.Result("test input string"));
        var mockedResult = new ChatCompletionResults(mockedChatCompletionResults);
        try (var sender = new AmazonBedrockMockRequestSender()) {
            sender.enqueue(mockedResult);
            var creator = new AmazonBedrockActionCreator(sender, serviceComponents, TIMEOUT);
            var model = AmazonBedrockChatCompletionModelTests.createModel(
                "test_id",
                "test_region",
                "test_model",
                AmazonBedrockProvider.AMAZONTITAN,
                null,
                null,
                null,
                null,
                null,
                "accesskey",
                "secretkey"
            );
            var action = creator.create(model, Map.of());
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(new ChatCompletionInput(List.of("abc")), InferenceAction.Request.DEFAULT_TIMEOUT, listener);
            var result = listener.actionGet(TIMEOUT);

            assertThat(result.asMap(), is(buildExpectationCompletion(List.of("test input string"))));

            assertThat(sender.sendCount(), is(1));
            var sentInputs = sender.getInputs();
            assertThat(sentInputs.size(), is(1));
            assertThat(sentInputs.get(0), is("abc"));
        }
    }

    public void testChatCompletionRequestAction_HandlesException() throws IOException {
        var serviceComponents = ServiceComponentsTests.createWithEmptySettings(threadPool);
        var mockedResult = new ElasticsearchException("mock exception");
        try (var sender = new AmazonBedrockMockRequestSender()) {
            sender.enqueue(mockedResult);
            var creator = new AmazonBedrockActionCreator(sender, serviceComponents, TIMEOUT);
            var model = AmazonBedrockChatCompletionModelTests.createModel(
                "test_id",
                "test_region",
                "test_model",
                AmazonBedrockProvider.AMAZONTITAN,
                null,
                null,
                null,
                null,
                null,
                "accesskey",
                "secretkey"
            );
            var action = creator.create(model, Map.of());
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(new ChatCompletionInput(List.of("abc")), InferenceAction.Request.DEFAULT_TIMEOUT, listener);
            var thrownException = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));

            assertThat(sender.sendCount(), is(1));
            assertThat(sender.getInputs().size(), is(1));
            assertThat(thrownException.getMessage(), is("mock exception"));
        }
    }

}
