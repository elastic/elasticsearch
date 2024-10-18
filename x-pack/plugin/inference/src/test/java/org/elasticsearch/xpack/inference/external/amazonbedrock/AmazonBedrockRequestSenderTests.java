/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.amazonbedrock;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.external.http.sender.AmazonBedrockChatCompletionRequestManager;
import org.elasticsearch.xpack.inference.external.http.sender.AmazonBedrockEmbeddingsRequestManager;
import org.elasticsearch.xpack.inference.external.http.sender.DocumentsOnlyInput;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.elasticsearch.xpack.inference.services.ServiceComponentsTests;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockProvider;
import org.elasticsearch.xpack.inference.services.amazonbedrock.completion.AmazonBedrockChatCompletionModelTests;
import org.elasticsearch.xpack.inference.services.amazonbedrock.embeddings.AmazonBedrockEmbeddingsModelTests;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityPool;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.external.amazonbedrock.AmazonBedrockExecutorTests.TEST_AMAZON_TITAN_EMBEDDINGS_RESULT;
import static org.elasticsearch.xpack.inference.results.ChatCompletionResultsTests.buildExpectationCompletion;
import static org.elasticsearch.xpack.inference.results.TextEmbeddingResultsTests.buildExpectationFloat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class AmazonBedrockRequestSenderTests extends ESTestCase {
    private static final TimeValue TIMEOUT = new TimeValue(30, TimeUnit.SECONDS);
    private ThreadPool threadPool;
    private final AtomicReference<Thread> threadRef = new AtomicReference<>();

    @Before
    public void init() throws Exception {
        threadPool = createThreadPool(inferenceUtilityPool());
        threadRef.set(null);
    }

    @After
    public void shutdown() throws IOException, InterruptedException {
        if (threadRef.get() != null) {
            threadRef.get().join(TIMEOUT.millis());
        }

        terminate(threadPool);
    }

    public void testCreateSender_SendsEmbeddingsRequestAndReceivesResponse() throws Exception {
        var senderFactory = createSenderFactory(threadPool, Settings.EMPTY);
        var requestSender = new AmazonBedrockMockExecuteRequestSender(new AmazonBedrockMockClientCache(), mock(ThrottlerManager.class));
        requestSender.enqueue(AmazonBedrockExecutorTests.getTestInvokeResult(TEST_AMAZON_TITAN_EMBEDDINGS_RESULT));
        try (var sender = createSender(senderFactory, requestSender)) {
            sender.start();

            var model = AmazonBedrockEmbeddingsModelTests.createModel(
                "test_id",
                "test_region",
                "test_model",
                AmazonBedrockProvider.AMAZONTITAN,
                "accesskey",
                "secretkey"
            );

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            var serviceComponents = ServiceComponentsTests.createWithEmptySettings(threadPool);
            var requestManager = new AmazonBedrockEmbeddingsRequestManager(
                model,
                serviceComponents.truncator(),
                threadPool,
                new TimeValue(30, TimeUnit.SECONDS)
            );
            sender.send(requestManager, new DocumentsOnlyInput(List.of("abc")), null, listener);

            var result = listener.actionGet(TIMEOUT);
            assertThat(result.asMap(), is(buildExpectationFloat(List.of(new float[] { 0.123F, 0.456F, 0.678F, 0.789F }))));
        }
    }

    public void testCreateSender_SendsCompletionRequestAndReceivesResponse() throws Exception {
        var senderFactory = createSenderFactory(threadPool, Settings.EMPTY);
        var requestSender = new AmazonBedrockMockExecuteRequestSender(new AmazonBedrockMockClientCache(), mock(ThrottlerManager.class));
        requestSender.enqueue(AmazonBedrockExecutorTests.getTestConverseResult("test response text"));
        try (var sender = createSender(senderFactory, requestSender)) {
            sender.start();

            var model = AmazonBedrockChatCompletionModelTests.createModel(
                "test_id",
                "test_region",
                "test_model",
                AmazonBedrockProvider.AMAZONTITAN,
                "accesskey",
                "secretkey"
            );

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            var requestManager = new AmazonBedrockChatCompletionRequestManager(model, threadPool, new TimeValue(30, TimeUnit.SECONDS));
            sender.send(requestManager, new DocumentsOnlyInput(List.of("abc")), null, listener);

            var result = listener.actionGet(TIMEOUT);
            assertThat(result.asMap(), is(buildExpectationCompletion(List.of("test response text"))));
        }
    }

    public static AmazonBedrockRequestSender.Factory createSenderFactory(ThreadPool threadPool, Settings settings) {
        return new AmazonBedrockRequestSender.Factory(
            ServiceComponentsTests.createWithSettings(threadPool, settings),
            mockClusterServiceEmpty()
        );
    }

    public static Sender createSender(AmazonBedrockRequestSender.Factory factory, AmazonBedrockExecuteOnlyRequestSender requestSender) {
        return factory.createSender(requestSender);
    }
}
