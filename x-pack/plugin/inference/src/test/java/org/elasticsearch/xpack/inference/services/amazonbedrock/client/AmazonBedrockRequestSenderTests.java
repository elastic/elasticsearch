/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock.client;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.external.http.sender.ChatCompletionInput;
import org.elasticsearch.xpack.inference.external.http.sender.EmbeddingsInput;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.elasticsearch.xpack.inference.services.ServiceComponentsTests;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockChatCompletionRequestManager;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockEmbeddingsRequestManager;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockProvider;
import org.elasticsearch.xpack.inference.services.amazonbedrock.completion.AmazonBedrockChatCompletionModelTests;
import org.elasticsearch.xpack.inference.services.amazonbedrock.embeddings.AmazonBedrockEmbeddingsModelTests;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.core.inference.results.ChatCompletionResultsTests.buildExpectationCompletion;
import static org.elasticsearch.xpack.core.inference.results.DenseEmbeddingFloatResultsTests.buildExpectationFloat;
import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityExecutors;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.client.AmazonBedrockExecutorTests.TEST_AMAZON_TITAN_EMBEDDINGS_RESULT;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;

public class AmazonBedrockRequestSenderTests extends ESTestCase {
    private static final TimeValue TIMEOUT = new TimeValue(30, TimeUnit.SECONDS);
    private ThreadPool threadPool;
    private final AtomicReference<Thread> threadRef = new AtomicReference<>();

    @Before
    public void init() throws Exception {
        threadPool = createThreadPool(inferenceUtilityExecutors());
        threadRef.set(null);
    }

    @After
    public void shutdown() throws IOException, InterruptedException {
        if (threadRef.get() != null) {
            threadRef.get().join(TIMEOUT.millis());
        }

        terminate(threadPool);
    }

    public void testCreateSender_UsesTheSameInstanceForRequestExecutor() throws Exception {
        var requestSender = new AmazonBedrockMockExecuteRequestSender(new AmazonBedrockMockClientCache(), mock(ThrottlerManager.class));
        requestSender.enqueue(AmazonBedrockExecutorTests.getTestInvokeResult(TEST_AMAZON_TITAN_EMBEDDINGS_RESULT));
        var senderFactory = createSenderFactory(threadPool, Settings.EMPTY, requestSender);

        var sender1 = createSender(senderFactory);
        var sender2 = createSender(senderFactory);

        assertThat(sender1, instanceOf(AmazonBedrockRequestSender.class));
        assertThat(sender2, instanceOf(AmazonBedrockRequestSender.class));

        assertThat(sender1, sameInstance(sender2));
    }

    public void testCreateSender_CanCallStartMultipleTimes() throws Exception {
        var requestSender = new AmazonBedrockMockExecuteRequestSender(new AmazonBedrockMockClientCache(), mock(ThrottlerManager.class));
        requestSender.enqueue(AmazonBedrockExecutorTests.getTestInvokeResult(TEST_AMAZON_TITAN_EMBEDDINGS_RESULT));
        var senderFactory = createSenderFactory(threadPool, Settings.EMPTY, requestSender);

        try (var sender = createSender(senderFactory)) {
            sender.startSynchronously();
            sender.startSynchronously();
            sender.startSynchronously();
        }
    }

    public void testCreateSender_SendsEmbeddingsRequestAndReceivesResponse() throws Exception {
        var requestSender = new AmazonBedrockMockExecuteRequestSender(new AmazonBedrockMockClientCache(), mock(ThrottlerManager.class));
        requestSender.enqueue(AmazonBedrockExecutorTests.getTestInvokeResult(TEST_AMAZON_TITAN_EMBEDDINGS_RESULT));
        var senderFactory = createSenderFactory(threadPool, Settings.EMPTY, requestSender);
        try (var sender = createSender(senderFactory)) {
            sender.startSynchronously();

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
            sender.send(requestManager, new EmbeddingsInput(List.of("abc"), null), null, listener);

            var result = listener.actionGet(TIMEOUT);
            assertThat(result.asMap(), is(buildExpectationFloat(List.of(new float[] { 0.123F, 0.456F, 0.678F, 0.789F }))));
        }
    }

    public void testCreateSender_SendsCompletionRequestAndReceivesResponse() throws Exception {
        var requestSender = new AmazonBedrockMockExecuteRequestSender(new AmazonBedrockMockClientCache(), mock(ThrottlerManager.class));
        requestSender.enqueue(AmazonBedrockExecutorTests.getTestConverseResult("test response text"));
        var senderFactory = createSenderFactory(threadPool, Settings.EMPTY, requestSender);
        try (var sender = createSender(senderFactory)) {
            sender.startSynchronously();

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
            sender.send(requestManager, new ChatCompletionInput(List.of("abc")), null, listener);

            var result = listener.actionGet(TIMEOUT);
            assertThat(result.asMap(), is(buildExpectationCompletion(List.of("test response text"))));
        }
    }

    public static AmazonBedrockRequestSender.Factory createSenderFactory(
        ThreadPool threadPool,
        Settings settings,
        AmazonBedrockMockExecuteRequestSender requestSender
    ) {
        return new AmazonBedrockRequestSender.Factory(
            ServiceComponentsTests.createWithSettings(threadPool, settings),
            mockClusterServiceEmpty(),
            requestSender
        );
    }

    public static Sender createSender(AmazonBedrockRequestSender.Factory factory) {
        return factory.createSender();
    }
}
