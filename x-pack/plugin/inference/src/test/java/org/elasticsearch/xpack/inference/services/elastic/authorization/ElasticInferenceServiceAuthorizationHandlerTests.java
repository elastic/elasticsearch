/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.authorization;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.EmptySecretSettings;
import org.elasticsearch.inference.EmptyTaskSettings;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.MinimalServiceSettings;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.Utils;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.external.response.elastic.ElasticInferenceServiceAuthorizationResponseEntity;
import org.elasticsearch.xpack.inference.services.elastic.DefaultModelConfig;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceComponents;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceSparseEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceSparseEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.elastic.completion.ElasticInferenceServiceCompletionModel;
import org.elasticsearch.xpack.inference.services.elastic.completion.ElasticInferenceServiceCompletionServiceSettings;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.inference.services.ServiceComponentsTests.createWithEmptySettings;
import static org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceService.defaultEndpointId;
import static org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceTests.mockModelRegistry;
import static org.hamcrest.CoreMatchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class ElasticInferenceServiceAuthorizationHandlerTests extends ESTestCase {
    // private ThreadPool threadPool;
    private DeterministicTaskQueue taskQueue;

    @Before
    public void init() throws Exception {
        // threadPool = createThreadPool(inferenceUtilityPool());
        taskQueue = new DeterministicTaskQueue();
    }

    @After
    public void shutdown() throws IOException {
        // terminate(threadPool);
    }

    public void testDefaultConfigs_Returns_DefaultChatCompletion_V1_WhenTaskTypeIsIncorrect() throws Exception {
        var callbackCount = new AtomicInteger(0);
        var latch = new CountDownLatch(2);
        final AtomicReference<ElasticInferenceServiceAuthorizationHandler> handlerRef = new AtomicReference<>();

        Runnable callback = () -> {
            if (callbackCount.incrementAndGet() == 1) {
                assertThat(handlerRef.get().supportedStreamingTasks(), is(EnumSet.noneOf(TaskType.class)));
            }
            latch.countDown();

            // only run the scheduled tasks if this was the first callback we got, we should have one more task to execute
            // otherwise we'll get in infinite loop
            if (callbackCount.get() == 1) {
                taskQueue.runAllRunnableTasks();
            } else {
                try {
                    handlerRef.get().close();
                } catch (IOException e) {
                    // ignore
                }
            }
        };

        var requestHandler = mockAuthorizationRequestHandler(
            ElasticInferenceServiceAuthorizationModel.of(
                new ElasticInferenceServiceAuthorizationResponseEntity(
                    List.of(
                        new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedModel(
                            "rainbow-sprinkles",
                            EnumSet.of(TaskType.SPARSE_EMBEDDING)
                        )
                    )
                )
            ),
            ElasticInferenceServiceAuthorizationModel.of(
                new ElasticInferenceServiceAuthorizationResponseEntity(
                    List.of(
                        new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedModel("abc", EnumSet.of(TaskType.CHAT_COMPLETION))
                    )
                )
            )
        );

        handlerRef.set(
            new ElasticInferenceServiceAuthorizationHandler(
                createWithEmptySettings(taskQueue.getThreadPool()),
                mockModelRegistry(taskQueue.getThreadPool()),
                requestHandler,
                initDefaultEndpoints(),
                EnumSet.of(TaskType.SPARSE_EMBEDDING, TaskType.CHAT_COMPLETION),
                null,
                mock(Sender.class),
                TimeValue.timeValueMillis(1),
                TimeValue.timeValueMillis(1),
                callback
            )
        );

        var handler = handlerRef.get();
        handler.init();
        taskQueue.runAllRunnableTasks();
        latch.await(Utils.TIMEOUT.getSeconds(), TimeUnit.SECONDS);

        assertThat(handler.supportedStreamingTasks(), is(EnumSet.of(TaskType.CHAT_COMPLETION)));
        assertThat(
            handler.defaultConfigIds(),
            is(List.of(new InferenceService.DefaultConfigId(".rainbow-sprinkles-elastic", MinimalServiceSettings.chatCompletion(), null)))
        );
        assertThat(handler.supportedTaskTypes(), is(EnumSet.of(TaskType.SPARSE_EMBEDDING)));

        PlainActionFuture<List<Model>> listener = new PlainActionFuture<>();
        handler.defaultConfigs(listener);

        var configs = listener.actionGet();
        assertThat(configs.get(0).getConfigurations().getInferenceEntityId(), is(".rainbow-sprinkles-elastic"));
        assertThat(configs.get(1).getConfigurations().getInferenceEntityId(), is(".rainbow-sprinkles-elastic"));
    }

    private static ElasticInferenceServiceAuthorizationRequestHandler mockAuthorizationRequestHandler(
        ElasticInferenceServiceAuthorizationModel firstAuthResponse,
        ElasticInferenceServiceAuthorizationModel secondAuthResponse
    ) {
        var mockAuthHandler = mock(ElasticInferenceServiceAuthorizationRequestHandler.class);
        doAnswer(invocation -> {
            ActionListener<ElasticInferenceServiceAuthorizationModel> listener = invocation.getArgument(0);
            listener.onResponse(firstAuthResponse);
            return Void.TYPE;
        }).doAnswer(invocation -> {
            ActionListener<ElasticInferenceServiceAuthorizationModel> listener = invocation.getArgument(0);
            listener.onResponse(secondAuthResponse);
            return Void.TYPE;
        }).when(mockAuthHandler).getAuthorization(any(), any());

        return mockAuthHandler;
    }

    private static Map<String, DefaultModelConfig> initDefaultEndpoints() {
        return Map.of(
            "rainbow-sprinkles",
            new DefaultModelConfig(
                new ElasticInferenceServiceCompletionModel(
                    defaultEndpointId("rainbow-sprinkles"),
                    TaskType.CHAT_COMPLETION,
                    "test",
                    new ElasticInferenceServiceCompletionServiceSettings("rainbow-sprinkles", null),
                    EmptyTaskSettings.INSTANCE,
                    EmptySecretSettings.INSTANCE,
                    ElasticInferenceServiceComponents.EMPTY_INSTANCE
                ),
                MinimalServiceSettings.chatCompletion()
            ),
            "elser-v2",
            new DefaultModelConfig(
                new ElasticInferenceServiceSparseEmbeddingsModel(
                    defaultEndpointId("elser-v2"),
                    TaskType.SPARSE_EMBEDDING,
                    "test",
                    new ElasticInferenceServiceSparseEmbeddingsServiceSettings("elser-v2", null, null),
                    EmptyTaskSettings.INSTANCE,
                    EmptySecretSettings.INSTANCE,
                    ElasticInferenceServiceComponents.EMPTY_INSTANCE
                ),
                MinimalServiceSettings.sparseEmbedding()
            )
        );
    }
}
