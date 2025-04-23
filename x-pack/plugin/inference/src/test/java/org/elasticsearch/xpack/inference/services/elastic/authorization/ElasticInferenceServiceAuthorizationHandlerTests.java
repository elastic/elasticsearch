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
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xpack.inference.LocalStateInferencePlugin;
import org.elasticsearch.xpack.inference.Utils;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.elasticsearch.xpack.inference.services.elastic.DefaultModelConfig;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceService;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceComponents;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.elastic.completion.ElasticInferenceServiceCompletionModel;
import org.elasticsearch.xpack.inference.services.elastic.completion.ElasticInferenceServiceCompletionServiceSettings;
import org.elasticsearch.xpack.inference.services.elastic.response.ElasticInferenceServiceAuthorizationResponseEntity;
import org.elasticsearch.xpack.inference.services.elastic.sparseembeddings.ElasticInferenceServiceSparseEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.elastic.sparseembeddings.ElasticInferenceServiceSparseEmbeddingsServiceSettings;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.inference.services.ServiceComponentsTests.createWithEmptySettings;
import static org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceService.defaultEndpointId;
import static org.hamcrest.CoreMatchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class ElasticInferenceServiceAuthorizationHandlerTests extends ESSingleNodeTestCase {
    private DeterministicTaskQueue taskQueue;
    private ModelRegistry modelRegistry;

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(LocalStateInferencePlugin.class);
    }

    @Before
    public void init() throws Exception {
        taskQueue = new DeterministicTaskQueue();
        modelRegistry = getInstanceFromNode(ModelRegistry.class);
    }

    public void testSendsAnAuthorizationRequestTwice() throws Exception {
        var callbackCount = new AtomicInteger(0);
        // we're only interested in two authorization calls which is why I'm using a value of 2 here
        var latch = new CountDownLatch(2);
        final AtomicReference<ElasticInferenceServiceAuthorizationHandler> handlerRef = new AtomicReference<>();

        Runnable callback = () -> {
            // the first authorization response does not contain a streaming task so we're expecting to not support streaming here
            if (callbackCount.incrementAndGet() == 1) {
                assertThat(handlerRef.get().supportedStreamingTasks(), is(EnumSet.noneOf(TaskType.class)));
            }
            latch.countDown();

            // we only want to run the tasks twice, so advance the time on the queue
            // which flags the scheduled authorization request to be ready to run
            if (callbackCount.get() == 1) {
                taskQueue.advanceTime();
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
                        new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedModel("abc", EnumSet.of(TaskType.SPARSE_EMBEDDING))
                    )
                )
            ),
            ElasticInferenceServiceAuthorizationModel.of(
                new ElasticInferenceServiceAuthorizationResponseEntity(
                    List.of(
                        new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedModel(
                            "rainbow-sprinkles",
                            EnumSet.of(TaskType.CHAT_COMPLETION)
                        )
                    )
                )
            )
        );

        handlerRef.set(
            new ElasticInferenceServiceAuthorizationHandler(
                createWithEmptySettings(taskQueue.getThreadPool()),
                modelRegistry,
                requestHandler,
                initDefaultEndpoints(),
                EnumSet.of(TaskType.SPARSE_EMBEDDING, TaskType.CHAT_COMPLETION),
                null,
                mock(Sender.class),
                ElasticInferenceServiceSettingsTests.create(null, TimeValue.timeValueMillis(1), TimeValue.timeValueMillis(1), true),
                callback
            )
        );

        var handler = handlerRef.get();
        handler.init();
        taskQueue.runAllRunnableTasks();
        latch.await(Utils.TIMEOUT.getSeconds(), TimeUnit.SECONDS);
        // this should be after we've received both authorization responses

        assertThat(handler.supportedStreamingTasks(), is(EnumSet.of(TaskType.CHAT_COMPLETION)));
        assertThat(
            handler.defaultConfigIds(),
            is(
                List.of(
                    new InferenceService.DefaultConfigId(
                        ".rainbow-sprinkles-elastic",
                        MinimalServiceSettings.chatCompletion(ElasticInferenceService.NAME),
                        null
                    )
                )
            )
        );
        assertThat(handler.supportedTaskTypes(), is(EnumSet.of(TaskType.SPARSE_EMBEDDING, TaskType.CHAT_COMPLETION)));

        PlainActionFuture<List<Model>> listener = new PlainActionFuture<>();
        handler.defaultConfigs(listener);

        var configs = listener.actionGet();
        assertThat(configs.get(0).getConfigurations().getInferenceEntityId(), is(".rainbow-sprinkles-elastic"));
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
                MinimalServiceSettings.chatCompletion(ElasticInferenceService.NAME)
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
                MinimalServiceSettings.sparseEmbedding(ElasticInferenceService.NAME)
            )
        );
    }
}
