/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.integration;

import org.elasticsearch.exception.ResourceNotFoundException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.MinimalServiceSettings;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnparsedModel;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.LocalStateInferencePlugin;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderTests;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceService;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.elastic.authorization.ElasticInferenceServiceAuthorizationRequestHandler;
import org.junit.After;
import org.junit.Before;

import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityPool;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.external.http.Utils.getUrl;
import static org.elasticsearch.xpack.inference.services.ServiceComponentsTests.createWithEmptySettings;
import static org.hamcrest.CoreMatchers.is;
import static org.mockito.Mockito.mock;

public class InferenceRevokeDefaultEndpointsIT extends ESSingleNodeTestCase {
    private static final TimeValue TIMEOUT = new TimeValue(30, TimeUnit.SECONDS);

    private ModelRegistry modelRegistry;
    private final MockWebServer webServer = new MockWebServer();
    private ThreadPool threadPool;
    private String gatewayUrl;

    @Before
    public void createComponents() throws Exception {
        threadPool = createThreadPool(inferenceUtilityPool());
        webServer.start();
        gatewayUrl = getUrl(webServer);
        modelRegistry = node().injector().getInstance(ModelRegistry.class);
    }

    @After
    public void shutdown() {
        terminate(threadPool);
        webServer.close();
    }

    @Override
    protected boolean resetNodeAfterTest() {
        return true;
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(ReindexPlugin.class, LocalStateInferencePlugin.class);
    }

    public void testDefaultConfigs_Returns_DefaultChatCompletion_V1_WhenTaskTypeIsCorrect() throws Exception {
        String responseJson = """
            {
                "models": [
                    {
                      "model_name": "rainbow-sprinkles",
                      "task_types": ["chat"]
                    }
                ]
            }
            """;

        webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

        try (var service = createElasticInferenceService()) {
            ensureAuthorizationCallFinished(service);
            assertThat(service.supportedStreamingTasks(), is(EnumSet.of(TaskType.CHAT_COMPLETION)));
            assertThat(
                service.defaultConfigIds(),
                is(
                    List.of(
                        new InferenceService.DefaultConfigId(
                            ".rainbow-sprinkles-elastic",
                            MinimalServiceSettings.chatCompletion(ElasticInferenceService.NAME),
                            service
                        )
                    )
                )
            );
            assertThat(service.supportedTaskTypes(), is(EnumSet.of(TaskType.CHAT_COMPLETION)));

            PlainActionFuture<List<Model>> listener = new PlainActionFuture<>();
            service.defaultConfigs(listener);
            assertThat(listener.actionGet(TIMEOUT).get(0).getConfigurations().getInferenceEntityId(), is(".rainbow-sprinkles-elastic"));
        }
    }

    public void testRemoves_DefaultChatCompletion_V1_WhenAuthorizationReturnsEmpty() throws Exception {
        {
            String responseJson = """
                {
                    "models": [
                        {
                          "model_name": "rainbow-sprinkles",
                          "task_types": ["chat"]
                        }
                    ]
                }
                """;

            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            try (var service = createElasticInferenceService()) {
                ensureAuthorizationCallFinished(service);

                assertThat(service.supportedStreamingTasks(), is(EnumSet.of(TaskType.CHAT_COMPLETION)));
                assertThat(
                    service.defaultConfigIds(),
                    is(
                        List.of(
                            new InferenceService.DefaultConfigId(
                                ".rainbow-sprinkles-elastic",
                                MinimalServiceSettings.chatCompletion(ElasticInferenceService.NAME),
                                service
                            )
                        )
                    )
                );
                assertThat(service.supportedTaskTypes(), is(EnumSet.of(TaskType.CHAT_COMPLETION)));

                PlainActionFuture<List<Model>> listener = new PlainActionFuture<>();
                service.defaultConfigs(listener);
                assertThat(listener.actionGet(TIMEOUT).get(0).getConfigurations().getInferenceEntityId(), is(".rainbow-sprinkles-elastic"));

                var getModelListener = new PlainActionFuture<UnparsedModel>();
                // persists the default endpoints
                modelRegistry.getModel(".rainbow-sprinkles-elastic", getModelListener);

                var inferenceEntity = getModelListener.actionGet(TIMEOUT);
                assertThat(inferenceEntity.inferenceEntityId(), is(".rainbow-sprinkles-elastic"));
                assertThat(inferenceEntity.taskType(), is(TaskType.CHAT_COMPLETION));
            }
        }
        {
            String noAuthorizationResponseJson = """
                {
                    "models": []
                }
                """;

            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(noAuthorizationResponseJson));

            try (var service = createElasticInferenceService()) {
                ensureAuthorizationCallFinished(service);

                assertThat(service.supportedStreamingTasks(), is(EnumSet.noneOf(TaskType.class)));
                assertTrue(service.defaultConfigIds().isEmpty());
                assertThat(service.supportedTaskTypes(), is(EnumSet.noneOf(TaskType.class)));

                var getModelListener = new PlainActionFuture<UnparsedModel>();
                modelRegistry.getModel(".rainbow-sprinkles-elastic", getModelListener);

                var exception = expectThrows(ResourceNotFoundException.class, () -> getModelListener.actionGet(TIMEOUT));
                assertThat(exception.getMessage(), is("Inference endpoint not found [.rainbow-sprinkles-elastic]"));
            }
        }
    }

    public void testRemoves_DefaultChatCompletion_V1_WhenAuthorizationDoesNotReturnAuthForIt() throws Exception {
        {
            String responseJson = """
                {
                    "models": [
                        {
                          "model_name": "rainbow-sprinkles",
                          "task_types": ["chat"]
                        },
                        {
                          "model_name": "elser-v2",
                          "task_types": ["embed/text/sparse"]
                        }
                    ]
                }
                """;

            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            try (var service = createElasticInferenceService()) {
                ensureAuthorizationCallFinished(service);

                assertThat(service.supportedStreamingTasks(), is(EnumSet.of(TaskType.CHAT_COMPLETION)));
                assertThat(
                    service.defaultConfigIds(),
                    is(
                        List.of(
                            new InferenceService.DefaultConfigId(
                                ".elser-v2-elastic",
                                MinimalServiceSettings.sparseEmbedding(ElasticInferenceService.NAME),
                                service
                            ),
                            new InferenceService.DefaultConfigId(
                                ".rainbow-sprinkles-elastic",
                                MinimalServiceSettings.chatCompletion(ElasticInferenceService.NAME),
                                service
                            )
                        )
                    )
                );
                assertThat(service.supportedTaskTypes(), is(EnumSet.of(TaskType.CHAT_COMPLETION, TaskType.SPARSE_EMBEDDING)));

                PlainActionFuture<List<Model>> listener = new PlainActionFuture<>();
                service.defaultConfigs(listener);
                assertThat(listener.actionGet(TIMEOUT).get(0).getConfigurations().getInferenceEntityId(), is(".elser-v2-elastic"));
                assertThat(listener.actionGet(TIMEOUT).get(1).getConfigurations().getInferenceEntityId(), is(".rainbow-sprinkles-elastic"));

                var getModelListener = new PlainActionFuture<UnparsedModel>();
                // persists the default endpoints
                modelRegistry.getModel(".rainbow-sprinkles-elastic", getModelListener);

                var inferenceEntity = getModelListener.actionGet(TIMEOUT);
                assertThat(inferenceEntity.inferenceEntityId(), is(".rainbow-sprinkles-elastic"));
                assertThat(inferenceEntity.taskType(), is(TaskType.CHAT_COMPLETION));
            }
        }
        {
            String noAuthorizationResponseJson = """
                {
                    "models": [
                        {
                          "model_name": "elser-v2",
                          "task_types": ["embed/text/sparse"]
                        }
                    ]
                }
                """;

            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(noAuthorizationResponseJson));

            try (var service = createElasticInferenceService()) {
                ensureAuthorizationCallFinished(service);

                assertThat(service.supportedStreamingTasks(), is(EnumSet.noneOf(TaskType.class)));
                assertThat(
                    service.defaultConfigIds(),
                    is(
                        List.of(
                            new InferenceService.DefaultConfigId(
                                ".elser-v2-elastic",
                                MinimalServiceSettings.sparseEmbedding(ElasticInferenceService.NAME),
                                service
                            )
                        )
                    )
                );
                assertThat(service.supportedTaskTypes(), is(EnumSet.of(TaskType.SPARSE_EMBEDDING)));

                var getModelListener = new PlainActionFuture<UnparsedModel>();
                modelRegistry.getModel(".rainbow-sprinkles-elastic", getModelListener);
                var exception = expectThrows(ResourceNotFoundException.class, () -> getModelListener.actionGet(TIMEOUT));
                assertThat(exception.getMessage(), is("Inference endpoint not found [.rainbow-sprinkles-elastic]"));
            }
        }
    }

    private void ensureAuthorizationCallFinished(ElasticInferenceService service) {
        service.onNodeStarted();
        service.waitForFirstAuthorizationToComplete(TIMEOUT);
    }

    private ElasticInferenceService createElasticInferenceService() {
        var httpManager = HttpClientManager.create(Settings.EMPTY, threadPool, mockClusterServiceEmpty(), mock(ThrottlerManager.class));
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, httpManager);

        return new ElasticInferenceService(
            senderFactory,
            createWithEmptySettings(threadPool),
            ElasticInferenceServiceSettingsTests.create(gatewayUrl),
            modelRegistry,
            new ElasticInferenceServiceAuthorizationRequestHandler(gatewayUrl, threadPool)
        );
    }
}
