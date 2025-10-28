/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.integration;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnparsedModel;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.LocalStateInferencePlugin;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceService;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceSettings;
import org.elasticsearch.xpack.inference.services.elastic.InternalPreconfiguredEndpoints;
import org.elasticsearch.xpack.inference.services.elastic.authorization.AuthorizationTaskExecutor;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityExecutors;
import static org.elasticsearch.xpack.inference.external.http.Utils.getUrl;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

public class AuthorizationTaskExecutorIT extends ESSingleNodeTestCase {

    private static final String EMPTY_AUTH_RESPONSE = """
        {
            "models": [
            ]
        }
        """;

    private static final String AUTHORIZED_RAINBOW_SPRINKLES_RESPONSE = """
        {
            "models": [
                {
                  "model_name": "rainbow-sprinkles",
                  "task_types": ["chat"]
                }
            ]
        }
        """;

    private static final MockWebServer webServer = new MockWebServer();
    private static String gatewayUrl;

    private ModelRegistry modelRegistry;
    private ThreadPool threadPool;
    private AuthorizationTaskExecutor authorizationTaskExecutor;

    @BeforeClass
    public static void initClass() throws IOException {
        webServer.start();
        gatewayUrl = getUrl(webServer);
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody(EMPTY_AUTH_RESPONSE));
    }

    @Before
    public void createComponents() {
        threadPool = createThreadPool(inferenceUtilityExecutors());
        modelRegistry = node().injector().getInstance(ModelRegistry.class);
        authorizationTaskExecutor = node().injector().getInstance(AuthorizationTaskExecutor.class);
    }

    @After
    public void shutdown() {
        // Delete all the eis preconfigured endpoints
        var listener = new PlainActionFuture<Boolean>();
        modelRegistry.deleteModels(InternalPreconfiguredEndpoints.EIS_PRECONFIGURED_ENDPOINT_IDS, listener);
        listener.actionGet(TimeValue.THIRTY_SECONDS);

        terminate(threadPool);
    }

    @AfterClass
    public static void cleanUpClass() {
        webServer.close();
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder()
            .put(ElasticInferenceServiceSettings.ELASTIC_INFERENCE_SERVICE_URL.getKey(), gatewayUrl)
            .put(ElasticInferenceServiceSettings.PERIODIC_AUTHORIZATION_ENABLED.getKey(), false)
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(ReindexPlugin.class, LocalStateInferencePlugin.class);
    }

    public void testCreatesEisChatCompletionEndpoint() throws Exception {
        assertNoAuthorizedEisEndpoints();

        webServer.enqueue(new MockResponse().setResponseCode(200).setBody(AUTHORIZED_RAINBOW_SPRINKLES_RESPONSE));
        waitForNewAuthorizationResponse();

        assertChatCompletionEndpointExists();
    }

    private void assertNoAuthorizedEisEndpoints() throws Exception {
        assertBusy(() -> {
            var newPoller = authorizationTaskExecutor.getCurrentPollerTask();
            assertNotNull(newPoller);
            newPoller.waitForAuthorizationToComplete(TimeValue.THIRTY_SECONDS);
        });

        var eisEndpoints = getEisEndpoints();
        assertThat(eisEndpoints, empty());
    }

    private List<UnparsedModel> getEisEndpoints() {
        var listener = new PlainActionFuture<List<UnparsedModel>>();
        modelRegistry.getAllModels(false, listener);

        var endpoints = listener.actionGet(TimeValue.THIRTY_SECONDS);
        return endpoints.stream().filter(m -> m.service().equals(ElasticInferenceService.NAME)).toList();
    }

    private void waitForNewAuthorizationResponse() throws Exception {
        var taskListener = new PlainActionFuture<Void>();

        authorizationTaskExecutor.abortTask(TimeValue.THIRTY_SECONDS, taskListener);
        // Ensure that the listener doesn't return a failure
        assertNull(taskListener.actionGet(TimeValue.THIRTY_SECONDS));

        // wait for the new task to be recreated
        assertBusy(() -> {
            var newPoller = authorizationTaskExecutor.getCurrentPollerTask();
            assertNotNull(newPoller);
            newPoller.waitForAuthorizationToComplete(TimeValue.THIRTY_SECONDS);
        });
    }

    public void testCreatesEisChatCompletion_DoesNotRemoveEndpointWhenNoLongerAuthorized() throws Exception {
        assertNoAuthorizedEisEndpoints();

        webServer.enqueue(new MockResponse().setResponseCode(200).setBody(AUTHORIZED_RAINBOW_SPRINKLES_RESPONSE));
        waitForNewAuthorizationResponse();

        assertChatCompletionEndpointExists();

        // Simulate that the model is no longer authorized
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody(EMPTY_AUTH_RESPONSE));
        waitForNewAuthorizationResponse();

        assertChatCompletionEndpointExists();
    }

    private void assertChatCompletionEndpointExists() {
        var eisEndpoints = getEisEndpoints();
        assertThat(eisEndpoints.size(), is(1));

        var rainbowSprinklesModel = eisEndpoints.get(0);
        assertChatCompletionUnparsedModel(rainbowSprinklesModel);
    }

    private void assertChatCompletionUnparsedModel(UnparsedModel rainbowSprinklesModel) {
        assertThat(rainbowSprinklesModel.taskType(), is(TaskType.CHAT_COMPLETION));
        assertThat(rainbowSprinklesModel.service(), is(ElasticInferenceService.NAME));
        assertThat(rainbowSprinklesModel.inferenceEntityId(), is(InternalPreconfiguredEndpoints.DEFAULT_CHAT_COMPLETION_ENDPOINT_ID_V1));
    }

    public void testCreatesChatCompletion_AndThenCreatesTextEmbedding() throws Exception {
        assertNoAuthorizedEisEndpoints();

        webServer.enqueue(new MockResponse().setResponseCode(200).setBody(AUTHORIZED_RAINBOW_SPRINKLES_RESPONSE));
        waitForNewAuthorizationResponse();

        assertChatCompletionEndpointExists();

        // Simulate that the model is no longer authorized
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody(EMPTY_AUTH_RESPONSE));
        waitForNewAuthorizationResponse();

        assertChatCompletionEndpointExists();

        // Simulate that a text embedding model is now authorized
        var authorizedTextEmbeddingResponse = """
            {
                "models": [
                    {
                      "model_name": "multilingual-embed-v1",
                      "task_types": ["embed/text/dense"]
                    }
                ]
            }
            """;

        webServer.enqueue(new MockResponse().setResponseCode(200).setBody(authorizedTextEmbeddingResponse));
        waitForNewAuthorizationResponse();

        var eisEndpoints = getEisEndpoints().stream().collect(Collectors.toMap(UnparsedModel::inferenceEntityId, Function.identity()));
        assertThat(eisEndpoints.size(), is(2));

        assertTrue(eisEndpoints.containsKey(InternalPreconfiguredEndpoints.DEFAULT_CHAT_COMPLETION_ENDPOINT_ID_V1));
        assertChatCompletionUnparsedModel(eisEndpoints.get(InternalPreconfiguredEndpoints.DEFAULT_CHAT_COMPLETION_ENDPOINT_ID_V1));

        assertTrue(eisEndpoints.containsKey(InternalPreconfiguredEndpoints.DEFAULT_MULTILINGUAL_EMBED_ENDPOINT_ID));

        var textEmbeddingEndpoint = eisEndpoints.get(InternalPreconfiguredEndpoints.DEFAULT_MULTILINGUAL_EMBED_ENDPOINT_ID);
        assertThat(textEmbeddingEndpoint.taskType(), is(TaskType.TEXT_EMBEDDING));
        assertThat(textEmbeddingEndpoint.service(), is(ElasticInferenceService.NAME));
    }

    public void testRestartsTaskAfterAbort() throws Exception {
        // Ensure the task is created and we get an initial authorization response
        assertNoAuthorizedEisEndpoints();

        webServer.enqueue(new MockResponse().setResponseCode(200).setBody(EMPTY_AUTH_RESPONSE));
        // Abort the task and ensure it is restarted
        waitForNewAuthorizationResponse();
    }
}
