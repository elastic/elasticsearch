/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.integration;

import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequestBuilder;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksRequestBuilder;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.AdminClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnparsedModel;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.xpack.inference.LocalStateInferencePlugin;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceService;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceSettings;
import org.elasticsearch.xpack.inference.services.elastic.authorization.AuthorizationPoller;
import org.elasticsearch.xpack.inference.services.elastic.authorization.AuthorizationTaskExecutor;
import org.elasticsearch.xpack.inference.services.elastic.ccm.CCMSettings;
import org.elasticsearch.xpack.inference.services.elastic.response.ElasticInferenceServiceAuthorizationResponseEntityTests;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.inference.external.http.Utils.getUrl;
import static org.elasticsearch.xpack.inference.services.elastic.response.ElasticInferenceServiceAuthorizationResponseEntityTests.EIS_EMPTY_RESPONSE;
import static org.elasticsearch.xpack.inference.services.elastic.response.ElasticInferenceServiceAuthorizationResponseEntityTests.ELSER_V2_ENDPOINT_ID;
import static org.elasticsearch.xpack.inference.services.elastic.response.ElasticInferenceServiceAuthorizationResponseEntityTests.JINA_EMBED_V3_ENDPOINT_ID;
import static org.elasticsearch.xpack.inference.services.elastic.response.ElasticInferenceServiceAuthorizationResponseEntityTests.RAINBOW_SPRINKLES_ENDPOINT_ID;
import static org.elasticsearch.xpack.inference.services.elastic.response.ElasticInferenceServiceAuthorizationResponseEntityTests.RERANK_V1_ENDPOINT_ID;
import static org.elasticsearch.xpack.inference.services.elastic.response.ElasticInferenceServiceAuthorizationResponseEntityTests.getEisRainbowSprinklesAuthorizationResponse;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class AuthorizationTaskExecutorIT extends ESSingleNodeTestCase {

    public static final Set<String> EIS_PRECONFIGURED_ENDPOINT_IDS = Set.of(
        RAINBOW_SPRINKLES_ENDPOINT_ID,
        ELSER_V2_ENDPOINT_ID,
        JINA_EMBED_V3_ENDPOINT_ID,
        RERANK_V1_ENDPOINT_ID
    );

    public static final String AUTH_TASK_ACTION = AuthorizationPoller.TASK_NAME + "[c]";

    private static final MockWebServer webServer = new MockWebServer();
    private static String gatewayUrl;
    private static String chatCompletionResponseBody;

    private ModelRegistry modelRegistry;
    private AuthorizationTaskExecutor authorizationTaskExecutor;

    @BeforeClass
    public static void initClass() throws IOException {
        webServer.start();
        gatewayUrl = getUrl(webServer);
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody(EIS_EMPTY_RESPONSE));
        chatCompletionResponseBody = getEisRainbowSprinklesAuthorizationResponse(gatewayUrl).responseJson();
    }

    @Before
    public void createComponents() {
        modelRegistry = node().injector().getInstance(ModelRegistry.class);
        authorizationTaskExecutor = node().injector().getInstance(AuthorizationTaskExecutor.class);
    }

    @After
    public void shutdown() {
        removeEisPreconfiguredEndpoints(modelRegistry);
    }

    static void removeEisPreconfiguredEndpoints(ModelRegistry modelRegistry) {
        // Delete all the eis preconfigured endpoints
        var listener = new PlainActionFuture<Boolean>();
        modelRegistry.deleteModels(EIS_PRECONFIGURED_ENDPOINT_IDS, listener);
        listener.actionGet(TimeValue.THIRTY_SECONDS);
    }

    @AfterClass
    public static void cleanUpClass() {
        webServer.close();
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder()
            // Disable CCM to ensure that we don't rely on a CCM configuration existing
            .put(CCMSettings.CCM_SUPPORTED_ENVIRONMENT.getKey(), false)
            .put(ElasticInferenceServiceSettings.ELASTIC_INFERENCE_SERVICE_URL.getKey(), gatewayUrl)
            // Ensure that the polling logic only occurs once so we can deterministically control when an authorization response is
            // received
            .put(ElasticInferenceServiceSettings.PERIODIC_AUTHORIZATION_ENABLED.getKey(), false)
            // Use very short intervals for testing purposes so that waiting for the task to be recreated is fast
            .put(ElasticInferenceServiceSettings.AUTHORIZATION_REQUEST_INTERVAL.getKey(), TimeValue.timeValueMillis(1))
            .put(ElasticInferenceServiceSettings.MAX_AUTHORIZATION_REQUEST_JITTER.getKey(), TimeValue.timeValueMillis(1))
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(ReindexPlugin.class, LocalStateInferencePlugin.class);
    }

    @Override
    protected boolean resetNodeAfterTest() {
        return true;
    }

    public void testCreatesEisChatCompletionEndpoint() throws Exception {
        assertNoAuthorizedEisEndpoints();

        webServer.clearRequests();
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody(chatCompletionResponseBody));
        restartPollingTaskAndWaitForAuthResponse();

        assertWebServerReceivedRequest();

        assertChatCompletionEndpointExists();
    }

    private void assertNoAuthorizedEisEndpoints() throws Exception {
        assertNoAuthorizedEisEndpoints(admin(), authorizationTaskExecutor, modelRegistry);
    }

    static void assertNoAuthorizedEisEndpoints(
        AdminClient adminClient,
        AuthorizationTaskExecutor authorizationTaskExecutor,
        ModelRegistry modelRegistry
    ) throws Exception {
        waitForTask(AUTH_TASK_ACTION, adminClient);

        assertBusy(() -> {
            var newPoller = authorizationTaskExecutor.getCurrentPollerTask();
            assertNotNull(newPoller);
            newPoller.waitForAuthorizationToComplete(TimeValue.THIRTY_SECONDS);
        });

        var eisEndpoints = getEisEndpoints(modelRegistry);
        assertThat(eisEndpoints, empty());

        for (String eisPreconfiguredEndpoints : EIS_PRECONFIGURED_ENDPOINT_IDS) {
            assertFalse(modelRegistry.containsPreconfiguredInferenceEndpointId(eisPreconfiguredEndpoints));
        }
    }

    public static TaskInfo waitForTask(String taskAction, AdminClient adminClient) throws Exception {
        var taskRef = new AtomicReference<TaskInfo>();
        var builder = new ListTasksRequestBuilder(adminClient.cluster());

        assertBusy(() -> {
            var response = builder.get();
            var authPollerTask = response.getTasks().stream().filter(task -> task.action().equals(taskAction)).findFirst();
            assertTrue(authPollerTask.isPresent());
            taskRef.set(authPollerTask.get());
        });

        return taskRef.get();
    }

    static void waitForNoTask(String taskAction, AdminClient adminClient) throws Exception {
        var builder = new ListTasksRequestBuilder(adminClient.cluster());

        assertBusy(() -> {
            var response = builder.get();
            var authPollerTask = response.getTasks().stream().filter(task -> task.action().equals(taskAction)).findFirst();
            assertFalse(authPollerTask.isPresent());
        });

    }

    private List<UnparsedModel> getEisEndpoints() {
        return getEisEndpoints(modelRegistry);
    }

    static List<UnparsedModel> getEisEndpoints(ModelRegistry modelRegistry) {
        var listener = new PlainActionFuture<List<UnparsedModel>>();
        modelRegistry.getAllModels(false, listener);

        var endpoints = listener.actionGet(TimeValue.THIRTY_SECONDS);
        return endpoints.stream().filter(m -> m.service().equals(ElasticInferenceService.NAME)).toList();
    }

    private void restartPollingTaskAndWaitForAuthResponse() throws Exception {
        restartPollingTaskAndWaitForAuthResponse(admin(), authorizationTaskExecutor);
    }

    private static void restartPollingTaskAndWaitForAuthResponse(AdminClient adminClient, AuthorizationTaskExecutor authTaskExecutor)
        throws Exception {
        cancelAuthorizationTask(adminClient);

        // wait for the new task to be recreated and an authorization response to be processed
        waitForAuthorizationToComplete(authTaskExecutor);
    }

    private static void assertWebServerReceivedRequest() throws Exception {
        assertBusy(() -> {
            var requests = webServer.requests();
            assertThat(requests.size(), is(1));
        });
    }

    static void waitForAuthorizationToComplete(AuthorizationTaskExecutor authTaskExecutor) throws Exception {
        assertBusy(() -> {
            var newPoller = authTaskExecutor.getCurrentPollerTask();
            assertNotNull(newPoller);
            newPoller.waitForAuthorizationToComplete(TimeValue.THIRTY_SECONDS);
        });
    }

    static void cancelAuthorizationTask(AdminClient adminClient) throws Exception {
        var pollerTask = waitForTask(AUTH_TASK_ACTION, adminClient);
        var builder = new CancelTasksRequestBuilder(adminClient.cluster());

        assertBusy(() -> {
            var cancelTaskResponse = builder.setActions(AUTH_TASK_ACTION).get();
            assertThat(cancelTaskResponse.getTasks().size(), is(1));
            assertThat(cancelTaskResponse.getTasks().get(0).action(), is(AUTH_TASK_ACTION));
        });

        var newPollerTask = waitForTask(AUTH_TASK_ACTION, adminClient);
        assertThat(newPollerTask.taskId(), is(not(pollerTask.taskId())));
    }

    public void testCreatesEisChatCompletion_DoesNotRemoveEndpointWhenNoLongerAuthorized() throws Exception {
        assertNoAuthorizedEisEndpoints();

        webServer.clearRequests();
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody(chatCompletionResponseBody));
        restartPollingTaskAndWaitForAuthResponse();
        assertWebServerReceivedRequest();

        assertChatCompletionEndpointExists();

        webServer.clearRequests();
        // Simulate that the model is no longer authorized
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody(EIS_EMPTY_RESPONSE));
        restartPollingTaskAndWaitForAuthResponse();
        assertWebServerReceivedRequest();

        assertChatCompletionEndpointExists();
    }

    private void assertChatCompletionEndpointExists() throws Exception {
        assertChatCompletionEndpointExists(modelRegistry);
    }

    static void assertChatCompletionEndpointExists(ModelRegistry modelRegistry) throws Exception {
        assertBusy(() -> {
            var eisEndpoints = getEisEndpoints(modelRegistry);
            assertThat(eisEndpoints.size(), is(1));

            var rainbowSprinklesModel = eisEndpoints.get(0);
            assertChatCompletionUnparsedModel(rainbowSprinklesModel);
            assertTrue(modelRegistry.containsPreconfiguredInferenceEndpointId(RAINBOW_SPRINKLES_ENDPOINT_ID));
        });
    }

    static void assertChatCompletionUnparsedModel(UnparsedModel rainbowSprinklesModel) {
        assertThat(rainbowSprinklesModel.taskType(), is(TaskType.CHAT_COMPLETION));
        assertThat(rainbowSprinklesModel.service(), is(ElasticInferenceService.NAME));
        assertThat(rainbowSprinklesModel.inferenceEntityId(), is(RAINBOW_SPRINKLES_ENDPOINT_ID));
    }

    public void testCreatesChatCompletion_AndThenCreatesTextEmbedding() throws Exception {
        assertNoAuthorizedEisEndpoints();

        webServer.clearRequests();
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody(chatCompletionResponseBody));
        restartPollingTaskAndWaitForAuthResponse();
        assertWebServerReceivedRequest();

        assertChatCompletionEndpointExists();

        // Simulate that the model is no longer authorized
        webServer.clearRequests();
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody(EIS_EMPTY_RESPONSE));
        restartPollingTaskAndWaitForAuthResponse();
        assertWebServerReceivedRequest();

        assertChatCompletionEndpointExists();

        webServer.clearRequests();
        // Simulate that a text embedding model is now authorized
        var jinaEmbedResponseBody = ElasticInferenceServiceAuthorizationResponseEntityTests.getEisJinaEmbedAuthorizationResponse(gatewayUrl)
            .responseJson();
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody(jinaEmbedResponseBody));

        restartPollingTaskAndWaitForAuthResponse();
        assertWebServerReceivedRequest();

        var eisEndpoints = getEisEndpoints().stream().collect(Collectors.toMap(UnparsedModel::inferenceEntityId, Function.identity()));
        assertThat(eisEndpoints.size(), is(2));

        assertTrue(eisEndpoints.containsKey(RAINBOW_SPRINKLES_ENDPOINT_ID));
        assertChatCompletionUnparsedModel(eisEndpoints.get(RAINBOW_SPRINKLES_ENDPOINT_ID));

        assertTrue(eisEndpoints.containsKey(JINA_EMBED_V3_ENDPOINT_ID));

        var textEmbeddingEndpoint = eisEndpoints.get(JINA_EMBED_V3_ENDPOINT_ID);
        assertThat(textEmbeddingEndpoint.taskType(), is(TaskType.TEXT_EMBEDDING));
        assertThat(textEmbeddingEndpoint.service(), is(ElasticInferenceService.NAME));
    }

    public void testRestartsTaskAfterAbort() throws Exception {
        // Ensure the task is created and we get an initial authorization response
        assertNoAuthorizedEisEndpoints();

        webServer.clearRequests();
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody(EIS_EMPTY_RESPONSE));
        // Abort the task and ensure it is restarted
        restartPollingTaskAndWaitForAuthResponse();
        assertWebServerReceivedRequest();
    }
}
