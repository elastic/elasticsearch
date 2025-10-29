/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.integration;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.license.LicenseSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.xpack.core.inference.action.GetInferenceModelAction;
import org.elasticsearch.xpack.inference.LocalStateInferencePlugin;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceService;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceSettings;
import org.elasticsearch.xpack.inference.services.elastic.InternalPreconfiguredEndpoints;
import org.elasticsearch.xpack.inference.services.elastic.authorization.AuthorizationPoller;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.inference.external.http.Utils.getUrl;
import static org.elasticsearch.xpack.inference.integration.AuthorizationTaskExecutorIT.AUTHORIZED_RAINBOW_SPRINKLES_RESPONSE;
import static org.elasticsearch.xpack.inference.integration.AuthorizationTaskExecutorIT.EMPTY_AUTH_RESPONSE;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.not;

/**
 * These tests ensure that when a node is shutdown that is running an AuthorizationTaskExecutor,
 * the task is properly relocated to another node.
 */
@ESTestCase.WithoutEntitlements // due to dependency issue ES-12435
public class AuthorizationTaskExecutorRelocationIT extends ESIntegTestCase {

    private static final String AUTH_TASK_ACTION = AuthorizationPoller.TASK_NAME + "[c]";
    private static final MockWebServer webServer = new MockWebServer();
    private static String gatewayUrl;

    @BeforeClass
    public static void initClass() throws IOException {
        webServer.start();
        gatewayUrl = getUrl(webServer);
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody(EMPTY_AUTH_RESPONSE));
    }

    @AfterClass
    public static void cleanUpClass() {
        webServer.close();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(ReindexPlugin.class, LocalStateInferencePlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(LicenseSettings.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial")
            .put(ElasticInferenceServiceSettings.ELASTIC_INFERENCE_SERVICE_URL.getKey(), gatewayUrl)
            .put(ElasticInferenceServiceSettings.PERIODIC_AUTHORIZATION_ENABLED.getKey(), false)
            .build();
    }

    @Override
    public Settings indexSettings() {
        return Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, randomIntBetween(3, 10)).build();
    }

    public void testAuthorizationTaskGetsRelocatedToAnotherNode_WhenTheNodeThatIsRunningItShutsDown() throws Exception {
        // Ensure we have multiple master and data nodes so we have somewhere to place the inference indices and so that we can safely
        // shut down the node that is running the authorization task. If there is only one master we'll get an error that we can't shut
        // down the only eligible master node
        internalCluster().startMasterOnlyNodes(2);
        internalCluster().ensureAtLeastNumDataNodes(2);
        awaitMasterNode();

        var nodeNameMapping = getNodeNames(internalCluster().getNodeNames());

        var pollerTask = waitForTask(internalCluster().getNodeNames(), AUTH_TASK_ACTION);

        var getAllEndpointsRequest = new GetInferenceModelAction.Request("*", TaskType.ANY, true);
        var endpoints = client().execute(GetInferenceModelAction.INSTANCE, getAllEndpointsRequest).actionGet();
        assertTrue(
            "expected no authorized EIS endpoints",
            endpoints.getEndpoints().stream().noneMatch(endpoint -> endpoint.getService().equals(ElasticInferenceService.NAME))
        );

        // queue a response that authorizes one model
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody(AUTHORIZED_RAINBOW_SPRINKLES_RESPONSE));

        assertTrue(internalCluster().stopNode(nodeNameMapping.get(pollerTask.node())));
        awaitMasterNode();

        assertBusy(() -> {
            var relocatedPollerTask = waitForTask(internalCluster().getNodeNames(), AUTH_TASK_ACTION);
            assertThat(relocatedPollerTask.node(), not(is(pollerTask.node())));
        });

        assertBusy(() -> {
            var allEndpoints = client().execute(GetInferenceModelAction.INSTANCE, getAllEndpointsRequest).actionGet();
            var eisEndpoints = allEndpoints.getEndpoints()
                .stream()
                .filter(endpoint -> endpoint.getService().equals(ElasticInferenceService.NAME))
                .toList();
            assertThat(eisEndpoints.size(), is(1));

            var rainbowSprinklesEndpoint = eisEndpoints.get(0);
            assertThat(rainbowSprinklesEndpoint.getService(), is(ElasticInferenceService.NAME));
            assertThat(
                rainbowSprinklesEndpoint.getInferenceEntityId(),
                is(InternalPreconfiguredEndpoints.DEFAULT_CHAT_COMPLETION_ENDPOINT_ID_V1)
            );
            assertThat(rainbowSprinklesEndpoint.getTaskType(), is(TaskType.CHAT_COMPLETION));
        });
    }

    private TaskInfo waitForTask(String[] nodes, String taskAction) throws Exception {
        var taskRef = new AtomicReference<TaskInfo>();
        assertBusy(() -> {
            var response = admin().cluster().prepareListTasks(nodes).get();
            var authPollerTask = response.getTasks().stream().filter(task -> task.action().equals(taskAction)).findFirst();
            assertTrue(authPollerTask.isPresent());
            taskRef.set(authPollerTask.get());
        });

        return taskRef.get();
    }

    private record NodeNameMapping(Map<String, String> nodeNamesMap) {
        public String get(String rawNodeName) {
            var nodeName = nodeNamesMap.get(rawNodeName);
            if (nodeName == null) {
                throw new IllegalArgumentException("No node name found for raw node name: " + rawNodeName);
            }

            return nodeName;
        }
    }

    /**
     * The node names created by the integration test framework take the form of "node_#", but the task api gives a raw node name
     * like 02PT2SBzRxC3cG-9mKCigQ, so we need to map between them to be able to act on a node that the task is currently running on.
     */
    private static NodeNameMapping getNodeNames(String[] nodes) {
        var nodeNamesMap = new HashMap<String, String>();
        for (var node : nodes) {
            var nodeTasks = admin().cluster().prepareListTasks(node).get();
            assertThat(nodeTasks.getTasks().size(), greaterThanOrEqualTo(1));
            nodeNamesMap.put(nodeTasks.getTasks().getFirst().node(), node);
        }

        return new NodeNameMapping(nodeNamesMap);
    }

}
