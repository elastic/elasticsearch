/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.integration;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.license.LicenseSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.xpack.core.inference.action.GetInferenceModelAction;
import org.elasticsearch.xpack.inference.LocalStateInferencePlugin;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceService;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceSettings;
import org.elasticsearch.xpack.inference.services.elastic.InternalPreconfiguredEndpoints;
import org.elasticsearch.xpack.inference.services.elastic.authorization.AuthorizationPoller;
import org.elasticsearch.xpack.inference.services.elastic.ccm.CCMSettings;
import org.junit.AfterClass;
import org.junit.Before;
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
import static org.elasticsearch.xpack.inference.integration.AuthorizationTaskExecutorIT.cancelAuthorizationTask;
import static org.elasticsearch.xpack.inference.integration.AuthorizationTaskExecutorIT.waitForTask;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.not;

/**
 * These tests handle testing task relocation and cancellation.
 * If the task is running on a node that is shutdown, it should be relocated to another node.
 * If the task is cancelled it should be restarted automatically.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class AuthorizationTaskExecutorMultipleNodesIT extends ESIntegTestCase {

    private static final int NUM_DATA_NODES = 2;
    private static final int NUM_MASTER_NODES = 2;
    private static final String AUTH_TASK_ACTION = AuthorizationPoller.TASK_NAME + "[c]";
    private static final MockWebServer webServer = new MockWebServer();
    private static String gatewayUrl;

    @BeforeClass
    public static void initClass() throws IOException {
        webServer.start();
        gatewayUrl = getUrl(webServer);
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody(EMPTY_AUTH_RESPONSE));
    }

    @Before
    public void startNodes() {
        // Ensure we have multiple master and data nodes so we have somewhere to place the inference indices and so that we can safely
        // shut down the node that is running the authorization task. If there is only one master and it is running the task,
        // we'll get an error that we can't shut down the only eligible master node
        internalCluster().startMasterOnlyNodes(NUM_MASTER_NODES);
        internalCluster().ensureAtLeastNumDataNodes(NUM_DATA_NODES);
        ensureStableCluster(NUM_MASTER_NODES + NUM_DATA_NODES);
    }

    @AfterClass
    public static void cleanUpClass() {
        webServer.close();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(LocalStateInferencePlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            // Disable CCM to ensure that only the authorization task executor is initialized in the inference plugin when it is created
            .put(CCMSettings.CCM_SUPPORTED_ENVIRONMENT.getKey(), false)
            .put(LicenseSettings.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial")
            .put(ElasticInferenceServiceSettings.ELASTIC_INFERENCE_SERVICE_URL.getKey(), gatewayUrl)
            .put(ElasticInferenceServiceSettings.PERIODIC_AUTHORIZATION_ENABLED.getKey(), false)
            .build();
    }

    public void testCancellingAuthorizationTaskRestartsIt() throws Exception {
        cancelAuthorizationTask(admin());
    }

    public void testAuthorizationTaskGetsRelocatedToAnotherNode_WhenTheNodeThatIsRunningItShutsDown() throws Exception {
        var nodeNameMapping = getNodeNames(internalCluster().getNodeNames());

        var pollerTask = waitForTask(AUTH_TASK_ACTION, admin());

        var endpoints = getAllEndpoints();
        assertTrue(
            "expected no authorized EIS endpoints",
            endpoints.getEndpoints().stream().noneMatch(endpoint -> endpoint.getService().equals(ElasticInferenceService.NAME))
        );

        // queue a response that authorizes one model
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody(AUTHORIZED_RAINBOW_SPRINKLES_RESPONSE));

        assertTrue("expected the node to shutdown properly", internalCluster().stopNode(nodeNameMapping.get(pollerTask.node())));

        assertBusy(() -> {
            var relocatedPollerTask = waitForTask(AUTH_TASK_ACTION, admin());
            assertThat(relocatedPollerTask.node(), not(is(pollerTask.node())));
        });

        assertBusy(() -> {
            var allEndpoints = getAllEndpoints();

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

    private GetInferenceModelAction.Response getAllEndpoints() throws Exception {
        var getAllEndpointsRequest = new GetInferenceModelAction.Request("*", TaskType.ANY, true);

        var allEndpointsRef = new AtomicReference<GetInferenceModelAction.Response>();
        assertBusy(() -> {
            try {
                allEndpointsRef.set(
                    internalCluster().masterClient().execute(GetInferenceModelAction.INSTANCE, getAllEndpointsRequest).actionGet()
                );
            } catch (Exception e) {
                // We probably got an all shards failed exception because the indices aren't ready yet, we'll just try again
                logger.warn("Failed to retrieve endpoints", e);
                fail("Failed to retrieve endpoints");
            }
        });

        return allEndpointsRef.get();
    }
}
