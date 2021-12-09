/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsAction;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.monitor.os.OsStats;
import org.elasticsearch.node.NodeRoleSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.xpack.autoscaling.action.GetAutoscalingCapacityAction;
import org.elasticsearch.xpack.autoscaling.action.PutAutoscalingPolicyAction;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderResults;
import org.elasticsearch.xpack.autoscaling.capacity.memory.AutoscalingMemoryInfoService;
import org.elasticsearch.xpack.autoscaling.master.DedicatedMasterNodesDeciderService;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

@ESIntegTestCase.ClusterScope(autoManageMasterNodes = false, numDataNodes = 0)
public class DedicatedMasterNodesDeciderIT extends ESIntegTestCase {
    private static final NodeStatsTestClient nodeStatsTestClient = new NodeStatsTestClient("DedicatedMasterNodesDeciderIT");

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), DedicatedMasterAutoscalingTestPlugin.class);
    }

    @AfterClass
    public static void tearDownClient() {
        nodeStatsTestClient.close();
    }

    @Before
    public void setUpClient() {
        nodeStatsTestClient.unsetFailingClient();
    }

    @After
    public void ensureClientFails() {
        // When we tear down the cluster, the master moves to another node
        // and tries to request node stats. Ensure that these request do not
        // trip assertions.
        nodeStatsTestClient.setFailingClient();
    }

    public static class DedicatedMasterAutoscalingTestPlugin extends LocalStateCompositeXPackPlugin {

        public DedicatedMasterAutoscalingTestPlugin(final Settings settings) {
            super(settings, null);
            plugins.add(new Autoscaling(new AutoscalingLicenseChecker(() -> true)) {
                @Override
                protected AutoscalingMemoryInfoService createAutoscalingMemoryInfoService(ClusterService clusterService, Client client) {
                    return new AutoscalingMemoryInfoService(clusterService, nodeStatsTestClient);
                }
            });
        }
    }

    public void testScaleUp() throws Exception {
        boolean largeHotNodes = randomBoolean();
        ByteSizeValue minDataNodeMemory = ByteSizeValue.ofGb(randomIntBetween(1, 64));

        ByteSizeValue hotNodeMemory = largeHotNodes ? ByteSizeValue.ofBytes(minDataNodeMemory.getBytes() * 2) : minDataNodeMemory;
        final ActionFuture<Void> nodeStatsRequested = nodeStatsTestClient.respond(
            (req, listener) -> respondWithFakeNodeMemory(req, hotNodeMemory, listener),
            3
        );

        internalCluster().setBootstrapMasterNodeIndex(0);
        startNodes(3, DiscoveryNodeRole.DATA_HOT_NODE_ROLE, DiscoveryNodeRole.DATA_CONTENT_NODE_ROLE, DiscoveryNodeRole.MASTER_ROLE);
        ensureStableCluster(3);

        putAutoscalingPolicy(
            Settings.builder().put(DedicatedMasterNodesDeciderService.MIN_HOT_NODE_MEMORY.getKey(), minDataNodeMemory).build()
        );
        nodeStatsRequested.get();

        if (largeHotNodes) {
            assertAutoscalingDeciderResults(deciderResults -> {
                assertThat(deciderResults.requiredCapacity().node().memory(), equalTo(ByteSizeValue.ofGb(1)));
                assertThat(deciderResults.requiredCapacity().total().memory(), equalTo(ByteSizeValue.ofGb(3)));

                assertThat(deciderResults.currentCapacity().total().memory(), equalTo(ByteSizeValue.ofGb(0)));
            });
        } else {
            assertAutoscalingDeciderResults(deciderResults -> {
                assertThat(deciderResults.requiredCapacity().node().memory(), equalTo(ByteSizeValue.ofGb(0)));
                assertThat(deciderResults.requiredCapacity().total().memory(), equalTo(ByteSizeValue.ofGb(0)));
            });

            final ActionFuture<Void> secondRoundOfNodeStatsRequested = nodeStatsTestClient.respond(
                (req, listener) -> respondWithFakeNodeMemory(req, hotNodeMemory, listener),
                3
            );

            startNodes(3, DiscoveryNodeRole.DATA_HOT_NODE_ROLE, DiscoveryNodeRole.DATA_CONTENT_NODE_ROLE, DiscoveryNodeRole.MASTER_ROLE);
            ensureStableCluster(6);
            secondRoundOfNodeStatsRequested.get();

            assertAutoscalingDeciderResults(deciderResults -> {
                assertThat(deciderResults.requiredCapacity().node().memory(), equalTo(ByteSizeValue.ofGb(1)));
                assertThat(deciderResults.requiredCapacity().total().memory(), equalTo(ByteSizeValue.ofGb(3)));

                assertThat(deciderResults.currentCapacity().total().memory(), equalTo(ByteSizeValue.ofGb(0)));
            });
        }
    }

    public void testSmallClusterDoNotNeedDedicatedMasters() throws Exception {
        final ActionFuture<Void> nodeStatsRequestedForAllNodes = nodeStatsTestClient.respond(
            (req, listener) -> respondWithFakeNodeMemory(req, ByteSizeValue.ofGb(1), listener),
            7
        );

        internalCluster().setBootstrapMasterNodeIndex(0);
        startNodes(3, DiscoveryNodeRole.DATA_HOT_NODE_ROLE, DiscoveryNodeRole.DATA_CONTENT_NODE_ROLE, DiscoveryNodeRole.MASTER_ROLE);
        startNodes(2, DiscoveryNodeRole.DATA_WARM_NODE_ROLE);
        startNodes(2, DiscoveryNodeRole.DATA_COLD_NODE_ROLE);
        ensureStableCluster(7);

        putAutoscalingPolicy(Settings.EMPTY);
        nodeStatsRequestedForAllNodes.get();

        assertAutoscalingDeciderResults(deciderResults -> {
            assertThat(deciderResults.requiredCapacity().node().memory(), equalTo(ByteSizeValue.ofGb(0)));
            assertThat(deciderResults.requiredCapacity().total().memory(), equalTo(ByteSizeValue.ofGb(0)));
        });
    }

    public void testDoesNotScaleDownOnceTheClusterHasDedicatedMasters() throws Exception {
        int numHotContentNodes = 6;
        final ActionFuture<Void> nodeStatsRequestedForAllNodes = nodeStatsTestClient.respond(
            (req, listener) -> respondWithFakeNodeMemory(req, ByteSizeValue.ofGb(64), listener),
            numHotContentNodes
        );

        internalCluster().setBootstrapMasterNodeIndex(0);
        startNodes(
            numHotContentNodes,
            DiscoveryNodeRole.DATA_HOT_NODE_ROLE,
            DiscoveryNodeRole.DATA_CONTENT_NODE_ROLE,
            DiscoveryNodeRole.MASTER_ROLE
        );
        ensureStableCluster(numHotContentNodes);

        putAutoscalingPolicy(Settings.EMPTY);
        nodeStatsRequestedForAllNodes.get();

        assertAutoscalingDeciderResults(deciderResults -> {
            assertThat(deciderResults.requiredCapacity().node().memory(), equalTo(ByteSizeValue.ofGb(1)));
            assertThat(deciderResults.requiredCapacity().total().memory(), equalTo(ByteSizeValue.ofGb(3)));
        });

        final int numMasterNodes = randomIntBetween(1, 3);
        final ActionFuture<Void> nodeStatsRequestedForMasterNodes = nodeStatsTestClient.respond(
            (req, listener) -> respondWithFakeNodeMemory(req, ByteSizeValue.ofGb(1), listener),
            numMasterNodes
        );

        internalCluster().startMasterOnlyNodes(numMasterNodes);

        nodeStatsRequestedForMasterNodes.get();

        internalCluster().stopRandomNonMasterNode();

        ensureStableCluster(numHotContentNodes - 1 + numMasterNodes);

        assertAutoscalingDeciderResults(deciderResults -> {
            assertThat(deciderResults.requiredCapacity().node().memory(), equalTo(ByteSizeValue.ofGb(1)));
            assertThat(deciderResults.requiredCapacity().total().memory(), equalTo(ByteSizeValue.ofGb(3)));
        });
    }

    private void assertAutoscalingDeciderResults(Consumer<AutoscalingDeciderResults> consumer) {
        GetAutoscalingCapacityAction.Request request = new GetAutoscalingCapacityAction.Request();
        final GetAutoscalingCapacityAction.Response capacity = client().execute(GetAutoscalingCapacityAction.INSTANCE, request).actionGet();
        final AutoscalingDeciderResults autoscalingDeciderResults = capacity.getResults().get("test");
        assertThat(autoscalingDeciderResults, is(notNullValue()));
        consumer.accept(autoscalingDeciderResults);
    }

    private void startNodes(int nodeCount, DiscoveryNodeRole... roles) {
        internalCluster().startNodes(
            nodeCount,
            Settings.builder()
                .putList(
                    NodeRoleSettings.NODE_ROLES_SETTING.getKey(),
                    Arrays.stream(roles).map(DiscoveryNodeRole::roleName).collect(Collectors.toList())
                )
                .build()
        );
    }

    private void respondWithFakeNodeMemory(NodesStatsRequest request, ByteSizeValue memory, ActionListener<NodesStatsResponse> listener) {
        client().admin().cluster().prepareState().execute(new ActionListener<>() {
            @Override
            public void onResponse(ClusterStateResponse stateResponse) {
                final ClusterState state = stateResponse.getState();
                final List<NodeStats> nodeStats = new ArrayList<>(request.nodesIds().length);
                final String[] nodesIds = state.nodes().resolveNodes(request.nodesIds());
                request.setConcreteNodes(Arrays.stream(nodesIds).map(state.nodes()::get).toArray(DiscoveryNode[]::new));
                for (DiscoveryNode node : request.concreteNodes()) {
                    final OsStats.Cpu cpu = new OsStats.Cpu(randomShort(), null);
                    final OsStats.Mem mem = new OsStats.Mem(memory.getBytes(), memory.getBytes(), memory.getBytes());
                    final OsStats.Swap swap = new OsStats.Swap(0, 0);
                    final OsStats osStats = new OsStats(System.currentTimeMillis(), cpu, mem, swap, null);

                    nodeStats.add(
                        new NodeStats(
                            node,
                            System.currentTimeMillis(),
                            null,
                            osStats,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null
                        )
                    );
                }
                listener.onResponse(new NodesStatsResponse(new ClusterName("test"), nodeStats, Collections.emptyList()));
            }

            @Override
            public void onFailure(Exception e) {
                assert false : "unexpected failure";
            }
        });
    }

    private void putAutoscalingPolicy(Settings settings) {
        final PutAutoscalingPolicyAction.Request request = new PutAutoscalingPolicyAction.Request(
            "test",
            new TreeSet<>(Set.of(DiscoveryNodeRole.MASTER_ROLE.roleName())),
            new TreeMap<>(Map.of(DedicatedMasterNodesDeciderService.NAME, settings))
        );
        assertAcked(client().execute(PutAutoscalingPolicyAction.INSTANCE, request).actionGet());
    }

    public static class NodeStatsTestClient extends NoOpClient {
        private NodeStatsFakeResponder nodeStatsFakeResponder;
        private volatile boolean failingClient;

        public NodeStatsTestClient(String name) {
            super(name);
        }

        public ActionFuture<Void> respond(
            BiConsumer<NodesStatsRequest, ActionListener<NodesStatsResponse>> responderValue,
            int expectedNumberOfNodeStatsRequested
        ) {
            assertThat(responderValue, notNullValue());
            assertThat(nodeStatsFakeResponder, is(nullValue()));
            this.nodeStatsFakeResponder = new NodeStatsFakeResponder(responderValue, expectedNumberOfNodeStatsRequested);

            return nodeStatsFakeResponder.future;
        }

        @Override
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            if (failingClient) {
                listener.onFailure(new RuntimeException());
                return;
            }
            assertThat(action, sameInstance(NodesStatsAction.INSTANCE));
            NodesStatsRequest nodesStatsRequest = (NodesStatsRequest) request;
            assertThat(nodeStatsFakeResponder, notNullValue());
            @SuppressWarnings("unchecked")
            ActionListener<NodesStatsResponse> statsListener = (ActionListener<NodesStatsResponse>) listener;
            if (nodeStatsFakeResponder.accept(nodesStatsRequest, statsListener)) {
                nodeStatsFakeResponder = null;
            }
        }

        public void setFailingClient() {
            failingClient = true;
        }

        public void unsetFailingClient() {
            failingClient = false;
        }
    }

    static class NodeStatsFakeResponder {
        final BiConsumer<NodesStatsRequest, ActionListener<NodesStatsResponse>> responder;
        final CountDown countDown;
        final PlainActionFuture<Void> future;
        boolean finished;

        NodeStatsFakeResponder(BiConsumer<NodesStatsRequest, ActionListener<NodesStatsResponse>> responder, int expectedNodeRequests) {
            this.responder = responder;
            this.countDown = new CountDown(expectedNodeRequests);
            this.future = PlainActionFuture.newFuture();
        }

        boolean accept(NodesStatsRequest nodesStatsRequest, ActionListener<NodesStatsResponse> listener) {
            assert finished == false;

            responder.accept(nodesStatsRequest, listener);
            for (int i = 0; i < nodesStatsRequest.nodesIds().length; i++) {
                if (countDown.countDown()) {
                    future.onResponse(null);
                    finished = true;
                    return true;
                }
            }
            return false;
        }
    }

}
