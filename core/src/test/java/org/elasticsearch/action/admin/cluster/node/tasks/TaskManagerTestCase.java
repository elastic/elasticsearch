/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.action.admin.cluster.node.tasks;

import org.elasticsearch.Version;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.TransportCancelTasksAction;
import org.elasticsearch.action.admin.cluster.node.tasks.list.TransportListTasksAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.BaseNodeRequest;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.tasks.MockTaskManager;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.local.LocalTransport;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.elasticsearch.test.ClusterServiceUtils.setState;

/**
 * The test case for unit testing task manager and related transport actions
 */
public abstract class TaskManagerTestCase extends ESTestCase {

    protected static ThreadPool threadPool;
    public static final Settings CLUSTER_SETTINGS = Settings.builder().put("cluster.name", "test-cluster").build();
    protected TestNode[] testNodes;
    protected int nodesCount;

    @BeforeClass
    public static void beforeClass() {
        threadPool = new TestThreadPool(TransportTasksActionTests.class.getSimpleName());
    }

    @AfterClass
    public static void afterClass() {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        threadPool = null;
    }

    public void setupTestNodes(Settings settings) {
        nodesCount = randomIntBetween(2, 10);
        testNodes = new TestNode[nodesCount];
        for (int i = 0; i < testNodes.length; i++) {
            testNodes[i] = new TestNode("node" + i, threadPool, settings);
        }
    }

    @After
    public final void shutdownTestNodes() throws Exception {
        for (TestNode testNode : testNodes) {
            testNode.close();
        }
    }


    static class NodeResponse extends BaseNodeResponse {

        protected NodeResponse() {
            super();
        }

        protected NodeResponse(DiscoveryNode node) {
            super(node);
        }
    }

    static class NodesResponse extends BaseNodesResponse<NodeResponse> {

        protected NodesResponse(ClusterName clusterName, List<NodeResponse> nodes, List<FailedNodeException> failures) {
            super(clusterName, nodes, failures);
        }

        @Override
        protected List<NodeResponse> readNodesFrom(StreamInput in) throws IOException {
            return in.readStreamableList(NodeResponse::new);
        }

        @Override
        protected void writeNodesTo(StreamOutput out, List<NodeResponse> nodes) throws IOException {
            out.writeStreamableList(nodes);
        }

        public int failureCount() {
            return failures().size();
        }
    }

    /**
     * Simulates node-based task that can be used to block node tasks so they are guaranteed to be registered by task manager
     */
    abstract class AbstractTestNodesAction<NodesRequest extends BaseNodesRequest<NodesRequest>, NodeRequest extends BaseNodeRequest>
            extends TransportNodesAction<NodesRequest, NodesResponse, NodeRequest, NodeResponse> {

        AbstractTestNodesAction(Settings settings, String actionName, ThreadPool threadPool,
                                ClusterService clusterService, TransportService transportService, Supplier<NodesRequest> request,
                                Supplier<NodeRequest> nodeRequest) {
            super(settings, actionName, threadPool, clusterService, transportService,
                    new ActionFilters(new HashSet<>()), new IndexNameExpressionResolver(Settings.EMPTY),
                    request, nodeRequest, ThreadPool.Names.GENERIC, NodeResponse.class);
        }

        @Override
        protected NodesResponse newResponse(NodesRequest request, List<NodeResponse> responses, List<FailedNodeException> failures) {
            return new NodesResponse(clusterService.getClusterName(), responses, failures);
        }

        @Override
        protected NodeResponse newNodeResponse() {
            return new NodeResponse();
        }

        @Override
        protected abstract NodeResponse nodeOperation(NodeRequest request);

        @Override
        protected boolean accumulateExceptions() {
            return true;
        }
    }

    public static class TestNode implements Releasable {
        public TestNode(String name, ThreadPool threadPool, Settings settings) {
            clusterService = createClusterService(threadPool);
            transportService = new TransportService(settings,
                    new LocalTransport(settings, threadPool, Version.CURRENT, new NamedWriteableRegistry(),
                        new NoneCircuitBreakerService()), threadPool) {
                @Override
                protected TaskManager createTaskManager() {
                    if (MockTaskManager.USE_MOCK_TASK_MANAGER_SETTING.get(settings)) {
                        return new MockTaskManager(settings);
                    } else {
                        return super.createTaskManager();
                    }
                }
            };
            transportService.start();
            clusterService.add(transportService.getTaskManager());
            discoveryNode = new DiscoveryNode(name, transportService.boundAddress().publishAddress(),
                    emptyMap(), emptySet(), Version.CURRENT);
            IndexNameExpressionResolver indexNameExpressionResolver = new IndexNameExpressionResolver(settings);
            ActionFilters actionFilters = new ActionFilters(emptySet());
            transportListTasksAction = new TransportListTasksAction(settings, threadPool, clusterService, transportService,
                    actionFilters, indexNameExpressionResolver);
            transportCancelTasksAction = new TransportCancelTasksAction(settings, threadPool, clusterService,
                    transportService, actionFilters, indexNameExpressionResolver);
            transportService.acceptIncomingRequests();
        }

        public final ClusterService clusterService;
        public final TransportService transportService;
        public final DiscoveryNode discoveryNode;
        public final TransportListTasksAction transportListTasksAction;
        public final TransportCancelTasksAction transportCancelTasksAction;

        @Override
        public void close() {
            clusterService.close();
            transportService.close();
        }

        public String getNodeId() {
            return discoveryNode.getId();
        }
    }

    public static void connectNodes(TestNode... nodes) {
        DiscoveryNode[] discoveryNodes = new DiscoveryNode[nodes.length];
        for (int i = 0; i < nodes.length; i++) {
            discoveryNodes[i] = nodes[i].discoveryNode;
        }
        DiscoveryNode master = discoveryNodes[0];
        for (TestNode node : nodes) {
            setState(node.clusterService, ClusterStateCreationUtils.state(node.discoveryNode, master, discoveryNodes));
        }
        for (TestNode nodeA : nodes) {
            for (TestNode nodeB : nodes) {
                nodeA.transportService.connectToNode(nodeB.discoveryNode);
            }
        }
    }

    public static RecordingTaskManagerListener[] setupListeners(TestNode[] nodes, String... actionMasks) {
        RecordingTaskManagerListener[] listeners = new RecordingTaskManagerListener[nodes.length];
        for (int i = 0; i < nodes.length; i++) {
            listeners[i] = new RecordingTaskManagerListener(nodes[i].discoveryNode, actionMasks);
            ((MockTaskManager) (nodes[i].transportService.getTaskManager())).addListener(listeners[i]);
        }
        return listeners;
    }
}
