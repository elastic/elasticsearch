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
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.BaseNodeRequest;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.cluster.TestClusterService;
import org.elasticsearch.test.tasks.MockTaskManager;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.local.LocalTransport;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * The test case for unit testing task manager and related transport actions
 */
public abstract class TaskManagerTestCase extends ESTestCase {

    protected static ThreadPool threadPool;
    public static final ClusterName clusterName = new ClusterName("test-cluster");
    protected TestNode[] testNodes;
    protected int nodesCount;

    @BeforeClass
    public static void beforeClass() {
        threadPool = new ThreadPool(TransportTasksActionTests.class.getSimpleName());
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
            ;
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

        private int failureCount;

        protected NodesResponse(ClusterName clusterName, NodeResponse[] nodes, int failureCount) {
            super(clusterName, nodes);
            this.failureCount = failureCount;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            failureCount = in.readVInt();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeVInt(failureCount);
        }

        public int failureCount() {
            return failureCount;
        }
    }

    /**
     * Simulates node-based task that can be used to block node tasks so they are guaranteed to be registered by task manager
     */
    abstract class AbstractTestNodesAction<NodesRequest extends BaseNodesRequest<NodesRequest>, NodeRequest extends BaseNodeRequest>
        extends TransportNodesAction<NodesRequest, NodesResponse, NodeRequest, NodeResponse> {

        AbstractTestNodesAction(Settings settings, String actionName, ClusterName clusterName, ThreadPool threadPool,
                                ClusterService clusterService, TransportService transportService, Class<NodesRequest> request,
                                Class<NodeRequest> nodeRequest) {
            super(settings, actionName, clusterName, threadPool, clusterService, transportService,
                new ActionFilters(new HashSet<ActionFilter>()), new IndexNameExpressionResolver(Settings.EMPTY),
                request, nodeRequest, ThreadPool.Names.GENERIC);
        }

        @Override
        protected NodesResponse newResponse(NodesRequest request, AtomicReferenceArray responses) {
            final List<NodeResponse> nodesList = new ArrayList<>();
            int failureCount = 0;
            for (int i = 0; i < responses.length(); i++) {
                Object resp = responses.get(i);
                if (resp instanceof NodeResponse) { // will also filter out null response for unallocated ones
                    nodesList.add((NodeResponse) resp);
                } else if (resp instanceof FailedNodeException) {
                    failureCount++;
                } else {
                    logger.warn("unknown response type [{}], expected NodeLocalGatewayMetaState or FailedNodeException", resp);
                }
            }
            return new NodesResponse(clusterName, nodesList.toArray(new NodeResponse[nodesList.size()]), failureCount);
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
            transportService = new TransportService(settings,
                new LocalTransport(settings, threadPool, Version.CURRENT, new NamedWriteableRegistry()),
                threadPool) {
                @Override
                protected TaskManager createTaskManager() {
                    if (settings.getAsBoolean(MockTaskManager.USE_MOCK_TASK_MANAGER, false)) {
                        return new MockTaskManager(settings);
                    } else {
                        return super.createTaskManager();
                    }
                }
            };
            transportService.start();
            clusterService = new TestClusterService(threadPool, transportService);
            clusterService.add(transportService.getTaskManager());
            discoveryNode = new DiscoveryNode(name, transportService.boundAddress().publishAddress(), Version.CURRENT);
            transportService.setLocalNode(discoveryNode);
            transportService.acceptIncomingRequests();
            IndexNameExpressionResolver indexNameExpressionResolver = new IndexNameExpressionResolver(settings);
            ActionFilters actionFilters = new ActionFilters(Collections.<ActionFilter>emptySet());
            transportListTasksAction = new TransportListTasksAction(settings, clusterName, threadPool, clusterService, transportService,
                actionFilters, indexNameExpressionResolver);
            transportCancelTasksAction = new TransportCancelTasksAction(settings, clusterName, threadPool, clusterService, transportService,
                actionFilters, indexNameExpressionResolver);
        }

        public final TestClusterService clusterService;
        public final TransportService transportService;
        public final DiscoveryNode discoveryNode;
        public final TransportListTasksAction transportListTasksAction;
        public final TransportCancelTasksAction transportCancelTasksAction;

        @Override
        public void close() {
            transportService.close();
        }
    }

    public static void connectNodes(TestNode... nodes) {
        DiscoveryNode[] discoveryNodes = new DiscoveryNode[nodes.length];
        for (int i = 0; i < nodes.length; i++) {
            discoveryNodes[i] = nodes[i].discoveryNode;
        }
        DiscoveryNode master = discoveryNodes[0];
        for (TestNode node : nodes) {
            node.clusterService.setState(ClusterStateCreationUtils.state(node.discoveryNode, master, discoveryNodes));
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
            ((MockTaskManager) (nodes[i].clusterService.getTaskManager())).addListener(listeners[i]);
        }
        return listeners;
    }

}
