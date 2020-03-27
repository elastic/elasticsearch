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

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.Version;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.TransportCancelTasksAction;
import org.elasticsearch.action.admin.cluster.node.tasks.list.TransportListTasksAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.tasks.MockTaskManager;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.AbstractSimpleTransportTestCase;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.nio.MockNioTransport;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.elasticsearch.test.ClusterServiceUtils.setState;

/**
 * The test case for unit testing task manager and related transport actions
 */
public abstract class TaskManagerTestCase extends ESTestCase {

    protected ThreadPool threadPool;
    protected TestNode[] testNodes;
    protected int nodesCount;

    @Before
    public void setupThreadPool() {
        threadPool = new TestThreadPool(TransportTasksActionTests.class.getSimpleName());
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
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        threadPool = null;
    }


    static class NodeResponse extends BaseNodeResponse {

        protected NodeResponse(StreamInput in) throws IOException {
            super(in);
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
            return in.readList(NodeResponse::new);
        }

        @Override
        protected void writeNodesTo(StreamOutput out, List<NodeResponse> nodes) throws IOException {
            out.writeList(nodes);
        }

        public int failureCount() {
            return failures().size();
        }
    }

    /**
     * Simulates node-based task that can be used to block node tasks so they are guaranteed to be registered by task manager
     */
    abstract class AbstractTestNodesAction<NodesRequest extends BaseNodesRequest<NodesRequest>, NodeRequest extends TransportRequest>
            extends TransportNodesAction<NodesRequest, NodesResponse, NodeRequest, NodeResponse> {

        AbstractTestNodesAction(String actionName, ThreadPool threadPool,
                                ClusterService clusterService, TransportService transportService, Writeable.Reader<NodesRequest> request,
                                Writeable.Reader<NodeRequest> nodeRequest) {
            super(actionName, threadPool, clusterService, transportService,
                    new ActionFilters(new HashSet<>()),
                request, nodeRequest, ThreadPool.Names.GENERIC, NodeResponse.class);
        }

        @Override
        protected NodesResponse newResponse(NodesRequest request, List<NodeResponse> responses, List<FailedNodeException> failures) {
            return new NodesResponse(clusterService.getClusterName(), responses, failures);
        }

        @Override
        protected NodeResponse newNodeResponse(StreamInput in) throws IOException {
            return new NodeResponse(in);
        }

        @Override
        protected abstract NodeResponse nodeOperation(NodeRequest request, Task task);

    }

    public static class TestNode implements Releasable {
        public TestNode(String name, ThreadPool threadPool, Settings settings) {
            final Function<BoundTransportAddress, DiscoveryNode> boundTransportAddressDiscoveryNodeFunction =
                address -> {
                 discoveryNode.set(new DiscoveryNode(name, address.publishAddress(), emptyMap(), emptySet(), Version.CURRENT));
                 return discoveryNode.get();
                };
            transportService = new TransportService(settings,
                new MockNioTransport(settings, Version.CURRENT, threadPool, new NetworkService(Collections.emptyList()),
                    PageCacheRecycler.NON_RECYCLING_INSTANCE, new NamedWriteableRegistry(ClusterModule.getNamedWriteables()),
                    new NoneCircuitBreakerService()),
                threadPool, TransportService.NOOP_TRANSPORT_INTERCEPTOR, boundTransportAddressDiscoveryNodeFunction, null,
                Collections.emptySet()) {
                @Override
                protected TaskManager createTaskManager(Settings settings, ThreadPool threadPool, Set<String> taskHeaders) {
                    if (MockTaskManager.USE_MOCK_TASK_MANAGER_SETTING.get(settings)) {
                        return new MockTaskManager(settings, threadPool, taskHeaders);
                    } else {
                        return super.createTaskManager(settings, threadPool, taskHeaders);
                    }
                }
            };
            transportService.start();
            clusterService = createClusterService(threadPool, discoveryNode.get());
            clusterService.addStateApplier(transportService.getTaskManager());
            ActionFilters actionFilters = new ActionFilters(emptySet());
            transportListTasksAction = new TransportListTasksAction(clusterService, transportService, actionFilters);
            transportCancelTasksAction = new TransportCancelTasksAction(clusterService, transportService, actionFilters);
            transportService.acceptIncomingRequests();
        }

        public final ClusterService clusterService;
        public final TransportService transportService;
        private final SetOnce<DiscoveryNode> discoveryNode = new SetOnce<>();
        public final TransportListTasksAction transportListTasksAction;
        public final TransportCancelTasksAction transportCancelTasksAction;

        @Override
        public void close() {
            clusterService.close();
            transportService.close();
        }

        public String getNodeId() {
            return discoveryNode().getId();
        }

        public DiscoveryNode discoveryNode() { return  discoveryNode.get(); }
    }

    public static void connectNodes(TestNode... nodes) {
        DiscoveryNode[] discoveryNodes = new DiscoveryNode[nodes.length];
        for (int i = 0; i < nodes.length; i++) {
            discoveryNodes[i] = nodes[i].discoveryNode();
        }
        DiscoveryNode master = discoveryNodes[0];
        for (TestNode node : nodes) {
            setState(node.clusterService, ClusterStateCreationUtils.state(node.discoveryNode(), master, discoveryNodes));
        }
        for (TestNode nodeA : nodes) {
            for (TestNode nodeB : nodes) {
                AbstractSimpleTransportTestCase.connectToNode(nodeA.transportService, nodeB.discoveryNode());
            }
        }
    }

    public static RecordingTaskManagerListener[] setupListeners(TestNode[] nodes, String... actionMasks) {
        RecordingTaskManagerListener[] listeners = new RecordingTaskManagerListener[nodes.length];
        for (int i = 0; i < nodes.length; i++) {
            listeners[i] = new RecordingTaskManagerListener(nodes[i].getNodeId(), actionMasks);
            ((MockTaskManager) (nodes[i].transportService.getTaskManager())).addListener(listeners[i]);
        }
        return listeners;
    }
}
