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

package org.elasticsearch.action.support.nodes;

import org.elasticsearch.Version;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.broadcast.node.TransportBroadcastByNodeActionTests;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.DummyTransportAddress;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.cluster.TestClusterService;
import org.elasticsearch.test.transport.CapturingTransport;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceArray;


public class TransportNodesActionTests extends ESTestCase {

    private static ThreadPool THREAD_POOL;
    private static ClusterName CLUSTER_NAME = new ClusterName("test-cluster");

    private TestClusterService clusterService;
    private CapturingTransport transport;
    private TransportService transportService;

    public void testRequestIsSentToEachNode() throws Exception {
        TransportNodesAction action = getTestTransportNodesAction();
        TestNodesRequest request = new TestNodesRequest();
        PlainActionFuture<TestNodesResponse> listener = new PlainActionFuture<>();
        action.doExecute(new Task(0, "some type", "some action", "test task"), request, listener);
        Map<String, List<CapturingTransport.CapturedRequest>> capturedRequests = transport.capturedRequestsByTargetNode();
        transport.clear();
        int numNodes = clusterService.state().getNodes().getSize();
        // check a request was sent to the right number of nodes
        assertEquals(numNodes, capturedRequests.size());
    }

    public void testFiltering() throws Exception {
        TransportNodesAction action = getFilteringTestTransportNodesAction();
        TestNodesRequest request = new TestNodesRequest();
        PlainActionFuture<TestNodesResponse> listener = new PlainActionFuture<>();
        action.doExecute(new Task(0, "some type", "some action", "test task"), request, listener);
        Map<String, List<CapturingTransport.CapturedRequest>> capturedRequests = transport.capturedRequestsByTargetNode();
        transport.clear();
        // check requests were only sent to data nodes
        for (String nodeTarget : capturedRequests.keySet()) {
            assertTrue(clusterService.state().nodes().get(nodeTarget).isDataNode());
        }
        assertEquals(clusterService.state().nodes().getDataNodes().size(), capturedRequests.size());
    }

    @BeforeClass
    public static void startThreadPool() {
        THREAD_POOL = new ThreadPool(TransportBroadcastByNodeActionTests.class.getSimpleName());
    }

    @AfterClass
    public static void destroyThreadPool() {
        ThreadPool.terminate(THREAD_POOL, 30, TimeUnit.SECONDS);
        // since static must set to null to be eligible for collection
        THREAD_POOL = null;
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        transport = new CapturingTransport();
        clusterService = new TestClusterService(THREAD_POOL);
        transportService = new TransportService(transport, THREAD_POOL);
        transportService.start();
        transportService.acceptIncomingRequests();
        int numNodes = randomIntBetween(3, 10);
        DiscoveryNodes.Builder discoBuilder = DiscoveryNodes.builder();
        List<DiscoveryNode> discoveryNodes = new ArrayList<>();
        for (int i = 0; i < numNodes; i++) {
            Map<String, String> attributes = getRandomAttributes();
            if (frequently()) {
                attributes.put("custom", randomBoolean() ? "match" : randomAsciiOfLengthBetween(3, 5));
            }
            final DiscoveryNode node = newNode(i, attributes);
            discoBuilder = discoBuilder.put(node);
            discoveryNodes.add(node);
        }
        discoBuilder.localNodeId(randomFrom(discoveryNodes).getId());
        discoBuilder.masterNodeId(randomFrom(discoveryNodes).getId());
        ClusterState.Builder stateBuilder = ClusterState.builder(CLUSTER_NAME);
        stateBuilder.nodes(discoBuilder);
        ClusterState clusterState = stateBuilder.build();
        clusterService.setState(clusterState);
    }

    private HashMap<String, String> getRandomAttributes() {
        String[] roles = new String[]{"master", "data", "ingest"};
        HashMap<String, String> attributes = new HashMap<>();
        for (String role : roles) {
            attributes.put(role, "true");
        }
        List<String> unsetRoles= randomSubsetOf(randomIntBetween(1,2), roles);
        for (String role : unsetRoles) {
            attributes.put(role, "false");
        }
        return attributes;
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        transport.close();
    }

    public TestTransportNodesAction getTestTransportNodesAction() {
        return new TestTransportNodesAction(
            Settings.EMPTY,
            THREAD_POOL,
            clusterService,
            transportService,
            new ActionFilters(new HashSet<ActionFilter>()),
            ThreadPool.Names.SAME
        );
    }

    public FilteringTestTransportNodesAction getFilteringTestTransportNodesAction() {
        return new FilteringTestTransportNodesAction(
            Settings.EMPTY,
            THREAD_POOL,
            clusterService,
            transportService,
            new ActionFilters(new HashSet<ActionFilter>()),
            ThreadPool.Names.SAME
        );
    }

    private static DiscoveryNode newNode(int nodeId, Map<String, String> attributes) {
        String node = "node_" + nodeId;
        return new DiscoveryNode(node, node, DummyTransportAddress.INSTANCE, attributes, Version.CURRENT);
    }

    private static class TestTransportNodesAction
        extends TransportNodesAction<TestNodesRequest, TestNodesResponse, TestNodeRequest, TestNodeResponse> {

        TestTransportNodesAction(Settings settings, ThreadPool threadPool, ClusterService clusterService, TransportService
            transportService, ActionFilters actionFilters, String nodeExecutor) {
            super(settings, "indices:admin/test", CLUSTER_NAME, threadPool, clusterService, transportService, actionFilters,
                null, TestNodesRequest.class, TestNodeRequest.class, nodeExecutor);
        }

        @Override
        protected TestNodesResponse newResponse(TestNodesRequest request, AtomicReferenceArray nodesResponses) {
            return new TestNodesResponse();
        }

        @Override
        protected TestNodeRequest newNodeRequest(String nodeId, TestNodesRequest request) {
            return new TestNodeRequest();
        }

        @Override
        protected TestNodeResponse newNodeResponse() {
            return new TestNodeResponse();
        }

        @Override
        protected TestNodeResponse nodeOperation(TestNodeRequest request) {
            return new TestNodeResponse();
        }

        @Override
        protected boolean accumulateExceptions() {
            return false;
        }
    }

    public static class FilteringTestTransportNodesAction
        extends TestTransportNodesAction {

        FilteringTestTransportNodesAction(Settings settings, ThreadPool threadPool, ClusterService clusterService, TransportService
            transportService, ActionFilters actionFilters, String nodeExecutor) {
            super(settings, threadPool, clusterService, transportService, actionFilters, nodeExecutor);
        }

        @Override
        protected String[] filterNodeIds(DiscoveryNodes nodes, String[] nodesIds) {
            return nodes.getDataNodes().keys().toArray(String.class);
        }
    }

    public static class TestNodesRequest extends BaseNodesRequest<TestNodesRequest> {
    }

    private static class TestNodesResponse extends BaseNodesResponse<TestNodeResponse> {
    }

    public static class TestNodeRequest extends BaseNodeRequest {
    }

    private static class TestNodeResponse extends BaseNodeResponse {
    }
}
