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
package org.elasticsearch.action.support.replication;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionWriteResponse;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.*;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.DummyTransportAddress;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.test.cluster.TestClusterService;
import org.elasticsearch.test.transport.CapturingTransport;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.cluster.metadata.IndexMetaData.*;
import static org.hamcrest.Matchers.*;

public class ShardReplicationOperationTests extends ElasticsearchTestCase {

    private TransportService transportService;

    static class Request extends ShardReplicationOperationRequest<Request> {
        int shardId;
        public AtomicBoolean processedOnPrimary = new AtomicBoolean();
        public AtomicInteger processedOnReplicas = new AtomicInteger();

        Request() {
        }

        Request(ShardId shardId) {
            this.shardId = shardId.id();
            this.index(shardId.index().name());
            // keep things simple
            this.operationThreaded(false);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeVInt(shardId);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            shardId = in.readVInt();
        }
    }

    static class Response extends ActionWriteResponse {

    }

    static class Action extends TransportShardReplicationOperationAction<Request, Request, Response> {

        protected Action(Settings settings, String actionName, TransportService transportService,
                         ClusterService clusterService,
                         ThreadPool threadPool) {
            super(settings, actionName, transportService, clusterService, null, threadPool,
                    new ShardStateAction(settings, clusterService, transportService, null, null),
                    new ActionFilters(new HashSet<ActionFilter>()));
        }

        @Override
        protected Request newRequestInstance() {
            return new Request();
        }

        @Override
        protected Request newReplicaRequestInstance() {
            return new Request();
        }

        @Override
        protected Response newResponseInstance() {
            return new Response();
        }

        @Override
        protected String executor() {
            return ThreadPool.Names.SAME;
        }

        @Override
        protected Tuple<Response, Request> shardOperationOnPrimary(ClusterState clusterState, PrimaryOperationRequest shardRequest) throws Throwable {
            boolean executedBefore = shardRequest.request.processedOnPrimary.getAndSet(true);
            assert executedBefore == false : "request has already been executed on the primary";
            return new Tuple<>(new Response(), shardRequest.request);
        }

        @Override
        protected void shardOperationOnReplica(ReplicaOperationRequest shardRequest) {
            shardRequest.request.processedOnReplicas.incrementAndGet();
        }

        @Override
        protected ShardIterator shards(ClusterState clusterState, InternalRequest request) throws ElasticsearchException {
            return clusterState.getRoutingTable().index(request.concreteIndex()).shard(request.request().shardId).shardsIt();
        }

        @Override
        protected boolean checkWriteConsistency() {
            return false;
        }

        @Override
        protected boolean resolveIndex() {
            return false;
        }
    }

    public static DiscoveryNode newNode(int nodeId) {
        return new DiscoveryNode("node_" + nodeId, DummyTransportAddress.INSTANCE, Version.CURRENT);
    }


    static ThreadPool threadPool;

    TestClusterService clusterService;
    CapturingTransport transport;


    @BeforeClass
    public static void beforeClass() {
        threadPool = new ThreadPool("ShardReplicationOperationTests");
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        transport = new CapturingTransport();
        clusterService = new TestClusterService(threadPool);
        transportService = new TransportService(transport, threadPool);
        transportService.start();
    }

    @AfterClass
    public static void afterClass() {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
    }


    <T> void assertListenerThrows(String msg, PlainActionFuture<T> listener, Class<?> klass) throws InterruptedException {
        try {
            listener.get();
            fail(msg);
        } catch (ExecutionException ex) {
            assertThat(ex.getCause(), instanceOf(klass));
        }

    }

    @Test
    public void testBlocks() throws ExecutionException, InterruptedException {
        Action action = new Action(ImmutableSettings.EMPTY, "test",
                new TransportService(transport, threadPool), clusterService, threadPool);

        Request request = new Request();
        PlainActionFuture<Response> listener = new PlainActionFuture<>();

        ClusterBlocks.Builder block = ClusterBlocks.builder()
                .addGlobalBlock(new ClusterBlock(1, "non retryable", false, true, RestStatus.SERVICE_UNAVAILABLE, ClusterBlockLevel.ALL));
        clusterService.setState(ClusterState.builder(clusterService.state()).blocks(block));
        TransportShardReplicationOperationAction<Request, Request, Response>.PrimaryPhase primaryPhase = action.new PrimaryPhase(request, listener);
        assertFalse("primary phase should stop execution", primaryPhase.checkBlocks());
        assertListenerThrows("primary phase should fail operation", listener, ClusterBlockException.class);

        block = ClusterBlocks.builder()
                .addGlobalBlock(new ClusterBlock(1, "retryable", true, true, RestStatus.SERVICE_UNAVAILABLE, ClusterBlockLevel.ALL));
        clusterService.setState(ClusterState.builder(clusterService.state()).blocks(block));
        listener = new PlainActionFuture<>();
        primaryPhase = action.new PrimaryPhase(new Request().timeout("5ms"), listener);
        assertFalse("primary phase should stop execution on retryable block", primaryPhase.checkBlocks());
        assertListenerThrows("failed to timeout on retryable block", listener, ClusterBlockException.class);


        listener = new PlainActionFuture<>();
        primaryPhase = action.new PrimaryPhase(new Request(), listener);
        assertFalse("primary phase should stop execution on retryable block", primaryPhase.checkBlocks());
        assertFalse("primary phase should wait on retryable block", listener.isDone());

        block = ClusterBlocks.builder()
                .addGlobalBlock(new ClusterBlock(1, "non retryable", false, true, RestStatus.SERVICE_UNAVAILABLE, ClusterBlockLevel.ALL));
        clusterService.setState(ClusterState.builder(clusterService.state()).blocks(block));
        assertListenerThrows("primary phase should fail operation when moving from a retryable block a non-retryable one", listener, ClusterBlockException.class);
    }


    ClusterState createStateWithSingleShardIndex(String index, boolean primaryAssigned, int numberOfReplicas) {
        final int numberOfNodes = numberOfReplicas + 1;
        final ShardId shardId = new ShardId(index, 0);
        DiscoveryNodes.Builder discoBuilder = DiscoveryNodes.builder();
        Set<DiscoveryNode> unassignedNodes = new HashSet<>();
        for (int i = 0; i < numberOfNodes + 1; i++) {
            final DiscoveryNode node = newNode(i);
            discoBuilder = discoBuilder.put(node);
            unassignedNodes.add(node);
        }
        discoBuilder.localNodeId(newNode(0).id());
        IndexMetaData indexMetaData = IndexMetaData.builder(index).settings(ImmutableSettings.builder()
                .put(SETTING_VERSION_CREATED, Version.CURRENT)
                .put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_NUMBER_OF_REPLICAS, numberOfReplicas)
                .put(SETTING_CREATION_DATE, System.currentTimeMillis())).build();

        RoutingTable.Builder routing = new RoutingTable.Builder();
        routing.addAsNew(indexMetaData);
        IndexShardRoutingTable.Builder indexShardRoutingBuilder = new IndexShardRoutingTable.Builder(shardId, false);

        boolean primaryStarted = false;
        DiscoveryNode primaryNode = randomFrom(unassignedNodes.toArray(new DiscoveryNode[0]));
        if (primaryAssigned) {
            unassignedNodes.remove(primaryNode);
            primaryStarted = randomBoolean();
            indexShardRoutingBuilder.addShard(
                    new ImmutableShardRouting(index, 0, primaryNode.id(), true, primaryStarted ? ShardRoutingState.STARTED : ShardRoutingState.INITIALIZING, 0));
        }

        int assignedReplicas = primaryStarted ? randomIntBetween(0, numberOfReplicas) : 0;
        for (int i = 0; i < assignedReplicas; i++) {
            DiscoveryNode replicaNode = randomFrom(unassignedNodes.toArray(new DiscoveryNode[0]));
            unassignedNodes.remove(replicaNode);
            indexShardRoutingBuilder.addShard(
                    new ImmutableShardRouting(index, shardId.id(), replicaNode.id(), false, randomBoolean() ? ShardRoutingState.STARTED : ShardRoutingState.INITIALIZING, 0));
        }

        for (int i = assignedReplicas; i <= numberOfReplicas; i++) {
            indexShardRoutingBuilder.addShard(new ImmutableShardRouting(index, shardId.id(), null, false, ShardRoutingState.UNASSIGNED, 0));
        }

        ClusterState.Builder state = ClusterState.builder(new ClusterName("test"));
        state.nodes(discoBuilder);
        state.metaData(MetaData.builder().put(indexMetaData, false).generateUuidIfNeeded());
        state.routingTable(RoutingTable.builder().add(IndexRoutingTable.builder(index).addIndexShard(indexShardRoutingBuilder.build())));
        return state.build();
    }

    @Test
    public void testRoutingToPrimary() {
        final String index = "test";
        final ShardId shardId = new ShardId(index, 0);
        clusterService.setState(createStateWithSingleShardIndex(index, true, randomInt(3)));

        logger.debug("using state: \n{}", clusterService.state().prettyPrint());

        final IndexShardRoutingTable shardRoutingTable = clusterService.state().routingTable().index(index).shard(shardId.id());
        final String primaryNodeId = shardRoutingTable.primaryShard().currentNodeId();
        Request request = new Request(shardId);
        PlainActionFuture<Response> listener = new PlainActionFuture<>();
        Action action = new Action(ImmutableSettings.EMPTY, "testAction", transportService, clusterService, threadPool);

        TransportShardReplicationOperationAction<Request, Request, Response>.PrimaryPhase primaryPhase = action.new PrimaryPhase(request, listener);
        assertTrue(primaryPhase.checkBlocks());
        primaryPhase.routeRequestOrPerformLocally(shardRoutingTable.primaryShard(), shardRoutingTable.shardsIt());
        if (primaryNodeId.equals(clusterService.localNode().id())) {
            logger.info("--> primary is assigned locally, testing for execution");
            assertTrue("request failed to be processed on a local primary", request.processedOnPrimary.get());
        } else {
            logger.info("--> primary is assigned to [{}], checking request forwarded", primaryNodeId);
            final List<CapturingTransport.CapturedRequest> capturedRequests = transport.capturedRequestsByTargetNode().get(primaryNodeId);
            assertThat(capturedRequests, notNullValue());
            assertThat(capturedRequests.size(), equalTo(1));
            assertThat(capturedRequests.get(0).action, equalTo("testAction"));
        }
    }
}
