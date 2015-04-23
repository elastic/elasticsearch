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

import org.apache.lucene.index.CorruptIndexException;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionWriteResponse;
import org.elasticsearch.action.UnavailableShardsException;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
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
import org.elasticsearch.index.shard.IndexShardNotStartedException;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.test.cluster.TestClusterService;
import org.elasticsearch.test.transport.CapturingTransport;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportResponse;
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

    private static ThreadPool threadPool;

    private TransportService transportService;
    private TestClusterService clusterService;
    private CapturingTransport transport;
    private Action action;


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
        action = new Action(ImmutableSettings.EMPTY, "testAction", transportService, clusterService, threadPool);
    }

    @AfterClass
    public static void afterClass() {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        threadPool = null;
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

    ClusterState stateWithUnassingedShards(String index, int numberOfReplicas) {
        ShardRoutingState[] replicaStates = new ShardRoutingState[numberOfReplicas];
        for (int i = 0; i < replicaStates.length; i++) {
            replicaStates[i] = ShardRoutingState.UNASSIGNED;
        }
        return state(index, false, ShardRoutingState.UNASSIGNED, replicaStates);
    }


    ClusterState stateWithStartedPrimary(String index, boolean primaryLocal, int numberOfReplicas) {
        ShardRoutingState[] replicaStates = new ShardRoutingState[numberOfReplicas];
        int assignedReplicas = randomIntBetween(0, replicaStates.length);
        // no point in randomizing - node assignment later on does it too.
        for (int i = 0; i < assignedReplicas; i++) {
            replicaStates[i] = randomFrom(ShardRoutingState.INITIALIZING, ShardRoutingState.STARTED, ShardRoutingState.RELOCATING);
        }
        for (int i = assignedReplicas; i < replicaStates.length; i++) {
            replicaStates[i] = ShardRoutingState.UNASSIGNED;
        }
        return state(index, primaryLocal, randomFrom(ShardRoutingState.STARTED, ShardRoutingState.RELOCATING), replicaStates);

    }

    ClusterState state(String index, boolean primaryLocal, ShardRoutingState primaryState, ShardRoutingState... replicaStates) {
        final int numberOfReplicas = replicaStates.length;

        int numberOfNodes = numberOfReplicas + 1;
        if (primaryState == ShardRoutingState.RELOCATING) {
            numberOfNodes++;
        }
        for (ShardRoutingState state : replicaStates) {
            if (state == ShardRoutingState.RELOCATING) {
                numberOfNodes++;
            }
        }
        numberOfNodes = Math.max(2, numberOfNodes); // we need a non-local master to test shard failures
        final ShardId shardId = new ShardId(index, 0);
        DiscoveryNodes.Builder discoBuilder = DiscoveryNodes.builder();
        Set<String> unassignedNodes = new HashSet<>();
        for (int i = 0; i < numberOfNodes + 1; i++) {
            final DiscoveryNode node = newNode(i);
            discoBuilder = discoBuilder.put(node);
            unassignedNodes.add(node.id());
        }
        discoBuilder.localNodeId(newNode(0).id());
        discoBuilder.masterNodeId(newNode(1).id()); // we need a non-local master to test shard failures
        IndexMetaData indexMetaData = IndexMetaData.builder(index).settings(ImmutableSettings.builder()
                .put(SETTING_VERSION_CREATED, Version.CURRENT)
                .put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_NUMBER_OF_REPLICAS, numberOfReplicas)
                .put(SETTING_CREATION_DATE, System.currentTimeMillis())).build();

        RoutingTable.Builder routing = new RoutingTable.Builder();
        routing.addAsNew(indexMetaData);
        IndexShardRoutingTable.Builder indexShardRoutingBuilder = new IndexShardRoutingTable.Builder(shardId, false);

        String primaryNode = null;
        String relocatingNode = null;
        if (primaryState != ShardRoutingState.UNASSIGNED) {
            if (primaryLocal) {
                primaryNode = newNode(0).id();
                unassignedNodes.remove(primaryNode);
            } else {
                primaryNode = selectAndRemove(unassignedNodes);
            }
            if (primaryState == ShardRoutingState.RELOCATING) {
                relocatingNode = selectAndRemove(unassignedNodes);
            }
        }
        indexShardRoutingBuilder.addShard(new ImmutableShardRouting(index, 0, primaryNode, relocatingNode, true, primaryState, 0));

        for (ShardRoutingState replicaState : replicaStates) {
            String replicaNode = null;
            relocatingNode = null;
            if (replicaState != ShardRoutingState.UNASSIGNED) {
                assert primaryNode != null : "a replica is assigned but the primary isn't";
                replicaNode = selectAndRemove(unassignedNodes);
                if (replicaState == ShardRoutingState.RELOCATING) {
                    relocatingNode = selectAndRemove(unassignedNodes);
                }
            }
            indexShardRoutingBuilder.addShard(
                    new ImmutableShardRouting(index, shardId.id(), replicaNode, relocatingNode, false, replicaState, 0));

        }

        ClusterState.Builder state = ClusterState.builder(new ClusterName("test"));
        state.nodes(discoBuilder);
        state.metaData(MetaData.builder().put(indexMetaData, false).generateUuidIfNeeded());
        state.routingTable(RoutingTable.builder().add(IndexRoutingTable.builder(index).addIndexShard(indexShardRoutingBuilder.build())));
        return state.build();
    }

    private String selectAndRemove(Set<String> strings) {
        String selection = randomFrom(strings.toArray(new String[strings.size()]));
        strings.remove(selection);
        return selection;
    }

    @Test
    public void testNotStartedPrimary() throws InterruptedException, ExecutionException {
        final String index = "test";
        final ShardId shardId = new ShardId(index, 0);
        // no replicas in oder to skip the replication part
        clusterService.setState(state(index, true,
                randomBoolean() ? ShardRoutingState.INITIALIZING : ShardRoutingState.UNASSIGNED));

        logger.debug("--> using initial state:\n{}", clusterService.state().prettyPrint());

        Request request = new Request(shardId).timeout("1ms");
        PlainActionFuture<Response> listener = new PlainActionFuture<>();
        TransportShardReplicationOperationAction<Request, Request, Response>.PrimaryPhase primaryPhase = action.new PrimaryPhase(request, listener);
        primaryPhase.start();
        assertListenerThrows("unassigned primary didn't cause a timeout", listener, UnavailableShardsException.class);

        request = new Request(shardId);
        listener = new PlainActionFuture<>();
        primaryPhase = action.new PrimaryPhase(request, listener);
        primaryPhase.start();
        assertFalse("unassigned primary didn't cause a retry", listener.isDone());

        clusterService.setState(state(index, true, ShardRoutingState.STARTED));
        logger.debug("--> primary assigned state:\n{}", clusterService.state().prettyPrint());

        listener.get();
        assertTrue("request wasn't processed on primary, despite of it being assigned", request.processedOnPrimary.get());
    }

    @Test
    public void testRoutingToPrimary() {
        final String index = "test";
        final ShardId shardId = new ShardId(index, 0);

        clusterService.setState(stateWithStartedPrimary(index, randomBoolean(), 3));

        logger.debug("using state: \n{}", clusterService.state().prettyPrint());

        final IndexShardRoutingTable shardRoutingTable = clusterService.state().routingTable().index(index).shard(shardId.id());
        final String primaryNodeId = shardRoutingTable.primaryShard().currentNodeId();
        Request request = new Request(shardId);
        PlainActionFuture<Response> listener = new PlainActionFuture<>();

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

    @Test
    public void testReplication() throws ExecutionException, InterruptedException {
        final String index = "test";
        final ShardId shardId = new ShardId(index, 0);

        clusterService.setState(stateWithStartedPrimary(index, true, randomInt(5)));

        final IndexShardRoutingTable shardRoutingTable = clusterService.state().routingTable().index(index).shard(shardId.id());
        int assignedReplicas = 0;
        int totalShards = 0;
        for (ShardRouting shard : shardRoutingTable) {
            totalShards++;
            if (shard.primary() == false && shard.assignedToNode()) {
                assignedReplicas++;
            }
            if (shard.relocating()) {
                assignedReplicas++;
                totalShards++;
            }
        }

        runReplicateTest(shardRoutingTable, assignedReplicas, totalShards);
    }

    @Test
    public void testReplicationWithShadowIndex() throws ExecutionException, InterruptedException {
        final String index = "test";
        final ShardId shardId = new ShardId(index, 0);

        ClusterState state = stateWithStartedPrimary(index, true, randomInt(5));
        MetaData.Builder metaData = MetaData.builder(state.metaData());
        ImmutableSettings.Builder settings = ImmutableSettings.builder().put(metaData.get(index).settings());
        settings.put(IndexMetaData.SETTING_SHADOW_REPLICAS, true);
        metaData.put(IndexMetaData.builder(metaData.get(index)).settings(settings));
        clusterService.setState(ClusterState.builder(state).metaData(metaData));

        final IndexShardRoutingTable shardRoutingTable = clusterService.state().routingTable().index(index).shard(shardId.id());
        int assignedReplicas = 0;
        int totalShards = 0;
        for (ShardRouting shard : shardRoutingTable) {
            totalShards++;
            if (shard.primary() && shard.relocating()) {
                assignedReplicas++;
                totalShards++;
            }
        }

        runReplicateTest(shardRoutingTable, assignedReplicas, totalShards);
    }


    protected void runReplicateTest(IndexShardRoutingTable shardRoutingTable, int assignedReplicas, int totalShards) throws InterruptedException, ExecutionException {
        final ShardRouting primaryShard = shardRoutingTable.primaryShard();
        final ShardIterator shardIt = shardRoutingTable.shardsIt();
        final ShardId shardId = shardIt.shardId();
        final Request request = new Request();
        PlainActionFuture<Response> listener = new PlainActionFuture<>();

        logger.debug("expecting [{}] assigned replicas, [{}] total shards. using state: \n{}", assignedReplicas, totalShards, clusterService.state().prettyPrint());


        final TransportShardReplicationOperationAction<Request, Request, Response>.InternalRequest internalRequest = action.new InternalRequest(request);
        internalRequest.concreteIndex(shardId.index().name());
        TransportShardReplicationOperationAction<Request, Request, Response>.ReplicationPhase replicationPhase =
                action.new ReplicationPhase(shardIt, request,
                        new Response(), new ClusterStateObserver(clusterService, logger),
                        primaryShard, internalRequest, listener);

        assertThat(replicationPhase.totalShards(), equalTo(totalShards));
        assertThat(replicationPhase.pending(), equalTo(assignedReplicas));
        replicationPhase.start();
        final CapturingTransport.CapturedRequest[] capturedRequests = transport.capturedRequests();
        transport.clear();
        assertThat(capturedRequests.length, equalTo(assignedReplicas));
        if (assignedReplicas > 0) {
            assertThat("listener is done, but there are outstanding replicas", listener.isDone(), equalTo(false));
        }
        int pending = replicationPhase.pending();
        int criticalFailures = 0; // failures that should fail the shard
        int successfull = 1;
        for (CapturingTransport.CapturedRequest capturedRequest : capturedRequests) {
            if (randomBoolean()) {
                Throwable t;
                if (randomBoolean()) {
                    t = new CorruptIndexException("simulated", (String) null);
                    criticalFailures++;
                } else {
                    t = new IndexShardNotStartedException(shardId, IndexShardState.RECOVERING);
                }
                logger.debug("--> simulating failure on {} with [{}]", capturedRequest.node, t.getClass().getSimpleName());
                transport.handleResponse(capturedRequest.requestId, t);
            } else {
                successfull++;
                transport.handleResponse(capturedRequest.requestId, TransportResponse.Empty.INSTANCE);
            }
            pending--;
            assertThat(replicationPhase.pending(), equalTo(pending));
            assertThat(replicationPhase.successful(), equalTo(successfull));
        }
        assertThat(listener.isDone(), equalTo(true));
        Response response = listener.get();
        final ActionWriteResponse.ShardInfo shardInfo = response.getShardInfo();
        assertThat(shardInfo.getFailed(), equalTo(criticalFailures));
        assertThat(shardInfo.getFailures(), arrayWithSize(criticalFailures));
        assertThat(shardInfo.getSuccessful(), equalTo(successfull));
        assertThat(shardInfo.getTotal(), equalTo(totalShards));

        assertThat("failed to see enough shard failures", transport.capturedRequests().length, equalTo(criticalFailures));
        for (CapturingTransport.CapturedRequest capturedRequest : transport.capturedRequests()) {
            assertThat(capturedRequest.action, equalTo(ShardStateAction.SHARD_FAILED_ACTION_NAME));
        }
    }



    static class Request extends ShardReplicationOperationRequest<Request> {
        int shardId;
        public AtomicBoolean processedOnPrimary = new AtomicBoolean();
        public AtomicInteger processedOnReplicas = new AtomicInteger();

        Request() {
            this.operationThreaded(false);
        }

        Request(ShardId shardId) {
            this();
            this.shardId = shardId.id();
            this.index(shardId.index().name());
            // keep things simple
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


}
