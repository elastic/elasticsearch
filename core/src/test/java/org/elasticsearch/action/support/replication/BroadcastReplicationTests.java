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

import org.apache.lucene.util.IOUtils;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.UnavailableShardsException;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.flush.FlushResponse;
import org.elasticsearch.action.admin.indices.flush.TransportFlushAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.broadcast.BroadcastRequest;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.MockTcpTransport;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.action.support.replication.ClusterStateCreationUtils.state;
import static org.elasticsearch.action.support.replication.ClusterStateCreationUtils.stateWithAssignedPrimariesAndOneReplica;
import static org.elasticsearch.action.support.replication.ClusterStateCreationUtils.stateWithNoShard;
import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.elasticsearch.test.ClusterServiceUtils.setState;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class BroadcastReplicationTests extends ESTestCase {

    private static ThreadPool threadPool;
    private static CircuitBreakerService circuitBreakerService;
    private ClusterService clusterService;
    private TransportService transportService;
    private TestBroadcastReplicationAction broadcastReplicationAction;

    @BeforeClass
    public static void beforeClass() {
        threadPool = new TestThreadPool("BroadcastReplicationTests");
        circuitBreakerService = new NoneCircuitBreakerService();
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        MockTcpTransport transport = new MockTcpTransport(Settings.EMPTY,
            threadPool, BigArrays.NON_RECYCLING_INSTANCE, circuitBreakerService, new NamedWriteableRegistry(Collections.emptyList()),
            new NetworkService(Collections.emptyList()));
        clusterService = createClusterService(threadPool);
        transportService = new TransportService(clusterService.getSettings(), transport, threadPool,
                TransportService.NOOP_TRANSPORT_INTERCEPTOR, x -> clusterService.localNode(), null);
        transportService.start();
        transportService.acceptIncomingRequests();
        broadcastReplicationAction = new TestBroadcastReplicationAction(Settings.EMPTY, threadPool, clusterService, transportService,
                new ActionFilters(new HashSet<>()), new IndexNameExpressionResolver(Settings.EMPTY), null);
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        IOUtils.close(clusterService, transportService);
    }

    @AfterClass
    public static void afterClass() {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        threadPool = null;
    }

    public void testNotStartedPrimary() throws InterruptedException, ExecutionException, IOException {
        final String index = "test";
        setState(clusterService, state(index, randomBoolean(),
                randomBoolean() ? ShardRoutingState.INITIALIZING : ShardRoutingState.UNASSIGNED, ShardRoutingState.UNASSIGNED));
        logger.debug("--> using initial state:\n{}", clusterService.state());
        Future<BroadcastResponse> response = (broadcastReplicationAction.execute(new DummyBroadcastRequest().indices(index)));
        for (Tuple<ShardId, ActionListener<ReplicationResponse>> shardRequests : broadcastReplicationAction.capturedShardRequests) {
            if (randomBoolean()) {
                shardRequests.v2().onFailure(new NoShardAvailableActionException(shardRequests.v1()));
            } else {
                shardRequests.v2().onFailure(new UnavailableShardsException(shardRequests.v1(), "test exception"));
            }
        }
        response.get();
        logger.info("total shards: {}, ", response.get().getTotalShards());
        // we expect no failures here because UnavailableShardsException does not count as failed
        assertBroadcastResponse(2, 0, 0, response.get(), null);
    }

    public void testStartedPrimary() throws InterruptedException, ExecutionException, IOException {
        final String index = "test";
        setState(clusterService, state(index, randomBoolean(),
                ShardRoutingState.STARTED));
        logger.debug("--> using initial state:\n{}", clusterService.state());
        Future<BroadcastResponse> response = (broadcastReplicationAction.execute(new DummyBroadcastRequest().indices(index)));
        for (Tuple<ShardId, ActionListener<ReplicationResponse>> shardRequests : broadcastReplicationAction.capturedShardRequests) {
            ReplicationResponse replicationResponse = new ReplicationResponse();
            replicationResponse.setShardInfo(new ReplicationResponse.ShardInfo(1, 1));
            shardRequests.v2().onResponse(replicationResponse);
        }
        logger.info("total shards: {}, ", response.get().getTotalShards());
        assertBroadcastResponse(1, 1, 0, response.get(), null);
    }

    public void testResultCombine() throws InterruptedException, ExecutionException, IOException {
        final String index = "test";
        int numShards = 1 + randomInt(3);
        setState(clusterService, stateWithAssignedPrimariesAndOneReplica(index, numShards));
        logger.debug("--> using initial state:\n{}", clusterService.state());
        Future<BroadcastResponse> response = (broadcastReplicationAction.execute(new DummyBroadcastRequest().indices(index)));
        int succeeded = 0;
        int failed = 0;
        for (Tuple<ShardId, ActionListener<ReplicationResponse>> shardRequests : broadcastReplicationAction.capturedShardRequests) {
            if (randomBoolean()) {
                ReplicationResponse.ShardInfo.Failure[] failures = new ReplicationResponse.ShardInfo.Failure[0];
                int shardsSucceeded = randomInt(1) + 1;
                succeeded += shardsSucceeded;
                ReplicationResponse replicationResponse = new ReplicationResponse();
                if (shardsSucceeded == 1 && randomBoolean()) {
                    //sometimes add failure (no failure means shard unavailable)
                    failures = new ReplicationResponse.ShardInfo.Failure[1];
                    failures[0] = new ReplicationResponse.ShardInfo.Failure(shardRequests.v1(), null, new Exception("pretend shard failed"), RestStatus.GATEWAY_TIMEOUT, false);
                    failed++;
                }
                replicationResponse.setShardInfo(new ReplicationResponse.ShardInfo(2, shardsSucceeded, failures));
                shardRequests.v2().onResponse(replicationResponse);
            } else {
                // sometimes fail
                failed += 2;
                // just add a general exception and see if failed shards will be incremented by 2
                shardRequests.v2().onFailure(new Exception("pretend shard failed"));
            }
        }
        assertBroadcastResponse(2 * numShards, succeeded, failed, response.get(), Exception.class);
    }

    public void testNoShards() throws InterruptedException, ExecutionException, IOException {
        setState(clusterService, stateWithNoShard());
        logger.debug("--> using initial state:\n{}", clusterService.state());
        BroadcastResponse response = executeAndAssertImmediateResponse(broadcastReplicationAction, new DummyBroadcastRequest());
        assertBroadcastResponse(0, 0, 0, response, null);
    }

    public void testShardsList() throws InterruptedException, ExecutionException {
        final String index = "test";
        final ShardId shardId = new ShardId(index, "_na_", 0);
        ClusterState clusterState = state(index, randomBoolean(),
                randomBoolean() ? ShardRoutingState.INITIALIZING : ShardRoutingState.UNASSIGNED, ShardRoutingState.UNASSIGNED);
        logger.debug("--> using initial state:\n{}", clusterService.state());
        List<ShardId> shards = broadcastReplicationAction.shards(new DummyBroadcastRequest().indices(shardId.getIndexName()), clusterState);
        assertThat(shards.size(), equalTo(1));
        assertThat(shards.get(0), equalTo(shardId));
    }

    private class TestBroadcastReplicationAction extends TransportBroadcastReplicationAction<DummyBroadcastRequest, BroadcastResponse, BasicReplicationRequest, ReplicationResponse> {
        protected final Set<Tuple<ShardId, ActionListener<ReplicationResponse>>> capturedShardRequests = ConcurrentCollections.newConcurrentSet();

        TestBroadcastReplicationAction(Settings settings, ThreadPool threadPool, ClusterService clusterService,
                                              TransportService transportService, ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                              TransportReplicationAction replicatedBroadcastShardAction) {
            super("test-broadcast-replication-action", DummyBroadcastRequest::new, settings, threadPool, clusterService, transportService,
                    actionFilters, indexNameExpressionResolver, replicatedBroadcastShardAction);
        }

        @Override
        protected ReplicationResponse newShardResponse() {
            return new ReplicationResponse();
        }

        @Override
        protected BasicReplicationRequest newShardRequest(DummyBroadcastRequest request, ShardId shardId) {
            return new BasicReplicationRequest().setShardId(shardId);
        }

        @Override
        protected BroadcastResponse newResponse(int successfulShards, int failedShards, int totalNumCopies,
                                                List<ShardOperationFailedException> shardFailures) {
            return new BroadcastResponse(totalNumCopies, successfulShards, failedShards, shardFailures);
        }

        @Override
        protected void shardExecute(Task task, DummyBroadcastRequest request, ShardId shardId, ActionListener<ReplicationResponse> shardActionListener) {
            capturedShardRequests.add(new Tuple<>(shardId, shardActionListener));
        }
    }

    public FlushResponse assertImmediateResponse(String index, TransportFlushAction flushAction) throws InterruptedException, ExecutionException {
        Date beginDate = new Date();
        FlushResponse flushResponse = flushAction.execute(new FlushRequest(index)).get();
        Date endDate = new Date();
        long maxTime = 500;
        assertThat("this should not take longer than " + maxTime + " ms. The request hangs somewhere", endDate.getTime() - beginDate.getTime(), lessThanOrEqualTo(maxTime));
        return flushResponse;
    }

    public BroadcastResponse executeAndAssertImmediateResponse(TransportBroadcastReplicationAction broadcastAction, DummyBroadcastRequest request) throws InterruptedException, ExecutionException {
        return (BroadcastResponse) broadcastAction.execute(request).actionGet("5s");
    }

    private void assertBroadcastResponse(int total, int successful, int failed, BroadcastResponse response, Class exceptionClass) {
        assertThat(response.getSuccessfulShards(), equalTo(successful));
        assertThat(response.getTotalShards(), equalTo(total));
        assertThat(response.getFailedShards(), equalTo(failed));
        for (int i = 0; i < failed; i++) {
            assertThat(response.getShardFailures()[0].getCause().getCause(), instanceOf(exceptionClass));
        }
    }

    public static class DummyBroadcastRequest extends BroadcastRequest<DummyBroadcastRequest> {

    }
}
