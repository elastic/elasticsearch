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

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionWriteResponse;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.UnavailableShardsException;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.flush.FlushResponse;
import org.elasticsearch.action.admin.indices.flush.TransportFlushAction;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.broadcast.BroadcastRequest;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.cluster.TestClusterService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.local.LocalTransport;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.action.support.replication.ClusterStateCreationUtils.*;
import static org.hamcrest.Matchers.*;

public class BroadcastReplicationTests extends ESTestCase {

    private static ThreadPool threadPool;
    private TestClusterService clusterService;
    private TransportService transportService;
    private LocalTransport transport;
    private TestBroadcastReplicationAction broadcastReplicationAction;

    @BeforeClass
    public static void beforeClass() {
        threadPool = new ThreadPool("BroadcastReplicationTests");
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        transport = new LocalTransport(Settings.EMPTY, threadPool, Version.CURRENT, new NamedWriteableRegistry());
        clusterService = new TestClusterService(threadPool);
        transportService = new TransportService(transport, threadPool);
        transportService.start();
        broadcastReplicationAction = new TestBroadcastReplicationAction(Settings.EMPTY, threadPool, clusterService, transportService, new ActionFilters(new HashSet<ActionFilter>()), new IndexNameExpressionResolver(Settings.EMPTY), null);
    }

    @AfterClass
    public static void afterClass() {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        threadPool = null;
    }

    @Test
    public void testNotStartedPrimary() throws InterruptedException, ExecutionException, IOException {
        final String index = "test";
        clusterService.setState(state(index, randomBoolean(),
                randomBoolean() ? ShardRoutingState.INITIALIZING : ShardRoutingState.UNASSIGNED, ShardRoutingState.UNASSIGNED));
        logger.debug("--> using initial state:\n{}", clusterService.state().prettyPrint());
        Future<BroadcastResponse> response = (broadcastReplicationAction.execute(new BroadcastRequest().indices(index)));
        for (Tuple<ShardId, ActionListener<ActionWriteResponse>> shardRequests : broadcastReplicationAction.capturedShardRequests) {
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

    @Test
    public void testStartedPrimary() throws InterruptedException, ExecutionException, IOException {
        final String index = "test";
        clusterService.setState(state(index, randomBoolean(),
                ShardRoutingState.STARTED));
        logger.debug("--> using initial state:\n{}", clusterService.state().prettyPrint());
        Future<BroadcastResponse> response = (broadcastReplicationAction.execute(new BroadcastRequest().indices(index)));
        for (Tuple<ShardId, ActionListener<ActionWriteResponse>> shardRequests : broadcastReplicationAction.capturedShardRequests) {
            ActionWriteResponse actionWriteResponse = new ActionWriteResponse();
            actionWriteResponse.setShardInfo(new ActionWriteResponse.ShardInfo(1, 1, new ActionWriteResponse.ShardInfo.Failure[0]));
            shardRequests.v2().onResponse(actionWriteResponse);
        }
        logger.info("total shards: {}, ", response.get().getTotalShards());
        assertBroadcastResponse(1, 1, 0, response.get(), null);
    }

    @Test
    public void testResultCombine() throws InterruptedException, ExecutionException, IOException {
        final String index = "test";
        int numShards = randomInt(3);
        clusterService.setState(stateWithAssignedPrimariesAndOneReplica(index, numShards));
        logger.debug("--> using initial state:\n{}", clusterService.state().prettyPrint());
        Future<BroadcastResponse> response = (broadcastReplicationAction.execute(new BroadcastRequest().indices(index)));
        int succeeded = 0;
        int failed = 0;
        for (Tuple<ShardId, ActionListener<ActionWriteResponse>> shardRequests : broadcastReplicationAction.capturedShardRequests) {
            if (randomBoolean()) {
                ActionWriteResponse.ShardInfo.Failure[] failures = new ActionWriteResponse.ShardInfo.Failure[0];
                int shardsSucceeded = randomInt(1) + 1;
                succeeded += shardsSucceeded;
                ActionWriteResponse actionWriteResponse = new ActionWriteResponse();
                if (shardsSucceeded == 1 && randomBoolean()) {
                    //sometimes add failure (no failure means shard unavailable)
                    failures = new ActionWriteResponse.ShardInfo.Failure[1];
                    failures[0] = new ActionWriteResponse.ShardInfo.Failure(index, shardRequests.v1().id(), null, new Exception("pretend shard failed"), RestStatus.GATEWAY_TIMEOUT, false);
                    failed++;
                }
                actionWriteResponse.setShardInfo(new ActionWriteResponse.ShardInfo(2, shardsSucceeded, failures));
                shardRequests.v2().onResponse(actionWriteResponse);
            } else {
                // sometimes fail
                failed += 2;
                // just add a general exception and see if failed shards will be incremented by 2
                shardRequests.v2().onFailure(new Exception("pretend shard failed"));
            }
        }
        assertBroadcastResponse(2 * numShards, succeeded, failed, response.get(), Exception.class);
    }

    @Test
    public void testNoShards() throws InterruptedException, ExecutionException, IOException {
        clusterService.setState(stateWithNoShard());
        logger.debug("--> using initial state:\n{}", clusterService.state().prettyPrint());
        BroadcastResponse response = executeAndAssertImmediateResponse(broadcastReplicationAction, new BroadcastRequest());
        assertBroadcastResponse(0, 0, 0, response, null);
    }

    @Test
    public void testShardsList() throws InterruptedException, ExecutionException {
        final String index = "test";
        final ShardId shardId = new ShardId(index, 0);
        ClusterState clusterState = state(index, randomBoolean(),
                randomBoolean() ? ShardRoutingState.INITIALIZING : ShardRoutingState.UNASSIGNED, ShardRoutingState.UNASSIGNED);
        logger.debug("--> using initial state:\n{}", clusterService.state().prettyPrint());
        List<ShardId> shards = broadcastReplicationAction.shards(new BroadcastRequest().indices(shardId.index().name()), clusterState);
        assertThat(shards.size(), equalTo(1));
        assertThat(shards.get(0), equalTo(shardId));
    }

    private class TestBroadcastReplicationAction extends TransportBroadcastReplicationAction<BroadcastRequest, BroadcastResponse, ReplicationRequest, ActionWriteResponse> {
        protected final Set<Tuple<ShardId, ActionListener<ActionWriteResponse>>> capturedShardRequests = ConcurrentCollections.newConcurrentSet();

        public TestBroadcastReplicationAction(Settings settings, ThreadPool threadPool, ClusterService clusterService, TransportService transportService, ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver, TransportReplicationAction replicatedBroadcastShardAction) {
            super("test-broadcast-replication-action", BroadcastRequest::new, settings, threadPool, clusterService, transportService, actionFilters, indexNameExpressionResolver, replicatedBroadcastShardAction);
        }

        @Override
        protected ActionWriteResponse newShardResponse() {
            return new ActionWriteResponse();
        }

        @Override
        protected ReplicationRequest newShardRequest(BroadcastRequest request, ShardId shardId) {
            return new ReplicationRequest().setShardId(shardId);
        }

        @Override
        protected BroadcastResponse newResponse(int successfulShards, int failedShards, int totalNumCopies, List shardFailures) {
            return new BroadcastResponse(totalNumCopies, successfulShards, failedShards, shardFailures);
        }

        @Override
        protected void shardExecute(BroadcastRequest request, ShardId shardId, ActionListener<ActionWriteResponse> shardActionListener) {
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

    public BroadcastResponse executeAndAssertImmediateResponse(TransportBroadcastReplicationAction broadcastAction, BroadcastRequest request) throws InterruptedException, ExecutionException {
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
}
