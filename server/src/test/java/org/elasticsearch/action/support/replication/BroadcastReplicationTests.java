/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.support.replication;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.UnavailableShardsException;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.flush.FlushResponse;
import org.elasticsearch.action.admin.indices.flush.TransportFlushAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.broadcast.BaseBroadcastResponse;
import org.elasticsearch.action.support.broadcast.BroadcastRequest;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.netty4.Netty4Transport;
import org.elasticsearch.transport.netty4.SharedGroupFactory;
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
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.action.support.replication.ClusterStateCreationUtils.state;
import static org.elasticsearch.action.support.replication.ClusterStateCreationUtils.stateWithAssignedPrimariesAndOneReplica;
import static org.elasticsearch.action.support.replication.ClusterStateCreationUtils.stateWithNoShard;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_VERSION_CREATED;
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
    private static final int MAX_REQUESTS_PER_NODE = between(1, 10);

    @BeforeClass
    public static void beforeClass() {
        threadPool = new TestThreadPool("BroadcastReplicationTests");
        circuitBreakerService = new NoneCircuitBreakerService();
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        TcpTransport transport = new Netty4Transport(
            Settings.EMPTY,
            Version.CURRENT,
            threadPool,
            new NetworkService(Collections.emptyList()),
            PageCacheRecycler.NON_RECYCLING_INSTANCE,
            new NamedWriteableRegistry(Collections.emptyList()),
            circuitBreakerService,
            new SharedGroupFactory(Settings.EMPTY)
        );
        clusterService = createClusterService(threadPool);
        transportService = new TransportService(
            clusterService.getSettings(),
            transport,
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> clusterService.localNode(),
            null,
            Collections.emptySet()
        );
        transportService.start();
        transportService.acceptIncomingRequests();
        broadcastReplicationAction = new TestBroadcastReplicationAction(
            clusterService,
            transportService,
            new ActionFilters(new HashSet<>()),
            TestIndexNameExpressionResolver.newInstance(threadPool.getThreadContext())
        );
    }

    @Override
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

    public void testNotStartedPrimary() throws InterruptedException, ExecutionException {
        final String index = "test";
        setState(
            clusterService,
            state(
                index,
                randomBoolean(),
                randomBoolean() ? ShardRoutingState.INITIALIZING : ShardRoutingState.UNASSIGNED,
                ShardRoutingState.UNASSIGNED
            )
        );
        logger.debug("--> using initial state:\n{}", clusterService.state());
        PlainActionFuture<BaseBroadcastResponse> response = PlainActionFuture.newFuture();
        ActionTestUtils.execute(broadcastReplicationAction, null, new DummyBroadcastRequest(index), response);
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

    public void testStartedPrimary() throws InterruptedException, ExecutionException {
        final String index = "test";
        setState(clusterService, state(index, randomBoolean(), ShardRoutingState.STARTED));
        logger.debug("--> using initial state:\n{}", clusterService.state());
        PlainActionFuture<BaseBroadcastResponse> response = PlainActionFuture.newFuture();
        ActionTestUtils.execute(broadcastReplicationAction, null, new DummyBroadcastRequest(index), response);
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
        PlainActionFuture<BaseBroadcastResponse> response = PlainActionFuture.newFuture();
        ActionTestUtils.execute(broadcastReplicationAction, null, new DummyBroadcastRequest().indices(index), response);
        int succeeded = 0;
        int failed = 0;
        for (Tuple<ShardId, ActionListener<ReplicationResponse>> shardRequests : broadcastReplicationAction.capturedShardRequests) {
            if (randomBoolean()) {
                ReplicationResponse.ShardInfo.Failure[] failures = new ReplicationResponse.ShardInfo.Failure[0];
                int shardsSucceeded = randomInt(1) + 1;
                succeeded += shardsSucceeded;
                ReplicationResponse replicationResponse = new ReplicationResponse();
                if (shardsSucceeded == 1 && randomBoolean()) {
                    // sometimes add failure (no failure means shard unavailable)
                    failures = new ReplicationResponse.ShardInfo.Failure[1];
                    failures[0] = new ReplicationResponse.ShardInfo.Failure(
                        shardRequests.v1(),
                        null,
                        new Exception("pretend shard failed"),
                        RestStatus.GATEWAY_TIMEOUT,
                        false
                    );
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

    public void testNoShards() {
        setState(clusterService, stateWithNoShard());
        logger.debug("--> using initial state:\n{}", clusterService.state());
        BaseBroadcastResponse response = executeAndAssertImmediateResponse(broadcastReplicationAction, new DummyBroadcastRequest());
        assertBroadcastResponse(0, 0, 0, response, null);
    }

    public void testShardsIteratorOneShard() {
        final String index = "test";
        final ShardId shardId = new ShardId(index, "_na_", 0);
        ClusterState clusterState = state(
            index,
            randomBoolean(),
            randomBoolean() ? ShardRoutingState.INITIALIZING : ShardRoutingState.UNASSIGNED,
            ShardRoutingState.UNASSIGNED
        );
        logger.debug("--> using initial state:\n{}", clusterService.state());
        var shards = broadcastReplicationAction.shardIds(new DummyBroadcastRequest().indices(shardId.getIndexName()), clusterState);
        assertTrue(shards.hasNext());
        assertEquals(shardId, shards.next());
        assertFalse(shards.hasNext());
    }

    public void testShardsIterator() {
        final var metadataBuilder = Metadata.builder();
        final var indexCount = between(1, 5);
        for (int i = 0; i < indexCount; i++) {
            metadataBuilder.put(
                new IndexMetadata.Builder("index-" + i).settings(
                    Settings.builder()
                        .put(SETTING_VERSION_CREATED, Version.CURRENT)
                        .put(SETTING_NUMBER_OF_SHARDS, between(1, 3))
                        .put(SETTING_NUMBER_OF_REPLICAS, between(0, 2))
                )
            );
        }
        final var clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadataBuilder).build();
        final var allIndexNames = clusterState.metadata().indices().keySet();
        final var indexNames = randomSubsetOf(between(1, indexCount), allIndexNames).toArray(Strings.EMPTY_ARRAY);

        final var expectedShards = new HashSet<ShardId>();
        for (final var indexName : indexNames) {
            final var indexMetadata = clusterState.metadata().index(indexName);
            for (int i = 0; i < indexMetadata.getNumberOfShards(); i++) {
                expectedShards.add(new ShardId(indexMetadata.getIndex(), i));
            }
        }

        final var actualShards = new HashSet<ShardId>();
        final var iterator = broadcastReplicationAction.shardIds(new DummyBroadcastRequest().indices(indexNames), clusterState);
        while (iterator.hasNext()) {
            actualShards.add(iterator.next());
        }

        assertEquals(expectedShards, actualShards);
    }

    public void testThrottling() {
        final var shardCount = between(MAX_REQUESTS_PER_NODE, MAX_REQUESTS_PER_NODE * 5);
        final var replicaCount = between(0, 2);
        setState(
            clusterService,
            ClusterState.builder(ClusterName.DEFAULT)
                .metadata(
                    Metadata.builder()
                        .put(
                            new IndexMetadata.Builder("test").settings(
                                Settings.builder()
                                    .put(SETTING_VERSION_CREATED, Version.CURRENT)
                                    .put(SETTING_NUMBER_OF_SHARDS, shardCount)
                                    .put(SETTING_NUMBER_OF_REPLICAS, replicaCount)
                            )
                        )
                )
                .nodes(DiscoveryNodes.builder().add(new DiscoveryNode("test", buildNewFakeTransportAddress(), Version.CURRENT)))
                .build()
        );

        PlainActionFuture<BaseBroadcastResponse> future = PlainActionFuture.newFuture();
        ActionTestUtils.execute(broadcastReplicationAction, null, new DummyBroadcastRequest().indices("test"), future);

        final var maxOutstandingRequests = clusterService.state().nodes().getDataNodes().size() * MAX_REQUESTS_PER_NODE;
        assertThat(broadcastReplicationAction.capturedShardRequests.size(), equalTo(maxOutstandingRequests));
        assertFalse(future.isDone());
        final boolean[] handled = new boolean[clusterService.state().metadata().index("test").getNumberOfShards()];

        var successes = 0;
        while (broadcastReplicationAction.capturedShardRequests.isEmpty() == false) {
            assertThat(broadcastReplicationAction.capturedShardRequests.size(), lessThanOrEqualTo(maxOutstandingRequests));
            final var request = randomFrom(broadcastReplicationAction.capturedShardRequests);
            assertTrue(broadcastReplicationAction.capturedShardRequests.remove(request));
            assertFalse(handled[request.v1().id()]);
            handled[request.v1().id()] = true;
            if (randomBoolean()) {
                successes += 1;
                ReplicationResponse replicationResponse = new ReplicationResponse();
                replicationResponse.setShardInfo(new ReplicationResponse.ShardInfo(replicaCount + 1, replicaCount + 1));
                request.v2().onResponse(replicationResponse);
            } else {
                request.v2().onFailure(new ElasticsearchException("unexpected"));
            }
        }

        var response = future.actionGet(10, TimeUnit.SECONDS);
        assertEquals(shardCount * (replicaCount + 1), response.getTotalShards());
        assertEquals(successes * (replicaCount + 1), response.getSuccessfulShards());
        assertEquals((shardCount - successes) * (replicaCount + 1), response.getFailedShards());
    }

    private class TestBroadcastReplicationAction extends TransportBroadcastReplicationAction<
        DummyBroadcastRequest,
        BaseBroadcastResponse,
        BasicReplicationRequest,
        ReplicationResponse> {
        protected final Set<Tuple<ShardId, ActionListener<ReplicationResponse>>> capturedShardRequests = ConcurrentCollections
            .newConcurrentSet();

        TestBroadcastReplicationAction(
            ClusterService clusterService,
            TransportService transportService,
            ActionFilters actionFilters,
            IndexNameExpressionResolver indexNameExpressionResolver
        ) {
            super(
                "internal:test-broadcast-replication-action",
                DummyBroadcastRequest::new,
                clusterService,
                transportService,
                null,
                actionFilters,
                indexNameExpressionResolver,
                null,
                ThreadPool.Names.SAME,
                MAX_REQUESTS_PER_NODE
            );
        }

        @Override
        protected BasicReplicationRequest newShardRequest(DummyBroadcastRequest request, ShardId shardId) {
            return new BasicReplicationRequest(shardId);
        }

        @Override
        protected BaseBroadcastResponse newResponse(
            int successfulShards,
            int failedShards,
            int totalNumCopies,
            List<DefaultShardOperationFailedException> shardFailures
        ) {
            return new BaseBroadcastResponse(totalNumCopies, successfulShards, failedShards, shardFailures);
        }

        @Override
        protected void shardExecute(
            Task task,
            DummyBroadcastRequest request,
            ShardId shardId,
            ActionListener<ReplicationResponse> shardActionListener
        ) {
            capturedShardRequests.add(new Tuple<>(shardId, shardActionListener));
        }
    }

    public FlushResponse assertImmediateResponse(String index, TransportFlushAction flushAction) {
        Date beginDate = new Date();
        FlushResponse flushResponse = ActionTestUtils.executeBlocking(flushAction, new FlushRequest(index));
        Date endDate = new Date();
        long maxTime = 500;
        assertThat(
            "this should not take longer than " + maxTime + " ms. The request hangs somewhere",
            endDate.getTime() - beginDate.getTime(),
            lessThanOrEqualTo(maxTime)
        );
        return flushResponse;
    }

    public BaseBroadcastResponse executeAndAssertImmediateResponse(
        TransportBroadcastReplicationAction<DummyBroadcastRequest, BaseBroadcastResponse, ?, ?> broadcastAction,
        DummyBroadcastRequest request
    ) {
        PlainActionFuture<BaseBroadcastResponse> response = PlainActionFuture.newFuture();
        ActionTestUtils.execute(broadcastAction, null, request, response);
        return response.actionGet("5s");
    }

    private void assertBroadcastResponse(int total, int successful, int failed, BaseBroadcastResponse response, Class<?> exceptionClass) {
        assertThat(response.getSuccessfulShards(), equalTo(successful));
        assertThat(response.getTotalShards(), equalTo(total));
        assertThat(response.getFailedShards(), equalTo(failed));
        for (int i = 0; i < failed; i++) {
            assertThat(response.getShardFailures()[0].getCause().getCause(), instanceOf(exceptionClass));
        }
    }

    public static class DummyBroadcastRequest extends BroadcastRequest<DummyBroadcastRequest> {
        DummyBroadcastRequest(String... indices) {
            super(indices);
        }

        DummyBroadcastRequest(StreamInput in) throws IOException {
            super(in);
        }
    }
}
