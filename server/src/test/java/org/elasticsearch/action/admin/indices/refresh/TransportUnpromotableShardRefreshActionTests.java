/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.refresh;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportUnpromotableShardRefreshActionTests extends ESTestCase {

    private ThreadPool threadPool;
    private ClusterService clusterService;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool("TransportUnpromotableShardRefreshActionTests");
        clusterService = ClusterServiceUtils.createClusterService(threadPool);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        clusterService.close();
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
    }

    public void testWaitForClusterState() {
        final TransportService transportService = mock(TransportService.class);
        when(transportService.getThreadPool()).thenReturn(threadPool);

        final AtomicReference<UnpromotableShardRefreshRequest> capturedRequest = new AtomicReference<>();
        final AtomicReference<Thread> capturedThread = new AtomicReference<>();
        final var action = new TransportUnpromotableShardRefreshAction(
            clusterService,
            transportService,
            mock(ShardStateAction.class),
            mock(ActionFilters.class),
            mock(IndicesService.class)
        ) {
            @Override
            void doUnpromotableShardOperation(
                UnpromotableShardRefreshRequest request,
                ActionListener<ActionResponse.Empty> responseListener
            ) {
                capturedRequest.set(request);
                capturedThread.set(Thread.currentThread());
                responseListener.onResponse(ActionResponse.Empty.INSTANCE);
            }
        };

        final Thread callerThread = Thread.currentThread();
        final long initialStateVersion = clusterService.state().version();

        // Request with known state version
        final var requestOfInitialState = createUnpromotableShardRefreshRequest(initialStateVersion);
        final PlainActionFuture<ActionResponse.Empty> futureOfInitialState = new PlainActionFuture<>();
        action.unpromotableShardOperation(mock(Task.class), requestOfInitialState, futureOfInitialState);
        safeGet(futureOfInitialState);
        assertThat(capturedRequest.get(), is(requestOfInitialState));
        assertThat(capturedThread.get(), is(callerThread));

        // Another request with a future state version
        capturedRequest.set(null);
        capturedThread.set(null);
        final long futureStateVersion = initialStateVersion + randomLongBetween(1, 100);
        final var requestOfFutureState = createUnpromotableShardRefreshRequest(futureStateVersion);
        final PlainActionFuture<ActionResponse.Empty> futureOfFutureState = new PlainActionFuture<>();
        action.unpromotableShardOperation(mock(Task.class), requestOfFutureState, futureOfFutureState);
        assertThat(futureOfFutureState.isDone(), is(false));
        assertThat(capturedRequest.get(), nullValue());

        ClusterServiceUtils.setState(
            clusterService,
            ClusterState.builder(clusterService.state()).version(randomLongBetween(initialStateVersion, futureStateVersion - 1L))
        );
        assertThat(futureOfFutureState.isDone(), is(false));
        assertThat(capturedRequest.get(), nullValue());

        ClusterServiceUtils.setState(clusterService, ClusterState.builder(clusterService.state()).version(futureStateVersion));
        safeGet(futureOfFutureState);
        assertThat(capturedRequest.get(), is(requestOfFutureState));
        assertThat(EsExecutors.executorName(capturedThread.get()), equalTo("refresh"));
    }

    private static UnpromotableShardRefreshRequest createUnpromotableShardRefreshRequest(long initialStateVersion) {
        final var shardId = new ShardId(new Index(randomIdentifier(), randomUUID()), between(0, 3));
        final var shardRouting = TestShardRouting.newShardRouting(shardId, randomUUID(), true, ShardRoutingState.STARTED);
        final var builder = new IndexShardRoutingTable.Builder(shardId);
        builder.addShard(shardRouting);

        return new UnpromotableShardRefreshRequest(
            builder.build(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomBoolean(),
            initialStateVersion
        );
    }
}
