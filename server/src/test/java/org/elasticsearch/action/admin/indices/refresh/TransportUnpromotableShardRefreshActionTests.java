/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.refresh;

import org.elasticsearch.exception.ElasticsearchTimeoutException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransport;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static java.util.Collections.emptySet;
import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.elasticsearch.test.ClusterServiceUtils.setState;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportUnpromotableShardRefreshActionTests extends ESTestCase {
    private ThreadPool threadPool;
    private ClusterService clusterService;
    private TransportService transportService;
    private DiscoveryNode localNode;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool("TransportUnpromotableShardRefreshActionTests");
        localNode = DiscoveryNodeUtils.create("local");
        clusterService = createClusterService(threadPool, localNode);
        final MockTransport transport = new MockTransport();
        transportService = transport.createTransportService(
            Settings.EMPTY,
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            boundTransportAddress -> localNode,
            null,
            emptySet()
        );

        transportService.start();
        transportService.acceptIncomingRequests();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        clusterService.close();
        transportService.stop();
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
    }

    public void testRespondOKToRefreshRequestBeforeShardIsCreated() {
        final var shardId = new ShardId(new Index(randomIdentifier(), randomUUID()), between(0, 3));
        final var indexShardRoutingTable = createShardRoutingTableWithPrimaryAndSearchShards(shardId, true);

        final var request = new UnpromotableShardRefreshRequest(
            indexShardRoutingTable,
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomBoolean()
        );

        final IndicesService indicesService = mock(IndicesService.class);
        if (randomBoolean()) {
            when(indicesService.indexService(shardId.getIndex())).thenReturn(null);
        } else {
            final IndexService indexService = mock(IndexService.class);
            when(indicesService.indexService(shardId.getIndex())).thenReturn(indexService);
            when(indexService.hasShard(shardId.id())).thenReturn(false);
        }

        // Register the action
        new TransportUnpromotableShardRefreshAction(
            clusterService,
            transportService,
            mock(ShardStateAction.class),
            new ActionFilters(Set.of()),
            indicesService,
            mock(ThreadPool.class)
        );

        final PlainActionFuture<ActionResponse.Empty> future = new PlainActionFuture<>();
        transportService.sendRequest(localNode, TransportUnpromotableShardRefreshAction.NAME, request, expectSuccess(future::onResponse));
        assertThat(safeGet(future), sameInstance(ActionResponse.Empty.INSTANCE));
    }

    public void testActionWaitsUntilIndexRefreshBlocksAreCleared() {
        final var shardId = new ShardId(new Index(randomIdentifier(), randomUUID()), between(0, 3));
        final var withSearchShards = randomBoolean();
        final var indexShardRoutingTable = createShardRoutingTableWithPrimaryAndSearchShards(shardId, withSearchShards);

        final var indicesService = mock(IndicesService.class);
        final var unpromotableShardOperationExecuted = new AtomicBoolean(false);
        final var waitForBlocks = randomBoolean();
        // Register the action
        new TransportUnpromotableShardRefreshAction(
            clusterService,
            transportService,
            mock(ShardStateAction.class),
            new ActionFilters(Set.of()),
            indicesService,
            threadPool,
            waitForBlocks
        ) {
            @Override
            protected void unpromotableShardOperation(
                Task task,
                UnpromotableShardRefreshRequest request,
                ActionListener<ActionResponse.Empty> responseListener
            ) {
                unpromotableShardOperationExecuted.set(true);
                ActionListener.completeWith(responseListener, () -> ActionResponse.Empty.INSTANCE);
            }
        };

        var withRefreshBlock = randomBoolean();
        if (withRefreshBlock) {
            setState(
                clusterService,
                ClusterState.builder(clusterService.state())
                    .blocks(ClusterBlocks.builder().addIndexBlock(shardId.getIndexName(), IndexMetadata.INDEX_REFRESH_BLOCK))
            );
        }

        final var future = new PlainActionFuture<ActionResponse.Empty>();
        final var request = new UnpromotableShardRefreshRequest(
            indexShardRoutingTable,
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomBoolean(),
            // Ensure that the request doesn't timeout
            TimeValue.timeValueSeconds(15)
        );
        transportService.sendRequest(localNode, TransportUnpromotableShardRefreshAction.NAME, request, expectSuccess(future::onResponse));

        // If the index is not blocked for refreshes, or if the node is not configured to wait for blocked refreshes,
        // the action should return a response immediately.
        if (withRefreshBlock && waitForBlocks) {
            assertThat(future.isDone(), is(false));
            assertThat(unpromotableShardOperationExecuted.get(), is(false));

            if (randomBoolean()) {
                setState(clusterService, ClusterState.builder(clusterService.state()).version(clusterService.state().version() + 1));
                assertThat(future.isDone(), is(false));
                assertThat(unpromotableShardOperationExecuted.get(), is(false));
            }

            setState(
                clusterService,
                ClusterState.builder(clusterService.state())
                    .blocks(
                        ClusterBlocks.builder()
                            .removeIndexBlock(Metadata.DEFAULT_PROJECT_ID, shardId.getIndexName(), IndexMetadata.INDEX_REFRESH_BLOCK)
                    )
            );
        }

        assertThat(safeGet(future), sameInstance(ActionResponse.Empty.INSTANCE));
        assertThat(unpromotableShardOperationExecuted.get(), is(withSearchShards));
    }

    public void testActionWaitsUntilShardRefreshBlocksAreClearedMightTimeout() {
        final var shardId = new ShardId(new Index(randomIdentifier(), randomUUID()), between(0, 3));
        final var indexShardRoutingTable = createShardRoutingTableWithPrimaryAndSearchShards(shardId, true);

        final IndicesService indicesService = mock(IndicesService.class);
        // Register the action
        new TransportUnpromotableShardRefreshAction(
            clusterService,
            transportService,
            mock(ShardStateAction.class),
            new ActionFilters(Set.of()),
            indicesService,
            threadPool,
            true
        ) {
            @Override
            protected void unpromotableShardOperation(
                Task task,
                UnpromotableShardRefreshRequest request,
                ActionListener<ActionResponse.Empty> responseListener
            ) {
                assert false : "Unexpected call";
                throw new AssertionError("Unexpected call");
            }
        };

        setState(
            clusterService,
            ClusterState.builder(clusterService.state())
                .blocks(ClusterBlocks.builder().addIndexBlock(shardId.getIndexName(), IndexMetadata.INDEX_REFRESH_BLOCK))
        );

        final var countDownLatch = new CountDownLatch(1);
        final var request = new UnpromotableShardRefreshRequest(
            indexShardRoutingTable,
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomBoolean(),
            TimeValue.timeValueSeconds(5)
        );
        transportService.sendRequest(localNode, TransportUnpromotableShardRefreshAction.NAME, request, expectError(e -> {
            assertThat(e.getCause(), instanceOf(ElasticsearchTimeoutException.class));
            assertThat(e.getCause().getMessage(), containsString("shard refresh timed out waiting for index block to be removed"));
            assertThat(e.getRootCause(), instanceOf(ClusterBlockException.class));
            countDownLatch.countDown();
        }));

        assertThat(countDownLatch.getCount(), is(equalTo(1L)));

        if (randomBoolean()) {
            setState(clusterService, ClusterState.builder(clusterService.state()).version(clusterService.state().version() + 1));
            assertThat(countDownLatch.getCount(), is(equalTo(1L)));
        }

        safeAwait(countDownLatch);
    }

    private IndexShardRoutingTable createShardRoutingTableWithPrimaryAndSearchShards(ShardId shardId, boolean withSearchShards) {
        final var shardRouting = TestShardRouting.newShardRouting(
            shardId,
            randomUUID(),
            true,
            ShardRoutingState.STARTED,
            ShardRouting.Role.INDEX_ONLY
        );
        final var indexShardRoutingTableBuilder = new IndexShardRoutingTable.Builder(shardId).addShard(shardRouting);

        if (withSearchShards) {
            final var unpromotableShardRouting = TestShardRouting.newShardRouting(
                shardId,
                localNode.getId(),
                false,
                ShardRoutingState.INITIALIZING,
                ShardRouting.Role.SEARCH_ONLY
            );
            indexShardRoutingTableBuilder.addShard(unpromotableShardRouting);
        }
        return indexShardRoutingTableBuilder.build();
    }

    private TransportResponseHandler<ActionResponse.Empty> expectSuccess(Consumer<ActionResponse.Empty> onResponse) {
        return responseHandler(onResponse, ESTestCase::fail);
    }

    private TransportResponseHandler<ActionResponse.Empty> expectError(Consumer<TransportException> onException) {
        return responseHandler(r -> { assert false : r; }, onException);
    }

    private TransportResponseHandler<ActionResponse.Empty> responseHandler(
        Consumer<ActionResponse.Empty> onResponse,
        Consumer<TransportException> onException
    ) {
        return new TransportResponseHandler<>() {

            @Override
            public ActionResponse.Empty read(StreamInput in) {
                return ActionResponse.Empty.INSTANCE;
            }

            @Override
            public Executor executor() {
                return TransportResponseHandler.TRANSPORT_WORKER;
            }

            @Override
            public void handleResponse(ActionResponse.Empty response) {
                onResponse.accept(response);
            }

            @Override
            public void handleException(TransportException exp) {
                onException.accept(exp);
            }
        };
    }
}
