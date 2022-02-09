/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.resync;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.coordination.NoMasterBlockService;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexingPressure;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ReplicationGroup;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.EmptySystemIndices;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.transport.netty4.Netty4Transport;
import org.elasticsearch.transport.netty4.SharedGroupFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.nio.charset.Charset;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Collections.emptyList;
import static org.elasticsearch.action.support.replication.ClusterStateCreationUtils.state;
import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.elasticsearch.test.ClusterServiceUtils.setState;
import static org.elasticsearch.transport.TransportService.NOOP_TRANSPORT_INTERCEPTOR;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportResyncReplicationActionTests extends ESTestCase {

    private static ThreadPool threadPool;

    @BeforeClass
    public static void beforeClass() {
        threadPool = new TestThreadPool("ShardReplicationTests");
    }

    @AfterClass
    public static void afterClass() {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        threadPool = null;
    }

    public void testResyncDoesNotBlockOnPrimaryAction() throws Exception {
        try (ClusterService clusterService = createClusterService(threadPool)) {
            final String indexName = randomAlphaOfLength(5);
            setState(clusterService, state(indexName, true, ShardRoutingState.STARTED));

            setState(
                clusterService,
                ClusterState.builder(clusterService.state())
                    .blocks(
                        ClusterBlocks.builder()
                            .addGlobalBlock(NoMasterBlockService.NO_MASTER_BLOCK_ALL)
                            .addIndexBlock(indexName, IndexMetadata.INDEX_WRITE_BLOCK)
                    )
            );

            try (
                TcpTransport transport = new Netty4Transport(
                    Settings.EMPTY,
                    Version.CURRENT,
                    threadPool,
                    new NetworkService(emptyList()),
                    PageCacheRecycler.NON_RECYCLING_INSTANCE,
                    new NamedWriteableRegistry(emptyList()),
                    new NoneCircuitBreakerService(),
                    new SharedGroupFactory(Settings.EMPTY)
                )
            ) {

                final MockTransportService transportService = new MockTransportService(
                    Settings.EMPTY,
                    transport,
                    threadPool,
                    NOOP_TRANSPORT_INTERCEPTOR,
                    x -> clusterService.localNode(),
                    null,
                    Collections.emptySet()
                );
                transportService.start();
                transportService.acceptIncomingRequests();
                final ShardStateAction shardStateAction = new ShardStateAction(clusterService, transportService, null, null, threadPool);

                final IndexMetadata indexMetadata = clusterService.state().metadata().index(indexName);
                final Index index = indexMetadata.getIndex();
                final ShardId shardId = new ShardId(index, 0);
                final IndexShardRoutingTable shardRoutingTable = clusterService.state().routingTable().shardRoutingTable(shardId);
                final ShardRouting primaryShardRouting = clusterService.state().routingTable().shardRoutingTable(shardId).primaryShard();
                final String allocationId = primaryShardRouting.allocationId().getId();
                final long primaryTerm = indexMetadata.primaryTerm(shardId.id());

                final AtomicInteger acquiredPermits = new AtomicInteger();
                final IndexShard indexShard = mock(IndexShard.class);
                when(indexShard.indexSettings()).thenReturn(new IndexSettings(indexMetadata, Settings.EMPTY));
                when(indexShard.shardId()).thenReturn(shardId);
                when(indexShard.routingEntry()).thenReturn(primaryShardRouting);
                when(indexShard.getPendingPrimaryTerm()).thenReturn(primaryTerm);
                when(indexShard.getOperationPrimaryTerm()).thenReturn(primaryTerm);
                when(indexShard.getActiveOperationsCount()).then(i -> acquiredPermits.get());
                doAnswer(invocation -> {
                    @SuppressWarnings("unchecked")
                    ActionListener<Releasable> callback = (ActionListener<Releasable>) invocation.getArguments()[0];
                    acquiredPermits.incrementAndGet();
                    callback.onResponse(acquiredPermits::decrementAndGet);
                    return null;
                }).when(indexShard).acquirePrimaryOperationPermit(anyActionListener(), anyString(), any(), eq(true));
                when(indexShard.getReplicationGroup()).thenReturn(
                    new ReplicationGroup(
                        shardRoutingTable,
                        clusterService.state().metadata().index(index).inSyncAllocationIds(shardId.id()),
                        shardRoutingTable.getAllAllocationIds(),
                        0
                    )
                );

                final IndexService indexService = mock(IndexService.class);
                when(indexService.getShard(eq(shardId.id()))).thenReturn(indexShard);

                final IndicesService indexServices = mock(IndicesService.class);
                when(indexServices.indexServiceSafe(eq(index))).thenReturn(indexService);

                final TransportResyncReplicationAction action = new TransportResyncReplicationAction(
                    Settings.EMPTY,
                    transportService,
                    clusterService,
                    indexServices,
                    threadPool,
                    shardStateAction,
                    new ActionFilters(new HashSet<>()),
                    new IndexingPressure(Settings.EMPTY),
                    EmptySystemIndices.INSTANCE
                );

                assertThat(action.globalBlockLevel(), nullValue());
                assertThat(action.indexBlockLevel(), nullValue());

                final Task task = mock(Task.class);
                when(task.getId()).thenReturn(randomNonNegativeLong());

                final byte[] bytes = "{}".getBytes(Charset.forName("UTF-8"));
                final ResyncReplicationRequest request = new ResyncReplicationRequest(
                    shardId,
                    42L,
                    100,
                    new Translog.Operation[] { new Translog.Index("id", 0, primaryTerm, 0L, bytes, null, -1) }
                );

                final PlainActionFuture<ResyncReplicationResponse> listener = new PlainActionFuture<>();
                action.sync(request, task, allocationId, primaryTerm, listener);

                assertThat(listener.get().getShardInfo().getFailed(), equalTo(0));
                assertThat(listener.isDone(), is(true));
            }
        }
    }
}
