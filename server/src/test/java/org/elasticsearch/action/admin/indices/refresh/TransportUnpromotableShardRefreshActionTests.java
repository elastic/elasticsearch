/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.refresh;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.sameInstance;
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

    public void testRespondOKToRefreshRequestBeforeShardIsCreated() {
        final var shardId = new ShardId(new Index(randomIdentifier(), randomUUID()), between(0, 3));
        final var shardRouting = TestShardRouting.newShardRouting(shardId, randomUUID(), true, ShardRoutingState.STARTED);
        final var indexShardRoutingTable = new IndexShardRoutingTable.Builder(shardId).addShard(shardRouting).build();

        final var request = new UnpromotableShardRefreshRequest(
            indexShardRoutingTable,
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomBoolean()
        );

        final TransportService transportService = mock(TransportService.class);
        when(transportService.getThreadPool()).thenReturn(threadPool);
        final IndicesService indicesService = mock(IndicesService.class);
        if (randomBoolean()) {
            when(indicesService.indexService(shardId.getIndex())).thenReturn(null);
        } else {
            final IndexService indexService = mock(IndexService.class);
            when(indicesService.indexService(shardId.getIndex())).thenReturn(indexService);
            when(indexService.hasShard(shardId.id())).thenReturn(false);
        }

        final var action = new TransportUnpromotableShardRefreshAction(
            clusterService,
            transportService,
            mock(ShardStateAction.class),
            mock(ActionFilters.class),
            indicesService
        );

        final PlainActionFuture<ActionResponse.Empty> future = new PlainActionFuture<>();
        action.unpromotableShardOperation(mock(Task.class), request, future);
        assertThat(safeGet(future), sameInstance(ActionResponse.Empty.INSTANCE));
    }
}
