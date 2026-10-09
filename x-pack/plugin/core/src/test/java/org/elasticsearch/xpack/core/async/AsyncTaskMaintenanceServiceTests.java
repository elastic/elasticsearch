/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.async;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mockito;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.core.XPackPlugin.ASYNC_RESULTS_INDEX;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;

public class AsyncTaskMaintenanceServiceTests extends ESTestCase {

    private TestThreadPool threadPool;

    @Before
    public void setUpThreadPool() {
        threadPool = new TestThreadPool(getTestName());
    }

    @After
    public void stopThreadPool() {
        threadPool.shutdown();
    }

    /**
     * Delete-by-query keeps a scroll open (default keep-alive 5 minutes) until its listener fires.
     * {@code pause()} waits that cleanup out so callers can drain search contexts.
     */
    @SuppressWarnings("unchecked")
    public void testPauseWaitsForInFlightDeleteByQuery() throws Exception {
        final String localNodeId = randomIdentifier();
        final IndexMetadata indexMetadata = IndexMetadata.builder(ASYNC_RESULTS_INDEX)
            .settings(indexSettings(IndexVersion.current(), 1, 0))
            .build();
        final var index = indexMetadata.getIndex();
        final ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(indexMetadata, false))
            .routingTable(
                RoutingTable.builder()
                    .add(
                        IndexRoutingTable.builder(index)
                            .addShard(
                                TestShardRouting.newShardRouting(new ShardId(index, 0), localNodeId, true, ShardRoutingState.STARTED)
                            )
                            .build()
                    )
                    .build()
            )
            .build();

        final Client client = Mockito.mock(Client.class);
        final AtomicReference<ActionListener<?>> heldListener = new AtomicReference<>();
        final CountDownLatch deleteByQueryStarted = new CountDownLatch(1);
        Mockito.doAnswer(invocation -> {
            heldListener.set(invocation.getArgument(2, ActionListener.class));
            deleteByQueryStarted.countDown();
            return null;
        }).when(client).execute(same(DeleteByQueryAction.INSTANCE), any(DeleteByQueryRequest.class), any(ActionListener.class));

        final AsyncTaskMaintenanceService service = new AsyncTaskMaintenanceService(
            Mockito.mock(ClusterService.class),
            localNodeId,
            Settings.EMPTY,
            threadPool,
            client
        );
        service.start();
        service.clusterChanged(new ClusterChangedEvent(getTestName(), clusterState, ClusterState.EMPTY_STATE));
        assertTrue(deleteByQueryStarted.await(10, TimeUnit.SECONDS));

        final AtomicBoolean pauseReturned = new AtomicBoolean();
        final Thread pauser = new Thread(() -> {
            service.pause();
            pauseReturned.set(true);
        }, "pause-maintenance");
        pauser.start();
        try {
            pauser.join(200);
            assertTrue("pause() returned while delete-by-query was still in flight", pauser.isAlive());
            assertFalse(pauseReturned.get());
        } finally {
            heldListener.get().onResponse(null);
        }
        pauser.join(10_000);
        assertFalse("pause() did not return after delete-by-query completed", pauser.isAlive());
        assertTrue(pauseReturned.get());
    }
}
