/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.root;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MainActionTests extends ESTestCase {

    public void testMainActionClusterAvailable() {
        final ClusterService clusterService = mock(ClusterService.class);
        final ClusterName clusterName = new ClusterName("elasticsearch");
        final Settings settings = Settings.builder().put("node.name", "my-node").build();
        final ClusterBlocks blocks;
        if (randomBoolean()) {
            if (randomBoolean()) {
                blocks = ClusterBlocks.EMPTY_CLUSTER_BLOCK;
            } else {
                blocks = ClusterBlocks.builder()
                    .addGlobalBlock(
                        new ClusterBlock(
                            randomIntBetween(1, 16),
                            "test global block 400",
                            randomBoolean(),
                            randomBoolean(),
                            false,
                            RestStatus.BAD_REQUEST,
                            ClusterBlockLevel.ALL
                        )
                    )
                    .build();
            }
        } else {
            blocks = ClusterBlocks.builder()
                .addGlobalBlock(
                    new ClusterBlock(
                        randomIntBetween(1, 16),
                        "test global block 503",
                        randomBoolean(),
                        randomBoolean(),
                        false,
                        RestStatus.SERVICE_UNAVAILABLE,
                        ClusterBlockLevel.ALL
                    )
                )
                .build();
        }
        final Metadata.Builder metadata = new Metadata.Builder();
        if (randomBoolean()) {
            metadata.clusterUUID(randomUUID());
            metadata.clusterUUIDCommitted(randomBoolean());
        }
        final ClusterState state = ClusterState.builder(clusterName).metadata(metadata).blocks(blocks).build();
        when(clusterService.state()).thenReturn(state);

        TransportService transportService = new TransportService(
            Settings.EMPTY,
            mock(Transport.class),
            mock(ThreadPool.class),
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> null,
            null,
            Collections.emptySet()
        );
        final AtomicBoolean listenerCalled = new AtomicBoolean();
        new TransportMainAction(settings, transportService, mock(ActionFilters.class), clusterService).doExecute(
            mock(Task.class),
            new MainRequest(),
            ActionTestUtils.assertNoFailureListener(mainResponse -> {
                assertNotNull(mainResponse);
                assertEquals(
                    state.metadata().clusterUUIDCommitted() ? state.metadata().clusterUUID() : Metadata.UNKNOWN_CLUSTER_UUID,
                    mainResponse.getClusterUuid()
                );
                assertFalse(listenerCalled.getAndSet(true));
            })
        );

        assertTrue(listenerCalled.get());
        verify(clusterService, times(1)).state();
    }
}
