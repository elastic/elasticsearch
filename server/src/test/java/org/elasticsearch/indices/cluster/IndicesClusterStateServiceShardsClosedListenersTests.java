/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.cluster;

import org.elasticsearch.action.admin.cluster.node.tasks.TransportTasksActionTests;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.seqno.RetentionLeaseSyncer;
import org.elasticsearch.index.shard.PrimaryReplicaSyncer;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.PeerRecoverySourceService;
import org.elasticsearch.indices.recovery.PeerRecoveryTargetService;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.snapshots.SnapshotShardsService;
import org.elasticsearch.test.transport.MockTransport;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;

public class IndicesClusterStateServiceShardsClosedListenersTests extends AbstractIndicesClusterStateServiceTestCase {

    protected ThreadPool threadPool;

    @Before
    public void setupThreadPool() {
        threadPool = new TestThreadPool(TransportTasksActionTests.class.getSimpleName());
    }

    @After
    public final void shutdownThreadPool() {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        threadPool = null;
    }

    public void testX() {
        try (TestIndicesClusterStateService x = new TestIndicesClusterStateService(threadPool)) {

        }
    }

    class TestIndicesClusterStateService extends IndicesClusterStateService {
        TestIndicesClusterStateService(ThreadPool threadPool) {
            super(
                Settings.EMPTY,
                new MockIndicesService(),
                new ClusterService(Settings.EMPTY, ClusterSettings.createBuiltInClusterSettings(), threadPool, null),
                threadPool,
                mock(PeerRecoveryTargetService.class),
                mock(ShardStateAction.class),
                mock(RepositoriesService.class),
                mock(SearchService.class),
                mock(PeerRecoverySourceService.class),
                new SnapshotShardsService(
                    Settings.EMPTY,
                    new ClusterService(Settings.EMPTY, ClusterSettings.createBuiltInClusterSettings(), threadPool, null),
                    mock(RepositoriesService.class),
                    MockTransportService.createMockTransportService(new MockTransport(), threadPool),
                    mock(IndicesService.class)
                ),
                mock(PrimaryReplicaSyncer.class),
                RetentionLeaseSyncer.EMPTY,
                mock(NodeClient.class)
            );
        }
    }
}
