/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.cluster;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.tasks.TransportTasksActionTests;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterChangedEvent;
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

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

    public void testRunnablesExecuteAfterAllPreviousListenersComplete() {
        AtomicInteger clusterStateAppliedRound = new AtomicInteger();
        int totalClusterStateAppliedRounds = randomIntBetween(10, 100);
        Map<Integer, List<Runnable>> runnablesOnShardsClosedPerRound = new ConcurrentHashMap<>();
        Map<Integer, List<ActionListener<Void>>> shardsClosedListenersPerRound = new ConcurrentHashMap<>();
        List<ActionListener<Void>> allShardsClosedListeners = Collections.synchronizedList(new ArrayList<>());
        try (
            TestIndicesClusterStateService testIndicesClusterStateService = new TestIndicesClusterStateService(
                threadPool,
                    // the apply cluster state hook
                    (indicesClusterStateService, clusterChangedEvent) -> {
                    final int round = clusterStateAppliedRound.get();
                    // maybe register runnable for when all the shards in the currently applied cluster states are closed
                    if (randomBoolean()) {
                        Runnable mockRunnable = mock(Runnable.class);
                        indicesClusterStateService.onClusterStateShardsClosed(mockRunnable);
                        runnablesOnShardsClosedPerRound.get(round).add(mockRunnable);
                    }
                    // maybe get some listeners as if asynchronously closing some shards
                    int listenersCount = randomIntBetween(0, 2);
                    for (int i = 0; i < listenersCount; i++) {
                        var shardsClosedListener = new SubscribableListener<Void>();
                        shardsClosedListener.addListener(indicesClusterStateService.getShardsClosedListener());
                        shardsClosedListenersPerRound.get(round).add(shardsClosedListener);
                        allShardsClosedListeners.add(shardsClosedListener);
                        shardsClosedListener.andThen(l -> {
                            shardsClosedListenersPerRound.get(round).remove(shardsClosedListener);
                            allShardsClosedListeners.remove(shardsClosedListener);
                        });
                    }
                    // maybe register runnable for when all the shards in the currently applied cluster states are closed
                    if (randomBoolean()) {
                        Runnable mockRunnable = mock(Runnable.class);
                        indicesClusterStateService.onClusterStateShardsClosed(mockRunnable);
                        runnablesOnShardsClosedPerRound.get(round).add(mockRunnable);
                    }
                }
            )
        ) {
            while (clusterStateAppliedRound.getAndIncrement() < totalClusterStateAppliedRounds) {
                final int round = clusterStateAppliedRound.get();
                runnablesOnShardsClosedPerRound.put(round, Collections.synchronizedList(new ArrayList<>()));
                shardsClosedListenersPerRound.put(round, Collections.synchronizedList(new ArrayList<>()));

                // apply cluster state this round
                testIndicesClusterStateService.applyClusterState(mock(ClusterChangedEvent.class));

                // maybe register runnable for when all the shards in the previously applied cluster states are closed
                runnablesOnShardsClosedPerRound.get(round).addAll(randomList(0, 2, () -> {
                    Runnable mockRunnable = mock(Runnable.class);
                    testIndicesClusterStateService.onClusterStateShardsClosed(mockRunnable);
                    return mockRunnable;
                }));

                // TODO pick random listeners and complete them
                if (randomBoolean() && allShardsClosedListeners.isEmpty() == false) {
                    randomFrom(allShardsClosedListeners).onResponse(null);
                }
            }
        }
    }

    class TestIndicesClusterStateService extends IndicesClusterStateService {
        BiConsumer<IndicesClusterStateService, ClusterChangedEvent> doApplyClusterStateHook;

        TestIndicesClusterStateService(
            ThreadPool threadPool,
            BiConsumer<IndicesClusterStateService, ClusterChangedEvent> doApplyClusterStateHook
        ) {
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
            this.doApplyClusterStateHook = doApplyClusterStateHook;
        }

        @Override
        protected void doApplyClusterState(final ClusterChangedEvent event) {
            doApplyClusterStateHook.accept(this, event);
        }
    }
}
