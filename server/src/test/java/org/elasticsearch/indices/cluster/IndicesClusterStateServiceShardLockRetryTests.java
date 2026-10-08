/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.cluster;

import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.ShardLockObtainFailedException;
import org.elasticsearch.index.seqno.RetentionLeaseSyncer;
import org.elasticsearch.index.shard.GlobalCheckpointSyncer;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.PrimaryReplicaSyncer;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.recovery.FailureStrategy;
import org.elasticsearch.indices.recovery.PeerRecoveryTargetService;
import org.elasticsearch.indices.recovery.RecoveryFailedException;
import org.elasticsearch.indices.recovery.RecoveryListener;
import org.elasticsearch.indices.recovery.RecoveryMetricsCollector;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.elasticsearch.indices.cluster.IndicesClusterStateService.SHARD_LOCK_RETRY_INTERVAL_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

/// Tests that the shard-lock retry path (the scheduled retry in
/// [IndicesClusterStateService#createShardWhenLockAvailable] and [IndicesClusterStateService#handleRecoveryFailure] hold the
/// same monitor and therefore can never run concurrently, even though they execute on different threads (the cluster applier thread vs. a
/// generic-pool thread).
public class IndicesClusterStateServiceShardLockRetryTests extends AbstractIndicesClusterStateServiceTestCase {

    private ThreadPool threadPool;
    private ClusterService clusterService;

    @Before
    public void createServices() {
        threadPool = new TestThreadPool(getClass().getName());
        clusterService = ClusterServiceUtils.createClusterService(threadPool);
    }

    @After
    public void stopServices() {
        clusterService.close();
        terminate(threadPool);
    }

    public void testRetryPathAndHandleRecoveryFailureAreMutuallyExclusive() throws Exception {
        disableRandomFailures();

        final String indexName = "test";
        final ClusterState stateWithIndex = ClusterStateCreationUtils.state(indexName, true, ShardRoutingState.INITIALIZING);

        final ShardId shardId = new ShardId(stateWithIndex.metadata().getProject().index(indexName).getIndex(), 0);
        final ShardRouting shardRouting = stateWithIndex.routingTable().shardRoutingTable(shardId).primaryShard();
        final DiscoveryNode localNode = stateWithIndex.nodes().getLocalNode();

        // Signals that the scheduled retry is executing indicesService.createShard() a second time, i.e. it is inside
        // IndicesClusterStateService's monitor on the cluster applier thread.
        final CountDownLatch retryEnteredCriticalSection = new CountDownLatch(1);
        // Held by the test until it has proven that handleRecoveryFailure is blocked.
        final CountDownLatch releaseCriticalSection = new CountDownLatch(1);
        final AtomicInteger createShardCalls = new AtomicInteger();

        final MockIndicesService indicesService = new MockIndicesService() {
            @Override
            public void createShard(
                ProjectId projectId,
                ShardRouting shardRouting,
                PeerRecoveryTargetService recoveryTargetService,
                RecoveryListener recoveryListener,
                RepositoriesService repositoriesService,
                Consumer<IndexShard.ShardFailure> onShardFailure,
                GlobalCheckpointSyncer globalCheckpointSyncer,
                RetentionLeaseSyncer retentionLeaseSyncer,
                DiscoveryNode targetNode,
                DiscoveryNode sourceNode,
                long clusterStateVersion
            ) throws IOException {
                if (createShardCalls.incrementAndGet() == 1) {
                    // forces IndicesClusterStateService to schedule the lock-retry path
                    throw new ShardLockObtainFailedException(shardRouting.shardId(), "simulated shard lock contention");
                }
                retryEnteredCriticalSection.countDown();
                safeAwait(releaseCriticalSection);
                super.createShard(
                    projectId,
                    shardRouting,
                    recoveryTargetService,
                    recoveryListener,
                    repositoriesService,
                    onShardFailure,
                    globalCheckpointSyncer,
                    retentionLeaseSyncer,
                    targetNode,
                    sourceNode,
                    clusterStateVersion
                );
            }
        };

        final Settings settings = Settings.builder().put(SHARD_LOCK_RETRY_INTERVAL_SETTING.getKey(), TimeValue.timeValueMillis(10)).build();

        final IndicesClusterStateService indicesClusterStateService = new IndicesClusterStateService(
            settings,
            indicesService,
            clusterService,
            threadPool,
            mock(PeerRecoveryTargetService.class),
            mock(ShardStateAction.class),
            mock(RepositoriesService.class),
            null,
            null,
            null,
            mock(PrimaryReplicaSyncer.class),
            RetentionLeaseSyncer.EMPTY,
            mock(NodeClient.class),
            RecoveryMetricsCollector.NOOP
        ) {
            @Override
            protected void updateGlobalCheckpointForShard(ShardId shardId) {}
        };

        try (indicesClusterStateService) {
            indicesClusterStateService.start();

            ClusterServiceUtils.setState(clusterService.getClusterApplierService(), stateWithIndex);

            // wait for the scheduled retry to re-enter indicesService.createShard() while holding the service's monitor
            safeAwait(retryEnteredCriticalSection);

            final CountDownLatch recoveryFailureHandlerStarted = new CountDownLatch(1);
            final CountDownLatch recoveryFailureHandlerCompleted = new CountDownLatch(1);
            final Thread recoveryFailureThread = new Thread(() -> {
                recoveryFailureHandlerStarted.countDown();
                final var recoveryState = new RecoveryState(shardRouting, localNode, null);
                indicesClusterStateService.handleRecoveryFailure(
                    shardRouting,
                    FailureStrategy.FAIL_SEND,
                    1L,
                    new RecoveryFailedException(recoveryState, "test induced failure", new RuntimeException("test induced failure"))
                );
                recoveryFailureHandlerCompleted.countDown();
            }, "handleRecoveryFailure-thread");
            recoveryFailureThread.start();
            safeAwait(recoveryFailureHandlerStarted);

            // handleRecoveryFailure must block trying to acquire the same monitor that the retry path is holding
            assertBusy(() -> assertThat(recoveryFailureThread.getState(), equalTo(Thread.State.BLOCKED)));
            assertThat(
                "handleRecoveryFailure must not complete while the retry path holds the monitor",
                recoveryFailureHandlerCompleted.getCount(),
                equalTo(1L)
            );

            // let the retry path finish and release the monitor
            releaseCriticalSection.countDown();

            safeAwait(recoveryFailureHandlerCompleted);
            recoveryFailureThread.join();
        }
    }
}
