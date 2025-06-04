/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.snapshots;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.create.TransportCreateSnapshotAction;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.coordination.Coordinator;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.disruption.NetworkDisruption;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPoolStats;
import org.elasticsearch.transport.TestTransportChannel;
import org.elasticsearch.transport.TransportResponse;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasSize;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SnapshotShardsServiceIT extends AbstractSnapshotIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockRepository.Plugin.class, MockTransportService.TestPlugin.class);
    }

    public void testRetryPostingSnapshotStatusMessages() throws Exception {
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();

        createRepository("test-repo", "mock");

        final int shards = between(1, 10);
        assertAcked(prepareCreate("test-index", 0, indexSettingsNoReplicas(shards)));
        ensureGreen();
        indexRandomDocs("test-index", scaledRandomIntBetween(50, 100));

        logger.info("--> blocking repository");
        String blockedNode = blockNodeWithIndex("test-repo", "test-index");
        dataNodeClient().admin()
            .cluster()
            .prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, "test-repo", "test-snap")
            .setWaitForCompletion(false)
            .setIndices("test-index")
            .get();
        waitForBlock(blockedNode, "test-repo");

        final SnapshotId snapshotId = getSnapshot("test-repo", "test-snap").snapshotId();

        logger.info("--> start disrupting cluster");
        final NetworkDisruption networkDisruption = isolateMasterDisruption(NetworkDisruption.NetworkDelay.random(random()));
        internalCluster().setDisruptionScheme(networkDisruption);
        networkDisruption.startDisrupting();

        logger.info("--> unblocking repository");
        unblockNode("test-repo", blockedNode);

        // Retrieve snapshot status from the data node.
        SnapshotShardsService snapshotShardsService = internalCluster().getInstance(SnapshotShardsService.class, blockedNode);
        assertBusy(() -> {
            final Snapshot snapshot = new Snapshot("test-repo", snapshotId);
            List<IndexShardSnapshotStatus.Stage> stages = snapshotShardsService.currentSnapshotShards(snapshot)
                .values()
                .stream()
                .map(IndexShardSnapshotStatus.Copy::getStage)
                .toList();
            assertThat(stages, hasSize(shards));
            assertThat(stages, everyItem(equalTo(IndexShardSnapshotStatus.Stage.DONE)));
        }, 30L, TimeUnit.SECONDS);

        logger.info("--> stop disrupting cluster");
        networkDisruption.stopDisrupting();
        internalCluster().clearDisruptionScheme(true);

        assertBusy(() -> {
            SnapshotInfo snapshotInfo = getSnapshot("test-repo", "test-snap");
            logger.info("Snapshot status [{}], successfulShards [{}]", snapshotInfo.state(), snapshotInfo.successfulShards());
            assertThat(snapshotInfo.state(), equalTo(SnapshotState.SUCCESS));
            assertThat(snapshotInfo.successfulShards(), equalTo(shards));
        }, 30L, TimeUnit.SECONDS);
    }

    @TestLogging(
        reason = "verifying debug logging",
        value = "org.elasticsearch.snapshots.SnapshotShardsService.ShardStatusConsistencyChecker:DEBUG"
    )
    public void testConsistencyCheckerLogging() {
        final var masterNode = internalCluster().startMasterOnlyNode();
        final var dataNode = internalCluster().startDataOnlyNode();

        final var repoName = randomIdentifier();
        createRepository(repoName, "fs");

        final var indexName = randomIdentifier();
        assertAcked(prepareCreate(indexName, 0, indexSettingsNoReplicas(between(1, 5))));
        indexRandomDocs(indexName, scaledRandomIntBetween(50, 100));

        // allow cluster states to go through normally until we see a shard snapshot update
        final var shardUpdateSeen = new AtomicBoolean();
        MockTransportService.getInstance(masterNode)
            .addRequestHandlingBehavior(SnapshotsService.UPDATE_SNAPSHOT_STATUS_ACTION_NAME, (handler, request, channel, task) -> {
                shardUpdateSeen.set(true);
                handler.messageReceived(request, channel, task);
            });

        // then, after the shard snapshot update, delay cluster state update commits on the data node until we see the
        // ShardStatusConsistencyChecker log an inconsistency.
        final var logMessageSeenListener = new SubscribableListener<Void>();
        MockTransportService.getInstance(dataNode)
            .addRequestHandlingBehavior(Coordinator.COMMIT_STATE_ACTION_NAME, (handler, request, channel, task) -> {
                if (shardUpdateSeen.get() == false || logMessageSeenListener.isDone()) {
                    handler.messageReceived(request, channel, task);
                } else {
                    // delay the commit ...
                    logMessageSeenListener.addListener(
                        ActionTestUtils.assertNoFailureListener(
                            ignored -> ActionListener.run(
                                ActionListener.<TransportResponse>noop(),
                                l -> handler.messageReceived(request, new TestTransportChannel(l), task)
                            )
                        )
                    );
                    // ... and send a failure straight back to the master so it applies and acks the state update anyway
                    channel.sendResponse(new ElasticsearchException("simulated"));
                }
            });

        MockLog.awaitLogger(
            () -> createSnapshot(repoName, randomIdentifier(), List.of(indexName)),
            SnapshotShardsService.ShardStatusConsistencyChecker.class,
            new MockLog.SeenEventExpectation(
                "debug log",
                SnapshotShardsService.ShardStatusConsistencyChecker.class.getCanonicalName(),
                Level.DEBUG,
                "*unexpectedly still in state [*INIT*] after notifying master"
            ) {
                @Override
                public boolean innerMatch(LogEvent event) {
                    if (event.getMessage().getFormattedMessage().contains("after notifying master")) {
                        // now that the inconsistency was logged, release the cluster state commits on the data node
                        logMessageSeenListener.onResponse(null);
                    }
                    return true;
                }
            }
        );
    }

    public void testStartSnapshotsConcurrently() {
        internalCluster().startMasterOnlyNode();
        final var dataNode = internalCluster().startDataOnlyNode();

        final var repoName = randomIdentifier();
        createRepository(repoName, "fs");

        final var threadPool = internalCluster().getInstance(ThreadPool.class, dataNode);
        final var snapshotThreadCount = threadPool.info(ThreadPool.Names.SNAPSHOT).getMax();

        final var indexName = randomIdentifier();
        final var shardCount = between(1, snapshotThreadCount * 2);
        assertAcked(prepareCreate(indexName, 0, indexSettingsNoReplicas(shardCount)));
        indexRandomDocs(indexName, scaledRandomIntBetween(50, 100));

        final var snapshotExecutor = threadPool.executor(ThreadPool.Names.SNAPSHOT);
        final var barrier = new CyclicBarrier(snapshotThreadCount + 1);
        for (int i = 0; i < snapshotThreadCount; i++) {
            snapshotExecutor.submit(() -> {
                safeAwait(barrier);
                safeAwait(barrier);
            });
        }

        // wait until the snapshot threads are all blocked
        safeAwait(barrier);

        safeGet(
            client().execute(
                TransportCreateSnapshotAction.TYPE,
                new CreateSnapshotRequest(TEST_REQUEST_TIMEOUT, repoName, randomIdentifier())
            )
        );

        // one task for each snapshot thread (throttled) or shard (if fewer), plus one for runSyncTasksEagerly()
        assertEquals(Math.min(snapshotThreadCount, shardCount) + 1, getSnapshotQueueLength(threadPool));

        // release all the snapshot threads
        safeAwait(barrier);

        // wait for completion
        safeAwait(ClusterServiceUtils.addMasterTemporaryStateListener(cs -> SnapshotsInProgress.get(cs).isEmpty()));
    }

    private static int getSnapshotQueueLength(ThreadPool threadPool) {
        for (ThreadPoolStats.Stats stats : threadPool.stats().stats()) {
            if (stats.name().equals(ThreadPool.Names.SNAPSHOT)) {
                return stats.queue();
            }
        }

        throw new AssertionError("threadpool stats for [" + ThreadPool.Names.SNAPSHOT + "] not found");
    }
}
