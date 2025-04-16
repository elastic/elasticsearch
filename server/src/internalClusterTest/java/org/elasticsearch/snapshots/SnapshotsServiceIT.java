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
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.SnapshotDeletionsInProgress;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class SnapshotsServiceIT extends AbstractSnapshotIntegTestCase {

    public void testDeletingSnapshotsIsLoggedAfterClusterStateIsProcessed() throws Exception {
        createRepository("test-repo", "fs");
        createIndexWithRandomDocs("test-index", randomIntBetween(1, 42));
        createSnapshot("test-repo", "test-snapshot", List.of("test-index"));

        try (var mockLog = MockLog.capture(SnapshotsService.class)) {
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation(
                    "[does-not-exist]",
                    SnapshotsService.class.getName(),
                    Level.INFO,
                    "deleting snapshots [does-not-exist] from repository [test-repo]"
                )
            );

            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "[deleting test-snapshot]",
                    SnapshotsService.class.getName(),
                    Level.INFO,
                    "deleting snapshots [test-snapshot] from repository [test-repo]"
                )
            );

            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "[test-snapshot deleted]",
                    SnapshotsService.class.getName(),
                    Level.INFO,
                    "snapshots [test-snapshot/*] deleted"
                )
            );

            final SnapshotMissingException e = expectThrows(
                SnapshotMissingException.class,
                startDeleteSnapshot("test-repo", "does-not-exist")
            );
            assertThat(e.getMessage(), containsString("[test-repo:does-not-exist] is missing"));
            assertThat(startDeleteSnapshot("test-repo", "test-snapshot").actionGet().isAcknowledged(), is(true));

            awaitNoMoreRunningOperations(); // ensure background file deletion is completed
            mockLog.assertAllExpectationsMatched();
        } finally {
            deleteRepository("test-repo");
        }
    }

    public void testSnapshotDeletionFailureShouldBeLogged() throws Exception {
        createRepository("test-repo", "mock");
        createIndexWithRandomDocs("test-index", randomIntBetween(1, 42));
        createSnapshot("test-repo", "test-snapshot", List.of("test-index"));

        try (var mockLog = MockLog.capture(SnapshotsService.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "[test-snapshot]",
                    SnapshotsService.class.getName(),
                    Level.WARN,
                    "failed to complete snapshot deletion for [test-snapshot] from repository [test-repo]"
                )
            );

            if (randomBoolean()) {
                // Failure when listing root blobs
                final MockRepository mockRepository = getRepositoryOnMaster("test-repo");
                mockRepository.setRandomControlIOExceptionRate(1.0);
                final Exception e = expectThrows(Exception.class, startDeleteSnapshot("test-repo", "test-snapshot"));
                assertThat(e.getCause().getMessage(), containsString("Random IOException"));
            } else {
                // Failure when finalizing on index-N file
                final ActionFuture<AcknowledgedResponse> deleteFuture;
                blockMasterFromFinalizingSnapshotOnIndexFile("test-repo");
                deleteFuture = startDeleteSnapshot("test-repo", "test-snapshot");
                waitForBlock(internalCluster().getMasterName(), "test-repo");
                unblockNode("test-repo", internalCluster().getMasterName());
                final Exception e = expectThrows(Exception.class, deleteFuture);
                assertThat(e.getCause().getMessage(), containsString("exception after block"));
            }

            mockLog.assertAllExpectationsMatched();
        } finally {
            deleteRepository("test-repo");
        }
    }

    public void testDeleteSnapshotWhenNotWaitingForCompletion() throws Exception {
        createIndexWithRandomDocs("test-index", randomIntBetween(1, 5));
        createRepository("test-repo", "mock");
        createSnapshot("test-repo", "test-snapshot", List.of("test-index"));
        MockRepository repository = getRepositoryOnMaster("test-repo");
        PlainActionFuture<AcknowledgedResponse> listener = new PlainActionFuture<>();
        SubscribableListener<Void> snapshotDeletionListener = createSnapshotDeletionListener("test-repo");
        repository.blockOnDataFiles();
        try {
            clusterAdmin().prepareDeleteSnapshot(TEST_REQUEST_TIMEOUT, "test-repo", "test-snapshot")
                .setWaitForCompletion(false)
                .execute(listener);
            // The request will complete as soon as the deletion is scheduled
            safeGet(listener);
            // The deletion won't complete until the block is removed
            assertFalse(snapshotDeletionListener.isDone());
        } finally {
            repository.unblock();
        }
        safeAwait(snapshotDeletionListener);
    }

    public void testDeleteSnapshotWhenWaitingForCompletion() throws Exception {
        createIndexWithRandomDocs("test-index", randomIntBetween(1, 5));
        createRepository("test-repo", "mock");
        createSnapshot("test-repo", "test-snapshot", List.of("test-index"));
        MockRepository repository = getRepositoryOnMaster("test-repo");
        PlainActionFuture<AcknowledgedResponse> requestCompleteListener = new PlainActionFuture<>();
        SubscribableListener<Void> snapshotDeletionListener = createSnapshotDeletionListener("test-repo");
        repository.blockOnDataFiles();
        try {
            clusterAdmin().prepareDeleteSnapshot(TEST_REQUEST_TIMEOUT, "test-repo", "test-snapshot")
                .setWaitForCompletion(true)
                .execute(requestCompleteListener);
            // Neither the request nor the deletion will complete until we remove the block
            assertFalse(requestCompleteListener.isDone());
            assertFalse(snapshotDeletionListener.isDone());
        } finally {
            repository.unblock();
        }
        safeGet(requestCompleteListener);
        safeAwait(snapshotDeletionListener);
    }

    /**
     * Create a listener that completes once it has observed a snapshot delete begin and end for a specific repository
     *
     * @param repositoryName The repository to monitor for deletions
     * @return the listener
     */
    private SubscribableListener<Void> createSnapshotDeletionListener(String repositoryName) {
        AtomicBoolean deleteHasStarted = new AtomicBoolean(false);
        return ClusterServiceUtils.addMasterTemporaryStateListener(state -> {
            SnapshotDeletionsInProgress deletionsInProgress = (SnapshotDeletionsInProgress) state.getCustoms()
                .get(SnapshotDeletionsInProgress.TYPE);
            if (deletionsInProgress == null) {
                return false;
            }
            if (deleteHasStarted.get() == false) {
                deleteHasStarted.set(deletionsInProgress.hasExecutingDeletion(repositoryName));
                return false;
            } else {
                return deletionsInProgress.hasExecutingDeletion(repositoryName) == false;
            }
        });
    }

    public void testRerouteWhenShardSnapshotsCompleted() throws Exception {
        final var repoName = randomIdentifier();
        createRepository(repoName, "mock");
        internalCluster().ensureAtLeastNumDataNodes(1);
        final var originalNode = internalCluster().startDataOnlyNode();

        final var indexName = randomIdentifier();
        createIndexWithContent(
            indexName,
            indexSettings(1, 0).put(IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_PREFIX + "._name", originalNode).build()
        );

        final var snapshotFuture = startFullSnapshotBlockedOnDataNode(randomIdentifier(), repoName, originalNode);

        // Use allocation filtering to push the shard to a new node, but it will not do so yet because of the ongoing snapshot.
        updateIndexSettings(
            Settings.builder()
                .putNull(IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_PREFIX + "._name")
                .put(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "._name", originalNode)
        );

        final var shardMovedListener = ClusterServiceUtils.addMasterTemporaryStateListener(state -> {
            final var primaryShard = state.routingTable().index(indexName).shard(0).primaryShard();
            return primaryShard.started() && originalNode.equals(state.nodes().get(primaryShard.currentNodeId()).getName()) == false;
        });
        assertFalse(shardMovedListener.isDone());

        unblockAllDataNodes(repoName);
        assertEquals(SnapshotState.SUCCESS, snapshotFuture.get(10, TimeUnit.SECONDS).getSnapshotInfo().state());

        // Now that the snapshot completed the shard should move to its new location.
        safeAwait(shardMovedListener);
        ensureGreen(indexName);
    }

    @TestLogging(reason = "testing task description, logged at DEBUG", value = "org.elasticsearch.cluster.service.MasterService:DEBUG")
    public void testCreateSnapshotTaskDescription() {
        createIndexWithRandomDocs(randomIdentifier(), randomIntBetween(1, 5));
        final var repositoryName = randomIdentifier();
        createRepository(repositoryName, "mock");

        final var snapshotName = randomIdentifier();
        MockLog.assertThatLogger(
            () -> createFullSnapshot(repositoryName, snapshotName),
            MasterService.class,
            new MockLog.SeenEventExpectation(
                "executing cluster state update debug message",
                MasterService.class.getCanonicalName(),
                Level.DEBUG,
                "executing cluster state update for [create_snapshot ["
                    + snapshotName
                    + "][CreateSnapshotTask{repository="
                    + repositoryName
                    + ", snapshot=*"
                    + snapshotName
                    + "*}]]"
            )
        );
    }

}
