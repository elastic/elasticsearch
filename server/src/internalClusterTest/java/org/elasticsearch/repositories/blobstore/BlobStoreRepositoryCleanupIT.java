/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.repositories.blobstore;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.admin.cluster.repositories.cleanup.CleanupRepositoryResponse;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.RepositoryCleanupInProgress;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFutureThrows;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class BlobStoreRepositoryCleanupIT extends AbstractSnapshotIntegTestCase {

    public void testMasterFailoverDuringCleanup() throws Exception {
        final ActionFuture<CleanupRepositoryResponse> cleanupFuture = startBlockedCleanup("test-repo");

        final int nodeCount = internalCluster().numDataAndMasterNodes();
        logger.info("-->  stopping master node");
        internalCluster().stopCurrentMasterNode();

        ensureStableCluster(nodeCount - 1);

        logger.info("-->  wait for cleanup to finish and disappear from cluster state");
        awaitClusterState(
            state -> state.custom(RepositoryCleanupInProgress.TYPE, RepositoryCleanupInProgress.EMPTY).hasCleanupInProgress() == false
        );

        try {
            cleanupFuture.get();
        } catch (ExecutionException e) {
            // rare case where the master failure triggers a client retry that executes quicker than the removal of the initial
            // cleanup in progress
            final Throwable ise = ExceptionsHelper.unwrap(e, IllegalStateException.class);
            assertThat(ise, instanceOf(IllegalStateException.class));
            assertThat(ise.getMessage(), containsString(" a repository cleanup is already in-progress in "));
        }
    }

    public void testRepeatCleanupsDontRemove() throws Exception {
        final ActionFuture<CleanupRepositoryResponse> cleanupFuture = startBlockedCleanup("test-repo");

        logger.info("-->  sending another cleanup");
        assertFutureThrows(clusterAdmin().prepareCleanupRepository("test-repo").execute(), IllegalStateException.class);

        logger.info("-->  ensure cleanup is still in progress");
        final RepositoryCleanupInProgress cleanup = clusterAdmin().prepareState().get().getState().custom(RepositoryCleanupInProgress.TYPE);
        assertTrue(cleanup.hasCleanupInProgress());

        logger.info("-->  unblocking master node");
        unblockNode("test-repo", internalCluster().getMasterName());

        logger.info("-->  wait for cleanup to finish and disappear from cluster state");
        awaitClusterState(
            state -> state.custom(RepositoryCleanupInProgress.TYPE, RepositoryCleanupInProgress.EMPTY).hasCleanupInProgress() == false
        );

        final ExecutionException e = expectThrows(ExecutionException.class, cleanupFuture::get);
        final Throwable ioe = ExceptionsHelper.unwrap(e, IOException.class);
        assertThat(ioe, instanceOf(IOException.class));
        assertThat(ioe.getMessage(), is("exception after block"));
    }

    private ActionFuture<CleanupRepositoryResponse> startBlockedCleanup(String repoName) throws Exception {
        logger.info("-->  starting two master nodes and one data node");
        internalCluster().startMasterOnlyNodes(2);
        internalCluster().startDataOnlyNodes(1);

        createRepository(repoName, "mock");

        logger.info("-->  snapshot");
        clusterAdmin().prepareCreateSnapshot(repoName, "test-snap").setWaitForCompletion(true).get();

        final BlobStoreRepository repository = getRepositoryOnMaster(repoName);

        logger.info("--> creating a garbage data blob");
        final PlainActionFuture<Void> garbageFuture = PlainActionFuture.newFuture();
        repository.threadPool()
            .generic()
            .execute(
                ActionRunnable.run(
                    garbageFuture,
                    () -> repository.blobStore()
                        .blobContainer(repository.basePath())
                        .writeBlob("snap-foo.dat", new BytesArray(new byte[1]), true)
                )
            );
        garbageFuture.get();

        blockMasterFromFinalizingSnapshotOnIndexFile(repoName);

        logger.info("--> starting repository cleanup");
        // running from a non-master client because shutting down a master while a request to it is pending might result in the future
        // never completing
        final ActionFuture<CleanupRepositoryResponse> future = internalCluster().nonMasterClient()
            .admin()
            .cluster()
            .prepareCleanupRepository(repoName)
            .execute();

        final String masterNode = internalCluster().getMasterName();
        waitForBlock(masterNode, repoName);
        awaitClusterState(
            state -> state.custom(RepositoryCleanupInProgress.TYPE, RepositoryCleanupInProgress.EMPTY).hasCleanupInProgress()
        );
        return future;
    }

    public void testCleanupOldIndexN() throws ExecutionException, InterruptedException {
        internalCluster().startNodes(Settings.EMPTY);

        final String repoName = "test-repo";
        createRepository(repoName, "fs");

        logger.info("--> create three snapshots");
        for (int i = 0; i < 3; ++i) {
            CreateSnapshotResponse createSnapshotResponse = clusterAdmin().prepareCreateSnapshot(repoName, "test-snap-" + i)
                .setWaitForCompletion(true)
                .get();
            assertThat(createSnapshotResponse.getSnapshotInfo().state(), is(SnapshotState.SUCCESS));
        }

        final BlobStoreRepository repository = getRepositoryOnMaster(repoName);
        logger.info("--> write two outdated index-N blobs");
        for (int i = 0; i < 2; ++i) {
            final PlainActionFuture<Void> createOldIndexNFuture = PlainActionFuture.newFuture();
            final int generation = i;
            repository.threadPool()
                .generic()
                .execute(
                    ActionRunnable.run(
                        createOldIndexNFuture,
                        () -> repository.blobStore()
                            .blobContainer(repository.basePath())
                            .writeBlob(BlobStoreRepository.INDEX_FILE_PREFIX + generation, new BytesArray(new byte[1]), true)
                    )
                );
            createOldIndexNFuture.get();
        }

        logger.info("--> cleanup repository");
        clusterAdmin().prepareCleanupRepository(repoName).get();

        BlobStoreTestUtil.assertConsistency(repository);
    }
}
