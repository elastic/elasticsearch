/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.repositories.blobstore;

import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.RepositoryCleanupInProgress;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.ByteArrayInputStream;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFutureThrows;
import static org.hamcrest.Matchers.is;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class BlobStoreRepositoryCleanupIT extends AbstractSnapshotIntegTestCase {

    public void testMasterFailoverDuringCleanup() throws Exception {
        startBlockedCleanup("test-repo");

        logger.info("-->  stopping master node");
        internalCluster().stopCurrentMasterNode();

        logger.info("-->  wait for cleanup to finish and disappear from cluster state");
        assertBusy(() -> {
            RepositoryCleanupInProgress cleanupInProgress =
                client().admin().cluster().prepareState().get().getState().custom(RepositoryCleanupInProgress.TYPE);
            assertFalse(cleanupInProgress.hasCleanupInProgress());
        }, 30, TimeUnit.SECONDS);
    }

    public void testRepeatCleanupsDontRemove() throws Exception {
        final String masterNode = startBlockedCleanup("test-repo");

        logger.info("-->  sending another cleanup");
        assertFutureThrows(client().admin().cluster().prepareCleanupRepository("test-repo").execute(), IllegalStateException.class);

        logger.info("-->  ensure cleanup is still in progress");
        final RepositoryCleanupInProgress cleanup =
            client().admin().cluster().prepareState().get().getState().custom(RepositoryCleanupInProgress.TYPE);
        assertTrue(cleanup.hasCleanupInProgress());

        logger.info("-->  unblocking master node");
        unblockNode("test-repo", masterNode);

        logger.info("-->  wait for cleanup to finish and disappear from cluster state");
        assertBusy(() -> {
            RepositoryCleanupInProgress cleanupInProgress =
                client().admin().cluster().prepareState().get().getState().custom(RepositoryCleanupInProgress.TYPE);
            assertFalse(cleanupInProgress.hasCleanupInProgress());
        }, 30, TimeUnit.SECONDS);
    }

    private String startBlockedCleanup(String repoName) throws Exception {
        logger.info("-->  starting two master nodes and one data node");
        internalCluster().startMasterOnlyNodes(2);
        internalCluster().startDataOnlyNodes(1);

        createRepository(repoName, "mock");

        logger.info("-->  snapshot");
        client().admin().cluster().prepareCreateSnapshot(repoName, "test-snap")
            .setWaitForCompletion(true).get();

        final RepositoriesService service = internalCluster().getInstance(RepositoriesService.class, internalCluster().getMasterName());
        final BlobStoreRepository repository = (BlobStoreRepository) service.repository(repoName);

        logger.info("--> creating a garbage data blob");
        final PlainActionFuture<Void> garbageFuture = PlainActionFuture.newFuture();
        repository.threadPool().generic().execute(ActionRunnable.run(garbageFuture, () -> repository.blobStore()
            .blobContainer(repository.basePath()).writeBlob("snap-foo.dat", new ByteArrayInputStream(new byte[1]), 1, true)));
        garbageFuture.get();

        final String masterNode = blockMasterFromFinalizingSnapshotOnIndexFile(repoName);

        logger.info("--> starting repository cleanup");
        client().admin().cluster().prepareCleanupRepository(repoName).execute();

        logger.info("--> waiting for block to kick in on " + masterNode);
        waitForBlock(masterNode, repoName, TimeValue.timeValueSeconds(60));
        return masterNode;
    }

    public void testCleanupOldIndexN() throws ExecutionException, InterruptedException {
        internalCluster().startNodes(Settings.EMPTY);

        final String repoName = "test-repo";
        createRepository(repoName, "fs");

        logger.info("--> create three snapshots");
        for (int i = 0; i < 3; ++i) {
            CreateSnapshotResponse createSnapshotResponse = client().admin().cluster().prepareCreateSnapshot(repoName, "test-snap-" + i)
                .setWaitForCompletion(true).get();
            assertThat(createSnapshotResponse.getSnapshotInfo().state(), is(SnapshotState.SUCCESS));
        }

        final RepositoriesService service = internalCluster().getInstance(RepositoriesService.class, internalCluster().getMasterName());
        final BlobStoreRepository repository = (BlobStoreRepository) service.repository(repoName);

        logger.info("--> write two outdated index-N blobs");
        for (int i = 0; i < 2; ++i) {
            final PlainActionFuture<Void> createOldIndexNFuture = PlainActionFuture.newFuture();
            final int generation = i;
            repository.threadPool().generic().execute(ActionRunnable.run(createOldIndexNFuture, () -> repository.blobStore()
                .blobContainer(repository.basePath()).writeBlob(BlobStoreRepository.INDEX_FILE_PREFIX + generation,
                    new ByteArrayInputStream(new byte[1]), 1, true)));
            createOldIndexNFuture.get();
        }

        logger.info("--> cleanup repository");
        client().admin().cluster().prepareCleanupRepository(repoName).get();

        BlobStoreTestUtil.assertConsistency(repository, repository.threadPool().generic());
    }
}
