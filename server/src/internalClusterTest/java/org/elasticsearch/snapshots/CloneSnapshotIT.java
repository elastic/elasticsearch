/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.snapshots;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotIndexStatus;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotStatus;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshots;
import org.elasticsearch.index.snapshots.blobstore.SnapshotFiles;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.RepositoryShardId;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.repositories.ShardSnapshotResult;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class CloneSnapshotIT extends AbstractSnapshotIntegTestCase {

    public void testShardClone() throws Exception {
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();
        final String repoName = "repo-name";
        final Path repoPath = randomRepoPath();
        createRepository(repoName, "fs", repoPath);

        final boolean useBwCFormat = randomBoolean();
        if (useBwCFormat) {
            initWithSnapshotVersion(repoName, repoPath, SnapshotsService.OLD_SNAPSHOT_FORMAT);
        }

        final String indexName = "test-index";
        createIndexWithRandomDocs(indexName, randomIntBetween(5, 10));
        final String sourceSnapshot = "source-snapshot";
        final SnapshotInfo sourceSnapshotInfo = createFullSnapshot(repoName, sourceSnapshot);

        final BlobStoreRepository repository = getRepositoryOnMaster(repoName);
        final RepositoryData repositoryData = getRepositoryData(repoName);
        final IndexId indexId = repositoryData.resolveIndexId(indexName);
        final int shardId = 0;
        final RepositoryShardId repositoryShardId = new RepositoryShardId(indexId, shardId);

        final SnapshotId targetSnapshotId = new SnapshotId("target-snapshot", UUIDs.randomBase64UUID(random()));

        final ShardGeneration currentShardGen;
        if (useBwCFormat) {
            currentShardGen = null;
        } else {
            currentShardGen = repositoryData.shardGenerations().getShardGen(indexId, shardId);
        }
        final ShardSnapshotResult shardSnapshotResult = safeAwait(
            listener -> repository.cloneShardSnapshot(
                sourceSnapshotInfo.snapshotId(),
                targetSnapshotId,
                repositoryShardId,
                currentShardGen,
                listener
            )
        );
        final ShardGeneration newShardGeneration = shardSnapshotResult.getGeneration();

        if (useBwCFormat) {
            assertEquals(newShardGeneration, new ShardGeneration(1L)); // Initial snapshot brought it to 0, clone increments it to 1
        }

        final BlobStoreIndexShardSnapshot targetShardSnapshot = readShardSnapshot(repository, repositoryShardId, targetSnapshotId);
        final BlobStoreIndexShardSnapshot sourceShardSnapshot = readShardSnapshot(
            repository,
            repositoryShardId,
            sourceSnapshotInfo.snapshotId()
        );
        assertThat(targetShardSnapshot.incrementalFileCount(), is(0));
        final List<BlobStoreIndexShardSnapshot.FileInfo> sourceFiles = sourceShardSnapshot.indexFiles();
        final List<BlobStoreIndexShardSnapshot.FileInfo> targetFiles = targetShardSnapshot.indexFiles();
        final int fileCount = sourceFiles.size();
        assertEquals(fileCount, targetFiles.size());
        for (int i = 0; i < fileCount; i++) {
            assertTrue(sourceFiles.get(i).isSame(targetFiles.get(i)));
        }
        final BlobStoreIndexShardSnapshots shardMetadata = readShardGeneration(repository, repositoryShardId, newShardGeneration);
        final List<SnapshotFiles> snapshotFiles = shardMetadata.snapshots();
        assertThat(snapshotFiles, hasSize(2));
        assertTrue(snapshotFiles.get(0).isSame(snapshotFiles.get(1)));

        // verify that repeated cloning is idempotent
        final ShardSnapshotResult shardSnapshotResult2 = safeAwait(
            listener -> repository.cloneShardSnapshot(
                sourceSnapshotInfo.snapshotId(),
                targetSnapshotId,
                repositoryShardId,
                newShardGeneration,
                listener
            )
        );
        assertEquals(newShardGeneration, shardSnapshotResult2.getGeneration());
        assertEquals(shardSnapshotResult.getSegmentCount(), shardSnapshotResult2.getSegmentCount());
        assertEquals(shardSnapshotResult.getSize(), shardSnapshotResult2.getSize());
    }

    public void testCloneSnapshotIndex() throws Exception {
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();
        final String repoName = "repo-name";
        createRepository(repoName, "fs");

        final String indexName = "index-1";
        createIndexWithRandomDocs(indexName, randomIntBetween(5, 10));
        final String sourceSnapshot = "source-snapshot";
        createFullSnapshot(repoName, sourceSnapshot);

        indexRandomDocs(indexName, randomIntBetween(20, 100));
        if (randomBoolean()) {
            assertAcked(indicesAdmin().prepareDelete(indexName));
        }
        final String targetSnapshot = "target-snapshot";
        assertAcked(startClone(repoName, sourceSnapshot, targetSnapshot, indexName).get());

        final List<SnapshotStatus> status = clusterAdmin().prepareSnapshotStatus(TEST_REQUEST_TIMEOUT, repoName)
            .setSnapshots(sourceSnapshot, targetSnapshot)
            .get()
            .getSnapshots();
        assertThat(status, hasSize(2));
        final SnapshotIndexStatus status1 = status.get(0).getIndices().get(indexName);
        final SnapshotIndexStatus status2 = status.get(1).getIndices().get(indexName);
        assertEquals(status1.getStats().getTotalFileCount(), status2.getStats().getTotalFileCount());
        assertEquals(status1.getStats().getTotalSize(), status2.getStats().getTotalSize());
    }

    public void testClonePreventsSnapshotDelete() throws Exception {
        final String masterName = internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();
        final String repoName = "repo-name";
        createRepository(repoName, "mock");

        final String indexName = "index-1";
        createIndexWithRandomDocs(indexName, randomIntBetween(5, 10));
        final String sourceSnapshot = "source-snapshot";
        createFullSnapshot(repoName, sourceSnapshot);

        indexRandomDocs(indexName, randomIntBetween(20, 100));

        final String targetSnapshot = "target-snapshot";
        blockNodeOnAnyFiles(repoName, masterName);
        final ActionFuture<AcknowledgedResponse> cloneFuture = startClone(repoName, sourceSnapshot, targetSnapshot, indexName);
        waitForBlock(masterName, repoName);
        assertFalse(cloneFuture.isDone());

        ConcurrentSnapshotExecutionException ex = expectThrows(
            ConcurrentSnapshotExecutionException.class,
            startDeleteSnapshot(repoName, sourceSnapshot)
        );
        assertThat(ex.getMessage(), containsString("cannot delete snapshot while it is being cloned"));

        unblockNode(repoName, masterName);
        assertAcked(cloneFuture.get());
        final List<SnapshotStatus> status = clusterAdmin().prepareSnapshotStatus(TEST_REQUEST_TIMEOUT, repoName)
            .setSnapshots(sourceSnapshot, targetSnapshot)
            .get()
            .getSnapshots();
        assertThat(status, hasSize(2));
        final SnapshotIndexStatus status1 = status.get(0).getIndices().get(indexName);
        final SnapshotIndexStatus status2 = status.get(1).getIndices().get(indexName);
        assertEquals(status1.getStats().getTotalFileCount(), status2.getStats().getTotalFileCount());
        assertEquals(status1.getStats().getTotalSize(), status2.getStats().getTotalSize());
    }

    public void testConcurrentCloneAndSnapshot() throws Exception {
        internalCluster().startMasterOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();
        final String repoName = "repo-name";
        createRepository(repoName, "mock");

        final String indexName = "index-1";
        createIndexWithRandomDocs(indexName, randomIntBetween(5, 10));
        final String sourceSnapshot = "source-snapshot";
        createFullSnapshot(repoName, sourceSnapshot);

        indexRandomDocs(indexName, randomIntBetween(20, 100));

        final String targetSnapshot = "target-snapshot";
        final ActionFuture<CreateSnapshotResponse> snapshot2Future = startFullSnapshotBlockedOnDataNode("snapshot-2", repoName, dataNode);
        waitForBlock(dataNode, repoName);
        final ActionFuture<AcknowledgedResponse> cloneFuture = startClone(repoName, sourceSnapshot, targetSnapshot, indexName);
        awaitNumberOfSnapshotsInProgress(2);
        unblockNode(repoName, dataNode);
        assertAcked(cloneFuture.get());
        assertSuccessful(snapshot2Future);
    }

    public void testLongRunningCloneAllowsConcurrentSnapshot() throws Exception {
        // large snapshot pool so blocked snapshot threads from cloning don't prevent concurrent snapshot finalizations
        final String masterNode = internalCluster().startMasterOnlyNode(LARGE_SNAPSHOT_POOL_SETTINGS);
        internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");
        final String indexSlow = "index-slow";
        createIndexWithContent(indexSlow);

        final String sourceSnapshot = "source-snapshot";
        createFullSnapshot(repoName, sourceSnapshot);

        final String targetSnapshot = "target-snapshot";
        blockMasterOnShardClone(repoName);
        final ActionFuture<AcknowledgedResponse> cloneFuture = startClone(repoName, sourceSnapshot, targetSnapshot, indexSlow);
        waitForBlock(masterNode, repoName);

        final String indexFast = "index-fast";
        createIndexWithRandomDocs(indexFast, randomIntBetween(20, 100));

        assertSuccessful(
            clusterAdmin().prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repoName, "fast-snapshot")
                .setIndices(indexFast)
                .setWaitForCompletion(true)
                .execute()
        );

        assertThat(cloneFuture.isDone(), is(false));
        unblockNode(repoName, masterNode);

        assertAcked(cloneFuture.get());
    }

    public void testLongRunningSnapshotAllowsConcurrentClone() throws Exception {
        internalCluster().startMasterOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");
        final String indexSlow = "index-slow";
        createIndexWithContent(indexSlow);

        final String sourceSnapshot = "source-snapshot";
        createFullSnapshot(repoName, sourceSnapshot);

        final String indexFast = "index-fast";
        createIndexWithRandomDocs(indexFast, randomIntBetween(20, 100));

        blockDataNode(repoName, dataNode);
        final ActionFuture<CreateSnapshotResponse> snapshotFuture = clusterAdmin().prepareCreateSnapshot(
            TEST_REQUEST_TIMEOUT,
            repoName,
            "fast-snapshot"
        ).setIndices(indexFast).setWaitForCompletion(true).execute();
        waitForBlock(dataNode, repoName);

        final String targetSnapshot = "target-snapshot";
        assertAcked(startClone(repoName, sourceSnapshot, targetSnapshot, indexSlow).get());

        assertThat(snapshotFuture.isDone(), is(false));
        unblockNode(repoName, dataNode);

        assertSuccessful(snapshotFuture);
    }

    public void testDeletePreventsClone() throws Exception {
        final String masterName = internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();
        final String repoName = "repo-name";
        createRepository(repoName, "mock");

        final String indexName = "index-1";
        createIndexWithRandomDocs(indexName, randomIntBetween(5, 10));
        final String sourceSnapshot = "source-snapshot";
        createFullSnapshot(repoName, sourceSnapshot);

        indexRandomDocs(indexName, randomIntBetween(20, 100));

        final String targetSnapshot = "target-snapshot";
        blockNodeOnAnyFiles(repoName, masterName);
        final ActionFuture<AcknowledgedResponse> deleteFuture = startDeleteSnapshot(repoName, sourceSnapshot);
        waitForBlock(masterName, repoName);
        assertFalse(deleteFuture.isDone());

        ConcurrentSnapshotExecutionException ex = expectThrows(
            ConcurrentSnapshotExecutionException.class,
            startClone(repoName, sourceSnapshot, targetSnapshot, indexName)
        );
        assertThat(ex.getMessage(), containsString("cannot clone from snapshot that is being deleted"));

        unblockNode(repoName, masterName);
        assertAcked(deleteFuture.get());
    }

    public void testBackToBackClonesForIndexNotInCluster() throws Exception {
        // large snapshot pool so blocked snapshot threads from cloning don't prevent concurrent snapshot finalizations
        final String masterNode = internalCluster().startMasterOnlyNode(LARGE_SNAPSHOT_POOL_SETTINGS);
        internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");
        final String indexBlocked = "index-blocked";
        createIndexWithContent(indexBlocked);

        final String sourceSnapshot = "source-snapshot";
        createFullSnapshot(repoName, sourceSnapshot);

        assertAcked(indicesAdmin().prepareDelete(indexBlocked).get());

        final String targetSnapshot1 = "target-snapshot";
        blockMasterOnShardClone(repoName);
        final ActionFuture<AcknowledgedResponse> cloneFuture1 = startClone(repoName, sourceSnapshot, targetSnapshot1, indexBlocked);
        waitForBlock(masterNode, repoName);
        assertThat(cloneFuture1.isDone(), is(false));

        final int extraClones = randomIntBetween(1, 5);
        final List<ActionFuture<AcknowledgedResponse>> extraCloneFutures = new ArrayList<>(extraClones);
        final boolean slowInitClones = extraClones > 1 && randomBoolean();
        if (slowInitClones) {
            blockMasterOnReadIndexMeta(repoName);
        }
        for (int i = 0; i < extraClones; i++) {
            extraCloneFutures.add(startClone(repoName, sourceSnapshot, "target-snapshot-" + i, indexBlocked));
        }
        awaitNumberOfSnapshotsInProgress(1 + extraClones);
        for (ActionFuture<AcknowledgedResponse> extraCloneFuture : extraCloneFutures) {
            assertFalse(extraCloneFuture.isDone());
        }

        final int extraSnapshots = randomIntBetween(0, 5);
        if (extraSnapshots > 0) {
            createIndexWithContent(indexBlocked);
        }

        final List<ActionFuture<CreateSnapshotResponse>> extraSnapshotFutures = new ArrayList<>(extraSnapshots);
        for (int i = 0; i < extraSnapshots; i++) {
            extraSnapshotFutures.add(startFullSnapshot(repoName, "extra-snap-" + i));
        }

        awaitNumberOfSnapshotsInProgress(1 + extraClones + extraSnapshots);
        for (ActionFuture<CreateSnapshotResponse> extraSnapshotFuture : extraSnapshotFutures) {
            assertFalse(extraSnapshotFuture.isDone());
        }

        unblockNode(repoName, masterNode);
        assertAcked(cloneFuture1.get());

        for (ActionFuture<AcknowledgedResponse> extraCloneFuture : extraCloneFutures) {
            assertAcked(extraCloneFuture.get());
        }
        for (ActionFuture<CreateSnapshotResponse> extraSnapshotFuture : extraSnapshotFutures) {
            assertSuccessful(extraSnapshotFuture);
        }
    }

    public void testMasterFailoverDuringCloneStep1() throws Exception {
        internalCluster().startMasterOnlyNodes(3);
        internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");
        final String testIndex = "index-test";
        createIndexWithContent(testIndex);

        final String sourceSnapshot = "source-snapshot";
        createFullSnapshot(repoName, sourceSnapshot);

        blockMasterOnReadIndexMeta(repoName);
        final String cloneName = "target-snapshot";
        final ActionFuture<AcknowledgedResponse> cloneFuture = startCloneFromDataNode(repoName, sourceSnapshot, cloneName, testIndex);
        awaitNumberOfSnapshotsInProgress(1);
        final String masterNode = internalCluster().getMasterName();
        waitForBlock(masterNode, repoName);
        internalCluster().restartNode(masterNode);
        boolean cloneSucceeded = false;
        try {
            cloneFuture.actionGet(TimeValue.timeValueSeconds(30L));
            cloneSucceeded = true;
        } catch (SnapshotException sne) {
            // ignored, most of the time we will throw here but we could randomly run into a situation where the data node retries the
            // snapshot on disconnect slowly enough for it to work out
        }

        awaitNoMoreRunningOperations();

        // Check if the clone operation worked out by chance as a result of the clone request being retried because of the master failover
        cloneSucceeded = cloneSucceeded
            || getRepositoryData(repoName).getSnapshotIds().stream().anyMatch(snapshotId -> snapshotId.getName().equals(cloneName));
        assertAllSnapshotsSuccessful(getRepositoryData(repoName), cloneSucceeded ? 2 : 1);
    }

    public void testFailsOnCloneMissingIndices() {
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();
        final String repoName = "repo-name";
        final Path repoPath = randomRepoPath();
        if (randomBoolean()) {
            createIndexWithContent("test-idx");
        }
        createRepository(repoName, "fs", repoPath);

        final String snapshotName = "snapshot";
        createFullSnapshot(repoName, snapshotName);
        expectThrows(IndexNotFoundException.class, startClone(repoName, snapshotName, "target-snapshot", "does-not-exist"));
    }

    public void testMasterFailoverDuringCloneStep2() throws Exception {
        // large snapshot pool so blocked snapshot threads from cloning don't prevent concurrent snapshot finalizations
        internalCluster().startMasterOnlyNodes(3, LARGE_SNAPSHOT_POOL_SETTINGS);
        internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");
        final String testIndex = "index-test";
        createIndexWithContent(testIndex);

        final String sourceSnapshot = "source-snapshot";
        createFullSnapshot(repoName, sourceSnapshot);

        final String targetSnapshot = "target-snapshot";
        blockMasterOnShardClone(repoName);
        final ActionFuture<AcknowledgedResponse> cloneFuture = startCloneFromDataNode(repoName, sourceSnapshot, targetSnapshot, testIndex);
        awaitNumberOfSnapshotsInProgress(1);
        final String masterNode = internalCluster().getMasterName();
        waitForBlock(masterNode, repoName);
        internalCluster().restartNode(masterNode);
        expectThrows(SnapshotException.class, cloneFuture);
        awaitNoMoreRunningOperations();

        assertAllSnapshotsSuccessful(getRepositoryData(repoName), 2);
    }

    public void testExceptionDuringShardClone() throws Exception {
        // large snapshot pool so blocked snapshot threads from cloning don't prevent concurrent snapshot finalizations
        internalCluster().startMasterOnlyNodes(3, LARGE_SNAPSHOT_POOL_SETTINGS);
        internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");
        final String testIndex = "index-test";
        createIndexWithContent(testIndex);

        final String sourceSnapshot = "source-snapshot";
        createFullSnapshot(repoName, sourceSnapshot);

        final String targetSnapshot = "target-snapshot";
        blockMasterFromFinalizingSnapshotOnSnapFile(repoName);
        final ActionFuture<AcknowledgedResponse> cloneFuture = startCloneFromDataNode(repoName, sourceSnapshot, targetSnapshot, testIndex);
        awaitNumberOfSnapshotsInProgress(1);
        final String masterNode = internalCluster().getMasterName();
        waitForBlock(masterNode, repoName);
        unblockNode(repoName, masterNode);
        expectThrows(SnapshotException.class, cloneFuture);
        awaitNoMoreRunningOperations();
        assertAllSnapshotsSuccessful(getRepositoryData(repoName), 1);
        assertAcked(startDeleteSnapshot(repoName, sourceSnapshot).get());
    }

    public void testDoesNotStartOnBrokenSourceSnapshot() throws Exception {
        internalCluster().startMasterOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");
        final String testIndex = "index-test";
        createIndexWithContent(testIndex);

        final String sourceSnapshot = "source-snapshot";
        blockDataNode(repoName, dataNode);
        final Client masterClient = internalCluster().masterClient();
        final ActionFuture<CreateSnapshotResponse> sourceSnapshotFuture = masterClient.admin()
            .cluster()
            .prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repoName, sourceSnapshot)
            .setWaitForCompletion(true)
            .execute();
        awaitNumberOfSnapshotsInProgress(1);
        waitForBlock(dataNode, repoName);
        internalCluster().restartNode(dataNode);
        assertThat(sourceSnapshotFuture.get().getSnapshotInfo().state(), is(SnapshotState.PARTIAL));

        final SnapshotException sne = expectThrows(
            SnapshotException.class,
            startClone(masterClient, repoName, sourceSnapshot, "target-snapshot", testIndex)
        );
        assertThat(
            sne.getMessage(),
            containsString(
                "Can't clone index [" + getRepositoryData(repoName).resolveIndexId(testIndex) + "] because its snapshot was not successful."
            )
        );
    }

    public void testSnapshotQueuedAfterCloneFromBrokenSourceSnapshot() throws Exception {
        internalCluster().startMasterOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");
        final String testIndex = "index-test";
        createIndexWithContent(testIndex);

        final String sourceSnapshot = "source-snapshot";
        blockDataNode(repoName, dataNode);
        final Client masterClient = internalCluster().masterClient();
        final ActionFuture<CreateSnapshotResponse> sourceSnapshotFuture = masterClient.admin()
            .cluster()
            .prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repoName, sourceSnapshot)
            .setWaitForCompletion(true)
            .execute();
        awaitNumberOfSnapshotsInProgress(1);
        waitForBlock(dataNode, repoName);
        internalCluster().restartNode(dataNode);
        ensureGreen();
        assertThat(sourceSnapshotFuture.get().getSnapshotInfo().state(), is(SnapshotState.PARTIAL));
        final String sourceSnapshotHealthy = "source-snapshot-healthy";
        createFullSnapshot(repoName, "source-snapshot-healthy");

        final ActionFuture<CreateSnapshotResponse> sn1 = startFullSnapshot(repoName, "concurrent-snapshot-1");
        final ActionFuture<AcknowledgedResponse> clone1 = startClone(
            masterClient,
            repoName,
            sourceSnapshotHealthy,
            "target-snapshot-1",
            testIndex
        );
        final ActionFuture<CreateSnapshotResponse> sn2 = startFullSnapshot(repoName, "concurrent-snapshot-2");
        final ActionFuture<AcknowledgedResponse> clone2 = startClone(
            masterClient,
            repoName,
            sourceSnapshotHealthy,
            "target-snapshot-2",
            testIndex
        );
        final ActionFuture<CreateSnapshotResponse> sn3 = startFullSnapshot(repoName, "concurrent-snapshot-3");
        final ActionFuture<AcknowledgedResponse> clone3 = startClone(
            masterClient,
            repoName,
            sourceSnapshotHealthy,
            "target-snapshot-3",
            testIndex
        );
        final SnapshotException sne = expectThrows(
            SnapshotException.class,
            startClone(masterClient, repoName, sourceSnapshot, "target-snapshot", testIndex)
        );
        assertThat(
            sne.getMessage(),
            containsString(
                "Can't clone index [" + getRepositoryData(repoName).resolveIndexId(testIndex) + "] because its snapshot was not successful."
            )
        );

        assertSuccessful(sn1);
        assertSuccessful(sn2);
        assertSuccessful(sn3);
        assertAcked(clone1.get());
        assertAcked(clone2.get());
        assertAcked(clone3.get());
    }

    public void testStartSnapshotWithSuccessfulShardClonePendingFinalization() throws Exception {
        final String masterName = internalCluster().startMasterOnlyNode(LARGE_SNAPSHOT_POOL_SETTINGS);
        final String dataNode = internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");

        final String indexName = "test-idx";
        createIndexWithContent(indexName);

        final String sourceSnapshot = "source-snapshot";
        createFullSnapshot(repoName, sourceSnapshot);

        blockMasterOnWriteIndexFile(repoName);
        final String cloneName = "clone-blocked";
        final ActionFuture<AcknowledgedResponse> blockedClone = startClone(repoName, sourceSnapshot, cloneName, indexName);
        waitForBlock(masterName, repoName);
        awaitNumberOfSnapshotsInProgress(1);
        blockNodeOnAnyFiles(repoName, dataNode);
        final ActionFuture<CreateSnapshotResponse> otherSnapshot = startFullSnapshot(repoName, "other-snapshot");
        awaitNumberOfSnapshotsInProgress(2);
        assertFalse(blockedClone.isDone());
        unblockNode(repoName, masterName);
        awaitNumberOfSnapshotsInProgress(1);
        awaitMasterFinishRepoOperations();
        unblockNode(repoName, dataNode);
        assertAcked(blockedClone.get());
        assertEquals(getSnapshot(repoName, cloneName).state(), SnapshotState.SUCCESS);
        assertSuccessful(otherSnapshot);
    }

    public void testStartCloneWithSuccessfulShardClonePendingFinalization() throws Exception {
        final String masterName = internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");

        final String indexName = "test-idx";
        createIndexWithContent(indexName);

        final String sourceSnapshot = "source-snapshot";
        createFullSnapshot(repoName, sourceSnapshot);

        blockMasterOnWriteIndexFile(repoName);
        final String cloneName = "clone-blocked";
        final ActionFuture<AcknowledgedResponse> blockedClone = startClone(repoName, sourceSnapshot, cloneName, indexName);
        waitForBlock(masterName, repoName);
        awaitNumberOfSnapshotsInProgress(1);
        final String otherCloneName = "other-clone";
        final ActionFuture<AcknowledgedResponse> otherClone = startClone(repoName, sourceSnapshot, otherCloneName, indexName);
        awaitNumberOfSnapshotsInProgress(2);
        assertFalse(blockedClone.isDone());
        unblockNode(repoName, masterName);
        awaitNoMoreRunningOperations(masterName);
        awaitMasterFinishRepoOperations();
        assertAcked(blockedClone.get());
        assertAcked(otherClone.get());
        assertEquals(getSnapshot(repoName, cloneName).state(), SnapshotState.SUCCESS);
        assertEquals(getSnapshot(repoName, otherCloneName).state(), SnapshotState.SUCCESS);
    }

    public void testStartCloneWithSuccessfulShardSnapshotPendingFinalization() throws Exception {
        final String masterName = internalCluster().startMasterOnlyNode(LARGE_SNAPSHOT_POOL_SETTINGS);
        internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");

        final String indexName = "test-idx";
        createIndexWithContent(indexName);

        final String sourceSnapshot = "source-snapshot";
        createFullSnapshot(repoName, sourceSnapshot);

        blockMasterOnWriteIndexFile(repoName);
        final ActionFuture<CreateSnapshotResponse> blockedSnapshot = startFullSnapshot(repoName, "snap-blocked");
        waitForBlock(masterName, repoName);
        awaitNumberOfSnapshotsInProgress(1);
        final String cloneName = "clone";
        final ActionFuture<AcknowledgedResponse> clone = startClone(repoName, sourceSnapshot, cloneName, indexName);
        logger.info("--> wait for clone to start fully with shards assigned in the cluster state");
        try {
            awaitClusterState(clusterState -> {
                final List<SnapshotsInProgress.Entry> entries = SnapshotsInProgress.get(clusterState).forRepo(repoName);
                return entries.size() == 2 && entries.get(1).shardSnapshotStatusByRepoShardId().isEmpty() == false;
            });
            assertFalse(blockedSnapshot.isDone());
        } finally {
            unblockNode(repoName, masterName);
        }
        awaitNoMoreRunningOperations();

        awaitMasterFinishRepoOperations();

        assertSuccessful(blockedSnapshot);
        assertAcked(clone.get());
        assertEquals(getSnapshot(repoName, cloneName).state(), SnapshotState.SUCCESS);
    }

    public void testStartCloneDuringRunningDelete() throws Exception {
        final String masterName = internalCluster().startMasterOnlyNode(LARGE_SNAPSHOT_POOL_SETTINGS);
        internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");

        final String indexName = "test-idx";
        createIndexWithContent(indexName);

        final String sourceSnapshot = "source-snapshot";
        createFullSnapshot(repoName, sourceSnapshot);

        final List<String> snapshotNames = createNSnapshots(repoName, randomIntBetween(1, 5));
        blockMasterOnWriteIndexFile(repoName);
        final ActionFuture<AcknowledgedResponse> deleteFuture = startDeleteSnapshot(repoName, randomFrom(snapshotNames));
        waitForBlock(masterName, repoName);
        awaitNDeletionsInProgress(1);

        final ActionFuture<AcknowledgedResponse> cloneFuture = startClone(repoName, sourceSnapshot, "target-snapshot", indexName);
        logger.info("--> waiting for snapshot clone to be fully initialized");
        awaitClusterState(state -> {
            for (SnapshotsInProgress.Entry entry : SnapshotsInProgress.get(state).forRepo(repoName)) {
                if (entry.shardSnapshotStatusByRepoShardId().isEmpty() == false) {
                    assertEquals(sourceSnapshot, entry.source().getName());
                    for (SnapshotsInProgress.ShardSnapshotStatus value : entry.shardSnapshotStatusByRepoShardId().values()) {
                        assertSame(value, SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED);
                    }
                    return true;
                }
            }
            return false;
        });
        unblockNode(repoName, masterName);
        assertAcked(deleteFuture.get());
        assertAcked(cloneFuture.get());
    }

    public void testManyConcurrentClonesStartOutOfOrder() throws Exception {
        // large snapshot pool to allow for concurrently finishing clone while another clone is blocked on trying to load SnapshotInfo
        final String masterName = internalCluster().startMasterOnlyNode(LARGE_SNAPSHOT_POOL_SETTINGS);
        internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");
        final String testIndex = "test-idx";
        createIndexWithContent(testIndex);

        final String sourceSnapshot = "source-snapshot";
        createFullSnapshot(repoName, sourceSnapshot);
        assertAcked(indicesAdmin().prepareDelete(testIndex).get());

        final MockRepository repo = getRepositoryOnMaster(repoName);
        repo.setBlockOnceOnReadSnapshotInfoIfAlreadyBlocked();
        repo.setBlockOnWriteIndexFile();

        final ActionFuture<AcknowledgedResponse> clone1 = startClone(repoName, sourceSnapshot, "target-snapshot-1", testIndex);
        // wait for this snapshot to show up in the cluster state
        awaitNumberOfSnapshotsInProgress(1);
        waitForBlock(masterName, repoName);

        final ActionFuture<AcknowledgedResponse> clone2 = startClone(repoName, sourceSnapshot, "target-snapshot-2", testIndex);

        awaitNumberOfSnapshotsInProgress(2);
        awaitClusterState(state -> SnapshotsInProgress.get(state).forRepo(repoName).stream().anyMatch(entry -> entry.state().completed()));
        repo.unblock();

        assertAcked(clone1.get());
        assertAcked(clone2.get());
    }

    public void testRemoveFailedCloneFromCSWithoutIO() throws Exception {
        final String masterNode = internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");
        final String testIndex = "index-test";
        createIndexWithContent(testIndex);

        final String sourceSnapshot = "source-snapshot";
        createFullSnapshot(repoName, sourceSnapshot);

        final String targetSnapshot = "target-snapshot";
        blockAndFailMasterOnShardClone(repoName);
        final ActionFuture<AcknowledgedResponse> cloneFuture = startClone(repoName, sourceSnapshot, targetSnapshot, testIndex);
        awaitNumberOfSnapshotsInProgress(1);
        waitForBlock(masterNode, repoName);
        unblockNode(repoName, masterNode);
        expectThrows(SnapshotException.class, cloneFuture);
        awaitNoMoreRunningOperations();
        assertAllSnapshotsSuccessful(getRepositoryData(repoName), 1);
        assertAcked(startDeleteSnapshot(repoName, sourceSnapshot).get());
    }

    public void testRemoveFailedCloneFromCSWithQueuedSnapshotInProgress() throws Exception {
        // single threaded master snapshot pool so we can selectively fail part of a clone by letting it run shard by shard
        final String masterNode = internalCluster().startMasterOnlyNode(
            Settings.builder().put("thread_pool.snapshot.core", 1).put("thread_pool.snapshot.max", 1).build()
        );
        final String dataNode = internalCluster().startDataOnlyNode(LARGE_SNAPSHOT_POOL_SETTINGS);
        final String repoName = "test-repo";
        createRepository(repoName, "mock");
        final String testIndex = "index-test";
        final String testIndex2 = "index-test-2";
        createIndexWithContent(testIndex);
        createIndexWithContent(testIndex2);

        final String sourceSnapshot = "source-snapshot";
        createFullSnapshot(repoName, sourceSnapshot);

        final String targetSnapshot = "target-snapshot";
        blockAndFailMasterOnShardClone(repoName);

        createIndexWithContent("test-index-3");
        blockDataNode(repoName, dataNode);
        final ActionFuture<CreateSnapshotResponse> fullSnapshotFuture1 = startFullSnapshot(repoName, "full-snapshot-1");
        waitForBlock(dataNode, repoName);
        // make sure we don't have so many files in the shard that will get blocked to fully clog up the snapshot pool on the data node
        final var files = indicesAdmin().prepareStats("test-index-3")
            .setSegments(true)
            .setIncludeSegmentFileSizes(true)
            .get()
            .getPrimaries()
            .getSegments()
            .getFiles();
        assertThat(files.size(), lessThan(LARGE_POOL_SIZE));
        final ActionFuture<AcknowledgedResponse> cloneFuture = startClone(repoName, sourceSnapshot, targetSnapshot, testIndex, testIndex2);
        awaitNumberOfSnapshotsInProgress(2);
        waitForBlock(masterNode, repoName);
        unblockNode(repoName, masterNode);
        final ActionFuture<CreateSnapshotResponse> fullSnapshotFuture2 = startFullSnapshot(repoName, "full-snapshot-2");
        expectThrows(SnapshotException.class, cloneFuture);
        unblockNode(repoName, dataNode);
        awaitNoMoreRunningOperations();
        assertSuccessful(fullSnapshotFuture1);
        assertSuccessful(fullSnapshotFuture2);
        assertAllSnapshotsSuccessful(getRepositoryData(repoName), 3);
        assertAcked(startDeleteSnapshot(repoName, sourceSnapshot).get());
    }

    public void testCloneAfterFailedShardSnapshot() throws Exception {
        final String masterNode = internalCluster().startMasterOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");
        final String testIndex = "index-test";
        createIndex(testIndex);
        final String sourceSnapshot = "source-snapshot";
        createFullSnapshot(repoName, sourceSnapshot);
        indexRandomDocs(testIndex, randomIntBetween(1, 100));
        blockDataNode(repoName, dataNode);
        final ActionFuture<CreateSnapshotResponse> snapshotFuture = client(masterNode).admin()
            .cluster()
            .prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repoName, "full-snapshot")
            .execute();
        awaitNumberOfSnapshotsInProgress(1);
        waitForBlock(dataNode, repoName);
        final ActionFuture<AcknowledgedResponse> cloneFuture = client(masterNode).admin()
            .cluster()
            .prepareCloneSnapshot(TEST_REQUEST_TIMEOUT, repoName, sourceSnapshot, "target-snapshot")
            .setIndices(testIndex)
            .execute();
        awaitNumberOfSnapshotsInProgress(2);
        internalCluster().stopNode(dataNode);
        assertAcked(cloneFuture.get());
        assertTrue(snapshotFuture.isDone());
    }

    private ActionFuture<AcknowledgedResponse> startCloneFromDataNode(
        String repoName,
        String sourceSnapshot,
        String targetSnapshot,
        String... indices
    ) {
        return startClone(dataNodeClient(), repoName, sourceSnapshot, targetSnapshot, indices);
    }

    private ActionFuture<AcknowledgedResponse> startClone(
        String repoName,
        String sourceSnapshot,
        String targetSnapshot,
        String... indices
    ) {
        return startClone(client(), repoName, sourceSnapshot, targetSnapshot, indices);
    }

    private static ActionFuture<AcknowledgedResponse> startClone(
        Client client,
        String repoName,
        String sourceSnapshot,
        String targetSnapshot,
        String... indices
    ) {
        return client.admin()
            .cluster()
            .prepareCloneSnapshot(TEST_REQUEST_TIMEOUT, repoName, sourceSnapshot, targetSnapshot)
            .setIndices(indices)
            .execute();
    }

    private void blockMasterOnReadIndexMeta(String repoName) {
        AbstractSnapshotIntegTestCase.<MockRepository>getRepositoryOnMaster(repoName).setBlockOnReadIndexMeta();
    }

    private void blockMasterOnShardClone(String repoName) {
        AbstractSnapshotIntegTestCase.<MockRepository>getRepositoryOnMaster(repoName).setBlockOnWriteShardLevelMeta();
    }

    private void blockAndFailMasterOnShardClone(String repoName) {
        AbstractSnapshotIntegTestCase.<MockRepository>getRepositoryOnMaster(repoName).setBlockAndFailOnWriteShardLevelMeta();
    }

    /**
     * Assert that given {@link RepositoryData} contains exactly the given number of snapshots and all of them are successful.
     */
    private static void assertAllSnapshotsSuccessful(RepositoryData repositoryData, int successfulSnapshotCount) {
        final Collection<SnapshotId> snapshotIds = repositoryData.getSnapshotIds();
        assertThat(snapshotIds, hasSize(successfulSnapshotCount));
        for (SnapshotId snapshotId : snapshotIds) {
            assertThat(repositoryData.getSnapshotState(snapshotId), is(SnapshotState.SUCCESS));
        }
    }

    private static BlobStoreIndexShardSnapshots readShardGeneration(
        BlobStoreRepository repository,
        RepositoryShardId repositoryShardId,
        ShardGeneration generation
    ) throws IOException {
        return BlobStoreRepository.INDEX_SHARD_SNAPSHOTS_FORMAT.read(
            repository.getMetadata().name(),
            repository.shardContainer(repositoryShardId.index(), repositoryShardId.shardId()),
            generation.getGenerationUUID(),
            NamedXContentRegistry.EMPTY
        );
    }

    private static BlobStoreIndexShardSnapshot readShardSnapshot(
        BlobStoreRepository repository,
        RepositoryShardId repositoryShardId,
        SnapshotId snapshotId
    ) {
        return repository.loadShardSnapshot(repository.shardContainer(repositoryShardId.index(), repositoryShardId.shardId()), snapshotId);
    }
}
