/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.snapshots;

import org.apache.logging.log4j.core.util.Throwables;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.StepListener;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsRequest;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotStatus;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusResponse;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.SnapshotDeletionsInProgress;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.UncategorizedExecutionException;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.discovery.AbstractDisruptionTestCase;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoryConflictException;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.ShardGenerations;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.disruption.NetworkDisruption;
import org.elasticsearch.test.transport.MockTransportService;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFileExists;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ConcurrentSnapshotsIT extends AbstractSnapshotIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockTransportService.TestPlugin.class, MockRepository.Plugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(AbstractDisruptionTestCase.DEFAULT_SETTINGS)
            .build();
    }

    public void testLongRunningSnapshotAllowsConcurrentSnapshot() throws Exception {
        internalCluster().startMasterOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");
        createIndexWithContent("index-slow");

        final ActionFuture<CreateSnapshotResponse> createSlowFuture = startFullSnapshotBlockedOnDataNode(
            "slow-snapshot",
            repoName,
            dataNode
        );

        final String dataNode2 = internalCluster().startDataOnlyNode();
        ensureStableCluster(3);
        final String indexFast = "index-fast";
        createIndexWithContent(indexFast, dataNode2, dataNode);

        assertSuccessful(
            clusterAdmin().prepareCreateSnapshot(repoName, "fast-snapshot").setIndices(indexFast).setWaitForCompletion(true).execute()
        );

        assertThat(createSlowFuture.isDone(), is(false));
        unblockNode(repoName, dataNode);

        assertSuccessful(createSlowFuture);
    }

    public void testRecreateCorruptedRepositoryDuringSnapshotsFails() throws Exception {
        internalCluster().startMasterOnlyNode();
        final String slowDataNode = internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");

        logger.info("--> issue a long-running slow snapshot");
        createIndexWithContent("index-slow");
        final ActionFuture<CreateSnapshotResponse> slowFuture = startFullSnapshotBlockedOnDataNode("slow-snapshot", repoName, slowDataNode);

        logger.info("--> execute a concurrent fast snapshot");
        final String fastDataNode = internalCluster().startDataOnlyNode();
        ensureStableCluster(3);
        final String indexFast = "index-fast";
        createIndexWithContent(indexFast, fastDataNode, slowDataNode);
        assertSuccessful(
            clusterAdmin().prepareCreateSnapshot(repoName, "fast-snapshot").setIndices(indexFast).setWaitForCompletion(true).execute()
        );

        logger.info("--> corrupting the repository by moving index-N blob to next generation");
        final RepositoryData repositoryData = getRepositoryData(repoName);
        Settings repoSettings = getRepositoryMetadata(repoName).settings();

        Path repo = PathUtils.get(repoSettings.get("location"));
        Files.move(repo.resolve("index-" + repositoryData.getGenId()), repo.resolve("index-" + (repositoryData.getGenId() + 1)));

        logger.info("--> trying to create another snapshot in order for repository to be marked as corrupt");
        final SnapshotException snapshotException = expectThrows(
            SnapshotException.class,
            () -> clusterAdmin().prepareCreateSnapshot(repoName, "fast-snapshot2")
                .setIndices(indexFast)
                .setWaitForCompletion(true)
                .execute()
                .actionGet()
        );
        assertThat(snapshotException.getMessage(), containsString("failed to update snapshot in repository"));
        assertEquals(RepositoryData.CORRUPTED_REPO_GEN, getRepositoryMetadata(repoName).generation());

        logger.info("--> recreating the repository in order to reset corrupted state, which should fail due to ongoing snapshot");
        final RepositoryConflictException repoException = expectThrows(
            RepositoryConflictException.class,
            () -> createRepository(repoName, "mock", Settings.builder().put(repoSettings))
        );
        assertThat(repoException.getMessage(), containsString("trying to modify or unregister repository that is currently used"));

        logger.info("--> unblocking slow snapshot and let it fail due to corrupt repository");
        assertThat(slowFuture.isDone(), is(false));
        unblockNode(repoName, slowDataNode);
        final ExecutionException executionException = expectThrows(ExecutionException.class, () -> slowFuture.get().getSnapshotInfo());
        final Throwable innermostException = Throwables.getRootCause(executionException);
        assertThat(innermostException, instanceOf(RepositoryException.class));
        assertThat(innermostException.getMessage(), containsString("The repository has been disabled to prevent data corruption"));

        logger.info("--> without snapshots in progress, finally recreate repository to reset corrupted state");
        createRepository(repoName, "mock", Settings.builder().put(repoSettings));
        assertNotEquals(RepositoryData.CORRUPTED_REPO_GEN, getRepositoryMetadata(repoName).generation());
    }

    public void testDeletesAreBatched() throws Exception {
        internalCluster().startMasterOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");

        createIndex("foo");
        ensureGreen();

        final int numSnapshots = randomIntBetween(1, 4);
        final Collection<String> snapshotNames = createNSnapshots(repoName, numSnapshots);

        createIndexWithContent("index-slow");

        final ActionFuture<CreateSnapshotResponse> createSlowFuture = startFullSnapshotBlockedOnDataNode(
            "blocked-snapshot",
            repoName,
            dataNode
        );

        final Collection<StepListener<AcknowledgedResponse>> deleteFutures = new ArrayList<>();
        while (snapshotNames.isEmpty() == false) {
            final Collection<String> toDelete = randomSubsetOf(snapshotNames);
            if (toDelete.isEmpty()) {
                continue;
            }
            snapshotNames.removeAll(toDelete);
            final StepListener<AcknowledgedResponse> future = new StepListener<>();
            clusterAdmin().prepareDeleteSnapshot(repoName, toDelete.toArray(Strings.EMPTY_ARRAY)).execute(future);
            deleteFutures.add(future);
        }

        assertThat(createSlowFuture.isDone(), is(false));

        final long repoGenAfterInitialSnapshots = getRepositoryData(repoName).getGenId();
        assertThat(repoGenAfterInitialSnapshots, is(numSnapshots - 1L));
        unblockNode(repoName, dataNode);

        final SnapshotInfo slowSnapshotInfo = assertSuccessful(createSlowFuture);

        logger.info("--> waiting for batched deletes to finish");
        final PlainActionFuture<Collection<AcknowledgedResponse>> allDeletesDone = new PlainActionFuture<>();
        final ActionListener<AcknowledgedResponse> deletesListener = new GroupedActionListener<>(deleteFutures.size(), allDeletesDone);
        for (StepListener<AcknowledgedResponse> deleteFuture : deleteFutures) {
            deleteFuture.addListener(deletesListener);
        }
        allDeletesDone.get();

        logger.info("--> verifying repository state");
        final RepositoryData repositoryDataAfterDeletes = getRepositoryData(repoName);
        // One increment for snapshot, one for all the deletes
        assertThat(repositoryDataAfterDeletes.getGenId(), is(repoGenAfterInitialSnapshots + 2));
        assertThat(repositoryDataAfterDeletes.getSnapshotIds(), contains(slowSnapshotInfo.snapshotId()));
    }

    public void testBlockedRepoDoesNotBlockOtherRepos() throws Exception {
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();
        final String blockedRepoName = "test-repo-blocked";
        final String otherRepoName = "test-repo";
        createRepository(blockedRepoName, "mock");
        createRepository(otherRepoName, "fs");
        createIndex("foo");
        ensureGreen();
        createIndexWithContent("index-slow");

        final ActionFuture<CreateSnapshotResponse> createSlowFuture = startAndBlockFailingFullSnapshot(blockedRepoName, "blocked-snapshot");

        clusterAdmin().prepareCreateSnapshot(otherRepoName, "snapshot").setIndices("does-not-exist-*").setWaitForCompletion(false).get();

        unblockNode(blockedRepoName, internalCluster().getMasterName());
        expectThrows(SnapshotException.class, createSlowFuture::actionGet);

        assertBusy(() -> assertThat(currentSnapshots(otherRepoName), empty()), 30L, TimeUnit.SECONDS);
    }

    public void testMultipleReposAreIndependent() throws Exception {
        internalCluster().startMasterOnlyNode();
        // We're blocking a some of the snapshot threads when we block the first repo below so we have to make sure we have enough threads
        // left for the second concurrent snapshot.
        final String dataNode = startDataNodeWithLargeSnapshotPool();
        final String blockedRepoName = "test-repo-blocked";
        final String otherRepoName = "test-repo";
        createRepository(blockedRepoName, "mock");
        createRepository(otherRepoName, "fs");
        createIndexWithContent("test-index");

        final ActionFuture<CreateSnapshotResponse> createSlowFuture = startFullSnapshotBlockedOnDataNode(
            "blocked-snapshot",
            blockedRepoName,
            dataNode
        );

        logger.info("--> waiting for concurrent snapshot(s) to finish");
        createNSnapshots(otherRepoName, randomIntBetween(1, 5));

        unblockNode(blockedRepoName, dataNode);
        assertSuccessful(createSlowFuture);
    }

    public void testMultipleReposAreIndependent2() throws Exception {
        internalCluster().startMasterOnlyNode();
        // We're blocking a some of the snapshot threads when we block the first repo below so we have to make sure we have enough threads
        // left for the second repository's concurrent operations.
        final String dataNode = startDataNodeWithLargeSnapshotPool();
        final String blockedRepoName = "test-repo-blocked";
        final String otherRepoName = "test-repo";
        createRepository(blockedRepoName, "mock");
        createRepository(otherRepoName, "fs");
        createIndexWithContent("test-index");

        final ActionFuture<CreateSnapshotResponse> createSlowFuture = startFullSnapshotBlockedOnDataNode(
            "blocked-snapshot",
            blockedRepoName,
            dataNode
        );

        logger.info("--> waiting for concurrent snapshot(s) to finish");
        createNSnapshots(otherRepoName, randomIntBetween(1, 5));
        assertAcked(startDeleteSnapshot(otherRepoName, "*").get());

        unblockNode(blockedRepoName, dataNode);
        assertSuccessful(createSlowFuture);
    }

    public void testMultipleReposAreIndependent3() throws Exception {
        final String masterNode = internalCluster().startMasterOnlyNode(LARGE_SNAPSHOT_POOL_SETTINGS);
        internalCluster().startDataOnlyNode();
        final String blockedRepoName = "test-repo-blocked";
        final String otherRepoName = "test-repo";
        createRepository(blockedRepoName, "mock");
        createRepository(otherRepoName, "fs");
        createIndexWithContent("test-index");

        createFullSnapshot(blockedRepoName, "blocked-snapshot");
        blockNodeOnAnyFiles(blockedRepoName, masterNode);
        final ActionFuture<AcknowledgedResponse> slowDeleteFuture = startDeleteSnapshot(blockedRepoName, "*");

        logger.info("--> waiting for concurrent snapshot(s) to finish");
        createNSnapshots(otherRepoName, randomIntBetween(1, 5));
        assertAcked(startDeleteSnapshot(otherRepoName, "*").get());

        unblockNode(blockedRepoName, masterNode);
        assertAcked(slowDeleteFuture.actionGet());
    }

    public void testSnapshotRunsAfterInProgressDelete() throws Exception {
        final String masterNode = internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");

        ensureGreen();
        createIndexWithContent("index-test");

        final String firstSnapshot = "first-snapshot";
        createFullSnapshot(repoName, firstSnapshot);

        blockMasterFromFinalizingSnapshotOnIndexFile(repoName);
        final ActionFuture<AcknowledgedResponse> deleteFuture = startDeleteSnapshot(repoName, firstSnapshot);
        waitForBlock(masterNode, repoName);

        final ActionFuture<CreateSnapshotResponse> snapshotFuture = startFullSnapshot(repoName, "second-snapshot");

        unblockNode(repoName, masterNode);
        final UncategorizedExecutionException ex = expectThrows(UncategorizedExecutionException.class, deleteFuture::actionGet);
        assertThat(ex.getRootCause(), instanceOf(IOException.class));

        assertSuccessful(snapshotFuture);
    }

    public void testAbortOneOfMultipleSnapshots() throws Exception {
        internalCluster().startMasterOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");
        final String firstIndex = "index-one";
        createIndexWithContent(firstIndex);

        final String firstSnapshot = "snapshot-one";
        final ActionFuture<CreateSnapshotResponse> firstSnapshotResponse = startFullSnapshotBlockedOnDataNode(
            firstSnapshot,
            repoName,
            dataNode
        );

        final String dataNode2 = internalCluster().startDataOnlyNode();
        ensureStableCluster(3);
        final String secondIndex = "index-two";
        createIndexWithContent(secondIndex, dataNode2, dataNode);

        final String secondSnapshot = "snapshot-two";
        final ActionFuture<CreateSnapshotResponse> secondSnapshotResponse = startFullSnapshot(repoName, secondSnapshot);

        logger.info("--> wait for snapshot on second data node to finish");
        awaitClusterState(state -> {
            final SnapshotsInProgress snapshotsInProgress = state.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY);
            return snapshotsInProgress.count() == 2 && snapshotHasCompletedShard(repoName, secondSnapshot, snapshotsInProgress);
        });

        final ActionFuture<AcknowledgedResponse> deleteSnapshotsResponse = startDeleteSnapshot(repoName, firstSnapshot);
        awaitNDeletionsInProgress(1);

        logger.info("--> start third snapshot");
        final ActionFuture<CreateSnapshotResponse> thirdSnapshotResponse = clusterAdmin().prepareCreateSnapshot(repoName, "snapshot-three")
            .setIndices(secondIndex)
            .setWaitForCompletion(true)
            .execute();

        assertThat(firstSnapshotResponse.isDone(), is(false));
        assertThat(secondSnapshotResponse.isDone(), is(false));

        unblockNode(repoName, dataNode);
        final SnapshotInfo firstSnapshotInfo = firstSnapshotResponse.get().getSnapshotInfo();
        assertThat(firstSnapshotInfo.state(), is(SnapshotState.FAILED));
        assertThat(firstSnapshotInfo.reason(), is("Snapshot was aborted by deletion"));

        final SnapshotInfo secondSnapshotInfo = assertSuccessful(secondSnapshotResponse);
        final SnapshotInfo thirdSnapshotInfo = assertSuccessful(thirdSnapshotResponse);

        assertThat(deleteSnapshotsResponse.get().isAcknowledged(), is(true));

        logger.info("--> verify that the first snapshot is gone");
        assertThat(
            clusterAdmin().prepareGetSnapshots(repoName).get().getSnapshots(),
            containsInAnyOrder(secondSnapshotInfo, thirdSnapshotInfo)
        );
    }

    public void testCascadedAborts() throws Exception {
        internalCluster().startMasterOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");
        createIndexWithContent("index-one");

        final String firstSnapshot = "snapshot-one";
        final ActionFuture<CreateSnapshotResponse> firstSnapshotResponse = startFullSnapshotBlockedOnDataNode(
            firstSnapshot,
            repoName,
            dataNode
        );

        final String dataNode2 = internalCluster().startDataOnlyNode();
        ensureStableCluster(3);
        createIndexWithContent("index-two", dataNode2, dataNode);

        final String secondSnapshot = "snapshot-two";
        final ActionFuture<CreateSnapshotResponse> secondSnapshotResponse = startFullSnapshot(repoName, secondSnapshot);

        logger.info("--> wait for snapshot on second data node to finish");
        awaitClusterState(state -> {
            final SnapshotsInProgress snapshotsInProgress = state.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY);
            return snapshotsInProgress.count() == 2 && snapshotHasCompletedShard(repoName, secondSnapshot, snapshotsInProgress);
        });

        final ActionFuture<AcknowledgedResponse> deleteSnapshotsResponse = startDeleteSnapshot(repoName, firstSnapshot);
        awaitNDeletionsInProgress(1);

        final ActionFuture<CreateSnapshotResponse> thirdSnapshotResponse = startFullSnapshot(repoName, "snapshot-three");

        assertThat(firstSnapshotResponse.isDone(), is(false));
        assertThat(secondSnapshotResponse.isDone(), is(false));

        logger.info("--> waiting for all three snapshots to show up as in-progress");
        assertBusy(() -> assertThat(currentSnapshots(repoName), hasSize(3)), 30L, TimeUnit.SECONDS);

        final ActionFuture<AcknowledgedResponse> allDeletedResponse = startDeleteSnapshot(repoName, "*");

        logger.info("--> waiting for second and third snapshot to finish");
        assertBusy(() -> {
            assertThat(currentSnapshots(repoName), hasSize(1));
            final SnapshotsInProgress snapshotsInProgress = clusterService().state().custom(SnapshotsInProgress.TYPE);
            assertThat(snapshotsInProgress.forRepo(repoName).get(0).state(), is(SnapshotsInProgress.State.ABORTED));
        }, 30L, TimeUnit.SECONDS);

        unblockNode(repoName, dataNode);

        logger.info("--> verify all snapshots were aborted");
        assertThat(firstSnapshotResponse.get().getSnapshotInfo().state(), is(SnapshotState.FAILED));
        assertThat(secondSnapshotResponse.get().getSnapshotInfo().state(), is(SnapshotState.FAILED));
        assertThat(thirdSnapshotResponse.get().getSnapshotInfo().state(), is(SnapshotState.FAILED));

        logger.info("--> verify both deletes have completed");
        assertAcked(deleteSnapshotsResponse.get());
        assertAcked(allDeletedResponse.get());

        logger.info("--> verify that all snapshots are gone");
        assertThat(clusterAdmin().prepareGetSnapshots(repoName).get().getSnapshots(), empty());
    }

    public void testMasterFailOverWithQueuedDeletes() throws Exception {
        internalCluster().startMasterOnlyNodes(3);
        final String dataNode = internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");

        final String firstIndex = "index-one";
        createIndexWithContent(firstIndex);

        final String firstSnapshot = "snapshot-one";
        blockDataNode(repoName, dataNode);
        final ActionFuture<CreateSnapshotResponse> firstSnapshotResponse = startFullSnapshotFromNonMasterClient(repoName, firstSnapshot);
        waitForBlock(dataNode, repoName);

        final String dataNode2 = internalCluster().startDataOnlyNode();
        ensureStableCluster(5);
        final String secondIndex = "index-two";
        createIndexWithContent(secondIndex, dataNode2, dataNode);

        final String secondSnapshot = "snapshot-two";
        final ActionFuture<CreateSnapshotResponse> secondSnapshotResponse = startFullSnapshot(repoName, secondSnapshot);

        logger.info("--> wait for snapshot on second data node to finish");
        awaitClusterState(state -> {
            final SnapshotsInProgress snapshotsInProgress = state.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY);
            return snapshotsInProgress.count() == 2 && snapshotHasCompletedShard(repoName, secondSnapshot, snapshotsInProgress);
        });

        final ActionFuture<AcknowledgedResponse> firstDeleteFuture = startDeleteFromNonMasterClient(repoName, firstSnapshot);
        awaitNDeletionsInProgress(1);

        blockNodeOnAnyFiles(repoName, dataNode2);
        final ActionFuture<CreateSnapshotResponse> snapshotThreeFuture = startFullSnapshotFromNonMasterClient(repoName, "snapshot-three");
        waitForBlock(dataNode2, repoName);

        assertThat(firstSnapshotResponse.isDone(), is(false));
        assertThat(secondSnapshotResponse.isDone(), is(false));

        logger.info("--> waiting for all three snapshots to show up as in-progress");
        assertBusy(() -> assertThat(currentSnapshots(repoName), hasSize(3)), 30L, TimeUnit.SECONDS);

        final ActionFuture<AcknowledgedResponse> deleteAllSnapshots = startDeleteFromNonMasterClient(repoName, "*");
        logger.info("--> wait for delete to be enqueued in cluster state");
        awaitClusterState(state -> {
            final SnapshotDeletionsInProgress deletionsInProgress = state.custom(SnapshotDeletionsInProgress.TYPE);
            return deletionsInProgress.getEntries().size() == 1 && deletionsInProgress.getEntries().get(0).getSnapshots().size() == 3;
        });

        logger.info("--> waiting for second snapshot to finish and the other two snapshots to become aborted");
        assertBusy(() -> {
            assertThat(currentSnapshots(repoName), hasSize(2));
            for (SnapshotsInProgress.Entry entry : clusterService().state()
                .custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY)
                .forRepo(repoName)) {
                assertThat(entry.state(), is(SnapshotsInProgress.State.ABORTED));
                assertThat(entry.snapshot().getSnapshotId().getName(), not(secondSnapshot));
            }
        }, 30L, TimeUnit.SECONDS);

        logger.info("--> stopping current master node");
        internalCluster().stopCurrentMasterNode();

        unblockNode(repoName, dataNode);
        unblockNode(repoName, dataNode2);

        for (ActionFuture<AcknowledgedResponse> deleteFuture : Arrays.asList(firstDeleteFuture, deleteAllSnapshots)) {
            try {
                assertAcked(deleteFuture.actionGet());
            } catch (RepositoryException rex) {
                // rarely the master node fails over twice when shutting down the initial master and fails the transport listener
                assertThat(rex.repository(), is("_all"));
                assertThat(rex.getMessage(), endsWith("Failed to update cluster state during repository operation"));
            } catch (SnapshotMissingException sme) {
                // very rarely a master node fail-over happens at such a time that the client on the data-node sees a disconnect exception
                // after the master has already started the delete, leading to the delete retry to run into a situation where the
                // snapshot has already been deleted potentially
                assertThat(sme.getSnapshotName(), is(firstSnapshot));
            }
        }
        expectThrows(SnapshotException.class, snapshotThreeFuture::actionGet);

        logger.info("--> verify that all snapshots are gone and no more work is left in the cluster state");
        awaitNoMoreRunningOperations();
        assertThat(clusterAdmin().prepareGetSnapshots(repoName).get().getSnapshots(), empty());
    }

    public void testAssertMultipleSnapshotsAndPrimaryFailOver() throws Exception {
        internalCluster().startMasterOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");

        final String testIndex = "index-one";
        createIndex(testIndex, 1, 1);
        ensureYellow(testIndex);
        indexDoc(testIndex, "some_id", "foo", "bar");

        blockDataNode(repoName, dataNode);
        final ActionFuture<CreateSnapshotResponse> firstSnapshotResponse = startFullSnapshotFromMasterClient(repoName, "snapshot-one");
        waitForBlock(dataNode, repoName);

        internalCluster().startDataOnlyNode();
        ensureStableCluster(3);
        ensureGreen(testIndex);

        final String secondSnapshot = "snapshot-two";
        final ActionFuture<CreateSnapshotResponse> secondSnapshotResponse = startFullSnapshotFromMasterClient(repoName, secondSnapshot);
        awaitNumberOfSnapshotsInProgress(2);

        internalCluster().restartNode(dataNode);

        assertThat(firstSnapshotResponse.get().getSnapshotInfo().state(), is(SnapshotState.PARTIAL));
        assertThat(secondSnapshotResponse.get().getSnapshotInfo().state(), is(SnapshotState.PARTIAL));
    }

    public void testQueuedDeletesWithFailures() throws Exception {
        final String masterNode = internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");
        createIndexWithContent("index-one");
        createNSnapshots(repoName, randomIntBetween(2, 5));

        blockMasterFromFinalizingSnapshotOnIndexFile(repoName);
        final ActionFuture<AcknowledgedResponse> firstDeleteFuture = startDeleteSnapshot(repoName, "*");
        waitForBlock(masterNode, repoName);

        final ActionFuture<CreateSnapshotResponse> snapshotFuture = startFullSnapshot(repoName, "snapshot-queued");
        awaitNumberOfSnapshotsInProgress(1);

        final ActionFuture<AcknowledgedResponse> secondDeleteFuture = startDeleteSnapshot(repoName, "*");
        awaitNDeletionsInProgress(2);

        unblockNode(repoName, masterNode);
        expectThrows(UncategorizedExecutionException.class, firstDeleteFuture::actionGet);

        // Second delete works out cleanly since the repo is unblocked now
        assertThat(secondDeleteFuture.get().isAcknowledged(), is(true));
        // Snapshot should have been aborted
        final SnapshotException snapshotException = expectThrows(SnapshotException.class, snapshotFuture::actionGet);
        assertThat(snapshotException.getMessage(), containsString(SnapshotsInProgress.ABORTED_FAILURE_TEXT));

        assertThat(clusterAdmin().prepareGetSnapshots(repoName).get().getSnapshots(), empty());
    }

    public void testQueuedDeletesWithOverlap() throws Exception {
        final String masterNode = internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");
        createIndexWithContent("index-one");
        createNSnapshots(repoName, randomIntBetween(2, 5));

        final ActionFuture<AcknowledgedResponse> firstDeleteFuture = startAndBlockOnDeleteSnapshot(repoName, "*");
        final ActionFuture<CreateSnapshotResponse> snapshotFuture = startFullSnapshot(repoName, "snapshot-queued");
        awaitNumberOfSnapshotsInProgress(1);

        final ActionFuture<AcknowledgedResponse> secondDeleteFuture = startDeleteSnapshot(repoName, "*");
        awaitNDeletionsInProgress(2);

        unblockNode(repoName, masterNode);
        assertThat(firstDeleteFuture.get().isAcknowledged(), is(true));

        // Second delete works out cleanly since the repo is unblocked now
        assertThat(secondDeleteFuture.get().isAcknowledged(), is(true));
        // Snapshot should have been aborted
        final SnapshotException snapshotException = expectThrows(SnapshotException.class, snapshotFuture::actionGet);
        assertThat(snapshotException.getMessage(), containsString(SnapshotsInProgress.ABORTED_FAILURE_TEXT));

        assertThat(clusterAdmin().prepareGetSnapshots(repoName).get().getSnapshots(), empty());
    }

    public void testQueuedOperationsOnMasterRestart() throws Exception {
        internalCluster().startMasterOnlyNodes(3);
        internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");
        createIndexWithContent("index-one");
        createNSnapshots(repoName, randomIntBetween(2, 5));

        startAndBlockOnDeleteSnapshot(repoName, "*");

        clusterAdmin().prepareCreateSnapshot(repoName, "snapshot-three").setWaitForCompletion(false).get();

        startDeleteSnapshot(repoName, "*");
        awaitNDeletionsInProgress(2);

        internalCluster().stopCurrentMasterNode();
        ensureStableCluster(3);

        awaitNoMoreRunningOperations();
    }

    public void testQueuedOperationsOnMasterDisconnect() throws Exception {
        internalCluster().startMasterOnlyNodes(3);
        final String dataNode = internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");
        createIndexWithContent("index-one");
        createNSnapshots(repoName, randomIntBetween(2, 5));

        final String masterNode = internalCluster().getMasterName();
        final NetworkDisruption networkDisruption = isolateMasterDisruption(NetworkDisruption.DISCONNECT);
        internalCluster().setDisruptionScheme(networkDisruption);

        blockNodeOnAnyFiles(repoName, masterNode);
        ActionFuture<AcknowledgedResponse> firstDeleteFuture = client(masterNode).admin()
            .cluster()
            .prepareDeleteSnapshot(repoName, "*")
            .execute();
        waitForBlock(masterNode, repoName);

        final ActionFuture<CreateSnapshotResponse> createThirdSnapshot = client(masterNode).admin()
            .cluster()
            .prepareCreateSnapshot(repoName, "snapshot-three")
            .setWaitForCompletion(true)
            .execute();
        awaitNumberOfSnapshotsInProgress(1);

        final ActionFuture<AcknowledgedResponse> secondDeleteFuture = client(masterNode).admin()
            .cluster()
            .prepareDeleteSnapshot(repoName, "*")
            .execute();
        awaitNDeletionsInProgress(2);

        networkDisruption.startDisrupting();
        ensureStableCluster(3, dataNode);
        unblockNode(repoName, masterNode);
        networkDisruption.stopDisrupting();

        logger.info("--> make sure all failing requests get a response");
        assertAcked(firstDeleteFuture.get());
        assertAcked(secondDeleteFuture.get());
        expectThrows(SnapshotException.class, createThirdSnapshot::actionGet);

        awaitNoMoreRunningOperations();
    }

    public void testQueuedOperationsOnMasterDisconnectAndRepoFailure() throws Exception {
        internalCluster().startMasterOnlyNodes(3);
        final String dataNode = internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");
        createIndexWithContent("index-one");
        createNSnapshots(repoName, randomIntBetween(2, 5));

        final String masterNode = internalCluster().getMasterName();
        final NetworkDisruption networkDisruption = isolateMasterDisruption(NetworkDisruption.DISCONNECT);
        internalCluster().setDisruptionScheme(networkDisruption);

        blockMasterFromFinalizingSnapshotOnIndexFile(repoName);
        final ActionFuture<CreateSnapshotResponse> firstFailedSnapshotFuture = startFullSnapshotFromMasterClient(
            repoName,
            "failing-snapshot-1"
        );
        waitForBlock(masterNode, repoName);
        final ActionFuture<CreateSnapshotResponse> secondFailedSnapshotFuture = startFullSnapshotFromMasterClient(
            repoName,
            "failing-snapshot-2"
        );
        awaitNumberOfSnapshotsInProgress(2);

        final ActionFuture<AcknowledgedResponse> deleteFuture = client(masterNode).admin()
            .cluster()
            .prepareDeleteSnapshot(repoName, "*")
            .execute();
        awaitNDeletionsInProgress(1);

        networkDisruption.startDisrupting();
        ensureStableCluster(3, dataNode);
        unblockNode(repoName, masterNode);
        networkDisruption.stopDisrupting();

        logger.info("--> make sure all failing requests get a response");
        expectThrows(SnapshotException.class, firstFailedSnapshotFuture::actionGet);
        expectThrows(SnapshotException.class, secondFailedSnapshotFuture::actionGet);
        assertAcked(deleteFuture.get());

        awaitNoMoreRunningOperations();
    }

    public void testQueuedOperationsAndBrokenRepoOnMasterFailOver() throws Exception {
        disableRepoConsistencyCheck("This test corrupts the repository on purpose");

        internalCluster().startMasterOnlyNodes(3);
        internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        final Path repoPath = randomRepoPath();
        createRepository(repoName, "mock", repoPath);
        createIndexWithContent("index-one");
        createNSnapshots(repoName, randomIntBetween(2, 5));

        final long generation = getRepositoryData(repoName).getGenId();

        startAndBlockOnDeleteSnapshot(repoName, "*");

        corruptIndexN(repoPath, generation);

        clusterAdmin().prepareCreateSnapshot(repoName, "snapshot-three").setWaitForCompletion(false).get();

        final ActionFuture<AcknowledgedResponse> deleteFuture = startDeleteFromNonMasterClient(repoName, "*");
        awaitNDeletionsInProgress(2);

        internalCluster().stopCurrentMasterNode();
        ensureStableCluster(3);

        awaitNoMoreRunningOperations();
        expectThrows(RepositoryException.class, deleteFuture::actionGet);
    }

    public void testQueuedSnapshotOperationsAndBrokenRepoOnMasterFailOver() throws Exception {
        disableRepoConsistencyCheck("This test corrupts the repository on purpose");

        internalCluster().startMasterOnlyNodes(3);
        internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        final Path repoPath = randomRepoPath();
        createRepository(repoName, "mock", repoPath);
        createIndexWithContent("index-one");
        createNSnapshots(repoName, randomIntBetween(2, 5));

        final long generation = getRepositoryData(repoName).getGenId();
        final String masterNode = internalCluster().getMasterName();
        blockNodeOnAnyFiles(repoName, masterNode);
        final ActionFuture<CreateSnapshotResponse> snapshotThree = startFullSnapshotFromNonMasterClient(repoName, "snapshot-three");
        waitForBlock(masterNode, repoName);

        corruptIndexN(repoPath, generation);

        final ActionFuture<CreateSnapshotResponse> snapshotFour = startFullSnapshotFromNonMasterClient(repoName, "snapshot-four");
        internalCluster().stopCurrentMasterNode();
        ensureStableCluster(3);

        awaitNoMoreRunningOperations();
        expectThrows(ElasticsearchException.class, snapshotThree::actionGet);
        expectThrows(ElasticsearchException.class, snapshotFour::actionGet);
    }

    public void testQueuedSnapshotOperationsAndBrokenRepoOnMasterFailOver2() throws Exception {
        disableRepoConsistencyCheck("This test corrupts the repository on purpose");

        internalCluster().startMasterOnlyNodes(3);
        final String dataNode = internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        final Path repoPath = randomRepoPath();
        createRepository(repoName, "mock", repoPath);
        createIndexWithContent("index-one");
        createNSnapshots(repoName, randomIntBetween(2, 5));

        final long generation = getRepositoryData(repoName).getGenId();
        final String masterNode = internalCluster().getMasterName();
        blockMasterFromFinalizingSnapshotOnIndexFile(repoName);
        final ActionFuture<CreateSnapshotResponse> snapshotThree = startFullSnapshotFromNonMasterClient(repoName, "snapshot-three");
        waitForBlock(masterNode, repoName);

        corruptIndexN(repoPath, generation);

        final ActionFuture<CreateSnapshotResponse> snapshotFour = startFullSnapshotFromNonMasterClient(repoName, "snapshot-four");
        awaitNumberOfSnapshotsInProgress(2);

        final NetworkDisruption networkDisruption = isolateMasterDisruption(NetworkDisruption.DISCONNECT);
        internalCluster().setDisruptionScheme(networkDisruption);
        networkDisruption.startDisrupting();
        ensureStableCluster(3, dataNode);
        unblockNode(repoName, masterNode);
        networkDisruption.stopDisrupting();
        awaitNoMoreRunningOperations();
        expectThrows(ElasticsearchException.class, snapshotThree::actionGet);
        expectThrows(ElasticsearchException.class, snapshotFour::actionGet);
    }

    public void testQueuedSnapshotOperationsAndBrokenRepoOnMasterFailOverMultipleRepos() throws Exception {
        disableRepoConsistencyCheck("This test corrupts the repository on purpose");

        internalCluster().startMasterOnlyNodes(3, LARGE_SNAPSHOT_POOL_SETTINGS);
        internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        final Path repoPath = randomRepoPath();
        createRepository(repoName, "mock", repoPath);
        createIndexWithContent("index-one");
        createNSnapshots(repoName, randomIntBetween(2, 5));

        final String masterNode = internalCluster().getMasterName();

        final String blockedRepoName = "repo-blocked";
        createRepository(blockedRepoName, "mock");
        createNSnapshots(blockedRepoName, randomIntBetween(1, 5));
        blockNodeOnAnyFiles(blockedRepoName, masterNode);
        final ActionFuture<AcknowledgedResponse> deleteFuture = startDeleteFromNonMasterClient(blockedRepoName, "*");
        waitForBlock(masterNode, blockedRepoName);
        awaitNDeletionsInProgress(1);
        final ActionFuture<CreateSnapshotResponse> createBlockedSnapshot = startFullSnapshotFromNonMasterClient(
            blockedRepoName,
            "queued-snapshot"
        );
        awaitNumberOfSnapshotsInProgress(1);

        final long generation = getRepositoryData(repoName).getGenId();
        blockNodeOnAnyFiles(repoName, masterNode);
        final ActionFuture<CreateSnapshotResponse> snapshotThree = startFullSnapshotFromNonMasterClient(repoName, "snapshot-three");
        waitForBlock(masterNode, repoName);
        awaitNumberOfSnapshotsInProgress(2);

        corruptIndexN(repoPath, generation);

        final ActionFuture<CreateSnapshotResponse> snapshotFour = startFullSnapshotFromNonMasterClient(repoName, "snapshot-four");
        awaitNumberOfSnapshotsInProgress(3);
        internalCluster().stopCurrentMasterNode();
        ensureStableCluster(3);

        awaitNoMoreRunningOperations();
        expectThrows(ElasticsearchException.class, snapshotThree::actionGet);
        expectThrows(ElasticsearchException.class, snapshotFour::actionGet);
        assertAcked(deleteFuture.get());
        try {
            createBlockedSnapshot.actionGet();
        } catch (ElasticsearchException ex) {
            // Ignored, thrown most of the time but due to retries when shutting down the master could randomly pass when the request is
            // retried and gets executed after the above delete
        }
    }

    public void testMultipleSnapshotsQueuedAfterDelete() throws Exception {
        final String masterNode = internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");
        createIndexWithContent("index-one");
        createNSnapshots(repoName, randomIntBetween(1, 5));

        final ActionFuture<AcknowledgedResponse> deleteFuture = startAndBlockOnDeleteSnapshot(repoName, "*");
        final ActionFuture<CreateSnapshotResponse> snapshotThree = startFullSnapshot(repoName, "snapshot-three");
        final ActionFuture<CreateSnapshotResponse> snapshotFour = startFullSnapshot(repoName, "snapshot-four");

        unblockNode(repoName, masterNode);

        assertSuccessful(snapshotThree);
        assertSuccessful(snapshotFour);
        assertAcked(deleteFuture.get());
    }

    public void testMultiplePartialSnapshotsQueuedAfterDelete() throws Exception {
        final String masterNode = internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");
        createIndexWithContent("index-one");
        createIndexWithContent("index-two");
        createNSnapshots(repoName, randomIntBetween(1, 5));

        final ActionFuture<AcknowledgedResponse> deleteFuture = startAndBlockOnDeleteSnapshot(repoName, "*");
        final ActionFuture<CreateSnapshotResponse> snapshotThree = startFullSnapshot(repoName, "snapshot-three", true);
        final ActionFuture<CreateSnapshotResponse> snapshotFour = startFullSnapshot(repoName, "snapshot-four", true);
        awaitNumberOfSnapshotsInProgress(2);

        assertAcked(client().admin().indices().prepareDelete("index-two"));
        unblockNode(repoName, masterNode);

        assertThat(snapshotThree.get().getSnapshotInfo().state(), is(SnapshotState.SUCCESS));
        assertThat(snapshotFour.get().getSnapshotInfo().state(), is(SnapshotState.SUCCESS));
        assertAcked(deleteFuture.get());
    }

    public void testQueuedSnapshotsWaitingForShardReady() throws Exception {
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNodes(2);
        final String repoName = "test-repo";
        createRepository(repoName, "fs");

        final String testIndex = "test-idx";
        // Create index on two nodes and make sure each node has a primary by setting no replicas
        assertAcked(prepareCreate(testIndex, 2, indexSettingsNoReplicas(between(2, 10))));

        ensureGreen(testIndex);

        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            indexDoc(testIndex, Integer.toString(i), "foo", "bar" + i);
        }
        refresh();
        assertThat(client().prepareSearch(testIndex).setSize(0).get().getHits().getTotalHits().value, equalTo(100L));

        logger.info("--> start relocations");
        allowNodes(testIndex, 1);

        logger.info("--> wait for relocations to start");
        assertBusy(
            () -> assertThat(clusterAdmin().prepareHealth(testIndex).execute().actionGet().getRelocatingShards(), greaterThan(0)),
            1L,
            TimeUnit.MINUTES
        );

        logger.info("--> start two snapshots");
        final String snapshotOne = "snap-1";
        final String snapshotTwo = "snap-2";
        final ActionFuture<CreateSnapshotResponse> snapOneResponse = clusterAdmin().prepareCreateSnapshot(repoName, snapshotOne)
            .setWaitForCompletion(false)
            .setIndices(testIndex)
            .execute();
        final ActionFuture<CreateSnapshotResponse> snapTwoResponse = clusterAdmin().prepareCreateSnapshot(repoName, snapshotTwo)
            .setWaitForCompletion(false)
            .setIndices(testIndex)
            .execute();

        snapOneResponse.get();
        snapTwoResponse.get();
        awaitNoMoreRunningOperations();
        for (String snapshot : Arrays.asList(snapshotOne, snapshotTwo)) {
            SnapshotInfo snapshotInfo = getSnapshot(repoName, snapshot);
            assertThat(snapshotInfo.state(), equalTo(SnapshotState.SUCCESS));
            assertThat(snapshotInfo.shardFailures().size(), equalTo(0));
        }
    }

    public void testBackToBackQueuedDeletes() throws Exception {
        final String masterName = internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");
        createIndexWithContent("index-test");
        final List<String> snapshots = createNSnapshots(repoName, 2);
        final String snapshotOne = snapshots.get(0);
        final String snapshotTwo = snapshots.get(1);

        final ActionFuture<AcknowledgedResponse> deleteSnapshotOne = startAndBlockOnDeleteSnapshot(repoName, snapshotOne);
        final ActionFuture<AcknowledgedResponse> deleteSnapshotTwo = startDeleteSnapshot(repoName, snapshotTwo);
        awaitNDeletionsInProgress(2);

        unblockNode(repoName, masterName);
        assertAcked(deleteSnapshotOne.get());
        assertAcked(deleteSnapshotTwo.get());

        final RepositoryData repositoryData = getRepositoryData(repoName);
        assertThat(repositoryData.getSnapshotIds(), empty());
        // Two snapshots and two distinct delete operations move us 4 steps from -1 to 3
        assertThat(repositoryData.getGenId(), is(3L));
    }

    public void testQueuedOperationsAfterFinalizationFailure() throws Exception {
        internalCluster().startMasterOnlyNodes(3);
        internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");
        createIndexWithContent("index-test");

        final List<String> snapshotNames = createNSnapshots(repoName, randomIntBetween(2, 5));

        final ActionFuture<CreateSnapshotResponse> snapshotThree = startAndBlockFailingFullSnapshot(repoName, "snap-other");

        final String masterName = internalCluster().getMasterName();

        final String snapshotOne = snapshotNames.get(0);
        final ActionFuture<AcknowledgedResponse> deleteSnapshotOne = startDeleteSnapshot(repoName, snapshotOne);
        awaitNDeletionsInProgress(1);

        unblockNode(repoName, masterName);

        expectThrows(SnapshotException.class, snapshotThree::actionGet);
        assertAcked(deleteSnapshotOne.get());
    }

    public void testStartDeleteDuringFinalizationCleanup() throws Exception {
        final String masterName = internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");
        createIndexWithContent("index-test");
        createNSnapshots(repoName, randomIntBetween(1, 5));
        final String snapshotName = "snap-name";
        blockMasterFromDeletingIndexNFile(repoName);
        final ActionFuture<CreateSnapshotResponse> snapshotFuture = startFullSnapshot(repoName, snapshotName);
        waitForBlock(masterName, repoName);
        final ActionFuture<AcknowledgedResponse> deleteFuture = startDeleteSnapshot(repoName, snapshotName);
        awaitNDeletionsInProgress(1);
        unblockNode(repoName, masterName);
        assertSuccessful(snapshotFuture);
        assertAcked(deleteFuture.get(30L, TimeUnit.SECONDS));
    }

    public void testEquivalentDeletesAreDeduplicated() throws Exception {
        final String masterName = internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");
        createIndexWithContent("index-test");
        createNSnapshots(repoName, randomIntBetween(1, 5));

        blockNodeOnAnyFiles(repoName, masterName);
        final int deletes = randomIntBetween(2, 10);
        final List<ActionFuture<AcknowledgedResponse>> deleteResponses = new ArrayList<>(deletes);
        for (int i = 0; i < deletes; ++i) {
            deleteResponses.add(clusterAdmin().prepareDeleteSnapshot(repoName, "*").execute());
        }
        waitForBlock(masterName, repoName);
        awaitNDeletionsInProgress(1);
        for (ActionFuture<AcknowledgedResponse> deleteResponse : deleteResponses) {
            assertFalse(deleteResponse.isDone());
        }
        awaitNDeletionsInProgress(1);
        unblockNode(repoName, masterName);
        for (ActionFuture<AcknowledgedResponse> deleteResponse : deleteResponses) {
            assertAcked(deleteResponse.get());
        }
    }

    public void testMasterFailoverOnFinalizationLoop() throws Exception {
        internalCluster().startMasterOnlyNodes(3);
        final String dataNode = internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");
        createIndexWithContent("index-test");
        final NetworkDisruption networkDisruption = isolateMasterDisruption(NetworkDisruption.DISCONNECT);
        internalCluster().setDisruptionScheme(networkDisruption);

        final List<String> snapshotNames = createNSnapshots(repoName, randomIntBetween(2, 5));
        final String masterName = internalCluster().getMasterName();
        blockMasterFromDeletingIndexNFile(repoName);
        final ActionFuture<CreateSnapshotResponse> snapshotOther = startFullSnapshotFromMasterClient(repoName, "snap-other");
        waitForBlock(masterName, repoName);

        final String snapshotOne = snapshotNames.get(0);
        final ActionFuture<AcknowledgedResponse> deleteSnapshotOne = startDeleteSnapshot(repoName, snapshotOne);
        awaitNDeletionsInProgress(1);
        networkDisruption.startDisrupting();
        ensureStableCluster(3, dataNode);

        unblockNode(repoName, masterName);
        networkDisruption.stopDisrupting();
        ensureStableCluster(4);

        assertSuccessful(snapshotOther);
        try {
            deleteSnapshotOne.actionGet();
        } catch (RepositoryException re) {
            // ignored
        } catch (SnapshotMissingException re) {
            // When master node is isolated during this test, the newly elected master takes over and executes the snapshot deletion. In
            // this case the retried delete snapshot operation on the new master can fail with SnapshotMissingException
        }
        awaitNoMoreRunningOperations();
    }

    public void testMasterFailoverDuringStaleIndicesCleanup() throws Exception {
        internalCluster().startMasterOnlyNodes(3);
        final String dataNode = internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");
        createFullSnapshot(repoName, "empty-snapshot");
        // use a few more shards to make master take a little longer to clean up the stale index and simulate more concurrency between
        // snapshot create and delete below
        createIndexWithContent("index-test", indexSettingsNoReplicas(randomIntBetween(6, 10)).build());
        final NetworkDisruption networkDisruption = isolateMasterDisruption(NetworkDisruption.DISCONNECT);
        internalCluster().setDisruptionScheme(networkDisruption);

        final List<String> fullSnapshotsToDelete = createNSnapshots(repoName, randomIntBetween(1, 5));
        final String masterName = internalCluster().getMasterName();
        blockMasterFromDeletingIndexNFile(repoName);
        final ActionFuture<AcknowledgedResponse> deleteAllSnapshotsWithIndex = startDeleteSnapshots(
            repoName,
            fullSnapshotsToDelete,
            masterName
        );

        // wait for the delete to show up in the CS so that the below snapshot is queued after it for sure
        awaitNDeletionsInProgress(1);
        final ActionFuture<CreateSnapshotResponse> snapshotFuture = startFullSnapshotFromDataNode(repoName, "new-full-snapshot");
        waitForBlock(masterName, repoName);
        awaitNumberOfSnapshotsInProgress(1);
        networkDisruption.startDisrupting();
        ensureStableCluster(3, dataNode);
        // wait for the snapshot to finish while the isolated master is stuck on deleting a data blob
        try {
            snapshotFuture.get();
        } catch (Exception e) {
            // ignore exceptions here, the snapshot will work out fine in all cases but the API might throw because of the master
            // fail-over during the snapshot
            // TODO: remove this leniency once we fix the API to handle master failover cleaner
        }
        awaitNoMoreRunningOperations(dataNode);

        // now unblock the stale master and have it continue deleting blobs from the repository
        unblockNode(repoName, masterName);

        networkDisruption.stopDisrupting();
        ensureStableCluster(4);
        try {
            deleteAllSnapshotsWithIndex.get();
        } catch (Exception ignored) {
            // ignored as we had a failover in here and will get all kinds of errors as a result, just making sure the future completes in
            // all cases for now
            // TODO: remove this leniency once we fix the API to handle master failover cleaner
        }
    }

    public void testStatusMultipleSnapshotsMultipleRepos() throws Exception {
        internalCluster().startMasterOnlyNode();
        // We're blocking a some of the snapshot threads when we block the first repo below so we have to make sure we have enough threads
        // left for the second concurrent snapshot.
        final String dataNode = startDataNodeWithLargeSnapshotPool();
        final String blockedRepoName = "test-repo-blocked-1";
        final String otherBlockedRepoName = "test-repo-blocked-2";
        createRepository(blockedRepoName, "mock");
        createRepository(otherBlockedRepoName, "mock");
        createIndexWithContent("test-index");

        final ActionFuture<CreateSnapshotResponse> createSlowFuture1 = startFullSnapshotBlockedOnDataNode(
            "blocked-snapshot",
            blockedRepoName,
            dataNode
        );
        final ActionFuture<CreateSnapshotResponse> createSlowFuture2 = startFullSnapshotBlockedOnDataNode(
            "blocked-snapshot-2",
            blockedRepoName,
            dataNode
        );
        final ActionFuture<CreateSnapshotResponse> createSlowFuture3 = startFullSnapshotBlockedOnDataNode(
            "other-blocked-snapshot",
            otherBlockedRepoName,
            dataNode
        );
        awaitNumberOfSnapshotsInProgress(3);

        assertSnapshotStatusCountOnRepo("_all", 3);
        assertSnapshotStatusCountOnRepo(blockedRepoName, 2);
        assertSnapshotStatusCountOnRepo(otherBlockedRepoName, 1);

        unblockNode(blockedRepoName, dataNode);
        awaitNumberOfSnapshotsInProgress(1);
        assertSnapshotStatusCountOnRepo("_all", 1);
        assertSnapshotStatusCountOnRepo(blockedRepoName, 0);
        assertSnapshotStatusCountOnRepo(otherBlockedRepoName, 1);

        unblockNode(otherBlockedRepoName, dataNode);
        assertSuccessful(createSlowFuture1);
        assertSuccessful(createSlowFuture2);
        assertSuccessful(createSlowFuture3);
    }

    public void testInterleavedAcrossMultipleRepos() throws Exception {
        internalCluster().startMasterOnlyNode();
        // We're blocking a some of the snapshot threads when we block the first repo below so we have to make sure we have enough threads
        // left for the second concurrent snapshot.
        final String dataNode = startDataNodeWithLargeSnapshotPool();
        final String blockedRepoName = "test-repo-blocked-1";
        final String otherBlockedRepoName = "test-repo-blocked-2";
        createRepository(blockedRepoName, "mock");
        createRepository(otherBlockedRepoName, "mock");
        createIndexWithContent("test-index");

        final ActionFuture<CreateSnapshotResponse> createSlowFuture1 = startFullSnapshotBlockedOnDataNode(
            "blocked-snapshot",
            blockedRepoName,
            dataNode
        );
        final ActionFuture<CreateSnapshotResponse> createSlowFuture2 = startFullSnapshotBlockedOnDataNode(
            "blocked-snapshot-2",
            blockedRepoName,
            dataNode
        );
        final ActionFuture<CreateSnapshotResponse> createSlowFuture3 = startFullSnapshotBlockedOnDataNode(
            "other-blocked-snapshot",
            otherBlockedRepoName,
            dataNode
        );
        awaitNumberOfSnapshotsInProgress(3);
        unblockNode(blockedRepoName, dataNode);
        unblockNode(otherBlockedRepoName, dataNode);

        assertSuccessful(createSlowFuture1);
        assertSuccessful(createSlowFuture2);
        assertSuccessful(createSlowFuture3);
    }

    public void testMasterFailoverAndMultipleQueuedUpSnapshotsAcrossTwoRepos() throws Exception {
        disableRepoConsistencyCheck("This test corrupts the repository on purpose");

        internalCluster().startMasterOnlyNodes(3, LARGE_SNAPSHOT_POOL_SETTINGS);
        final String dataNode = internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        final String otherRepoName = "other-test-repo";
        final Path repoPath = randomRepoPath();
        createRepository(repoName, "mock", repoPath);
        createRepository(otherRepoName, "mock");
        createIndexWithContent("index-one");
        createNSnapshots(repoName, randomIntBetween(2, 5));
        final int countOtherRepo = randomIntBetween(2, 5);
        createNSnapshots(otherRepoName, countOtherRepo);

        corruptIndexN(repoPath, getRepositoryData(repoName).getGenId());

        blockMasterFromFinalizingSnapshotOnIndexFile(repoName);
        blockMasterFromFinalizingSnapshotOnIndexFile(otherRepoName);

        clusterAdmin().prepareCreateSnapshot(repoName, "snapshot-blocked-1").setWaitForCompletion(false).get();
        clusterAdmin().prepareCreateSnapshot(repoName, "snapshot-blocked-2").setWaitForCompletion(false).get();
        clusterAdmin().prepareCreateSnapshot(otherRepoName, "snapshot-other-blocked-1").setWaitForCompletion(false).get();
        clusterAdmin().prepareCreateSnapshot(otherRepoName, "snapshot-other-blocked-2").setWaitForCompletion(false).get();

        awaitNumberOfSnapshotsInProgress(4);
        final String initialMaster = internalCluster().getMasterName();
        waitForBlock(initialMaster, repoName);
        waitForBlock(initialMaster, otherRepoName);

        internalCluster().stopCurrentMasterNode();
        ensureStableCluster(3, dataNode);
        awaitNoMoreRunningOperations();

        final RepositoryData repositoryData = getRepositoryData(otherRepoName);
        assertThat(repositoryData.getSnapshotIds(), hasSize(countOtherRepo + 2));
    }

    public void testConcurrentOperationsLimit() throws Exception {
        final String masterName = internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");
        createIndexWithContent("index-test");

        final int limitToTest = randomIntBetween(1, 3);
        final List<String> snapshotNames = createNSnapshots(repoName, limitToTest + 1);

        updateClusterSettings(Settings.builder().put(SnapshotsService.MAX_CONCURRENT_SNAPSHOT_OPERATIONS_SETTING.getKey(), limitToTest));

        blockNodeOnAnyFiles(repoName, masterName);
        int blockedSnapshots = 0;
        final List<ActionFuture<CreateSnapshotResponse>> snapshotFutures = new ArrayList<>();
        ActionFuture<AcknowledgedResponse> deleteFuture = null;
        for (int i = 0; i < limitToTest; ++i) {
            if (deleteFuture != null || randomBoolean()) {
                snapshotFutures.add(startFullSnapshot(repoName, "snap-" + i));
                ++blockedSnapshots;
            } else {
                deleteFuture = startDeleteSnapshot(repoName, randomFrom(snapshotNames));
            }
        }
        awaitNumberOfSnapshotsInProgress(blockedSnapshots);
        if (deleteFuture != null) {
            awaitNDeletionsInProgress(1);
        }
        waitForBlock(masterName, repoName);

        final ConcurrentSnapshotExecutionException cse = expectThrows(
            ConcurrentSnapshotExecutionException.class,
            () -> clusterAdmin().prepareCreateSnapshot(repoName, "expected-to-fail").execute().actionGet()
        );
        assertThat(
            cse.getMessage(),
            containsString(
                "Cannot start another operation, already running ["
                    + limitToTest
                    + "] operations and the current limit for concurrent snapshot operations is set to ["
                    + limitToTest
                    + "]"
            )
        );
        boolean deleteAndAbortAll = false;
        if (deleteFuture == null && randomBoolean()) {
            deleteFuture = clusterAdmin().prepareDeleteSnapshot(repoName, "*").execute();
            deleteAndAbortAll = true;
            if (randomBoolean()) {
                awaitNDeletionsInProgress(1);
            }
        }

        unblockNode(repoName, masterName);
        if (deleteFuture != null) {
            assertAcked(deleteFuture.get());
        }

        if (deleteAndAbortAll) {
            awaitNumberOfSnapshotsInProgress(0);
            for (ActionFuture<CreateSnapshotResponse> snapshotFuture : snapshotFutures) {
                // just check that the futures resolve, whether or not things worked out with the snapshot actually finalizing or failing
                // due to the abort does not matter
                assertBusy(() -> assertTrue(snapshotFuture.isDone()));
            }
            assertThat(getRepositoryData(repoName).getSnapshotIds(), empty());
        } else {
            for (ActionFuture<CreateSnapshotResponse> snapshotFuture : snapshotFutures) {
                assertSuccessful(snapshotFuture);
            }
        }
    }

    public void testConcurrentSnapshotWorksWithOldVersionRepo() throws Exception {
        internalCluster().startMasterOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        final Path repoPath = randomRepoPath();
        createRepository(
            repoName,
            "mock",
            Settings.builder().put(BlobStoreRepository.CACHE_REPOSITORY_DATA.getKey(), false).put("location", repoPath)
        );
        initWithSnapshotVersion(repoName, repoPath, SnapshotsService.OLD_SNAPSHOT_FORMAT);

        createIndexWithContent("index-slow");

        final ActionFuture<CreateSnapshotResponse> createSlowFuture = startFullSnapshotBlockedOnDataNode(
            "slow-snapshot",
            repoName,
            dataNode
        );

        final String dataNode2 = internalCluster().startDataOnlyNode();
        ensureStableCluster(3);
        final String indexFast = "index-fast";
        createIndexWithContent(indexFast, dataNode2, dataNode);

        final ActionFuture<CreateSnapshotResponse> createFastSnapshot = startFullSnapshot(repoName, "fast-snapshot");

        assertThat(createSlowFuture.isDone(), is(false));
        unblockNode(repoName, dataNode);

        assertSuccessful(createFastSnapshot);
        assertSuccessful(createSlowFuture);

        final RepositoryData repositoryData = getRepositoryData(repoName);
        assertThat(repositoryData.shardGenerations(), is(ShardGenerations.EMPTY));
    }

    public void testQueuedDeleteAfterFinalizationFailure() throws Exception {
        final String masterNode = internalCluster().startMasterOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");
        blockMasterFromFinalizingSnapshotOnIndexFile(repoName);
        final String snapshotName = "snap-1";
        final ActionFuture<CreateSnapshotResponse> snapshotFuture = startFullSnapshot(repoName, snapshotName);
        waitForBlock(masterNode, repoName);
        final ActionFuture<AcknowledgedResponse> deleteFuture = startDeleteSnapshot(repoName, snapshotName);
        awaitNDeletionsInProgress(1);
        unblockNode(repoName, masterNode);
        assertAcked(deleteFuture.get());
        final SnapshotException sne = expectThrows(SnapshotException.class, snapshotFuture::actionGet);
        assertThat(sne.getCause().getMessage(), containsString("exception after block"));
    }

    public void testAbortNotStartedSnapshotWithoutIO() throws Exception {
        internalCluster().startMasterOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");
        createIndexWithContent("test-index");

        final ActionFuture<CreateSnapshotResponse> createSnapshot1Future = startFullSnapshotBlockedOnDataNode(
            "first-snapshot",
            repoName,
            dataNode
        );

        final String snapshotTwo = "second-snapshot";
        final ActionFuture<CreateSnapshotResponse> createSnapshot2Future = startFullSnapshot(repoName, snapshotTwo);

        awaitNumberOfSnapshotsInProgress(2);

        assertAcked(startDeleteSnapshot(repoName, snapshotTwo).get());
        final SnapshotException sne = expectThrows(SnapshotException.class, createSnapshot2Future::actionGet);

        assertFalse(createSnapshot1Future.isDone());
        unblockNode(repoName, dataNode);
        assertSuccessful(createSnapshot1Future);
        assertThat(getRepositoryData(repoName).getGenId(), is(0L));
    }

    public void testStartWithSuccessfulShardSnapshotPendingFinalization() throws Exception {
        final String masterName = internalCluster().startMasterOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");

        createIndexWithContent("test-idx");
        createFullSnapshot(repoName, "first-snapshot");

        blockMasterOnWriteIndexFile(repoName);
        final ActionFuture<CreateSnapshotResponse> blockedSnapshot = startFullSnapshot(repoName, "snap-blocked");
        waitForBlock(masterName, repoName);
        awaitNumberOfSnapshotsInProgress(1);
        blockNodeOnAnyFiles(repoName, dataNode);
        final ActionFuture<CreateSnapshotResponse> otherSnapshot = startFullSnapshot(repoName, "other-snapshot");
        awaitNumberOfSnapshotsInProgress(2);
        assertFalse(blockedSnapshot.isDone());
        unblockNode(repoName, masterName);
        awaitNumberOfSnapshotsInProgress(1);

        awaitMasterFinishRepoOperations();

        unblockNode(repoName, dataNode);
        assertSuccessful(blockedSnapshot);
        assertSuccessful(otherSnapshot);
    }

    public void testConcurrentRestoreDeleteAndClone() throws Exception {
        final String repository = "test-repo";
        createRepository(logger, repository, "fs");

        final int nbIndices = randomIntBetween(10, 20);

        for (int i = 0; i < nbIndices; i++) {
            final String index = "index-" + i;
            createIndexWithContent(index);
            final String snapshot = "snapshot-" + i;
            createSnapshot(repository, snapshot, List.of(index));
        }

        final List<ActionFuture<AcknowledgedResponse>> cloneFutures = new ArrayList<>();
        final List<ActionFuture<RestoreSnapshotResponse>> restoreFutures = new ArrayList<>();

        for (int i = 0; i < nbIndices; i++) {
            if (randomBoolean()) {
                restoreFutures.add(
                    clusterAdmin().prepareRestoreSnapshot(repository, "snapshot-" + i)
                        .setIndices("index-" + i)
                        .setRenamePattern("(.+)")
                        .setRenameReplacement("$1-restored-" + i)
                        .setWaitForCompletion(true)
                        .execute()
                );
            } else {
                cloneFutures.add(
                    clusterAdmin().prepareCloneSnapshot(repository, "snapshot-" + i, "clone-" + i).setIndices("index-" + i).execute()
                );
            }
        }

        // make deletes and clones complete concurrently
        final List<ActionFuture<AcknowledgedResponse>> deleteFutures = new ArrayList<>(nbIndices);
        for (int i = 0; i < nbIndices; i++) {
            deleteFutures.add(startDeleteSnapshot(repository, "snapshot-" + i));
        }

        for (ActionFuture<RestoreSnapshotResponse> operation : restoreFutures) {
            try {
                final RestoreInfo restoreResponse = operation.get().getRestoreInfo();
                assertThat(restoreResponse.successfulShards(), greaterThanOrEqualTo(1));
                assertEquals(0, restoreResponse.failedShards());
            } catch (ExecutionException e) {
                final Throwable csee = ExceptionsHelper.unwrap(e, ConcurrentSnapshotExecutionException.class);
                assertThat(csee, instanceOf(ConcurrentSnapshotExecutionException.class));
            }
        }
        for (ActionFuture<AcknowledgedResponse> operation : cloneFutures) {
            try {
                assertAcked(operation.get());
            } catch (ExecutionException e) {
                final Throwable csee = ExceptionsHelper.unwrap(e, SnapshotException.class);
                assertThat(
                    csee,
                    either(instanceOf(ConcurrentSnapshotExecutionException.class)).or(instanceOf(SnapshotMissingException.class))
                );
            }
        }
        for (ActionFuture<AcknowledgedResponse> operation : deleteFutures) {
            try {
                assertAcked(operation.get());
            } catch (ExecutionException e) {
                final Throwable csee = ExceptionsHelper.unwrap(e, ConcurrentSnapshotExecutionException.class);
                assertThat(csee, instanceOf(ConcurrentSnapshotExecutionException.class));
            }
        }
        awaitNoMoreRunningOperations();
    }

    public void testOutOfOrderFinalization() throws Exception {
        internalCluster().startMasterOnlyNode();
        final List<String> dataNodes = internalCluster().startDataOnlyNodes(2);
        final String index1 = "index-1";
        final String index2 = "index-2";
        createIndexWithContent(index1, dataNodes.get(0), dataNodes.get(1));
        createIndexWithContent(index2, dataNodes.get(1), dataNodes.get(0));

        final String repository = "test-repo";
        createRepository(repository, "mock");

        blockNodeWithIndex(repository, index2);

        final ActionFuture<CreateSnapshotResponse> snapshot1 = clusterAdmin().prepareCreateSnapshot(repository, "snapshot-1")
            .setIndices(index1, index2)
            .setWaitForCompletion(true)
            .execute();
        awaitNumberOfSnapshotsInProgress(1);
        final ActionFuture<CreateSnapshotResponse> snapshot2 = clusterAdmin().prepareCreateSnapshot(repository, "snapshot-2")
            .setIndices(index1)
            .setWaitForCompletion(true)
            .execute();
        assertSuccessful(snapshot2);
        unblockAllDataNodes(repository);
        final SnapshotInfo sn1 = assertSuccessful(snapshot1);

        assertAcked(startDeleteSnapshot(repository, sn1.snapshot().getSnapshotId().getName()).get());

        assertThat(
            clusterAdmin().prepareSnapshotStatus().setSnapshots("snapshot-2").setRepository(repository).get().getSnapshots(),
            hasSize(1)
        );
    }

    public void testOutOfOrderAndConcurrentFinalization() throws Exception {
        final String master = internalCluster().startMasterOnlyNode();
        final List<String> dataNodes = internalCluster().startDataOnlyNodes(2);
        final String index1 = "index-1";
        final String index2 = "index-2";
        createIndexWithContent(index1, dataNodes.get(0), dataNodes.get(1));
        createIndexWithContent(index2, dataNodes.get(1), dataNodes.get(0));

        final String repository = "test-repo";
        createRepository(repository, "mock");

        blockNodeWithIndex(repository, index2);

        final ActionFuture<CreateSnapshotResponse> snapshot1 = clusterAdmin().prepareCreateSnapshot(repository, "snapshot-1")
            .setIndices(index1, index2)
            .setWaitForCompletion(true)
            .execute();
        awaitNumberOfSnapshotsInProgress(1);

        blockMasterOnWriteIndexFile(repository);
        final ActionFuture<CreateSnapshotResponse> snapshot2 = clusterAdmin().prepareCreateSnapshot(repository, "snapshot-2")
            .setIndices(index1)
            .setWaitForCompletion(true)
            .execute();

        awaitClusterState(state -> {
            final List<SnapshotsInProgress.Entry> snapshotsInProgress = state.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY)
                .forRepo(repository);
            return snapshotsInProgress.size() == 2 && snapshotsInProgress.get(1).state().completed();
        });

        unblockAllDataNodes(repository);
        awaitClusterState(
            state -> state.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY).forRepo(repository).get(0).state().completed()
        );

        unblockNode(repository, master);
        assertSuccessful(snapshot2);

        final SnapshotInfo sn1 = assertSuccessful(snapshot1);
        assertAcked(startDeleteSnapshot(repository, sn1.snapshot().getSnapshotId().getName()).get());

        assertThat(
            clusterAdmin().prepareSnapshotStatus().setSnapshots("snapshot-2").setRepository(repository).get().getSnapshots(),
            hasSize(1)
        );
    }

    public void testOutOfOrderFinalizationWithConcurrentClone() throws Exception {
        internalCluster().startMasterOnlyNode();
        final List<String> dataNodes = internalCluster().startDataOnlyNodes(2);
        final String index1 = "index-1";
        final String index2 = "index-2";
        createIndexWithContent(index1, dataNodes.get(0), dataNodes.get(1));
        createIndexWithContent(index2, dataNodes.get(1), dataNodes.get(0));

        final String repository = "test-repo";
        createRepository(repository, "mock");
        final String sourceSnapshot = "source-snapshot";
        createFullSnapshot(repository, sourceSnapshot);
        indexDoc(index2, "doc_id", "foo", "bar");

        blockNodeWithIndex(repository, index2);

        final String sn1 = "snapshot-1";
        final ActionFuture<CreateSnapshotResponse> snapshot1 = clusterAdmin().prepareCreateSnapshot(repository, sn1)
            .setIndices(index1, index2)
            .setWaitForCompletion(true)
            .execute();
        awaitNumberOfSnapshotsInProgress(1);

        final String targetSnapshot = "target-snapshot";
        final ActionFuture<AcknowledgedResponse> clone = clusterAdmin().prepareCloneSnapshot(repository, sourceSnapshot, targetSnapshot)
            .setIndices(index1)
            .execute();
        assertAcked(clone.get());

        unblockAllDataNodes(repository);
        assertSuccessful(snapshot1);

        logger.info("--> deleting snapshots [{},{}] from repo [{}]", sn1, sourceSnapshot, repository);
        assertAcked(clusterAdmin().prepareDeleteSnapshot(repository).setSnapshots(sn1, sourceSnapshot).get());

        assertThat(
            clusterAdmin().prepareSnapshotStatus().setSnapshots(targetSnapshot).setRepository(repository).get().getSnapshots(),
            hasSize(1)
        );
    }

    public void testOutOfOrderCloneFinalization() throws Exception {
        final String master = internalCluster().startMasterOnlyNode(LARGE_SNAPSHOT_POOL_SETTINGS);
        internalCluster().startDataOnlyNode();
        final String index1 = "index-1";
        final String index2 = "index-2";
        createIndexWithContent(index1);
        createIndexWithContent(index2);

        final String repository = "test-repo";
        createRepository(repository, "mock");

        final String sourceSnapshot = "source-snapshot";
        createFullSnapshot(repository, sourceSnapshot);

        final IndexId index1Id = getRepositoryData(repository).resolveIndexId(index1);
        blockMasterOnShardLevelSnapshotFile(repository, index1Id.getId());

        final String cloneTarget = "target-snapshot";
        final ActionFuture<AcknowledgedResponse> cloneSnapshot = clusterAdmin().prepareCloneSnapshot(
            repository,
            sourceSnapshot,
            cloneTarget
        ).setIndices(index1, index2).execute();
        awaitNumberOfSnapshotsInProgress(1);
        waitForBlock(master, repository);

        final ActionFuture<CreateSnapshotResponse> snapshot2 = clusterAdmin().prepareCreateSnapshot(repository, "snapshot-2")
            .setIndices(index2)
            .setWaitForCompletion(true)
            .execute();
        assertSuccessful(snapshot2);

        unblockNode(repository, master);
        assertAcked(cloneSnapshot.get());
        assertAcked(startDeleteSnapshot(repository, cloneTarget).get());

        assertThat(
            clusterAdmin().prepareSnapshotStatus().setSnapshots("snapshot-2").setRepository(repository).get().getSnapshots(),
            hasSize(1)
        );
    }

    public void testCorrectlyFinalizeOutOfOrderPartialFailures() throws Exception {
        internalCluster().startMasterOnlyNode();
        final String dataNode1 = internalCluster().startDataOnlyNode();
        final String dataNode2 = internalCluster().startDataOnlyNode();
        final String index1 = "index-1";
        final String index2 = "index-2";
        createIndexWithContent(index1, dataNode1, dataNode2);
        createIndexWithContent(index2, dataNode2, dataNode1);

        final String repository = "test-repo";
        createRepository(repository, "mock");

        createFullSnapshot(repository, "snapshot-1");
        index(index1, "some_doc", Map.of("foo", "bar"));
        index(index2, "some_doc", Map.of("foo", "bar"));
        blockAndFailDataNode(repository, dataNode1);
        blockDataNode(repository, dataNode2);
        final ActionFuture<CreateSnapshotResponse> snapshotBlocked = startFullSnapshot(repository, "snapshot-2");
        waitForBlock(dataNode1, repository);
        waitForBlock(dataNode2, repository);

        unblockNode(repository, dataNode1);
        assertAcked(clusterAdmin().prepareCloneSnapshot(repository, "snapshot-1", "target-1").setIndices(index1).get());
        unblockNode(repository, dataNode2);
        snapshotBlocked.get();

        assertThat(
            clusterAdmin().prepareSnapshotStatus().setSnapshots("target-1").setRepository(repository).get().getSnapshots(),
            hasSize(1)
        );

        createFullSnapshot(repository, "snapshot-3");
    }

    public void testIndexDeletedWhileSnapshotQueuedAfterClone() throws Exception {
        final String master = internalCluster().startMasterOnlyNode(LARGE_SNAPSHOT_POOL_SETTINGS);
        internalCluster().startDataOnlyNode();
        final String index1 = "index-1";
        final String index2 = "index-2";
        createIndexWithContent(index1);
        createIndexWithContent(index2);

        final String repository = "test-repo";
        createRepository(repository, "mock");

        final String sourceSnapshot = "source-snapshot";
        createFullSnapshot(repository, sourceSnapshot);

        final IndexId index1Id = getRepositoryData(repository).resolveIndexId(index1);
        blockMasterOnShardLevelSnapshotFile(repository, index1Id.getId());

        final String cloneTarget = "target-snapshot";
        final ActionFuture<AcknowledgedResponse> cloneSnapshot = clusterAdmin().prepareCloneSnapshot(
            repository,
            sourceSnapshot,
            cloneTarget
        ).setIndices(index1, index2).execute();
        awaitNumberOfSnapshotsInProgress(1);
        waitForBlock(master, repository);

        final ActionFuture<CreateSnapshotResponse> snapshot3 = clusterAdmin().prepareCreateSnapshot(repository, "snapshot-3")
            .setIndices(index1, index2)
            .setWaitForCompletion(true)
            .setPartial(true)
            .execute();
        final ActionFuture<CreateSnapshotResponse> snapshot2 = clusterAdmin().prepareCreateSnapshot(repository, "snapshot-2")
            .setIndices(index2)
            .setWaitForCompletion(true)
            .execute();
        assertSuccessful(snapshot2);
        awaitNumberOfSnapshotsInProgress(2);
        assertFalse(snapshot3.isDone());
        assertAcked(admin().indices().prepareDelete(index1).get());
        assertSuccessful(snapshot3);
        unblockNode(repository, master);

        assertAcked(cloneSnapshot.get());
        assertAcked(startDeleteSnapshot(repository, cloneTarget).get());

        assertThat(
            clusterAdmin().prepareSnapshotStatus().setSnapshots("snapshot-2", "snapshot-3").setRepository(repository).get().getSnapshots(),
            hasSize(2)
        );
    }

    public void testIndexDeletedWhileSnapshotAndCloneQueuedAfterClone() throws Exception {
        final String master = internalCluster().startMasterOnlyNode(LARGE_SNAPSHOT_POOL_SETTINGS);
        internalCluster().startDataOnlyNode();
        final String index1 = "index-1";
        final String index2 = "index-2";
        createIndexWithContent(index1);
        createIndexWithContent(index2);

        final String repository = "test-repo";
        createRepository(repository, "mock");

        final String sourceSnapshot = "source-snapshot";
        createFullSnapshot(repository, sourceSnapshot);

        final IndexId index1Id = getRepositoryData(repository).resolveIndexId(index1);
        blockMasterOnShardLevelSnapshotFile(repository, index1Id.getId());

        final String cloneTarget = "target-snapshot";
        final ActionFuture<AcknowledgedResponse> cloneSnapshot = clusterAdmin().prepareCloneSnapshot(
            repository,
            sourceSnapshot,
            cloneTarget
        ).setIndices(index1, index2).execute();
        awaitNumberOfSnapshotsInProgress(1);
        waitForBlock(master, repository);

        final ActionFuture<CreateSnapshotResponse> snapshot3 = clusterAdmin().prepareCreateSnapshot(repository, "snapshot-3")
            .setIndices(index1, index2)
            .setWaitForCompletion(true)
            .setPartial(true)
            .execute();
        final ActionFuture<CreateSnapshotResponse> snapshot2 = clusterAdmin().prepareCreateSnapshot(repository, "snapshot-2")
            .setIndices(index2)
            .setWaitForCompletion(true)
            .execute();
        assertSuccessful(snapshot2);
        awaitNumberOfSnapshotsInProgress(2);
        assertFalse(snapshot3.isDone());

        final String cloneTarget2 = "target-snapshot-2";
        final ActionFuture<AcknowledgedResponse> cloneSnapshot2 = clusterAdmin().prepareCloneSnapshot(
            repository,
            sourceSnapshot,
            cloneTarget2
        ).setIndices(index1, index2).execute();

        assertAcked(admin().indices().prepareDelete(index1).get());
        assertSuccessful(snapshot3);
        unblockNode(repository, master);

        assertAcked(cloneSnapshot.get());
        assertAcked(cloneSnapshot2.get());
        assertAcked(startDeleteSnapshot(repository, cloneTarget).get());

        assertThat(
            clusterAdmin().prepareSnapshotStatus().setSnapshots("snapshot-2", "snapshot-3").setRepository(repository).get().getSnapshots(),
            hasSize(2)
        );
    }

    public void testQueuedAfterFailedShardSnapshot() throws Exception {
        internalCluster().startMasterOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();

        final String repository = "test-repo";
        createRepository(repository, "mock");

        final String indexName = "test-idx";
        createIndexWithContent(indexName);
        final String fullSnapshot = "full-snapshot";
        createFullSnapshot(repository, fullSnapshot);

        indexDoc(indexName, "some_id", "foo", "bar");
        blockAndFailDataNode(repository, dataNode);
        final ActionFuture<CreateSnapshotResponse> snapshotFutureFailure = startFullSnapshot(repository, "failing-snapshot");
        awaitNumberOfSnapshotsInProgress(1);
        waitForBlock(dataNode, repository);
        final ActionFuture<CreateSnapshotResponse> snapshotFutureSuccess = startFullSnapshot(repository, "successful-snapshot");
        awaitNumberOfSnapshotsInProgress(2);
        unblockNode(repository, dataNode);

        assertSuccessful(snapshotFutureSuccess);
        final SnapshotInfo failedSnapshot = snapshotFutureFailure.get().getSnapshotInfo();
        assertEquals(SnapshotState.PARTIAL, failedSnapshot.state());

        final SnapshotsStatusResponse snapshotsStatusResponse1 = clusterAdmin().prepareSnapshotStatus(repository)
            .setSnapshots(fullSnapshot)
            .get();

        final String tmpSnapshot = "snapshot-tmp";
        createFullSnapshot(repository, tmpSnapshot);
        assertAcked(startDeleteSnapshot(repository, tmpSnapshot).get());

        final SnapshotsStatusResponse snapshotsStatusResponse2 = clusterAdmin().prepareSnapshotStatus(repository)
            .setSnapshots(fullSnapshot)
            .get();
        assertEquals(snapshotsStatusResponse1, snapshotsStatusResponse2);

        assertAcked(startDeleteSnapshot(repository, "successful-snapshot").get());

        final SnapshotsStatusResponse snapshotsStatusResponse3 = clusterAdmin().prepareSnapshotStatus(repository)
            .setSnapshots(fullSnapshot)
            .get();
        assertEquals(snapshotsStatusResponse1, snapshotsStatusResponse3);
    }

    public void testOutOfOrderFinalizationManySnapshots() throws Exception {
        internalCluster().startMasterOnlyNode();
        final List<String> dataNodes = internalCluster().startDataOnlyNodes(2);
        final String index1 = "index-1";
        final String index2 = "index-2";
        createIndexWithContent(index1, dataNodes.get(0), dataNodes.get(1));
        createIndexWithContent(index2, dataNodes.get(1), dataNodes.get(0));

        final String repository = "test-repo";
        createRepository(repository, "mock");

        blockNodeWithIndex(repository, index2);

        final ActionFuture<CreateSnapshotResponse> snapshot1 = clusterAdmin().prepareCreateSnapshot(repository, "snapshot-1")
            .setIndices(index1, index2)
            .setWaitForCompletion(true)
            .execute();
        final ActionFuture<CreateSnapshotResponse> snapshot2 = clusterAdmin().prepareCreateSnapshot(repository, "snapshot-2")
            .setIndices(index1, index2)
            .setWaitForCompletion(true)
            .execute();
        awaitNumberOfSnapshotsInProgress(2);
        final ActionFuture<CreateSnapshotResponse> snapshot3 = clusterAdmin().prepareCreateSnapshot(repository, "snapshot-3")
            .setIndices(index1)
            .setWaitForCompletion(true)
            .execute();
        assertSuccessful(snapshot3);
        unblockAllDataNodes(repository);
        assertSuccessful(snapshot1);
        assertSuccessful(snapshot2);

        assertThat(
            clusterAdmin().prepareSnapshotStatus().setSnapshots("snapshot-2").setRepository(repository).get().getSnapshots(),
            hasSize(1)
        );
    }

    public void testCloneQueuedAfterMissingShard() throws Exception {
        final String master = internalCluster().startMasterOnlyNode();
        final List<String> dataNodes = internalCluster().startDataOnlyNodes(2);
        final String index1 = "index-1";
        final String index2 = "index-2";
        createIndexWithContent(index1, dataNodes.get(0), dataNodes.get(1));
        createIndexWithContent(index2, dataNodes.get(1), dataNodes.get(0));

        final String repository = "test-repo";
        createRepository(repository, "mock");
        final String snapshotToDelete = "snapshot-to-delete";
        createFullSnapshot(repository, snapshotToDelete);
        final String cloneSource = "source-snapshot";
        createFullSnapshot(repository, cloneSource);

        internalCluster().stopNode(dataNodes.get(0));

        blockMasterOnWriteIndexFile(repository);
        final ActionFuture<AcknowledgedResponse> deleteFuture = clusterAdmin().prepareDeleteSnapshot(repository, snapshotToDelete)
            .execute();
        awaitNDeletionsInProgress(1);

        final ActionFuture<CreateSnapshotResponse> snapshot1 = startFullSnapshot(repository, "snapshot-1", true);
        awaitNumberOfSnapshotsInProgress(1);

        final ActionFuture<AcknowledgedResponse> cloneFuture = clusterAdmin().prepareCloneSnapshot(
            repository,
            cloneSource,
            "target-snapshot"
        ).setIndices(index1).execute();
        awaitNumberOfSnapshotsInProgress(2);

        unblockNode(repository, master);
        assertAcked(deleteFuture.get());
        assertAcked(cloneFuture.get());
        awaitNoMoreRunningOperations();
        assertThat(snapshot1.get().getSnapshotInfo().state(), is(SnapshotState.PARTIAL));
    }

    public void testSnapshotQueuedAfterMissingShard() throws Exception {
        final String master = internalCluster().startMasterOnlyNode();
        final List<String> dataNodes = internalCluster().startDataOnlyNodes(2);
        final String index1 = "index-1";
        final String index2 = "index-2";
        createIndexWithContent(index1, dataNodes.get(0), dataNodes.get(1));
        createIndexWithContent(index2, dataNodes.get(1), dataNodes.get(0));

        final String repository = "test-repo";
        createRepository(repository, "mock");
        final String snapshotToDelete = "snapshot-to-delete";
        createFullSnapshot(repository, snapshotToDelete);

        internalCluster().stopNode(dataNodes.get(0));

        blockMasterOnWriteIndexFile(repository);
        final ActionFuture<AcknowledgedResponse> deleteFuture = startDeleteSnapshot(repository, snapshotToDelete);
        awaitNDeletionsInProgress(1);

        final ActionFuture<CreateSnapshotResponse> snapshot1 = startFullSnapshot(repository, "snapshot-1", true);
        awaitNumberOfSnapshotsInProgress(1);

        final ActionFuture<CreateSnapshotResponse> snapshot2 = startFullSnapshot(repository, "snapshot-2", true);
        awaitNumberOfSnapshotsInProgress(2);

        unblockNode(repository, master);
        assertAcked(deleteFuture.get());
        awaitNoMoreRunningOperations();
        assertThat(snapshot1.get().getSnapshotInfo().state(), is(SnapshotState.PARTIAL));
        assertThat(snapshot2.get().getSnapshotInfo().state(), is(SnapshotState.PARTIAL));
    }

    public void testSnapshotAndCloneQueuedAfterMissingShard() throws Exception {
        final String master = internalCluster().startMasterOnlyNode();
        final List<String> dataNodes = internalCluster().startDataOnlyNodes(2);
        final String index1 = "index-1";
        final String index2 = "index-2";
        createIndexWithContent(index1, dataNodes.get(0), dataNodes.get(1));
        createIndexWithContent(index2, dataNodes.get(1), dataNodes.get(0));

        final String repository = "test-repo";
        createRepository(repository, "mock");
        final String snapshotToDelete = "snapshot-to-delete";
        createFullSnapshot(repository, snapshotToDelete);
        final String cloneSource = "source-snapshot";
        createFullSnapshot(repository, cloneSource);

        internalCluster().stopNode(dataNodes.get(0));

        blockMasterOnWriteIndexFile(repository);
        final ActionFuture<AcknowledgedResponse> deleteFuture = clusterAdmin().prepareDeleteSnapshot(repository, snapshotToDelete)
            .execute();
        awaitNDeletionsInProgress(1);

        final ActionFuture<CreateSnapshotResponse> snapshot1 = startFullSnapshot(repository, "snapshot-1", true);
        awaitNumberOfSnapshotsInProgress(1);

        final ActionFuture<CreateSnapshotResponse> snapshot2 = startFullSnapshot(repository, "snapshot-2", true);
        awaitNumberOfSnapshotsInProgress(2);

        final ActionFuture<AcknowledgedResponse> cloneFuture = clusterAdmin().prepareCloneSnapshot(
            repository,
            cloneSource,
            "target-snapshot"
        ).setIndices(index1).execute();
        awaitNumberOfSnapshotsInProgress(3);

        unblockNode(repository, master);
        assertAcked(deleteFuture.get());
        assertAcked(cloneFuture.get());
        awaitNoMoreRunningOperations();
        assertThat(snapshot1.get().getSnapshotInfo().state(), is(SnapshotState.PARTIAL));
        assertThat(snapshot2.get().getSnapshotInfo().state(), is(SnapshotState.PARTIAL));
    }

    public void testQueuedSnapshotAfterPartialWithIndexRecreate() throws Exception {
        internalCluster().startNodes(3);
        // create an index with a large number of shards so that the nodes will not be able to start all shard snapshots before the index
        // is deleted
        final Settings highShardCountSettings = indexSettingsNoReplicas(randomIntBetween(12, 24)).build();
        final String index1 = "index-1";
        createIndexWithContent(index1, highShardCountSettings);
        final String index2 = "index-2";
        createIndexWithContent(index2);
        final String repoName = "test-repo";
        createRepository(repoName, "mock");
        final ActionFuture<CreateSnapshotResponse> partialFuture = startFullSnapshot(repoName, "partial-snapshot", true);
        blockAllDataNodes(repoName);
        waitForBlockOnAnyDataNode(repoName);
        // recreate index and start full snapshot to test that shard state updates from the first partial snapshot are correctly are
        // correctly applied to the second snapshot that will contain a different index by the same name
        assertAcked(client().admin().indices().prepareDelete(index1).get());
        createIndexWithContent(index1, highShardCountSettings);
        final ActionFuture<CreateSnapshotResponse> nonPartialFuture = startFullSnapshot(repoName, "full-snapshot");
        unblockAllDataNodes(repoName);
        assertSuccessful(nonPartialFuture);
        assertSuccessful(partialFuture);
    }

    private static void assertSnapshotStatusCountOnRepo(String otherBlockedRepoName, int count) {
        final SnapshotsStatusResponse snapshotsStatusResponse = clusterAdmin().prepareSnapshotStatus(otherBlockedRepoName).get();
        final List<SnapshotStatus> snapshotStatuses = snapshotsStatusResponse.getSnapshots();
        assertThat(snapshotStatuses, hasSize(count));
    }

    private ActionFuture<AcknowledgedResponse> startDeleteFromNonMasterClient(String repoName, String snapshotName) {
        logger.info("--> deleting snapshot [{}] from repo [{}] from non master client", snapshotName, repoName);
        return internalCluster().nonMasterClient().admin().cluster().prepareDeleteSnapshot(repoName, snapshotName).execute();
    }

    private ActionFuture<CreateSnapshotResponse> startFullSnapshotFromNonMasterClient(String repoName, String snapshotName) {
        logger.info("--> creating full snapshot [{}] to repo [{}] from non master client", snapshotName, repoName);
        return internalCluster().nonMasterClient()
            .admin()
            .cluster()
            .prepareCreateSnapshot(repoName, snapshotName)
            .setWaitForCompletion(true)
            .execute();
    }

    private ActionFuture<CreateSnapshotResponse> startFullSnapshotFromDataNode(String repoName, String snapshotName) {
        logger.info("--> creating full snapshot [{}] to repo [{}] from data node client", snapshotName, repoName);
        return internalCluster().dataNodeClient()
            .admin()
            .cluster()
            .prepareCreateSnapshot(repoName, snapshotName)
            .setWaitForCompletion(true)
            .execute();
    }

    private ActionFuture<CreateSnapshotResponse> startFullSnapshotFromMasterClient(String repoName, String snapshotName) {
        logger.info("--> creating full snapshot [{}] to repo [{}] from master client", snapshotName, repoName);
        return internalCluster().masterClient()
            .admin()
            .cluster()
            .prepareCreateSnapshot(repoName, snapshotName)
            .setWaitForCompletion(true)
            .execute();
    }

    private void createIndexWithContent(String indexName, String nodeInclude, String nodeExclude) {
        createIndexWithContent(
            indexName,
            indexSettingsNoReplicas(1).put("index.routing.allocation.include._name", nodeInclude)
                .put("index.routing.allocation.exclude._name", nodeExclude)
                .build()
        );
    }

    private static boolean snapshotHasCompletedShard(String repoName, String snapshot, SnapshotsInProgress snapshotsInProgress) {
        for (SnapshotsInProgress.Entry entry : snapshotsInProgress.forRepo(repoName)) {
            if (entry.snapshot().getSnapshotId().getName().equals(snapshot)) {
                for (SnapshotsInProgress.ShardSnapshotStatus shard : entry.shards().values()) {
                    if (shard.state().completed()) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private void corruptIndexN(Path repoPath, long generation) throws IOException {
        logger.info("--> corrupting [index-{}] in [{}]", generation, repoPath);
        Path indexNBlob = repoPath.resolve(BlobStoreRepository.INDEX_FILE_PREFIX + generation);
        assertFileExists(indexNBlob);
        Files.write(indexNBlob, randomByteArrayOfLength(1), StandardOpenOption.TRUNCATE_EXISTING);
    }

    private static List<SnapshotInfo> currentSnapshots(String repoName) {
        return clusterAdmin().prepareGetSnapshots(repoName).setSnapshots(GetSnapshotsRequest.CURRENT_SNAPSHOT).get().getSnapshots();
    }

    private ActionFuture<AcknowledgedResponse> startAndBlockOnDeleteSnapshot(String repoName, String snapshotName) throws Exception {
        final String masterName = internalCluster().getMasterName();
        blockNodeOnAnyFiles(repoName, masterName);
        final ActionFuture<AcknowledgedResponse> fut = startDeleteSnapshot(repoName, snapshotName);
        waitForBlock(masterName, repoName);
        return fut;
    }

    private ActionFuture<CreateSnapshotResponse> startAndBlockFailingFullSnapshot(String blockedRepoName, String snapshotName)
        throws Exception {
        blockMasterFromFinalizingSnapshotOnIndexFile(blockedRepoName);
        final ActionFuture<CreateSnapshotResponse> fut = startFullSnapshot(blockedRepoName, snapshotName);
        waitForBlock(internalCluster().getMasterName(), blockedRepoName);
        return fut;
    }
}
