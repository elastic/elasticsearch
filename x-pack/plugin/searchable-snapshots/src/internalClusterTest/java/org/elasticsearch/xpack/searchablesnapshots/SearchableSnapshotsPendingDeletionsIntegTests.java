/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.RestoreInProgress;
import org.elasticsearch.cluster.SnapshotDeletionsPending;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.TriConsumer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.snapshots.ConcurrentSnapshotExecutionException;
import org.elasticsearch.snapshots.RestoreInfo;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotMissingException;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotAction;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotRequest;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

import static org.elasticsearch.index.IndexSettings.INDEX_SOFT_DELETES_SETTING;
import static org.elasticsearch.repositories.blobstore.BlobStoreRepository.READONLY_SETTING_KEY;
import static org.elasticsearch.snapshots.SearchableSnapshotsSettings.SEARCHABLE_SNAPSHOTS_DELETE_SNAPSHOT_ON_INDEX_DELETION;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class SearchableSnapshotsPendingDeletionsIntegTests extends BaseFrozenSearchableSnapshotsIntegTestCase {

    public void testSnapshotPendingDeletionCannotBeMounted() throws Exception {
        blockPendingDeletionThenExecute((repository, snapshot, index) -> {
            ConcurrentSnapshotExecutionException exception = expectThrows(
                ConcurrentSnapshotExecutionException.class,
                () -> mountSnapshot(repository, snapshot.getName(), index, Settings.EMPTY)
            );
            assertThat(exception.getMessage(), containsString("cannot restore a snapshot already marked as deleted"));
        });
    }

    public void testSnapshotPendingDeletionCannotBeRestored() throws Exception {
        blockPendingDeletionThenExecute((repository, snapshot, index) -> {
            ConcurrentSnapshotExecutionException exception = expectThrows(
                ConcurrentSnapshotExecutionException.class,
                () -> client().admin().cluster().prepareRestoreSnapshot(repository, snapshot.getName()).setWaitForCompletion(true).get()
            );
            assertThat(exception.getMessage(), containsString("cannot restore a snapshot already marked as deleted"));
        });
    }

    public void testSearchableSnapshotIsDeletedWhenRepoIsRecreated() throws Exception {
        mountIndexThenExecute((repository, snapshot, index) -> {
            try {
                final Settings.Builder repositorySettings = getRepositorySettings(repository);
                updateRepositoryReadOnly(repository, true);

                assertAcked(client().admin().indices().prepareDelete(mountedIndex(index)));
                awaitSnapshotPendingDeletion(snapshot);

                assertAcked(client().admin().cluster().prepareDeleteRepository(repository));
                awaitSnapshotPendingDeletion(snapshot);

                final String repoName;
                if (randomBoolean()) {
                    // re register the repository without verification: the snapshot
                    // pending deletion logic should try to delete the snapshot based
                    // on the repository name
                    repoName = repository;
                    createRepository(repoName, "mock", repositorySettings, false);
                } else {
                    // re register the repository under a different name: the snapshot
                    // pending deletion logic should try to delete the snapshot based
                    // on the repository uuid, that is why we force a verification here
                    repoName = "new_" + repository;
                    createRepository(repoName, "mock", repositorySettings, true);
                }
                awaitNoMoreSnapshotsDeletions();

                expectThrows(
                    SnapshotMissingException.class,
                    () -> client().admin().cluster().prepareGetSnapshots(repoName).setSnapshots(snapshot.getName()).get()
                );
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        });
    }

    public void testSearchableSnapshotIsDeletedWithOnGoingRestore() throws Exception {
        mountIndexThenExecute((repository, snapshot, index) -> {
            try {
                final String masterNode = internalCluster().getMasterName();
                blockMasterOnShardLevelSnapshotFile(repository, getRepositoryData(repository).resolveIndexId(index).getId());

                final ActionFuture<RestoreSnapshotResponse> restoreFuture = client().admin()
                    .cluster()
                    .prepareRestoreSnapshot(repository, snapshot.getName())
                    .setIndices(index)
                    .setRenamePattern("(.+)")
                    .setRenameReplacement("old_$1")
                    .setWaitForCompletion(true)
                    .execute();
                awaitClusterState(state -> state.custom(RestoreInProgress.TYPE, RestoreInProgress.EMPTY).isEmpty() == false);
                waitForBlock(masterNode, repository);

                assertAcked(client().admin().indices().prepareDelete(mountedIndex(index)));
                awaitSnapshotPendingDeletion(snapshot);
                assertFalse(restoreFuture.isDone());

                unblockNode(repository, masterNode);
                awaitNoMoreSnapshotsDeletions();

                final RestoreInfo restoreInfoResponse = restoreFuture.actionGet().getRestoreInfo();
                assertThat(restoreInfoResponse.successfulShards(), greaterThan(0));
                assertThat(restoreInfoResponse.failedShards(), equalTo(0));

                expectThrows(
                    SnapshotMissingException.class,
                    () -> client().admin().cluster().prepareGetSnapshots(repository).setSnapshots(snapshot.getName()).get()
                );
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        });
    }

    public void testSearchableSnapshotIsDeletedWithOnGoingClone() throws Exception {
        mountIndexThenExecute((repository, snapshot, index) -> {
            try {
                final String masterNode = internalCluster().getMasterName();
                blockMasterOnShardLevelSnapshotFile(repository, getRepositoryData(repository).resolveIndexId(index).getId());

                final String cloneTarget = "target-snapshot";
                final ActionFuture<AcknowledgedResponse> cloneFuture = clusterAdmin().prepareCloneSnapshot(
                    repository,
                    snapshot.getName(),
                    cloneTarget
                ).setIndices(index).execute();
                awaitNumberOfSnapshotsInProgress(1);
                waitForBlock(masterNode, repository);

                assertAcked(client().admin().indices().prepareDelete(mountedIndex(index)));
                awaitSnapshotPendingDeletion(snapshot);
                assertFalse(cloneFuture.isDone());

                unblockNode(repository, masterNode);
                awaitNoMoreSnapshotsDeletions();
                assertAcked(cloneFuture.get());

                expectThrows(
                    SnapshotMissingException.class,
                    () -> client().admin().cluster().prepareGetSnapshots(repository).setSnapshots(snapshot.getName()).get()
                );
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        });
    }

    public void testSearchableSnapshotIsDeletedWithOnGoingDeletion() throws Exception {
        mountIndexThenExecute((repository, snapshot, index) -> {
            try {
                blockMasterOnWriteIndexFile(repository);
                final String masterNode = internalCluster().getMasterName();

                final ActionFuture<AcknowledgedResponse> deleteFuture = startDeleteSnapshot(repository, snapshot.getName());
                waitForBlock(masterNode, repository);

                assertAcked(client().admin().indices().prepareDelete(mountedIndex(index)));
                awaitSnapshotPendingDeletion(snapshot);
                assertFalse(deleteFuture.isDone());

                unblockNode(repository, masterNode);
                awaitNoMoreSnapshotsDeletions();
                assertAcked(deleteFuture.get());

                expectThrows(
                    SnapshotMissingException.class,
                    () -> client().admin().cluster().prepareGetSnapshots(repository).setSnapshots(snapshot.getName()).get()
                );
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        });
    }

    public void testSearchableSnapshotsDeletionsWithConcurrentDeletes() throws Exception {
        final String repository = "repository";
        final Settings.Builder repositorySettings = randomRepositorySettings();
        createRepository(repository, FsRepository.TYPE, repositorySettings);

        final List<String> indices = new ArrayList<>();
        for (int i = 0; i < randomIntBetween(1, 10); i++) {
            final String index = "index-" + i;
            assertAcked(
                prepareCreate(
                    index,
                    Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .put(INDEX_SOFT_DELETES_SETTING.getKey(), true)
                )
            );
            indices.add(index);
        }

        final int nbSnapshots = randomIntBetween(1, 10);
        final CountDownLatch snapshotLatch = new CountDownLatch(nbSnapshots);
        final List<SnapshotInfo> snapshots = new CopyOnWriteArrayList<>();
        for (int i = 0; i < nbSnapshots; i++) {
            final String snapshot = "snapshot-" + i;
            client().admin()
                .cluster()
                .prepareCreateSnapshot(repository, snapshot)
                // must contain 1 index to be used with SEARCHABLE_SNAPSHOTS_DELETE_SNAPSHOT_ON_INDEX_DELETION
                .setIndices(randomFrom(indices))
                .setWaitForCompletion(true)
                .execute(new ActionListener<>() {
                    @Override
                    public void onResponse(CreateSnapshotResponse response) {
                        assertThat(response.getSnapshotInfo().state(), equalTo(SnapshotState.SUCCESS));
                        snapshots.add(response.getSnapshotInfo());
                        snapshotLatch.countDown();
                    }

                    @Override
                    public void onFailure(Exception e) {
                        final AssertionError error = new AssertionError("error during snapshot", e);
                        logger.error("test failed", error);
                        snapshotLatch.countDown();
                        throw error;
                    }
                });
        }
        snapshotLatch.await();

        final int nbMounts = randomIntBetween(1, 10);
        final CountDownLatch mountLatch = new CountDownLatch(nbMounts);
        final Map<String, SnapshotId> mounts = ConcurrentCollections.newConcurrentMap();

        for (int i = 0; i < nbMounts; i++) {
            final String mount = "mount-" + i;

            final SnapshotInfo snapshotInfo = randomFrom(snapshots);
            client().execute(
                MountSearchableSnapshotAction.INSTANCE,
                new MountSearchableSnapshotRequest(
                    mount,
                    repository,
                    snapshotInfo.snapshot().getSnapshotId().getName(),
                    randomFrom(snapshotInfo.indices()),
                    Settings.builder().put(SEARCHABLE_SNAPSHOTS_DELETE_SNAPSHOT_ON_INDEX_DELETION, true).build(),
                    Strings.EMPTY_ARRAY,
                    true,
                    randomFrom(MountSearchableSnapshotRequest.Storage.values())
                ),
                new ActionListener<>() {
                    @Override
                    public void onResponse(RestoreSnapshotResponse response) {
                        assertThat(response.getRestoreInfo().successfulShards(), greaterThan(0));
                        assertThat(response.getRestoreInfo().failedShards(), equalTo(0));
                        mounts.put(mount, snapshotInfo.snapshotId());
                        mountLatch.countDown();
                    }

                    @Override
                    public void onFailure(Exception e) {
                        final AssertionError error = new AssertionError("error during mount", e);
                        logger.error("test failed", error);
                        mountLatch.countDown();
                        throw error;
                    }
                }
            );
        }
        mountLatch.await();

        if (randomBoolean()) {
            // we can't delete a repository that is used by searchable snapshot indices but we can update it
            assertAcked(
                clusterAdmin().preparePutRepository(repository)
                    .setType(FsRepository.TYPE)
                    .setVerify(randomBoolean())
                    .setSettings(randomBoolean() ? repositorySettings : repositorySettings.put("dummy", randomInt()))
            );
        }

        final CyclicBarrier startThreads = new CyclicBarrier(2);
        final Thread deleteAllSnapshotsThread = new Thread(() -> {
            try {
                startThreads.await();
                final CountDownLatch latch = new CountDownLatch(snapshots.size());
                for (SnapshotInfo snapshot : snapshots) {
                    client().admin()
                        .cluster()
                        .prepareDeleteSnapshot(repository, snapshot.snapshotId().getName())
                        .execute(ActionListener.wrap(latch::countDown));

                }
                latch.await();
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        });
        deleteAllSnapshotsThread.start();

        final Thread deleteAllMountedIndicesThread = new Thread(() -> {
            try {
                ensureGreen("mount-*");
                startThreads.await();
                final Set<String> mountedIndices = new HashSet<>(mounts.keySet());
                do {
                    List<String> deletions = randomSubsetOf(Math.max(1, mountedIndices.size() - 1), mountedIndices);
                    assertAcked(client().admin().indices().prepareDelete(deletions.toArray(String[]::new)));
                    deletions.forEach(mountedIndices::remove);
                } while (mountedIndices.isEmpty() == false);
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        });
        deleteAllMountedIndicesThread.start();

        deleteAllMountedIndicesThread.join();
        deleteAllSnapshotsThread.join();
        awaitNoMoreSnapshotsDeletions();

        final GetSnapshotsResponse getSnapshotsResponse = client().admin().cluster().prepareGetSnapshots(repository).get();
        assertTrue(getSnapshotsResponse.getSnapshots().stream().noneMatch(snapshotInfo -> mounts.containsValue(snapshotInfo.snapshotId())));
    }

    private void mountIndexThenExecute(final TriConsumer<String, SnapshotId, String> test) throws Exception {
        final String suffix = randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
        final String repository = "repository-" + suffix;
        final Settings.Builder repositorySettings = randomRepositorySettings();
        createRepository(repository, "mock", repositorySettings);

        final String index = "index-" + suffix;
        assertAcked(prepareCreate(index, Settings.builder().put(INDEX_SOFT_DELETES_SETTING.getKey(), true)));
        ensureGreen(index);
        populateIndex(index, scaledRandomIntBetween(10, 5_000));
        refresh(index);

        final TotalHits totalHits = internalCluster().client().prepareSearch(index).setTrackTotalHits(true).get().getHits().getTotalHits();
        final SnapshotId snapshotId = createSnapshot(repository, "snapshot-" + suffix, List.of(index)).snapshotId();

        final String restored = mountedIndex(index);
        mountSnapshot(
            repository,
            snapshotId.getName(),
            index,
            restored,
            Settings.builder().put(SEARCHABLE_SNAPSHOTS_DELETE_SNAPSHOT_ON_INDEX_DELETION, true).build()
        );
        assertHitCount(client().prepareSearch(restored).setTrackTotalHits(true).get(), totalHits.value);
        test.apply(repository, snapshotId, index);
    }

    private void blockPendingDeletionThenExecute(final TriConsumer<String, SnapshotId, String> test) throws Exception {
        mountIndexThenExecute((repository, snapshot, index) -> {
            try {
                updateRepositoryReadOnly(repository, true);

                assertAcked(client().admin().indices().prepareDelete(mountedIndex(index)));
                awaitSnapshotPendingDeletion(snapshot);
                test.apply(repository, snapshot, index);

                updateRepositoryReadOnly(repository, false);
                awaitNoMoreSnapshotsDeletions();

                expectThrows(
                    SnapshotMissingException.class,
                    () -> client().admin().cluster().prepareGetSnapshots(repository).setSnapshots(snapshot.getName()).get()
                );
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        });
    }

    protected void awaitSnapshotPendingDeletion(final SnapshotId snapshotId) throws Exception {
        logger.info("--> wait for snapshot [{}] to be show up as pending deletion in the cluster state", snapshotId);
        awaitClusterState(state -> state.custom(SnapshotDeletionsPending.TYPE, SnapshotDeletionsPending.EMPTY).contains(snapshotId));
    }

    private void updateRepositoryReadOnly(String repository, boolean readOnly) {
        logger.info("--> updating repository [{}] with read-only [{}]", repository, readOnly);
        final Settings.Builder repositorySettings = getRepositorySettings(repository);
        repositorySettings.put(READONLY_SETTING_KEY, readOnly);
        assertAcked(
            clusterAdmin().preparePutRepository(repository)
                // randomization here is important to have registered repository with _na_ uuid
                .setVerify(randomBoolean())
                .setType("mock")
                .setSettings(repositorySettings)
        );
    }

    private Settings.Builder getRepositorySettings(String repository) {
        return Settings.builder().put(client().admin().cluster().prepareGetRepositories(repository).get().repositories().get(0).settings());
    }

    private String mountedIndex(String index) {
        return "restored-" + index;
    }
}
