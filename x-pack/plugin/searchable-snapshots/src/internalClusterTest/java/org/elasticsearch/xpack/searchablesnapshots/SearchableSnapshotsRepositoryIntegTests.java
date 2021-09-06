/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.RestoreInProgress;
import org.elasticsearch.cluster.SnapshotDeletionsInProgress;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.snapshots.ConcurrentSnapshotExecutionException;
import org.elasticsearch.snapshots.RestoreInfo;
import org.elasticsearch.snapshots.SnapshotException;
import org.elasticsearch.snapshots.SnapshotMissingException;
import org.elasticsearch.snapshots.SnapshotRestoreException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING;
import static org.elasticsearch.index.IndexSettings.INDEX_SOFT_DELETES_SETTING;
import static org.elasticsearch.snapshots.SearchableSnapshotsSettings.SEARCHABLE_SNAPSHOTS_DELETE_SNAPSHOT_ON_INDEX_DELETION;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotRequest.Storage;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

public class SearchableSnapshotsRepositoryIntegTests extends BaseFrozenSearchableSnapshotsIntegTestCase {

    public void testRepositoryUsedBySearchableSnapshotCanBeUpdatedButNotUnregistered() throws Exception {
        final String repositoryName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final Settings.Builder repositorySettings = randomRepositorySettings();
        createRepository(repositoryName, FsRepository.TYPE, repositorySettings);

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createAndPopulateIndex(
            indexName,
            Settings.builder().put(INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1).put(INDEX_SOFT_DELETES_SETTING.getKey(), true)
        );

        final TotalHits totalHits = internalCluster().client()
            .prepareSearch(indexName)
            .setTrackTotalHits(true)
            .get()
            .getHits()
            .getTotalHits();

        final String snapshotName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createSnapshot(repositoryName, snapshotName, List.of(indexName));
        assertAcked(client().admin().indices().prepareDelete(indexName));

        final int nbMountedIndices = 1;
        randomIntBetween(1, 5);
        final String[] mountedIndices = new String[nbMountedIndices];

        for (int i = 0; i < nbMountedIndices; i++) {
            Storage storage = randomFrom(Storage.values());
            String restoredIndexName = (storage == Storage.FULL_COPY ? "fully-mounted-" : "partially-mounted-") + indexName + '-' + i;
            mountSnapshot(repositoryName, snapshotName, indexName, restoredIndexName, Settings.EMPTY, storage);
            assertHitCount(client().prepareSearch(restoredIndexName).setTrackTotalHits(true).get(), totalHits.value);
            mountedIndices[i] = restoredIndexName;
        }

        assertAcked(
            clusterAdmin().preparePutRepository(repositoryName)
                .setType(FsRepository.TYPE)
                .setSettings(
                    Settings.builder()
                        .put(repositorySettings.build())
                        .put(FsRepository.REPOSITORIES_CHUNK_SIZE_SETTING.getKey(), ByteSizeValue.ofMb(1L))
                        .build()
                )
        );

        final String updatedRepositoryName;
        if (randomBoolean()) {
            final String snapshotWithMountedIndices = snapshotName + "-with-mounted-indices";
            createSnapshot(repositoryName, snapshotWithMountedIndices, Arrays.asList(mountedIndices));
            assertAcked(client().admin().indices().prepareDelete(mountedIndices));
            assertAcked(clusterAdmin().prepareDeleteRepository(repositoryName));

            updatedRepositoryName = repositoryName + "-with-mounted-indices";
            createRepository(updatedRepositoryName, FsRepository.TYPE, repositorySettings, randomBoolean());

            final RestoreSnapshotResponse restoreResponse = clusterAdmin().prepareRestoreSnapshot(
                updatedRepositoryName,
                snapshotWithMountedIndices
            ).setWaitForCompletion(true).setIndices(mountedIndices).get();
            assertEquals(restoreResponse.getRestoreInfo().totalShards(), restoreResponse.getRestoreInfo().successfulShards());
        } else {
            updatedRepositoryName = repositoryName;
        }

        for (int i = 0; i < nbMountedIndices; i++) {
            IllegalStateException exception = expectThrows(
                IllegalStateException.class,
                () -> clusterAdmin().prepareDeleteRepository(updatedRepositoryName).get()
            );
            assertThat(
                exception.getMessage(),
                containsString(
                    "trying to modify or unregister repository ["
                        + updatedRepositoryName
                        + "] that is currently used (found "
                        + (nbMountedIndices - i)
                        + " searchable snapshots indices that use the repository:"
                )
            );
            assertAcked(client().admin().indices().prepareDelete(mountedIndices[i]));
        }

        assertAcked(clusterAdmin().prepareDeleteRepository(updatedRepositoryName));
    }

    public void testMountIndexWithDeletionOfSnapshotFailsIfNotSingleIndexSnapshot() throws Exception {
        final String repository = "repository-" + getTestName().toLowerCase(Locale.ROOT);
        createRepository(repository, FsRepository.TYPE, randomRepositorySettings());

        final int nbIndices = randomIntBetween(2, 5);
        for (int i = 0; i < nbIndices; i++) {
            createAndPopulateIndex(
                "index-" + i,
                Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).put(INDEX_SOFT_DELETES_SETTING.getKey(), true)
            );
        }

        final String snapshot = "snapshot";
        createFullSnapshot(repository, snapshot);
        assertAcked(client().admin().indices().prepareDelete("index-*"));

        final String index = "index-" + randomInt(nbIndices - 1);
        final String mountedIndex = "mounted-" + index;

        final SnapshotRestoreException exception = expectThrows(
            SnapshotRestoreException.class,
            () -> mountSnapshot(repository, snapshot, index, mountedIndex, deleteSnapshotIndexSettings(true), randomFrom(Storage.values()))
        );
        assertThat(
            exception.getMessage(),
            allOf(
                containsString("cannot mount snapshot [" + repository + '/'),
                containsString(snapshot + "] as index [" + mountedIndex + "] with the deletion of snapshot on index removal enabled"),
                containsString("[index.store.snapshot.delete_searchable_snapshot: true]; "),
                containsString("snapshot contains [" + nbIndices + "] indices instead of 1.")
            )
        );
        awaitNoMoreSnapshotsDeletions();
    }

    public void testMountIndexWithDifferentDeletionOfSnapshot() throws Exception {
        final String repository = "repository-" + getTestName().toLowerCase(Locale.ROOT);
        createRepository(repository, FsRepository.TYPE, randomRepositorySettings());

        final String index = "index";
        createAndPopulateIndex(index, Settings.builder().put(INDEX_SOFT_DELETES_SETTING.getKey(), true));

        final TotalHits totalHits = internalCluster().client().prepareSearch(index).setTrackTotalHits(true).get().getHits().getTotalHits();

        final String snapshot = "snapshot";
        createSnapshot(repository, snapshot, List.of(index));
        assertAcked(client().admin().indices().prepareDelete(index));

        final boolean deleteSnapshot = randomBoolean();
        final String mounted = "mounted-with-setting-" + deleteSnapshot;
        final Settings indexSettings = deleteSnapshotIndexSettingsOrNull(deleteSnapshot);

        logger.info("--> mounting index [{}] with index settings [{}]", mounted, indexSettings);
        mountSnapshot(repository, snapshot, index, mounted, indexSettings, randomFrom(Storage.values()));
        assertThat(
            getDeleteSnapshotIndexSetting(mounted),
            indexSettings.hasValue(SEARCHABLE_SNAPSHOTS_DELETE_SNAPSHOT_ON_INDEX_DELETION)
                ? equalTo(Boolean.toString(deleteSnapshot))
                : nullValue()
        );
        assertHitCount(client().prepareSearch(mounted).setTrackTotalHits(true).get(), totalHits.value);

        final String mountedAgain = randomValueOtherThan(mounted, () -> randomAlphaOfLength(10).toLowerCase(Locale.ROOT));
        final SnapshotRestoreException exception = expectThrows(
            SnapshotRestoreException.class,
            () -> mountSnapshot(repository, snapshot, index, mountedAgain, deleteSnapshotIndexSettings(deleteSnapshot == false))
        );
        assertThat(
            exception.getMessage(),
            allOf(
                containsString("cannot mount snapshot [" + repository + '/'),
                containsString(':' + snapshot + "] as index [" + mountedAgain + "] with "),
                containsString("[index.store.snapshot.delete_searchable_snapshot: " + (deleteSnapshot == false) + "]; another "),
                containsString("index [" + mounted + '/'),
                containsString("is mounted with [index.store.snapshot.delete_searchable_snapshot: " + deleteSnapshot + "].")
            )
        );

        mountSnapshot(repository, snapshot, index, mountedAgain, deleteSnapshotIndexSettings(deleteSnapshot));
        assertThat(
            getDeleteSnapshotIndexSetting(mounted),
            indexSettings.hasValue(SEARCHABLE_SNAPSHOTS_DELETE_SNAPSHOT_ON_INDEX_DELETION)
                ? equalTo(Boolean.toString(deleteSnapshot))
                : nullValue()
        );
        assertHitCount(client().prepareSearch(mountedAgain).setTrackTotalHits(true).get(), totalHits.value);

        assertAcked(client().admin().indices().prepareDelete(mountedAgain));
        assertAcked(client().admin().indices().prepareDelete(mounted));
        awaitNoMoreSnapshotsDeletions();
    }

    public void testDeletionOfSnapshotSettingCannotBeUpdated() throws Exception {
        final String repository = "repository-" + getTestName().toLowerCase(Locale.ROOT);
        createRepository(repository, FsRepository.TYPE, randomRepositorySettings());

        final String index = "index";
        createAndPopulateIndex(index, Settings.builder().put(INDEX_SOFT_DELETES_SETTING.getKey(), true));

        final TotalHits totalHits = internalCluster().client().prepareSearch(index).setTrackTotalHits(true).get().getHits().getTotalHits();

        final String snapshot = "snapshot";
        createSnapshot(repository, snapshot, List.of(index));
        assertAcked(client().admin().indices().prepareDelete(index));

        final String mounted = "mounted-" + index;
        final boolean deleteSnapshot = randomBoolean();
        final Settings indexSettings = deleteSnapshotIndexSettingsOrNull(deleteSnapshot);

        mountSnapshot(repository, snapshot, index, mounted, indexSettings, randomFrom(Storage.values()));
        assertThat(
            getDeleteSnapshotIndexSetting(mounted),
            indexSettings.hasValue(SEARCHABLE_SNAPSHOTS_DELETE_SNAPSHOT_ON_INDEX_DELETION)
                ? equalTo(Boolean.toString(deleteSnapshot))
                : nullValue()
        );
        assertHitCount(client().prepareSearch(mounted).setTrackTotalHits(true).get(), totalHits.value);

        if (randomBoolean()) {
            assertAcked(client().admin().indices().prepareClose(mounted));
        }

        final IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> client().admin()
                .indices()
                .prepareUpdateSettings(mounted)
                .setSettings(deleteSnapshotIndexSettings(deleteSnapshot == false))
                .get()
        );
        assertThat(
            exception.getMessage(),
            containsString("can not update private setting [index.store.snapshot.delete_searchable_snapshot]; ")
        );

        assertAcked(client().admin().indices().prepareDelete(mounted));
        awaitNoMoreSnapshotsDeletions();
    }

    public void testRestoreSearchableSnapshotIndexConflicts() throws Exception {
        final String repository = "repository-" + getTestName().toLowerCase(Locale.ROOT);
        createRepository(repository, FsRepository.TYPE, randomRepositorySettings());

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createAndPopulateIndex(indexName, Settings.builder().put(INDEX_SOFT_DELETES_SETTING.getKey(), true));

        final String snapshotOfIndex = "snapshot-of-index";
        createSnapshot(repository, snapshotOfIndex, List.of(indexName));
        assertAcked(client().admin().indices().prepareDelete(indexName));

        final String mountedIndex = "mounted-index";
        final Settings indexSettings = deleteSnapshotIndexSettingsOrNull(false);
        logger.info("--> mounting snapshot of index [{}] as [{}] with index settings [{}]", indexName, mountedIndex, indexSettings);
        mountSnapshot(repository, snapshotOfIndex, indexName, mountedIndex, indexSettings, randomFrom(Storage.values()));
        assertThat(
            getDeleteSnapshotIndexSetting(mountedIndex),
            indexSettings.hasValue(SEARCHABLE_SNAPSHOTS_DELETE_SNAPSHOT_ON_INDEX_DELETION) ? equalTo("false") : nullValue()
        );

        final String snapshotOfMountedIndex = "snapshot-of-mounted-index";
        createSnapshot(repository, snapshotOfMountedIndex, List.of(mountedIndex));
        assertAcked(client().admin().indices().prepareDelete(mountedIndex));

        final String mountedIndexAgain = "mounted-index-again";
        final Settings indexSettingsAgain = deleteSnapshotIndexSettings(true);
        logger.info("--> mounting snapshot of index [{}] again as [{}] with index settings [{}]", indexName, mountedIndex, indexSettings);
        mountSnapshot(repository, snapshotOfIndex, indexName, mountedIndexAgain, indexSettingsAgain, randomFrom(Storage.values()));
        assertThat(
            getDeleteSnapshotIndexSetting(mountedIndexAgain),
            indexSettingsAgain.hasValue(SEARCHABLE_SNAPSHOTS_DELETE_SNAPSHOT_ON_INDEX_DELETION) ? equalTo("true") : nullValue()
        );

        logger.info("--> restoring snapshot of searchable snapshot index [{}] should be conflicting", mountedIndex);
        final SnapshotRestoreException exception = expectThrows(
            SnapshotRestoreException.class,
            () -> client().admin()
                .cluster()
                .prepareRestoreSnapshot(repository, snapshotOfMountedIndex)
                .setIndices(mountedIndex)
                .setWaitForCompletion(true)
                .get()
        );
        assertThat(
            exception.getMessage(),
            allOf(
                containsString("cannot mount snapshot [" + repository + '/'),
                containsString(':' + snapshotOfMountedIndex + "] as index [" + mountedIndex + "] with "),
                containsString("[index.store.snapshot.delete_searchable_snapshot: false]; another "),
                containsString("index [" + mountedIndexAgain + '/'),
                containsString("is mounted with [index.store.snapshot.delete_searchable_snapshot: true].")
            )
        );
        assertAcked(client().admin().indices().prepareDelete("mounted-*"));
        awaitNoMoreSnapshotsDeletions();
    }

    public void testRestoreSearchableSnapshotIndexWithDifferentSettingsConflicts() throws Exception {
        final String repository = "repository-" + getTestName().toLowerCase(Locale.ROOT);
        createRepository(repository, FsRepository.TYPE, randomRepositorySettings());

        final int nbIndices = randomIntBetween(1, 3);
        for (int i = 0; i < nbIndices; i++) {
            createAndPopulateIndex("index-" + i, Settings.builder().put(INDEX_SOFT_DELETES_SETTING.getKey(), true));
        }

        final String snapshotOfIndices = "snapshot-of-indices";
        createFullSnapshot(repository, snapshotOfIndices);

        final int nbMountedIndices = randomIntBetween(1, 3);
        final Set<String> mountedIndices = new HashSet<>(nbMountedIndices);

        final boolean deleteSnapshot = nbIndices == 1 && randomBoolean();
        final Settings indexSettings = deleteSnapshotIndexSettingsOrNull(deleteSnapshot);

        for (int i = 0; i < nbMountedIndices; i++) {
            final String index = "index-" + randomInt(nbIndices - 1);
            final String mountedIndex = "mounted-" + i;
            logger.info("--> mounting snapshot of index [{}] as [{}] with index settings [{}]", index, mountedIndex, indexSettings);
            mountSnapshot(repository, snapshotOfIndices, index, mountedIndex, indexSettings, randomFrom(Storage.values()));
            assertThat(
                getDeleteSnapshotIndexSetting(mountedIndex),
                indexSettings.hasValue(SEARCHABLE_SNAPSHOTS_DELETE_SNAPSHOT_ON_INDEX_DELETION)
                    ? equalTo(Boolean.toString(deleteSnapshot))
                    : nullValue()
            );
            if (randomBoolean()) {
                assertAcked(client().admin().indices().prepareClose(mountedIndex));
            }
            mountedIndices.add(mountedIndex);
        }

        final String snapshotOfMountedIndices = "snapshot-of-mounted-indices";
        createSnapshot(repository, snapshotOfMountedIndices, List.of("mounted-*"));

        List<String> restorables = randomBoolean()
            ? List.of("mounted-*")
            : randomSubsetOf(randomIntBetween(1, nbMountedIndices), mountedIndices);
        final SnapshotRestoreException exception = expectThrows(
            SnapshotRestoreException.class,
            () -> client().admin()
                .cluster()
                .prepareRestoreSnapshot(repository, snapshotOfMountedIndices)
                .setIndices(restorables.toArray(String[]::new))
                .setIndexSettings(deleteSnapshotIndexSettings(deleteSnapshot == false))
                .setRenameReplacement("restored-with-different-setting-$1")
                .setRenamePattern("(.+)")
                .setWaitForCompletion(true)
                .get()
        );

        assertThat(
            exception.getMessage(),
            containsString(
                "cannot change value of [index.store.snapshot.delete_searchable_snapshot] when restoring searchable snapshot ["
                    + repository
                    + ':'
                    + snapshotOfMountedIndices
                    + "] as index [mounted-"
            )
        );

        final RestoreSnapshotResponse restoreResponse = client().admin()
            .cluster()
            .prepareRestoreSnapshot(repository, snapshotOfMountedIndices)
            .setIndices(restorables.toArray(String[]::new))
            .setIndexSettings(indexSettings)
            .setRenameReplacement("restored-with-same-setting-$1")
            .setRenamePattern("(.+)")
            .setWaitForCompletion(true)
            .get();
        assertThat(restoreResponse.getRestoreInfo().totalShards(), greaterThan(0));
        assertThat(restoreResponse.getRestoreInfo().failedShards(), equalTo(0));

        assertAcked(client().admin().indices().prepareDelete("mounted-*"));
        assertAcked(client().admin().indices().prepareDelete("restored-with-same-setting-*"));
        awaitNoMoreSnapshotsDeletions();
    }

    public void testSnapshotMarkedAsToDeleteCannotBeMounted() throws Exception {
        final String suffix = getTestName().toLowerCase(Locale.ROOT);
        final String repository = "repository-" + suffix;
        createRepository(repository, FsRepository.TYPE, randomRepositorySettings());

        final String index = "index-" + suffix;
        createAndPopulateIndex(index, Settings.builder().put(INDEX_SOFT_DELETES_SETTING.getKey(), true));

        final TotalHits totalHits = internalCluster().client().prepareSearch(index).setTrackTotalHits(true).get().getHits().getTotalHits();

        final String snapshot = "snapshot-" + suffix;
        createSnapshot(repository, snapshot, List.of(index));
        assertAcked(client().admin().indices().prepareDelete(index));

        final String mounted = mountSnapshot(repository, snapshot, index, deleteSnapshotIndexSettings(true));
        assertHitCount(client().prepareSearch(mounted).setTrackTotalHits(true).get(), totalHits.value);
        assertAcked(client().admin().indices().prepareDelete(mounted));

        final SnapshotException exception = expectThrows(
            SnapshotException.class,
            () -> mountSnapshot(repository, snapshot, index, deleteSnapshotIndexSettings(true))
        );
        assertThat(exception, anyOf(instanceOf(SnapshotMissingException.class), instanceOf(ConcurrentSnapshotExecutionException.class)));
        assertThat(
            exception.getMessage(),
            anyOf(
                containsString("cannot restore a snapshot while a snapshot deletion is in-progress"),
                containsString("cannot restore a snapshot already marked as deleted"),
                containsString(snapshot + "] is missing")
            )
        );
        awaitNoMoreSnapshotsDeletions();
    }

    public void testSnapshotMarkedAsToDeleteCannotBeCloned() throws Exception {
        final String suffix = getTestName().toLowerCase(Locale.ROOT);
        final String repository = "repository-" + suffix;
        createRepository(repository, FsRepository.TYPE, randomRepositorySettings());

        final String index = "index-" + suffix;
        createAndPopulateIndex(index, Settings.builder().put(INDEX_SOFT_DELETES_SETTING.getKey(), true));

        final TotalHits totalHits = internalCluster().client().prepareSearch(index).setTrackTotalHits(true).get().getHits().getTotalHits();

        final String snapshot = "snapshot-" + suffix;
        createSnapshot(repository, snapshot, List.of(index));
        assertAcked(client().admin().indices().prepareDelete(index));

        final String mounted = mountSnapshot(repository, snapshot, index, deleteSnapshotIndexSettings(true));
        assertHitCount(client().prepareSearch(mounted).setTrackTotalHits(true).get(), totalHits.value);
        assertAcked(client().admin().indices().prepareDelete(mounted));

        final SnapshotException exception = expectThrows(
            SnapshotException.class,
            () -> client().admin().cluster().prepareCloneSnapshot(repository, snapshot, "clone-" + snapshot).setIndices(index).get()
        );
        assertThat(exception, anyOf(instanceOf(SnapshotMissingException.class), instanceOf(ConcurrentSnapshotExecutionException.class)));
        assertThat(
            exception.getMessage(),
            anyOf(
                containsString("cannot clone a snapshot that is marked as deleted"),
                containsString("cannot clone from snapshot that is being deleted"),
                containsString(snapshot + "] is missing")
            )
        );
        awaitNoMoreSnapshotsDeletions();
    }

    public void testSnapshotMarkedAsToDeleteCannotBeRestored() throws Exception {
        final String suffix = getTestName().toLowerCase(Locale.ROOT);
        final String repository = "repository-" + suffix;
        createRepository(repository, FsRepository.TYPE, randomRepositorySettings());

        final String index = "index-" + suffix;
        createAndPopulateIndex(index, Settings.builder().put(INDEX_SOFT_DELETES_SETTING.getKey(), true));

        final TotalHits totalHits = internalCluster().client().prepareSearch(index).setTrackTotalHits(true).get().getHits().getTotalHits();

        final String snapshot = "snapshot-" + suffix;
        createSnapshot(repository, snapshot, List.of(index));
        assertAcked(client().admin().indices().prepareDelete(index));

        final String mounted = mountSnapshot(repository, snapshot, index, deleteSnapshotIndexSettings(true));
        assertHitCount(client().prepareSearch(mounted).setTrackTotalHits(true).get(), totalHits.value);
        assertAcked(client().admin().indices().prepareDelete(mounted));

        final SnapshotException exception = expectThrows(
            SnapshotException.class,
            () -> client().admin().cluster().prepareRestoreSnapshot(repository, snapshot).setIndices(index).setWaitForCompletion(true).get()
        );
        assertThat(exception, anyOf(instanceOf(SnapshotMissingException.class), instanceOf(ConcurrentSnapshotExecutionException.class)));
        assertThat(
            exception.getMessage(),
            anyOf(
                containsString("cannot restore a snapshot while a snapshot deletion is in-progress"),
                containsString("cannot restore a snapshot already marked as deleted"),
                containsString(snapshot + "] is missing")
            )
        );
        awaitNoMoreSnapshotsDeletions();
    }

    public void testSearchableSnapshotIsDeletedAfterIndexIsDeleted() throws Exception {
        final String suffix = getTestName().toLowerCase(Locale.ROOT);
        final String repository = "repository-" + suffix;
        createRepository(repository, FsRepository.TYPE, randomRepositorySettings());

        final String index = "index-" + suffix;
        createAndPopulateIndex(index, Settings.builder().put(INDEX_SOFT_DELETES_SETTING.getKey(), true));

        final TotalHits totalHits = internalCluster().client().prepareSearch(index).setTrackTotalHits(true).get().getHits().getTotalHits();

        final String snapshot = "snapshot-" + suffix;
        createSnapshot(repository, snapshot, List.of(index));
        assertAcked(client().admin().indices().prepareDelete(index));

        final String mounted = "mounted-" + suffix;
        mountSnapshot(repository, snapshot, index, mounted, deleteSnapshotIndexSettings(true), randomFrom(Storage.values()));
        assertHitCount(client().prepareSearch(mounted).setTrackTotalHits(true).get(), totalHits.value);

        assertAcked(client().admin().indices().prepareDelete(mounted));
        awaitNoMoreSnapshotsDeletions();

        expectThrows(SnapshotMissingException.class, () -> clusterAdmin().prepareDeleteSnapshot(repository, snapshot).get());
    }

    public void testSearchableSnapshotsAreDeletedAfterMountedIndicesAreDeleted() throws Exception {
        final String repository = "repository-" + randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createRepository(logger, repository, "mock");

        final List<String> snapshots = new ArrayList<>();
        final int nbSnapshots = randomIntBetween(2, 10);
        for (int s = 0; s < nbSnapshots; s++) {
            createAndPopulateIndex("index", Settings.builder().put(INDEX_SOFT_DELETES_SETTING.getKey(), true));
            final String snapshot = "snapshot-" + s;
            createSnapshot(repository, snapshot, List.of("index"));
            assertAcked(client().admin().indices().prepareDelete("index"));
            snapshots.add(snapshot);
        }

        final int nbIndices = randomIntBetween(2, 10);
        final Map<String, String> mounts = new HashMap<>(nbIndices);
        for (int i = 0; i < nbIndices; i++) {
            final String mounted = "mounted-" + i;
            final String snapshot = randomFrom(snapshots);
            logger.info("--> mounting snapshot [{}] as index [{}]", snapshot, mounted);
            mountSnapshot(repository, snapshot, "index", mounted, deleteSnapshotIndexSettings(true), randomFrom(Storage.values()));
            mounts.put(mounted, snapshot);
        }

        awaitNoMoreRunningOperations();

        final List<ActionFuture<?>> futures = new ArrayList<>();
        blockAllDataNodes(repository);

        for (int i = 0; i < nbSnapshots; i++) {
            final ActionFuture<?> future;
            switch (randomInt(2)) {
                case 0:
                    future = client().admin()
                        .cluster()
                        .prepareRestoreSnapshot(repository, "snapshot-" + i)
                        .setIndices("index")
                        .setRenamePattern("(.+)")
                        .setRenameReplacement("$1-restored-" + i)
                        .setWaitForCompletion(true)
                        .execute();
                    break;
                case 1:
                    future = client().admin()
                        .cluster()
                        .prepareCloneSnapshot(repository, "snapshot-" + i, "clone-" + i)
                        .setIndices("index")
                        .execute();
                    break;
                case 2:
                    future = client().admin().cluster().prepareDeleteSnapshot(repository, "snapshot-" + i).execute();
                    break;
                default:
                    throw new AssertionError();
            }
            futures.add(future);
        }

        awaitClusterState(
            state -> state.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY).entries().size() > 0
                || state.custom(RestoreInProgress.TYPE, RestoreInProgress.EMPTY).isEmpty() == false
                || state.custom(SnapshotDeletionsInProgress.TYPE, SnapshotDeletionsInProgress.EMPTY).hasDeletionsInProgress()
        );

        final List<String> remainingIndicesToDelete = new ArrayList<>(mounts.keySet());
        while (remainingIndicesToDelete.isEmpty() == false) {
            final List<String> toDelete = randomValueOtherThanMany(List::isEmpty, () -> randomSubsetOf(remainingIndicesToDelete));
            logger.info("--> deleting mounted indices [{}]", toDelete);
            futures.add(client().admin().indices().prepareDelete(toDelete.toArray(String[]::new)).execute());
            toDelete.forEach(remainingIndicesToDelete::remove);
        }

        unblockAllDataNodes(repository);

        assertBusy(() -> {
            for (ActionFuture<?> future : futures) {
                assertTrue(future.isDone());
                try {
                    Object response = future.get();
                    if (response instanceof AcknowledgedResponse) {
                        assertAcked((AcknowledgedResponse) response);

                    } else if (response instanceof RestoreSnapshotResponse) {
                        final RestoreSnapshotResponse restoreResponse = ((RestoreSnapshotResponse) response);
                        assertThat(restoreResponse.getRestoreInfo().successfulShards(), greaterThanOrEqualTo(1));
                        assertThat(restoreResponse.getRestoreInfo().failedShards(), equalTo(0));

                    } else {
                        throw new AssertionError("Unknown response type: " + response);
                    }
                } catch (ExecutionException e) {
                    Throwable cause = ExceptionsHelper.unwrap(e, SnapshotException.class);
                    if (cause == null) {
                        cause = ExceptionsHelper.unwrapCause(e);
                    }
                    assertThat(
                        cause,
                        anyOf(instanceOf(ConcurrentSnapshotExecutionException.class), instanceOf(SnapshotMissingException.class))
                    );
                    assertThat(
                        cause.getMessage(),
                        anyOf(
                            containsString("cannot restore a snapshot already marked as deleted"),
                            containsString("cannot clone a snapshot that is marked as deleted"),
                            containsString("cannot clone from snapshot that is being deleted"),
                            allOf(containsString('[' + repository + ":snapshot-"), containsString(" is missing"))
                        )
                    );
                }
            }
        });

        awaitNoMoreSnapshotsDeletions();

        assertBusy(() -> {
            for (Map.Entry<String, String> mount : mounts.entrySet()) {
                expectThrows(
                    IndexNotFoundException.class,
                    "Expected index to be deleted: " + mount.getKey(),
                    () -> client().admin().indices().prepareGetIndex().setIndices(mount.getKey()).get()
                );
                expectThrows(
                    SnapshotMissingException.class,
                    "Expected snapshot to be deleted: " + mount.getValue(),
                    () -> client().admin().cluster().prepareGetSnapshots(repository).setSnapshots(mount.getValue()).get()
                );
            }
        });
    }

    public void testSearchableSnapshotIsDeletedWithOnGoingRestore() throws Exception {
        final String repository = "repository-" + randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createRepository(logger, repository, "mock");

        final String index = "index";
        assertAcked(prepareCreate(index, Settings.builder().put(INDEX_SOFT_DELETES_SETTING.getKey(), true)));
        ensureGreen(index);
        populateIndex(index, scaledRandomIntBetween(0, 5_000));
        refresh(index);

        final TotalHits totalHits = internalCluster().client().prepareSearch(index).setTrackTotalHits(true).get().getHits().getTotalHits();
        final NumShards numShards = getNumShards(index);

        final String snapshot = "snapshot";
        createSnapshot(repository, snapshot, List.of(index));
        assertAcked(client().admin().indices().prepareDelete(index));

        final String mounted = "mounted-" + index;
        mountSnapshot(repository, snapshot, index, mounted, deleteSnapshotIndexSettings(true), randomFrom(Storage.values()));
        assertHitCount(client().prepareSearch(mounted).setTrackTotalHits(true).get(), totalHits.value);

        final IndexId indexId = getRepositoryData(repository).resolveIndexId(index);
        blockMasterOnShardLevelSnapshotFile(repository, indexId.getId());

        final ActionFuture<RestoreSnapshotResponse> restoreFuture = client().admin()
            .cluster()
            .prepareRestoreSnapshot(repository, snapshot)
            .setIndices(index)
            .setRenamePattern("(.+)")
            .setRenameReplacement("restored-$1")
            .setWaitForCompletion(true)
            .execute();

        awaitClusterState(state -> state.custom(RestoreInProgress.TYPE, RestoreInProgress.EMPTY).isEmpty() == false);

        final ActionFuture<AcknowledgedResponse> deleteIndexFuture = client().admin().indices().prepareDelete(mounted).execute();

        awaitClusterState(
            state -> state.metadata()
                .custom(RepositoriesMetadata.TYPE, RepositoriesMetadata.EMPTY)
                .repository(repository)
                .hasSnapshotsToDelete()
        );

        unblockAllDataNodes(repository);
        awaitNoMoreSnapshotsDeletions();

        final RestoreInfo restoreInfoResponse = restoreFuture.actionGet().getRestoreInfo();
        assertThat(restoreInfoResponse.successfulShards(), equalTo(numShards.numPrimaries));
        assertThat(restoreInfoResponse.failedShards(), equalTo(0));
        assertAcked(deleteIndexFuture.get());

        expectThrows(SnapshotMissingException.class, () -> clusterAdmin().prepareDeleteSnapshot(repository, snapshot).get());
    }

    public void testSearchableSnapshotIsDeletedWithOnGoingClone() throws Exception {
        final String repository = "repository-" + randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createRepository(logger, repository, "mock");

        final String index = "index";
        assertAcked(prepareCreate(index, Settings.builder().put(INDEX_SOFT_DELETES_SETTING.getKey(), true)));
        ensureGreen(index);
        populateIndex(index, scaledRandomIntBetween(0, 5_000));
        refresh(index);

        final TotalHits totalHits = internalCluster().client().prepareSearch(index).setTrackTotalHits(true).get().getHits().getTotalHits();

        final String sourceSnapshot = "source-snapshot";
        createFullSnapshot(repository, sourceSnapshot);
        assertAcked(client().admin().indices().prepareDelete(index));

        final String mounted = "mounted-" + index;
        mountSnapshot(repository, sourceSnapshot, index, mounted, deleteSnapshotIndexSettings(true), randomFrom(Storage.values()));
        assertHitCount(client().prepareSearch(mounted).setTrackTotalHits(true).get(), totalHits.value);

        final IndexId indexId = getRepositoryData(repository).resolveIndexId(index);
        blockMasterOnShardLevelSnapshotFile(repository, indexId.getId());

        final String cloneTarget = "target-snapshot";
        final ActionFuture<AcknowledgedResponse> cloneSnapshot = clusterAdmin().prepareCloneSnapshot(
            repository,
            sourceSnapshot,
            cloneTarget
        ).setIndices(index).execute();
        awaitNumberOfSnapshotsInProgress(1);

        final String masterNode = internalCluster().getMasterName();
        waitForBlock(masterNode, repository);

        final ActionFuture<AcknowledgedResponse> deleteIndex = client().admin().indices().prepareDelete(mounted).execute();
        awaitClusterState(
            state -> state.metadata()
                .custom(RepositoriesMetadata.TYPE, RepositoriesMetadata.EMPTY)
                .repository(repository)
                .hasSnapshotsToDelete()
        );

        assertFalse(cloneSnapshot.isDone());
        unblockNode(repository, masterNode);
        awaitNoMoreSnapshotsDeletions();

        assertAcked(cloneSnapshot.get());
        assertAcked(deleteIndex.get());

        assertThat(clusterAdmin().prepareSnapshotStatus().setRepository(repository).get().getSnapshots(), hasSize(0));
    }

    public void testSearchableSnapshotDeletedFromReadOnlyRepository() throws Exception {
        final String repository = "repository-" + randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final Settings.Builder repositorySettings = randomRepositorySettings();
        createRepository(repository, "mock", repositorySettings);

        final String index = "index";
        assertAcked(prepareCreate(index, Settings.builder().put(INDEX_SOFT_DELETES_SETTING.getKey(), true)));
        ensureGreen(index);
        populateIndex(index, scaledRandomIntBetween(10, 5_000));
        refresh(index);

        final TotalHits totalHits = internalCluster().client().prepareSearch(index).setTrackTotalHits(true).get().getHits().getTotalHits();

        final String sourceSnapshot = "source-snapshot";
        createFullSnapshot(repository, sourceSnapshot);
        assertAcked(client().admin().indices().prepareDelete(index));

        final String mounted = "mounted-" + index;
        mountSnapshot(repository, sourceSnapshot, index, mounted, deleteSnapshotIndexSettings(true), randomFrom(Storage.values()));
        assertHitCount(client().prepareSearch(mounted).setTrackTotalHits(true).get(), totalHits.value);

        logger.info("--> updating repository [{}] to be readonly", repository);
        assertAcked(
            client().admin()
                .cluster()
                .preparePutRepository(repository)
                .setVerify(randomBoolean())
                .setType(FsRepository.TYPE)
                .setSettings(repositorySettings.put(BlobStoreRepository.READONLY_SETTING_KEY, true))
        );

        logger.info("--> deleting snapshot backed index [{}]", mounted);
        assertAcked(client().admin().indices().prepareDelete(mounted));

        awaitClusterState(state -> {
            final RepositoriesMetadata repositories = state.metadata().custom(RepositoriesMetadata.TYPE, RepositoriesMetadata.EMPTY);
            final RepositoryMetadata repositoryMetadata = repositories.repository(repository);
            return repositoryMetadata != null
                && repositoryMetadata.hasSnapshotsToDelete()
                && repositoryMetadata.settings().getAsBoolean(BlobStoreRepository.READONLY_SETTING_KEY, false);
        });

        logger.info("--> updating repository [{}] to be writeable again", repository);
        assertBusy(() -> {
            try {
                AcknowledgedResponse response = client().admin()
                    .cluster()
                    .preparePutRepository(repository)
                    .setVerify(randomBoolean())
                    .setType(FsRepository.TYPE)
                    .setSettings(repositorySettings.putNull(BlobStoreRepository.READONLY_SETTING_KEY))
                    .get();
                assertAcked(response);
            } catch (IllegalStateException e) {
                assertThat(
                    e.getMessage(),
                    containsString(
                        "trying to modify or unregister repository ["
                            + repository
                            + "] that is currently used (snapshot deletion is in progress)"
                    )
                );
            }
        });

        awaitNoMoreSnapshotsDeletions();
        expectThrows(
            SnapshotMissingException.class,
            () -> clusterAdmin().prepareSnapshotStatus().setRepository(repository).setSnapshots(sourceSnapshot).get()
        );
    }

    private static Settings deleteSnapshotIndexSettings(boolean value) {
        return Settings.builder().put(SEARCHABLE_SNAPSHOTS_DELETE_SNAPSHOT_ON_INDEX_DELETION, value).build();
    }

    private static Settings deleteSnapshotIndexSettingsOrNull(boolean value) {
        if (value) {
            return deleteSnapshotIndexSettings(true);
        } else if (randomBoolean()) {
            return deleteSnapshotIndexSettings(false);
        } else {
            return Settings.EMPTY;
        }
    }

    @Nullable
    private static String getDeleteSnapshotIndexSetting(String indexName) {
        final GetSettingsResponse getSettingsResponse = client().admin().indices().prepareGetSettings(indexName).get();
        return getSettingsResponse.getSetting(indexName, SEARCHABLE_SNAPSHOTS_DELETE_SNAPSHOT_ON_INDEX_DELETION);
    }

    protected void awaitNoMoreSnapshotsDeletions() throws Exception {
        final String master = internalCluster().getMasterName();
        awaitClusterState(logger, master, state -> {
            SnapshotDeletionsInProgress deletions = state.custom(SnapshotDeletionsInProgress.TYPE, SnapshotDeletionsInProgress.EMPTY);
            if (deletions.hasDeletionsInProgress()) {
                return false;
            }
            RepositoriesMetadata repositories = state.metadata().custom(RepositoriesMetadata.TYPE, RepositoriesMetadata.EMPTY);
            for (RepositoryMetadata repository : repositories.repositories()) {
                if (repository.hasSnapshotsToDelete()) {
                    return false;
                }
            }
            return true;
        });
    }
}
