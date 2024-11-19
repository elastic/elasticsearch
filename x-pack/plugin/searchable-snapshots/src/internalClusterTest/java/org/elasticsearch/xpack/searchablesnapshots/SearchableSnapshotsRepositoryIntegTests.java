/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.repositories.RepositoryConflictException;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.snapshots.SnapshotRestoreException;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING;
import static org.elasticsearch.index.IndexSettings.INDEX_SOFT_DELETES_SETTING;
import static org.elasticsearch.snapshots.SearchableSnapshotsSettings.SEARCHABLE_SNAPSHOTS_DELETE_SNAPSHOT_ON_INDEX_DELETION;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotRequest.Storage;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
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

        final TotalHits totalHits = SearchResponseUtils.getTotalHits(
            internalCluster().client().prepareSearch(indexName).setTrackTotalHits(true)
        );

        final String snapshotName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createSnapshot(repositoryName, snapshotName, List.of(indexName));
        assertAcked(indicesAdmin().prepareDelete(indexName));

        final int nbMountedIndices = 1;
        randomIntBetween(1, 5);
        final String[] mountedIndices = new String[nbMountedIndices];

        for (int i = 0; i < nbMountedIndices; i++) {
            Storage storage = randomFrom(Storage.values());
            String restoredIndexName = (storage == Storage.FULL_COPY ? "fully-mounted-" : "partially-mounted-") + indexName + '-' + i;
            mountSnapshot(repositoryName, snapshotName, indexName, restoredIndexName, Settings.EMPTY, storage);
            assertHitCount(prepareSearch(restoredIndexName).setTrackTotalHits(true), totalHits.value());
            mountedIndices[i] = restoredIndexName;
        }

        assertAcked(
            clusterAdmin().preparePutRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, repositoryName)
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
            assertAcked(indicesAdmin().prepareDelete(mountedIndices));
            assertAcked(clusterAdmin().prepareDeleteRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, repositoryName));

            updatedRepositoryName = repositoryName + "-with-mounted-indices";
            createRepository(updatedRepositoryName, FsRepository.TYPE, repositorySettings, randomBoolean());

            final RestoreSnapshotResponse restoreResponse = clusterAdmin().prepareRestoreSnapshot(
                TEST_REQUEST_TIMEOUT,
                updatedRepositoryName,
                snapshotWithMountedIndices
            ).setWaitForCompletion(true).setIndices(mountedIndices).get();
            assertEquals(restoreResponse.getRestoreInfo().totalShards(), restoreResponse.getRestoreInfo().successfulShards());
        } else {
            updatedRepositoryName = repositoryName;
        }

        for (int i = 0; i < nbMountedIndices; i++) {
            RepositoryConflictException exception = expectThrows(
                RepositoryConflictException.class,
                () -> clusterAdmin().prepareDeleteRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, updatedRepositoryName).get()
            );
            assertThat(
                exception.getMessage(),
                containsString(
                    "["
                        + updatedRepositoryName
                        + "] trying to modify or unregister repository that is currently used (found "
                        + (nbMountedIndices - i)
                        + " searchable snapshots indices that use the repository:"
                )
            );
            assertAcked(indicesAdmin().prepareDelete(mountedIndices[i]));
        }

        assertAcked(clusterAdmin().prepareDeleteRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, updatedRepositoryName));
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
        assertAcked(indicesAdmin().prepareDelete("index-*"));

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
    }

    public void testMountIndexWithDifferentDeletionOfSnapshot() throws Exception {
        final String repository = "repository-" + getTestName().toLowerCase(Locale.ROOT);
        createRepository(repository, FsRepository.TYPE, randomRepositorySettings());

        final String index = "index";
        createAndPopulateIndex(index, Settings.builder().put(INDEX_SOFT_DELETES_SETTING.getKey(), true));

        final TotalHits totalHits = SearchResponseUtils.getTotalHits(
            internalCluster().client().prepareSearch(index).setTrackTotalHits(true)
        );

        final String snapshot = "snapshot";
        createSnapshot(repository, snapshot, List.of(index));
        assertAcked(indicesAdmin().prepareDelete(index));

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
        assertHitCount(prepareSearch(mounted).setTrackTotalHits(true), totalHits.value());

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
        assertHitCount(prepareSearch(mountedAgain).setTrackTotalHits(true), totalHits.value());

        assertAcked(indicesAdmin().prepareDelete(mountedAgain));
        assertAcked(indicesAdmin().prepareDelete(mounted));
    }

    public void testDeletionOfSnapshotSettingCannotBeUpdated() throws Exception {
        final String repository = "repository-" + getTestName().toLowerCase(Locale.ROOT);
        createRepository(repository, FsRepository.TYPE, randomRepositorySettings());

        final String index = "index";
        createAndPopulateIndex(index, Settings.builder().put(INDEX_SOFT_DELETES_SETTING.getKey(), true));

        final TotalHits totalHits = SearchResponseUtils.getTotalHits(
            internalCluster().client().prepareSearch(index).setTrackTotalHits(true)
        );

        final String snapshot = "snapshot";
        createSnapshot(repository, snapshot, List.of(index));
        assertAcked(indicesAdmin().prepareDelete(index));

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
        assertHitCount(prepareSearch(mounted).setTrackTotalHits(true), totalHits.value());

        if (randomBoolean()) {
            assertAcked(indicesAdmin().prepareClose(mounted));
        }

        final IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> indicesAdmin().prepareUpdateSettings(mounted).setSettings(deleteSnapshotIndexSettings(deleteSnapshot == false)).get()
        );
        assertThat(
            exception.getMessage(),
            containsString("can not update private setting [index.store.snapshot.delete_searchable_snapshot]; ")
        );

        assertAcked(indicesAdmin().prepareDelete(mounted));
    }

    public void testRestoreSearchableSnapshotIndexConflicts() throws Exception {
        final String repository = "repository-" + getTestName().toLowerCase(Locale.ROOT);
        createRepository(repository, FsRepository.TYPE, randomRepositorySettings());

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createAndPopulateIndex(indexName, Settings.builder().put(INDEX_SOFT_DELETES_SETTING.getKey(), true));

        final String snapshotOfIndex = "snapshot-of-index";
        createSnapshot(repository, snapshotOfIndex, List.of(indexName));
        assertAcked(indicesAdmin().prepareDelete(indexName));

        final String mountedIndex = "mounted-index";
        final boolean deleteSnapshot = randomBoolean();
        final Settings indexSettings = deleteSnapshotIndexSettingsOrNull(deleteSnapshot);
        logger.info("--> mounting snapshot of index [{}] as [{}] with index settings [{}]", indexName, mountedIndex, indexSettings);
        mountSnapshot(repository, snapshotOfIndex, indexName, mountedIndex, indexSettings, randomFrom(Storage.values()));
        assertThat(
            getDeleteSnapshotIndexSetting(mountedIndex),
            indexSettings.hasValue(SEARCHABLE_SNAPSHOTS_DELETE_SNAPSHOT_ON_INDEX_DELETION)
                ? equalTo(Boolean.toString(deleteSnapshot))
                : nullValue()
        );

        final String snapshotOfMountedIndex = "snapshot-of-mounted-index";
        createSnapshot(repository, snapshotOfMountedIndex, List.of(mountedIndex));
        assertAcked(indicesAdmin().prepareDelete(mountedIndex));

        final String mountedIndexAgain = "mounted-index-again";
        final boolean deleteSnapshotAgain = deleteSnapshot == false;
        final Settings indexSettingsAgain = deleteSnapshotIndexSettings(deleteSnapshotAgain);
        logger.info("--> mounting snapshot of index [{}] again as [{}] with index settings [{}]", indexName, mountedIndex, indexSettings);
        mountSnapshot(repository, snapshotOfIndex, indexName, mountedIndexAgain, indexSettingsAgain, randomFrom(Storage.values()));
        assertThat(
            getDeleteSnapshotIndexSetting(mountedIndexAgain),
            indexSettingsAgain.hasValue(SEARCHABLE_SNAPSHOTS_DELETE_SNAPSHOT_ON_INDEX_DELETION)
                ? equalTo(Boolean.toString(deleteSnapshotAgain))
                : nullValue()
        );

        logger.info("--> restoring snapshot of searchable snapshot index [{}] should be conflicting", mountedIndex);
        final SnapshotRestoreException exception = expectThrows(
            SnapshotRestoreException.class,
            () -> clusterAdmin().prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, repository, snapshotOfMountedIndex)
                .setIndices(mountedIndex)
                .setWaitForCompletion(true)
                .get()
        );
        assertThat(
            exception.getMessage(),
            allOf(
                containsString("cannot mount snapshot [" + repository + '/'),
                containsString(':' + snapshotOfMountedIndex + "] as index [" + mountedIndex + "] with "),
                containsString("[index.store.snapshot.delete_searchable_snapshot: " + deleteSnapshot + "]; another "),
                containsString("index [" + mountedIndexAgain + '/'),
                containsString("is mounted with [index.store.snapshot.delete_searchable_snapshot: " + deleteSnapshotAgain + "].")
            )
        );
        assertAcked(indicesAdmin().prepareDelete("mounted-*"));
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
        final Set<String> mountedIndices = Sets.newHashSetWithExpectedSize(nbMountedIndices);

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
                assertAcked(indicesAdmin().prepareClose(mountedIndex));
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
            () -> clusterAdmin().prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, repository, snapshotOfMountedIndices)
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

        final RestoreSnapshotResponse restoreResponse = clusterAdmin().prepareRestoreSnapshot(
            TEST_REQUEST_TIMEOUT,
            repository,
            snapshotOfMountedIndices
        )
            .setIndices(restorables.toArray(String[]::new))
            .setIndexSettings(indexSettings)
            .setRenameReplacement("restored-with-same-setting-$1")
            .setRenamePattern("(.+)")
            .setWaitForCompletion(true)
            .get();
        assertThat(restoreResponse.getRestoreInfo().totalShards(), greaterThan(0));
        assertThat(restoreResponse.getRestoreInfo().failedShards(), equalTo(0));

        assertAcked(indicesAdmin().prepareDelete("mounted-*"));
        assertAcked(indicesAdmin().prepareDelete("restored-with-same-setting-*"));
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
        final GetSettingsResponse getSettingsResponse = indicesAdmin().prepareGetSettings(indexName).get();
        return getSettingsResponse.getSetting(indexName, SEARCHABLE_SNAPSHOTS_DELETE_SNAPSHOT_ON_INDEX_DELETION);
    }
}
