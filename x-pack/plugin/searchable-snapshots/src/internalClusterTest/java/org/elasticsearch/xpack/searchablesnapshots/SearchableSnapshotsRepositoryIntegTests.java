/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.snapshots.SnapshotException;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING;
import static org.elasticsearch.index.IndexSettings.INDEX_SOFT_DELETES_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotRequest.Storage;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

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

        final int nbMountedIndices = randomIntBetween(1, 5);
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

    public void testSnapshotsUsedBySearchableSnapshotCannotBeDeleted() throws Exception {
        final String repositoryName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final Settings.Builder repositorySettings = randomRepositorySettings();
        createRepository(repositoryName, FsRepository.TYPE, repositorySettings);

        final int nbIndices = randomIntBetween(1, 5);
        final Map<String, Long> docsPerIndex = new HashMap<>(nbIndices);
        final int nbSnapshots = randomIntBetween(1, 5);
        final Map<SnapshotId, List<String>> indicesPerSnapshot = new HashMap<>(nbSnapshots);
        final Set<SnapshotId> mountedSnapshots = new HashSet<>();

        for (int i = 0; i < nbIndices; i++) {
            final String indexName = "index-" + i;
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
            docsPerIndex.put(indexName, totalHits.value);
        }

        for (int i = 0; i < nbSnapshots; i++) {
            final SnapshotInfo snapshotInfo = createSnapshot(
                repositoryName,
                "snapshot-" + i,
                randomSubsetOf(between(1, nbIndices), docsPerIndex.keySet())
            );
            indicesPerSnapshot.put(snapshotInfo.snapshotId(), snapshotInfo.indices());

            if (randomBoolean()) {
                final String snapshotName = snapshotInfo.snapshotId().getName();
                final String cloneSnapshotName = "clone-" + snapshotName;
                assertAcked(
                    client().admin()
                        .cluster()
                        .prepareCloneSnapshot(repositoryName, snapshotName, cloneSnapshotName)
                        .setIndices(
                            randomSubsetOf(between(1, snapshotInfo.indices().size()), snapshotInfo.indices()).toArray(String[]::new)
                        )
                        .get()
                );

                final GetSnapshotsResponse snapshots = client().admin()
                    .cluster()
                    .prepareGetSnapshots(repositoryName)
                    .addSnapshots(cloneSnapshotName)
                    .get();
                final SnapshotInfo cloneSnapshotInfo = snapshots.getSnapshots(repositoryName).get(0);
                assertThat(cloneSnapshotInfo, notNullValue());
                assertThat(cloneSnapshotInfo.snapshotId().getName(), equalTo(cloneSnapshotName));
                indicesPerSnapshot.put(cloneSnapshotInfo.snapshotId(), cloneSnapshotInfo.indices());
            }
        }

        assertAcked(client().admin().indices().prepareDelete("index-*"));

        for (Map.Entry<SnapshotId, List<String>> snapshot : indicesPerSnapshot.entrySet()) {
            final String snapshotName = snapshot.getKey().getName();
            if (indicesPerSnapshot.size() == 1 || randomBoolean()) {
                for (String index : snapshot.getValue()) {
                    Storage storage = randomFrom(Storage.values());
                    String restoredIndexName = "mounted-" + snapshotName + '-' + index + '-' + storage.name().toLowerCase(Locale.ROOT);
                    mountSnapshot(repositoryName, snapshotName, index, restoredIndexName, Settings.EMPTY, storage);
                    assertHitCount(client().prepareSearch(restoredIndexName).setTrackTotalHits(true).get(), docsPerIndex.get(index));
                }
                mountedSnapshots.add(snapshot.getKey());
            }
        }

        for (Map.Entry<SnapshotId, List<String>> snapshot : indicesPerSnapshot.entrySet()) {
            final SnapshotId snapshotId = snapshot.getKey();
            if (mountedSnapshots.contains(snapshotId) == false) {
                assertAcked(clusterAdmin().prepareDeleteSnapshot(repositoryName, snapshotId.getName()).get());
            } else {
                SnapshotException exception = expectThrows(
                    SnapshotException.class,
                    () -> clusterAdmin().prepareDeleteSnapshot(repositoryName, snapshotId.getName()).get()
                );
                assertThat(
                    exception.getMessage(),
                    containsString(
                        "["
                            + repositoryName
                            + ':'
                            + snapshotId
                            + "] found "
                            + snapshot.getValue().size()
                            + " searchable snapshots indices that use the snapshot:"
                    )
                );
            }
        }

        assertAcked(client().admin().indices().prepareDelete("mounted-*"));

        for (SnapshotId snapshotId : indicesPerSnapshot.keySet()) {
            if (mountedSnapshots.contains(snapshotId)) {
                assertAcked(clusterAdmin().prepareDeleteSnapshot(repositoryName, snapshotId.getName()).get());
            }
        }

        final GetSnapshotsResponse snapshots = clusterAdmin().prepareGetSnapshots(repositoryName).get();
        assertThat(snapshots.getSnapshots(repositoryName), hasSize(0));
    }
}
