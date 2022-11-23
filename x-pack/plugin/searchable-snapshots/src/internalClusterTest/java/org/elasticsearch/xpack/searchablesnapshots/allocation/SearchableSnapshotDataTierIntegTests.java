/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.allocation;

import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotRequest;
import org.elasticsearch.xpack.searchablesnapshots.BaseFrozenSearchableSnapshotsIntegTestCase;

import java.util.Map;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class SearchableSnapshotDataTierIntegTests extends BaseFrozenSearchableSnapshotsIntegTestCase {

    private static final String repoName = "test-repo";
    private static final String indexName = "test-index";
    private static final String snapshotName = "test-snapshot";
    private static final String mountedIndexName = "test-index-mounted";
    private static final Settings frozenSettings = Settings.builder().put(DataTier.TIER_PREFERENCE, DataTier.DATA_FROZEN).build();

    public void testPartialLegalOnFrozen() throws Exception {
        createRepository(repoName, "fs");
        createIndex(indexName);
        createFullSnapshot(repoName, snapshotName);
        Settings mountSettings = randomFrom(Settings.EMPTY, frozenSettings);
        mountSnapshot(
            repoName,
            snapshotName,
            indexName,
            mountedIndexName,
            mountSettings,
            MountSearchableSnapshotRequest.Storage.SHARED_CACHE
        );

        updatePreference(DataTier.DATA_FROZEN);
    }

    public void testFullIllegalOnFrozen() throws Exception {
        createRepository(repoName, "fs");
        createIndex(indexName);
        createFullSnapshot(repoName, snapshotName);
        expectThrows(
            IllegalArgumentException.class,
            () -> mountSnapshot(
                repoName,
                snapshotName,
                indexName,
                mountedIndexName,
                frozenSettings,
                MountSearchableSnapshotRequest.Storage.FULL_COPY
            )
        );
        Settings mountSettings = randomFrom(
            Settings.EMPTY,
            Settings.builder()
                .put(DataTier.TIER_PREFERENCE, randomValueOtherThan(DataTier.DATA_FROZEN, () -> randomFrom(DataTier.ALL_DATA_TIERS)))
                .build()
        );
        mountSnapshot(repoName, snapshotName, indexName, mountedIndexName, mountSettings, MountSearchableSnapshotRequest.Storage.FULL_COPY);

        expectThrows(IllegalArgumentException.class, () -> updatePreference(DataTier.DATA_FROZEN));
    }

    private void updatePreference(String tier) {
        client().admin()
            .indices()
            .updateSettings(new UpdateSettingsRequest(mountedIndexName).settings(Map.of(DataTier.TIER_PREFERENCE, tier)))
            .actionGet();
    }
}
