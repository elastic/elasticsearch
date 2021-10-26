/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.allocation;

import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotAction;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotRequest;
import org.elasticsearch.xpack.searchablesnapshots.BaseSearchableSnapshotsIntegTestCase;
import org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots;

import java.util.List;
import java.util.Locale;

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_SETTING;
import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_SETTING;
import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_SETTING;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.routing.allocation.decider.FilterAllocationDecider.CLUSTER_ROUTING_EXCLUDE_GROUP_SETTING;
import static org.elasticsearch.index.IndexSettings.INDEX_SOFT_DELETES_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class AllocationFilteringIntegTests extends BaseSearchableSnapshotsIntegTestCase {

    private MountSearchableSnapshotRequest prepareMountRequest(
        Settings.Builder originalIndexSettings,
        Settings.Builder mountedIndexSettings
    ) throws InterruptedException {

        final String fsRepoName = randomAlphaOfLength(10);
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final String snapshotName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);

        createRepository(fsRepoName, "fs", Settings.builder().put("location", randomRepoPath()));
        assertAcked(
            prepareCreate(
                indexName,
                Settings.builder()
                    .put(INDEX_SOFT_DELETES_SETTING.getKey(), true)
                    .put(SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(originalIndexSettings.build())
            )
        );
        populateIndex(indexName, 10);
        ensureGreen(indexName);
        createFullSnapshot(fsRepoName, snapshotName);
        assertAcked(client().admin().indices().prepareDelete(indexName));

        final Settings.Builder indexSettingsBuilder = Settings.builder()
            .put(SearchableSnapshots.SNAPSHOT_CACHE_ENABLED_SETTING.getKey(), true)
            .put(mountedIndexSettings.build());

        return new MountSearchableSnapshotRequest(
            indexName,
            fsRepoName,
            snapshotName,
            indexName,
            indexSettingsBuilder.build(),
            Strings.EMPTY_ARRAY,
            true,
            MountSearchableSnapshotRequest.Storage.FULL_COPY
        );
    }

    public void testRemovesRequireFilter() throws InterruptedException {
        runTest(INDEX_ROUTING_REQUIRE_GROUP_SETTING, true, null, true);
    }

    public void testRemovesIncludeFilter() throws InterruptedException {
        runTest(INDEX_ROUTING_INCLUDE_GROUP_SETTING, true, null, true);
    }

    public void testRemovesExcludeFilter() throws InterruptedException {
        runTest(INDEX_ROUTING_EXCLUDE_GROUP_SETTING, false, null, true);
    }

    public void testReplacesRequireFilter() throws InterruptedException {
        runTest(INDEX_ROUTING_REQUIRE_GROUP_SETTING, true, INDEX_ROUTING_REQUIRE_GROUP_SETTING, true);
    }

    public void testReplacesIncludeFilter() throws InterruptedException {
        runTest(INDEX_ROUTING_INCLUDE_GROUP_SETTING, true, INDEX_ROUTING_INCLUDE_GROUP_SETTING, true);
    }

    public void testReplacesExcludeFilter() throws InterruptedException {
        runTest(INDEX_ROUTING_EXCLUDE_GROUP_SETTING, false, INDEX_ROUTING_EXCLUDE_GROUP_SETTING, false);
    }

    public void testReplacesIncludeFilterWithExcludeFilter() throws InterruptedException {
        runTest(INDEX_ROUTING_INCLUDE_GROUP_SETTING, true, INDEX_ROUTING_EXCLUDE_GROUP_SETTING, false);
    }

    /**
     * Starts two nodes, allocates the original index on the first node but then excludes that node and verifies that the index can still be
     * mounted and allocated to the other node.
     *
     * @param indexSetting           an allocation filter setting to apply to the original index
     * @param mountSetting           an (optional) allocation filter setting to apply at mount time
     * @param indexSettingIsPositive whether {@code indexSetting} is positive (i.e. include/require) or negative (i.e. exclude)
     * @param mountSettingIsPositive whether {@code mountSetting} is positive (i.e. include/require) or negative (i.e. exclude)
     */
    private void runTest(
        Setting.AffixSetting<String> indexSetting,
        boolean indexSettingIsPositive,
        @Nullable Setting.AffixSetting<String> mountSetting,
        boolean mountSettingIsPositive
    ) throws InterruptedException {
        final List<String> nodes = internalCluster().startNodes(2);

        // apply an index setting to restrict the original index to node 0
        final Settings.Builder indexSettings = Settings.builder()
            .put(indexSetting.getConcreteSettingForNamespace("_name").getKey(), nodes.get(indexSettingIsPositive ? 0 : 1));

        final Settings.Builder mountSettings = Settings.builder();
        if (mountSetting != null) {
            // apply an index setting to restrict the mounted index to node 1
            mountSettings.put(mountSetting.getConcreteSettingForNamespace("_name").getKey(), nodes.get(mountSettingIsPositive ? 1 : 0));
        }

        final MountSearchableSnapshotRequest mountRequest = prepareMountRequest(indexSettings, mountSettings);

        // block allocation to node 0 at the cluster level
        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(
                    Settings.builder()
                        .put(CLUSTER_ROUTING_EXCLUDE_GROUP_SETTING.getConcreteSettingForNamespace("_name").getKey(), nodes.get(0))
                )
        );

        // mount snapshot and verify it is allocated as expected
        final RestoreSnapshotResponse restoreSnapshotResponse = client().execute(MountSearchableSnapshotAction.INSTANCE, mountRequest)
            .actionGet();
        assertThat(restoreSnapshotResponse.getRestoreInfo().failedShards(), equalTo(0));
        ensureGreen(mountRequest.mountedIndexName());

        final Settings mountedIndexSettings = client().admin()
            .indices()
            .prepareGetSettings(mountRequest.mountedIndexName())
            .get()
            .getIndexToSettings()
            .get(mountRequest.mountedIndexName());

        if (mountSetting != null) {
            assertTrue(mountedIndexSettings.toString(), mountSetting.getConcreteSettingForNamespace("_name").exists(mountedIndexSettings));
        }

        if (indexSetting != mountSetting) {
            assertFalse(mountedIndexSettings.toString(), indexSetting.getConcreteSettingForNamespace("_name").exists(mountedIndexSettings));
        }

        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(
                    Settings.builder().putNull(CLUSTER_ROUTING_EXCLUDE_GROUP_SETTING.getConcreteSettingForNamespace("_name").getKey())
                )
        );
    }

}
