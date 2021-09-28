/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots;

import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotAction;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotRequest;
import org.junit.Before;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class SearchableSnapshotsSettingValidationIntegTests extends BaseFrozenSearchableSnapshotsIntegTestCase {

    private static final String repoName = "test-repo";
    private static final String indexName = "test-index";
    private static final String snapshotName = "test-snapshot";

    @Before
    public void createAndMountSearchableSnapshot() throws Exception {
        createRepository(repoName, "fs");
        createIndex(indexName);
        createFullSnapshot(repoName, snapshotName);

        assertAcked(client().admin().indices().prepareDelete(indexName));

        final MountSearchableSnapshotRequest req = new MountSearchableSnapshotRequest(
            indexName,
            repoName,
            snapshotName,
            indexName,
            Settings.EMPTY,
            Strings.EMPTY_ARRAY,
            true,
            randomFrom(MountSearchableSnapshotRequest.Storage.values())
        );

        final RestoreSnapshotResponse restoreSnapshotResponse = client().execute(MountSearchableSnapshotAction.INSTANCE, req).get();
        assertThat(restoreSnapshotResponse.getRestoreInfo().failedShards(), equalTo(0));
        ensureGreen(indexName);
    }

    public void testCannotRemoveWriteBlock() {
        final IllegalArgumentException iae = expectThrows(
            IllegalArgumentException.class,
            () -> client().admin()
                .indices()
                .prepareUpdateSettings(indexName)
                .setSettings(Settings.builder().put(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey(), false))
                .get()
        );
        assertThat(
            iae.getMessage(),
            containsString("illegal value can't update [" + IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey() + "] from [true] to [false]")
        );
        assertNotNull(iae.getCause());
        assertThat(iae.getCause().getMessage(), containsString("Cannot remove write block from searchable snapshot index"));

        client().admin()
            .indices()
            .prepareUpdateSettings(indexName)
            .setSettings(Settings.builder().put(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey(), true))
            .get();
    }

}
