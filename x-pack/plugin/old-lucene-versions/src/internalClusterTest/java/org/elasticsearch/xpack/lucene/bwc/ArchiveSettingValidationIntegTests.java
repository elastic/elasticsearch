/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.lucene.bwc;

import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;

import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class ArchiveSettingValidationIntegTests extends AbstractArchiveTestCase {
    public void testCannotRemoveWriteBlock() throws ExecutionException, InterruptedException {
        final RestoreSnapshotRequest req = new RestoreSnapshotRequest(repoName, snapshotName).indices(indexName).waitForCompletion(true);

        final RestoreSnapshotResponse restoreSnapshotResponse = clusterAdmin().restoreSnapshot(req).get();
        assertThat(restoreSnapshotResponse.getRestoreInfo().failedShards(), equalTo(0));
        ensureGreen(indexName);

        final IllegalArgumentException iae = expectThrows(
            IllegalArgumentException.class,
            () -> indicesAdmin().prepareUpdateSettings(indexName)
                .setSettings(Settings.builder().put(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey(), false))
                .get()
        );
        assertThat(
            iae.getMessage(),
            containsString("illegal value can't update [" + IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey() + "] from [true] to [false]")
        );
        assertNotNull(iae.getCause());
        assertThat(iae.getCause().getMessage(), containsString("Cannot remove write block from archive index"));

        updateIndexSettings(Settings.builder().put(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey(), true), indexName);
    }
}
