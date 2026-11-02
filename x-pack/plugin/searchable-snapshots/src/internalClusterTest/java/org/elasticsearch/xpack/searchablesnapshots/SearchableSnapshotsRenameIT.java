/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots;

import org.elasticsearch.action.admin.indices.rename.RenameIndexAction;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotRequest.Storage;

/**
 * A mounted searchable snapshot index keeps the snapshot and repository it is backed by in its settings, which move with the
 * metadata on a rename, so the renamed index must stay searchable and must recover from the snapshot again under its new name.
 */
public class SearchableSnapshotsRenameIT extends BaseSearchableSnapshotsIntegTestCase {

    public void testRenameMountedIndex() throws Exception {
        final String index = "source";
        final String mounted = "mounted";
        final String renamed = "mounted-renamed";
        createAndPopulateIndex(index, Settings.builder().put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true));
        final long docs = client().prepareSearch(index).setSize(0).get().getHits().getTotalHits().value();
        createRepository("repo", "fs");
        createFullSnapshot("repo", "snap");
        assertAcked(indicesAdmin().prepareDelete(index));

        final Storage storage = randomFrom(Storage.values());
        mountSnapshot("repo", "snap", index, mounted, Settings.EMPTY, storage);
        ensureGreen(mounted);
        ElasticsearchAssertions.assertHitCount(client().prepareSearch(mounted).setSize(0), docs);

        final var response = client().execute(
            RenameIndexAction.INSTANCE,
            new RenameIndexAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, mounted, renamed)
        ).actionGet();
        assertTrue(response.isAcknowledged());
        ensureGreen(renamed);
        ElasticsearchAssertions.assertHitCount(client().prepareSearch(renamed).setSize(0), docs);

        // a restart forces the renamed index to be recovered from the snapshot again under its new name
        internalCluster().fullRestart();
        ensureGreen(renamed);
        ElasticsearchAssertions.assertHitCount(client().prepareSearch(renamed).setSize(0), docs);
        assertFalse(indexExists(mounted));
    }
}
