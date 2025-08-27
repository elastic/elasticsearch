/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Stream;

import static org.elasticsearch.index.IndexSettings.INDEX_SEARCH_IDLE_AFTER;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class RemediateSnapshotIT extends AbstractSnapshotIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Stream.concat(Stream.of(RemediateSnapshotTestPlugin.class), super.nodePlugins().stream()).toList();
    }

    public void testRemediationOnRestore() {
        Client client = client();

        createRepository("test-repo", "fs");

        createIndex("test-idx-1", "test-idx-2", "test-idx-3");
        ensureGreen();

        GetIndexResponse getIndexResponse = client.admin()
            .indices()
            .prepareGetIndex(TEST_REQUEST_TIMEOUT)
            .setIndices("test-idx-1", "test-idx-2", "test-idx-3")
            .get();

        assertThat(
            INDEX_SEARCH_IDLE_AFTER.get(getIndexResponse.settings().get("test-idx-1")),
            equalTo(INDEX_SEARCH_IDLE_AFTER.getDefault(Settings.EMPTY))
        );
        assertThat(
            INDEX_SEARCH_IDLE_AFTER.get(getIndexResponse.settings().get("test-idx-2")),
            equalTo(INDEX_SEARCH_IDLE_AFTER.getDefault(Settings.EMPTY))
        );
        assertThat(
            INDEX_SEARCH_IDLE_AFTER.get(getIndexResponse.settings().get("test-idx-3")),
            equalTo(INDEX_SEARCH_IDLE_AFTER.getDefault(Settings.EMPTY))
        );

        indexRandomDocs("test-idx-1", 100);
        indexRandomDocs("test-idx-2", 100);

        createSnapshot("test-repo", "test-snap", Arrays.asList("test-idx-1", "test-idx-2"));

        logger.info("--> close snapshot indices");
        client.admin().indices().prepareClose("test-idx-1", "test-idx-2").get();

        logger.info("--> restore indices");
        RestoreSnapshotResponse restoreSnapshotResponse = client.admin()
            .cluster()
            .prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, "test-repo", "test-snap")
            .setWaitForCompletion(true)
            .get();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));

        GetIndexResponse getIndexResponseAfter = client.admin()
            .indices()
            .prepareGetIndex(TEST_REQUEST_TIMEOUT)
            .setIndices("test-idx-1", "test-idx-2", "test-idx-3")
            .get();

        assertDocCount("test-idx-1", 100L);
        assertThat(INDEX_SEARCH_IDLE_AFTER.get(getIndexResponseAfter.settings().get("test-idx-1")), equalTo(TimeValue.timeValueMinutes(2)));
        assertDocCount("test-idx-2", 100L);
        assertThat(INDEX_SEARCH_IDLE_AFTER.get(getIndexResponseAfter.settings().get("test-idx-2")), equalTo(TimeValue.timeValueMinutes(2)));
    }

    /**
     * Dummy plugin to load SPI function off of
     */
    public static class RemediateSnapshotTestPlugin extends Plugin {}

    public static class RemediateSnapshotTestTransformer implements IndexMetadataRestoreTransformer {
        @Override
        public IndexMetadata updateIndexMetadata(IndexMetadata original) {
            // Set a property to something mild, outside its default
            return IndexMetadata.builder(original)
                .settings(
                    Settings.builder()
                        .put(original.getSettings())
                        .put(INDEX_SEARCH_IDLE_AFTER.getKey(), TimeValue.timeValueMinutes(2))
                        .build()
                )
                .build();
        }
    }
}
