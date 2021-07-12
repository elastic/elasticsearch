/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots;

import org.elasticsearch.action.admin.indices.shrink.ResizeType;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.After;
import org.junit.Before;

import java.util.List;

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING;
import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_NUMBER_OF_ROUTING_SHARDS_SETTING;
import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING;
import static org.elasticsearch.index.IndexSettings.INDEX_SOFT_DELETES_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotRequest.Storage;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(numDataNodes = 1)
public class SearchableSnapshotsResizeIntegTests extends BaseFrozenSearchableSnapshotsIntegTestCase {

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        createRepository("repository", FsRepository.TYPE);
        assertAcked(
            prepareCreate(
                "index",
                Settings.builder()
                    .put(INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
                    .put(INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 2)
                    .put(INDEX_NUMBER_OF_ROUTING_SHARDS_SETTING.getKey(), 4)
                    .put(INDEX_SOFT_DELETES_SETTING.getKey(), true)
            )
        );
        createSnapshot("repository", "snapshot", List.of("index"));
        assertAcked(client().admin().indices().prepareDelete("index"));
        mountSnapshot("repository", "snapshot", "index", "mounted-index", Settings.EMPTY, randomFrom(Storage.values()));
        ensureGreen("mounted-index");
    }

    @After
    @Override
    public void tearDown() throws Exception {
        assertAcked(client().admin().indices().prepareDelete("mounted-*"));
        assertAcked(client().admin().cluster().prepareDeleteSnapshot("repository", "snapshot").get());
        assertAcked(client().admin().cluster().prepareDeleteRepository("repository"));
        super.tearDown();
    }

    private void assertCannotResize(ThrowingRunnable runnable) {
        final IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, runnable);
        assertThat(exception.getMessage(), equalTo("searchable snapshot index [mounted-index] cannot be resized"));
    }

    public void testShrinkSearchableSnapshotIndex() {
        assertCannotResize(
            () -> client().admin()
                .indices()
                .prepareResizeIndex("mounted-index", "shrunk-index")
                .setResizeType(ResizeType.SHRINK)
                .setSettings(
                    Settings.builder()
                        .put(INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
                        .put(INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                        .build()
                )
                .get()
        );
    }

    public void testSplitSearchableSnapshotIndex() {
        assertCannotResize(
            () -> client().admin()
                .indices()
                .prepareResizeIndex("mounted-index", "split-index")
                .setResizeType(ResizeType.SPLIT)
                .setSettings(
                    Settings.builder()
                        .put(INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
                        .put(INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 4)
                        .build()
                )
                .get()
        );
    }

    public void testCloneSearchableSnapshotIndex() {
        assertCannotResize(
            () -> client().admin().indices().prepareResizeIndex("mounted-index", "cloned-index").setResizeType(ResizeType.CLONE).get()
        );
    }
}
