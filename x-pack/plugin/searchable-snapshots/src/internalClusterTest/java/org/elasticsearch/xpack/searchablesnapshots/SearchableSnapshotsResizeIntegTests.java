/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots;

import org.elasticsearch.action.admin.indices.shrink.ResizeType;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexModule;
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
        indexRandomDocs("index", scaledRandomIntBetween(0, 1_000));
        createSnapshot("repository", "snapshot", List.of("index"));
        assertAcked(indicesAdmin().prepareDelete("index"));
        mountSnapshot("repository", "snapshot", "index", "mounted-index", Settings.EMPTY, randomFrom(Storage.values()));
        ensureGreen("mounted-index");
    }

    @After
    @Override
    public void tearDown() throws Exception {
        assertAcked(indicesAdmin().prepareDelete("mounted-*"));
        assertAcked(client().admin().cluster().prepareDeleteSnapshot("repository", "snapshot").get());
        assertAcked(client().admin().cluster().prepareDeleteRepository("repository"));
        super.tearDown();
    }

    public void testShrinkSearchableSnapshotIndex() {
        final IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> indicesAdmin().prepareResizeIndex("mounted-index", "shrunk-index")
                .setResizeType(ResizeType.SHRINK)
                .setSettings(indexSettingsNoReplicas(1).build())
                .get()
        );
        assertThat(exception.getMessage(), equalTo("can't shrink searchable snapshot index [mounted-index]"));
    }

    public void testSplitSearchableSnapshotIndex() {
        final IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> indicesAdmin().prepareResizeIndex("mounted-index", "split-index")
                .setResizeType(ResizeType.SPLIT)
                .setSettings(indexSettingsNoReplicas(4).build())
                .get()
        );
        assertThat(exception.getMessage(), equalTo("can't split searchable snapshot index [mounted-index]"));
    }

    public void testCloneSearchableSnapshotIndex() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> indicesAdmin().prepareResizeIndex("mounted-index", "cloned-index").setResizeType(ResizeType.CLONE).get()
        );
        assertThat(
            exception.getMessage(),
            equalTo("can't clone searchable snapshot index [mounted-index]; setting [index.store.type] should be overridden")
        );

        exception = expectThrows(
            IllegalArgumentException.class,
            () -> indicesAdmin().prepareResizeIndex("mounted-index", "cloned-index")
                .setResizeType(ResizeType.CLONE)
                .setSettings(Settings.builder().putNull(IndexModule.INDEX_STORE_TYPE_SETTING.getKey()).build())
                .get()
        );
        assertThat(
            exception.getMessage(),
            equalTo("can't clone searchable snapshot index [mounted-index]; setting [index.recovery.type] should be overridden")
        );

        assertAcked(
            indicesAdmin().prepareResizeIndex("mounted-index", "cloned-index")
                .setResizeType(ResizeType.CLONE)
                .setSettings(
                    Settings.builder()
                        .putNull(IndexModule.INDEX_STORE_TYPE_SETTING.getKey())
                        .putNull(IndexModule.INDEX_RECOVERY_TYPE_SETTING.getKey())
                        .put(DataTier.TIER_PREFERENCE, DataTier.DATA_HOT)
                        .put(INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
                        .build()
                )
        );
        ensureGreen("cloned-index");
        assertAcked(indicesAdmin().prepareDelete("cloned-index"));
    }
}
