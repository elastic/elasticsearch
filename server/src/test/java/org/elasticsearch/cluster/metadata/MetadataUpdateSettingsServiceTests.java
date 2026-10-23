/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.apache.logging.log4j.Level;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;

import java.util.Set;
import java.util.function.Function;

import static org.elasticsearch.index.IndexModule.Type.MMAPFS;
import static org.elasticsearch.index.IndexModule.Type.NIOFS;

public class MetadataUpdateSettingsServiceTests extends ESTestCase {
    private final Index index = new Index("test", UUIDs.randomBase64UUID());
    private final Settings metaSettings = Settings.builder()
        .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID())
        .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
        .put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), IndexVersion.current())
        .build();
    private final IndexScopedSettings indexScopedSettings = new IndexScopedSettings(
        Settings.EMPTY,
        IndexScopedSettings.BUILT_IN_INDEX_SETTINGS
    );

    /**
     * test update dynamic settings for open index, also test override existing settings if preserveExisting is false
     */
    public void testUpdateOpenIndexSettings() {
        // original settings: {"index.number_of_replicas": 5, "index.refresh_interval": "5s"}
        ProjectMetadata metadata = mockMetadata(
            index,
            Settings.builder()
                .put(metaSettings)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 5)
                .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), "5s")
                .build()
        );
        ProjectMetadata.Builder metadataBuilder = ProjectMetadata.builder(metadata);
        // settings to apply: {"index.refresh_interval": "2s", "index.translog.durability": "ASYNC"}
        Settings settingToApply = Settings.builder()
            .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), "2s")
            .put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), Translog.Durability.ASYNC)
            .build();
        // update settings
        MetadataUpdateSettingsService.updateIndexSettings(
            Set.of(index),
            metadataBuilder,
            (index, indexSettings) -> indexScopedSettings.updateDynamicSettings(
                settingToApply,
                indexSettings,
                Settings.builder(),
                index.getName()
            ),
            false,
            indexScopedSettings
        );
        // expected settings: {"index.refresh_interval": "2s", "index.number_of_replicas": 5, "index.translog.durability": "ASYNC"}
        assertEquals(
            Settings.builder()
                .put(metaSettings)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 5)
                .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), "2s")
                .put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), Translog.Durability.ASYNC)
                .build(),
            metadataBuilder.get(index.getName()).getSettings()
        );
    }

    /**
     * test update dynamic and static settings for closed index, also test not override existing settings if preserveExisting is ture
     */
    public void testUpdateClosedIndexSettings() {
        // original dynamic settings: {"index.number_of_replicas": 5, "index.refresh_interval": "5s"}
        // original static settings: {"index.codec": "best_compression", "index.store.type": "niofs"}
        ProjectMetadata metadata = mockMetadata(
            index,
            Settings.builder()
                .put(metaSettings)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 5)
                .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), "5s")
                .put(EngineConfig.INDEX_CODEC_SETTING.getKey(), CodecService.BEST_COMPRESSION_CODEC)
                .put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), NIOFS.getSettingsKey())
                .build()
        );
        ProjectMetadata.Builder metadataBuilder = ProjectMetadata.builder(metadata);
        // dynamic settings to apply: {"index.refresh_interval": "2s", "index.translog.durability": "ASYNC"}
        // static settings to apply : {"index.store.type": "mmapfs", "index.store.preload": ["dvd", "tmp"]}
        Settings settingToApply = Settings.builder()
            .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), "2s")
            .put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), Translog.Durability.ASYNC)
            .put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), MMAPFS.getSettingsKey())
            .putList(IndexModule.INDEX_STORE_PRE_LOAD_SETTING.getKey(), "dvd", "tmp")
            .build();
        // update settings
        MetadataUpdateSettingsService.updateIndexSettings(
            Set.of(index),
            metadataBuilder,
            (index, indexSettings) -> indexScopedSettings.updateSettings(
                settingToApply,
                indexSettings,
                Settings.builder(),
                index.getName()
            ),
            true,
            indexScopedSettings
        );
        // expected dynamic settings: {"index.refresh_interval": "2s", "index.number_of_replicas": 5, "index.translog.durability": "async"}
        // expected static settings: {"index.codec": "best_compression", "index.store.type": "mmapfs", "index.store.preload": ["dvd",
        // "tmp"]}
        assertEquals(
            Settings.builder()
                .put(metaSettings)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 5)
                .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), "5s")
                .put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), Translog.Durability.ASYNC)
                .put(EngineConfig.INDEX_CODEC_SETTING.getKey(), CodecService.BEST_COMPRESSION_CODEC)
                .put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), NIOFS.getSettingsKey())
                .putList(IndexModule.INDEX_STORE_PRE_LOAD_SETTING.getKey(), "dvd", "tmp")
                .build(),
            metadataBuilder.get(index.getName()).getSettings()
        );
    }

    /**
     * Index block settings ({@code index.blocks.*}) are applied as cluster blocks and bypass the settings update
     * consumers that log other index setting changes, so historically they were updated silently. This checks that
     * adding or removing a block is logged, which matters for troubleshooting cases such as the flood-stage watermark
     * putting an index under a {@code read_only_allow_delete} block. A no-op update (the block is already in the wanted
     * state) must not log.
     */
    public void testClusterBlockUpdatesAreLogged() {
        final ProjectId projectId = randomProjectIdOrDefault();
        final ProjectMetadata.Builder metadataBuilder = ProjectMetadata.builder(projectId)
            .put(
                IndexMetadata.builder(index.getName())
                    .settings(Settings.builder().put(metaSettings).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0))
                    .build(),
                true
            );
        final IndexMetadata.APIBlock apiBlock = IndexMetadata.APIBlock.READ;
        final String settingKey = apiBlock.settingName();
        final String[] indices = new String[] { index.getName() };
        final Function<String, Boolean> notVerifiedReadOnly = indexName -> false;

        MockLog.assertThatLogger(
            () -> MetadataUpdateSettingsService.maybeUpdateClusterBlock(
                projectId,
                indices,
                ClusterBlocks.builder(),
                apiBlock.getBlock(),
                apiBlock.setting(),
                Settings.builder().put(settingKey, true).build(),
                metadataBuilder,
                notVerifiedReadOnly
            ),
            MetadataUpdateSettingsService.class,
            new MockLog.SeenEventExpectation(
                "block added",
                MetadataUpdateSettingsService.class.getCanonicalName(),
                Level.INFO,
                "updating [" + settingKey + "] to [true] for index [" + index.getName() + "]"
            )
        );

        MockLog.assertThatLogger(
            () -> MetadataUpdateSettingsService.maybeUpdateClusterBlock(
                projectId,
                indices,
                ClusterBlocks.builder().addIndexBlock(projectId, index.getName(), apiBlock.getBlock()),
                apiBlock.getBlock(),
                apiBlock.setting(),
                Settings.builder().put(settingKey, false).build(),
                metadataBuilder,
                notVerifiedReadOnly
            ),
            MetadataUpdateSettingsService.class,
            new MockLog.SeenEventExpectation(
                "block removed",
                MetadataUpdateSettingsService.class.getCanonicalName(),
                Level.INFO,
                "updating [" + settingKey + "] to [false] for index [" + index.getName() + "]"
            )
        );

        MockLog.assertThatLogger(
            () -> MetadataUpdateSettingsService.maybeUpdateClusterBlock(
                projectId,
                indices,
                ClusterBlocks.builder().addIndexBlock(projectId, index.getName(), apiBlock.getBlock()),
                apiBlock.getBlock(),
                apiBlock.setting(),
                Settings.builder().put(settingKey, true).build(),
                metadataBuilder,
                notVerifiedReadOnly
            ),
            MetadataUpdateSettingsService.class,
            new MockLog.UnseenEventExpectation(
                "no-op block update not logged",
                MetadataUpdateSettingsService.class.getCanonicalName(),
                Level.INFO,
                "updating [" + settingKey + "] to [true] for index [" + index.getName() + "]"
            )
        );
    }

    private ProjectMetadata mockMetadata(Index index, Settings indexSettings) {
        return ProjectMetadata.builder(randomProjectIdOrDefault())
            .put(IndexMetadata.builder(index.getName()).settings(Settings.builder().put(indexSettings)).build(), true)
            .build();
    }
}
