/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;

import java.util.List;
import java.util.UUID;

public class TransportDownsampleActionTests extends ESTestCase {
    public void testCopyIndexMetadata() {
        // GIVEN
        final List<String> tiers = List.of(DataTier.DATA_HOT, DataTier.DATA_WARM, DataTier.DATA_COLD, DataTier.DATA_CONTENT);
        final IndexMetadata source = new IndexMetadata.Builder(randomAlphaOfLength(10)).settings(
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, randomIntBetween(1, 5))
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, randomIntBetween(1, 3))
                .put(IndexMetadata.INDEX_DOWNSAMPLE_SOURCE_UUID_KEY, UUID.randomUUID().toString())
                .put(IndexSettings.DEFAULT_PIPELINE.getKey(), randomAlphaOfLength(15))
                .put(IndexSettings.FINAL_PIPELINE.getKey(), randomAlphaOfLength(11))
                .put(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey(), randomBoolean())
                .put(DataTier.TIER_PREFERENCE_SETTING.getKey(), randomFrom(tiers))
                .put(IndexMetadata.SETTING_INDEX_PROVIDED_NAME, randomAlphaOfLength(20))
                .put(IndexMetadata.SETTING_CREATION_DATE, System.currentTimeMillis())
                .put(LifecycleSettings.LIFECYCLE_NAME, randomAlphaOfLength(9))
        ).build();
        final IndexMetadata target = new IndexMetadata.Builder(randomAlphaOfLength(10)).settings(
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 6)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 4)
                .put(IndexMetadata.INDEX_DOWNSAMPLE_SOURCE_UUID_KEY, UUID.randomUUID().toString())
                .put(IndexSettings.DEFAULT_PIPELINE.getKey(), randomAlphaOfLength(15))
                .put(IndexSettings.FINAL_PIPELINE.getKey(), randomAlphaOfLength(11))
                .put(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey(), randomBoolean())
                .put(DataTier.TIER_PREFERENCE_SETTING.getKey(), randomFrom(tiers))
                .put(IndexMetadata.SETTING_CREATION_DATE, System.currentTimeMillis())
        ).build();
        final IndexScopedSettings indexScopedSettings = new IndexScopedSettings(
            Settings.EMPTY,
            IndexScopedSettings.BUILT_IN_INDEX_SETTINGS
        );

        // WHEN
        final Settings indexMetadata = TransportDownsampleAction.copyIndexMetadata(source, target, indexScopedSettings)
            .build()
            .getSettings();

        // THEN
        assertTargetSettings(target, indexMetadata);
        assertSourceSettings(source, indexMetadata);
        assertEquals(IndexMetadata.INDEX_UUID_NA_VALUE, indexMetadata.get(IndexMetadata.INDEX_DOWNSAMPLE_SOURCE_UUID_KEY));
        assertFalse(LifecycleSettings.LIFECYCLE_NAME_SETTING.exists(indexMetadata));
    }

    private static void assertSourceSettings(final IndexMetadata indexMetadata, final Settings settings) {
        assertEquals(indexMetadata.getIndex().getName(), settings.get(IndexMetadata.INDEX_DOWNSAMPLE_SOURCE_NAME_KEY));
        assertEquals(
            indexMetadata.getSettings().get(DataTier.TIER_PREFERENCE_SETTING.getKey()),
            settings.get(DataTier.TIER_PREFERENCE_SETTING.getKey())
        );
        assertEquals(indexMetadata.getSettings().get(IndexMetadata.LIFECYCLE_NAME), settings.get(IndexMetadata.LIFECYCLE_NAME));
        // NOTE: setting only in source (not forbidden, not to override): expect source setting is used
        assertEquals(
            indexMetadata.getSettings().get(IndexMetadata.SETTING_INDEX_PROVIDED_NAME),
            settings.get(IndexMetadata.SETTING_INDEX_PROVIDED_NAME)
        );
    }

    private static void assertTargetSettings(final IndexMetadata indexMetadata, final Settings settings) {
        assertEquals(
            indexMetadata.getSettings().get(IndexMetadata.SETTING_NUMBER_OF_SHARDS),
            settings.get(IndexMetadata.SETTING_NUMBER_OF_SHARDS)
        );
        assertEquals(
            indexMetadata.getSettings().get(IndexMetadata.SETTING_NUMBER_OF_REPLICAS),
            settings.get(IndexMetadata.SETTING_NUMBER_OF_REPLICAS)
        );
        assertEquals(
            indexMetadata.getSettings().get(IndexSettings.FINAL_PIPELINE.getKey()),
            settings.get(IndexSettings.FINAL_PIPELINE.getKey())
        );
        assertEquals(
            indexMetadata.getSettings().get(IndexSettings.DEFAULT_PIPELINE.getKey()),
            settings.get(IndexSettings.DEFAULT_PIPELINE.getKey())
        );
        assertEquals(
            indexMetadata.getSettings().get(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey()),
            settings.get(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey())
        );
        assertEquals(indexMetadata.getSettings().get(LifecycleSettings.LIFECYCLE_NAME), settings.get(LifecycleSettings.LIFECYCLE_NAME));
        // NOTE: setting both in source and target (not forbidden, not to override): expect target setting is used
        assertEquals(
            indexMetadata.getSettings().get(IndexMetadata.SETTING_CREATION_DATE),
            settings.get(IndexMetadata.SETTING_CREATION_DATE)
        );
    }
}
