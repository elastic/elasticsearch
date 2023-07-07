/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.test;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class IndexSettingsModule extends AbstractModule {

    private final Index index;
    private final Settings settings;

    public IndexSettingsModule(Index index, Settings settings) {
        this.settings = settings;
        this.index = index;

    }

    @Override
    protected void configure() {
        bind(IndexSettings.class).toInstance(newIndexSettings(index, settings));
    }

    public static IndexSettings newIndexSettings(String index, Settings settings, Setting<?>... setting) {
        return newIndexSettings(
            new Index(index, settings.get(IndexMetadata.SETTING_INDEX_UUID, IndexMetadata.INDEX_UUID_NA_VALUE)),
            settings,
            setting
        );
    }

    public static IndexSettings newIndexSettings(Index index, Settings settings, Setting<?>... setting) {
        return newIndexSettings(index, settings, Settings.EMPTY, setting);
    }

    public static IndexSettings newIndexSettings(Index index, Settings indexSetting, Settings nodeSettings, Setting<?>... setting) {
        Settings build = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(indexSetting)
            .build();
        IndexMetadata metadata = IndexMetadata.builder(index.getName())
            .system(IndexSettings.INDEX_FAST_REFRESH_SETTING.get(indexSetting)) // using fast refresh requires a system index
            .settings(build)
            .build();
        Set<Setting<?>> settingSet = new HashSet<>(IndexScopedSettings.BUILT_IN_INDEX_SETTINGS);
        if (setting.length > 0) {
            settingSet.addAll(Arrays.asList(setting));
        }
        return new IndexSettings(metadata, nodeSettings, new IndexScopedSettings(Settings.EMPTY, settingSet));
    }

    public static IndexSettings newIndexSettings(Index index, Settings settings, IndexScopedSettings indexScopedSettings) {
        Settings build = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(settings)
            .build();
        IndexMetadata metadata = IndexMetadata.builder(index.getName())
            .system(IndexSettings.INDEX_FAST_REFRESH_SETTING.get(settings)) // using fast refresh requires a system index
            .settings(build)
            .build();
        return new IndexSettings(metadata, Settings.EMPTY, indexScopedSettings);
    }

    public static IndexSettings newIndexSettings(final IndexMetadata indexMetadata, Setting<?>... setting) {
        Set<Setting<?>> settingSet = new HashSet<>(IndexScopedSettings.BUILT_IN_INDEX_SETTINGS);
        if (setting.length > 0) {
            settingSet.addAll(Arrays.asList(setting));
        }
        return new IndexSettings(indexMetadata, Settings.EMPTY, new IndexScopedSettings(Settings.EMPTY, settingSet));
    }
}
