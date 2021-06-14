/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.searchablesnapshots;

import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

import java.util.Map;

import static org.elasticsearch.index.IndexModule.INDEX_STORE_TYPE_SETTING;

public class SearchableSnapshotsConstants {
    public static final String SNAPSHOT_DIRECTORY_FACTORY_KEY = "snapshot";

    public static final String SNAPSHOT_RECOVERY_STATE_FACTORY_KEY = "snapshot_prewarm";
    public static final Setting<Boolean> SNAPSHOT_PARTIAL_SETTING = Setting.boolSetting(
        "index.store.snapshot.partial",
        false,
        Setting.Property.IndexScope,
        Setting.Property.PrivateIndex,
        Setting.Property.NotCopyableOnResize
    );

    public static boolean isSearchableSnapshotStore(Settings indexSettings) {
        return SNAPSHOT_DIRECTORY_FACTORY_KEY.equals(INDEX_STORE_TYPE_SETTING.get(indexSettings));
    }

    /**
     * Based on a map from setting to value, do the settings represent a partial searchable snapshot index?
     *
     * Both index.store.type and index.store.snapshot.partial must be supplied.
     */
    public static boolean isPartialSearchableSnapshotIndex(Map<Setting<?>, Object> indexSettings) {
        assert indexSettings.containsKey(INDEX_STORE_TYPE_SETTING) : "must include store type in map";
        assert indexSettings.get(SNAPSHOT_PARTIAL_SETTING) != null : "partial setting must be non-null in map (has default value)";
        return SNAPSHOT_DIRECTORY_FACTORY_KEY.equals(indexSettings.get(INDEX_STORE_TYPE_SETTING))
            && (boolean) indexSettings.get(SNAPSHOT_PARTIAL_SETTING);
    }

    public static boolean isPartialSearchableSnapshotIndex(Settings indexSettings) {
        return SNAPSHOT_DIRECTORY_FACTORY_KEY.equals(INDEX_STORE_TYPE_SETTING.get(indexSettings))
            && SNAPSHOT_PARTIAL_SETTING.get(indexSettings);
    }

    public static final String CACHE_FETCH_ASYNC_THREAD_POOL_NAME = "searchable_snapshots_cache_fetch_async";
    public static final String CACHE_FETCH_ASYNC_THREAD_POOL_SETTING = "xpack.searchable_snapshots.cache_fetch_async_thread_pool";

    public static final String CACHE_PREWARMING_THREAD_POOL_NAME = "searchable_snapshots_cache_prewarming";
    public static final String CACHE_PREWARMING_THREAD_POOL_SETTING = "xpack.searchable_snapshots.cache_prewarming_thread_pool";

    public static final String SNAPSHOT_BLOB_CACHE_INDEX = ".snapshot-blob-cache";

    public static final Version SHARED_CACHE_VERSION = Version.V_7_12_0;

}
