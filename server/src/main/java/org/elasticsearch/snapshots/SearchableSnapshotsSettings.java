/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

import java.util.Map;

import static org.elasticsearch.index.IndexModule.INDEX_STORE_TYPE_SETTING;

public final class SearchableSnapshotsSettings {

    public static final String SEARCHABLE_SNAPSHOT_STORE_TYPE = "snapshot";
    public static final String SEARCHABLE_SNAPSHOT_PARTIAL_SETTING_KEY = "index.store.snapshot.partial";

    public static final Setting<Boolean> SNAPSHOT_PARTIAL_SETTING = Setting.boolSetting(
        SEARCHABLE_SNAPSHOT_PARTIAL_SETTING_KEY,
        false,
        Setting.Property.IndexScope,
        Setting.Property.PrivateIndex,
        Setting.Property.NotCopyableOnResize
    );
    public static final String SEARCHABLE_SNAPSHOTS_REPOSITORY_NAME_SETTING_KEY = "index.store.snapshot.repository_name";
    public static final String SEARCHABLE_SNAPSHOTS_REPOSITORY_UUID_SETTING_KEY = "index.store.snapshot.repository_uuid";
    public static final String SEARCHABLE_SNAPSHOTS_SNAPSHOT_NAME_SETTING_KEY = "index.store.snapshot.snapshot_name";
    public static final String SEARCHABLE_SNAPSHOTS_SNAPSHOT_UUID_SETTING_KEY = "index.store.snapshot.snapshot_uuid";
    public static final String SEARCHABLE_SNAPSHOTS_DELETE_SNAPSHOT_ON_INDEX_DELETION = "index.store.snapshot.delete_searchable_snapshot";

    private SearchableSnapshotsSettings() {}

    public static boolean isSearchableSnapshotStore(Settings indexSettings) {
        return SEARCHABLE_SNAPSHOT_STORE_TYPE.equals(INDEX_STORE_TYPE_SETTING.get(indexSettings));
    }

    public static boolean isPartialSearchableSnapshotIndex(Settings indexSettings) {
        return isSearchableSnapshotStore(indexSettings) && indexSettings.getAsBoolean(SEARCHABLE_SNAPSHOT_PARTIAL_SETTING_KEY, false);
    }

    /**
     * Based on a map from setting to value, do the settings represent a partial searchable snapshot index?
     *
     * Both index.store.type and index.store.snapshot.partial must be supplied.
     */
    public static boolean isPartialSearchableSnapshotIndex(Map<Setting<?>, Object> indexSettings) {
        assert indexSettings.containsKey(INDEX_STORE_TYPE_SETTING) : "must include store type in map";
        assert indexSettings.get(SNAPSHOT_PARTIAL_SETTING) != null : "partial setting must be non-null in map (has default value)";
        return SEARCHABLE_SNAPSHOT_STORE_TYPE.equals(indexSettings.get(INDEX_STORE_TYPE_SETTING))
            && (boolean) indexSettings.get(SNAPSHOT_PARTIAL_SETTING);
    }
}
