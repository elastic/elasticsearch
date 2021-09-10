/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.searchablesnapshots;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicensedFeature;
import org.elasticsearch.snapshots.SearchableSnapshotsSettings;

import java.util.Map;

import static org.elasticsearch.index.IndexModule.INDEX_STORE_TYPE_SETTING;
import static org.elasticsearch.snapshots.SearchableSnapshotsSettings.SEARCHABLE_SNAPSHOT_PARTIAL_SETTING_KEY;

public class SearchableSnapshotsConstants {

    // This should really be in the searchable-snapshots module, but ILM needs access to it
    // to short-circuit if not allowed. We should consider making the coupling looser,
    // perhaps through SPI.
    public static final LicensedFeature.Momentary SEARCHABLE_SNAPSHOT_FEATURE =
        LicensedFeature.momentary(null, "searchable-snapshots", License.OperationMode.PLATINUM);

    public static final Setting<Boolean> SNAPSHOT_PARTIAL_SETTING = Setting.boolSetting(
        SEARCHABLE_SNAPSHOT_PARTIAL_SETTING_KEY,
        false,
        Setting.Property.IndexScope,
        Setting.Property.PrivateIndex,
        Setting.Property.NotCopyableOnResize
    );

    /**
     * Based on a map from setting to value, do the settings represent a partial searchable snapshot index?
     *
     * Both index.store.type and index.store.snapshot.partial must be supplied.
     */
    public static boolean isPartialSearchableSnapshotIndex(Map<Setting<?>, Object> indexSettings) {
        assert indexSettings.containsKey(INDEX_STORE_TYPE_SETTING) : "must include store type in map";
        assert indexSettings.get(SNAPSHOT_PARTIAL_SETTING) != null : "partial setting must be non-null in map (has default value)";
        return SearchableSnapshotsSettings.SEARCHABLE_SNAPSHOT_STORE_TYPE.equals(indexSettings.get(INDEX_STORE_TYPE_SETTING))
            && (boolean) indexSettings.get(SNAPSHOT_PARTIAL_SETTING);
    }
}
