/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.common.settings;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.snapshots.SnapshotsSettings;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.analytics.AnalyticsSettings;
import org.elasticsearch.xpack.core.ccr.CcrSettings;
import org.elasticsearch.xpack.core.frozen.FrozenIndicesSettings;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.watcher.WatcherSettings;

import java.util.Set;

public class XPackIndexScopedSettings {
    public static final Set<Setting<?>> X_PACK_INDEX_SETTINGS = Set.of(
        SnapshotsSettings.SOURCE_ONLY_SETTING,
        AnalyticsSettings.MAX_BUCKET_SIZE_SETTING,
        XPackSettings.X_PACK_VERSION_SETTING,
        CcrSettings.CCR_FOLLOWING_INDEX_SETTING,
        LifecycleSettings.LIFECYCLE_NAME_SETTING,
        LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE_SETTING,
        LifecycleSettings.LIFECYCLE_ORIGINATION_DATE_SETTING,
        LifecycleSettings.LIFECYCLE_PARSE_ORIGINATION_DATE_SETTING,
        LifecycleSettings.LIFECYCLE_ROLLOVER_ALIAS_SETTING,
        FrozenIndicesSettings.INDEX_FROZEN_SETTING,
        WatcherSettings.INDEX_WATCHER_TEMPLATE_VERSION_SETTING
    );
}
