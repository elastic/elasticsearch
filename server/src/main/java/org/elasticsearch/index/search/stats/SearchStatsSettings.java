/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.search.stats;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Container for cluster settings
 */
public class SearchStatsSettings {

    public static final TimeValue RECENT_READ_LOAD_HALF_LIFE_DEFAULT = TimeValue.timeValueMinutes(5);
    static final TimeValue RECENT_READ_LOAD_HALF_LIFE_MIN = TimeValue.timeValueSeconds(1); // A sub-second half-life makes no sense
    static final TimeValue RECENT_READ_LOAD_HALF_LIFE_MAX = TimeValue.timeValueDays(100_000); // Long.MAX_VALUE nanos, rounded down

    /**
     * A cluster setting giving the half-life, in seconds, to use for the Exponentially Weighted Moving Rate calculation used for the
     * recency-weighted read load
     *
     * <p>This is dynamic, but changes only apply to newly-opened shards.
     */
    public static final Setting<TimeValue> RECENT_READ_LOAD_HALF_LIFE_SETTING = Setting.timeSetting(
        "indices.stats.recent_read_load.half_life",
        RECENT_READ_LOAD_HALF_LIFE_DEFAULT,
        RECENT_READ_LOAD_HALF_LIFE_MIN,
        RECENT_READ_LOAD_HALF_LIFE_MAX,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private TimeValue recentReadLoadHalfLifeForNewShards = RECENT_READ_LOAD_HALF_LIFE_SETTING.getDefault(Settings.EMPTY);


    public SearchStatsSettings(ClusterSettings clusterSettings) {
        clusterSettings.initializeAndWatch(RECENT_READ_LOAD_HALF_LIFE_SETTING,value -> recentReadLoadHalfLifeForNewShards = value);
    }

    public TimeValue getRecentReadLoadHalfLifeForNewShards() {
        return recentReadLoadHalfLifeForNewShards;
    }
}
