/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.shard;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Container for cluster settings related to {@link IndexingStats}.
 */
public class IndexingStatsSettings {

    static final TimeValue RECENT_WRITE_LOAD_HALF_LIFE_DEFAULT = TimeValue.timeValueMinutes(5); // Aligns with the interval between DSL runs
    static final TimeValue RECENT_WRITE_LOAD_HALF_LIFE_MIN = TimeValue.timeValueSeconds(1); // A sub-second half-life makes no sense
    static final TimeValue RECENT_WRITE_LOAD_HALF_LIFE_MAX = TimeValue.timeValueDays(100_000); // Long.MAX_VALUE nanos, rounded down

    /**
     * A cluster setting giving the half-life, in seconds, to use for the Exponentially Weighted Moving Rate calculation used for the
     * recency-weighted write load returned by {@link IndexingStats.Stats#getRecentWriteLoad()}.
     *
     * <p>This is dynamic, but changes only apply to newly-opened shards.
     */
    public static final Setting<TimeValue> RECENT_WRITE_LOAD_HALF_LIFE_SETTING = Setting.timeSetting(
        "indices.stats.recent_write_load.half_life",
        RECENT_WRITE_LOAD_HALF_LIFE_DEFAULT,
        RECENT_WRITE_LOAD_HALF_LIFE_MIN,
        RECENT_WRITE_LOAD_HALF_LIFE_MAX,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private final AtomicReference<TimeValue> recentWriteLoadHalfLifeForNewShards = new AtomicReference<>(
        RECENT_WRITE_LOAD_HALF_LIFE_SETTING.getDefault(Settings.EMPTY)
    );

    public IndexingStatsSettings(ClusterSettings clusterSettings) {
        clusterSettings.initializeAndWatch(RECENT_WRITE_LOAD_HALF_LIFE_SETTING, recentWriteLoadHalfLifeForNewShards::set);
    }

    TimeValue getRecentWriteLoadHalfLifeForNewShards() {
        return recentWriteLoadHalfLifeForNewShards.get();
    }
}
