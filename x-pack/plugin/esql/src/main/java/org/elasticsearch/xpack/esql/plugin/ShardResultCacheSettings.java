/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;

import java.util.List;

/**
 * Node-level knobs for the ES|QL shard result cache. The cache itself is {@link org.elasticsearch.indices.IndicesRequestCache},
 * so its size, expiry, cleanup interval, per-index enablement and {@code _cache/clear?request=true} handling are the
 * existing {@code indices.requests.cache.*} / {@code index.requests.cache.enable} settings. Only the ES|QL-specific
 * kill switch and admission thresholds live here.
 * <p>
 * Values are read per request on the data node so that all three are effective as dynamic cluster settings.
 */
public final class ShardResultCacheSettings {

    /**
     * Master switch. On by default; set to {@code false} to disable all probing and storing without restarting.
     */
    public static final Setting<Boolean> ENABLED = Setting.boolSetting(
        "esql.shard_result_cache.enabled",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Hard cap on the serialized size of one shard's result. Every ES|QL query is row-bounded, but a bounded row count
     * is not a bounded byte count, so bytes get their own limit. A shard whose result exceeds this is computed
     * normally and simply not stored.
     */
    public static final Setting<ByteSizeValue> MAX_VALUE_SIZE = Setting.byteSizeSetting(
        "esql.shard_result_cache.max_value_size",
        ByteSizeValue.of(1, ByteSizeUnit.MB),
        ByteSizeValue.ZERO,
        ByteSizeValue.of(Integer.MAX_VALUE, ByteSizeUnit.BYTES),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * The churn gate: a shard is only admitted when it has not been written to for at least this long. Invalidation is
     * reader-close driven and readers only turn over when there is something new to expose, so recent writes predict
     * entries that die before they are read again. Set to zero to admit every shard, which is what a test wanting a
     * deterministic hit wants.
     */
    public static final Setting<TimeValue> MIN_SHARD_IDLE_TIME = Setting.timeSetting(
        "esql.shard_result_cache.min_shard_idle_time",
        TimeValue.timeValueSeconds(30),
        TimeValue.ZERO,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /** Registered by {@link EsqlPlugin#getSettings()}. */
    public static List<Setting<?>> settings() {
        return List.of(ENABLED, MAX_VALUE_SIZE, MIN_SHARD_IDLE_TIME);
    }

    private final boolean enabled;
    private final long maxValueSizeInBytes;
    private final long minShardIdleTimeNanos;

    ShardResultCacheSettings(ClusterSettings clusterSettings) {
        this(
            clusterSettings.get(ENABLED),
            clusterSettings.get(MAX_VALUE_SIZE).getBytes(),
            clusterSettings.get(MIN_SHARD_IDLE_TIME).nanos()
        );
    }

    ShardResultCacheSettings(boolean enabled, long maxValueSizeInBytes, long minShardIdleTimeNanos) {
        this.enabled = enabled;
        this.maxValueSizeInBytes = maxValueSizeInBytes;
        this.minShardIdleTimeNanos = minShardIdleTimeNanos;
    }

    boolean enabled() {
        return enabled;
    }

    long maxValueSizeInBytes() {
        return maxValueSizeInBytes;
    }

    long minShardIdleTimeNanos() {
        return minShardIdleTimeNanos;
    }
}
