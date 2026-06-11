/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;

import java.util.List;

/**
 * Cluster settings for ESQL external source caching.
 * Cache size and TTL values are restart-only (NodeScope). The enabled flag is dynamic.
 */
public final class ExternalSourceCacheSettings {

    private ExternalSourceCacheSettings() {}

    public static final Setting<ByteSizeValue> CACHE_SIZE = Setting.memorySizeSetting(
        "esql.source.cache.size",
        "0.4%",
        Setting.Property.NodeScope
    );

    public static final Setting<Boolean> CACHE_ENABLED = Setting.boolSetting(
        "esql.source.cache.enabled",
        true,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<TimeValue> SCHEMA_TTL = Setting.positiveTimeSetting(
        "esql.source.cache.schema.ttl",
        TimeValue.timeValueMinutes(5),
        Setting.Property.NodeScope
    );

    public static final Setting<TimeValue> LISTING_TTL = Setting.positiveTimeSetting(
        "esql.source.cache.listing.ttl",
        TimeValue.timeValueSeconds(30),
        Setting.Property.NodeScope
    );

    /**
     * Canonical stripe size for row-format external-source statistics, in decompressed-stream bytes.
     * A file's stripe grid is {@code [k*B, (k+1)*B)} realigned forward to record boundaries; stats are
     * captured, deduplicated, and cached per stripe (see {@code ExternalSourceCacheService}). The value
     * participates in stripe identity, so it is restart-only and cluster-uniform: changing it simply
     * makes previously cached stripe entries unmatchable (a clean invalidation, never a mixed grid).
     * It deliberately does NOT influence how reads are partitioned or scheduled — chunk dispatch,
     * macro-splits, and parallelism are unaffected; segmentators only add cut points at stripe
     * boundaries so per-chunk stats nest within exactly one stripe.
     */
    public static final Setting<ByteSizeValue> STRIPE_SIZE = Setting.byteSizeSetting(
        "esql.source.cache.stripe.size",
        ByteSizeValue.ofMb(8),
        ByteSizeValue.ofKb(64),
        ByteSizeValue.ofGb(1),
        Setting.Property.NodeScope
    );

    public static List<Setting<?>> settings() {
        return List.of(CACHE_SIZE, CACHE_ENABLED, SCHEMA_TTL, LISTING_TTL, STRIPE_SIZE);
    }
}
