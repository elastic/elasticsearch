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
import org.elasticsearch.xpack.esql.datasources.spi.StripeColumnScope;

import java.util.List;

/**
 * Cluster settings for ESQL external source caching.
 * Everything here is restart-only (NodeScope) except the enabled flag, which is dynamic and wired to a
 * live consumer in {@code EsqlPlugin.createComponents}. A setting must not be declared Dynamic unless a
 * {@code ClusterSettings.addSettingsUpdateConsumer} actually observes updates — a Dynamic flag without a
 * consumer accepts runtime updates and silently ignores them.
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
     * Canonical stripe size for row-format external-source statistics, in file/decompressed-stream
     * bytes. A stripe is a pure ADDRESSING grid over file content: the reader attributes each record to
     * stripe {@code floor(recordStartOffset / B)} as it parses, and stats are captured, deduplicated,
     * and cached per stripe (see {@code ExternalSourceCacheService}). It is orthogonal to partitioning
     * — chunk dispatch, macro-splits, and parallelism are unaffected; the grid only determines which
     * stripe a record's stats land in. The value participates in stripe identity, so it is restart-only
     * and cluster-uniform: changing it simply makes previously cached stripe entries unmatchable (a
     * clean invalidation, never a mixed grid).
     * <p>
     * Default 8 MB, derived (not arbitrary) from the ClickBench text-format file-size distribution
     * against the schema-cache budget: a representative ~1.8 GB shard yields ~231 stripes (ample
     * pruning resolution), and a 500-hot-file working set consumes ~11 MB — 42% of the ~26 MB schema
     * budget on a 32 GB heap. Smaller grids (≤1 MB) overflow the budget on realistic working sets;
     * larger grids (≥32 MB) coarsen a representative shard to &lt;60 stripes, blunting per-stripe min/max
     * pruning. 8 MB is the knee.
     */
    public static final Setting<ByteSizeValue> STRIPE_SIZE = Setting.byteSizeSetting(
        "esql.source.cache.stripe.size",
        ByteSizeValue.ofMb(8),
        ByteSizeValue.ofKb(64),
        ByteSizeValue.ofGb(1),
        Setting.Property.NodeScope
    );

    /**
     * How much per-stripe statistics a row-format external read harvests while it scans. Orthogonal to
     * {@link #STRIPE_SIZE}: the grid size decides which stripe a record lands in, this scope decides what
     * is summarised per stripe. Modes ({@link StripeColumnScope}):
     * <ul>
     *   <li>{@code none} — harvest nothing; a warm aggregate always re-scans.</li>
     *   <li>{@code count} — per-stripe row count only (no per-column min/max/null). Enough to serve a warm
     *       {@code COUNT(*)}.</li>
     *   <li>{@code projected} — row count plus min/max/null for the query's projected columns. The default.</li>
     *   <li>{@code all} — row count plus min/max/null for every column in the file's schema (the reader
     *       materialises the unprojected columns for the stats pass).</li>
     * </ul>
     * Row count is harvested in {@code count}, {@code projected}, and {@code all} — everything except
     * {@code none}. This is what lets a {@code COUNT(*)} read (zero projected columns) still record each
     * stripe's row count.
     * <p>
     * Unlike {@link #STRIPE_SIZE}, this does NOT participate in stripe identity — it only changes how much
     * a fresh scan harvests, never which stripe a record belongs to — so it COULD safely become dynamic.
     * It is restart-only today because its one consumer ({@code FileSourceFactory}) reads it from the
     * node's startup {@code Settings}; declaring it Dynamic without a live settings consumer would accept
     * a runtime update and silently never observe it.
     */
    public static final Setting<StripeColumnScope> STRIPE_COLUMNS = Setting.enumSetting(
        StripeColumnScope.class,
        "esql.source.cache.stripe.columns",
        StripeColumnScope.PROJECTED,
        Setting.Property.NodeScope
    );

    public static List<Setting<?>> settings() {
        return List.of(CACHE_SIZE, CACHE_ENABLED, SCHEMA_TTL, LISTING_TTL, STRIPE_SIZE, STRIPE_COLUMNS);
    }
}
