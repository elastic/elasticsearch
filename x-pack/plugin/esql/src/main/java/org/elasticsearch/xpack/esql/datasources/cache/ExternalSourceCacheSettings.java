/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.MemorySizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceSettings;
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

    /**
     * Deprecated former key for {@link #CACHE_SIZE}, from before the external-dataset settings were unified
     * under {@code esql.external.*}. It shipped in released versions, so it stays registered — a node
     * carrying it in {@code elasticsearch.yml} would otherwise fail startup on an unregistered setting. It
     * emits a deprecation warning when set and is the fallback {@link #CACHE_SIZE} resolves through.
     */
    public static final Setting<ByteSizeValue> CACHE_SIZE_OLD = Setting.memorySizeSetting(
        "esql.source.cache.size",
        "0.4%",
        Setting.Property.DeprecatedWarning,
        Setting.Property.NodeScope
    );

    public static final Setting<ByteSizeValue> CACHE_SIZE = Setting.memorySizeSetting(
        "esql.external.cache.size",
        CACHE_SIZE_OLD,
        Setting.Property.NodeScope
    );

    /**
     * Deprecated former key for {@link #CACHE_ENABLED} — see {@link #CACHE_SIZE_OLD} for why it stays
     * registered. Deliberately NOT Dynamic: no consumer observes this key (the live consumer in
     * {@code EsqlPlugin.createComponents} is wired to {@link #CACHE_ENABLED}), so a dynamic update through
     * it is rejected with a clear error, pointing the operator at the new key, instead of being silently
     * ignored. Set in {@code elasticsearch.yml}, it still takes effect through the fallback resolution.
     */
    public static final Setting<Boolean> CACHE_ENABLED_OLD = Setting.boolSetting(
        "esql.source.cache.enabled",
        true,
        Setting.Property.DeprecatedWarning,
        Setting.Property.NodeScope
    );

    public static final Setting<Boolean> CACHE_ENABLED = Setting.boolSetting(
        "esql.external.cache.enabled",
        CACHE_ENABLED_OLD,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Deprecated no-op. The schema (per-file) and dataset-aggregate caches are invalidated by identity
     * (mtime / file-set fingerprint in the key) and bounded by CACHE_SIZE + LRU, never by a clock — see
     * {@link ExternalSourceCacheService}. This setting formerly capped the schema cache with a hard TTL;
     * it is retained, registered, and ignored so a node that carries it in {@code elasticsearch.yml} from
     * an earlier version still starts (removing a released node setting would fail startup). It is wired to
     * nothing and emits a deprecation warning when set.
     */
    public static final Setting<TimeValue> SCHEMA_TTL = Setting.positiveTimeSetting(
        "esql.source.cache.schema.ttl",
        TimeValue.timeValueMinutes(5),
        Setting.Property.DeprecatedWarning,
        Setting.Property.NodeScope
    );

    /**
     * Deprecated former key for {@link #LISTING_TTL} — see {@link #CACHE_SIZE_OLD} for why it stays
     * registered.
     */
    public static final Setting<TimeValue> LISTING_TTL_OLD = Setting.positiveTimeSetting(
        "esql.source.cache.listing.ttl",
        TimeValue.timeValueSeconds(30),
        Setting.Property.DeprecatedWarning,
        Setting.Property.NodeScope
    );

    // Only the listing cache carries a time-based refresh: it discovers file identity and has no per-file
    // key to invalidate on. The schema and dataset-aggregate caches invalidate by identity, not by a clock.
    public static final Setting<TimeValue> LISTING_TTL = Setting.positiveTimeSetting(
        "esql.external.cache.listing.ttl",
        LISTING_TTL_OLD,
        TimeValue.timeValueMillis(0),
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
        "esql.external.cache.stripe.size",
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
        "esql.external.cache.stripe.columns",
        StripeColumnScope.PROJECTED,
        Setting.Property.NodeScope
    );

    /**
     * Rejects a non-positive footer cache budget at settings-parse time. Both footer caches are
     * built lazily, when a format reader is first constructed, so without this the node would
     * accept {@code 0b}/{@code 0%} at startup and only fail on the first Parquet or ORC query.
     */
    private static final Setting.Validator<ByteSizeValue> FOOTER_CACHE_BUDGET_VALIDATOR = value -> {
        if (value.getBytes() <= 0) {
            throw new IllegalArgumentException("footer cache budget must be greater than 0, got [" + value + "]");
        }
    };

    /**
     * Byte budget for a columnar format reader's footer-byte cache ({@link FooterByteCache}): raw
     * footer/tail bytes reused across the resolution, split-discovery, and execution phases of a
     * query, and across back-to-back queries within {@link #FOOTER_CACHE_TTL}. The budget is per
     * reader instance (the Parquet and ORC readers each own one cache), so the node-wide worst
     * case is twice this value; absolute values are accepted.
     * <p>
     * The default is sized so that a single query's whole file set fits: one query can discover at
     * most {@link ExternalSourceSettings#MAX_DISCOVERED_FILES} files, and 0.5% of an 8 GB heap is
     * ~41 MiB, which holds that many footers as long as they average under ~4 KiB.
     * <p>
     * Note that only files too large to be fetched whole reach this cache. A file that fits in the
     * format reader's sliding window is filled by one whole-file read, which is deliberately not
     * stored here so that file bodies cannot displace genuine footers.
     */
    public static final Setting<ByteSizeValue> FOOTER_CACHE_SIZE = new Setting<>(
        "esql.external.cache.footer.size",
        "0.5%",
        s -> MemorySizeValue.parseBytesSizeValueOrHeapRatio(s, "esql.external.cache.footer.size"),
        FOOTER_CACHE_BUDGET_VALIDATOR,
        Setting.Property.NodeScope
    );

    /**
     * Byte budget for a columnar format reader's parsed-footer cache ({@link ParsedFooterCache}):
     * deserialized footer structures (Parquet {@code ParquetMetadata}, ORC {@code OrcTail}),
     * weighed by a per-format structural estimate. Like {@link #FOOTER_CACHE_SIZE} the budget is
     * per reader instance, so the node-wide worst case is twice this value.
     * <p>
     * A parsed footer costs several times its serialized form and scales with column count rather
     * than file size (at Parquet's estimate a single-row-group file weighs ~13 KiB at 15 columns
     * and ~55 KiB at 100), and files whose writer embeds a schema document in the footer's
     * key-value metadata (Spark, Iceberg and pandas all do) weigh more again. Doubling
     * {@link #FOOTER_CACHE_SIZE}'s ratio does not usually offset that, so for typical schemas this
     * cache holds fewer entries than the byte cache and is the one that evicts first.
     * <p>
     * That ordering is worth having rather than a shortcoming, because it keeps the common failure
     * mode cheap: when a footer is dropped here the raw bytes are often still cached, so the next
     * phase pays a re-parse (CPU) instead of a fresh network round trip. It is a tendency, not a
     * guarantee. Two cases invert it. A file small enough to be fetched whole (see the sliding
     * window in the format reader's storage adapter) is never stored as a footer entry at all, so
     * this cache is the only one holding anything for it. And a file carrying large per-column
     * statistics can have a serialized footer bigger than the structural estimate charged here.
     * In both, an eviction from this cache does cost a re-read. Deployments running wide schemas
     * over large file sets can raise this to buy back the re-parse; the cost is heap held for the
     * whole {@link #FOOTER_CACHE_TTL}.
     */
    public static final Setting<ByteSizeValue> FOOTER_PARSED_CACHE_SIZE = new Setting<>(
        "esql.external.cache.footer.parsed.size",
        "1%",
        s -> MemorySizeValue.parseBytesSizeValueOrHeapRatio(s, "esql.external.cache.footer.parsed.size"),
        FOOTER_CACHE_BUDGET_VALIDATOR,
        Setting.Property.NodeScope
    );

    /**
     * Expire-after-access TTL shared by both footer caches. If the bytes are stale, the parse
     * derived from them is stale too. Must bridge the gaps between resolution, split discovery,
     * and execution of one query over a large file set, plus dashboard refresh intervals. The
     * trade-off: footer cache keys are {@code (path, fileLength)} without mtime (adding it would
     * cost a HEAD request per range split; see {@link FooterByteCache}), so a file overwritten
     * in place with identical length can be served stale for up to this long. Object-store
     * analytics layouts treat data files as immutable, and this setting is the operator escape
     * hatch where they do not.
     */
    public static final Setting<TimeValue> FOOTER_CACHE_TTL = Setting.positiveTimeSetting(
        "esql.external.cache.footer.ttl",
        TimeValue.timeValueMinutes(5),
        Setting.Property.NodeScope
    );

    public static List<Setting<?>> settings() {
        return List.of(
            CACHE_SIZE,
            CACHE_SIZE_OLD,
            CACHE_ENABLED,
            CACHE_ENABLED_OLD,
            SCHEMA_TTL,
            LISTING_TTL,
            LISTING_TTL_OLD,
            STRIPE_SIZE,
            STRIPE_COLUMNS,
            FOOTER_CACHE_SIZE,
            FOOTER_PARSED_CACHE_SIZE,
            FOOTER_CACHE_TTL
        );
    }
}
