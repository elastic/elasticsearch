/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import org.elasticsearch.xpack.esql.datasources.SourceStatisticsSerializer;
import org.elasticsearch.xpack.esql.datasources.SplitStats;
import org.elasticsearch.xpack.esql.datasources.spi.StripeStatsSink;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * One staged stripe, held in the compact form it was folded in and presented as the flat {@code _stats.*} map
 * a contribution is published as. A read stages one per stripe and they live until the query ends, so the flat
 * form -- some four hundred string-keyed entries over a few arrays -- is built on demand instead of kept.
 * <p>
 * A walk builds and discards, because a writer is walking the whole pile and one record kept flat would put it
 * back. A lookup builds and keeps, because hundreds more follow it. A membership test of an always-present key
 * builds nothing; see {@link #containsKey}.
 * <p>
 * Entries and size both come from the statistics, never from a schema: a stream writes the size as a length
 * prefix ahead of the entries, so two sources would let the far side read the following bytes as more entries.
 * Equality is the flat map's; iteration order is not, and nothing reads it.
 */
final class StagedStripeRecord extends AbstractMap<String, Object> {

    /**
     * The flat keys every record carries whatever its statistics, so {@link #containsKey} can answer them and
     * {@link #size} can count them without building anything.
     * <p>
     * This set and what {@link #flatten} writes unconditionally must stay in step. A key flatten always writes
     * and this omits makes the length prefix one short and the reader stop an entry early; one here that
     * flatten does not always write over-counts it. A swap keeps the size the same and so passes every count,
     * which is why membership is held key by key and separately: {@code StripeStatsStagingTests}'
     * {@code testTheSizeAgreesWithTheBuiltMap} and {@code testMembershipAgreesWithTheBuiltMap}.
     */
    private static final Set<String> ALWAYS_PRESENT_KEYS = Set.of(
        ExternalStats.MTIME_MILLIS_KEY,
        ExternalStats.CONFIG_FINGERPRINT_KEY,
        ExternalStats.PARTIAL_CHUNK_KEY,
        ExternalStats.STRIPE_SIZE_KEY,
        ExternalStats.STRIPE_ORDINAL_KEY,
        ExternalStats.COVERAGE_START_KEY,
        ExternalStats.COVERAGE_END_KEY,
        ExternalStats.STRIPE_AT_START_KEY,
        ExternalStats.STRIPE_AT_END_KEY,
        ExternalStats.COVERAGE_IS_LAST_KEY,
        SourceStatisticsSerializer.STATS_ROW_COUNT
    );

    /** Flat keys every harvested column carries whatever its values: its null count and its value count. */
    private static final int COUNT_KEYS_PER_COLUMN = 2;

    private final StripeStatsSink.ReadIdentity identity;
    private final long ordinal;
    private final long start;
    private final long end;
    private final boolean atStripeStart;
    private final boolean atStripeEnd;
    private final boolean eof;
    private final SplitStats stats;

    /** The flat form a lookup built and kept; {@code null} until one does. See the class note on the two consumers. */
    private Map<String, Object> flat;

    /**
     * How many times this record has built its flat form. Not read on any production path: it exists because
     * "builds nothing" and "keeps nothing" are different claims and {@link #keepsFlatForm} can only see the
     * second — a walk builds and discards, so it reports {@code false} either way.
     */
    private int builds;

    StagedStripeRecord(
        StripeStatsSink.ReadIdentity identity,
        long ordinal,
        long start,
        long end,
        boolean atStripeStart,
        boolean atStripeEnd,
        boolean eof,
        SplitStats stats
    ) {
        this.identity = identity;
        this.ordinal = ordinal;
        this.start = start;
        this.end = end;
        this.atStripeStart = atStripeStart;
        this.atStripeEnd = atStripeEnd;
        this.eof = eof;
        this.stats = stats;
    }

    @Override
    public Set<Entry<String, Object>> entrySet() {
        return walk().entrySet();
    }

    /**
     * Counted from the compact form, so writing this record costs one build rather than two: a stream asks the
     * size and then the entries. Both counts read the same statistics.
     */
    @Override
    public int size() {
        Map<String, Object> kept = flat;
        if (kept != null) {
            return kept.size();
        }
        int size = ALWAYS_PRESENT_KEYS.size();
        if (identity.readConfig() != null && identity.readConfig().isEmpty() == false) {
            size++;
        }
        if (identity.rowCountReadConfigIndependent()) {
            size++;
        }
        for (int i = 0; i < stats.columnCount(); i++) {
            size += COUNT_KEYS_PER_COLUMN;
            if (stats.min(i) != null) {
                size++;
            }
            if (stats.max(i) != null) {
                size++;
            }
        }
        return size;
    }

    @Override
    public boolean isEmpty() {
        return false; // a stripe record always carries its addressing keys
    }

    @Override
    public Object get(Object key) {
        return look().get(key);
    }

    /**
     * Answers the always-present keys from the key set, building nothing. Asking whether a key is there is not
     * evidence its value is about to be read: {@code AsyncExternalSourceOperator.Status.stripesCommitted} probes
     * {@link ExternalStats#STRIPE_ORDINAL_KEY} and {@link ExternalStats#STRIPE_SIZE_KEY} on every record of
     * every file from {@code toXContent} and reads neither, so building here would cost one map per staged
     * record on a {@code GET _tasks?detailed}. Any other key walks.
     */
    @Override
    public boolean containsKey(Object key) {
        if (key instanceof String name && ALWAYS_PRESENT_KEYS.contains(name)) {
            return true;
        }
        return walk().containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return walk().containsValue(value);
    }

    /** The flat form for a consumer that walks the whole map once, keeping it only if a lookup already has. */
    private Map<String, Object> walk() {
        Map<String, Object> kept = flat;
        return kept != null ? kept : flatten();
    }

    /** The flat form for a consumer that looks keys up, kept because it is about to look up many more. */
    private Map<String, Object> look() {
        Map<String, Object> kept = flat;
        if (kept == null) {
            kept = flatten();
            flat = kept;
        }
        return kept;
    }

    /**
     * Unmodifiable, so a write through this map fails rather than being dropped by the next rebuild.
     * <p>
     * Every harvested column is written even when it holds nothing but zeros, and that part is load-bearing.
     * "Harvested, every row null" and "never harvested" are different answers and the reconciler tells them
     * apart by whether the column is present at all -- {@code ExternalSourceAggregatePushdown.columnStatUnservable}
     * makes an absent column unservable for text formats, so the first serves a warm {@code COUNT} and the
     * second must scan. Dropping an all-null column turns the first into the second; writing a column never
     * harvested turns the second into the first, which is a wrong answer rather than a missed one.
     * <p>
     * A column whose extremum a fold found unservable is written with its counts and no extremum, which is the
     * only encoding a staged contribution has had: the reconciler reads a column that is present, carries
     * non-null rows and offers no extremum as one it cannot answer, and poisons it as the marker would.
     */
    private Map<String, Object> flatten() {
        builds++;
        Map<String, ExternalStats.ColumnStats> columns = new HashMap<>();
        for (int i = 0; i < stats.columnCount(); i++) {
            columns.put(
                stats.columnName(i),
                new ExternalStats.ColumnStats(stats.nullCount(i), stats.valueCount(i), stats.min(i), stats.max(i))
            );
        }
        Map<String, Object> built = StripeStatsHarvester.flatten(
            identity,
            ordinal,
            start,
            end,
            atStripeStart,
            atStripeEnd,
            eof,
            TextFormatStats.harvested(stats.rowCount(), columns)
        );
        int counted = size();
        if (counted != built.size()) {
            throw new IllegalStateException(
                "a staged record counts its entries and writes them from the same statistics, so the two can only "
                    + "disagree if the counting rule and the key-writing rule have drifted apart -- and the wire "
                    + "prefix is written from size(), which would make the reader take the bytes after this record "
                    + "as further entries of it: counted "
                    + counted
                    + ", wrote "
                    + built.size()
            );
        }
        return Collections.unmodifiableMap(built);
    }

    /** Whether a lookup has made this record keep its flat form. The contract a walk must not break. */
    boolean keepsFlatForm() {
        return flat != null;
    }

    /** How many flat forms this record has built. The contract a membership test of an always-present key must not break. */
    int buildCount() {
        return builds;
    }
}
