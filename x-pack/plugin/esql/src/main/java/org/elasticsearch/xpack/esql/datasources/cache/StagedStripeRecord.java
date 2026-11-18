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
 * One staged stripe, held in the compact typed form it was folded in and presented as the flat
 * {@code _stats.*} map every consumer of a contribution expects.
 * <p>
 * A read stages one of these per stripe and they live until the query ends, so what they hold is what the read
 * costs: the compact form of a 105-column stripe is a few arrays, the column extrema, and its column names —
 * which it mostly does not own. {@link SplitStats#fold} returns the dictionary's own string for a single-segment
 * name, so an ordinary column's name is one String shared by the record, its pieces and the schema it was split
 * from; only a DOTTED name is rejoined from its segments, and a record carries its own copy of those. The flat map
 * over the same values is some four hundred string-keyed entries, and each of those keys is another String
 * carrying the column name again, so what the compact form saves is the entries and their keys rather than the
 * names. The flat map is built when someone asks for it and the compact form is what is kept.
 * <p>
 * Building it on demand divides the consumers in three. A WRITER walks the map
 * once — {@code writeGenericMap} asks the size and then the entries — and wants nothing left behind, because it
 * is walking every staged record in turn and a record kept flat would put the whole pile back. A READER asks for
 * one key and then asks for hundreds more, one per column statistic, and wants the map it is reading to stay.
 * A MEMBERSHIP TEST wants neither: see {@link #containsKey}, which answers the keys every record carries from
 * the compact form and builds nothing at all. So a walk builds and discards, a lookup builds and keeps, a
 * membership test of an always-present key builds nothing, and once a lookup has kept one the walk uses it.
 * <p>
 * Equality and hashing are the flat map's, so a staged record carries the same entries as the flat map a chunk
 * publishes directly for the same stripe and compares equal to it — that is the point: this changes what a
 * contribution costs in memory, never what it says. Iteration order is NOT part of that and is deliberately not
 * preserved: the entries follow the statistics rather than the schema, so the same entries may serialize in a
 * different order. Nothing reads that order — {@code StreamInput.readGenericMap} rebuilds a {@code HashMap} on
 * the far side.
 * <p>
 * The kept map is not published under a lock. A race between two readers builds it twice and one wins, which
 * costs a rebuild and nothing else, because both are equal and every value in them is immutable.
 */
final class StagedStripeRecord extends AbstractMap<String, Object> {

    /**
     * The flat keys every stripe record carries whatever its statistics: the eight addressing keys, the file
     * version, the config fingerprint, and the row count. {@link #flatten} writes each of them unconditionally,
     * which is what lets {@link #containsKey} answer them without building anything, and what lets {@link #size}
     * count them without building anything. The read configuration and the count licence are written only when
     * they are set, so {@code size} adds those separately.
     * <p>
     * A key added to {@code flatten} unconditionally belongs here too, and leaving one out is as dangerous as
     * adding one that does not belong. {@code size} starts from this set's own size, so a key {@code flatten}
     * always writes but this set omits makes the length prefix one SHORT and the reader stops an entry early;
     * one added here that {@code flatten} does not always write makes {@code containsKey} claim a key the
     * entries do not carry, and {@code size} over-count the entries a stream then writes. Both
     * are held against the built map, and by separate tests, because a swap keeps this set the same size and so
     * passes every count: {@code StripeStatsStagingTests#testTheSizeAgreesWithTheBuiltMap} holds the count, and
     * {@code #testMembershipAgreesWithTheBuiltMap} holds membership key by key over the whole vocabulary.
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
     * Counted from the compact form rather than from a built map, so writing this record costs ONE build: a
     * stream writes the size and then the entries, and building for each would double the allocation on the
     * path this class exists to make cheap. {@code StripeStatsStagingTests#testTheSizeAgreesWithTheBuiltMap}
     * holds the count equal to the built map's across every shape, because a length written from one count while
     * the entries come from another map is how a stream gets corrupted.
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
     * Membership neither keeps the flat form nor builds it, for the keys every record carries. Asking whether a
     * key is there is not evidence that its value is about to be read, and one caller proves it:
     * {@code AsyncExternalSourceOperator.Status.stripesCommitted} tests {@link ExternalStats#STRIPE_ORDINAL_KEY}
     * and {@link ExternalStats#STRIPE_SIZE_KEY} on every record of every file and reads neither, from
     * {@code toXContent} — so a membership test that built the flat map would have one
     * {@code GET _tasks?detailed} during a large text scan build it once per staged record, and one that kept
     * the map would pin every record for the rest of the query. Both keys are in
     * {@link #ALWAYS_PRESENT_KEYS}, so the answer comes from the compact form and the render costs two set
     * probes per record. Any other key walks, which builds a map and discards it.
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
     * Unmodifiable so a write through this map fails rather than being silently dropped by the next rebuild.
     * The columns come from the statistics, which is also where {@link #size} counts them, so the entries and
     * the length prefix written ahead of them cannot be drawn from two different column sets.
     * <p>
     * Every harvested column is written even when it holds nothing but zeros, and that is the load-bearing
     * part. "Harvested, and every row of it was null" and "never harvested" are different answers — the first
     * lets a warm {@code COUNT} be served, the second must send the query to a scan — and the reconciler tells
     * them apart by whether the column is in the record at all, which is what
     * {@code SplitStats#hasColumn} reports. Dropping an all-null column here would turn the first answer into
     * the second; writing a column that was never harvested would turn the second into the first, which is a
     * wrong answer rather than a missed one.
     * <p>
     * A column whose extremum a fold found unservable is written with its counts and WITHOUT an extremum,
     * which is the only encoding a staged contribution has ever had and is the conservative one: the
     * reconciler reads a column that is present, carries rows that are not all null and offers no extremum as
     * one it cannot answer, and poisons it exactly as the unservable marker would.
     * <p>
     * That the two conditions coincide is a property of the fold, not luck, and the step that carries it is
     * this: every route to unservable runs through a piece that HELD a value, so that piece's
     * {@code nullCount} is strictly below its own {@code rowCount}, and therefore the folded {@code nullCount}
     * is strictly below the folded {@code rowCount} — which is the condition {@code ColumnFold}'s
     * {@code else if (sp.nullCounts[i] != sp.rowCount)} tests on the way back in. An all-null column is the
     * only one that reaches the fold with no candidate extremum and no poison, and it is exactly the one whose
     * counts say so.
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
        assert size() == built.size()
            : "a staged record counts its entries and writes them from the same statistics, so the two can only "
                + "disagree if the counting rule and the key-writing rule have drifted apart -- and the wire "
                + "prefix is written from size(), which would make the reader take the bytes after this record "
                + "as further entries of it: counted "
                + size()
                + ", wrote "
                + built.size();
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
