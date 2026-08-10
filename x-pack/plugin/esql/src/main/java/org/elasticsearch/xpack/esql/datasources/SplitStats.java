/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Compact representation of per-split (row-group/stripe) statistics with path-segment
 * dictionary compression. Replaces raw {@code Map<String, Object>} for wire efficiency:
 * column names are stored as ordinal references into a shared segment dictionary, and
 * per-column stats use parallel arrays instead of string-keyed map entries.
 * <p>
 * Use {@link Builder} to construct instances from format reader metadata, and
 * {@link #toMap()} for backward-compatible access by consumers that expect the flat
 * {@code _stats.*} key convention from {@link SourceStatisticsSerializer}.
 * <p>
 * Instances are effectively immutable after construction and safe for use within a single
 * request processing thread.
 */
public final class SplitStats implements org.elasticsearch.xpack.esql.datasources.spi.SplitStats, Writeable {

    /**
     * Gates the per-column {@code valueCount} AND the per-column {@code minServable}/{@code maxServable} markers on
     * the wire. {@link SplitStats} is serialized under {@code FileSplit}'s {@code ESQL_SPLIT_STATS_COMPACT} gate;
     * these newer fields are read/written only when both peers support this version, otherwise {@code valueCount}
     * defaults to {@code -1} (COUNT(col) falls back to {@code rowCount - nullCount}) and the servability markers
     * default to {@code true} (servable) -- both the pre-marker behaviors. Reuses this PR's transport version: the
     * warm-aggregate profile, the {@code valueCount} wire addition, and these markers all ship together, so one
     * version marks all of them (a TV name may gate fields in more than one class).
     */
    static final TransportVersion ESQL_SPLIT_STATS_VALUE_COUNT = TransportVersion.fromName("esql_external_warm_aggregate_profile");

    /** Empty stats with zero rows and no columns. */
    public static final SplitStats EMPTY = new SplitStats(
        0,
        -1,
        new String[0],
        new int[0][],
        new long[0],
        new long[0],
        new Object[0],
        new Object[0],
        new long[0],
        new boolean[0],
        new boolean[0]
    );

    private final long rowCount;
    private final long sizeInBytes;
    private final String[] segments;
    private final int[][] columnSegmentOrdinals;
    private final long[] nullCounts;
    private final long[] valueCounts;
    private final Object[] mins;
    private final Object[] maxs;
    private final long[] sizesBytes;
    /**
     * Per-column servability of the extremum: {@code false} = the min/max was POISONED during a fold (incompatible
     * types / NaN across sub-units) and must NOT be served -- distinct from "no value" (an all-null or unharvested
     * column, where the value is simply absent). Carrying this explicitly makes {@link SplitStats} a lossless
     * superset of the flat {@code _stats.*} map: the {@code .min_unservable}/{@code .max_unservable} markers the
     * coordinator fold sets survive a round-trip through the compact form, instead of being silently dropped and
     * indistinguishable from "no value". Defaults to {@code true} (servable) everywhere the marker is absent.
     */
    private final boolean[] minServable;
    private final boolean[] maxServable;

    SplitStats(
        long rowCount,
        long sizeInBytes,
        String[] segments,
        int[][] columnSegmentOrdinals,
        long[] nullCounts,
        long[] valueCounts,
        Object[] mins,
        Object[] maxs,
        long[] sizesBytes,
        boolean[] minServable,
        boolean[] maxServable
    ) {
        this.rowCount = rowCount;
        this.sizeInBytes = sizeInBytes;
        this.segments = segments;
        this.columnSegmentOrdinals = columnSegmentOrdinals;
        this.nullCounts = nullCounts;
        this.valueCounts = valueCounts;
        this.mins = mins;
        this.maxs = maxs;
        this.sizesBytes = sizesBytes;
        this.minServable = minServable;
        this.maxServable = maxServable;
        assert servabilityConsistent() : "an unservable extremum must carry no value (minServable||min==null)";
    }

    /**
     * Invariant: a poisoned (unservable) extremum carries NO value. Load-bearing for BWC -- {@code writeTo} ships
     * the value whenever it is non-null, and an old peer strips the servability marker (defaults servable), so a
     * value present alongside an unservable bit would be served as real on the old peer. Enforced here so the
     * "poison ⟹ absent value" guarantee is fail-loud, not convention-only.
     */
    private boolean servabilityConsistent() {
        for (int i = 0; i < mins.length; i++) {
            if (minServable[i] == false && mins[i] != null) {
                return false;
            }
            if (maxServable[i] == false && maxs[i] != null) {
                return false;
            }
        }
        return true;
    }

    public SplitStats(StreamInput in) throws IOException {
        this.rowCount = in.readVLong();
        this.sizeInBytes = in.readZLong();
        int segCount = in.readVInt();
        this.segments = new String[segCount];
        for (int i = 0; i < segCount; i++) {
            segments[i] = in.readString();
        }
        int colCount = in.readVInt();
        this.columnSegmentOrdinals = new int[colCount][];
        this.nullCounts = new long[colCount];
        this.valueCounts = new long[colCount];
        this.mins = new Object[colCount];
        this.maxs = new Object[colCount];
        this.sizesBytes = new long[colCount];
        this.minServable = new boolean[colCount];
        this.maxServable = new boolean[colCount];
        boolean withValueCount = in.getTransportVersion().supports(ESQL_SPLIT_STATS_VALUE_COUNT);
        for (int i = 0; i < colCount; i++) {
            int segLen = in.readVInt();
            columnSegmentOrdinals[i] = new int[segLen];
            for (int j = 0; j < segLen; j++) {
                columnSegmentOrdinals[i][j] = in.readVInt();
            }
            nullCounts[i] = in.readZLong();
            if (withValueCount) {
                valueCounts[i] = in.readZLong();
                minServable[i] = in.readBoolean();
                maxServable[i] = in.readBoolean();
            } else {
                // Old peer: no value count, no servability markers -- a poisoned extremum arrived as an absent
                // value (the pre-marker behavior), so "servable" is the correct default (the value is what governs).
                valueCounts[i] = -1L;
                minServable[i] = true;
                maxServable[i] = true;
            }
            if (in.readBoolean()) {
                mins[i] = in.readGenericValue();
            }
            if (in.readBoolean()) {
                maxs[i] = in.readGenericValue();
            }
            sizesBytes[i] = in.readZLong();
        }
        assert servabilityConsistent() : "wire produced an unservable extremum carrying a value";
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(rowCount);
        out.writeZLong(sizeInBytes);
        out.writeVInt(segments.length);
        for (String seg : segments) {
            out.writeString(seg);
        }
        out.writeVInt(columnCount());
        boolean withValueCount = out.getTransportVersion().supports(ESQL_SPLIT_STATS_VALUE_COUNT);
        for (int i = 0; i < columnCount(); i++) {
            int[] segs = columnSegmentOrdinals[i];
            out.writeVInt(segs.length);
            for (int ord : segs) {
                out.writeVInt(ord);
            }
            out.writeZLong(nullCounts[i]);
            if (withValueCount) {
                out.writeZLong(valueCounts[i]);
                out.writeBoolean(minServable[i]);
                out.writeBoolean(maxServable[i]);
            }
            if (mins[i] != null) {
                out.writeBoolean(true);
                out.writeGenericValue(mins[i]);
            } else {
                out.writeBoolean(false);
            }
            if (maxs[i] != null) {
                out.writeBoolean(true);
                out.writeGenericValue(maxs[i]);
            } else {
                out.writeBoolean(false);
            }
            out.writeZLong(sizesBytes[i]);
        }
    }

    public int columnCount() {
        return columnSegmentOrdinals.length;
    }

    @Override
    public long rowCount() {
        return rowCount;
    }

    /** Returns size in bytes, or {@code -1} if unknown. */
    @Override
    public long sizeInBytes() {
        return sizeInBytes;
    }

    /**
     * Resolves the full dotted column name for the given ordinal by joining
     * the path segments from the dictionary. Each call allocates a new String;
     * callers that iterate over columns should prefer {@link #resolveColumnName(int, StringBuilder)}
     * to reuse a buffer.
     */
    public String columnName(int ordinal) {
        Objects.checkIndex(ordinal, columnCount());
        int[] segs = columnSegmentOrdinals[ordinal];
        if (segs.length == 1) {
            return segments[segs[0]];
        }
        StringBuilder sb = new StringBuilder();
        for (int j = 0; j < segs.length; j++) {
            if (j > 0) sb.append('.');
            sb.append(segments[segs[j]]);
        }
        return sb.toString();
    }

    /**
     * Resolves the full dotted column name into the given {@link StringBuilder}, which is
     * cleared before use. Avoids per-call String allocation when iterating over columns.
     */
    String resolveColumnName(int ordinal, StringBuilder sb) {
        Objects.checkIndex(ordinal, columnCount());
        sb.setLength(0);
        int[] segs = columnSegmentOrdinals[ordinal];
        for (int j = 0; j < segs.length; j++) {
            if (j > 0) sb.append('.');
            sb.append(segments[segs[j]]);
        }
        return sb.toString();
    }

    /** Returns null count for the column, or {@code -1} if unknown. */
    public long nullCount(int col) {
        return nullCounts[col];
    }

    /** Returns value count (non-null values, multivalue-aware) for the column, or {@code -1} if unknown. */
    public long valueCount(int col) {
        return valueCounts[col];
    }

    /** Returns the min value for the column, or {@code null} if unknown or poisoned (unservable). */
    @Nullable
    public Object min(int col) {
        return minServable[col] ? mins[col] : null;
    }

    /** Returns the max value for the column, or {@code null} if unknown or poisoned (unservable). */
    @Nullable
    public Object max(int col) {
        return maxServable[col] ? maxs[col] : null;
    }

    /** Returns size in bytes for the column, or {@code -1} if unknown. */
    public long sizeBytes(int col) {
        return sizesBytes[col];
    }

    /**
     * Finds a column ordinal by its full dotted name. Returns {@code -1} if not found.
     * Matches directly against the segment dictionary without materializing column name
     * strings, keeping the compact representation intact.
     */
    public int findColumn(String name) {
        int nameLen = name.length();
        for (int i = 0; i < columnCount(); i++) {
            int[] segs = columnSegmentOrdinals[i];
            int pos = 0;
            boolean match = true;
            for (int j = 0; j < segs.length; j++) {
                if (j > 0) {
                    if (pos >= nameLen || name.charAt(pos) != '.') {
                        match = false;
                        break;
                    }
                    pos++;
                }
                String seg = segments[segs[j]];
                int segLen = seg.length();
                if (pos + segLen > nameLen || name.regionMatches(pos, seg, 0, segLen) == false) {
                    match = false;
                    break;
                }
                pos += segLen;
            }
            if (match && pos == nameLen) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Returns the null count for the column with the given name under the SPI's "implicit nulls"
     * contract: a column physically absent from this split contributes {@code rowCount} implicit
     * nulls, since every row would deserialize as {@code null}. Format readers (Parquet, ORC)
     * emit at least one column-family stat key (e.g. {@code size_bytes}/{@code null_count}) for
     * every column they physically contain, so {@link #findColumn} returning {@code -1} is
     * equivalent to "column is not in this file/split".
     * <p>
     * Returns {@code -1} only in the rare case where the column is physically present but the
     * reader could not extract a null count (e.g. Parquet stats disabled at write time). Callers
     * such as {@link org.elasticsearch.xpack.esql.optimizer.rules.physical.local.PushStatsToExternalSource}
     * rely on this contract so that {@code Count(col) = rowCount - columnNullCount} is correct
     * across UNION_BY_NAME mixes where some files lack the column.
     */
    @Override
    public long columnNullCount(String name) {
        int col = findColumn(name);
        if (col < 0) {
            return rowCount;
        }
        return nullCounts[col];
    }

    /**
     * Returns the count of non-null values for the column with the given name (multivalue-aware:
     * a multivalued cell contributes one per value), or {@code -1} if the column is not present in
     * this split's statistics. {@code -1} signals "not available" so the caller falls back to
     * {@code rowCount - columnNullCount}.
     */
    @Override
    public long columnValueCount(String name) {
        int col = findColumn(name);
        return col >= 0 ? valueCounts[col] : -1;
    }

    /**
     * Returns {@code true} iff this split carries a column-family entry for {@code name}. A column is
     * present in the compact representation only when the reader emitted at least one stat for it (the
     * producer contract documented on {@link org.elasticsearch.xpack.esql.datasources.spi.SplitStats#columnNullCount}),
     * so this is exactly "the stats layer observed this column" — the predicate the optimizer uses to
     * tell a harvested-but-all-null text column apart from an unharvested one.
     */
    @Override
    public boolean hasColumn(String name) {
        return findColumn(name) >= 0;
    }

    /** Returns the min value for the column with the given name, or {@code null} if not found or poisoned (unservable). */
    @Override
    @Nullable
    public Object columnMin(String name) {
        int col = findColumn(name);
        return (col >= 0 && minServable[col]) ? mins[col] : null;
    }

    /** Returns the max value for the column with the given name, or {@code null} if not found or poisoned (unservable). */
    @Override
    @Nullable
    public Object columnMax(String name) {
        int col = findColumn(name);
        return (col >= 0 && maxServable[col]) ? maxs[col] : null;
    }

    /** Returns the size in bytes for the column with the given name, or {@code -1} if not found. */
    @Override
    public long columnSizeBytes(String name) {
        int col = findColumn(name);
        return col >= 0 ? sizesBytes[col] : -1;
    }

    /**
     * Merges two min stat values with cross-type numeric widening. Handles nulls: if one side
     * is null the other is returned. Returns {@code null} when both inputs are non-null but
     * incompatible (e.g. Long + Double) — callers should treat this as "unknown" and clear
     * the stat so it is not fed into stats-driven optimizations with a wrong value.
     * <p>
     * Covers {@link SchemaReconciliation#schemaWiden} cases at the stats-value level
     * (Integer+Long→Long, Integer+Double→Double), plus Parquet FLOAT vs DOUBLE which both
     * map to ESQL {@code DOUBLE} at the schema level but retain distinct Java stat types.
     * Long+Double and Long+Float are intentionally incompatible (lossy above 2^53).
     * <p>
     * DATETIME (epoch-millis) and DATE_NANOS (epoch-nanos) stats are both {@code Long} at the Java level, so
     * this same-class fast path merges them numerically. That is correct because per-file/per-split stats are
     * already reconciled to the query column's unit (millis rescaled to nanos, or poisoned on overflow) at the
     * SOURCE boundary by {@link SourceStatisticsSerializer#normalizeStatsToReconciled} — applied in
     * {@code ExternalSourceResolver.aggregateFileStatistics} for the whole-file fold and in
     * {@code FileSplitProvider} for footer splits — BEFORE any value reaches this merge. When
     * {@link SchemaReconciliation} widens a column that is {@code timestamp[ms]} in one file and
     * {@code timestamp[ns]} in another to DATE_NANOS, the millis file's extrema are rescaled ONCE at that
     * boundary. That boundary is the single owner of the unit axis: this method and {@link MergedSplitStats}
     * stay a pure value-fold and must NOT re-reconcile units — a second rescale here would double-apply it.
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Nullable
    static Object mergedMin(@Nullable Object existing, @Nullable Object incoming) {
        if (existing == null) return incoming;
        if (incoming == null) return existing;
        // A NaN operand is unmergeable: the runtime aggregator propagates NaN (Math.min(x,NaN)=NaN), but
        // Comparable.compareTo orders NaN as the largest double, which would silently drop it from a MIN.
        // Returning null poisons the fold so the column safe-misses and a full scan returns the correct NaN.
        if (isNaN(existing) || isNaN(incoming)) return null;
        if (existing.getClass() == incoming.getClass()) {
            return sameClassExtremum(existing, incoming, true);
        }
        return crossTypeExtremum(existing, incoming, true);
    }

    /** True for a {@code Double}/{@code Float} NaN; such a value cannot be folded as an extremum. */
    private static boolean isNaN(Object o) {
        return (o instanceof Double d && d.isNaN()) || (o instanceof Float f && f.isNaN());
    }

    /**
     * Picks the min ({@code wantMin}) or max of two same-class stat values. {@code String} keyword extrema are
     * ordered by UTF-8 bytes ({@link BytesRef} order) — the SAME order the runtime keyword {@code MIN}/{@code MAX}
     * aggregators use — NOT {@link String#compareTo}'s UTF-16 code-unit order, which disagrees whenever the
     * divergence point pairs a supplementary (astral) char against a BMP char in {@code [U+E000..U+FFFF]}
     * (e.g. an emoji vs the replacement char). Serving the UTF-16 winner would be a wrong warm answer vs a scan.
     * Any non-{@code Comparable} same-class pair returns {@code null} (safe-miss).
     */
    @Nullable
    private static Object sameClassExtremum(Object existing, Object incoming, boolean wantMin) {
        if (existing instanceof String) {
            int cmp = SourceStatisticsSerializer.compareKeywordUtf8(existing, incoming);
            return (wantMin ? cmp <= 0 : cmp >= 0) ? existing : incoming;
        }
        if (existing instanceof Comparable) {
            @SuppressWarnings({ "rawtypes", "unchecked" })
            int cmp = ((Comparable) existing).compareTo(incoming);
            return (wantMin ? cmp <= 0 : cmp >= 0) ? existing : incoming;
        }
        return null;
    }

    /**
     * Merges two max stat values with cross-type numeric widening. Same contract as
     * {@link #mergedMin} — returns {@code null} for incompatible non-null inputs.
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Nullable
    static Object mergedMax(@Nullable Object existing, @Nullable Object incoming) {
        if (existing == null) return incoming;
        if (incoming == null) return existing;
        if (isNaN(existing) || isNaN(incoming)) return null; // NaN is unmergeable — see mergedMin
        if (existing.getClass() == incoming.getClass()) {
            return sameClassExtremum(existing, incoming, false);
        }
        return crossTypeExtremum(existing, incoming, false);
    }

    /**
     * Selects the min ({@code selectMin=true}) or max of two values whose Java types differ,
     * promoting both to a common numeric type. Returns the winner in the widened type, or
     * {@code null} if the pair is incompatible. Zero-allocation beyond the return-value autobox.
     */
    @Nullable
    private static Object crossTypeExtremum(Object a, Object b, boolean selectMin) {
        if (a instanceof Integer ai && b instanceof Long bl) {
            long la = ai.longValue();
            return (selectMin ? la <= bl : la >= bl) ? la : bl;
        }
        if (a instanceof Long al && b instanceof Integer bi) {
            long lb = bi.longValue();
            return (selectMin ? al <= lb : al >= lb) ? al : lb;
        }
        // Long + Double/Float is intentionally incompatible (lossy above 2^53)
        if (a instanceof Long || b instanceof Long) {
            return null;
        }
        // Remaining pairs: Integer, Float, Double — all safely promotable to double
        if (a instanceof Number na && b instanceof Number nb) {
            double da = na.doubleValue(), db = nb.doubleValue();
            return (selectMin ? da <= db : da >= db) ? da : db;
        }
        return null;
    }

    /**
     * Produces a flat {@code Map<String, Object>} using the same {@code _stats.*} key
     * conventions as {@link SourceStatisticsSerializer}, for backward compatibility with
     * all existing consumers.
     */
    public Map<String, Object> toMap() {
        Map<String, Object> map = new HashMap<>();
        map.put(SourceStatisticsSerializer.STATS_ROW_COUNT, rowCount);
        if (sizeInBytes >= 0) {
            map.put(SourceStatisticsSerializer.STATS_SIZE_BYTES, sizeInBytes);
        }
        StringBuilder nameBuf = new StringBuilder();
        for (int i = 0; i < columnCount(); i++) {
            String name = resolveColumnName(i, nameBuf);
            if (nullCounts[i] >= 0) {
                map.put(SourceStatisticsSerializer.columnNullCountKey(name), nullCounts[i]);
            }
            if (valueCounts[i] >= 0) {
                map.put(SourceStatisticsSerializer.columnValueCountKey(name), valueCounts[i]);
            }
            // Servable + present -> write the value; poisoned -> write the marker (mutually exclusive, matching
            // the coordinator fold's convention where a poisoned extremum drops the value and sets the marker).
            if (minServable[i] == false) {
                map.put(SourceStatisticsSerializer.columnMinUnservableKey(name), Boolean.TRUE);
            } else if (mins[i] != null) {
                map.put(SourceStatisticsSerializer.columnMinKey(name), mins[i]);
            }
            if (maxServable[i] == false) {
                map.put(SourceStatisticsSerializer.columnMaxUnservableKey(name), Boolean.TRUE);
            } else if (maxs[i] != null) {
                map.put(SourceStatisticsSerializer.columnMaxKey(name), maxs[i]);
            }
            if (sizesBytes[i] >= 0) {
                map.put(SourceStatisticsSerializer.columnSizeBytesKey(name), sizesBytes[i]);
            }
        }
        return Map.copyOf(map);
    }

    /**
     * Folds multiple {@link SplitStats} into one, using the SAME law and policy as
     * {@link SourceStatisticsSerializer#mergeStatistics}: SUM for row/size/null/value/size-bytes, the MIN/MAX
     * law ({@link #mergedMin}/{@link #mergedMax}) for extrema with per-extremum POISON becoming an unservable
     * bit, and the implicit-nulls (footer) or partial-column-drop (text) policy for a column absent from some
     * splits. This is the compact-model twin of that flat-map fold: {@code fold(splits, m).toMap()} was proven
     * key-equivalent to the former {@code mergeStatistics(splits.map(SplitStats::toMap), m)} by the differential
     * test (before {@code mergeStatistics} was switched to delegate here). The servability MARKER of the flat form
     * becomes the servable BIT here, folded by AND (unservable is sticky across levels), so a poisoned extremum
     * cannot be refilled by a sibling.
     *
     * @param implicitNullsForAbsentColumn footer (true): a split lacking a column folds its rows into that
     *        column's null_count. Text (false): a column absent from a NON-EMPTY split is dropped entirely.
     * @return folded stats, or {@code null} if null/empty; the single element unchanged when size 1.
     */
    static SplitStats fold(List<SplitStats> splits, boolean implicitNullsForAbsentColumn) {
        if (splits == null || splits.isEmpty()) {
            return null;
        }
        if (splits.size() == 1) {
            return splits.get(0);
        }
        long totalRows = 0;
        long totalSize = 0;
        boolean anySize = false;
        Map<String, ColumnFold> byName = new LinkedHashMap<>();
        long[] perSplitRows = new long[splits.size()];
        List<Set<String>> perSplitPresent = new ArrayList<>(splits.size());

        StringBuilder nameBuf = new StringBuilder();
        for (int s = 0; s < splits.size(); s++) {
            SplitStats sp = splits.get(s);
            totalRows += sp.rowCount;
            if (sp.sizeInBytes >= 0) {
                totalSize += sp.sizeInBytes;
                anySize = true;
            }
            perSplitRows[s] = sp.rowCount;
            Set<String> present = new HashSet<>();
            for (int i = 0; i < sp.columnCount(); i++) {
                String name = sp.resolveColumnName(i, nameBuf);
                present.add(name);
                byName.computeIfAbsent(name, k -> new ColumnFold()).accept(sp, i);
            }
            perSplitPresent.add(present);
        }

        // Absent-column policy: footer folds each absent split's rows into null_count; text drops a column not
        // observed in every NON-EMPTY split (a 0-row split's absence does not count against presence).
        for (Map.Entry<String, ColumnFold> e : byName.entrySet()) {
            for (int s = 0; s < splits.size(); s++) {
                if (perSplitPresent.get(s).contains(e.getKey())) {
                    continue;
                }
                if (implicitNullsForAbsentColumn) {
                    e.getValue().absentFooter(perSplitRows[s]);
                } else if (perSplitRows[s] > 0) {
                    e.getValue().dropped = true;
                }
            }
        }

        Builder b = new Builder().rowCount(totalRows);
        if (anySize && totalSize != 0) { // a zero total size is not emitted (the "no file reported a size" contract)
            b.sizeInBytes(totalSize);
        }
        for (Map.Entry<String, ColumnFold> e : byName.entrySet()) {
            ColumnFold cf = e.getValue();
            if (cf.dropped) {
                // Text partial-harvest: a column not observed in every non-empty split cannot be served, so it is
                // dropped ENTIRELY -- no value, no count, and no unservable marker. (The former in-place fold set
                // the poison marker AFTER the drop, leaving a harmless orphan; dropping it here is the cleanup.
                // Serve safe-misses the column either way via hasColumn == false.)
                continue;
            }
            int ord = b.addColumn(e.getKey());
            if (cf.nullCountPoisoned == false && cf.nullCountContributed) {
                b.nullCount(ord, cf.nullCountSum);
            }
            if (cf.valueCountKnown && cf.valueCountPoisoned == false) {
                // Leaving the value count unset keeps the Builder's -1 default, so columnValueCount safe-misses.
                b.valueCount(ord, cf.valueCountSum);
            }
            if (cf.sizeKnown) {
                b.sizeBytes(ord, cf.sizeSum);
            }
            if (cf.minServable) {
                if (cf.min != null) {
                    b.min(ord, cf.min);
                }
            } else {
                b.minUnservable(ord);
            }
            if (cf.maxServable) {
                if (cf.max != null) {
                    b.max(ord, cf.max);
                }
            } else {
                b.maxUnservable(ord);
            }
        }
        return b.build();
    }

    /**
     * Per-column running fold state for {@link #fold}. Mirrors {@code mergeStatistics}: SUM the counts/size,
     * apply the MIN/MAX law to extrema (POISON -> unservable), and track presence for the absent-column policy.
     * An input extremum already unservable (its bit false) keeps the result unservable (AND of bits).
     */
    private static final class ColumnFold {
        long nullCountSum = 0;
        boolean nullCountContributed = false; // any present null_count OR a footer add
        boolean nullCountPoisoned = false;    // a split had the column but no null_count -> unknown
        long valueCountSum = 0;
        boolean valueCountKnown = false;
        boolean valueCountPoisoned = false; // a split HAS the column but reports vc == -1 -> summing would under-count
        long sizeSum = 0;
        boolean sizeKnown = false;
        Object min = null;
        boolean minServable = true;
        boolean minSeen = false;
        Object max = null;
        boolean maxServable = true;
        boolean maxSeen = false;
        boolean dropped = false;

        void accept(SplitStats sp, int i) {
            if (sp.nullCounts[i] >= 0) {
                nullCountSum += sp.nullCounts[i];
                nullCountContributed = true;
            } else {
                nullCountPoisoned = true;
            }
            if (sp.valueCounts[i] >= 0) {
                valueCountSum += sp.valueCounts[i];
                valueCountKnown = true;
            } else {
                // The column is present in this split but its value count is unknown; summing only the
                // others would under-count COUNT(col). Poison so the fold safe-misses -- matching
                // MergedSplitStats.columnValueCount, which returns -1 the moment any child returns -1.
                valueCountPoisoned = true;
            }
            if (sp.sizesBytes[i] >= 0) {
                sizeSum += sp.sizesBytes[i];
                sizeKnown = true;
            }
            // MIN: an already-unservable input, a non-Comparable first value, or an incompatible merge all poison.
            if (sp.minServable[i] == false) {
                minServable = false;
            } else if (sp.mins[i] != null) {
                if (minSeen == false) {
                    if (sp.mins[i] instanceof Comparable) {
                        min = sp.mins[i];
                        minSeen = true;
                    } else {
                        minServable = false;
                    }
                } else {
                    Object merged = mergedMin(min, sp.mins[i]);
                    if (merged == null) {
                        minServable = false;
                    } else {
                        min = merged;
                    }
                }
            } else if (sp.nullCounts[i] != sp.rowCount) {
                // Present (or unknown-null-count) column with no min value: we cannot rule out a smaller value
                // in this split, so poison rather than serve the other splits' subset minimum — matching
                // MergedSplitStats.columnMin. Only an all-null column (nullCount == rowCount) yields no candidate.
                minServable = false;
            }
            if (sp.maxServable[i] == false) {
                maxServable = false;
            } else if (sp.maxs[i] != null) {
                if (maxSeen == false) {
                    if (sp.maxs[i] instanceof Comparable) {
                        max = sp.maxs[i];
                        maxSeen = true;
                    } else {
                        maxServable = false;
                    }
                } else {
                    Object merged = mergedMax(max, sp.maxs[i]);
                    if (merged == null) {
                        maxServable = false;
                    } else {
                        max = merged;
                    }
                }
            } else if (sp.nullCounts[i] != sp.rowCount) {
                // Symmetric to MIN: a present column with no max value poisons (can't rule out a larger value).
                maxServable = false;
            }
        }

        void absentFooter(long rows) {
            nullCountSum += rows;
            nullCountContributed = true;
        }
    }

    /**
     * Parses a flat {@code _stats.*} map (old format) into a {@code SplitStats} instance.
     * Returns {@code null} if the map is null, empty, or missing the required row count key.
     * <p>
     * Column ordering in the result depends on the input map's iteration order: stable
     * (e.g. {@link LinkedHashMap}) inputs produce stable ordinals. The {@link #toMap()}
     * output is always key-equivalent regardless of column ordering.
     */
    @Nullable
    public static SplitStats of(Map<String, Object> map) {
        if (map == null || map.isEmpty()) {
            return null;
        }
        Object rc = map.get(SourceStatisticsSerializer.STATS_ROW_COUNT);
        if (rc instanceof Number == false) {
            return null;
        }
        Builder builder = new Builder();
        builder.rowCount(((Number) rc).longValue());
        Object sb = map.get(SourceStatisticsSerializer.STATS_SIZE_BYTES);
        if (sb instanceof Number) {
            builder.sizeInBytes(((Number) sb).longValue());
        }

        // Group column keys by column name
        Map<String, Map<String, Object>> columnKeys = new LinkedHashMap<>();
        String prefix = "_stats.columns.";
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith(prefix) == false) {
                continue;
            }
            String rest = key.substring(prefix.length());
            int dotIdx = rest.lastIndexOf('.');
            if (dotIdx <= 0) {
                continue;
            }
            String colName = rest.substring(0, dotIdx);
            String suffix = rest.substring(dotIdx);
            columnKeys.computeIfAbsent(colName, k -> new HashMap<>()).put(suffix, entry.getValue());
        }

        for (Map.Entry<String, Map<String, Object>> colEntry : columnKeys.entrySet()) {
            String colName = colEntry.getKey();
            Map<String, Object> stats = colEntry.getValue();
            int ordinal = builder.addColumn(colName);

            Object nc = stats.get(".null_count");
            if (nc instanceof Number) {
                builder.nullCount(ordinal, ((Number) nc).longValue());
            }
            Object vc = stats.get(".value_count");
            if (vc instanceof Number) {
                builder.valueCount(ordinal, ((Number) vc).longValue());
            }
            // Marker WINS over a value: a poisoned extremum carries no servable value. A well-formed map never
            // has both, but normalizing here (drop the value when the marker is present) enforces the
            // minServable||min==null invariant so a malformed map can never round-trip a poisoned value.
            if (Boolean.TRUE.equals(stats.get(SourceStatisticsSerializer.MIN_UNSERVABLE_SUFFIX))) {
                builder.minUnservable(ordinal);
            } else {
                Object min = stats.get(".min");
                if (min != null) {
                    builder.min(ordinal, min);
                }
            }
            if (Boolean.TRUE.equals(stats.get(SourceStatisticsSerializer.MAX_UNSERVABLE_SUFFIX))) {
                builder.maxUnservable(ordinal);
            } else {
                Object max = stats.get(".max");
                if (max != null) {
                    builder.max(ordinal, max);
                }
            }
            Object csb = stats.get(".size_bytes");
            if (csb instanceof Number) {
                builder.sizeBytes(ordinal, ((Number) csb).longValue());
            }
        }

        return builder.build();
    }

    /**
     * Resolves the effective {@link org.elasticsearch.xpack.esql.datasources.spi.SplitStats} for
     * a set of splits. Uses {@link ExternalSplit#splitStats()} on each split, which handles both
     * {@link FileSplit} and {@link CoalescedSplit} transparently without flattening. For single-split
     * queries the per-split stats (if available) or the sourceMetadata map is used; for multi-split
     * queries a {@link MergedSplitStats} over the per-split stats is returned. Returns {@code null}
     * if any split lacks stats.
     */
    @Nullable
    public static org.elasticsearch.xpack.esql.datasources.spi.SplitStats resolveEffectiveStats(
        List<? extends ExternalSplit> splits,
        Map<String, Object> sourceMetadata
    ) {
        if (splits.size() <= 1) {
            if (splits.size() == 1) {
                org.elasticsearch.xpack.esql.datasources.spi.SplitStats perSplit = splits.getFirst().splitStats();
                if (perSplit != null) {
                    return perSplit;
                }
            }
            if (sourceMetadata != null && Boolean.TRUE.equals(sourceMetadata.get(SourceStatisticsSerializer.STATS_PARTIAL))) {
                return null;
            }
            return of(sourceMetadata);
        }
        List<org.elasticsearch.xpack.esql.datasources.spi.SplitStats> perSplitStats = new ArrayList<>(splits.size());
        for (ExternalSplit split : splits) {
            org.elasticsearch.xpack.esql.datasources.spi.SplitStats stats = split.splitStats();
            if (stats == null) {
                return null;
            }
            perSplitStats.add(stats);
        }
        // Pure value-fold: per-split stats are already normalized to the reconciled type at split construction
        // (FileSplitProvider), so the merge does not reconcile units. See MergedSplitStats.
        return new MergedSplitStats(perSplitStats);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SplitStats that = (SplitStats) o;
        return rowCount == that.rowCount
            && sizeInBytes == that.sizeInBytes
            && Arrays.equals(segments, that.segments)
            && Arrays.deepEquals(columnSegmentOrdinals, that.columnSegmentOrdinals)
            && Arrays.equals(nullCounts, that.nullCounts)
            && Arrays.equals(valueCounts, that.valueCounts)
            && Arrays.deepEquals(mins, that.mins)
            && Arrays.deepEquals(maxs, that.maxs)
            && Arrays.equals(sizesBytes, that.sizesBytes)
            && Arrays.equals(minServable, that.minServable)
            && Arrays.equals(maxServable, that.maxServable);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(rowCount, sizeInBytes);
        result = 31 * result + Arrays.hashCode(segments);
        result = 31 * result + Arrays.deepHashCode(columnSegmentOrdinals);
        result = 31 * result + Arrays.hashCode(nullCounts);
        result = 31 * result + Arrays.hashCode(valueCounts);
        result = 31 * result + Arrays.deepHashCode(mins);
        result = 31 * result + Arrays.deepHashCode(maxs);
        result = 31 * result + Arrays.hashCode(sizesBytes);
        result = 31 * result + Arrays.hashCode(minServable);
        result = 31 * result + Arrays.hashCode(maxServable);
        return result;
    }

    @Override
    public String toString() {
        return "SplitStats[rowCount=" + rowCount + ", columns=" + columnCount() + "]";
    }

    /**
     * Builder for constructing {@link SplitStats} instances. Columns are added via
     * {@link #addColumn(String)} which returns an ordinal for subsequent stat calls.
     * Path segments are deduplicated automatically.
     */
    public static final class Builder {
        private long rowCount = -1;
        private long sizeInBytes = -1;
        private final List<int[]> columnOrdinals = new ArrayList<>();
        private final List<Long> nullCountsList = new ArrayList<>();
        private final List<Long> valueCountsList = new ArrayList<>();
        private final List<Object> minsList = new ArrayList<>();
        private final List<Object> maxsList = new ArrayList<>();
        private final List<Boolean> minServableList = new ArrayList<>();
        private final List<Boolean> maxServableList = new ArrayList<>();
        private final List<Long> sizesBytesList = new ArrayList<>();
        private final Map<String, Integer> segmentIndex = new LinkedHashMap<>();
        private final Map<String, Integer> columnNameToOrdinal = new LinkedHashMap<>();

        public Builder rowCount(long rowCount) {
            this.rowCount = rowCount;
            return this;
        }

        public Builder sizeInBytes(long sizeInBytes) {
            this.sizeInBytes = sizeInBytes;
            return this;
        }

        /**
         * Adds a column and returns its ordinal. The column name is split on '.'
         * and each segment is deduplicated in the path-segment dictionary.
         *
         * @throws IllegalArgumentException if a column with the same name was already added
         */
        public int addColumn(String name) {
            if (columnNameToOrdinal.containsKey(name)) {
                throw new IllegalArgumentException("duplicate column name: " + name);
            }
            int ordinal = columnOrdinals.size();
            columnNameToOrdinal.put(name, ordinal);
            String[] parts = name.split("\\.", -1);
            int[] ords = new int[parts.length];
            for (int i = 0; i < parts.length; i++) {
                ords[i] = segmentIndex.computeIfAbsent(parts[i], k -> segmentIndex.size());
            }
            columnOrdinals.add(ords);
            nullCountsList.add(-1L);
            valueCountsList.add(-1L);
            minsList.add(null);
            maxsList.add(null);
            minServableList.add(Boolean.TRUE);
            maxServableList.add(Boolean.TRUE);
            sizesBytesList.add(-1L);
            return ordinal;
        }

        /**
         * Adds a column with all stats in a single call and returns its ordinal.
         * Use {@code -1} for unknown longs and {@code null} for unknown objects.
         *
         * @throws IllegalArgumentException if a column with the same name was already added
         */
        public int addColumn(String name, long nullCount, @Nullable Object min, @Nullable Object max, long sizeBytes) {
            int ordinal = addColumn(name);
            nullCountsList.set(ordinal, nullCount);
            minsList.set(ordinal, min);
            maxsList.set(ordinal, max);
            sizesBytesList.set(ordinal, sizeBytes);
            return ordinal;
        }

        public Builder nullCount(int columnOrdinal, long nullCount) {
            nullCountsList.set(columnOrdinal, nullCount);
            return this;
        }

        public Builder valueCount(int columnOrdinal, long valueCount) {
            valueCountsList.set(columnOrdinal, valueCount);
            return this;
        }

        public Builder min(int columnOrdinal, Object min) {
            minsList.set(columnOrdinal, min);
            return this;
        }

        public Builder max(int columnOrdinal, Object max) {
            maxsList.set(columnOrdinal, max);
            return this;
        }

        public Builder sizeBytes(int columnOrdinal, long sizeBytes) {
            sizesBytesList.set(columnOrdinal, sizeBytes);
            return this;
        }

        /** Marks the column's MIN as poisoned (unservable): a fold found it incompatible, so it must not be served. */
        public Builder minUnservable(int columnOrdinal) {
            minServableList.set(columnOrdinal, Boolean.FALSE);
            return this;
        }

        /** Marks the column's MAX as poisoned (unservable): a fold found it incompatible, so it must not be served. */
        public Builder maxUnservable(int columnOrdinal) {
            maxServableList.set(columnOrdinal, Boolean.FALSE);
            return this;
        }

        public SplitStats build() {
            if (rowCount < 0) {
                throw new IllegalArgumentException("rowCount must be set (>= 0) before building");
            }
            String[] segs = segmentIndex.keySet().toArray(String[]::new);
            int colCount = columnOrdinals.size();
            int[][] colSegOrdinals = columnOrdinals.toArray(int[][]::new);
            long[] nc = new long[colCount];
            long[] vc = new long[colCount];
            Object[] mn = new Object[colCount];
            Object[] mx = new Object[colCount];
            long[] sb = new long[colCount];
            boolean[] mnServable = new boolean[colCount];
            boolean[] mxServable = new boolean[colCount];
            for (int i = 0; i < colCount; i++) {
                nc[i] = nullCountsList.get(i);
                vc[i] = valueCountsList.get(i);
                mn[i] = minsList.get(i);
                mx[i] = maxsList.get(i);
                sb[i] = sizesBytesList.get(i);
                mnServable[i] = minServableList.get(i);
                mxServable[i] = maxServableList.get(i);
            }
            return new SplitStats(rowCount, sizeInBytes, segs, colSegOrdinals, nc, vc, mn, mx, sb, mnServable, mxServable);
        }
    }
}
