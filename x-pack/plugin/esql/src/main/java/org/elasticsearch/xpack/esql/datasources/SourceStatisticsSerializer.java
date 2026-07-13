/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.SourceStatistics;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

/**
 * Serializes and deserializes {@link SourceStatistics} to/from a flat {@code Map<String, Object>}
 * using well-known keys. This allows statistics to flow through the opaque {@code sourceMetadata}
 * map in {@link org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec} without requiring
 * new fields or serialization format changes.
 */
public final class SourceStatisticsSerializer {

    /** Common prefix of every flat statistics key (row count, size, per-column stats, partial flag). */
    public static final String STATS_KEY_PREFIX = "_stats.";
    public static final String STATS_ROW_COUNT = "_stats.row_count";
    public static final String STATS_SIZE_BYTES = "_stats.size_bytes";
    /**
     * When set to {@code true} in sourceMetadata, indicates that the statistics are derived
     * from a single anchor file in a multi-file glob query ({@code FIRST_FILE_WINS} schema
     * resolution) and do not represent the full dataset. This flag is set by
     * {@code ExternalSourceResolver.markStatsAsPartial} when a glob matches more than one file.
     * <p>
     * The aggregate pushdown rule ({@code PushStatsToExternalSource}) checks this flag
     * via {@code SplitStats.resolveEffectiveStats} and bails out when set. Note that once
     * per-split statistics are available (populated during split discovery), the merged
     * per-split stats take precedence and this flag is not consulted.
     */
    public static final String STATS_PARTIAL = "_stats.partial";
    /** Number of files matched by the glob pattern; useful for observability and debugging. */
    public static final String STATS_FILE_COUNT = "_stats.file_count";
    public static final String STATS_COL_PREFIX = "_stats.columns.";
    /**
     * Names of the Hive-partition (path-derived) columns, stamped here at resolution so the DATA-NODE fold can
     * recognize them: the coordinator-only {@code FileList} (which carries {@link PartitionMetadata}) deserializes
     * to {@code UNRESOLVED} on a remote node, but {@code sourceMetadata} travels with the serialized relation. A
     * partition column is absent from every file's column stats, so without this signal a data-node
     * {@code COUNT(partition_col)} would serve {@code rowCount - rowCount = 0}. Deliberately NOT under the
     * {@code _stats.} prefix so the FirstFileWins / union-by-name stat merges preserve it. Value: {@code List<String>}.
     */
    public static final String PARTITION_COLUMNS_KEY = "_partition.columns";
    // Package-private: consumed by the *Key helpers here and by SplitStats.of/toMap (the round-trip between the
    // flat keys and the compact model), all within this package.
    static final String NULL_COUNT_SUFFIX = ".null_count";
    static final String VALUE_COUNT_SUFFIX = ".value_count";
    static final String MIN_SUFFIX = ".min";
    static final String MAX_SUFFIX = ".max";
    static final String SIZE_BYTES_SUFFIX = ".size_bytes";
    // Per-statistic unservability markers. A marker is a PRESENT key (not an absent one): it forces the
    // matching extremum to safe-miss at serve AND, because it is present and OR-folds across contributions,
    // a sibling's finite extremum cannot refill a dropped one at the next fold level. This replaces the
    // blunt "drop the whole column" taint -- COUNT-family (null_count/value_count) survives an extremum taint.
    static final String MIN_UNSERVABLE_SUFFIX = ".min_unservable";
    static final String MAX_UNSERVABLE_SUFFIX = ".max_unservable";

    private SourceStatisticsSerializer() {}

    /**
     * The names of the Hive-partition (path-derived) columns for a source, read from the serialized
     * {@link #PARTITION_COLUMNS_KEY} stamp in {@code sourceMetadata}. This is the ONE node-safe channel for
     * partition identity: the {@code FileList} that carries {@code PartitionMetadata} is coordinator-only and
     * deserializes to {@code UNRESOLVED} on a data node, so any consumer that reads partition names off the
     * fileList sees an empty set there. Every node-agnostic consumer reads partition identity through this
     * method (usually via the {@code partitionColumnNames()} accessor on {@code ExternalSourceExec} /
     * {@code ExternalRelation}), never off the fileList. Returns an empty set when the source is not
     * partitioned, and otherwise preserves the stamped order (the partition nesting, e.g. {@code region} then
     * {@code tier}).
     * <p>
     * Deliberately a {@link LinkedHashSet} rather than {@code Set.copyOf}: no consumer reads this set by
     * iteration today (they all use {@code contains} / {@code isEmpty}), but {@code Set.copyOf} randomizes
     * iteration order <em>per JVM</em>, so a coordinator and a data node would order it differently. The day
     * these names surface anywhere user-visible — an error message, a plan string, debug output — that would be
     * nondeterministic and divergent across nodes. Ordering a handful of strings once per plan costs nothing and
     * removes the failure mode.
     */
    @SuppressWarnings("unchecked")
    public static Set<String> partitionColumnNames(Map<String, Object> sourceMetadata) {
        if (sourceMetadata == null) {
            return Set.of();
        }
        Object names = sourceMetadata.get(PARTITION_COLUMNS_KEY);
        if (names instanceof Collection<?> collection) {
            // The stamp is written as List.copyOf(names), which rejects nulls, so the copy cannot NPE here.
            return Collections.unmodifiableSet(new LinkedHashSet<>((Collection<String>) collection));
        }
        return Set.of();
    }

    /**
     * Merges statistics entries into a new map that includes both the original sourceMetadata
     * entries and the serialized statistics. Returns the original map if statistics are absent.
     */
    public static Map<String, Object> embedStatistics(Map<String, Object> sourceMetadata, SourceStatistics statistics) {
        if (statistics == null) {
            return sourceMetadata;
        }
        Map<String, Object> result = new HashMap<>(sourceMetadata);
        statistics.rowCount().ifPresent(rc -> result.put(STATS_ROW_COUNT, rc));
        statistics.sizeInBytes().ifPresent(sb -> result.put(STATS_SIZE_BYTES, sb));
        statistics.columnStatistics().ifPresent(cols -> {
            for (Map.Entry<String, SourceStatistics.ColumnStatistics> entry : cols.entrySet()) {
                String prefix = STATS_COL_PREFIX + entry.getKey();
                SourceStatistics.ColumnStatistics cs = entry.getValue();
                cs.nullCount().ifPresent(nc -> result.put(prefix + NULL_COUNT_SUFFIX, nc));
                cs.valueCount().ifPresent(vc -> result.put(prefix + VALUE_COUNT_SUFFIX, vc));
                cs.minValue().ifPresent(mv -> result.put(prefix + MIN_SUFFIX, mv));
                cs.maxValue().ifPresent(mv -> result.put(prefix + MAX_SUFFIX, mv));
                cs.sizeInBytes().ifPresent(sb -> result.put(prefix + SIZE_BYTES_SUFFIX, sb));
            }
        });
        return result;
    }

    /**
     * Extracts statistics from the sourceMetadata map. Returns empty if no statistics keys are present.
     */
    public static Optional<SourceStatistics> extractStatistics(Map<String, Object> sourceMetadata) {
        if (sourceMetadata == null || sourceMetadata.containsKey(STATS_ROW_COUNT) == false) {
            return Optional.empty();
        }

        return Optional.of(new SourceStatistics() {
            @Override
            public OptionalLong rowCount() {
                return toOptionalLong(asBoxedLong(sourceMetadata.get(STATS_ROW_COUNT)));
            }

            @Override
            public OptionalLong sizeInBytes() {
                return toOptionalLong(asBoxedLong(sourceMetadata.get(STATS_SIZE_BYTES)));
            }

            @Override
            public Optional<Map<String, ColumnStatistics>> columnStatistics() {
                Map<String, ColumnStatistics> cols = new HashMap<>();
                for (String key : sourceMetadata.keySet()) {
                    if (key.startsWith(STATS_COL_PREFIX) == false) {
                        continue;
                    }
                    String rest = key.substring(STATS_COL_PREFIX.length());
                    int dotIdx = rest.lastIndexOf('.');
                    if (dotIdx <= 0) {
                        continue;
                    }
                    String colName = rest.substring(0, dotIdx);
                    cols.computeIfAbsent(colName, n -> new DeserializedColumnStatistics(sourceMetadata, n));
                }
                return cols.isEmpty() ? Optional.empty() : Optional.of(cols);
            }
        });
    }

    /**
     * Extracts the row count directly from the sourceMetadata map.
     * Returns {@code null} if the metadata is null or the row count is absent/non-numeric.
     */
    @Nullable
    public static Long extractRowCount(Map<String, Object> sourceMetadata) {
        return sourceMetadata != null ? asBoxedLong(sourceMetadata.get(STATS_ROW_COUNT)) : null;
    }

    /**
     * Extracts the null count for a specific column directly from the sourceMetadata map.
     * Returns {@code null} if the metadata is null or the null count is absent/non-numeric.
     */
    @Nullable
    public static Long extractColumnNullCount(Map<String, Object> sourceMetadata, String columnName) {
        return sourceMetadata != null ? asBoxedLong(sourceMetadata.get(columnNullCountKey(columnName))) : null;
    }

    /**
     * Extracts the value count (count of non-null values) for a specific column directly from the
     * sourceMetadata map. Returns {@code null} if the metadata is null or the value count is
     * absent/non-numeric.
     */
    @Nullable
    public static Long extractColumnValueCount(Map<String, Object> sourceMetadata, String columnName) {
        return sourceMetadata != null ? asBoxedLong(sourceMetadata.get(columnValueCountKey(columnName))) : null;
    }

    /**
     * Extracts the min value for a specific column directly from the sourceMetadata map.
     * Returns {@code null} if the metadata is null or the min is absent.
     */
    @Nullable
    public static Object extractColumnMin(Map<String, Object> sourceMetadata, String columnName) {
        return sourceMetadata != null ? sourceMetadata.get(columnMinKey(columnName)) : null;
    }

    /**
     * Extracts the max value for a specific column directly from the sourceMetadata map.
     * Returns {@code null} if the metadata is null or the max is absent.
     */
    @Nullable
    public static Object extractColumnMax(Map<String, Object> sourceMetadata, String columnName) {
        return sourceMetadata != null ? sourceMetadata.get(columnMaxKey(columnName)) : null;
    }

    /** Returns the flat key used for a column's null count statistic. */
    public static String columnNullCountKey(String columnName) {
        return STATS_COL_PREFIX + columnName + NULL_COUNT_SUFFIX;
    }

    /** Returns the flat key used for a column's value count (non-null values) statistic. */
    public static String columnValueCountKey(String columnName) {
        return STATS_COL_PREFIX + columnName + VALUE_COUNT_SUFFIX;
    }

    /** Returns the flat key used for a column's min statistic. */
    public static String columnMinKey(String columnName) {
        return STATS_COL_PREFIX + columnName + MIN_SUFFIX;
    }

    /** Returns the flat key used for a column's max statistic. */
    public static String columnMaxKey(String columnName) {
        return STATS_COL_PREFIX + columnName + MAX_SUFFIX;
    }

    /** Returns the flat key used for a column's size in bytes statistic. */
    public static String columnSizeBytesKey(String columnName) {
        return STATS_COL_PREFIX + columnName + SIZE_BYTES_SUFFIX;
    }

    /** Returns the flat key that marks a column's {@code min} statistic unservable. */
    public static String columnMinUnservableKey(String columnName) {
        return STATS_COL_PREFIX + columnName + MIN_UNSERVABLE_SUFFIX;
    }

    /** Returns the flat key that marks a column's {@code max} statistic unservable. */
    public static String columnMaxUnservableKey(String columnName) {
        return STATS_COL_PREFIX + columnName + MAX_UNSERVABLE_SUFFIX;
    }

    /**
     * Poisons a column's {@code min}/{@code max} in-place: drops the extremum values and writes the unservable
     * markers so the column safe-misses to a scan. Count stats (row/null/value counts) are left intact. Used when
     * the extremum cannot be trusted — e.g. the FIRST_FILE_WINS fold detects a column whose physical type diverges
     * across files, so both the unit-blind fold AND the anchor-schema misread of the divergent file make a warm
     * extremum unable to match a scan.
     */
    public static void poisonColumnExtrema(Map<String, Object> statsMap, String columnName) {
        statsMap.remove(columnMinKey(columnName));
        statsMap.remove(columnMaxKey(columnName));
        statsMap.put(columnMinUnservableKey(columnName), Boolean.TRUE);
        statsMap.put(columnMaxUnservableKey(columnName), Boolean.TRUE);
    }

    // All seven per-column stat suffixes, for the declared-overlay rekey: a column's whole stat family moves together,
    // including the unservable markers (an upstream FFW-divergence poison must survive a `path` rename).
    private static final String[] COLUMN_STAT_SUFFIXES = {
        NULL_COUNT_SUFFIX,
        VALUE_COUNT_SUFFIX,
        MIN_SUFFIX,
        MAX_SUFFIX,
        SIZE_BYTES_SUFFIX,
        MIN_UNSERVABLE_SUFFIX,
        MAX_UNSERVABLE_SUFFIX };

    /**
     * The declared-schema overlay's stats boundary — the fourth, after reconciliation-normalize, FFW-divergence poison,
     * and commit-time coercion. Stats are produced keyed by <b>physical</b> (file) column names holding <b>inferred</b>-type
     * values; the declared overlay renames/retypes the plan afterwards, so without this the warm path serves physical-keyed
     * stats under logical names (a renamed {@code COUNT(col)} serves 0) and inferred-type extrema/counts a coerced scan
     * never produces. This (1) REKEYS every per-column stat family physical&rarr;logical for each {@code path} rename — a
     * pure move changes no value, so the rekeyed stats stay exactly correct and warm serving survives the rename; (2)
     * POISONS the extrema and DROPS {@code value_count}/{@code null_count} for {@code poisonColumns} (declared retype or
     * declared date format — read-time coercion can null cells and re-represent values, so no pre-coercion stat is
     * trustworthy). {@code row_count}/{@code file_count}/{@code size_bytes} and non-column keys are untouched, so
     * {@code COUNT(*)} stays warm and the cost estimator keeps its byte signal. Returns the input instance unchanged when
     * there is nothing to do; otherwise a new map (inputs are routinely {@code Map.copyOf}-immutable).
     *
     * @param physicalToLogical physical&rarr;logical name moves (the inverse of {@code DeclaredReadSpec#renames}; 1:1 by
     *                          validation, so the inverse is well-defined)
     * @param poisonColumns     LOGICAL names whose extrema + counts must safe-miss
     */
    public static Map<String, Object> overlayDeclaredSchemaOnStats(
        Map<String, Object> statsMap,
        Map<String, String> physicalToLogical,
        Set<String> poisonColumns
    ) {
        if (statsMap == null || statsMap.isEmpty() || (physicalToLogical.isEmpty() && poisonColumns.isEmpty())) {
            return statsMap;
        }
        Map<String, Object> out = new HashMap<>(statsMap);
        // Two-phase move: renames may swap/chain (logical `a`->physical `b` while `b`->`a` — the overlap
        // PhysicalNames.noLogicalNamesRemain documents), so remove ALL source families first, then write.
        Map<String, Object> staged = new HashMap<>();
        for (Map.Entry<String, String> move : physicalToLogical.entrySet()) {
            String physicalPrefix = STATS_COL_PREFIX + move.getKey();
            String logicalPrefix = STATS_COL_PREFIX + move.getValue();
            for (String suffix : COLUMN_STAT_SUFFIXES) {
                Object value = out.remove(physicalPrefix + suffix);
                if (value != null) {
                    staged.put(logicalPrefix + suffix, value);
                }
            }
        }
        out.putAll(staged);
        for (String column : poisonColumns) {
            poisonColumnExtrema(out, column);        // drops .min/.max, writes both unservable markers
            out.remove(columnValueCountKey(column)); // count-family unknown, not zero: SplitStats.of defaults both
            out.remove(columnNullCountKey(column));  // to -1 -> COUNT(col) safe-misses; COUNT(*) is unaffected
        }
        return out;
    }

    /**
     * Compares two keyword/text stat extrema in UTF-8 byte order — the SAME order the runtime keyword MIN/MAX
     * aggregators and comparisons use ({@link BytesRef} unsigned-byte order) — NOT {@link String#compareTo}'s
     * UTF-16 code-unit order, which disagrees for supplementary (astral) chars vs BMP chars in
     * {@code [U+E000..U+FFFF]}. Accepts either representation a stat value takes: {@code String} (parquet footer)
     * or {@link BytesRef} (text harvest). Single owner of keyword-stat ordering: the cross-file fold
     * ({@code SplitStats.mergedMin}/{@code mergedMax}), the split-filter classifier ({@code StatValueComparator}),
     * and the parquet cross-row-group fold ({@code ParquetFormatReader}) all delegate here.
     */
    public static int compareKeywordUtf8(Object a, Object b) {
        BytesRef ba = a instanceof BytesRef br ? br : new BytesRef((String) a);
        BytesRef bb = b instanceof BytesRef br ? br : new BytesRef((String) b);
        return ba.compareTo(bb);
    }

    /**
     * Normalizes a per-file stat map's {@code min}/{@code max} to the RECONCILED column type, at the boundary
     * where BOTH the file's own type and the multi-file reconciled type are known (multi-file discovery /
     * split construction). Per-file stats are stored in the file's LOCAL unit/representation, but every warm
     * consumer — the split-filter classifier, the filtered/whole-file merge, the source-level fold, and the
     * MIN/MAX serve — reads the value AS the reconciled type ({@code af.dataType()}) with no further rescale.
     * Normalizing here, once, is what makes those consumers correct instead of comparing file-local units
     * unit-blind. Two cases need it (the numeric Long/Double flap within one representation is handled
     * separately by the cache-path {@code coerceColumnStatsToResolvedTypes} and the poison fold):
     * <ul>
     *   <li><b>Temporal widening</b> — a {@code DATETIME} (epoch-millis) file column reconciled to
     *   {@code DATE_NANOS} (epoch-nanos) has its min/max rescaled ×1e6 ({@link Math#multiplyExact}); on
     *   overflow the value is dropped and the unservable marker written (safe-miss), never a wrong nanos value.</li>
     *   <li><b>Representation change</b> — a numeric/temporal file column reconciled to {@code KEYWORD}/{@code TEXT}
     *   ({@link org.elasticsearch.xpack.esql.datasources.SchemaReconciliation}'s non-widenable fallback) would be
     *   served under lexicographic/stringified order, not numeric, so its numeric min/max is dropped and the
     *   marker written (safe-miss).</li>
     * </ul>
     * Count stats (value_count/null_count/row_count) are unit- and representation-independent and pass through.
     * The unservable marker (not a bare removal) is written so marker-wins normalization in {@code SplitStats.of}
     * forces a safe-miss even if a later overlay would otherwise resurrect a stale value. Returns the input
     * unchanged when no column needed normalization.
     */
    public static Map<String, Object> normalizeStatsToReconciled(
        Map<String, Object> statsMap,
        Map<String, DataType> fileTypes,
        Map<String, DataType> reconciledTypes
    ) {
        if (statsMap == null || statsMap.isEmpty() || fileTypes == null || reconciledTypes == null) {
            return statsMap;
        }
        Map<String, Object> out = null; // copied lazily, only if a column actually needs a change
        for (Map.Entry<String, DataType> entry : reconciledTypes.entrySet()) {
            String col = entry.getKey();
            DataType reconciled = entry.getValue();
            DataType file = fileTypes.get(col);
            if (file == null || file == reconciled) {
                continue; // column absent from this file, or already the reconciled type
            }
            for (String[] pair : List.of(
                new String[] { columnMinKey(col), columnMinUnservableKey(col) },
                new String[] { columnMaxKey(col), columnMaxUnservableKey(col) }
            )) {
                Object value = statsMap.get(pair[0]);
                if (value instanceof Number == false) {
                    continue;
                }
                Object normalized = normalizeExtremumToReconciled((Number) value, file, reconciled);
                if (normalized != null) {
                    if (normalized.equals(value)) {
                        continue; // no change
                    }
                    if (out == null) {
                        out = new HashMap<>(statsMap);
                    }
                    out.put(pair[0], normalized);
                } else {
                    if (out == null) {
                        out = new HashMap<>(statsMap);
                    }
                    out.remove(pair[0]);
                    out.put(pair[1], Boolean.TRUE);
                }
            }
        }
        return out != null ? out : statsMap;
    }

    /** Rescales/validates one extremum from its file type to the reconciled type; {@code null} = not servable. */
    private static Object normalizeExtremumToReconciled(Number value, DataType fileType, DataType reconciledType) {
        if (fileType == DataType.DATETIME && reconciledType == DataType.DATE_NANOS) {
            try {
                return Math.multiplyExact(value.longValue(), 1_000_000L); // epoch-millis → epoch-nanos
            } catch (ArithmeticException overflow) {
                return null; // safe-miss
            }
        }
        if (reconciledType == DataType.KEYWORD || reconciledType == DataType.TEXT) {
            return null; // numeric/temporal served under lexicographic KEYWORD order would be wrong → safe-miss
        }
        if (fileType == DataType.DATE_NANOS && reconciledType == DataType.DATETIME) {
            return null; // widening never narrows nanos→millis; if it somehow reaches here, safe-miss
        }
        // Same numeric/temporal family with only a Long/Double representation flap: left to the cache-path
        // coerce + the poison fold. Pass the value through unchanged here.
        return value;
    }

    /**
     * Extracts the size in bytes for a specific column directly from the sourceMetadata map.
     * Returns {@code null} if the metadata is null or the size is absent/non-numeric.
     */
    @Nullable
    public static Long extractColumnSizeBytes(Map<String, Object> sourceMetadata, String columnName) {
        return sourceMetadata != null ? asBoxedLong(sourceMetadata.get(columnSizeBytesKey(columnName))) : null;
    }

    /**
     * Merges per-file {@code _stats.*} maps into a single dataset-wide map.
     * <p>
     * Implements the "implicit nulls" contract for UNION_BY_NAME aggregate pushdown: a column
     * absent from a per-file map (no {@code _stats.columns.<col>.*} keys at all) means the
     * column is physically absent from that file, so its entire row count is folded into the
     * merged {@code null_count} accumulator for that column. This makes
     * {@code Count(col) = totalRowCount - mergedNullCount} correct downstream in
     * {@link org.elasticsearch.xpack.esql.optimizer.rules.physical.local.PushStatsToExternalSource}.
     * <p>
     * Format-reader ground truth: Parquet always writes {@code size_bytes} for present columns
     * and ORC always writes {@code null_count}, so any column-family key in a per-file map is
     * sufficient to mark the column as physically present in that file. The rare exception is
     * Parquet writing a column with stats disabled — present, with {@code size_bytes}, but no
     * {@code null_count}. We refuse to fabricate a null count in that case: the cross-file fold marks the
     * column's {@code null_count} poisoned and drops the entry, so downstream consumers see "unknown" and fall
     * back rather than under-count.
     * <p>
     * Min/max/size_bytes accumulators are unchanged: they only sum across files where the
     * column is present, which is the correct semantics regardless of implicit nulls.
     */
    public static Map<String, Object> mergeStatistics(List<Map<String, Object>> splitStats) {
        // Footer formats (Parquet/ORC) always write complete per-file column stats, so an absent
        // column folds into implicit nulls. This is the default for callers that only merge such stats.
        return mergeStatistics(splitStats, true);
    }

    /**
     * @param implicitNullsForAbsentColumn when {@code true} (footer formats), a column absent from a
     *        per-file map is treated as physically absent and its rows fold into the merged null_count
     *        (UNION_BY_NAME semantics). When {@code false} (text formats under partial harvest), a
     *        column absent from any file's stats is "not harvested" -- it may be physically present --
     *        so the merged column is dropped entirely, forcing downstream COUNT/MIN/MAX to safe-miss
     *        (re-scan) rather than undercount or serve a subset extremum.
     */
    public static Map<String, Object> mergeStatistics(List<Map<String, Object>> splitStats, boolean implicitNullsForAbsentColumn) {
        if (splitStats == null || splitStats.isEmpty()) {
            return null;
        }
        if (splitStats.size() == 1) {
            Map<String, Object> single = splitStats.get(0);
            return single != null && single.get(STATS_ROW_COUNT) instanceof Number ? single : null;
        }

        // Fold via the compact model: deserialize each input, fold with the one canonical engine, re-serialize.
        // The equivalence to the former in-place flat-map fold was proven by
        // SplitStatsTests#testFoldMatchesMergeStatisticsDifferential against that fold BEFORE this delegation landed
        // (git history); the test now guards the of/fold/toMap round-trip. The per-field law lives once in
        // SplitStats.mergedMin/mergedMax and the fold's SUM/AND -- there is no longer a parallel per-key law table.
        List<SplitStats> splits = new ArrayList<>(splitStats.size());
        for (Map<String, Object> stats : splitStats) {
            if (stats == null || stats.get(STATS_ROW_COUNT) instanceof Number == false) {
                return null;
            }
            SplitStats s = SplitStats.of(stats);
            if (s == null) {
                return null;
            }
            splits.add(s);
        }
        SplitStats folded = SplitStats.fold(splits, implicitNullsForAbsentColumn);
        // Mutable copy: callers (e.g. ExternalSourceCacheService.foldFragments) re-attach the keying fields.
        return folded == null ? null : new HashMap<>(folded.toMap());
    }

    @Nullable
    private static Long asBoxedLong(Object value) {
        return value instanceof Number n ? n.longValue() : null;
    }

    private static OptionalLong toOptionalLong(Long value) {
        return value != null ? OptionalLong.of(value) : OptionalLong.empty();
    }

    private static class DeserializedColumnStatistics implements SourceStatistics.ColumnStatistics {
        private final Map<String, Object> map;
        private final String colName;

        DeserializedColumnStatistics(Map<String, Object> map, String colName) {
            this.map = map;
            this.colName = colName;
        }

        @Override
        public OptionalLong nullCount() {
            return toOptionalLong(extractColumnNullCount(map, colName));
        }

        @Override
        public OptionalLong valueCount() {
            return toOptionalLong(extractColumnValueCount(map, colName));
        }

        @Override
        public OptionalLong distinctCount() {
            return OptionalLong.empty();
        }

        @Override
        public Optional<Object> minValue() {
            return Optional.ofNullable(extractColumnMin(map, colName));
        }

        @Override
        public Optional<Object> maxValue() {
            return Optional.ofNullable(extractColumnMax(map, colName));
        }

        @Override
        public OptionalLong sizeInBytes() {
            return toOptionalLong(extractColumnSizeBytes(map, colName));
        }
    }
}
