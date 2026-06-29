/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.datasources.spi.SourceStatistics;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
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
     * The aggregate pushdown rule ({@code PushAggregatesToExternalSource}) checks this flag
     * via {@code SplitStats.resolveEffectiveStats} and bails out when set. Note that once
     * per-split statistics are available (populated during split discovery), the merged
     * per-split stats take precedence and this flag is not consulted.
     */
    public static final String STATS_PARTIAL = "_stats.partial";
    /** Number of files matched by the glob pattern; useful for observability and debugging. */
    public static final String STATS_FILE_COUNT = "_stats.file_count";
    public static final String STATS_COL_PREFIX = "_stats.columns.";
    // Package-private: the only off-class consumer is StatFolds, in this package.
    static final String NULL_COUNT_SUFFIX = ".null_count";
    static final String MIN_SUFFIX = ".min";
    static final String MAX_SUFFIX = ".max";
    static final String SIZE_BYTES_SUFFIX = ".size_bytes";

    private SourceStatisticsSerializer() {}

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
     * {@link org.elasticsearch.xpack.esql.optimizer.rules.physical.local.PushAggregatesToExternalSource}.
     * <p>
     * Format-reader ground truth: Parquet always writes {@code size_bytes} for present columns
     * and ORC always writes {@code null_count}, so any column-family key in a per-file map is
     * sufficient to mark the column as physically present in that file. The rare exception is
     * Parquet writing a column with stats disabled — present, with {@code size_bytes}, but no
     * {@code null_count}. We refuse to fabricate a null count in that case: the merged map
     * drops the {@code null_count} entry entirely (via {@code poisonedNullCounts}), so
     * downstream consumers see "unknown" and fall back rather than under-count.
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

        // One accumulator over the foldable statistics, combined by each key's Fold law (selected by
        // StatFolds.foldFor) instead of four per-stat maps and inline merge lambdas. A key whose fold
        // yields POISON (incompatible inputs, e.g. a cross-type extremum) is dropped and pinned, so a
        // later compatible file cannot resurrect a subset value.
        Map<String, Object> acc = new HashMap<>();
        Set<String> poisoned = new HashSet<>();
        // Columns physically present in a file but lacking a null_count there: their merged null_count
        // is unreconstructable, so it is dropped to signal "unknown" downstream.
        Set<String> poisonedNullCounts = new HashSet<>();
        // Per-file row count and the set of columns physically present, so absent-column rows can fold
        // into implicit nulls (footer) or partial columns can be dropped (text) in a second pass.
        long[] perFileRowCounts = new long[splitStats.size()];
        List<Set<String>> perFileColumns = new ArrayList<>(splitStats.size());
        Set<String> allColumns = new LinkedHashSet<>();

        int fileIndex = 0;
        for (Map<String, Object> stats : splitStats) {
            Object rc = stats == null ? null : stats.get(STATS_ROW_COUNT);
            if (rc instanceof Number == false) {
                return null;
            }
            // The column is "present" iff any _stats.columns.<col>.* key is in the map (matches
            // SplitStats.of's logic); nullCountSeen tracks which present columns emitted a null_count.
            Set<String> columnsInThisFile = new HashSet<>();
            Set<String> nullCountSeenInThisFile = new HashSet<>();
            for (Map.Entry<String, Object> entry : stats.entrySet()) {
                String key = entry.getKey();
                Fold fold = StatFolds.foldFor(key);
                if (fold == null) {
                    continue;
                }
                if (key.startsWith(STATS_COL_PREFIX)) {
                    String rest = key.substring(STATS_COL_PREFIX.length());
                    int dotIdx = rest.lastIndexOf('.');
                    if (dotIdx > 0) {
                        String colName = rest.substring(0, dotIdx);
                        columnsInThisFile.add(colName);
                        if (key.endsWith(NULL_COUNT_SUFFIX) && entry.getValue() instanceof Number) {
                            nullCountSeenInThisFile.add(colName);
                        }
                    }
                }
                if (poisoned.contains(key)) {
                    continue;
                }
                Object existing = acc.get(key);
                Object folded = existing == null ? fold.first(entry.getValue()) : fold.apply(existing, entry.getValue());
                if (folded == Fold.POISON) {
                    poisoned.add(key);
                    acc.remove(key);
                } else {
                    acc.put(key, folded);
                }
            }
            for (String present : columnsInThisFile) {
                if (nullCountSeenInThisFile.contains(present) == false) {
                    poisonedNullCounts.add(present);
                }
            }
            perFileRowCounts[fileIndex++] = ((Number) rc).longValue();
            perFileColumns.add(columnsInThisFile);
            allColumns.addAll(columnsInThisFile);
        }

        if (implicitNullsForAbsentColumn) {
            // Footer (UNION_BY_NAME) pass: fold the row count of files that do not physically contain a
            // column into that column's null_count, so COUNT(col) = rowCount - mergedNullCount is correct.
            for (String colName : allColumns) {
                String key = columnNullCountKey(colName);
                if (poisoned.contains(key)) {
                    continue;
                }
                for (int i = 0; i < perFileColumns.size(); i++) {
                    if (perFileColumns.get(i).contains(colName) == false) {
                        Object cur = acc.get(key);
                        long base = cur instanceof Number n ? n.longValue() : 0L;
                        acc.put(key, base + perFileRowCounts[i]);
                    }
                }
            }
        } else {
            // Text partial-harvest pass: a column absent from any file's stats is "not harvested" (the
            // file may physically contain it), not "all null". The merge cannot serve a correct
            // COUNT/MIN/MAX for it, so drop it entirely -- downstream safe-misses (re-scans) rather than
            // undercounting (COUNT) or serving a subset extremum (MIN/MAX).
            for (String colName : allColumns) {
                boolean observedInEveryFile = true;
                for (Set<String> cols : perFileColumns) {
                    if (cols.contains(colName) == false) {
                        observedInEveryFile = false;
                        break;
                    }
                }
                if (observedInEveryFile == false) {
                    acc.remove(columnNullCountKey(colName));
                    acc.remove(columnMinKey(colName));
                    acc.remove(columnMaxKey(colName));
                    acc.remove(columnSizeBytesKey(colName));
                }
            }
        }

        for (String colName : poisonedNullCounts) {
            acc.remove(columnNullCountKey(colName));
        }
        // Match the prior contract: a zero total size is not emitted (no file reported size bytes).
        Object size = acc.get(STATS_SIZE_BYTES);
        if (size instanceof Number n && n.longValue() == 0) {
            acc.remove(STATS_SIZE_BYTES);
        }
        return acc;
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
