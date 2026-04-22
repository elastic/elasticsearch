/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.SourceStatistics;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * Serializes and deserializes {@link SourceStatistics} to/from a flat {@code Map<String, Object>}
 * using well-known keys. This allows statistics to flow through the opaque {@code sourceMetadata}
 * map in {@link org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec} without requiring
 * new fields or serialization format changes.
 */
public final class SourceStatisticsSerializer {

    public static final String STATS_ROW_COUNT = "_stats.row_count";
    public static final String STATS_SIZE_BYTES = "_stats.size_bytes";
    private static final String STATS_COL_PREFIX = "_stats.columns.";
    private static final String NULL_COUNT_SUFFIX = ".null_count";
    private static final String MIN_SUFFIX = ".min";
    private static final String MAX_SUFFIX = ".max";
    private static final String SIZE_BYTES_SUFFIX = ".size_bytes";

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
     * Resolves the effective metadata for a set of splits. For single-split queries returns
     * the given sourceMetadata directly; for multi-split queries merges per-split statistics
     * from each {@link FileSplit}. Returns {@code null} if any split is not a {@code FileSplit}
     * or if {@link #mergeStatistics} cannot produce a valid merged result (e.g. missing or
     * non-numeric row counts).
     */
    public static Map<String, Object> resolveEffectiveMetadata(List<? extends ExternalSplit> splits, Map<String, Object> sourceMetadata) {
        if (splits.size() <= 1) {
            return sourceMetadata;
        }
        List<Map<String, Object>> splitStats = new ArrayList<>(splits.size());
        for (ExternalSplit split : splits) {
            if (split instanceof FileSplit fileSplit) {
                splitStats.add(fileSplit.statistics());
            } else {
                return null;
            }
        }
        return mergeStatistics(splitStats);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static Map<String, Object> mergeStatistics(List<Map<String, Object>> splitStats) {
        if (splitStats == null || splitStats.isEmpty()) {
            return null;
        }
        if (splitStats.size() == 1) {
            Map<String, Object> single = splitStats.get(0);
            return single != null && single.get(STATS_ROW_COUNT) instanceof Number ? single : null;
        }

        long totalRows = 0;
        long totalSize = 0;
        Map<String, long[]> nullCounts = new HashMap<>();
        Map<String, Comparable[]> mins = new HashMap<>();
        Map<String, Comparable[]> maxs = new HashMap<>();
        Map<String, long[]> colSizeBytes = new HashMap<>();

        for (Map<String, Object> stats : splitStats) {
            if (stats == null || stats.containsKey(STATS_ROW_COUNT) == false) {
                return null;
            }
            Object rc = stats.get(STATS_ROW_COUNT);
            if (rc instanceof Number rcNum) {
                totalRows += rcNum.longValue();
            } else {
                return null;
            }
            Object sb = stats.get(STATS_SIZE_BYTES);
            if (sb instanceof Number sbNum) totalSize += sbNum.longValue();

            for (Map.Entry<String, Object> entry : stats.entrySet()) {
                String key = entry.getKey();
                if (key.startsWith(STATS_COL_PREFIX) == false) continue;
                if (key.endsWith(NULL_COUNT_SUFFIX) && entry.getValue() instanceof Number ncNum) {
                    nullCounts.merge(key, new long[] { ncNum.longValue() }, (a, b) -> {
                        a[0] += b[0];
                        return a;
                    });
                } else if (key.endsWith(MIN_SUFFIX) && entry.getValue() instanceof Comparable c) {
                    mins.merge(key, new Comparable[] { c }, (a, b) -> {
                        int cmp = a[0].compareTo(b[0]);
                        if (cmp > 0) a[0] = b[0];
                        return a;
                    });
                } else if (key.endsWith(MAX_SUFFIX) && entry.getValue() instanceof Comparable c) {
                    maxs.merge(key, new Comparable[] { c }, (a, b) -> {
                        int cmp = a[0].compareTo(b[0]);
                        if (cmp < 0) a[0] = b[0];
                        return a;
                    });
                } else if (key.endsWith(SIZE_BYTES_SUFFIX) && entry.getValue() instanceof Number sbNum) {
                    colSizeBytes.merge(key, new long[] { sbNum.longValue() }, (a, b) -> {
                        a[0] += b[0];
                        return a;
                    });
                }
            }
        }

        Map<String, Object> merged = new HashMap<>();
        merged.put(STATS_ROW_COUNT, totalRows);
        if (totalSize > 0) {
            merged.put(STATS_SIZE_BYTES, totalSize);
        }
        nullCounts.forEach((k, v) -> merged.put(k, v[0]));
        mins.forEach((k, v) -> merged.put(k, v[0]));
        maxs.forEach((k, v) -> merged.put(k, v[0]));
        colSizeBytes.forEach((k, v) -> merged.put(k, v[0]));
        return merged;
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
