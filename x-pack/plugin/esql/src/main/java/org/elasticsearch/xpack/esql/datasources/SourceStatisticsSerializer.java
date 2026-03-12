/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.xpack.esql.datasources.spi.SourceStatistics;

import java.util.HashMap;
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
                return toLong(sourceMetadata.get(STATS_ROW_COUNT));
            }

            @Override
            public OptionalLong sizeInBytes() {
                return toLong(sourceMetadata.get(STATS_SIZE_BYTES));
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
     * Extracts the row count directly from the sourceMetadata map without constructing a full
     * SourceStatistics object.
     */
    public static OptionalLong extractRowCount(Map<String, Object> sourceMetadata) {
        if (sourceMetadata == null) {
            return OptionalLong.empty();
        }
        return toLong(sourceMetadata.get(STATS_ROW_COUNT));
    }

    /**
     * Extracts the null count for a specific column directly from the sourceMetadata map.
     */
    public static OptionalLong extractColumnNullCount(Map<String, Object> sourceMetadata, String columnName) {
        if (sourceMetadata == null) {
            return OptionalLong.empty();
        }
        return toLong(sourceMetadata.get(STATS_COL_PREFIX + columnName + NULL_COUNT_SUFFIX));
    }

    /**
     * Extracts the min value for a specific column directly from the sourceMetadata map.
     */
    public static Optional<Object> extractColumnMin(Map<String, Object> sourceMetadata, String columnName) {
        if (sourceMetadata == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(sourceMetadata.get(STATS_COL_PREFIX + columnName + MIN_SUFFIX));
    }

    /**
     * Extracts the max value for a specific column directly from the sourceMetadata map.
     */
    public static Optional<Object> extractColumnMax(Map<String, Object> sourceMetadata, String columnName) {
        if (sourceMetadata == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(sourceMetadata.get(STATS_COL_PREFIX + columnName + MAX_SUFFIX));
    }

    private static OptionalLong toLong(Object value) {
        if (value instanceof Number n) {
            return OptionalLong.of(n.longValue());
        }
        return OptionalLong.empty();
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
            return extractColumnNullCount(map, colName);
        }

        @Override
        public OptionalLong distinctCount() {
            return OptionalLong.empty();
        }

        @Override
        public Optional<Object> minValue() {
            return extractColumnMin(map, colName);
        }

        @Override
        public Optional<Object> maxValue() {
            return extractColumnMax(map, colName);
        }
    }
}
