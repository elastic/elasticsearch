/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.cbo.stats;

import org.elasticsearch.action.fieldcaps.IndexFieldStats;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.datasources.spi.SourceStatistics;
import org.elasticsearch.xpack.esql.index.IndexProperties;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * Presents the doc and field stats that field caps reported for the indices of an {@link EsRelation} as
 * {@link SourceStatistics}, so indices and external sources share one estimation path. The relation size stays unknown
 * on purpose: the on-disk size of compressed doc values says little about the decoded blocks a plan holds.
 */
record IndexSourceStatistics(OptionalLong rowCount, Map<String, ColumnStatistics> columns) implements SourceStatistics {

    static IndexSourceStatistics of(EsRelation relation) {
        Collection<IndexProperties> indices = relation.indexProperties().values();
        OptionalLong rows = rowCount(indices);
        Map<String, ColumnStatistics> columns = new HashMap<>();
        for (Attribute attribute : relation.output()) {
            if (attribute instanceof FieldAttribute field) {
                IndexColumnStatistics column = column(indices, field.fieldName().string(), rows);
                if (column != null) {
                    columns.put(attribute.name(), column);
                }
            }
        }
        return new IndexSourceStatistics(rows, columns);
    }

    @Override
    public OptionalLong sizeInBytes() {
        return OptionalLong.empty();
    }

    @Override
    public Optional<Map<String, ColumnStatistics>> columnStatistics() {
        return Optional.of(columns);
    }

    private static OptionalLong rowCount(Collection<IndexProperties> indices) {
        if (indices.isEmpty()) {
            return OptionalLong.empty();
        }
        long total = 0;
        for (IndexProperties index : indices) {
            if (index.docCount() < 0) {
                return OptionalLong.empty();
            }
            total += index.docCount();
        }
        return OptionalLong.of(total);
    }

    /**
     * Merges the stats of one field over all indices, or returns {@code null} if any index has none for it. An index can
     * lack them because the field type has no cheap stats or because the field is unmapped there; either way the merged
     * column would understate the values, so it is left unknown.
     */
    private static IndexColumnStatistics column(Collection<IndexProperties> indices, String fieldName, OptionalLong rows) {
        if (indices.isEmpty()) {
            return null;
        }
        long docsWithValue = 0;
        long valueCount = 0;
        long valueBytes = 0;
        Number min = null;
        Number max = null;
        boolean boundsKnown = true;
        for (IndexProperties index : indices) {
            IndexFieldStats stats = index.fieldStats().get(fieldName);
            if (stats == null) {
                return null;
            }
            docsWithValue = add(docsWithValue, stats.docsWithValue());
            valueCount = add(valueCount, stats.valueCount());
            valueBytes = add(valueBytes, stats.valueBytes());
            if (stats.min() == null || stats.max() == null) {
                boundsKnown = false;
            } else if (boundsKnown) {
                min = min == null || compare(stats.min(), min) < 0 ? stats.min() : min;
                max = max == null || compare(stats.max(), max) > 0 ? stats.max() : max;
            }
        }
        OptionalLong nullCount = rows.isPresent() && docsWithValue >= 0
            ? OptionalLong.of(Math.max(0, rows.getAsLong() - docsWithValue))
            : OptionalLong.empty();
        return new IndexColumnStatistics(
            nullCount,
            valueCount < 0 ? OptionalLong.empty() : OptionalLong.of(valueCount),
            boundsKnown ? Optional.<Object>ofNullable(min) : Optional.empty(),
            boundsKnown ? Optional.<Object>ofNullable(max) : Optional.empty(),
            valueBytes < 0 ? OptionalLong.empty() : OptionalLong.of(valueBytes)
        );
    }

    private static long add(long total, long value) {
        if (total < 0 || value < 0) {
            return -1;
        }
        long sum = total + value;
        return sum < 0 ? -1 : sum;
    }

    private static int compare(Number left, Number right) {
        if (left instanceof Long l && right instanceof Long r) {
            return Long.compare(l, r);
        }
        return Double.compare(left.doubleValue(), right.doubleValue());
    }

    /**
     * Field caps does not count distinct values, so {@link #distinctCount()} is always unknown, as for Parquet sources.
     */
    private record IndexColumnStatistics(
        OptionalLong nullCount,
        OptionalLong valueCount,
        Optional<Object> minValue,
        Optional<Object> maxValue,
        OptionalLong sizeInBytes
    ) implements ColumnStatistics {
        @Override
        public OptionalLong distinctCount() {
            return OptionalLong.empty();
        }
    }
}
