/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.datasources.spi.SourceStatistics;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * Builds a {@link SourceStatistics} from a cached {@link ExternalStats.Stats} for the
 * line-oriented text-format readers (CSV / TSV / NDJSON).
 * <p>
 * Cache miss → row count + column statistics absent; {@code sizeInBytes} still published when
 * derivable from {@code StorageObject.length()}. Cache hit → row count populated, plus
 * stream-derived {@code bytesRead} fed into {@code sizeInBytes} when {@code length()} was
 * unknown at the time of metadata resolution (stream-only compression sources), plus per-column
 * statistics for every column whose name resolves to an attribute in the schema.
 */
public final class TextFormatStats {

    private TextFormatStats() {}

    public static SourceStatistics build(
        Optional<ExternalStats.Stats> cachedStats,
        OptionalLong lengthDerivedSizeInBytes,
        List<Attribute> schema
    ) {
        OptionalLong rowCount;
        OptionalLong sizeInBytes;
        Map<String, SourceStatistics.ColumnStatistics> columns;
        if (cachedStats.isPresent()) {
            ExternalStats.Stats s = cachedStats.get();
            rowCount = OptionalLong.of(s.rowCount());
            sizeInBytes = lengthDerivedSizeInBytes.isPresent() ? lengthDerivedSizeInBytes : s.bytesRead();
            columns = buildColumns(s.columns(), schema);
        } else {
            rowCount = OptionalLong.empty();
            sizeInBytes = lengthDerivedSizeInBytes;
            columns = Map.of();
        }
        final OptionalLong rc = rowCount;
        final OptionalLong sb = sizeInBytes;
        final Map<String, SourceStatistics.ColumnStatistics> cols = columns;
        return new SourceStatistics() {
            @Override
            public OptionalLong rowCount() {
                return rc;
            }

            @Override
            public OptionalLong sizeInBytes() {
                return sb;
            }

            @Override
            public Optional<Map<String, ColumnStatistics>> columnStatistics() {
                return cols.isEmpty() ? Optional.empty() : Optional.of(cols);
            }
        };
    }

    private static Map<String, SourceStatistics.ColumnStatistics> buildColumns(
        Map<String, ExternalStats.ColumnStats> cached,
        List<Attribute> schema
    ) {
        if (cached.isEmpty() || schema == null || schema.isEmpty()) {
            return Map.of();
        }
        // The schema-name filter is intentional: only publish stats for columns the resolver will
        // see in the query schema. Stale entries (column dropped from schema between cold and warm)
        // get dropped here rather than leaking into sourceMetadata.
        Map<String, SourceStatistics.ColumnStatistics> out = new HashMap<>();
        for (Attribute a : schema) {
            ExternalStats.ColumnStats cs = cached.get(a.name());
            if (cs == null) {
                continue;
            }
            out.put(a.name(), toSpi(cs));
        }
        return out;
    }

    private static SourceStatistics.ColumnStatistics toSpi(ExternalStats.ColumnStats cs) {
        return new SourceStatistics.ColumnStatistics() {
            @Override
            public OptionalLong nullCount() {
                return OptionalLong.of(cs.nullCount());
            }

            @Override
            public OptionalLong valueCount() {
                return OptionalLong.of(cs.valueCount());
            }

            @Override
            public OptionalLong distinctCount() {
                return OptionalLong.empty();
            }

            @Override
            public Optional<Object> minValue() {
                return Optional.ofNullable(cs.min());
            }

            @Override
            public Optional<Object> maxValue() {
                return Optional.ofNullable(cs.max());
            }
        };
    }
}
