/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import org.apache.lucene.util.BytesRef;
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

    /**
     * The widest text extremum a read publishes, in bytes. Every contribution a scan stages, and every record
     * the statistics store keeps, carries a copy of both extrema of every harvested column, so without a limit
     * one long value in each column sets the size of all of them.
     */
    static final int MAX_EXTREMUM_BYTES = 1024;

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
        return statistics(rowCount, sizeInBytes, columns);
    }

    /**
     * The statistics of one already-filtered harvest, with no schema applied. The schema filter belongs at
     * the harvest — {@link StripeStatsHarvester#emit} runs {@link #build} before a piece is handed on — so a
     * consumer holding a filtered harvest reads its columns from the harvest itself. Applying the schema a
     * second time there would make the columns a record COUNTS and the columns it WRITES come from two
     * different places.
     */
    static SourceStatistics harvested(long rowCount, Map<String, ExternalStats.ColumnStats> columns) {
        Map<String, SourceStatistics.ColumnStatistics> cols = new HashMap<>();
        for (Map.Entry<String, ExternalStats.ColumnStats> entry : columns.entrySet()) {
            cols.put(entry.getKey(), toSpi(entry.getValue()));
        }
        return statistics(OptionalLong.of(rowCount), OptionalLong.empty(), cols);
    }

    private static SourceStatistics statistics(
        OptionalLong rowCount,
        OptionalLong sizeInBytes,
        Map<String, SourceStatistics.ColumnStatistics> columns
    ) {
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
                return Optional.ofNullable(publishable(cs.min()));
            }

            @Override
            public Optional<Object> maxValue() {
                return Optional.ofNullable(publishable(cs.max()));
            }
        };
    }

    /**
     * {@code extremum}, or {@code null} when it is wider than {@link #MAX_EXTREMUM_BYTES}. The limit is on
     * the stored {@link BytesRef}, so it covers {@code KEYWORD}, {@code TEXT} and {@code IP} alike; an
     * address is 16 bytes and never reaches it. Counts are unaffected, and so is the other end of the same
     * column — a withheld end is answered by a scan, and a fold marks that end unservable rather than
     * answer it from the reads that did publish one.
     * <p>
     * This is the publish side. {@link #build} calls its parameter {@code cachedStats}, but what it is
     * handed is a measurement the current read just made; a retained record is read back through
     * {@code SourceStatisticsSerializer}'s flat map, which does not come through here. So a wide extremum
     * already in the store is served until its entry turns over.
     */
    static Object publishable(Object extremum) {
        assert extremum instanceof CharSequence == false
            : "a text extremum must reach the cap as a BytesRef; [" + extremum.getClass().getName() + "] would pass uncapped";
        return extremum instanceof BytesRef bytes && bytes.length > MAX_EXTREMUM_BYTES ? null : extremum;
    }
}
