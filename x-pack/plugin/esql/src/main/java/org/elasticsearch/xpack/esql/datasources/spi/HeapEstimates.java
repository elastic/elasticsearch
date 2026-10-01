/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.core.Nullable;

import java.util.Map;
import java.util.Optional;

/**
 * Heap weights for the caches and planning reservations that hold datasource values. Lives in {@code spi} so the
 * cache, the {@link FileList} implementations, and the resolver can charge the same estimate without the SPI
 * depending on cache internals.
 */
public final class HeapEstimates {

    /** Flat per-entry charge shared by the map and statistics estimates. Not a measured size. */
    private static final long MAP_ENTRY_BYTES = 100L;

    /**
     * Allowance for a {@code ReferenceAttribute} plus its {@code NameId}, excluding the column name. Not a measured
     * deep size.
     */
    private static final long COLUMN_SHELL_BYTES = 128L;

    private HeapEstimates() {}

    /**
     * Heap one schema column keeps reachable: the attribute shell plus its name. The name is charged because a
     * flattened nested field is named by its whole dotted path, so a column's name can dwarf the rest of it and a
     * schema's size is then the sum of its names, not its column count.
     *
     * @param nameLength {@code String#length()} of the column name
     */
    public static long columnBytes(int nameLength) {
        return COLUMN_SHELL_BYTES + 40 + nameLength * (long) Character.BYTES;
    }

    /**
     * About 40 bytes for the {@code String} object and its backing array headers on a 64-bit JVM with compressed
     * references, plus two bytes per character. Both parts round up on purpose (compact Latin-1 strings use one byte
     * per character); this feeds a cache budget, where over-counting evicts a little early and under-counting lets the
     * cache outgrow its budget.
     */
    public static long stringBytes(@Nullable String s) {
        return 40 + (s != null ? s.length() * (long) Character.BYTES : 0);
    }

    /**
     * About 40 bytes for the {@link BytesRef} object and its backing array headers, plus the live byte length.
     * Keyword extrema harvested from text are stored as {@code BytesRef}s; under-counting them lets the schema cache
     * retain unbounded text against a fixed byte budget.
     */
    public static long bytesRefBytes(@Nullable BytesRef bytes) {
        return 40 + (bytes != null ? bytes.length : 0L);
    }

    /**
     * Shape charge (~100B per entry) plus payload for {@link String} / {@link BytesRef} values. Fixed-size
     * values (numbers, booleans) stay on the flat constant alone. Nested maps recurse one level for
     * per-stripe statistics; deeper nesting is not expected in this metadata map.
     */
    public static long mapBytes(@Nullable Map<String, Object> map) {
        if (map == null) {
            return 0L;
        }
        long bytes = mapShapeBytes(map);
        for (Object value : map.values()) {
            bytes += valuePayloadBytes(value);
            if (value instanceof Map<?, ?> nested) {
                bytes += mapShapeBytes(nested);
                for (Object nestedValue : nested.values()) {
                    bytes += valuePayloadBytes(nestedValue);
                }
            }
        }
        return bytes;
    }

    /**
     * The ~100B per-entry shape charge of {@link #mapBytes} without any value payload. For a shallow copy of a map
     * ({@code Map.copyOf}, {@code new HashMap<>(other)}): the copy owns its entries, but its keys and values are the
     * same objects as the original's, so charging their text again counts each string once per copy.
     */
    public static long mapShapeBytes(@Nullable Map<?, ?> map) {
        return map == null ? 0L : MAP_ENTRY_BYTES * map.size();
    }

    /**
     * A {@link SourceStatistics} held in memory: a shell, plus per column the same ~100B entry charge as
     * {@link #mapBytes}, the column name, and the payload of variable-width extrema. Not a measured deep size.
     */
    public static long statisticsBytes(SourceStatistics statistics) {
        long bytes = 64L;
        Optional<Map<String, SourceStatistics.ColumnStatistics>> columns = statistics.columnStatistics();
        if (columns != null && columns.isPresent()) {
            for (Map.Entry<String, SourceStatistics.ColumnStatistics> column : columns.get().entrySet()) {
                bytes += MAP_ENTRY_BYTES + stringBytes(column.getKey());
                SourceStatistics.ColumnStatistics stats = column.getValue();
                if (stats != null) {
                    bytes += valuePayloadBytes(stats.minValue().orElse(null));
                    bytes += valuePayloadBytes(stats.maxValue().orElse(null));
                }
            }
        }
        return bytes;
    }

    private static long valuePayloadBytes(@Nullable Object value) {
        if (value instanceof String s) {
            return stringBytes(s);
        }
        if (value instanceof BytesRef bytesRef) {
            return bytesRefBytes(bytesRef);
        }
        return 0L;
    }
}
