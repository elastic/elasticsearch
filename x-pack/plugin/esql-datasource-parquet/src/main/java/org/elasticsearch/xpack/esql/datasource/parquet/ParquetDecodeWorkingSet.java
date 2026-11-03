/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.schema.PrimitiveType;
import org.elasticsearch.xpack.esql.datasources.spi.HeapFootprint;

import java.util.Set;

/**
 * Footer estimate of the Parquet decode working set that sits beside I/O buffers: grow-only
 * uncompressed page dest, dictionary-page copy, and decoded {@code value × width} blocks.
 * Tickets admit {@code I/O + this estimate} as a <em>concurrency cap</em> on how many groups
 * decode at once. The REQUEST circuit breaker remains the hard stop: dest/dict still charge
 * {@code parquet page decompression} / {@code parquet dictionary page copy}, and
 * {@code <esql_block_factory>} still charges assembled ES|QL blocks. Shortfalls
 * {@code forceAdd} and never wait.
 * <p>
 * {@code BINARY} dest is the uncompressed string payload, counted once. Extra BytesRef/block
 * copy overhead is not in this estimate; do not double-count uncompressed pages to cover it.
 */
final class ParquetDecodeWorkingSet {

    private ParquetDecodeWorkingSet() {}

    /**
     * Ticket size: coalesced I/O footprint plus {@link #estimateBytes}.
     */
    static long admitTotal(long ioBytes, long decodeBytes) {
        return addSaturating(Math.max(0L, ioBytes), Math.max(0L, decodeBytes));
    }

    static long admitBytes(BlockMetaData block, Set<String> projectedColumns) {
        return admitTotal(ColumnChunkPrefetcher.computePrefetchBytes(block, projectedColumns), estimateBytes(block, projectedColumns));
    }

    /**
     * Peak decode bytes for {@code projectedColumns} in {@code block}. Missing footer fields
     * contribute 0; the live dest/dict charges {@link ParquetDecodeBudget#consume} any shortfall.
     */
    static long estimateBytes(BlockMetaData block, Set<String> projectedColumns) {
        if (block == null || block.getColumns() == null || block.getColumns().isEmpty()) {
            return 0L;
        }
        long total = 0L;
        for (ColumnChunkMetaData column : block.getColumns()) {
            if (projectedColumns != null && projectedColumns.contains(column.getPath().toDotString()) == false) {
                continue;
            }
            total = addSaturating(total, uncompressedDestBytes(column));
            total = addSaturating(total, dictionaryCopyBytes(column));
            total = addSaturating(total, valueWidthBytes(column, block.getRowCount()));
        }
        return total;
    }

    private static long uncompressedDestBytes(ColumnChunkMetaData column) {
        long dest = column.getTotalUncompressedSize();
        if (dest <= 0L) {
            // Missing uncompressed footer still grows dest; floor from compressed so the
            // ticket does not collapse to I/O-only and later forceAdd over the cap.
            dest = column.getTotalSize();
        }
        if (dest <= 0L) {
            return 0L;
        }
        return HeapFootprint.byteArrayBytes(dest);
    }

    private static long dictionaryCopyBytes(ColumnChunkMetaData column) {
        if (column.hasDictionaryPage() == false) {
            return 0L;
        }
        long firstData = column.getFirstDataPageOffset();
        CoalescedRangeReader.ByteRange range = ColumnChunkPrefetcher.dictionaryPageRange(column, firstData);
        if (range == null || range.length() <= 0L) {
            return 0L;
        }
        return HeapFootprint.byteArrayBytes(range.length());
    }

    private static long valueWidthBytes(ColumnChunkMetaData column, long rowCount) {
        long values = column.getValueCount();
        if (values <= 0L) {
            values = rowCount;
        }
        if (values <= 0L) {
            return 0L;
        }
        int width = primitiveWidth(column);
        if (width < 0) {
            // BINARY values are the uncompressed pages already counted in uncompressedDestBytes.
            return 0L;
        }
        if (width > 0 && values > Long.MAX_VALUE / width) {
            return Long.MAX_VALUE;
        }
        return values * width;
    }

    /**
     * Fixed physical width in bytes, or {@code -1} for variable-width {@code BINARY} where the
     * values are the uncompressed pages.
     */
    @SuppressWarnings("deprecation")
    static int primitiveWidth(ColumnChunkMetaData column) {
        PrimitiveType type = column.getPrimitiveType();
        PrimitiveType.PrimitiveTypeName name = type != null ? type.getPrimitiveTypeName() : column.getType();
        return switch (name) {
            case BOOLEAN -> 1;
            case INT32, FLOAT -> 4;
            case INT64, DOUBLE -> 8;
            case INT96 -> 12;
            case FIXED_LEN_BYTE_ARRAY -> type != null && type.getTypeLength() > 0 ? type.getTypeLength() : 0;
            case BINARY -> -1;
        };
    }

    static long addSaturating(long left, long right) {
        long sum = left + right;
        if (left < 0L || right < 0L) {
            throw new IllegalArgumentException("decode working-set bytes must be non-negative");
        }
        return sum < 0L ? Long.MAX_VALUE : sum;
    }
}
