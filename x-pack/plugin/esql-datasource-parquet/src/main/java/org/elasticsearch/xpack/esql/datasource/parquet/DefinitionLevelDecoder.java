/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridDecoder;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.BitSet;

/**
 * Bulk decoder for Parquet definition levels, producing a {@link BitSet} of null positions
 * for nullable flat columns. Delegates to parquet-java's {@link RunLengthBitPackingHybridDecoder}
 * for the actual RLE hybrid stream decoding.
 *
 * <p>For non-nullable columns ({@code maxDefLevel == 0}), no def-level section exists in the
 * page and all operations are no-ops.
 */
final class DefinitionLevelDecoder {

    private boolean nonNullable;
    private int maxDefLevel;
    private RunLengthBitPackingHybridDecoder rleDecoder;

    DefinitionLevelDecoder() {}

    void init(ByteBuffer defLevelBytes, int valueCount, int maxDefLevel, boolean hasLengthPrefix) {
        this.maxDefLevel = maxDefLevel;
        if (maxDefLevel <= 0) {
            this.nonNullable = true;
            this.rleDecoder = null;
            return;
        }
        this.nonNullable = false;
        int bitWidth = 32 - Integer.numberOfLeadingZeros(maxDefLevel);
        ByteBuffer source = defLevelBytes.duplicate().order(ByteOrder.LITTLE_ENDIAN);
        if (hasLengthPrefix) {
            int payloadLen = source.getInt();
            byte[] payload = new byte[payloadLen];
            source.get(payload);
            this.rleDecoder = new RunLengthBitPackingHybridDecoder(bitWidth, new ByteArrayInputStream(payload));
        } else {
            byte[] data = new byte[source.remaining()];
            source.get(data);
            this.rleDecoder = new RunLengthBitPackingHybridDecoder(bitWidth, new ByteArrayInputStream(data));
        }
    }

    /**
     * Decodes the next {@code count} definition levels, setting null positions in {@code nulls}
     * starting at {@code offset}. Returns the number of non-null values.
     */
    int readBatch(int count, BitSet nulls, int offset) {
        if (nonNullable) {
            return count;
        }
        if (count == 0) {
            return 0;
        }
        int nonNull = 0;
        try {
            for (int i = 0; i < count; i++) {
                int def = rleDecoder.readInt();
                if (def < maxDefLevel) {
                    nulls.set(offset + i);
                } else {
                    nonNull++;
                }
            }
        } catch (IOException e) {
            throw new QlIllegalArgumentException("Failed to read definition levels: " + e.getMessage(), e);
        }
        return nonNull;
    }

    /**
     * Decodes {@code count} definition levels but only populates null positions for selected rows.
     * Returns the number of non-null values among selected rows (needed to size the value output array).
     * The RLE stream is advanced for all {@code count} rows regardless of selection.
     *
     * <p>Null bit positions in {@code nulls} are set relative to output position (starting at
     * {@code outOffset}), not input position — matching the compacted output array layout.
     */
    int readBatchSelective(int count, BitSet nulls, int outOffset, RowSelection selection) {
        if (nonNullable) {
            return selection.selectedCount();
        }
        if (count == 0) {
            return 0;
        }
        int nonNullSelected = 0;
        int outPos = outOffset;
        int selIdx = selection.nextSelected(0);
        try {
            for (int i = 0; i < count; i++) {
                int def = rleDecoder.readInt();
                if (i == selIdx) {
                    if (def < maxDefLevel) {
                        nulls.set(outPos);
                    } else {
                        nonNullSelected++;
                    }
                    outPos++;
                    selIdx = selection.nextSelected(selIdx + 1);
                }
            }
        } catch (IOException e) {
            throw new QlIllegalArgumentException("Failed to read definition levels selectively: " + e.getMessage(), e);
        }
        return nonNullSelected;
    }

    /**
     * Skips {@code count} definition levels without materializing values.
     * Returns the number of non-null values that were skipped.
     */
    int skip(int count) {
        if (nonNullable || count == 0) {
            return count;
        }
        int nonNull = 0;
        try {
            for (int i = 0; i < count; i++) {
                int def = rleDecoder.readInt();
                if (def >= maxDefLevel) {
                    nonNull++;
                }
            }
        } catch (IOException e) {
            throw new QlIllegalArgumentException("Failed to skip definition levels: " + e.getMessage(), e);
        }
        return nonNull;
    }
}
