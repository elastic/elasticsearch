/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.bytes.PagedBytesBuilder;
import org.elasticsearch.common.bytes.PagedBytesCursor;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasable;

import java.util.List;

/**
 * Encodes the values at a given position across multiple blocks into a single composite key.
 * Multivalued positions are serialized with list semantics: the value count is written first, then each value
 * in block iteration order. This means {@code [1, 2]} and {@code [2, 1]} produce different keys.
 * Null positions are encoded as a value count of zero.
 */
public class GroupKeyEncoder implements Accountable, Releasable {

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(GroupKeyEncoder.class);

    private final int[] groupChannels;
    private final ElementType[] elementTypes;
    private final PagedBytesBuilder row;
    private final PagedBytesCursor cursorScratch = new PagedBytesCursor();

    public GroupKeyEncoder(int[] groupChannels, List<ElementType> elementTypes, PagedBytesBuilder row) {
        this.groupChannels = groupChannels;
        this.elementTypes = new ElementType[groupChannels.length];
        for (int i = 0; i < groupChannels.length; i++) {
            this.elementTypes[i] = elementTypes.get(groupChannels[i]);
        }
        this.row = row;
    }

    /**
     * Encode the group key for the given position from the page into a {@link PagedBytesCursor}.
     * The returned cursor is only valid until the next call to {@code encode}.
     */
    public PagedBytesCursor encode(Page page, int position) {
        row.clear();
        for (int i = 0; i < groupChannels.length; i++) {
            Block block = page.getBlock(groupChannels[i]);
            encodeBlock(block, elementTypes[i], position);
        }
        return row.view(cursorScratch);
    }

    private void encodeBlock(Block block, ElementType type, int position) {
        if (block.isNull(position)) {
            row.appendVInt(0);
            return;
        }
        int firstValueIndex = block.getFirstValueIndex(position);
        int valueCount = block.getValueCount(position);
        row.appendVInt(valueCount);
        switch (type) {
            case INT -> {
                IntBlock b = (IntBlock) block;
                for (int v = 0; v < valueCount; v++) {
                    row.append(b.getInt(firstValueIndex + v));
                }
            }
            case LONG -> {
                LongBlock b = (LongBlock) block;
                for (int v = 0; v < valueCount; v++) {
                    row.append(b.getLong(firstValueIndex + v));
                }
            }
            case DOUBLE -> {
                DoubleBlock b = (DoubleBlock) block;
                for (int v = 0; v < valueCount; v++) {
                    row.append(Double.doubleToRawLongBits(b.getDouble(firstValueIndex + v)));
                }
            }
            case FLOAT -> {
                FloatBlock b = (FloatBlock) block;
                for (int v = 0; v < valueCount; v++) {
                    row.append(Float.floatToRawIntBits(b.getFloat(firstValueIndex + v)));
                }
            }
            case BOOLEAN -> {
                BooleanBlock b = (BooleanBlock) block;
                for (int v = 0; v < valueCount; v++) {
                    row.append(b.getBoolean(firstValueIndex + v) ? (byte) 1 : (byte) 0);
                }
            }
            case BYTES_REF -> {
                BytesRefBlock b = (BytesRefBlock) block;
                for (int v = 0; v < valueCount; v++) {
                    row.appendLengthPrefixed(b.get(firstValueIndex + v, cursorScratch));
                }
            }
            case NULL -> {
                // already handled by isNull above; nothing extra to write
            }
            default -> throw new IllegalArgumentException("unsupported element type for group key encoding: " + type);
        }
    }

    public int[] groupChannels() {
        return groupChannels;
    }

    @Override
    public long ramBytesUsed() {
        long size = SHALLOW_SIZE;
        size += RamUsageEstimator.sizeOf(groupChannels);
        size += RamUsageEstimator.shallowSizeOf(elementTypes);
        size += row.ramBytesUsed();
        size += PagedBytesCursor.SHALLOW_SIZE;
        return size;
    }

    @Override
    public void close() {
        row.close();
    }
}
