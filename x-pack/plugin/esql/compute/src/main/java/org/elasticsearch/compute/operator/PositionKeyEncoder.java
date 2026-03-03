/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;

import java.util.List;

/**
 * Encodes the values at a given position across multiple blocks into a single {@link BytesRef} composite key.
 * Multivalued positions are serialized with list semantics: the value count is written first, then each value
 * in block iteration order. This means {@code [1, 2]} and {@code [2, 1]} produce different keys.
 * Null positions are encoded as a value count of zero.
 */
public class PositionKeyEncoder implements Accountable {

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(PositionKeyEncoder.class);

    private final int[] groupChannels;
    private final ElementType[] elementTypes;
    private final BytesRefBuilder scratch = new BytesRefBuilder();
    private final BytesRef scratchBytesRef = new BytesRef();

    public PositionKeyEncoder(int[] groupChannels, List<ElementType> elementTypes) {
        this.groupChannels = groupChannels;
        this.elementTypes = new ElementType[groupChannels.length];
        for (int i = 0; i < groupChannels.length; i++) {
            this.elementTypes[i] = elementTypes.get(groupChannels[i]);
        }
    }

    /**
     * Encode the group key for the given position from the page into a {@link BytesRef}.
     * The returned reference is only valid until the next call to {@code encode}.
     */
    public BytesRef encode(Page page, int position) {
        scratch.clear();
        for (int i = 0; i < groupChannels.length; i++) {
            Block block = page.getBlock(groupChannels[i]);
            encodeBlock(block, elementTypes[i], position);
        }
        return scratch.get();
    }

    private void encodeBlock(Block block, ElementType type, int position) {
        if (block.isNull(position)) {
            writeVInt(0);
            return;
        }
        int firstValueIndex = block.getFirstValueIndex(position);
        int valueCount = block.getValueCount(position);
        writeVInt(valueCount);
        switch (type) {
            case INT -> {
                IntBlock b = (IntBlock) block;
                for (int v = 0; v < valueCount; v++) {
                    writeInt(b.getInt(firstValueIndex + v));
                }
            }
            case LONG -> {
                LongBlock b = (LongBlock) block;
                for (int v = 0; v < valueCount; v++) {
                    writeLong(b.getLong(firstValueIndex + v));
                }
            }
            case DOUBLE -> {
                DoubleBlock b = (DoubleBlock) block;
                for (int v = 0; v < valueCount; v++) {
                    writeLong(Double.doubleToLongBits(b.getDouble(firstValueIndex + v)));
                }
            }
            case FLOAT -> {
                FloatBlock b = (FloatBlock) block;
                for (int v = 0; v < valueCount; v++) {
                    writeInt(Float.floatToIntBits(b.getFloat(firstValueIndex + v)));
                }
            }
            case BOOLEAN -> {
                BooleanBlock b = (BooleanBlock) block;
                for (int v = 0; v < valueCount; v++) {
                    scratch.append((byte) (b.getBoolean(firstValueIndex + v) ? 1 : 0));
                }
            }
            case BYTES_REF -> {
                BytesRefBlock b = (BytesRefBlock) block;
                for (int v = 0; v < valueCount; v++) {
                    BytesRef ref = b.getBytesRef(firstValueIndex + v, scratchBytesRef);
                    writeVInt(ref.length);
                    scratch.append(ref.bytes, ref.offset, ref.length);
                }
            }
            case NULL -> {
                // already handled by isNull above; nothing extra to write
            }
            default -> throw new IllegalArgumentException("unsupported element type for group key encoding: " + type);
        }
    }

    private void writeVInt(int value) {
        while ((value & ~0x7F) != 0) {
            scratch.append((byte) ((value & 0x7F) | 0x80));
            value >>>= 7;
        }
        scratch.append((byte) value);
    }

    private void writeInt(int value) {
        scratch.append((byte) (value >> 24));
        scratch.append((byte) (value >> 16));
        scratch.append((byte) (value >> 8));
        scratch.append((byte) value);
    }

    private void writeLong(long value) {
        writeInt((int) (value >> 32));
        writeInt((int) value);
    }

    @Override
    public long ramBytesUsed() {
        long size = SHALLOW_SIZE;
        size += RamUsageEstimator.sizeOf(groupChannels);
        size += RamUsageEstimator.shallowSizeOf(elementTypes);
        size += RamUsageEstimator.shallowSizeOfInstance(BytesRefBuilder.class);
        size += RamUsageEstimator.sizeOf(scratch.bytes());
        size += RamUsageEstimator.shallowSizeOfInstance(BytesRef.class);
        return size;
    }
}
