/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.PagedBytesBuilder;
import org.elasticsearch.common.util.BytesRefHashTable;
import org.elasticsearch.common.util.PageCacheRecycler;
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
 * Encodes the values at a given position across multiple blocks into a composite key and
 * adds it to a {@link BytesRefHashTable}. Multivalued positions are serialized with list
 * semantics: the value count is written first, then each value in block iteration order.
 * This means {@code [1, 2]} and {@code [2, 1]} produce different keys.
 * Null positions are encoded as a value count of zero.
 */
public class GroupKeyEncoder implements Accountable, Releasable {

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(GroupKeyEncoder.class);

    private final int[] groupChannels;
    private final ElementType[] elementTypes;
    private final PagedBytesBuilder scratch;
    private final BytesRef scratchBytesRef = new BytesRef();

    public GroupKeyEncoder(int[] groupChannels, List<ElementType> elementTypes, CircuitBreaker breaker, PageCacheRecycler recycler) {
        this.groupChannels = groupChannels;
        this.elementTypes = new ElementType[groupChannels.length];
        for (int i = 0; i < groupChannels.length; i++) {
            this.elementTypes[i] = elementTypes.get(groupChannels[i]);
        }
        this.scratch = new PagedBytesBuilder(recycler, breaker, "group-key-encoder", 0);
    }

    /**
     * Encodes the group key for the given position from the page and adds it to {@code table}.
     * Returns the same ordinal semantics as {@link BytesRefHashTable#add(BytesRef)}.
     */
    public long encodeAndAdd(Page page, int position, BytesRefHashTable table) {
        scratch.clear();
        for (int i = 0; i < groupChannels.length; i++) {
            encodeBlock(page.getBlock(groupChannels[i]), elementTypes[i], position);
        }
        return table.add(scratch.view());
    }

    private void encodeBlock(Block block, ElementType type, int position) {
        if (block.isNull(position)) {
            scratch.appendVInt(0);
            return;
        }
        int firstValueIndex = block.getFirstValueIndex(position);
        int valueCount = block.getValueCount(position);
        scratch.appendVInt(valueCount);
        switch (type) {
            case INT -> {
                IntBlock b = (IntBlock) block;
                for (int v = 0; v < valueCount; v++) {
                    scratch.append(b.getInt(firstValueIndex + v));
                }
            }
            case LONG -> {
                LongBlock b = (LongBlock) block;
                for (int v = 0; v < valueCount; v++) {
                    scratch.append(b.getLong(firstValueIndex + v));
                }
            }
            case DOUBLE -> {
                DoubleBlock b = (DoubleBlock) block;
                for (int v = 0; v < valueCount; v++) {
                    scratch.append(Double.doubleToRawLongBits(b.getDouble(firstValueIndex + v)));
                }
            }
            case FLOAT -> {
                FloatBlock b = (FloatBlock) block;
                for (int v = 0; v < valueCount; v++) {
                    scratch.append(Float.floatToRawIntBits(b.getFloat(firstValueIndex + v)));
                }
            }
            case BOOLEAN -> {
                BooleanBlock b = (BooleanBlock) block;
                for (int v = 0; v < valueCount; v++) {
                    scratch.append(b.getBoolean(firstValueIndex + v) ? (byte) 1 : (byte) 0);
                }
            }
            case BYTES_REF -> {
                BytesRefBlock b = (BytesRefBlock) block;
                for (int v = 0; v < valueCount; v++) {
                    BytesRef ref = b.getBytesRef(firstValueIndex + v, scratchBytesRef);
                    scratch.appendVInt(ref.length);
                    scratch.append(ref);
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
        size += scratch.ramBytesUsed();
        size += RamUsageEstimator.shallowSizeOfInstance(BytesRef.class);
        return size;
    }

    @Override
    public void close() {
        scratch.close();
    }
}
