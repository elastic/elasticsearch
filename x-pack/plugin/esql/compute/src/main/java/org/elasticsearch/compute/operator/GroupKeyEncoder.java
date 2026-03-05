/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.topn.DefaultUnsortableTopNEncoder;
import org.elasticsearch.compute.operator.topn.TopNEncoder;
import org.elasticsearch.core.Releasable;

import java.util.List;

/**
 * Encodes the values at a given position across multiple blocks into a single {@link BytesRef} composite key.
 * Multivalued positions are serialized with list semantics: the value count is written first, then each value
 * in block iteration order. This means {@code [1, 2]} and {@code [2, 1]} produce different keys.
 * Null positions are encoded as a value count of zero.
 */
public class GroupKeyEncoder implements Releasable {

    private static final DefaultUnsortableTopNEncoder encoder = TopNEncoder.DEFAULT_UNSORTABLE;

    private final int[] groupChannels;
    private final ElementType[] elementTypes;
    private final BreakingBytesRefBuilder scratch;
    private final BytesRef scratchBytesRef = new BytesRef();

    public GroupKeyEncoder(int[] groupChannels, List<ElementType> elementTypes, BreakingBytesRefBuilder scratch) {
        this.groupChannels = groupChannels;
        this.elementTypes = new ElementType[groupChannels.length];
        for (int i = 0; i < groupChannels.length; i++) {
            this.elementTypes[i] = elementTypes.get(groupChannels[i]);
        }
        this.scratch = scratch;
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
        return scratch.bytesRefView();
    }

    private void encodeBlock(Block block, ElementType type, int position) {
        if (block.isNull(position)) {
            encoder.encodeVInt(0, scratch);
            return;
        }
        int firstValueIndex = block.getFirstValueIndex(position);
        int valueCount = block.getValueCount(position);
        encoder.encodeVInt(valueCount, scratch);
        switch (type) {
            case INT -> {
                IntBlock b = (IntBlock) block;
                for (int v = 0; v < valueCount; v++) {
                    encoder.encodeInt(b.getInt(firstValueIndex + v), scratch);
                }
            }
            case LONG -> {
                LongBlock b = (LongBlock) block;
                for (int v = 0; v < valueCount; v++) {
                    encoder.encodeLong(b.getLong(firstValueIndex + v), scratch);
                }
            }
            case DOUBLE -> {
                DoubleBlock b = (DoubleBlock) block;
                for (int v = 0; v < valueCount; v++) {
                    encoder.encodeDouble(b.getDouble(firstValueIndex + v), scratch);
                }
            }
            case FLOAT -> {
                FloatBlock b = (FloatBlock) block;
                for (int v = 0; v < valueCount; v++) {
                    encoder.encodeFloat(b.getFloat(firstValueIndex + v), scratch);
                }
            }
            case BOOLEAN -> {
                BooleanBlock b = (BooleanBlock) block;
                for (int v = 0; v < valueCount; v++) {
                    encoder.encodeBoolean(b.getBoolean(firstValueIndex + v), scratch);
                }
            }
            case BYTES_REF -> {
                BytesRefBlock b = (BytesRefBlock) block;
                for (int v = 0; v < valueCount; v++) {
                    BytesRef ref = b.getBytesRef(firstValueIndex + v, scratchBytesRef);
                    encoder.encodeBytesRef(ref, scratch);
                }
            }
            case NULL -> {
                // already handled by isNull above; nothing extra to write
            }
            default -> throw new IllegalArgumentException("unsupported element type for group key encoding: " + type);
        }
    }

    @Override
    public void close() {
        scratch.close();
    }
}
