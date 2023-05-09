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
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;

public class PositionMergingSourceOperator extends MappingSourceOperator {
    public PositionMergingSourceOperator(SourceOperator delegate) {
        super(delegate);
    }

    @Override
    protected Page map(Page page) {
        Block[] merged = new Block[page.getBlockCount()];
        for (int b = 0; b < page.getBlockCount(); b++) {
            merged[b] = merge(b, page.getBlock(b));
        }
        return new Page(merged);
    }

    protected Block merge(int blockIndex, Block block) {
        Block.Builder builder = block.elementType().newBlockBuilder(block.getPositionCount());
        for (int p = 0; p + 1 < block.getPositionCount(); p += 2) {
            if (block.isNull(p) || block.isNull(p + 1)) {
                builder.appendNull();
                continue;
            }

            int firstCount = block.getValueCount(p);
            int secondCount = block.getValueCount(p + 1);
            if (firstCount + secondCount == 1) {
                if (firstCount == 1) {
                    builder.copyFrom(block, p, p + 1);
                } else {
                    builder.copyFrom(block, p + 1, p + 2);
                }
                continue;
            }

            builder.beginPositionEntry();
            copyTo(builder, block, p, firstCount);
            copyTo(builder, block, p + 1, secondCount);
            builder.endPositionEntry();
        }
        if (block.getPositionCount() % 2 == 1) {
            builder.copyFrom(block, block.getPositionCount() - 1, block.getPositionCount());
        }
        return builder.build();
    }

    private void copyTo(Block.Builder builder, Block in, int position, int valueCount) {
        int start = in.getFirstValueIndex(position);
        int end = start + valueCount;
        BytesRef scratch = new BytesRef();
        for (int i = start; i < end; i++) {
            switch (in.elementType()) {
                case BOOLEAN -> ((BooleanBlock.Builder) builder).appendBoolean(((BooleanBlock) in).getBoolean(i));
                case BYTES_REF -> ((BytesRefBlock.Builder) builder).appendBytesRef(((BytesRefBlock) in).getBytesRef(i, scratch));
                case DOUBLE -> ((DoubleBlock.Builder) builder).appendDouble(((DoubleBlock) in).getDouble(i));
                case INT -> ((IntBlock.Builder) builder).appendInt(((IntBlock) in).getInt(i));
                case LONG -> ((LongBlock.Builder) builder).appendLong(((LongBlock) in).getLong(i));
                default -> throw new IllegalArgumentException("unsupported type [" + in.elementType() + "]");
            }
        }
    }

}
