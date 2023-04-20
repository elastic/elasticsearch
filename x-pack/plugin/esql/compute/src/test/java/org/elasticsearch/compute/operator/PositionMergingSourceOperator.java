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
            Block in = page.getBlock(b);
            Block.Builder builder = in.elementType().newBlockBuilder(page.getPositionCount());
            for (int p = 0; p + 1 < page.getPositionCount(); p += 2) {
                if (in.isNull(p) || in.isNull(p + 1)) {
                    builder.appendNull();
                    continue;
                }

                int firstCount = in.getValueCount(p);
                int secondCount = in.getValueCount(p + 1);
                if (firstCount + secondCount == 1) {
                    if (firstCount == 1) {
                        builder.copyFrom(in, p, p + 1);
                    } else {
                        builder.copyFrom(in, p + 1, p + 2);
                    }
                    continue;
                }

                builder.beginPositionEntry();
                copyTo(builder, in, p, firstCount);
                copyTo(builder, in, p + 1, secondCount);
                builder.endPositionEntry();
            }
            if (page.getPositionCount() % 2 == 1) {
                builder.copyFrom(in, page.getPositionCount() - 1, page.getPositionCount());
            }
            merged[b] = builder.build();
        }
        return new Page(merged);
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
