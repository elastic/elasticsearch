/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;

import java.util.Arrays;

import static org.elasticsearch.test.ESTestCase.between;

/**
 * Inserts nulls into the last block.
 */
public class NullInsertingSourceOperator extends MappingSourceOperator {
    public NullInsertingSourceOperator(SourceOperator delegate) {
        super(delegate);
    }

    @Override
    protected Page map(Page page) {
        if (page == null) {
            return null;
        }
        Block.Builder[] builders = new Block.Builder[page.getBlockCount()];
        for (int b = 0; b < builders.length; b++) {
            ElementType elementType = page.getBlock(b).elementType();
            switch (elementType) {
                case LONG:
                    builders[b] = LongBlock.newBlockBuilder(page.getPositionCount());
                    break;
                case DOUBLE:
                    builders[b] = DoubleBlock.newBlockBuilder(page.getPositionCount());
                    break;
                default:
                    throw new IllegalArgumentException("unknown block type " + elementType);
            }
        }
        for (int position = 0; position < page.getPositionCount(); position++) {
            for (int nulls = between(0, 3); nulls > 0; nulls--) {
                for (int b = 0; b < builders.length - 1; b++) {
                    copyValues(page.getBlock(b), position, builders[b]);
                }
                builders[builders.length - 1].appendNull();
            }
            for (int b = 0; b < builders.length; b++) {
                copyValues(page.getBlock(b), position, builders[b]);
            }
        }
        return new Page(page.getPositionCount(), Arrays.stream(builders).map(Block.Builder::build).toArray(Block[]::new));
    }

    private void copyValues(Block from, int position, Block.Builder into) {
        if (from.isNull(position)) {
            into.appendNull();
            return;
        }

        int valueCount = from.getValueCount(position);
        int firstValue = from.getFirstValueIndex(position);
        if (valueCount == 1) {
            copyValue(from, firstValue, into);
            return;
        }
        into.beginPositionEntry();
        int end = firstValue + valueCount;
        for (int valueIndex = firstValue; valueIndex < end; valueIndex++) {
            copyValue(from, valueIndex, into);
        }
        into.endPositionEntry();
    }

    private void copyValue(Block from, int valueIndex, Block.Builder into) {
        ElementType elementType = from.elementType();
        switch (elementType) {
            case LONG:
                ((LongBlock.Builder) into).appendLong(((LongBlock) from).getLong(valueIndex));
                break;
            case DOUBLE:
                ((DoubleBlock.Builder) into).appendDouble(((DoubleBlock) from).getDouble(valueIndex));
                break;
            default:
                throw new IllegalArgumentException("unknown block type " + elementType);
        }
    }
}
