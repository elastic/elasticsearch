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
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.test.ESTestCase;

/**
 * A {@link SourceOperator} that inserts random garbage into data from another
 * {@link SourceOperator}. It also inserts an extra channel at the end of the page
 * containing a {@code boolean} column. If it is {@code true} then the data came
 * from the original operator. If it's {@code false} then the data is random
 * garbage inserted by this operator.
 */
public class AddGarbageRowsSourceOperator extends SourceOperator {
    public static EvalOperator.ExpressionEvaluator.Factory filterFactory() {
        /*
         * Grabs the filter from the last block. That's where we put it.
         */
        return ctx -> new EvalOperator.ExpressionEvaluator() {
            @Override
            public Block eval(Page page) {
                Block block = page.getBlock(page.getBlockCount() - 1);
                block.incRef();
                return block;
            }

            @Override
            public void close() {}
        };
    }

    private final SourceOperator next;

    public AddGarbageRowsSourceOperator(SourceOperator next) {
        this.next = next;
    }

    @Override
    public void finish() {
        next.finish();
    }

    @Override
    public boolean isFinished() {
        return next.isFinished();
    }

    @Override
    public Page getOutput() {
        Page page = next.getOutput();
        if (page == null) {
            return null;
        }
        Block.Builder[] newBlocks = new Block.Builder[page.getBlockCount() + 1];
        try {
            for (int b = 0; b < page.getBlockCount(); b++) {
                Block block = page.getBlock(b);
                newBlocks[b] = block.elementType().newBlockBuilder(page.getPositionCount(), block.blockFactory());
            }
            newBlocks[page.getBlockCount()] = page.getBlock(0).blockFactory().newBooleanBlockBuilder(page.getPositionCount());

            for (int p = 0; p < page.getPositionCount(); p++) {
                if (ESTestCase.randomBoolean()) {
                    insertGarbageRows(newBlocks, page);
                }
                copyPosition(newBlocks, page, p);
                if (ESTestCase.randomBoolean()) {
                    insertGarbageRows(newBlocks, page);
                }
            }

            return new Page(Block.Builder.buildAll(newBlocks));
        } finally {
            Releasables.close(Releasables.wrap(newBlocks), page::releaseBlocks);
        }
    }

    private void copyPosition(Block.Builder[] newBlocks, Page page, int p) {
        for (int b = 0; b < page.getBlockCount(); b++) {
            Block block = page.getBlock(b);
            newBlocks[b].copyFrom(block, p, p + 1);
        }
        signalKeep(newBlocks, true);
    }

    private void insertGarbageRows(Block.Builder[] newBlocks, Page page) {
        int count = ESTestCase.between(1, 5);
        for (int c = 0; c < count; c++) {
            insertGarbageRow(newBlocks, page);
        }
    }

    private void insertGarbageRow(Block.Builder[] newBlocks, Page page) {
        for (int b = 0; b < page.getBlockCount(); b++) {
            Block block = page.getBlock(b);
            switch (block.elementType()) {
                case BOOLEAN -> ((BooleanBlock.Builder) newBlocks[b]).appendBoolean(ESTestCase.randomBoolean());
                case BYTES_REF -> ((BytesRefBlock.Builder) newBlocks[b]).appendBytesRef(new BytesRef(ESTestCase.randomAlphaOfLength(5)));
                case COMPOSITE, DOC, UNKNOWN -> throw new UnsupportedOperationException();
                case INT -> ((IntBlock.Builder) newBlocks[b]).appendInt(ESTestCase.randomInt());
                case LONG -> ((LongBlock.Builder) newBlocks[b]).appendLong(ESTestCase.randomLong());
                case NULL -> newBlocks[b].appendNull();
                case DOUBLE -> ((DoubleBlock.Builder) newBlocks[b]).appendDouble(ESTestCase.randomDouble());
                case FLOAT -> ((FloatBlock.Builder) newBlocks[b]).appendFloat(ESTestCase.randomFloat());
            }
        }
        signalKeep(newBlocks, false);
    }

    private void signalKeep(Block.Builder[] newBlocks, boolean shouldKeep) {
        ((BooleanBlock.Builder) newBlocks[newBlocks.length - 1]).appendBoolean(shouldKeep);
    }

    @Override
    public void close() {
        next.close();
    }
}
