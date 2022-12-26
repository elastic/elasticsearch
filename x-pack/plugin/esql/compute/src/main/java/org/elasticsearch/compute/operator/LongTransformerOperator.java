/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.Experimental;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockBuilder;
import org.elasticsearch.compute.data.Page;

import java.util.function.LongFunction;

/**
 * Streaming operator that applies a long-value transformation to a given field
 */
@Experimental
public class LongTransformerOperator implements Operator {

    private final int channel;
    private final LongFunction<Long> longTransformer;

    boolean finished;

    Page lastInput;

    public LongTransformerOperator(int channel, LongFunction<Long> longTransformer) {
        this.channel = channel;
        this.longTransformer = longTransformer;
    }

    @Override
    public Page getOutput() {
        if (lastInput == null) {
            return null;
        }
        Block block = lastInput.getBlock(channel);
        BlockBuilder blockBuilder = BlockBuilder.newLongBlockBuilder(block.getPositionCount());
        for (int i = 0; i < block.getPositionCount(); i++) {
            blockBuilder.appendLong(longTransformer.apply(block.getLong(i)));
        }
        Page lastPage = lastInput.appendBlock(blockBuilder.build());
        lastInput = null;
        return lastPage;
    }

    @Override
    public boolean isFinished() {
        return lastInput == null && finished;
    }

    @Override
    public void finish() {
        finished = true;
    }

    @Override
    public boolean needsInput() {
        return lastInput == null && finished == false;
    }

    @Override
    public void addInput(Page page) {
        lastInput = page;
    }

    @Override
    public void close() {

    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.getClass().getSimpleName()).append("[");
        sb.append("channel=").append(channel).append(", ");
        sb.append("longTransformer=").append(longTransformer);
        sb.append("]");
        return sb.toString();
    }
}
