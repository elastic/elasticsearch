/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.action.compute.operator;

import org.elasticsearch.xpack.sql.action.compute.data.Block;
import org.elasticsearch.xpack.sql.action.compute.data.LongBlock;
import org.elasticsearch.xpack.sql.action.compute.data.Page;

import java.util.function.LongFunction;

/**
 * Streaming operator that applies a long-value transformation to a given field
 */
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
        long[] newBlock = new long[block.getPositionCount()];
        for (int i = 0; i < block.getPositionCount(); i++) {
            newBlock[i] = longTransformer.apply(block.getLong(i));
        }
        Page lastPage = lastInput.appendColumn(new LongBlock(newBlock, block.getPositionCount()));
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
}
