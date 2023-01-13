/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.ann.Experimental;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;

/**
 * Operator that computes the max value of a long field
 * and outputs a page at the end that contains that max value.
 * Only outputs page once all input pages are consumed.
 */
@Experimental
public class LongMaxOperator implements Operator {
    boolean finished;
    boolean returnedResult;
    long max = Long.MIN_VALUE;
    private final int channel;

    public LongMaxOperator(int channel) {
        this.channel = channel;
    }

    @Override
    public Page getOutput() {
        if (finished && returnedResult == false) {
            returnedResult = true;
            return new Page(LongBlock.newConstantBlockWith(max, 1));
        }
        return null;
    }

    @Override
    public boolean isFinished() {
        return finished && returnedResult;
    }

    @Override
    public void finish() {
        finished = true;
    }

    @Override
    public boolean needsInput() {
        return finished == false;
    }

    @Override
    public void addInput(Page page) {
        LongBlock block = page.getBlock(channel);
        for (int i = 0; i < block.getPositionCount(); i++) {
            max = Math.max(block.getLong(i), max);
        }
    }

    @Override
    public void close() {

    }
}
