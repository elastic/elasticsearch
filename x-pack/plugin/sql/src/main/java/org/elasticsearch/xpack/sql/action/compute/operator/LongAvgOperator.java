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

public class LongAvgOperator implements Operator {
    boolean finished;
    boolean returnedResult;
    long count;
    long sum;
    private final int channel;

    public LongAvgOperator(int channel) {
        this.channel = channel;

    }

    @Override
    public void close() { /* no-op */ }

    @Override
    public Page getOutput() {
        if (finished && returnedResult == false) {
            returnedResult = true;
            return new Page(new LongBlock(new long[] { sum / count }, 1));
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
        Block block = page.getBlock(channel);
        for (int i = 0; i < block.getPositionCount(); i++) {
            sum += block.getLong(i);
        }
        count += block.getPositionCount();
    }
}
