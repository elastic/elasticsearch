/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.action.compute;

public class MaxOperator implements Operator {
    boolean finished;
    boolean returnedResult;
    int max = Integer.MIN_VALUE;
    private final int channel;

    public MaxOperator(int channel) {
        this.channel = channel;
    }

    @Override
    public Page getOutput() {
        if (finished && returnedResult == false) {
            returnedResult = true;
            return new Page(new IntBlock(new int[] {max}, 1));
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
            max = Math.max(block.getInt(i), max);
        }
    }
}
