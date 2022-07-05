/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.action.compute;

import java.util.function.IntFunction;

public class IntTransformer implements Operator {

    private final int channel;
    private final IntFunction<Integer> intTransformer;

    boolean finished;

    Page lastInput;

    public IntTransformer(int channel, IntFunction<Integer> intTransformer) {
        this.channel = channel;
        this.intTransformer = intTransformer;
    }

    @Override
    public Page getOutput() {
        if (lastInput == null) {
            return null;
        }
        Block block = lastInput.getBlock(channel);
        int[] newBlock = new int[block.getPositionCount()];
        for (int i = 0; i < block.getPositionCount(); i++) {
            newBlock[i] = intTransformer.apply(block.getInt(i));
        }
        return lastInput.appendColumn(new IntBlock(newBlock, block.getPositionCount()));
    }

    @Override
    public boolean isFinished() {
        return finished;
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
        lastInput = page;
    }
}
