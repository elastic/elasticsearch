/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.Experimental;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockBuilder;
import org.elasticsearch.compute.data.Page;

import java.util.function.LongFunction;

/**
 * Streaming operator that applies a double-value transformation to a given long field
 */
@Experimental
public class DoubleTransformerOperator implements Operator {

    private final int channel;
    private final LongFunction<Double> doubleTransformer;

    boolean finished;

    Page lastInput;

    public static class DoubleTransformerOperatorFactory implements OperatorFactory {

        private final int channel;

        private final LongFunction<Double> doubleTransformer;

        public DoubleTransformerOperatorFactory(int channel, LongFunction<Double> doubleTransformer) {
            this.channel = channel;
            this.doubleTransformer = doubleTransformer;
        }

        @Override
        public Operator get() {
            return new DoubleTransformerOperator(channel, doubleTransformer);
        }

        @Override
        public String describe() {
            return "DoubleTransformerOperator(channel = " + channel + ")";
        }
    }

    public DoubleTransformerOperator(int channel, LongFunction<Double> doubleTransformer) {
        this.channel = channel;
        this.doubleTransformer = doubleTransformer;
    }

    @Override
    public Page getOutput() {
        if (lastInput == null) {
            return null;
        }
        Block block = lastInput.getBlock(channel);
        int len = block.getPositionCount();
        BlockBuilder blockBuilder = BlockBuilder.newDoubleBlockBuilder(len);
        for (int i = 0; i < block.getPositionCount(); i++) {
            blockBuilder.appendDouble(doubleTransformer.apply(block.getLong(i)));
        }
        Page lastPage = lastInput.replaceBlock(channel, blockBuilder.build());
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
        sb.append("doubleTransformer=").append(doubleTransformer);
        sb.append("]");
        return sb.toString();
    }
}
