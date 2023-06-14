/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;

public class LimitOperator implements Operator {

    private int limit;

    private Page lastInput;

    private State state;

    private enum State {
        NEEDS_INPUT,
        FINISHING,
        FINISHED
    }

    public LimitOperator(int limit) {
        this.limit = limit;
        this.state = State.NEEDS_INPUT;
    }

    public record LimitOperatorFactory(int limit) implements OperatorFactory {

        @Override
        public Operator get(DriverContext driverContext) {
            return new LimitOperator(limit);
        }

        @Override
        public String describe() {
            return "LimitOperator[limit = " + limit + "]";
        }
    }

    @Override
    public boolean needsInput() {
        return lastInput == null && state == State.NEEDS_INPUT;
    }

    @Override
    public void addInput(Page page) {
        lastInput = page;
    }

    @Override
    public void finish() {
        if (lastInput == null) {
            this.state = State.FINISHED;
        } else {
            this.state = State.FINISHING;
        }
    }

    @Override
    public boolean isFinished() {
        return state == State.FINISHED;
    }

    @Override
    public Page getOutput() {
        if (lastInput == null || state == State.FINISHED) {
            return null;
        }

        Page result;
        if (lastInput.getPositionCount() <= limit) {
            result = lastInput;
            limit -= lastInput.getPositionCount();
            if (state == State.FINISHING) {
                state = State.FINISHED;
            }
        } else {
            int[] filter = new int[limit];
            for (int i = 0; i < limit; i++) {
                filter[i] = i;
            }
            Block[] blocks = new Block[lastInput.getBlockCount()];
            for (int b = 0; b < blocks.length; b++) {
                blocks[b] = lastInput.getBlock(b).filter(filter);
            }
            result = new Page(blocks);
            limit = 0;
            state = State.FINISHED;
        }

        lastInput = null;

        return result;
    }

    @Override
    public void close() {

    }
}
