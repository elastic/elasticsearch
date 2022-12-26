/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;

import java.util.Arrays;

public class FilterOperator implements Operator {

    private final EvalOperator.ExpressionEvaluator evaluator;

    private Page lastInput;
    boolean finished = false;

    public record FilterOperatorFactory(EvalOperator.ExpressionEvaluator evaluator) implements OperatorFactory {

        @Override
        public Operator get() {
            return new FilterOperator(evaluator);
        }

        @Override
        public String describe() {
            return "FilterOperator()";
        }
    }

    public FilterOperator(EvalOperator.ExpressionEvaluator evaluator) {
        this.evaluator = evaluator;
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
    public void finish() {
        finished = true;
    }

    @Override
    public boolean isFinished() {
        return lastInput == null && finished;
    }

    @Override
    public Page getOutput() {
        if (lastInput == null) {
            return null;
        }

        int[] positions = new int[lastInput.getPositionCount()];
        int rowCount = 0;

        for (int i = 0; i < lastInput.getPositionCount(); i++) {
            if ((Boolean) evaluator.computeRow(lastInput, i)) {
                positions[rowCount++] = i;
            }
        }

        Page output;

        if (rowCount == 0) {
            output = null;
        } else if (rowCount == lastInput.getPositionCount()) {
            output = lastInput;
        } else {
            positions = Arrays.copyOf(positions, rowCount);

            Block[] filteredBlocks = new Block[lastInput.getBlockCount()];
            for (int i = 0; i < lastInput.getBlockCount(); i++) {
                filteredBlocks[i] = lastInput.getBlock(i).filter(positions);
            }

            output = new Page(filteredBlocks);
        }

        lastInput = null;
        return output;
    }

    @Override
    public void close() {}
}
