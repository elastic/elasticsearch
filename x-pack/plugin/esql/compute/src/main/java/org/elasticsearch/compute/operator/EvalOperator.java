/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.Experimental;
import org.elasticsearch.compute.data.BlockBuilder;
import org.elasticsearch.compute.data.Page;

@Experimental
public class EvalOperator implements Operator {

    private final ExpressionEvaluator evaluator;
    private final Class<? extends Number> dataType;

    boolean finished;

    Page lastInput;

    public record EvalOperatorFactory(ExpressionEvaluator evaluator, Class<? extends Number> dataType) implements OperatorFactory {

        @Override
        public Operator get() {
            return new EvalOperator(evaluator, dataType);
        }

        @Override
        public String describe() {
            return "EvalOperator(datatype = " + dataType + ")";
        }
    }

    public EvalOperator(ExpressionEvaluator evaluator, Class<? extends Number> dataType) {
        this.evaluator = evaluator;
        this.dataType = dataType;
    }

    @Override
    public Page getOutput() {
        if (lastInput == null) {
            return null;
        }
        Page lastPage;
        int rowsCount = lastInput.getPositionCount();
        if (dataType.equals(Long.TYPE)) {
            BlockBuilder blockBuilder = BlockBuilder.newLongBlockBuilder(rowsCount);
            for (int i = 0; i < rowsCount; i++) {
                Number result = (Number) evaluator.computeRow(lastInput, i);
                if (result == null) {
                    blockBuilder.appendNull();
                } else {
                    blockBuilder.appendLong(result.longValue());
                }
            }
            lastPage = lastInput.appendBlock(blockBuilder.build());
        } else if (dataType.equals(Double.TYPE)) {
            BlockBuilder blockBuilder = BlockBuilder.newDoubleBlockBuilder(rowsCount);
            for (int i = 0; i < lastInput.getPositionCount(); i++) {
                Number result = (Number) evaluator.computeRow(lastInput, i);
                if (result == null) {
                    blockBuilder.appendNull();
                } else {
                    blockBuilder.appendDouble(result.doubleValue());
                }
            }
            lastPage = lastInput.appendBlock(blockBuilder.build());
        } else {
            throw new UnsupportedOperationException();
        }
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
        sb.append("dataType=").append(dataType).append(", ");
        sb.append("evaluator=").append(evaluator);
        sb.append("]");
        return sb.toString();
    }

    public interface ExpressionEvaluator {
        Object computeRow(Page page, int position);
    }
}
