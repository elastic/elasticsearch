/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.Experimental;
import org.elasticsearch.compute.data.DoubleArrayBlock;
import org.elasticsearch.compute.data.LongArrayBlock;
import org.elasticsearch.compute.data.Page;

@Experimental
public class EvalOperator implements Operator {

    private final ExpressionEvaluator evaluator;
    private final Class<? extends Number> dataType;

    boolean finished;

    Page lastInput;

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
        if (dataType.equals(Long.TYPE)) {
            long[] newBlock = new long[lastInput.getPositionCount()];
            for (int i = 0; i < lastInput.getPositionCount(); i++) {
                newBlock[i] = ((Number) evaluator.computeRow(lastInput, i)).longValue();
            }
            lastPage = lastInput.appendBlock(new LongArrayBlock(newBlock, lastInput.getPositionCount()));
        } else if (dataType.equals(Double.TYPE)) {
            double[] newBlock = new double[lastInput.getPositionCount()];
            for (int i = 0; i < lastInput.getPositionCount(); i++) {
                newBlock[i] = ((Number) evaluator.computeRow(lastInput, i)).doubleValue();
            }
            lastPage = lastInput.appendBlock(new DoubleArrayBlock(newBlock, lastInput.getPositionCount()));
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

    public interface ExpressionEvaluator {
        Object computeRow(Page page, int position);
    }
}
