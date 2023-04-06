/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.compute.ann.Experimental;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;

import java.util.function.Supplier;

@Experimental
public class ColumnExtractOperator implements Operator {

    public record Factory(
        ElementType[] types,
        Supplier<EvalOperator.ExpressionEvaluator> inputEvalSupplier,
        Supplier<ColumnExtractOperator.Evaluator> evaluatorSupplier
    ) implements OperatorFactory {

        @Override
        public Operator get() {
            return new ColumnExtractOperator(types, inputEvalSupplier.get(), evaluatorSupplier.get());
        }

        @Override
        public String describe() {
            return "ColumnExtractOperator[evaluator=" + evaluatorSupplier.get() + "]";
        }
    }

    private final ElementType[] types;
    private final EvalOperator.ExpressionEvaluator inputEvaluator;
    private final ColumnExtractOperator.Evaluator evaluator;

    boolean finished;

    Page lastInput;

    public ColumnExtractOperator(
        ElementType[] types,
        EvalOperator.ExpressionEvaluator inputEvaluator,
        ColumnExtractOperator.Evaluator evaluator
    ) {
        this.types = types;
        this.inputEvaluator = inputEvaluator;
        this.evaluator = evaluator;
    }

    @Override
    public Page getOutput() {
        if (lastInput == null) {
            return null;
        }

        int rowsCount = lastInput.getPositionCount();

        Block.Builder[] blockBuilders = new Block.Builder[types.length];
        for (int i = 0; i < types.length; i++) {
            blockBuilders[i] = types[i].newBlockBuilder(rowsCount);
        }

        Page lastPage = lastInput;
        for (int row = 0; row < rowsCount; row++) {
            Object input = inputEvaluator.computeRow(lastPage, row);
            evaluator.computeRow(BytesRefs.toBytesRef(input), blockBuilders);
        }

        Block[] blocks = new Block[blockBuilders.length];
        for (int i = 0; i < blockBuilders.length; i++) {
            blocks[i] = blockBuilders[i].build();
        }
        lastPage = lastPage.appendBlocks(blocks);

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
        sb.append("evaluator=");
        sb.append(evaluator.toString());
        sb.append("]");
        return sb.toString();
    }

    public interface Evaluator {
        void computeRow(BytesRef input, Block.Builder[] target);
    }

}
