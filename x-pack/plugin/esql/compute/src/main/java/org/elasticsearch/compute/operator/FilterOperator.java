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
import java.util.function.Supplier;

public class FilterOperator extends AbstractPageMappingOperator {

    private final EvalOperator.ExpressionEvaluator evaluator;

    public record FilterOperatorFactory(Supplier<EvalOperator.ExpressionEvaluator> evaluatorSupplier) implements OperatorFactory {

        @Override
        public Operator get() {
            return new FilterOperator(evaluatorSupplier.get());
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
    protected Page process(Page page) {
        int[] positions = new int[page.getPositionCount()];
        int rowCount = 0;

        for (int i = 0; i < page.getPositionCount(); i++) {
            Object result = evaluator.computeRow(page, i);
            // possible 3vl evaluation results: true, false, null
            // provided condition must evaluate to `true`, otherwise the position is filtered out
            if (result instanceof Boolean bool && bool) {
                positions[rowCount++] = i;
            }
        }

        if (rowCount == 0) {
            return null;
        }
        if (rowCount == page.getPositionCount()) {
            return page;
        }
        positions = Arrays.copyOf(positions, rowCount);

        Block[] filteredBlocks = new Block[page.getBlockCount()];
        for (int i = 0; i < page.getBlockCount(); i++) {
            filteredBlocks[i] = page.getBlock(i).filter(positions);
        }

        return new Page(filteredBlocks);
    }

    @Override
    public String toString() {
        return "FilterOperator{" + "evaluator=" + evaluator + '}';
    }
}
