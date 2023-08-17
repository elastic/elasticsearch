/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.Page;

import java.util.Arrays;
import java.util.function.Supplier;

public class FilterOperator extends AbstractPageMappingOperator {

    private final EvalOperator.ExpressionEvaluator evaluator;

    public record FilterOperatorFactory(Supplier<EvalOperator.ExpressionEvaluator> evaluatorSupplier) implements OperatorFactory {

        @Override
        public Operator get(DriverContext driverContext) {
            return new FilterOperator(evaluatorSupplier.get());
        }

        @Override
        public String describe() {
            return "FilterOperator[evaluator=" + evaluatorSupplier.get() + "]";
        }
    }

    public FilterOperator(EvalOperator.ExpressionEvaluator evaluator) {
        this.evaluator = evaluator;
    }

    @Override
    protected Page process(Page page) {
        int rowCount = 0;
        int[] positions = new int[page.getPositionCount()];

        Block uncastTest = evaluator.eval(page);
        if (uncastTest.areAllValuesNull()) {
            // All results are null which is like false. No values selected.
            return null;
        }
        BooleanBlock test = (BooleanBlock) uncastTest;
        // TODO we can detect constant true or false from the type
        // TODO or we could make a new method in bool-valued evaluators that returns a list of numbers
        for (int p = 0; p < page.getPositionCount(); p++) {
            if (test.isNull(p) || test.getValueCount(p) != 1) {
                // Null is like false
                // And, for now, multivalued results are like false too
                continue;
            }
            if (test.getBoolean(test.getFirstValueIndex(p))) {
                positions[rowCount++] = p;
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
        return "FilterOperator[" + "evaluator=" + evaluator + ']';
    }
}
