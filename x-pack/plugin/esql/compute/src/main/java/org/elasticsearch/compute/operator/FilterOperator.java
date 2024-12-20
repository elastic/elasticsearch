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
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.core.Releasables;

import java.util.Arrays;

public class FilterOperator extends AbstractPageMappingOperator {

    private final EvalOperator.ExpressionEvaluator evaluator;

    public record FilterOperatorFactory(ExpressionEvaluator.Factory evaluatorSupplier) implements OperatorFactory {

        @Override
        public Operator get(DriverContext driverContext) {
            return new FilterOperator(evaluatorSupplier.get(driverContext));
        }

        @Override
        public String describe() {
            return "FilterOperator[evaluator=" + evaluatorSupplier + "]";
        }
    }

    public FilterOperator(EvalOperator.ExpressionEvaluator evaluator) {
        this.evaluator = evaluator;
    }

    @Override
    protected Page process(Page page) {
        int rowCount = 0;
        int[] positions = new int[page.getPositionCount()];

        try (BooleanBlock test = (BooleanBlock) evaluator.eval(page)) {
            if (test.areAllValuesNull()) {
                // All results are null which is like false. No values selected.
                page.releaseBlocks();
                return null;
            }
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
                page.releaseBlocks();
                return null;
            }
            if (rowCount == page.getPositionCount()) {
                return page;
            }
            positions = Arrays.copyOf(positions, rowCount);

            Block[] filteredBlocks = new Block[page.getBlockCount()];
            boolean success = false;
            try {
                for (int i = 0; i < page.getBlockCount(); i++) {
                    filteredBlocks[i] = page.getBlock(i).filter(positions);
                }
                success = true;
            } finally {
                page.releaseBlocks();
                if (success == false) {
                    Releasables.closeExpectNoException(filteredBlocks);
                }
            }
            return new Page(filteredBlocks);
        }
    }

    @Override
    public String toString() {
        return "FilterOperator[" + "evaluator=" + evaluator + ']';
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(evaluator, super::close);
    }
}
