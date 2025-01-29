/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.core.Releasables;

import java.util.Arrays;

import static org.elasticsearch.compute.lucene.LuceneQueryExpressionEvaluator.SCORE_FOR_FALSE;

public class FilterOperator extends AbstractPageMappingOperator {

    public static final int SCORE_BLOCK_INDEX = 1;

    private final EvalOperator.ExpressionEvaluator evaluator;
    private final boolean usesScoring;
    private final BlockFactory blockFactory;

    public record FilterOperatorFactory(ExpressionEvaluator.Factory evaluatorSupplier, boolean usesScoring) implements OperatorFactory {

        @Override
        public Operator get(DriverContext driverContext) {
            return new FilterOperator(evaluatorSupplier.get(driverContext), usesScoring, driverContext.blockFactory());
        }

        @Override
        public String describe() {
            return "FilterOperator[evaluator=" + evaluatorSupplier + "]";
        }
    }

    public FilterOperator(ExpressionEvaluator evaluator, boolean usesScoring, BlockFactory blockFactory) {
        this.evaluator = evaluator;
        this.usesScoring = usesScoring;
        this.blockFactory = blockFactory;
    }

    @Override
    protected Page process(Page page) {
        int rowCount = 0;
        int[] positions = new int[page.getPositionCount()];

        try (Block filterResultBlock = evaluator.eval(page)) {
            if (filterResultBlock.areAllValuesNull()) {
                // All results are null which is like false. No values selected.
                page.releaseBlocks();
                return null;
            }

            // Explicit types to avoid casting on every element
            DoubleBlock scoreBlock = null;
            BooleanBlock testBlock = null;
            if (usesScoring) {
                assert filterResultBlock instanceof DoubleBlock : "Evaluated block should be a DoubleBlock when using scoring";
                scoreBlock = (DoubleBlock) filterResultBlock;
            } else {
                assert filterResultBlock instanceof BooleanBlock : "Evaluated block should be a BooleanBlock when not using scoring";
                testBlock = (BooleanBlock) filterResultBlock;
            }

            // TODO we can detect constant true or false from the type
            // TODO or we could make a new method in bool-valued evaluators that returns a list of numbers
            for (int p = 0; p < page.getPositionCount(); p++) {
                if (filterResultBlock.isNull(p) || filterResultBlock.getValueCount(p) != 1) {
                    // Null is like false
                    // And, for now, multivalued results are like false too
                    continue;
                }
                if (usesScoring && scoreBlock.getDouble(scoreBlock.getFirstValueIndex(p)) != SCORE_FOR_FALSE) {
                    positions[rowCount++] = p;
                } else if (usesScoring == false && testBlock.getBoolean(testBlock.getFirstValueIndex(p))) {
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
                    if (usesScoring && i == SCORE_BLOCK_INDEX) {
                        filteredBlocks[i] = createScoresBlock(rowCount, (DoubleBlock) filterResultBlock, positions);
                    } else {
                        filteredBlocks[i] = page.getBlock(i).filter(positions);
                    }
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

    private Block createScoresBlock(int rowCount, DoubleBlock scoreBlock, int[] positions) {
        // Create a new scores block with the retrieved scores, that will replace the existing one on the result page
        DoubleVector.Builder updatedScoresBuilder = blockFactory.newDoubleVectorBuilder(rowCount);
        for (int j = 0; j < rowCount; j++) {
            updatedScoresBuilder.appendDouble(scoreBlock.getDouble(positions[j]));
        }
        return updatedScoresBuilder.build().asBlock();
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
