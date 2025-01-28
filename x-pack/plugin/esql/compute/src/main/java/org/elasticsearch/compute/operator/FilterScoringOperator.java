/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.core.Releasables;

import java.util.Arrays;

import static org.elasticsearch.compute.lucene.LuceneQueryExpressionEvaluator.SCORE_FOR_FALSE;

public class FilterScoringOperator extends AbstractPageMappingOperator {

    public static final int SCORE_BLOCK_INDEX = 1;
    private final ExpressionEvaluator evaluator;
    private final BlockFactory blockFactory;

    public record FilterScoringOperatorFactory(ExpressionEvaluator.Factory evaluatorSupplier) implements OperatorFactory {

        @Override
        public Operator get(DriverContext driverContext) {
            return new FilterScoringOperator(evaluatorSupplier.get(driverContext), driverContext.blockFactory());
        }

        @Override
        public String describe() {
            return "FilterScoringOperator[evaluator=" + evaluatorSupplier + "]";
        }
    }

    public FilterScoringOperator(ExpressionEvaluator evaluator, BlockFactory blockFactory) {
        this.evaluator = evaluator;
        this.blockFactory = blockFactory;
    }

    @Override
    protected Page process(Page page) {
        assert page.getBlockCount() > SCORE_BLOCK_INDEX : "Scoring block not found on page";
        assert page.getBlock(SCORE_BLOCK_INDEX) instanceof DoubleBlock : "Scoring block should be at index 1";

        int rowCount = 0;
        int[] positions = new int[page.getPositionCount()];

        try (DoubleBlock scoreBlock = (DoubleBlock) evaluator.eval(page)) {
            if (scoreBlock.areAllValuesNull()) {
                // All results are null which is like false. No values selected.
                page.releaseBlocks();
                return null;
            }
            for (int p = 0; p < page.getPositionCount(); p++) {
                if (scoreBlock.isNull(p) || scoreBlock.getValueCount(p) != 1) {
                    // Null is like false
                    // And, for now, multivalued results are like false too
                    continue;
                }
                if (scoreBlock.getDouble(scoreBlock.getFirstValueIndex(p)) != SCORE_FOR_FALSE) {
                    positions[rowCount++] = p;
                }
            }

            if (rowCount == 0) {
                page.releaseBlocks();
                return null;
            }
            positions = Arrays.copyOf(positions, rowCount);

            Block[] filteredBlocks = new Block[page.getBlockCount()];

            boolean success = false;
            try {
                for (int i = 0; i < page.getBlockCount(); i++) {
                    if (i == SCORE_BLOCK_INDEX) {
                        // Create a new scores block with the retrieved scores, that will replace the existing one on the result page
                        DoubleVector.Builder updatedScoresBuilder = blockFactory.newDoubleVectorBuilder(rowCount);
                        for (int j = 0; j < rowCount; j++) {
                            updatedScoresBuilder.appendDouble(scoreBlock.getDouble(positions[j]));
                        }
                        filteredBlocks[i] = updatedScoresBuilder.build().asBlock();
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

    @Override
    public String toString() {
        return "FilterOperator[" + "evaluator=" + evaluator + ']';
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(evaluator, super::close);
    }
}
