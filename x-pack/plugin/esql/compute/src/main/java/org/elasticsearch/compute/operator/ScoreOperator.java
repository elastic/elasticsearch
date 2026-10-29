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
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.core.Releasables;

/**
 * Adds the scores produced by a score {@link ExpressionEvaluator} (an evaluator yielding a {@link DoubleBlock}
 * of per-row scores) to the existing scores in the input page. A row whose existing score is {@code null} has
 * nothing to add onto and stays {@code null}.
 */
public class ScoreOperator extends AbstractPageMappingOperator {

    public record ScoreOperatorFactory(ExpressionEvaluator.Factory scorerFactory, int scoreBlockPosition) implements OperatorFactory {

        @Override
        public Operator get(DriverContext driverContext) {
            return new ScoreOperator(driverContext.blockFactory(), scorerFactory.get(driverContext), scoreBlockPosition);
        }

        @Override
        public String describe() {
            return "ScoreOperator[scorer=" + scorerFactory + "]";
        }
    }

    private final BlockFactory blockFactory;
    private final ExpressionEvaluator scorer;
    private final int scoreBlockPosition;

    public ScoreOperator(BlockFactory blockFactory, ExpressionEvaluator scorer, int scoreBlockPosition) {
        this.blockFactory = blockFactory;
        this.scorer = scorer;
        this.scoreBlockPosition = scoreBlockPosition;
    }

    @Override
    protected Page process(Page page) {
        assert page.getBlockCount() > scoreBlockPosition : "Expected to get a score block in position " + scoreBlockPosition;

        Block[] blocks = new Block[page.getBlockCount()];
        for (int i = 0; i < page.getBlockCount(); i++) {
            if (i == scoreBlockPosition) {
                blocks[i] = calculateScoresBlock(page);
            } else {
                blocks[i] = page.getBlock(i);
            }
        }

        return new Page(blocks);
    }

    private Block calculateScoresBlock(Page page) {
        try (DoubleBlock evalScores = (DoubleBlock) scorer.eval(page); DoubleBlock existingScores = page.getBlock(scoreBlockPosition)) {
            // TODO Optimize for constant scores?
            int rowCount = page.getPositionCount();
            DoubleVector existingVector = existingScores.asVector();
            if (existingVector != null) {
                DoubleVector.Builder builder = blockFactory.newDoubleVectorFixedBuilder(rowCount);
                for (int i = 0; i < rowCount; i++) {
                    builder.appendDouble(existingVector.getDouble(i) + evalScores.getDouble(i));
                }
                return builder.build().asBlock();
            }
            // A row can arrive without a score baseline: a subquery union branch that did not ask for METADATA _score
            // null-fills the column, as does a federated relation answered by a node that predates runtime scoring for
            // it. Such a row has no score to add onto, so it stays null rather than starting from zero.
            try (DoubleBlock.Builder builder = blockFactory.newDoubleBlockBuilder(rowCount)) {
                for (int i = 0; i < rowCount; i++) {
                    if (existingScores.isNull(i)) {
                        builder.appendNull();
                    } else {
                        builder.appendDouble(existingScores.getDouble(existingScores.getFirstValueIndex(i)) + evalScores.getDouble(i));
                    }
                }
                return builder.build();
            }
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[scorer=" + scorer + "]";
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(scorer, super::close);
    }
}
