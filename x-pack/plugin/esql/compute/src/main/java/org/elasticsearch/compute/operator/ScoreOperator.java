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
 * of per-row scores) to the existing scores in the input page.
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
        assert page.getBlock(scoreBlockPosition).asVector() instanceof DoubleVector
            : "Expected a DoubleVector as a score block, got " + page.getBlock(scoreBlockPosition).asVector();

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
            DoubleVector.Builder builder = blockFactory.newDoubleVectorFixedBuilder(rowCount);
            for (int i = 0; i < rowCount; i++) {
                builder.appendDouble(existingScores.getDouble(i) + evalScores.getDouble(i));
            }
            return builder.build().asBlock();
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
