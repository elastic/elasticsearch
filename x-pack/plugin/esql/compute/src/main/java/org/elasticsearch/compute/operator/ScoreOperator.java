/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

/**
 * Evaluates scores for a ExpressionScorer. The scores are added to the existing scores in the input page
 */
public class ScoreOperator extends AbstractPageMappingOperator {

    public record ScoreOperatorFactory(ExpressionScorer.Factory scorerFactory, int scoreBlockPosition) implements OperatorFactory {

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
    private final ExpressionScorer scorer;
    private final int scoreBlockPosition;

    public ScoreOperator(BlockFactory blockFactory, ExpressionScorer scorer, int scoreBlockPosition) {
        this.blockFactory = blockFactory;
        this.scorer = scorer;
        this.scoreBlockPosition = scoreBlockPosition;
    }

    @Override
    protected Page process(Page page) {
        assert page.getBlockCount() >= 2 : "Expected at least 2 blocks, got " + page.getBlockCount();
        assert page.getBlock(0).asVector() instanceof DocVector : "Expected a DocVector, got " + page.getBlock(0).asVector();
        assert page.getBlock(1).asVector() instanceof DoubleVector : "Expected a DoubleVector, got " + page.getBlock(1).asVector();

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
        try (DoubleBlock evalScores = scorer.score(page); DoubleBlock existingScores = page.getBlock(scoreBlockPosition)) {
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

    /**
     * Evaluates the score of an expression one {@link Page} at a time.
     */
    public interface ExpressionScorer extends Releasable {
        /** A Factory for creating ExpressionScorers. */
        interface Factory {
            ExpressionScorer get(DriverContext context);
        }

        /**
         * Scores the expression.
         * @return the returned Block has its own reference and the caller is responsible for releasing it.
         */
        DoubleBlock score(Page page);
    }
}
