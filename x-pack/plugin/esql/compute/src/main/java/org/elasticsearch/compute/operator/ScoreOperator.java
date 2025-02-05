/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

/**
 * Evaluates a tree of functions for every position in the block, resulting in a
 * new block which is appended to the page.
 */
public class ScoreOperator extends AbstractPageMappingOperator {

    public record ScoreOperatorFactory(ExpressionScorer.Factory scorerFactory) implements OperatorFactory {

        @Override
        public Operator get(DriverContext driverContext) {
            return new ScoreOperator(driverContext.blockFactory(), scorerFactory.get(driverContext));
        }

        @Override
        public String describe() {
            return "ScoreOperator[scorer=" + scorerFactory + "]";
        }
    }

    private final BlockFactory blockFactory;
    private final ExpressionScorer scorer;

    public ScoreOperator(BlockFactory blockFactory, ExpressionScorer scorer) {
        this.blockFactory = blockFactory;
        this.scorer = scorer;
    }

    @Override
    protected Page process(Page page) {
        DoubleBlock block = scorer.score(page);
        return page.appendBlock(block);
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
