/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

/**
 * Evaluates a tree of functions for every position in the block, resulting in a
 * new block which is appended to the page.
 */
public class EvalOperator extends AbstractPageMappingOperator {

    public record EvalOperatorFactory(ExpressionEvaluator.Factory evaluator) implements OperatorFactory {

        @Override
        public Operator get(DriverContext driverContext) {
            return new EvalOperator(driverContext.blockFactory(), evaluator.get(driverContext));
        }

        @Override
        public String describe() {
            return "EvalOperator[evaluator=" + evaluator + "]";
        }
    }

    private final BlockFactory blockFactory;
    private final ExpressionEvaluator evaluator;

    public EvalOperator(BlockFactory blockFactory, ExpressionEvaluator evaluator) {
        this.blockFactory = blockFactory;
        this.evaluator = evaluator;
    }

    @Override
    protected Page process(Page page) {
        Block block = evaluator.eval(page);
        return page.appendBlock(block);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[evaluator=" + evaluator + "]";
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(evaluator, super::close);
    }

    /**
     * Evaluates an expression {@code a + b} or {@code log(c)} one {@link Page} at a time.
     */
    public interface ExpressionEvaluator extends Releasable {
        /** A Factory for creating ExpressionEvaluators. */
        interface Factory {
            ExpressionEvaluator get(DriverContext context);
        }

        /**
         * Evaluate the expression.
         * @return the returned Block has its own reference and the caller is responsible for releasing it.
         */
        Block eval(Page page);
    }

    public static final ExpressionEvaluator.Factory CONSTANT_NULL_FACTORY = new ExpressionEvaluator.Factory() {
        @Override
        public ExpressionEvaluator get(DriverContext driverContext) {
            return CONSTANT_NULL;
        }

        @Override
        public String toString() {
            return CONSTANT_NULL.toString();
        }
    };

    public static final ExpressionEvaluator CONSTANT_NULL = new ExpressionEvaluator() {
        @Override
        public Block eval(Page page) {
            return Block.constantNullBlock(page.getPositionCount());
        }

        @Override
        public String toString() {
            return "ConstantNull";
        }

        @Override
        public void close() {}
    };
}
