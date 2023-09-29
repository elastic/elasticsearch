/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.util.stream.IntStream;

/**
 * Evaluates a tree of functions for every position in the block, resulting in a
 * new block which is appended to the page.
 */
public class EvalOperator extends AbstractPageMappingOperator {

    public record EvalOperatorFactory(ExpressionEvaluator.Factory evaluator) implements OperatorFactory {

        @Override
        public Operator get(DriverContext driverContext) {
            return new EvalOperator(evaluator.get(driverContext));
        }

        @Override
        public String describe() {
            // TODO ThrowingDriverContext blows up when combined with Concat
            return "EvalOperator[evaluator=" + evaluator.get(new ThrowingDriverContext()) + "]";
        }
    }

    private final ExpressionEvaluator evaluator;

    public EvalOperator(ExpressionEvaluator evaluator) {
        this.evaluator = evaluator;
    }

    @Override
    protected Page process(Page page) {
        Block.Ref ref = evaluator.eval(page);
        Block block = ref.floating() ? ref.block() : BlockUtils.deepCopyOf(ref.block());
        return page.appendBlock(block);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[evaluator=" + evaluator + "]";
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(evaluator);
    }

    /** Returns a copy of the give block, if the block appears in the page. */
    // TODO: this is a catch all, can be removed when we validate that evaluators always return copies
    // for now it just looks like Attributes returns a reference?
    static Block maybeCopyBlock(Page page, Block block) {
        if (IntStream.range(0, page.getBlockCount()).mapToObj(page::getBlock).anyMatch(b -> b == block)) {
            return BlockUtils.deepCopyOf(block);
        }
        return block;
    }

    /**
     * Evaluates an expression {@code a + b} or {@code log(c)} one {@link Page} at a time.
     */
    public interface ExpressionEvaluator extends Releasable {
        /** A Factory for creating ExpressionEvaluators. */
        interface Factory {
            ExpressionEvaluator get(DriverContext driverContext);
        }

        /**
         * Evaluate the expression.
         */
        Block.Ref eval(Page page);
    }

    public static final ExpressionEvaluator CONSTANT_NULL = new ExpressionEvaluator() {
        @Override
        public Block.Ref eval(Page page) {
            return Block.Ref.floating(Block.constantNullBlock(page.getPositionCount()));
        }

        @Override
        public String toString() {
            return "ConstantNull";
        }

        @Override
        public void close() {}
    };
}
