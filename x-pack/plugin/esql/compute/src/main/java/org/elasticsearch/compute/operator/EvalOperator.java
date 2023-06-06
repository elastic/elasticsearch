/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.ann.Experimental;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;

import java.util.function.Supplier;

/**
 * Evaluates a tree of functions for every position in the block, resulting in a
 * new block which is appended to the page.
 */
@Experimental
public class EvalOperator extends AbstractPageMappingOperator {

    public record EvalOperatorFactory(Supplier<ExpressionEvaluator> evaluator) implements OperatorFactory {

        @Override
        public Operator get(DriverContext driverContext) {
            return new EvalOperator(evaluator.get());
        }

        @Override
        public String describe() {
            return "EvalOperator[evaluator=" + evaluator.get() + "]";
        }
    }

    private final ExpressionEvaluator evaluator;

    public EvalOperator(ExpressionEvaluator evaluator) {
        this.evaluator = evaluator;
    }

    @Override
    protected Page process(Page page) {
        return page.appendBlock(evaluator.eval(page));
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[evaluator=" + evaluator + "]";
    }

    public interface ExpressionEvaluator {
        Block eval(Page page);
    }

    public static final ExpressionEvaluator CONSTANT_NULL = page -> Block.constantNullBlock(page.getPositionCount());
}
