/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.ann.Experimental;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;

import java.util.function.Supplier;

@Experimental
public class EvalOperator extends AbstractPageMappingOperator {

    public record EvalOperatorFactory(Supplier<ExpressionEvaluator> evaluator, ElementType elementType) implements OperatorFactory {

        @Override
        public Operator get() {
            return new EvalOperator(evaluator.get(), elementType);
        }

        @Override
        public String describe() {
            return "EvalOperator[elementType=" + elementType + ", evaluator=" + evaluator.get() + "]";
        }
    }

    private final ExpressionEvaluator evaluator;
    private final ElementType elementType;    // TODO we no longer need this parameter

    public EvalOperator(ExpressionEvaluator evaluator, ElementType elementType) {
        this.evaluator = evaluator;
        this.elementType = elementType;
    }

    @Override
    protected Page process(Page page) {
        return page.appendBlock(evaluator.eval(page));
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.getClass().getSimpleName()).append("[");
        sb.append("elementType=").append(elementType).append(", ");
        sb.append("evaluator=").append(evaluator);
        sb.append("]");
        return sb.toString();
    }

    public interface ExpressionEvaluator {
        Block eval(Page page);
    }
}
