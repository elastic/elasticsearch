/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.Vector;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.expression.function.scalar.UnaryScalarFunction;
import org.elasticsearch.xpack.esql.planner.Mappable;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.function.Supplier;

/**
 * Base class for functions that converts a field into a function-specific type.
 */
public abstract class AbstractConvertFunction extends UnaryScalarFunction implements Mappable {
    protected AbstractConvertFunction(Source source, Expression field) {
        super(source, field);
    }

    /**
     * Build the evaluator given the evaluator a multivalued field.
     */
    protected abstract Supplier<EvalOperator.ExpressionEvaluator> evaluator(Supplier<EvalOperator.ExpressionEvaluator> fieldEval);

    @Override
    protected final TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }
        return resolveFieldType();
    }

    protected abstract TypeResolution resolveFieldType();

    @Override
    public final Object fold() {
        return Mappable.super.fold();
    }

    @Override
    public final Supplier<EvalOperator.ExpressionEvaluator> toEvaluator(
        java.util.function.Function<Expression, Supplier<EvalOperator.ExpressionEvaluator>> toEvaluator
    ) {
        return evaluator(toEvaluator.apply(field()));
    }

    public abstract static class AbstractEvaluator implements EvalOperator.ExpressionEvaluator {
        private final EvalOperator.ExpressionEvaluator fieldEvaluator;

        protected AbstractEvaluator(EvalOperator.ExpressionEvaluator field) {
            this.fieldEvaluator = field;
        }

        protected abstract String name();

        /**
         * Called when evaluating a {@link Block} that contains null values.
         */
        protected abstract Block evalBlock(Block b);

        /**
         * Called when evaluating a {@link Block} that does not contain null values.
         */
        protected abstract Vector evalVector(Vector v);

        public Block eval(Page page) {
            Block block = fieldEvaluator.eval(page);
            if (block.areAllValuesNull()) {
                return Block.constantNullBlock(page.getPositionCount());
            }
            Vector vector = block.asVector();
            return vector == null ? evalBlock(block) : evalVector(vector).asBlock();
        }

        @Override
        public final String toString() {
            return name() + "[field=" + fieldEvaluator + "]";
        }
    }
}
