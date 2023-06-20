/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

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
 * Base class for functions that reduce multivalued fields into single valued fields.
 */
public abstract class AbstractMultivalueFunction extends UnaryScalarFunction implements Mappable {
    protected AbstractMultivalueFunction(Source source, Expression field) {
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
        private final EvalOperator.ExpressionEvaluator field;

        protected AbstractEvaluator(EvalOperator.ExpressionEvaluator field) {
            this.field = field;
        }

        protected abstract String name();

        /**
         * Called when evaluating a {@link Block} that contains null values.
         */
        protected abstract Block evalNullable(Block fieldVal);

        /**
         * Called when evaluating a {@link Block} that does not contain null values.
         * It's useful to specialize this from {@link #evalNullable} because it knows
         * that it's producing an "array vector" because it only ever emits single
         * valued fields and no null values. Building an array vector directly is
         * generally faster than building it via a {@link Block.Builder}.
         */
        protected abstract Vector evalNotNullable(Block fieldVal);

        /**
         * Called to evaluate single valued fields when the target block has null
         * values.
         */
        protected Block evalSingleValuedNullable(Block fieldVal) {
            return fieldVal;
        }

        /**
         * Called to evaluate single valued fields when the target block does not
         * have null values.
         */
        protected Vector evalSingleValuedNotNullable(Block fieldVal) {
            return fieldVal.asVector();
        }

        @Override
        public final Block eval(Page page) {
            Block fieldVal = field.eval(page);
            if (fieldVal.mayHaveMultivaluedFields() == false) {
                if (fieldVal.mayHaveNulls()) {
                    return evalSingleValuedNullable(fieldVal);
                }
                return evalSingleValuedNotNullable(fieldVal).asBlock();
            }
            if (fieldVal.mayHaveNulls()) {
                return evalNullable(fieldVal);
            }
            return evalNotNullable(fieldVal).asBlock();
        }

        @Override
        public final String toString() {
            return name() + "[field=" + field + "]";
        }
    }
}
