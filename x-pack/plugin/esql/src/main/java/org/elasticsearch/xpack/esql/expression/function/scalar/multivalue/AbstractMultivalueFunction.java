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
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.function.scalar.UnaryScalarFunction;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.Source;

/**
 * Base class for functions that reduce multivalued fields into single valued fields.
 */
public abstract class AbstractMultivalueFunction extends UnaryScalarFunction implements EvaluatorMapper {
    protected AbstractMultivalueFunction(Source source, Expression field) {
        super(source, field);
    }

    /**
     * Build the evaluator given the evaluator a multivalued field.
     */
    protected abstract ExpressionEvaluator.Factory evaluator(ExpressionEvaluator.Factory fieldEval);

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
        return EvaluatorMapper.super.fold();
    }

    @Override
    public final ExpressionEvaluator.Factory toEvaluator(java.util.function.Function<Expression, ExpressionEvaluator.Factory> toEvaluator) {
        return evaluator(toEvaluator.apply(field()));
    }

    /**
     * Base evaluator that can handle both nulls- and no-nulls-containing blocks.
     */
    public abstract static class AbstractEvaluator extends AbstractNullableEvaluator {
        protected AbstractEvaluator(EvalOperator.ExpressionEvaluator field) {
            super(field);
        }

        /**
         * Called when evaluating a {@link Block} that does not contain null values.
         * It's useful to specialize this from {@link #evalNullable} because it knows
         * that it's producing an "array vector" because it only ever emits single
         * valued fields and no null values. Building an array vector directly is
         * generally faster than building it via a {@link Block.Builder}.
         */
        protected abstract Vector evalNotNullable(Block fieldVal);

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
    }

    /**
     * Base evaluator that can handle evaluator-checked exceptions; i.e. for expressions that can be evaluated to null.
     */
    public abstract static class AbstractNullableEvaluator implements EvalOperator.ExpressionEvaluator {
        protected final EvalOperator.ExpressionEvaluator field;

        protected AbstractNullableEvaluator(EvalOperator.ExpressionEvaluator field) {
            this.field = field;
        }

        protected abstract String name();

        /**
         * Called when evaluating a {@link Block} that contains null values.
         */
        protected abstract Block evalNullable(Block fieldVal);

        /**
         * Called to evaluate single valued fields when the target block has null
         * values.
         */
        protected Block evalSingleValuedNullable(Block fieldVal) {
            return fieldVal;
        }

        @Override
        public Block eval(Page page) {
            Block fieldVal = field.eval(page);
            return fieldVal.mayHaveMultivaluedFields() ? evalNullable(fieldVal) : evalSingleValuedNullable(fieldVal);
        }

        @Override
        public final String toString() {
            return name() + "[field=" + field + "]";
        }
    }
}
