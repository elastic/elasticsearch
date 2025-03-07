/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.scalar.UnaryScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;

/**
 * Base class for functions that reduce multivalued fields into single valued fields.
 * <p>
 *     We have a guide for writing these in the javadoc for
 *     {@link org.elasticsearch.xpack.esql.expression.function.scalar}.
 * </p>
 */
public abstract class AbstractMultivalueFunction extends UnaryScalarFunction {

    protected AbstractMultivalueFunction(Source source, Expression field) {
        super(source, field);
    }

    protected AbstractMultivalueFunction(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(Expression.class));
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(field);
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
    public final ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        return evaluator(toEvaluator.apply(field()));
    }

    /**
     * Base evaluator that can handle both nulls- and no-nulls-containing blocks.
     */
    public abstract static class AbstractEvaluator extends AbstractNullableEvaluator {
        protected AbstractEvaluator(DriverContext driverContext, EvalOperator.ExpressionEvaluator field) {
            super(driverContext, field);
        }

        /**
         * Called when evaluating a {@link Block} that does not contain null values.
         * It’s useful to specialize this from {@link #evalNullable} because it knows
         * that it’s producing an "array vector" because it only ever emits single
         * valued fields and no null values. Building an array vector directly is
         * generally faster than building it via a {@link Block.Builder}.
         *
         * @return the returned Block has its own reference and the caller is responsible for releasing it.
         */
        protected abstract Block evalNotNullable(Block fieldVal);

        /**
         * Called to evaluate single valued fields when the target block does not have null values.
         *
         * @return the returned Block has its own reference and the caller is responsible for releasing it.
         */
        protected Block evalSingleValuedNotNullable(Block fieldRef) {
            fieldRef.incRef();
            return fieldRef;
        }

        @Override
        public final Block eval(Page page) {
            try (Block block = field.eval(page)) {
                if (block.mayHaveMultivaluedFields()) {
                    if (block.mayHaveNulls()) {
                        return evalNullable(block);
                    } else {
                        return evalNotNullable(block);
                    }
                }
                if (block.mayHaveNulls()) {
                    return evalSingleValuedNullable(block);
                } else {
                    return evalSingleValuedNotNullable(block);
                }
            }
        }
    }

    /**
     * Base evaluator that can handle evaluator-checked exceptions; i.e. for expressions that can be evaluated to null.
     */
    public abstract static class AbstractNullableEvaluator implements EvalOperator.ExpressionEvaluator {
        protected final DriverContext driverContext;
        protected final EvalOperator.ExpressionEvaluator field;

        protected AbstractNullableEvaluator(DriverContext driverContext, EvalOperator.ExpressionEvaluator field) {
            this.driverContext = driverContext;
            this.field = field;
        }

        protected abstract String name();

        /**
         * Called when evaluating a {@link Block} that contains null values.
         * @return the returned Block has its own reference and the caller is responsible for releasing it.
         */
        protected abstract Block evalNullable(Block fieldVal);

        /**
         * Called to evaluate single valued fields when the target block has null values.
         * @return the returned Block has its own reference and the caller is responsible for releasing it.
         */
        protected Block evalSingleValuedNullable(Block fieldRef) {
            fieldRef.incRef();
            return fieldRef;
        }

        @Override
        public Block eval(Page page) {
            try (Block block = field.eval(page)) {
                if (block.mayHaveMultivaluedFields()) {
                    return evalNullable(block);
                } else {
                    return evalSingleValuedNullable(block);
                }
            }
        }

        @Override
        public final String toString() {
            return name() + "[field=" + field + "]";
        }

        @Override
        public void close() {
            Releasables.closeExpectNoException(field);
        }
    }
}
