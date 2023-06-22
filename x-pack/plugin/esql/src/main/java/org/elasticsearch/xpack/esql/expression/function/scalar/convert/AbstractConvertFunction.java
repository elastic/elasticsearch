/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.Vector;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.expression.function.Warnings;
import org.elasticsearch.xpack.esql.expression.function.scalar.UnaryScalarFunction;
import org.elasticsearch.xpack.esql.planner.Mappable;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;

import java.util.Locale;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isType;

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
    protected Supplier<EvalOperator.ExpressionEvaluator> evaluator(Supplier<EvalOperator.ExpressionEvaluator> fieldEval) {
        DataType sourceType = field().dataType();
        var evaluator = evaluators().get(sourceType);
        if (evaluator == null) {
            throw new AssertionError("unsupported type [" + sourceType + "]");
        }
        return () -> evaluator.apply(fieldEval.get(), source());
    }

    @Override
    protected final TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }
        return isType(
            field(),
            evaluators()::containsKey,
            sourceText(),
            null,
            evaluators().keySet().stream().map(dt -> dt.name().toLowerCase(Locale.ROOT)).sorted().toArray(String[]::new)
        );
    }

    protected abstract Map<DataType, BiFunction<EvalOperator.ExpressionEvaluator, Source, EvalOperator.ExpressionEvaluator>> evaluators();

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

        private static final Log logger = LogFactory.getLog(AbstractEvaluator.class);

        private final EvalOperator.ExpressionEvaluator fieldEvaluator;
        private final Warnings warnings;

        protected AbstractEvaluator(EvalOperator.ExpressionEvaluator field, Source source) {
            this.fieldEvaluator = field;
            this.warnings = new Warnings(source);
        }

        protected abstract String name();

        /**
         * Called when evaluating a {@link Block} that contains null values.
         */
        protected abstract Block evalBlock(Block b);

        /**
         * Called when evaluating a {@link Block} that does not contain null values.
         */
        protected abstract Block evalVector(Vector v);

        public Block eval(Page page) {
            Block block = fieldEvaluator.eval(page);
            if (block.areAllValuesNull()) {
                return Block.constantNullBlock(page.getPositionCount());
            }
            Vector vector = block.asVector();
            return vector == null ? evalBlock(block) : evalVector(vector);
        }

        protected final void registerException(Exception exception) {
            logger.trace("conversion failure", exception);
            warnings.registerException(exception);
        }

        @Override
        public final String toString() {
            return name() + "[field=" + fieldEvaluator + "]";
        }
    }
}
