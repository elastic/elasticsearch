/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import org.apache.lucene.geo.Component2D;
import org.elasticsearch.common.TriFunction;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;

import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.xpack.esql.expression.function.scalar.spatial.SpatialRelatesUtils.asLuceneComponent2D;

/**
 * SpatialRelatesFunction classes, like SpatialIntersects, support various combinations of incoming types, which can be sourced from
 * constant literals (foldable), or from the index, which could provide either source values or doc-values. This class is used to
 * create the appropriate evaluator for the given combination of types.
 * @param <V>
 * @param <T>
 */
abstract class SpatialEvaluatorFactory<V, T> {
    protected final TriFunction<Source, V, T, EvalOperator.ExpressionEvaluator.Factory> factoryCreator;

    SpatialEvaluatorFactory(TriFunction<Source, V, T, EvalOperator.ExpressionEvaluator.Factory> factoryCreator) {
        this.factoryCreator = factoryCreator;
    }

    public abstract EvalOperator.ExpressionEvaluator.Factory get(
        SpatialSourceSupplier function,
        Function<Expression, EvalOperator.ExpressionEvaluator.Factory> toEvaluator
    );

    public static EvalOperator.ExpressionEvaluator.Factory makeSpatialEvaluator(
        SpatialSourceSupplier s,
        Map<SpatialEvaluatorKey, SpatialEvaluatorFactory<?, ?>> evaluatorRules,
        Function<Expression, EvalOperator.ExpressionEvaluator.Factory> toEvaluator
    ) {
        var evaluatorKey = new SpatialEvaluatorKey(s.crsType(), s.useDocValues(), fieldKey(s.left()), fieldKey(s.right()));
        SpatialEvaluatorFactory<?, ?> factory = evaluatorRules.get(evaluatorKey);
        if (factory == null) {
            evaluatorKey = evaluatorKey.swapSides();
            factory = evaluatorRules.get(evaluatorKey);
            if (factory == null) {
                throw evaluatorKey.unsupported();
            }
            return factory.get(new SwappedSpatialSourceSupplier(s), toEvaluator);
        }
        return factory.get(s, toEvaluator);
    }

    protected static SpatialEvaluatorFieldKey fieldKey(Expression expression) {
        return new SpatialEvaluatorFieldKey(expression.dataType(), expression.foldable());
    }

    /**
     * This interface defines a supplier of the key information needed by the spatial evaluator factories.
     * The SpatialRelatesFunction will use this to supply the necessary information to the factories.
     * When we need to swap left and right sides around, we can use a SwappableSpatialSourceSupplier.
     */
    interface SpatialSourceSupplier {
        Source source();

        Expression left();

        Expression right();

        SpatialRelatesFunction.SpatialCrsType crsType();

        boolean useDocValues();
    }

    protected static class SwappedSpatialSourceSupplier implements SpatialSourceSupplier {
        private final SpatialSourceSupplier delegate;

        public SwappedSpatialSourceSupplier(SpatialSourceSupplier delegate) {
            this.delegate = delegate;
        }

        @Override
        public Source source() {
            return delegate.source();
        }

        @Override
        public SpatialRelatesFunction.SpatialCrsType crsType() {
            return delegate.crsType();
        }

        @Override
        public boolean useDocValues() {
            return delegate.useDocValues();
        }

        @Override
        public Expression left() {
            return delegate.right();
        }

        @Override
        public Expression right() {
            return delegate.left();
        }
    }

    protected static class SpatialEvaluatorFactoryWithFields extends SpatialEvaluatorFactory<
        EvalOperator.ExpressionEvaluator.Factory,
        EvalOperator.ExpressionEvaluator.Factory> {
        SpatialEvaluatorFactoryWithFields(
            TriFunction<
                Source,
                EvalOperator.ExpressionEvaluator.Factory,
                EvalOperator.ExpressionEvaluator.Factory,
                EvalOperator.ExpressionEvaluator.Factory> factoryCreator
        ) {
            super(factoryCreator);
        }

        @Override
        public EvalOperator.ExpressionEvaluator.Factory get(
            SpatialSourceSupplier s,
            Function<Expression, EvalOperator.ExpressionEvaluator.Factory> toEvaluator
        ) {
            return factoryCreator.apply(s.source(), toEvaluator.apply(s.left()), toEvaluator.apply(s.right()));
        }
    }

    protected static class SpatialEvaluatorWithConstantFactory extends SpatialEvaluatorFactory<
        EvalOperator.ExpressionEvaluator.Factory,
        Component2D> {

        SpatialEvaluatorWithConstantFactory(
            TriFunction<
                Source,
                EvalOperator.ExpressionEvaluator.Factory,
                Component2D,
                EvalOperator.ExpressionEvaluator.Factory> factoryCreator
        ) {
            super(factoryCreator);
        }

        @Override
        public EvalOperator.ExpressionEvaluator.Factory get(
            SpatialSourceSupplier s,
            Function<Expression, EvalOperator.ExpressionEvaluator.Factory> toEvaluator
        ) {
            return factoryCreator.apply(s.source(), toEvaluator.apply(s.left()), asLuceneComponent2D(s.crsType(), s.right()));
        }
    }

    protected record SpatialEvaluatorFieldKey(DataType dataType, boolean isConstant) {}

    protected record SpatialEvaluatorKey(
        SpatialRelatesFunction.SpatialCrsType crsType,
        boolean useDocValues,
        SpatialEvaluatorFieldKey left,
        SpatialEvaluatorFieldKey right
    ) {
        SpatialEvaluatorKey withDocValues() {
            return new SpatialEvaluatorKey(crsType, true, left, right);
        }

        SpatialEvaluatorKey swapSides() {
            return new SpatialEvaluatorKey(crsType, useDocValues, right, left);
        }

        SpatialEvaluatorKey withConstants(boolean leftConstant, boolean rightConstant) {
            return new SpatialEvaluatorKey(
                crsType,
                useDocValues,
                new SpatialEvaluatorFieldKey(left.dataType, leftConstant),
                new SpatialEvaluatorFieldKey(right.dataType, rightConstant)
            );
        }

        static SpatialEvaluatorKey fromSourceAndConstant(DataType left, DataType right) {
            return new SpatialEvaluatorKey(
                SpatialRelatesFunction.SpatialCrsType.fromDataType(left),
                false,
                new SpatialEvaluatorFieldKey(left, false),
                new SpatialEvaluatorFieldKey(right, true)
            );
        }

        static SpatialEvaluatorKey fromSources(DataType left, DataType right) {
            return new SpatialEvaluatorKey(
                SpatialRelatesFunction.SpatialCrsType.fromDataType(left),
                false,
                new SpatialEvaluatorFieldKey(left, false),
                new SpatialEvaluatorFieldKey(right, false)
            );
        }

        static SpatialEvaluatorKey fromConstants(DataType left, DataType right) {
            return new SpatialEvaluatorKey(
                SpatialRelatesFunction.SpatialCrsType.fromDataType(left),
                false,
                new SpatialEvaluatorFieldKey(left, true),
                new SpatialEvaluatorFieldKey(right, true)
            );
        }

        UnsupportedOperationException unsupported() {
            return new UnsupportedOperationException("Unsupported spatial relation combination: " + this);
        }
    }
}
