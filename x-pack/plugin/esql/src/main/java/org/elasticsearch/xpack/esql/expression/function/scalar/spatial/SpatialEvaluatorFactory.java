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
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;

import java.util.Map;

import static org.elasticsearch.xpack.esql.expression.function.scalar.spatial.SpatialRelatesUtils.asLuceneComponent2D;
import static org.elasticsearch.xpack.esql.expression.function.scalar.spatial.SpatialRelatesUtils.asLuceneComponent2Ds;

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

    public abstract EvalOperator.ExpressionEvaluator.Factory get(SpatialSourceSupplier function, EvaluatorMapper.ToEvaluator toEvaluator);

    static EvalOperator.ExpressionEvaluator.Factory makeSpatialEvaluator(
        SpatialSourceSupplier s,
        Map<SpatialEvaluatorKey, SpatialEvaluatorFactory<?, ?>> evaluatorRules,
        EvaluatorMapper.ToEvaluator toEvaluator
    ) {
        var evaluatorKey = new SpatialEvaluatorKey(
            s.crsType(),
            s.leftDocValues(),
            s.rightDocValues(),
            fieldKey(s.left()),
            fieldKey(s.right())
        );
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

        BinarySpatialFunction.SpatialCrsType crsType();

        boolean leftDocValues();

        boolean rightDocValues();
    }

    /**
     * When performing type resolution we need also write access to the SpatialSourceSupplier for setting the CRS
     */
    interface SpatialSourceResolution extends SpatialSourceSupplier {
        void setCrsType(DataType dataType);
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
        public BinarySpatialFunction.SpatialCrsType crsType() {
            return delegate.crsType();
        }

        @Override
        public boolean leftDocValues() {
            return delegate.leftDocValues();
        }

        @Override
        public boolean rightDocValues() {
            return delegate.rightDocValues();
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

    /**
     * This evaluator factory is used when both sides are not constants or literal, and need to be evaluated.
     * They could be sourced from the index, or from previous evaluators.
     */
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
        public EvalOperator.ExpressionEvaluator.Factory get(SpatialSourceSupplier s, EvaluatorMapper.ToEvaluator toEvaluator) {
            return factoryCreator.apply(s.source(), toEvaluator.apply(s.left()), toEvaluator.apply(s.right()));
        }
    }

    /**
     * This evaluator factory is used when the right hand side is a constant or literal,
     * and the left is sourced from the index, or from previous evaluators.
     */
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
        public EvalOperator.ExpressionEvaluator.Factory get(SpatialSourceSupplier s, EvaluatorMapper.ToEvaluator toEvaluator) {
            return factoryCreator.apply(
                s.source(),
                toEvaluator.apply(s.left()),
                asLuceneComponent2D(toEvaluator.foldCtx(), s.crsType(), s.right())
            );
        }
    }

    /**
     * This evaluator factory is used when the right hand side is a constant or literal,
     * and the left is sourced from the index, or from previous evaluators.
     * It uses an array of Component2Ds to model the constant side for use within CONTAINS which is does not directly support multi-shapes,
     * so we need to split the shapes into multiple components and perform operations on each.
     */
    protected static class SpatialEvaluatorWithConstantArrayFactory extends SpatialEvaluatorFactory<
        EvalOperator.ExpressionEvaluator.Factory,
        Component2D[]> {

        SpatialEvaluatorWithConstantArrayFactory(
            TriFunction<
                Source,
                EvalOperator.ExpressionEvaluator.Factory,
                Component2D[],
                EvalOperator.ExpressionEvaluator.Factory> factoryCreator
        ) {
            super(factoryCreator);
        }

        @Override
        public EvalOperator.ExpressionEvaluator.Factory get(SpatialSourceSupplier s, EvaluatorMapper.ToEvaluator toEvaluator) {
            return factoryCreator.apply(
                s.source(),
                toEvaluator.apply(s.left()),
                asLuceneComponent2Ds(toEvaluator.foldCtx(), s.crsType(), s.right())
            );
        }
    }

    protected record SpatialEvaluatorFieldKey(DataType dataType, boolean isConstant) {}

    record SpatialEvaluatorKey(
        BinarySpatialFunction.SpatialCrsType crsType,
        boolean leftDocValues,
        boolean rightDocValues,
        SpatialEvaluatorFieldKey left,
        SpatialEvaluatorFieldKey right
    ) {
        SpatialEvaluatorKey(BinarySpatialFunction.SpatialCrsType crsType, SpatialEvaluatorFieldKey left, SpatialEvaluatorFieldKey right) {
            this(crsType, false, false, left, right);
        }

        SpatialEvaluatorKey withLeftDocValues() {
            return new SpatialEvaluatorKey(crsType, true, false, left, right);
        }

        SpatialEvaluatorKey swapSides() {
            return new SpatialEvaluatorKey(crsType, rightDocValues, leftDocValues, right, left);
        }

        static SpatialEvaluatorKey fromSourceAndConstant(DataType left, DataType right) {
            return new SpatialEvaluatorKey(
                BinarySpatialFunction.SpatialCrsType.fromDataType(left),
                new SpatialEvaluatorFieldKey(left, false),
                new SpatialEvaluatorFieldKey(right, true)
            );
        }

        static SpatialEvaluatorKey fromSources(DataType left, DataType right) {
            return new SpatialEvaluatorKey(
                BinarySpatialFunction.SpatialCrsType.fromDataType(left),
                new SpatialEvaluatorFieldKey(left, false),
                new SpatialEvaluatorFieldKey(right, false)
            );
        }

        UnsupportedOperationException unsupported() {
            return new UnsupportedOperationException("Unsupported spatial relation combination: " + this);
        }
    }
}
