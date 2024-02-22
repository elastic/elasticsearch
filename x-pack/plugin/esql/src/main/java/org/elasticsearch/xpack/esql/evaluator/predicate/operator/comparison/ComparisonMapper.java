/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison;

import org.elasticsearch.common.TriFunction;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.evaluator.mapper.ExpressionMapper;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Cast;
import org.elasticsearch.xpack.esql.planner.Layout;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeRegistry;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.expression.predicate.BinaryOperator;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.BinaryComparison;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import static org.elasticsearch.xpack.esql.evaluator.EvalMapper.toEvaluator;

public abstract class ComparisonMapper<T extends BinaryComparison> extends ExpressionMapper<T> {
    public static final ExpressionMapper<?> EQUALS = new ComparisonMapper<Equals>(
        org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.EqualsIntsEvaluator.Factory::new,
        org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.EqualsLongsEvaluator.Factory::new,
        org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.EqualsDoublesEvaluator.Factory::new,
        org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.EqualsKeywordsEvaluator.Factory::new,
        org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.EqualsBoolsEvaluator.Factory::new,
        (s, l, r, t) -> new EqualsGeometriesEvaluator.Factory(s, l, r)
    ) {
    };

    public static final ExpressionMapper<?> NOT_EQUALS = new ComparisonMapper<NotEquals>(
        org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.NotEqualsIntsEvaluator.Factory::new,
        org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.NotEqualsLongsEvaluator.Factory::new,
        org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.NotEqualsDoublesEvaluator.Factory::new,
        org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.NotEqualsKeywordsEvaluator.Factory::new,
        org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.NotEqualsBoolsEvaluator.Factory::new,
        (s, l, r, t) -> new NotEqualsGeometriesEvaluator.Factory(s, l, r)
    ) {
    };

    public static final ExpressionMapper<?> GREATER_THAN = new ComparisonMapper<GreaterThan>(
        org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.GreaterThanIntsEvaluator.Factory::new,
        org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.GreaterThanLongsEvaluator.Factory::new,
        org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.GreaterThanDoublesEvaluator.Factory::new,
        org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.GreaterThanKeywordsEvaluator.Factory::new
    ) {
    };

    public static final ExpressionMapper<?> GREATER_THAN_OR_EQUAL = new ComparisonMapper<GreaterThanOrEqual>(
        org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.GreaterThanOrEqualIntsEvaluator.Factory::new,
        org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.GreaterThanOrEqualLongsEvaluator.Factory::new,
        org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.GreaterThanOrEqualDoublesEvaluator.Factory::new,
        org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.GreaterThanOrEqualKeywordsEvaluator.Factory::new
    ) {
    };

    public static final ExpressionMapper<?> LESS_THAN = new ComparisonMapper<LessThan>(
        org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.LessThanIntsEvaluator.Factory::new,
        org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.LessThanLongsEvaluator.Factory::new,
        org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.LessThanDoublesEvaluator.Factory::new,
        org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.LessThanKeywordsEvaluator.Factory::new
    ) {
    };

    public static final ExpressionMapper<?> LESS_THAN_OR_EQUAL = new ComparisonMapper<LessThanOrEqual>(
        org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.LessThanOrEqualIntsEvaluator.Factory::new,
        org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.LessThanOrEqualLongsEvaluator.Factory::new,
        org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.LessThanOrEqualDoublesEvaluator.Factory::new,
        org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.LessThanOrEqualKeywordsEvaluator.Factory::new
    ) {
    };

    private final TriFunction<Source, ExpressionEvaluator.Factory, ExpressionEvaluator.Factory, ExpressionEvaluator.Factory> ints;
    private final TriFunction<Source, ExpressionEvaluator.Factory, ExpressionEvaluator.Factory, ExpressionEvaluator.Factory> longs;
    private final TriFunction<Source, ExpressionEvaluator.Factory, ExpressionEvaluator.Factory, ExpressionEvaluator.Factory> doubles;
    private final TriFunction<Source, ExpressionEvaluator.Factory, ExpressionEvaluator.Factory, ExpressionEvaluator.Factory> keywords;
    private final TriFunction<Source, ExpressionEvaluator.Factory, ExpressionEvaluator.Factory, ExpressionEvaluator.Factory> bools;
    private final EvaluatorFunctionWithType<DataType> geometries;

    @FunctionalInterface
    private interface EvaluatorFunctionWithType<T extends DataType> {
        ExpressionEvaluator.Factory apply(Source s, ExpressionEvaluator.Factory t, ExpressionEvaluator.Factory u, T dataType);
    }

    private ComparisonMapper(
        TriFunction<Source, ExpressionEvaluator.Factory, ExpressionEvaluator.Factory, ExpressionEvaluator.Factory> ints,
        TriFunction<Source, ExpressionEvaluator.Factory, ExpressionEvaluator.Factory, ExpressionEvaluator.Factory> longs,
        TriFunction<Source, ExpressionEvaluator.Factory, ExpressionEvaluator.Factory, ExpressionEvaluator.Factory> doubles,
        TriFunction<Source, ExpressionEvaluator.Factory, ExpressionEvaluator.Factory, ExpressionEvaluator.Factory> keywords,
        TriFunction<Source, ExpressionEvaluator.Factory, ExpressionEvaluator.Factory, ExpressionEvaluator.Factory> bools,
        EvaluatorFunctionWithType<DataType> geometries
    ) {
        this.ints = ints;
        this.longs = longs;
        this.doubles = doubles;
        this.keywords = keywords;
        this.bools = bools;
        this.geometries = geometries;
    }

    private ComparisonMapper(
        TriFunction<Source, ExpressionEvaluator.Factory, ExpressionEvaluator.Factory, ExpressionEvaluator.Factory> ints,
        TriFunction<Source, ExpressionEvaluator.Factory, ExpressionEvaluator.Factory, ExpressionEvaluator.Factory> longs,
        TriFunction<Source, ExpressionEvaluator.Factory, ExpressionEvaluator.Factory, ExpressionEvaluator.Factory> doubles,
        TriFunction<Source, ExpressionEvaluator.Factory, ExpressionEvaluator.Factory, ExpressionEvaluator.Factory> keywords,
        TriFunction<Source, ExpressionEvaluator.Factory, ExpressionEvaluator.Factory, ExpressionEvaluator.Factory> bools
    ) {
        this.ints = ints;
        this.longs = longs;
        this.doubles = doubles;
        this.keywords = keywords;
        this.bools = bools;
        this.geometries = (source, lhs, rhs, dataType) -> { throw EsqlIllegalArgumentException.illegalDataType(dataType); };
    }

    ComparisonMapper(
        TriFunction<Source, ExpressionEvaluator.Factory, ExpressionEvaluator.Factory, ExpressionEvaluator.Factory> ints,
        TriFunction<Source, ExpressionEvaluator.Factory, ExpressionEvaluator.Factory, ExpressionEvaluator.Factory> longs,
        TriFunction<Source, ExpressionEvaluator.Factory, ExpressionEvaluator.Factory, ExpressionEvaluator.Factory> doubles,
        TriFunction<Source, ExpressionEvaluator.Factory, ExpressionEvaluator.Factory, ExpressionEvaluator.Factory> keywords
    ) {
        this.ints = ints;
        this.longs = longs;
        this.doubles = doubles;
        this.keywords = keywords;
        this.bools = (source, lhs, rhs) -> { throw EsqlIllegalArgumentException.illegalDataType(DataTypes.BOOLEAN); };
        this.geometries = (source, lhs, rhs, dataType) -> { throw EsqlIllegalArgumentException.illegalDataType(dataType); };
    }

    @Override
    public final ExpressionEvaluator.Factory map(BinaryComparison bc, Layout layout) {
        DataType leftType = bc.left().dataType();
        if (leftType.isNumeric()) {
            DataType type = EsqlDataTypeRegistry.INSTANCE.commonType(leftType, bc.right().dataType());
            if (type == DataTypes.INTEGER) {
                return castToEvaluator(bc, layout, DataTypes.INTEGER, ints);
            }
            if (type == DataTypes.LONG) {
                return castToEvaluator(bc, layout, DataTypes.LONG, longs);
            }
            if (type == DataTypes.DOUBLE) {
                return castToEvaluator(bc, layout, DataTypes.DOUBLE, doubles);
            }
            if (type == DataTypes.UNSIGNED_LONG) {
                // using the long comparators will work on UL as well
                return castToEvaluator(bc, layout, DataTypes.UNSIGNED_LONG, longs);
            }
        }
        var leftEval = toEvaluator(bc.left(), layout);
        var rightEval = toEvaluator(bc.right(), layout);
        if (leftType == DataTypes.KEYWORD || leftType == DataTypes.TEXT || leftType == DataTypes.IP || leftType == DataTypes.VERSION) {
            return keywords.apply(bc.source(), leftEval, rightEval);
        }
        if (leftType == DataTypes.BOOLEAN) {
            return bools.apply(bc.source(), leftEval, rightEval);
        }
        if (leftType == DataTypes.DATETIME) {
            return longs.apply(bc.source(), leftEval, rightEval);
        }
        if (EsqlDataTypes.isSpatial(leftType)) {
            return geometries.apply(bc.source(), leftEval, rightEval, leftType);
        }
        throw new EsqlIllegalArgumentException("resolved type for [" + bc + "] but didn't implement mapping");
    }

    public static ExpressionEvaluator.Factory castToEvaluator(
        BinaryOperator<?, ?, ?, ?> op,
        Layout layout,
        DataType required,
        TriFunction<Source, ExpressionEvaluator.Factory, ExpressionEvaluator.Factory, ExpressionEvaluator.Factory> factory
    ) {
        var lhs = Cast.cast(op.source(), op.left().dataType(), required, toEvaluator(op.left(), layout));
        var rhs = Cast.cast(op.source(), op.right().dataType(), required, toEvaluator(op.right(), layout));
        return factory.apply(op.source(), lhs, rhs);
    }
}
