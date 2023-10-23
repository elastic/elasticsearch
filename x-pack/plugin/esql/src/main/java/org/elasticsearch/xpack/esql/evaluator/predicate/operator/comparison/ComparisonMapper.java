/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison;

import org.elasticsearch.common.TriFunction;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.evaluator.mapper.ExpressionMapper;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Cast;
import org.elasticsearch.xpack.esql.planner.Layout;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeRegistry;
import org.elasticsearch.xpack.ql.expression.predicate.BinaryOperator;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.BinaryComparison;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import static org.elasticsearch.xpack.esql.evaluator.EvalMapper.toEvaluator;

public abstract class ComparisonMapper<T extends BinaryComparison> extends ExpressionMapper<T> {
    public static final ExpressionMapper<?> EQUALS = new ComparisonMapper<Equals>(
        org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.EqualsIntsEvaluator::new,
        org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.EqualsLongsEvaluator::new,
        org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.EqualsDoublesEvaluator::new,
        org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.EqualsKeywordsEvaluator::new,
        org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.EqualsBoolsEvaluator::new
    ) {
    };

    public static final ExpressionMapper<?> NOT_EQUALS = new ComparisonMapper<NotEquals>(
        org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.NotEqualsIntsEvaluator::new,
        org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.NotEqualsLongsEvaluator::new,
        org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.NotEqualsDoublesEvaluator::new,
        org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.NotEqualsKeywordsEvaluator::new,
        org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.NotEqualsBoolsEvaluator::new
    ) {
    };

    public static final ExpressionMapper<?> GREATER_THAN = new ComparisonMapper<GreaterThan>(
        org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.GreaterThanIntsEvaluator::new,
        org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.GreaterThanLongsEvaluator::new,
        org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.GreaterThanDoublesEvaluator::new,
        org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.GreaterThanKeywordsEvaluator::new
    ) {
    };

    public static final ExpressionMapper<?> GREATER_THAN_OR_EQUAL = new ComparisonMapper<GreaterThanOrEqual>(
        org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.GreaterThanOrEqualIntsEvaluator::new,
        org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.GreaterThanOrEqualLongsEvaluator::new,
        org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.GreaterThanOrEqualDoublesEvaluator::new,
        org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.GreaterThanOrEqualKeywordsEvaluator::new
    ) {
    };

    public static final ExpressionMapper<?> LESS_THAN = new ComparisonMapper<LessThan>(
        org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.LessThanIntsEvaluator::new,
        org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.LessThanLongsEvaluator::new,
        org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.LessThanDoublesEvaluator::new,
        org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.LessThanKeywordsEvaluator::new
    ) {
    };

    public static final ExpressionMapper<?> LESS_THAN_OR_EQUAL = new ComparisonMapper<LessThanOrEqual>(
        org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.LessThanOrEqualIntsEvaluator::new,
        org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.LessThanOrEqualLongsEvaluator::new,
        org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.LessThanOrEqualDoublesEvaluator::new,
        org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.LessThanOrEqualKeywordsEvaluator::new
    ) {
    };

    private final TriFunction<ExpressionEvaluator, ExpressionEvaluator, DriverContext, ExpressionEvaluator> ints;
    private final TriFunction<ExpressionEvaluator, ExpressionEvaluator, DriverContext, ExpressionEvaluator> longs;
    private final TriFunction<ExpressionEvaluator, ExpressionEvaluator, DriverContext, ExpressionEvaluator> doubles;
    private final TriFunction<ExpressionEvaluator, ExpressionEvaluator, DriverContext, ExpressionEvaluator> keywords;
    private final TriFunction<ExpressionEvaluator, ExpressionEvaluator, DriverContext, ExpressionEvaluator> bools;

    private ComparisonMapper(
        TriFunction<ExpressionEvaluator, ExpressionEvaluator, DriverContext, ExpressionEvaluator> ints,
        TriFunction<ExpressionEvaluator, ExpressionEvaluator, DriverContext, ExpressionEvaluator> longs,
        TriFunction<ExpressionEvaluator, ExpressionEvaluator, DriverContext, ExpressionEvaluator> doubles,
        TriFunction<ExpressionEvaluator, ExpressionEvaluator, DriverContext, ExpressionEvaluator> keywords,
        TriFunction<ExpressionEvaluator, ExpressionEvaluator, DriverContext, ExpressionEvaluator> bools
    ) {
        this.ints = ints;
        this.longs = longs;
        this.doubles = doubles;
        this.keywords = keywords;
        this.bools = bools;
    }

    ComparisonMapper(
        TriFunction<ExpressionEvaluator, ExpressionEvaluator, DriverContext, ExpressionEvaluator> ints,
        TriFunction<ExpressionEvaluator, ExpressionEvaluator, DriverContext, ExpressionEvaluator> longs,
        TriFunction<ExpressionEvaluator, ExpressionEvaluator, DriverContext, ExpressionEvaluator> doubles,
        TriFunction<ExpressionEvaluator, ExpressionEvaluator, DriverContext, ExpressionEvaluator> keywords
    ) {
        this.ints = ints;
        this.longs = longs;
        this.doubles = doubles;
        this.keywords = keywords;
        this.bools = (lhs, rhs, dvrCtx) -> { throw EsqlIllegalArgumentException.illegalDataType(DataTypes.BOOLEAN); };
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
            return dvrCtx -> keywords.apply(leftEval.get(dvrCtx), rightEval.get(dvrCtx), dvrCtx);
        }
        if (leftType == DataTypes.BOOLEAN) {
            return dvrCtx -> bools.apply(leftEval.get(dvrCtx), rightEval.get(dvrCtx), dvrCtx);
        }
        if (leftType == DataTypes.DATETIME) {
            return dvrCtx -> longs.apply(leftEval.get(dvrCtx), rightEval.get(dvrCtx), dvrCtx);
        }
        throw new EsqlIllegalArgumentException("resolved type for [" + bc + "] but didn't implement mapping");
    }

    public static ExpressionEvaluator.Factory castToEvaluator(
        BinaryOperator<?, ?, ?, ?> op,
        Layout layout,
        DataType required,
        TriFunction<ExpressionEvaluator, ExpressionEvaluator, DriverContext, ExpressionEvaluator> buildEvaluator
    ) {
        var lhs = Cast.cast(op.left().dataType(), required, toEvaluator(op.left(), layout));
        var rhs = Cast.cast(op.right().dataType(), required, toEvaluator(op.right(), layout));
        return dvrCtx -> buildEvaluator.apply(lhs.get(dvrCtx), rhs.get(dvrCtx), dvrCtx);
    }

    public static ExpressionEvaluator.Factory castToEvaluatorWithSource(
        BinaryOperator<?, ?, ?, ?> op,
        Layout layout,
        DataType required,
        TriFunction<
            Source,
            EvalOperator.ExpressionEvaluator,
            EvalOperator.ExpressionEvaluator,
            EvalOperator.ExpressionEvaluator> buildEvaluator
    ) {
        var lhs = Cast.cast(op.left().dataType(), required, toEvaluator(op.left(), layout));
        var rhs = Cast.cast(op.right().dataType(), required, toEvaluator(op.right(), layout));
        return dvrCtx -> buildEvaluator.apply(op.source(), lhs.get(dvrCtx), rhs.get(dvrCtx));
    }
}
