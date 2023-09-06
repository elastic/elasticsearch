/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison;

import org.elasticsearch.common.TriFunction;
import org.elasticsearch.compute.operator.EvalOperator;
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

import java.util.function.BiFunction;
import java.util.function.Supplier;

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

    private final BiFunction<EvalOperator.ExpressionEvaluator, EvalOperator.ExpressionEvaluator, EvalOperator.ExpressionEvaluator> ints;
    private final BiFunction<EvalOperator.ExpressionEvaluator, EvalOperator.ExpressionEvaluator, EvalOperator.ExpressionEvaluator> longs;
    private final BiFunction<EvalOperator.ExpressionEvaluator, EvalOperator.ExpressionEvaluator, EvalOperator.ExpressionEvaluator> doubles;
    private final BiFunction<EvalOperator.ExpressionEvaluator, EvalOperator.ExpressionEvaluator, EvalOperator.ExpressionEvaluator> keywords;
    private final BiFunction<EvalOperator.ExpressionEvaluator, EvalOperator.ExpressionEvaluator, EvalOperator.ExpressionEvaluator> bools;

    private ComparisonMapper(
        BiFunction<EvalOperator.ExpressionEvaluator, EvalOperator.ExpressionEvaluator, EvalOperator.ExpressionEvaluator> ints,
        BiFunction<EvalOperator.ExpressionEvaluator, EvalOperator.ExpressionEvaluator, EvalOperator.ExpressionEvaluator> longs,
        BiFunction<EvalOperator.ExpressionEvaluator, EvalOperator.ExpressionEvaluator, EvalOperator.ExpressionEvaluator> doubles,
        BiFunction<EvalOperator.ExpressionEvaluator, EvalOperator.ExpressionEvaluator, EvalOperator.ExpressionEvaluator> keywords,
        BiFunction<EvalOperator.ExpressionEvaluator, EvalOperator.ExpressionEvaluator, EvalOperator.ExpressionEvaluator> bools
    ) {
        this.ints = ints;
        this.longs = longs;
        this.doubles = doubles;
        this.keywords = keywords;
        this.bools = bools;
    }

    ComparisonMapper(
        BiFunction<EvalOperator.ExpressionEvaluator, EvalOperator.ExpressionEvaluator, EvalOperator.ExpressionEvaluator> ints,
        BiFunction<EvalOperator.ExpressionEvaluator, EvalOperator.ExpressionEvaluator, EvalOperator.ExpressionEvaluator> longs,
        BiFunction<EvalOperator.ExpressionEvaluator, EvalOperator.ExpressionEvaluator, EvalOperator.ExpressionEvaluator> doubles,
        BiFunction<EvalOperator.ExpressionEvaluator, EvalOperator.ExpressionEvaluator, EvalOperator.ExpressionEvaluator> keywords
    ) {
        this.ints = ints;
        this.longs = longs;
        this.doubles = doubles;
        this.keywords = keywords;
        this.bools = (lhs, rhs) -> { throw EsqlIllegalArgumentException.illegalDataType(DataTypes.BOOLEAN); };
    }

    @Override
    public final Supplier<EvalOperator.ExpressionEvaluator> map(BinaryComparison bc, Layout layout) {
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
        Supplier<EvalOperator.ExpressionEvaluator> leftEval = toEvaluator(bc.left(), layout);
        Supplier<EvalOperator.ExpressionEvaluator> rightEval = toEvaluator(bc.right(), layout);
        if (leftType == DataTypes.KEYWORD || leftType == DataTypes.TEXT || leftType == DataTypes.IP || leftType == DataTypes.VERSION) {
            return () -> keywords.apply(leftEval.get(), rightEval.get());
        }
        if (leftType == DataTypes.BOOLEAN) {
            return () -> bools.apply(leftEval.get(), rightEval.get());
        }
        if (leftType == DataTypes.DATETIME) {
            return () -> longs.apply(leftEval.get(), rightEval.get());
        }
        throw new EsqlIllegalArgumentException("resolved type for [" + bc + "] but didn't implement mapping");
    }

    public static Supplier<EvalOperator.ExpressionEvaluator> castToEvaluator(
        BinaryOperator<?, ?, ?, ?> op,
        Layout layout,
        DataType required,
        BiFunction<EvalOperator.ExpressionEvaluator, EvalOperator.ExpressionEvaluator, EvalOperator.ExpressionEvaluator> buildEvaluator
    ) {
        Supplier<EvalOperator.ExpressionEvaluator> lhs = Cast.cast(op.left().dataType(), required, toEvaluator(op.left(), layout));
        Supplier<EvalOperator.ExpressionEvaluator> rhs = Cast.cast(op.right().dataType(), required, toEvaluator(op.right(), layout));
        return () -> buildEvaluator.apply(lhs.get(), rhs.get());
    }

    public static Supplier<EvalOperator.ExpressionEvaluator> castToEvaluator(
        BinaryOperator<?, ?, ?, ?> op,
        Layout layout,
        DataType required,
        TriFunction<
            Source,
            EvalOperator.ExpressionEvaluator,
            EvalOperator.ExpressionEvaluator,
            EvalOperator.ExpressionEvaluator> buildEvaluator
    ) {
        Supplier<EvalOperator.ExpressionEvaluator> lhs = Cast.cast(op.left().dataType(), required, toEvaluator(op.left(), layout));
        Supplier<EvalOperator.ExpressionEvaluator> rhs = Cast.cast(op.right().dataType(), required, toEvaluator(op.right(), layout));
        return () -> buildEvaluator.apply(op.source(), lhs.get(), rhs.get());
    }
}
