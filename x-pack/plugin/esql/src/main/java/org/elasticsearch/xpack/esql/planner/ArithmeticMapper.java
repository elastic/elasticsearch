/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeRegistry;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.ArithmeticOperation;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.Div;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.Mod;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.Mul;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.Sub;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.function.BiFunction;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.planner.ComparisonMapper.castToEvaluator;

abstract class ArithmeticMapper<T extends ArithmeticOperation> extends EvalMapper.ExpressionMapper<T> {
    static final EvalMapper.ExpressionMapper<?> ADD = new ArithmeticMapper<Add>(
        org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.AddIntsEvaluator::new,
        org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.AddLongsEvaluator::new,
        org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.AddDoublesEvaluator::new
    ) {
    };

    static final EvalMapper.ExpressionMapper<?> DIV = new ArithmeticMapper<Div>(
        org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.DivIntsEvaluator::new,
        org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.DivLongsEvaluator::new,
        org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.DivDoublesEvaluator::new
    ) {
    };

    static final EvalMapper.ExpressionMapper<?> MOD = new ArithmeticMapper<Mod>(
        org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.ModIntsEvaluator::new,
        org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.ModLongsEvaluator::new,
        org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.ModDoublesEvaluator::new
    ) {
    };

    static final EvalMapper.ExpressionMapper<?> MUL = new ArithmeticMapper<Mul>(
        org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.MulIntsEvaluator::new,
        org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.MulLongsEvaluator::new,
        org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.MulDoublesEvaluator::new
    ) {
    };

    static final EvalMapper.ExpressionMapper<?> SUB = new ArithmeticMapper<Sub>(
        org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.SubIntsEvaluator::new,
        org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.SubLongsEvaluator::new,
        org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.SubDoublesEvaluator::new
    ) {
    };

    private final BiFunction<EvalOperator.ExpressionEvaluator, EvalOperator.ExpressionEvaluator, EvalOperator.ExpressionEvaluator> ints;
    private final BiFunction<EvalOperator.ExpressionEvaluator, EvalOperator.ExpressionEvaluator, EvalOperator.ExpressionEvaluator> longs;
    private final BiFunction<EvalOperator.ExpressionEvaluator, EvalOperator.ExpressionEvaluator, EvalOperator.ExpressionEvaluator> doubles;

    private ArithmeticMapper(
        BiFunction<EvalOperator.ExpressionEvaluator, EvalOperator.ExpressionEvaluator, EvalOperator.ExpressionEvaluator> ints,
        BiFunction<EvalOperator.ExpressionEvaluator, EvalOperator.ExpressionEvaluator, EvalOperator.ExpressionEvaluator> longs,
        BiFunction<EvalOperator.ExpressionEvaluator, EvalOperator.ExpressionEvaluator, EvalOperator.ExpressionEvaluator> doubles
    ) {
        this.ints = ints;
        this.longs = longs;
        this.doubles = doubles;
    }

    @Override
    protected final Supplier<EvalOperator.ExpressionEvaluator> map(ArithmeticOperation op, Layout layout) {
        if (op.left().dataType().isNumeric()) {
            DataType type = EsqlDataTypeRegistry.INSTANCE.commonType(op.left().dataType(), op.right().dataType());
            if (type == DataTypes.INTEGER) {
                return castToEvaluator(op, layout, DataTypes.INTEGER, ints);
            }
            if (type == DataTypes.LONG) {
                return castToEvaluator(op, layout, DataTypes.LONG, longs);
            }
            if (type == DataTypes.DOUBLE) {
                return castToEvaluator(op, layout, DataTypes.DOUBLE, doubles);
            }
        }
        throw new AssertionError("resolved type for [" + op + "] but didn't implement mapping");
    }
}
