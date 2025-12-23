/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql.operator.arithmetic;

import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Pow;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Div;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mod;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mul;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Sub;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorBinaryOperator;
import org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorMatch;

public class VectorBinaryArithmetic extends VectorBinaryOperator {

    public enum ArithmeticOp implements BinaryOp {
        ADD,
        SUB,
        MUL,
        DIV,
        MOD,
        POW;

        @Override
        public ScalarFunctionFactory asFunction() {
            return switch (this) {
                case ADD -> Add::new;
                case SUB -> Sub::new;
                case MUL -> Mul::new;
                case DIV -> Div::new;
                case MOD -> Mod::new;
                case POW -> Pow::new;
            };
        }
    }

    private final ArithmeticOp op;

    public VectorBinaryArithmetic(Source source, LogicalPlan left, LogicalPlan right, VectorMatch match, ArithmeticOp op) {
        super(source, left, right, match, true, op);
        this.op = op;
    }

    public ArithmeticOp op() {
        return op;
    }

    @Override
    public VectorBinaryOperator replaceChildren(LogicalPlan newLeft, LogicalPlan newRight) {
        return new VectorBinaryArithmetic(source(), newLeft, newRight, match(), op());
    }

    @Override
    protected NodeInfo<VectorBinaryArithmetic> info() {
        return NodeInfo.create(this, VectorBinaryArithmetic::new, left(), right(), match(), op());
    }
}
