/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.xpack.esql.expression.function.scalar.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.hamcrest.Matcher;

import java.util.List;

public abstract class AbstractRationalUnaryPredicateTests extends AbstractScalarFunctionTestCase {
    protected abstract RationalUnaryPredicate build(Source source, Expression value);

    protected abstract Matcher<Object> resultMatcher(double d);

    @Override
    protected final List<Object> simpleData() {
        return List.of(switch (between(0, 2)) {
            case 0 -> Double.NaN;
            case 1 -> randomBoolean() ? Double.POSITIVE_INFINITY : Double.NEGATIVE_INFINITY;
            case 2 -> randomDouble();
            default -> throw new IllegalArgumentException();
        });
    }

    @Override
    protected final Expression expressionForSimpleData() {
        return build(Source.EMPTY, field("v", DataTypes.DOUBLE));
    }

    @Override
    protected DataType expectedType(List<DataType> argTypes) {
        return DataTypes.BOOLEAN;
    }

    @Override
    protected final Matcher<Object> resultMatcher(List<Object> data) {
        double d = (Double) data.get(0);
        return resultMatcher(d);
    }

    @Override
    protected final Expression constantFoldable(List<Object> data) {
        return build(Source.EMPTY, new Literal(Source.EMPTY, data.get(0), DataTypes.DOUBLE));
    }

    @Override
    protected final List<ArgumentSpec> argSpec() {
        return List.of(required(rationals()));
    }

    @Override
    protected Expression build(Source source, List<Literal> args) {
        return build(source, args.get(0));
    }

    private void testCase(double d) {
        BooleanBlock block = (BooleanBlock) evaluator(expressionForSimpleData()).get().eval(row(List.of(d)));
        assertThat(block.getBoolean(0), resultMatcher(d));
    }

    public final void testNaN() {
        testCase(Double.NaN);
    }

    public final void testPositiveInfinity() {
        testCase(Double.POSITIVE_INFINITY);
    }

    public final void testNegativeInfinity() {
        testCase(Double.NEGATIVE_INFINITY);
    }

    public final void testPositiveSmallDouble() {
        testCase(randomDouble());
    }

    public final void testNegativeSmallDouble() {
        testCase(-randomDouble());
    }

    public final void testPositiveBigDouble() {
        testCase(1 / randomDouble());
    }

    public final void testNegativeBigDouble() {
        testCase(-1 / randomDouble());
    }

}
