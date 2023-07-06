/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import org.elasticsearch.xpack.esql.expression.function.scalar.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.hamcrest.Matcher;

import java.util.List;

import static org.elasticsearch.compute.data.BlockUtils.toJavaObject;
import static org.hamcrest.Matchers.equalTo;

public class AbsTests extends AbstractScalarFunctionTestCase {
    @Override
    protected List<Object> simpleData() {
        return List.of(randomInt());
    }

    @Override
    protected Expression expressionForSimpleData() {
        return new Abs(Source.EMPTY, field("arg", DataTypes.INTEGER));
    }

    @Override
    protected Matcher<Object> resultMatcher(List<Object> data, DataType dataType) {
        Object in = data.get(0);
        if (dataType == DataTypes.INTEGER) {
            return equalTo(Math.abs(((Integer) in).intValue()));
        }
        if (dataType == DataTypes.LONG) {
            return equalTo(Math.abs(((Long) in).longValue()));
        }
        if (dataType == DataTypes.UNSIGNED_LONG) {
            return equalTo(in);
        }
        if (dataType == DataTypes.DOUBLE) {
            return equalTo(Math.abs(((Double) in).doubleValue()));
        }
        throw new IllegalArgumentException("can't match " + in);
    }

    @Override
    protected String expectedEvaluatorSimpleToString() {
        return "AbsIntEvaluator[fieldVal=Attribute[channel=0]]";
    }

    @Override
    protected Expression constantFoldable(List<Object> data) {
        return new Abs(Source.EMPTY, new Literal(Source.EMPTY, data.get(0), DataTypes.INTEGER));
    }

    @Override
    protected Expression build(Source source, List<Literal> args) {
        return new Abs(source, args.get(0));
    }

    @Override
    protected List<ArgumentSpec> argSpec() {
        return List.of(required(numerics()));
    }

    @Override
    protected DataType expectedType(List<DataType> argTypes) {
        return argTypes.get(0);
    }

    public final void testLong() {
        List<Object> data = List.of(randomLong());
        Expression expression = new Abs(Source.EMPTY, field("arg", DataTypes.LONG));
        Object result = toJavaObject(evaluator(expression).get().eval(row(data)), 0);
        assertThat(result, resultMatcher(data, DataTypes.LONG));
    }

    public final void testUnsignedLong() {
        List<Object> data = List.of(randomLong());
        Expression expression = new Abs(Source.EMPTY, field("arg", DataTypes.UNSIGNED_LONG));
        Object result = toJavaObject(evaluator(expression).get().eval(row(data)), 0);
        assertThat(result, resultMatcher(data, DataTypes.UNSIGNED_LONG));
    }

    public final void testInt() {
        List<Object> data = List.of(randomInt());
        Expression expression = new Abs(Source.EMPTY, field("arg", DataTypes.INTEGER));
        Object result = toJavaObject(evaluator(expression).get().eval(row(data)), 0);
        assertThat(result, resultMatcher(data, DataTypes.INTEGER));
    }

    public final void testDouble() {
        List<Object> data = List.of(randomDouble());
        Expression expression = new Abs(Source.EMPTY, field("arg", DataTypes.DOUBLE));
        Object result = toJavaObject(evaluator(expression).get().eval(row(data)), 0);
        assertThat(result, resultMatcher(data, DataTypes.DOUBLE));
    }
}
