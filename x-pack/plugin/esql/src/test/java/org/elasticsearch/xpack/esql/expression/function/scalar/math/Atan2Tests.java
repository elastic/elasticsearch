/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import org.elasticsearch.xpack.esql.expression.function.scalar.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.hamcrest.Matcher;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class Atan2Tests extends AbstractScalarFunctionTestCase {
    @Override
    protected TestCase getSimpleTestCase() {
        double y = randomDoubleBetween(Double.MIN_VALUE, Double.MAX_VALUE, true);
        double x = randomDoubleBetween(Double.MIN_VALUE, Double.MAX_VALUE, true);
        List<TypedData> typedData = List.of(new TypedData(y, DataTypes.DOUBLE, "y"), new TypedData(x, DataTypes.DOUBLE, "x"));
        return new TestCase(Source.EMPTY, typedData, equalTo(Math.atan2(y, x)));
    }

    @Override
    protected DataType expectedType(List<DataType> argTypes) {
        return DataTypes.DOUBLE;
    }

    @Override
    protected Matcher<Object> resultMatcher(List<Object> data, DataType dataType) {
        return equalTo(Math.atan2(((Number) data.get(0)).doubleValue(), ((Number) data.get(1)).doubleValue()));
    }

    @Override
    protected String expectedEvaluatorSimpleToString() {
        return "Atan2Evaluator[y=Attribute[channel=0], x=Attribute[channel=1]]";
    }

    @Override
    protected List<ArgumentSpec> argSpec() {
        return List.of(required(numerics()), required(numerics()));
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Atan2(source, args.get(0), args.get(1));
    }
}
