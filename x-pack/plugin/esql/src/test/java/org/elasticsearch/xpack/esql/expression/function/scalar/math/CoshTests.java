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

public class CoshTests extends AbstractScalarFunctionTestCase {
    @Override
    protected TestCase getSimpleTestCase() {
        double d = 1 / randomDouble();
        List<TypedData> typedData = List.of(new TypedData(d, DataTypes.DOUBLE, "arg"));
        return new TestCase(Source.EMPTY, typedData, equalTo(Math.cosh(d)));
    }

    @Override
    protected DataType expectedType(List<DataType> argTypes) {
        return DataTypes.DOUBLE;
    }

    @Override
    protected Matcher<Object> resultMatcher(List<Object> data, DataType dataType) {
        return equalTo(Math.cosh(((Number) data.get(0)).doubleValue()));
    }

    @Override
    protected String expectedEvaluatorSimpleToString() {
        return "CoshEvaluator[val=Attribute[channel=0]]";
    }

    @Override
    protected List<ArgumentSpec> argSpec() {
        return List.of(required(numerics()));
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Cosh(source, args.get(0));
    }
}
