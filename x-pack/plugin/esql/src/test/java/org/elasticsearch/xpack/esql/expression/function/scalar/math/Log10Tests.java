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
import org.hamcrest.Matcher;

import java.util.List;

import static org.elasticsearch.xpack.ql.type.DataTypes.DOUBLE;
import static org.hamcrest.Matchers.equalTo;

public class Log10Tests extends AbstractScalarFunctionTestCase {

    @Override
    protected List<Object> simpleData() {
        return List.of(1000.0d);
    }

    @Override
    protected Expression expressionForSimpleData() {
        return new Log10(Source.EMPTY, field("arg", DOUBLE));
    }

    @Override
    protected Matcher<Object> resultMatcher(List<Object> data, DataType dataType) {
        return equalTo(Math.log10((Double) data.get(0)));
    }

    @Override
    protected Matcher<Object> resultMatcher(List<Object> data) {
        return equalTo(Math.log10((Double) data.get(0)));
    }

    @Override
    protected String expectedEvaluatorSimpleToString() {
        return "Log10DoubleEvaluator[val=Attribute[channel=0]]";
    }

    @Override
    protected Expression constantFoldable(List<Object> data) {
        return new Log10(Source.EMPTY, new Literal(Source.EMPTY, data.get(0), DOUBLE));
    }

    @Override
    protected Expression build(Source source, List<Literal> args) {
        return new Log10(source, args.get(0));
    }

    @Override
    protected List<ArgumentSpec> argSpec() {
        return List.of(required(numerics()));
    }

    @Override
    protected DataType expectedType(List<DataType> argTypes) {
        return DOUBLE;
    }
}
