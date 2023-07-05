/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.xpack.esql.expression.function.scalar.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.hamcrest.Matcher;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class PiTests extends AbstractScalarFunctionTestCase {
    @Override
    protected List<Object> simpleData() {
        return List.of(1); // Need to put some data in the input page or it'll fail to build
    }

    @Override
    protected Expression expressionForSimpleData() {
        return new Pi(Source.EMPTY);
    }

    @Override
    protected Matcher<Object> resultMatcher(List<Object> data, DataType dataType) {
        return equalTo(Math.PI);
    }

    @Override
    protected String expectedEvaluatorSimpleToString() {
        return "LiteralsEvaluator[block=3.141592653589793]";
    }

    @Override
    protected Expression constantFoldable(List<Object> data) {
        return expressionForSimpleData();
    }

    @Override
    protected Expression build(Source source, List<Literal> args) {
        return expressionForSimpleData();
    }

    @Override
    protected List<ArgumentSpec> argSpec() {
        return List.of();
    }

    @Override
    protected DataType expectedType(List<DataType> argTypes) {
        return DataTypes.DOUBLE;
    }

    @Override
    protected void assertSimpleWithNulls(List<Object> data, Block value, int nullBlock) {
        assertThat(((DoubleBlock) value).asVector().getDouble(0), equalTo(Math.PI));
    }
}
