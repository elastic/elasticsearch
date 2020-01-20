/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.parser;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.Mul;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.Sub;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.sql.parser.ParsingException;
import org.elasticsearch.xpack.sql.parser.SqlParser;
import org.elasticsearch.xpack.sql.proto.SqlTypedParamValue;

import java.util.Arrays;
import java.util.Collections;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;


public class ParameterTests extends ESTestCase {

    public void testSingleParameter() {
        Expression expression = new SqlParser().createExpression("a = \n?",
                Collections.singletonList(
                        new SqlTypedParamValue(DataType.KEYWORD.typeName(), "foo")
                ));
        logger.info(expression);
        assertThat(expression, instanceOf(Equals.class));
        Expression right = ((Equals) expression).right();
        assertThat(right, instanceOf(Literal.class));
        Literal param = (Literal) right;
        assertThat(param.dataType(), equalTo(DataType.KEYWORD));
        assertThat(param.dataType(), equalTo(DataType.KEYWORD));
        assertThat(param.value(), equalTo("foo"));
    }

    public void testMultipleParameters() {
        Expression expression = new SqlParser().createExpression("(? + ? * ?) - ?", Arrays.asList(
                new SqlTypedParamValue(DataType.LONG.typeName(), 1L),
                new SqlTypedParamValue(DataType.LONG.typeName(), 2L),
                new SqlTypedParamValue(DataType.LONG.typeName(), 3L),
                new SqlTypedParamValue(DataType.LONG.typeName(), 4L)
                ));
        assertThat(expression, instanceOf(Sub.class));
        Sub sub = (Sub) expression;
        assertThat(((Literal) sub.right()).value(), equalTo(4L));
        assertThat(sub.left(), instanceOf(Add.class));
        Add add = (Add) sub.left();
        assertThat(((Literal) add.left()).value(), equalTo(1L));
        assertThat(add.right(), instanceOf(Mul.class));
        Mul mul = (Mul) add.right();
        assertThat(((Literal) mul.left()).value(), equalTo(2L));
        assertThat(((Literal) mul.right()).value(), equalTo(3L));
    }

    public void testNotEnoughParameters() {
        ParsingException ex = expectThrows(ParsingException.class,
                () -> new SqlParser().createExpression("(? + ? * ?) - ?", Arrays.asList(
                        new SqlTypedParamValue(DataType.LONG.typeName(), 1L),
                        new SqlTypedParamValue(DataType.LONG.typeName(), 2L),
                        new SqlTypedParamValue(DataType.LONG.typeName(), 3L)
                )));
        assertThat(ex.getMessage(), containsString("Not enough actual parameters"));
    }
}
