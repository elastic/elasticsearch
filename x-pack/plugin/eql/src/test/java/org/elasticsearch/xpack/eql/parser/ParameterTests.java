/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */


package org.elasticsearch.xpack.eql.parser;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.ql.expression.function.UnresolvedFunction;
import org.elasticsearch.xpack.ql.expression.predicate.logical.And;
import org.elasticsearch.xpack.ql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.Neg;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.sql.proto.SqlTypedParamValue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.xpack.eql.parser.AbstractBuilder.unquoteString;
import static org.elasticsearch.xpack.ql.type.DataTypes.KEYWORD;
import static org.elasticsearch.xpack.ql.type.DataTypes.LONG;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class ParameterTests extends ESTestCase {

    private final EqlParser parser = new EqlParser();

    public Expression expr(String source) {
        return parser.createExpression(source);
    }
    public Expression expr(String source, List<Object> params) {
        return parser.createExpression(source, params);
    }

    public void testSingleParameter() {
        assertEquals(expr("a == ?", Collections.singletonList("test")), expr("a == 'test'"));
        assertEquals(expr("a == ?", Collections.singletonList(1)), expr("a == 1"));
        assertEquals(expr("a == ?", Collections.singletonList(1.0)), expr("a == 1.0"));
        assertEquals(expr("a == ?", Collections.singletonList(true)), expr("a == true"));
        assertEquals(expr("a == ?", Collections.singletonList(false)), expr("a == false"));
        assertEquals(expr("a == ?", Collections.singletonList(null)), expr("a == null"));
    }

    public void testInvalidParameters() {
        ParsingException ex = expectThrows(ParsingException.class,
            () -> new EqlParser().createExpression("a + ?", Collections.singletonList(new Object())));
        assertThat(ex.getMessage(), containsString("Invalid parameter"));
    }
    public void testMultipleParameters() {
        Expression expression = expr("(? + ? * ?) - ?", Arrays.asList(1, 2, 3, 4));
        assertEquals(expression, expr("(1 + 2 * 3) - 4"));
    }

    public void testNotEnoughParameters() {
        ParsingException ex = expectThrows(ParsingException.class,
            () -> new EqlParser().createExpression("(? + ? * ?) - ?", Arrays.asList(1, 2, 3)));
        assertThat(ex.getMessage(), containsString("Not enough actual parameters"));
    }
}
