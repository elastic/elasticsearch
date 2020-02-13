/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */


package org.elasticsearch.xpack.eql.parser;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.expression.Expression;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.containsString;

public class ParameterTests extends ESTestCase {

    private final EqlParser parser = new EqlParser();

    private Expression expr(String source) {
        return parser.createExpression(source);
    }

    private Expression expr(String source, List<Object> params) {
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

    public void testEscapes() {
        assertEquals(expr("a == ? and b == ?'test'", Collections.singletonList("test \\ \n \r \t ' \"")),
                     expr("a == 'test \\\\ \\n \\r \\t \\' \\\"' and b == 'test'"));
    }

    public void testInvalidParameters() {
        ParsingException ex = expectThrows(ParsingException.class,
            () -> expr("a + ?", Collections.singletonList(new Object())));

        assertThat(ex.getMessage(), containsString("Invalid parameter"));
    }

    public void testMultipleParameters() {
        Expression expression = expr("(? + ? * ?) - ?", Arrays.asList(1, 2, 3, 4));
        assertEquals(expression, expr("(1 + 2 * 3) - 4"));
    }

    public void testNotEnoughParameters() {
        ParsingException ex = expectThrows(ParsingException.class,
            () -> expr("(? + ? * ?) - ?", Arrays.asList(1, 2, 3)));

        assertThat(ex.getMessage(), containsString("Not enough actual parameters"));
    }
}
