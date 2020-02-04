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

import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.xpack.eql.parser.AbstractBuilder.unquoteString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class ExpressionTests extends ESTestCase {

    private final EqlParser parser = new EqlParser();

    public Expression expr(String source) {
        return parser.createExpression(source);
    }


    public void testStrings() throws Exception {
        assertEquals("hello\"world", unquoteString("'hello\"world'"));
        assertEquals("hello'world", unquoteString("\"hello'world\""));
        assertEquals("hello\nworld", unquoteString("'hello\\nworld'"));
        assertEquals("hello\\\nworld", unquoteString("'hello\\\\\\nworld'"));
        assertEquals("hello\\\"world", unquoteString("'hello\\\\\\\"world'"));

        // test for unescaped strings: ?"...." or ?'....'
        assertEquals("hello\"world", unquoteString("?'hello\"world'"));
        assertEquals("hello\\\"world", unquoteString("?'hello\\\"world'"));
        assertEquals("hello'world", unquoteString("?\"hello'world\""));
        assertEquals("hello\\nworld", unquoteString("?'hello\\nworld'"));
        assertEquals("hello\\\\nworld", unquoteString("?'hello\\\\nworld'"));
        assertEquals("hello\\\\\\nworld", unquoteString("?'hello\\\\\\nworld'"));
        assertEquals("hello\\\\\\\"world", unquoteString("?'hello\\\\\\\"world'"));
    }

    public void testLiterals() {
        assertEquals(Literal.TRUE, expr("true"));
        assertEquals(Literal.FALSE, expr("false"));
        assertEquals(Literal.NULL, expr("null"));
    }

    public void testSingleQuotedString() {
        // "hello \" world"
        Expression parsed = expr("'hello \\' world!'");
        Expression expected = new Literal(null, "hello ' world!", DataTypes.KEYWORD);
        assertEquals(expected, parsed);
    }

    public void testDoubleQuotedString() {
        // "hello \" world"
        Expression parsed = expr("\"hello \\\" world!\"");
        Expression expected = new Literal(null, "hello \" world!", DataTypes.KEYWORD);
        assertEquals(expected, parsed);
    }

    public void testSingleQuotedUnescapedString() {
        // "hello \" world"
        Expression parsed = expr("?'hello \\' world!'");
        Expression expected = new Literal(null, "hello \\' world!", DataTypes.KEYWORD);
        assertEquals(expected, parsed);
    }

    public void testDoubleQuotedUnescapedString() {
        // "hello \" world"
        Expression parsed = expr("?\"hello \\\" world!\"");
        Expression expected = new Literal(null, "hello \\\" world!", DataTypes.KEYWORD);
        assertEquals(expected, parsed);
    }

    public void testNumbers() {
        assertEquals(new Literal(null, 8589934592L, DataTypes.LONG), expr("8589934592"));
        assertEquals(new Literal(null, 5, DataTypes.INTEGER), expr("5"));
        assertEquals(new Literal(null, 5e14, DataTypes.DOUBLE), expr("5e14"));
        assertEquals(new Literal(null, 5.2, DataTypes.DOUBLE), expr("5.2"));

        Expression parsed = expr("-5.2");
        Expression expected = new Neg(null, new Literal(null, 5.2, DataTypes.DOUBLE));
        assertEquals(expected, parsed);
    }

    public void testBackQuotedAttribute() {
        String quote = "`";
        String qualifier = "table";
        String name = "@timestamp";
        Expression exp = expr(quote + qualifier + quote + "." + quote + name + quote);
        assertThat(exp, instanceOf(UnresolvedAttribute.class));
        UnresolvedAttribute ua = (UnresolvedAttribute) exp;
        assertThat(ua.name(), equalTo(qualifier + "." + name));
        assertThat(ua.qualifiedName(), equalTo(qualifier + "." + name));
        assertThat(ua.qualifier(), is(nullValue()));
    }

    public void testFunctions() {
        List<Expression> arguments = Arrays.asList(
            new UnresolvedAttribute(null, "some.field"),
            new Literal(null, "test string", DataTypes.KEYWORD)
        );
        UnresolvedFunction.ResolutionType resolutionType = UnresolvedFunction.ResolutionType.STANDARD;
        Expression expected = new UnresolvedFunction(null, "concat", resolutionType, arguments);

        assertEquals(expected, expr("concat(some.field, 'test string')"));
    }

    public void testComparison() {
        String fieldText = "field";
        String valueText = "2.0";

        Expression field = expr(fieldText);
        Expression value = expr(valueText);

        assertEquals(new Equals(null, field, value), expr(fieldText + "==" + valueText));
        assertEquals(new NotEquals(null, field, value), expr(fieldText + "!=" + valueText));
        assertEquals(new LessThanOrEqual(null, field, value), expr(fieldText + "<=" + valueText));
        assertEquals(new GreaterThanOrEqual(null, field, value), expr(fieldText + ">=" + valueText));
        assertEquals(new GreaterThan(null, field, value), expr(fieldText + ">" + valueText));
        assertEquals(new LessThan(null, field, value), expr(fieldText + "<" + valueText));
    }

    public void testBoolean() {
        String leftText = "process_name == 'net.exe'";
        String rightText = "command_line == '* localgroup*'";

        Expression lhs = expr(leftText);
        Expression rhs = expr(rightText);

        Expression booleanAnd = expr(leftText + " and " + rightText);
        assertEquals(new And(null, lhs, rhs), booleanAnd);

        Expression booleanOr = expr(leftText + " or " + rightText);
        assertEquals(new Or(null, lhs, rhs), booleanOr);
    }

    public void testInSet() {
        assertEquals(
            expr("name in ('net.exe')"),
            expr("name == 'net.exe'")
        );

        assertEquals(
            expr("name in ('net.exe', 'whoami.exe', 'hostname.exe')"),
            expr("name == 'net.exe' or name == 'whoami.exe' or name == 'hostname.exe'")
        );

        assertEquals(
            expr("name not in ('net.exe', 'whoami.exe', 'hostname.exe')"),
            expr("not (name == 'net.exe' or name == 'whoami.exe' or name == 'hostname.exe')")
        );
    }
}
