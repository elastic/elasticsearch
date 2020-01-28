/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
        assertEquals("hello\"world", AstBuilder.unquoteString("'hello\"world'"));
        assertEquals("hello'world", AstBuilder.unquoteString("\"hello'world\""));
        assertEquals("hello\nworld", AstBuilder.unquoteString("'hello\\nworld'"));
        assertEquals("hello\\\nworld", AstBuilder.unquoteString("'hello\\\\\\nworld'"));
        assertEquals("hello\\\"world", AstBuilder.unquoteString("'hello\\\\\\\"world'"));

        // test for unescaped strings: ?"...." or ?'....'
        assertEquals("hello\"world", AstBuilder.unquoteString("?'hello\"world'"));
        assertEquals("hello\\\"world", AstBuilder.unquoteString("?'hello\\\"world'"));
        assertEquals("hello'world", AstBuilder.unquoteString("?\"hello'world\""));
        assertEquals("hello\\nworld", AstBuilder.unquoteString("?'hello\\nworld'"));
        assertEquals("hello\\\\nworld", AstBuilder.unquoteString("?'hello\\\\nworld'"));
        assertEquals("hello\\\\\\nworld", AstBuilder.unquoteString("?'hello\\\\\\nworld'"));
        assertEquals("hello\\\\\\\"world", AstBuilder.unquoteString("?'hello\\\\\\\"world'"));
    }

    public void testLiterals() {
        assertEquals(Literal.TRUE, parser.createExpression("true"));
        assertEquals(Literal.FALSE, parser.createExpression("false"));
        assertEquals(Literal.NULL, parser.createExpression("null"));
    }

    public void testSingleQuotedString() {
        // "hello \" world"
        Expression parsed = parser.createExpression("'hello \\' world!'");
        Expression expected = new Literal(null, "hello ' world!", DataTypes.KEYWORD);
        assertEquals(expected, parsed);
    }

    public void testDoubleQuotedString() {
        // "hello \" world"
        Expression parsed = parser.createExpression("\"hello \\\" world!\"");
        Expression expected = new Literal(null, "hello \" world!", DataTypes.KEYWORD);
        assertEquals(expected, parsed);
    }

    public void testSingleQuotedUnescapedString() {
        // "hello \" world"
        Expression parsed = parser.createExpression("?'hello \\' world!'");
        Expression expected = new Literal(null, "hello \\' world!", DataTypes.KEYWORD);
        assertEquals(expected, parsed);
    }

    public void testDoubleQuotedUnescapedString() {
        // "hello \" world"
        Expression parsed = parser.createExpression("?\"hello \\\" world!\"");
        Expression expected = new Literal(null, "hello \\\" world!", DataTypes.KEYWORD);
        assertEquals(expected, parsed);
    }

    public void testNumbers() {
        assertEquals(new Literal(null, 8589934592L, DataTypes.LONG), parser.createExpression("8589934592"));
        assertEquals(new Literal(null, 5, DataTypes.INTEGER), parser.createExpression("5"));
        assertEquals(new Literal(null, 5e14, DataTypes.DOUBLE), parser.createExpression("5e14"));
        assertEquals(new Literal(null, 5.2, DataTypes.DOUBLE), parser.createExpression("5.2"));

        Expression parsed = parser.createExpression("-5.2");
        Expression expected = new Neg(null, new Literal(null, 5.2, DataTypes.DOUBLE));
        assertEquals(expected, parsed);
    }

    public void testBackQuotedAttribute() {
        String quote = "`";
        String qualifier = "table";
        String name = "@timestamp";
        Expression exp = parser.createExpression(quote + qualifier + quote + "." + quote + name + quote);
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

        assertEquals(expected, parser.createExpression("concat(some.field, 'test string')"));
    }

    public void testComparison() {
        String fieldText = "field";
        String valueText = "2.0";

        Expression field = expr(fieldText);
        Expression value = expr(valueText);

        assertEquals( new Equals(null, field, value), expr(fieldText + "==" + valueText));
        assertEquals( new NotEquals(null, field, value), expr(fieldText + "!=" + valueText));
        assertEquals( new LessThanOrEqual(null, field, value), expr(fieldText + "<=" + valueText));
        assertEquals( new GreaterThanOrEqual(null, field, value), expr(fieldText + ">=" + valueText));
        assertEquals( new GreaterThan(null, field, value), expr(fieldText + ">" + valueText));
        assertEquals( new LessThan(null, field, value), expr(fieldText + "<" + valueText));
    }

    public void testEventQuery() {
        Expression fullQuery = parser.createStatement("process where process_name == 'net.exe'");
        Expression baseExpression = parser.createExpression("process_name == 'net.exe'");
        Expression fullExpression = parser.createExpression("event.category == 'process' and process_name == 'net.exe'");
        assertEquals(fullQuery, fullExpression);
        assertNotEquals(baseExpression, fullExpression);
    }

    public void testBoolean() {
        String leftText = "process_name == 'net.exe'";
        String rightText = "command_line == '* localgroup*'";

        Expression lhs = parser.createExpression(leftText);
        Expression rhs = parser.createExpression(rightText);

        Expression booleanAnd = parser.createExpression(leftText + " and " + rightText);
        assertEquals(new And(null, lhs, rhs), booleanAnd);

        Expression booleanOr = parser.createExpression(leftText + " or " + rightText);
        assertEquals(new Or(null, lhs, rhs), booleanOr);
    }

    public void testWildcard() {
        assertEquals(
            parser.createExpression("command_line == '* localgroup*'"),
            parser.createExpression("wildcard(command_line, '* localgroup*')")
        );

        assertEquals(
            parser.createExpression("command_line != '* localgroup*'"),
            parser.createExpression("not wildcard(command_line, '* localgroup*')")
        );
    }

    public void testInSet() {
        assertEquals(
            parser.createExpression("name in ('net.exe')"),
            parser.createExpression("name == 'net.exe'")
        );

        assertEquals(
            parser.createExpression("name in ('net.exe', 'whoami.exe', 'hostname.exe')"),
            parser.createExpression("name == 'net.exe' or name == 'whoami.exe' or name == 'hostname.exe'")
        );

        assertEquals(
            parser.createExpression("name not in ('net.exe', 'whoami.exe', 'hostname.exe')"),
            parser.createExpression("not (name == 'net.exe' or name == 'whoami.exe' or name == 'hostname.exe')")
        );
    }
}
