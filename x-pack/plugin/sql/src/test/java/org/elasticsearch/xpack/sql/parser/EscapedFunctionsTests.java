/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.parser;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.Literal;
import org.elasticsearch.xpack.sql.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.sql.expression.function.Function;
import org.elasticsearch.xpack.sql.expression.function.UnresolvedFunction;
import org.elasticsearch.xpack.sql.expression.predicate.regex.Like;
import org.elasticsearch.xpack.sql.expression.predicate.regex.LikePattern;
import org.elasticsearch.xpack.sql.plan.logical.Limit;
import org.elasticsearch.xpack.sql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.sql.plan.logical.With;
import org.elasticsearch.xpack.sql.proto.SqlTypedParamValue;
import org.elasticsearch.xpack.sql.type.DataType;
import org.junit.Assert;

import java.util.List;
import java.util.Locale;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class EscapedFunctionsTests extends ESTestCase {

    private final SqlParser parser = new SqlParser();

    private Literal dateLiteral(String date) {
        Expression exp = parser.createExpression(format(Locale.ROOT, "{d '%s'}", date));
        assertThat(exp, instanceOf(Expression.class));
        return (Literal) exp;
    }

    private Literal timeLiteral(String date) {
        Expression exp = parser.createExpression(format(Locale.ROOT, "{t '%s'}", date));
        assertThat(exp, instanceOf(Expression.class));
        return (Literal) exp;
    }

    private Literal timestampLiteral(String date) {
        Expression exp = parser.createExpression(format(Locale.ROOT, "{ts '%s'}", date));
        assertThat(exp, instanceOf(Expression.class));
        return (Literal) exp;
    }

    private Literal guidLiteral(String date) {
        Expression exp = parser.createExpression(format(Locale.ROOT, "{guid '%s'}", date));
        assertThat(exp, instanceOf(Expression.class));
        return (Literal) exp;
    }

    private Limit limit(int limit) {
        LogicalPlan plan = parser.createStatement(format(Locale.ROOT, "SELECT * FROM emp {limit %d}", limit));
        assertThat(plan, instanceOf(With.class));
        With with = (With) plan;
        Limit limitPlan = (Limit) (with.child());
        assertThat(limitPlan.limit(), instanceOf(Literal.class));
        return limitPlan;
    }

    private LikePattern likeEscape(String like, String character) {
        Expression exp = parser.createExpression(format(Locale.ROOT, "exp LIKE '%s' {escape '%s'}", like, character));
        assertThat(exp, instanceOf(Like.class));
        return ((Like) exp).pattern();
    }

    private Function function(String name) {
        Expression exp = parser.createExpression(format(Locale.ROOT, "{fn %s}", name));
        assertThat(exp, instanceOf(Function.class));
        return (Function) exp;
    }

    public void testFunctionNoArg() {
        Function f = function("SCORE()");
        assertEquals("SCORE", f.functionName());
    }

    public void testFunctionOneArg() {
        Function f = function("ABS(foo)");
        assertEquals("ABS", f.functionName());
        assertEquals(1, f.arguments().size());
        Expression arg = f.arguments().get(0);
        assertThat(arg, instanceOf(UnresolvedAttribute.class));
        UnresolvedAttribute ua = (UnresolvedAttribute) arg;
        assertThat(ua.name(), is("foo"));
    }

    public void testFunctionOneArgFunction() {
        Function f = function("ABS({fn SCORE()})");
        assertEquals("ABS", f.functionName());
        assertEquals(1, f.arguments().size());
        Expression arg = f.arguments().get(0);
        assertThat(arg, instanceOf(UnresolvedFunction.class));
        UnresolvedFunction uf = (UnresolvedFunction) arg;
        assertThat(uf.name(), is("SCORE"));
    }

    public void testFunctionFloorWithExtract() {
        Function f = function("CAST({fn FLOOR({fn EXTRACT(YEAR FROM \"foo\")})} AS int)");
        assertEquals("CAST", f.functionName());
        assertEquals(1, f.arguments().size());
        Expression arg = f.arguments().get(0);
        assertThat(arg, instanceOf(UnresolvedFunction.class));
        f = (Function) arg;
        assertEquals("FLOOR", f.functionName());
        assertEquals(1, f.arguments().size());
        arg = f.arguments().get(0);
        assertThat(arg, instanceOf(UnresolvedFunction.class));
        UnresolvedFunction uf = (UnresolvedFunction) arg;
        assertThat(uf.name(), is("YEAR"));
    }

    public void testFunctionWithFunctionWithArg() {
        Function f = function("POWER(foo, {fn POWER({fn SCORE()}, {fN SCORE()})})");
        assertEquals("POWER", f.functionName());
        assertEquals(2, f.arguments().size());
        Expression arg = f.arguments().get(1);
        assertThat(arg, instanceOf(UnresolvedFunction.class));
        UnresolvedFunction uf = (UnresolvedFunction) arg;
        assertThat(uf.name(), is("POWER"));
        assertEquals(2, uf.arguments().size());

        List<Expression> args = uf.arguments();
        arg = args.get(0);
        assertThat(arg, instanceOf(UnresolvedFunction.class));
        uf = (UnresolvedFunction) arg;
        assertThat(uf.name(), is("SCORE"));

        arg = args.get(1);
        assertThat(arg, instanceOf(UnresolvedFunction.class));
        uf = (UnresolvedFunction) arg;
        assertThat(uf.name(), is("SCORE"));
    }

    public void testFunctionWithFunctionWithArgAndParams() {
        Function f = (Function) parser.createExpression("POWER(?, {fn POWER({fn ABS(?)}, {fN ABS(?)})})",
                asList(new SqlTypedParamValue(DataType.LONG.esType, 1),
                       new SqlTypedParamValue(DataType.LONG.esType, 1),
                       new SqlTypedParamValue(DataType.LONG.esType, 1)));

        assertEquals("POWER", f.functionName());
        assertEquals(2, f.arguments().size());
        Expression arg = f.arguments().get(1);
        assertThat(arg, instanceOf(UnresolvedFunction.class));
        UnresolvedFunction uf = (UnresolvedFunction) arg;
        assertThat(uf.name(), is("POWER"));
        assertEquals(2, uf.arguments().size());

        List<Expression> args = uf.arguments();
        arg = args.get(0);
        assertThat(arg, instanceOf(UnresolvedFunction.class));
        uf = (UnresolvedFunction) arg;
        assertThat(uf.name(), is("ABS"));

        arg = args.get(1);
        assertThat(arg, instanceOf(UnresolvedFunction.class));
        uf = (UnresolvedFunction) arg;
        assertThat(uf.name(), is("ABS"));
    }

    public void testDateLiteral() {
        Literal l = dateLiteral("2012-01-01");
        assertThat(l.dataType(), is(DataType.DATE));
    }

    public void testDateLiteralValidation() {
        ParsingException ex = expectThrows(ParsingException.class, () -> dateLiteral("2012-13-01"));
        assertEquals("line 1:2: Invalid date received; Cannot parse \"2012-13-01\": Value 13 for monthOfYear must be in the range [1,12]",
                ex.getMessage());
    }

    public void testTimeLiteralUnsupported() {
        SqlIllegalArgumentException ex = expectThrows(SqlIllegalArgumentException.class, () -> timeLiteral("10:10:10"));
        assertThat(ex.getMessage(), is("Time (only) literals are not supported; a date component is required as well"));
    }

    public void testTimeLiteralValidation() {
        ParsingException ex = expectThrows(ParsingException.class, () -> timeLiteral("10:10:65"));
        assertEquals("line 1:2: Invalid time received; Cannot parse \"10:10:65\": Value 65 for secondOfMinute must be in the range [0,59]",
                ex.getMessage());
    }

    public void testTimestampLiteral() {
        Literal l = timestampLiteral("2012-01-01 10:01:02.3456");
        assertThat(l.dataType(), is(DataType.DATE));
    }

    public void testTimestampLiteralValidation() {
        ParsingException ex = expectThrows(ParsingException.class, () -> timestampLiteral("2012-01-01T10:01:02.3456"));
        assertEquals(
                "line 1:2: Invalid timestamp received; Invalid format: \"2012-01-01T10:01:02.3456\" is malformed at \"T10:01:02.3456\"",
                ex.getMessage());
    }

    public void testGUID() {
        Literal l = guidLiteral("12345678-90ab-cdef-0123-456789abcdef");
        assertThat(l.dataType(), is(DataType.KEYWORD));

        l = guidLiteral("12345678-90AB-cdef-0123-456789ABCdef");
        assertThat(l.dataType(), is(DataType.KEYWORD));
    }

    public void testGUIDValidationHexa() {
        ParsingException ex = expectThrows(ParsingException.class, () -> guidLiteral("12345678-90ab-cdef-0123-456789abcdeH"));
        assertEquals("line 1:8: Invalid GUID, expected hexadecimal at offset[35], found [H]", ex.getMessage());
    }

    public void testGUIDValidationGroups() {
        ParsingException ex = expectThrows(ParsingException.class, () -> guidLiteral("12345678A90ab-cdef-0123-456789abcdeH"));
        assertEquals("line 1:8: Invalid GUID, expected group separator at offset [8], found [A]", ex.getMessage());
    }

    public void testGUIDValidationLength() {
        ParsingException ex = expectThrows(ParsingException.class, () -> guidLiteral("12345678A90"));
        assertEquals("line 1:8: Invalid GUID, too short", ex.getMessage());
    }


    public void testLimit() {
        Limit limit = limit(10);
        Literal l = (Literal) limit.limit();
        Assert.assertThat(l.value(), is(10));
    }

    public void testLikeEscape() {
        LikePattern pattern = likeEscape("|%tring", "|");
        assertThat(pattern.escape(), is('|'));
    }
}