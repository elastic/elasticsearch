/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.parser;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.ql.expression.function.Function;
import org.elasticsearch.xpack.ql.expression.function.UnresolvedFunction;
import org.elasticsearch.xpack.ql.expression.predicate.regex.Like;
import org.elasticsearch.xpack.ql.expression.predicate.regex.LikePattern;
import org.elasticsearch.xpack.ql.plan.logical.Limit;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.sql.plan.logical.With;
import org.elasticsearch.xpack.sql.proto.SqlTypedParamValue;
import org.junit.Assert;

import java.util.List;
import java.util.Locale;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.elasticsearch.xpack.ql.type.DataTypes.DATETIME;
import static org.elasticsearch.xpack.ql.type.DataTypes.KEYWORD;
import static org.elasticsearch.xpack.ql.type.DataTypes.LONG;
import static org.elasticsearch.xpack.sql.SqlTestUtils.randomWhitespaces;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.DATE;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.TIME;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.matchesPattern;

public class EscapedFunctionsTests extends ESTestCase {

    private final SqlParser parser = new SqlParser();

    private String buildExpression(String escape, String pattern, Object value) {
        return format(Locale.ROOT, "{" + randomWhitespaces() + escape + " " + randomWhitespaces() +
            pattern + randomWhitespaces() + "}", value);
    }

    private Literal dateLiteral(String date) {
        Expression exp = parser.createExpression(buildExpression("d", "'%s'", date));
        assertThat(exp, instanceOf(Expression.class));
        return (Literal) exp;
    }

    private Literal timeLiteral(String date) {
        Expression exp = parser.createExpression(buildExpression("t", "'%s'", date));
        assertThat(exp, instanceOf(Expression.class));
        return (Literal) exp;
    }

    private Literal timestampLiteral(String date) {
        Expression exp = parser.createExpression(buildExpression("ts", "'%s'", date));
        assertThat(exp, instanceOf(Expression.class));
        return (Literal) exp;
    }

    private Literal guidLiteral(String guid) {
        Expression exp = parser.createExpression(buildExpression("guid", "'%s'", guid));
        assertThat(exp, instanceOf(Expression.class));
        return (Literal) exp;
    }

    private Limit limit(int limit) {
        LogicalPlan plan = parser.createStatement("SELECT * FROM emp " + buildExpression("limit", "%d", limit));
        assertThat(plan, instanceOf(With.class));
        With with = (With) plan;
        Limit limitPlan = (Limit) (with.child());
        assertThat(limitPlan.limit(), instanceOf(Literal.class));
        return limitPlan;
    }

    private LikePattern likeEscape(String like, String character) {
        Expression exp = parser.createExpression(format(Locale.ROOT, "exp LIKE '%s' ", like) +
                buildExpression("escape", "'%s'", character));
        assertThat(exp, instanceOf(Like.class));
        return ((Like) exp).pattern();
    }

    private Function function(String name) {
        Expression exp = parser.createExpression(
            format(Locale.ROOT, "{" + randomWhitespaces() + "fn" + randomWhitespaces() + "%s" + randomWhitespaces() + "}", name));
        assertThat(exp, instanceOf(Function.class));
        return (Function) exp;
    }

    private void assertFunction(String name, String result) {
        String escapedName = name.replace("(", "\\(").replace(")", "\\)").replace("{", "\\{").replace("}", "\\}");
        assertThat(result, matchesPattern("\\{\\s*fn\\s*" + escapedName + "\\s*}"));
    }
    public void testFunctionNoArg() {
        Function f = function("SCORE()");
        assertFunction("SCORE()", f.sourceText());
    }

    public void testFunctionOneArg() {
        Function f = function("ABS(foo)");
        assertFunction("ABS(foo)", f.sourceText());
        assertEquals(1, f.arguments().size());
        Expression arg = f.arguments().get(0);
        assertThat(arg, instanceOf(UnresolvedAttribute.class));
        UnresolvedAttribute ua = (UnresolvedAttribute) arg;
        assertThat(ua.name(), is("foo"));
    }

    public void testFunctionOneArgFunction() {
        Function f = function("ABS({fn SCORE()})");
        assertFunction("ABS({fn SCORE()})", f.sourceText());
        assertEquals(1, f.arguments().size());
        Expression arg = f.arguments().get(0);
        assertThat(arg, instanceOf(UnresolvedFunction.class));
        UnresolvedFunction uf = (UnresolvedFunction) arg;
        assertThat(uf.sourceText(), is("{fn SCORE()}"));
    }

    public void testFunctionFloorWithExtract() {
        String e = "CAST({fn FLOOR({fn EXTRACT(YEAR FROM \"foo\")})} AS int)";
        Function f = function(e);
        assertEquals(e, f.sourceText());
        assertEquals(1, f.arguments().size());
        Expression arg = f.arguments().get(0);
        assertThat(arg, instanceOf(UnresolvedFunction.class));
        f = (Function) arg;
        assertEquals("{fn FLOOR({fn EXTRACT(YEAR FROM \"foo\")})}", f.sourceText());
        assertEquals(1, f.arguments().size());
        arg = f.arguments().get(0);
        assertThat(arg, instanceOf(UnresolvedFunction.class));
        UnresolvedFunction uf = (UnresolvedFunction) arg;
        assertThat(uf.sourceText(), is("EXTRACT(YEAR FROM \"foo\")"));
    }

    public void testFunctionWithFunctionWithArg() {
        Function f = function("POWER(foo, {fn POWER({fn SCORE()}, {fN SCORE()})})");
        assertFunction("POWER(foo, {fn POWER({fn SCORE()}, {fN SCORE()})})", f.sourceText());
        assertEquals(2, f.arguments().size());
        Expression arg = f.arguments().get(1);
        assertThat(arg, instanceOf(UnresolvedFunction.class));
        UnresolvedFunction uf = (UnresolvedFunction) arg;
        assertThat(uf.sourceText(), is("{fn POWER({fn SCORE()}, {fN SCORE()})}"));
        assertEquals(2, uf.arguments().size());

        List<Expression> args = uf.arguments();
        arg = args.get(0);
        assertThat(arg, instanceOf(UnresolvedFunction.class));
        uf = (UnresolvedFunction) arg;
        assertThat(uf.sourceText(), is("{fn SCORE()}"));

        arg = args.get(1);
        assertThat(arg, instanceOf(UnresolvedFunction.class));
        uf = (UnresolvedFunction) arg;
        assertThat(uf.sourceText(), is("{fN SCORE()}"));
    }

    public void testFunctionWithFunctionWithArgAndParams() {
        String e = "POWER(?, {fn POWER({fn ABS(?)}, {fN ABS(?)})})";
        Function f = (Function) parser.createExpression(e,
                asList(new SqlTypedParamValue(LONG.typeName(), 1), new SqlTypedParamValue(LONG.typeName(), 1),
                        new SqlTypedParamValue(LONG.typeName(), 1)));

        assertEquals(e, f.sourceText());
        assertEquals(2, f.arguments().size());
        Expression arg = f.arguments().get(1);
        assertThat(arg, instanceOf(UnresolvedFunction.class));
        UnresolvedFunction uf = (UnresolvedFunction) arg;
        assertEquals("{fn POWER({fn ABS(?)}, {fN ABS(?)})}", uf.sourceText());
        assertEquals(2, uf.arguments().size());

        List<Expression> args = uf.arguments();
        arg = args.get(0);
        assertThat(arg, instanceOf(UnresolvedFunction.class));
        uf = (UnresolvedFunction) arg;
        assertThat(uf.sourceText(), is("{fn ABS(?)}"));

        arg = args.get(1);
        assertThat(arg, instanceOf(UnresolvedFunction.class));
        uf = (UnresolvedFunction) arg;
        assertThat(uf.sourceText(), is("{fN ABS(?)}"));
    }

    public void testDateLiteral() {
        Literal l = dateLiteral("2012-01-01");
        assertThat(l.dataType(), is(DATE));
    }

    public void testDateLiteralValidation() {
        ParsingException ex = expectThrows(ParsingException.class, () -> dateLiteral("2012-13-01"));
        assertEquals("line 1:2: Invalid date received; Text '2012-13-01' could not be parsed: " +
                "Invalid value for MonthOfYear (valid values 1 - 12): 13",
                ex.getMessage());
    }

    public void testTimeLiteral() {
        Literal l = timeLiteral("12:23:56");
        assertThat(l.dataType(), is(TIME));
    }

    public void testTimeLiteralValidation() {
        ParsingException ex = expectThrows(ParsingException.class, () -> timeLiteral("10:10:65"));
        assertEquals("line 1:2: Invalid time received; Text '10:10:65' could not be parsed: " +
                "Invalid value for SecondOfMinute (valid values 0 - 59): 65",
                ex.getMessage());
    }

    public void testTimestampLiteral() {
        Literal l = timestampLiteral("2012-01-01 10:01:02.3456");
        assertThat(l.dataType(), is(DATETIME));
    }

    public void testTimestampLiteralValidation() {
        ParsingException ex = expectThrows(ParsingException.class, () -> timestampLiteral("2012-01-01T10:01:02.3456"));
        assertEquals(
                "line 1:2: Invalid timestamp received; Text '2012-01-01T10:01:02.3456' could not be parsed at index 10",
                ex.getMessage());
    }

    public void testGUID() {
        Literal l = guidLiteral("12345678-90ab-cdef-0123-456789abcdef");
        assertThat(l.dataType(), is(KEYWORD));

        l = guidLiteral("12345678-90AB-cdef-0123-456789ABCdef");
        assertThat(l.dataType(), is(KEYWORD));
    }

    public void testGUIDValidationHexa() {
        ParsingException ex = expectThrows(ParsingException.class, () -> guidLiteral("12345678-90ab-cdef-0123-456789abcdeH"));
        assertThat(ex.getMessage(), endsWith(": Invalid GUID, expected hexadecimal at offset[35], found [H]"));
    }

    public void testGUIDValidationGroups() {
        ParsingException ex = expectThrows(ParsingException.class, () -> guidLiteral("12345678A90ab-cdef-0123-456789abcdeH"));
        assertThat(ex.getMessage(), endsWith(": Invalid GUID, expected group separator at offset [8], found [A]"));
    }

    public void testGUIDValidationLength() {
        ParsingException ex = expectThrows(ParsingException.class, () -> guidLiteral("12345678A90"));
        assertThat(ex.getMessage(), endsWith(": Invalid GUID, too short"));
    }

    public void testCurrentTimestampAsEscapedExpression() {
        Expression expr = parser.createExpression("{fn CURRENT_TIMESTAMP(2)}");
        assertEquals(UnresolvedFunction.class, expr.getClass());
        UnresolvedFunction ur = (UnresolvedFunction) expr;
        assertEquals("{fn CURRENT_TIMESTAMP(2)}", ur.sourceText());
        assertEquals(1, ur.children().size());
    }

    public void testCurrentDateAsEscapedExpression() {
        Expression expr = parser.createExpression("{fn CURRENT_DATE()}");
        assertEquals(UnresolvedFunction.class, expr.getClass());
        UnresolvedFunction ur = (UnresolvedFunction) expr;
        assertEquals("{fn CURRENT_DATE()}", ur.sourceText());
        assertEquals(0, ur.children().size());
    }

    public void testCurrentTimeAsEscapedExpression() {
        Expression expr = parser.createExpression("{fn CURRENT_TIME(2)}");
        assertEquals(UnresolvedFunction.class, expr.getClass());
        UnresolvedFunction ur = (UnresolvedFunction) expr;
        assertEquals("{fn CURRENT_TIME(2)}", ur.sourceText());
        assertEquals(1, ur.children().size());
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
