/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.plan.logical.Drop;
import org.elasticsearch.xpack.esql.plan.logical.Rename;
import org.elasticsearch.xpack.ql.expression.Alias;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.ql.expression.UnresolvedStar;
import org.elasticsearch.xpack.ql.expression.function.UnresolvedFunction;
import org.elasticsearch.xpack.ql.expression.predicate.logical.And;
import org.elasticsearch.xpack.ql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.ql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.Div;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.Mul;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.Neg;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.Sub;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.ql.plan.logical.Filter;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.Project;
import org.elasticsearch.xpack.ql.type.DataType;

import java.time.Duration;
import java.time.Period;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypes.DATE_PERIOD;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypes.TIME_DURATION;
import static org.elasticsearch.xpack.ql.expression.function.FunctionResolutionStrategy.DEFAULT;
import static org.elasticsearch.xpack.ql.tree.Source.EMPTY;
import static org.elasticsearch.xpack.ql.type.DataTypes.DOUBLE;
import static org.elasticsearch.xpack.ql.type.DataTypes.INTEGER;
import static org.elasticsearch.xpack.ql.type.DataTypes.KEYWORD;
import static org.elasticsearch.xpack.ql.type.DataTypes.LONG;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class ExpressionTests extends ESTestCase {
    private final EsqlParser parser = new EsqlParser();

    public void testBooleanLiterals() {
        assertEquals(Literal.TRUE, whereExpression("true"));
        assertEquals(Literal.FALSE, whereExpression("false"));
        assertEquals(Literal.NULL, whereExpression("null"));
    }

    public void testNumberLiterals() {
        assertEquals(l(123, INTEGER), whereExpression("123"));
        assertEquals(l(123, INTEGER), whereExpression("+123"));
        assertEquals(new Neg(null, l(123, INTEGER)), whereExpression("-123"));
        assertEquals(l(123.123, DOUBLE), whereExpression("123.123"));
        assertEquals(l(123.123, DOUBLE), whereExpression("+123.123"));
        assertEquals(new Neg(null, l(123.123, DOUBLE)), whereExpression("-123.123"));
        assertEquals(l(0.123, DOUBLE), whereExpression(".123"));
        assertEquals(l(0.123, DOUBLE), whereExpression("0.123"));
        assertEquals(l(0.123, DOUBLE), whereExpression("+0.123"));
        assertEquals(new Neg(null, l(0.123, DOUBLE)), whereExpression("-0.123"));
        assertEquals(l(12345678901L, LONG), whereExpression("12345678901"));
        assertEquals(l(12345678901L, LONG), whereExpression("+12345678901"));
        assertEquals(new Neg(null, l(12345678901L, LONG)), whereExpression("-12345678901"));
        assertEquals(l(123e12, DOUBLE), whereExpression("123e12"));
        assertEquals(l(123e-12, DOUBLE), whereExpression("123e-12"));
        assertEquals(l(123E12, DOUBLE), whereExpression("123E12"));
        assertEquals(l(123E-12, DOUBLE), whereExpression("123E-12"));
    }

    public void testMinusSign() {
        assertEquals(new Neg(null, l(123, INTEGER)), whereExpression("+(-123)"));
        assertEquals(new Neg(null, l(123, INTEGER)), whereExpression("+(+(-123))"));
        // we could do better here. ES SQL is smarter and accounts for the number of minuses
        assertEquals(new Neg(null, new Neg(null, l(123, INTEGER))), whereExpression("-(-123)"));
    }

    public void testStringLiterals() {
        assertEquals(l("abc", KEYWORD), whereExpression("\"abc\""));
        assertEquals(l("123.123", KEYWORD), whereExpression("\"123.123\""));

        assertEquals(l("hello\"world", KEYWORD), whereExpression("\"hello\\\"world\""));
        assertEquals(l("hello'world", KEYWORD), whereExpression("\"hello'world\""));
        assertEquals(l("\"hello\"world\"", KEYWORD), whereExpression("\"\\\"hello\\\"world\\\"\""));
        assertEquals(l("\"hello\nworld\"", KEYWORD), whereExpression("\"\\\"hello\\nworld\\\"\""));
        assertEquals(l("hello\nworld", KEYWORD), whereExpression("\"hello\\nworld\""));
        assertEquals(l("hello\\world", KEYWORD), whereExpression("\"hello\\\\world\""));
        assertEquals(l("hello\rworld", KEYWORD), whereExpression("\"hello\\rworld\""));
        assertEquals(l("hello\tworld", KEYWORD), whereExpression("\"hello\\tworld\""));
        assertEquals(l("C:\\Program Files\\Elastic", KEYWORD), whereExpression("\"C:\\\\Program Files\\\\Elastic\""));

        assertEquals(l("C:\\Program Files\\Elastic", KEYWORD), whereExpression("\"\"\"C:\\Program Files\\Elastic\"\"\""));
        assertEquals(l("\"\"hello world\"\"", KEYWORD), whereExpression("\"\"\"\"\"hello world\"\"\"\"\""));
        assertEquals(l("hello \"\"\" world", KEYWORD), whereExpression("\"hello \\\"\\\"\\\" world\""));
        assertEquals(l("hello\\nworld", KEYWORD), whereExpression("\"\"\"hello\\nworld\"\"\""));
        assertEquals(l("hello\\tworld", KEYWORD), whereExpression("\"\"\"hello\\tworld\"\"\""));
        assertEquals(l("hello world\\", KEYWORD), whereExpression("\"\"\"hello world\\\"\"\""));
        assertEquals(l("hello            world\\", KEYWORD), whereExpression("\"\"\"hello            world\\\"\"\""));
        assertEquals(l("\t \n \r \" \\ ", KEYWORD), whereExpression("\"\\t \\n \\r \\\" \\\\ \""));
    }

    public void testStringLiteralsExceptions() {
        assertParsingException(() -> whereExpression("\"\"\"\"\"\"foo\"\""), "line 1:22: mismatched input 'foo' expecting {<EOF>,");
        assertParsingException(
            () -> whereExpression("\"foo\" == \"\"\"\"\"\"bar\"\"\""),
            "line 1:31: mismatched input 'bar' expecting {<EOF>,"
        );
        assertParsingException(
            () -> whereExpression("\"\"\"\"\"\\\"foo\"\"\"\"\"\" != \"\"\"bar\"\"\""),
            "line 1:31: mismatched input '\" != \"' expecting {<EOF>,"
        );
        assertParsingException(
            () -> whereExpression("\"\"\"\"\"\\\"foo\"\"\\\"\"\"\" == \"\"\"\"\"\\\"bar\\\"\\\"\"\"\"\"\""),
            "line 1:55: token recognition error at: '\"'"
        );
        assertParsingException(
            () -> whereExpression("\"\"\"\"\"\" foo \"\"\"\" == abc"),
            "line 1:23: mismatched input 'foo' expecting {<EOF>,"
        );
    }

    public void testBooleanLiteralsCondition() {
        Expression expression = whereExpression("true and false");
        assertThat(expression, instanceOf(And.class));
        And and = (And) expression;
        assertThat(and.left(), equalTo(Literal.TRUE));
        assertThat(and.right(), equalTo(Literal.FALSE));
    }

    public void testArithmeticOperationCondition() {
        Expression expression = whereExpression("-a-b*c == 123");
        assertThat(expression, instanceOf(Equals.class));
        Equals eq = (Equals) expression;
        assertThat(eq.right(), instanceOf(Literal.class));
        assertThat(((Literal) eq.right()).value(), equalTo(123));
        assertThat(eq.left(), instanceOf(Sub.class));
        Sub sub = (Sub) eq.left();
        assertThat(sub.left(), instanceOf(Neg.class));
        Neg subLeftNeg = (Neg) sub.left();
        assertThat(subLeftNeg.field(), instanceOf(UnresolvedAttribute.class));
        assertThat(((UnresolvedAttribute) subLeftNeg.field()).name(), equalTo("a"));
        Mul mul = (Mul) sub.right();
        assertThat(mul.left(), instanceOf(UnresolvedAttribute.class));
        assertThat(mul.right(), instanceOf(UnresolvedAttribute.class));
        assertThat(((UnresolvedAttribute) mul.left()).name(), equalTo("b"));
        assertThat(((UnresolvedAttribute) mul.right()).name(), equalTo("c"));
    }

    public void testConjunctionDisjunctionCondition() {
        Expression expression = whereExpression("not aaa and b or c");
        assertThat(expression, instanceOf(Or.class));
        Or or = (Or) expression;
        assertThat(or.right(), instanceOf(UnresolvedAttribute.class));
        assertThat(((UnresolvedAttribute) or.right()).name(), equalTo("c"));
        assertThat(or.left(), instanceOf(And.class));
        And and = (And) or.left();
        assertThat(and.right(), instanceOf(UnresolvedAttribute.class));
        assertThat(((UnresolvedAttribute) and.right()).name(), equalTo("b"));
        assertThat(and.left(), instanceOf(Not.class));
        Not not = (Not) and.left();
        assertThat(not.children().size(), equalTo(1));
        assertThat(not.children().get(0), instanceOf(UnresolvedAttribute.class));
        assertThat(((UnresolvedAttribute) not.children().get(0)).name(), equalTo("aaa"));
    }

    public void testParenthesizedExpression() {
        Expression expression = whereExpression("((a and ((b and c))) or (((x or y))))");
        assertThat(expression, instanceOf(Or.class));
        Or or = (Or) expression;

        assertThat(or.right(), instanceOf(Or.class));
        Or orRight = (Or) or.right();
        assertThat(orRight.right(), instanceOf(UnresolvedAttribute.class));
        assertThat(((UnresolvedAttribute) orRight.right()).name(), equalTo("y"));
        assertThat(orRight.left(), instanceOf(UnresolvedAttribute.class));
        assertThat(orRight.left(), instanceOf(UnresolvedAttribute.class));
        assertThat(((UnresolvedAttribute) orRight.left()).name(), equalTo("x"));

        assertThat(or.left(), instanceOf(And.class));
        And and = (And) or.left();
        assertThat(and.right(), instanceOf(And.class));
        And andRight = (And) and.right();
        assertThat(andRight.right(), instanceOf(UnresolvedAttribute.class));
        assertThat(((UnresolvedAttribute) andRight.right()).name(), equalTo("c"));
        assertThat(andRight.left(), instanceOf(UnresolvedAttribute.class));
        assertThat(((UnresolvedAttribute) andRight.left()).name(), equalTo("b"));

        assertThat(and.left(), instanceOf(UnresolvedAttribute.class));
        assertThat(((UnresolvedAttribute) and.left()).name(), equalTo("a"));
    }

    public void testCommandNamesAsIdentifiers() {
        Expression expr = whereExpression("from and where");
        assertThat(expr, instanceOf(And.class));
        And and = (And) expr;

        assertThat(and.left(), instanceOf(UnresolvedAttribute.class));
        assertThat(((UnresolvedAttribute) and.left()).name(), equalTo("from"));

        assertThat(and.right(), instanceOf(UnresolvedAttribute.class));
        assertThat(((UnresolvedAttribute) and.right()).name(), equalTo("where"));
    }

    public void testIdentifiersCaseSensitive() {
        Expression expr = whereExpression("hElLo");

        assertThat(expr, instanceOf(UnresolvedAttribute.class));
        assertThat(((UnresolvedAttribute) expr).name(), equalTo("hElLo"));
    }

    /*
     * a > 1 and b > 1 + 2 => (a > 1) and (b > (1 + 2))
     */
    public void testOperatorsPrecedenceWithConjunction() {
        Expression expression = whereExpression("a > 1 and b > 1 + 2");
        assertThat(expression, instanceOf(And.class));
        And and = (And) expression;

        assertThat(and.left(), instanceOf(GreaterThan.class));
        GreaterThan gt = (GreaterThan) and.left();
        assertThat(gt.left(), instanceOf(UnresolvedAttribute.class));
        assertThat(((UnresolvedAttribute) gt.left()).name(), equalTo("a"));
        assertThat(gt.right(), instanceOf(Literal.class));
        assertThat(((Literal) gt.right()).value(), equalTo(1));

        assertThat(and.right(), instanceOf(GreaterThan.class));
        gt = (GreaterThan) and.right();
        assertThat(gt.left(), instanceOf(UnresolvedAttribute.class));
        assertThat(((UnresolvedAttribute) gt.left()).name(), equalTo("b"));
        assertThat(gt.right(), instanceOf(Add.class));
        Add add = (Add) gt.right();
        assertThat(((Literal) add.right()).value(), equalTo(2));
        assertThat(((Literal) add.left()).value(), equalTo(1));
    }

    /*
     * a <= 1 or b >= 5 / 2 and c != 5 => (a <= 1) or (b >= (5 / 2) and not(c == 5))
     */
    public void testOperatorsPrecedenceWithDisjunction() {
        Expression expression = whereExpression("a <= 1 or b >= 5 / 2 and c != 5");
        assertThat(expression, instanceOf(Or.class));
        Or or = (Or) expression;

        assertThat(or.left(), instanceOf(LessThanOrEqual.class));
        LessThanOrEqual lte = (LessThanOrEqual) or.left();
        assertThat(lte.left(), instanceOf(UnresolvedAttribute.class));
        assertThat(((UnresolvedAttribute) lte.left()).name(), equalTo("a"));
        assertThat(lte.right(), instanceOf(Literal.class));
        assertThat(((Literal) lte.right()).value(), equalTo(1));

        assertThat(or.right(), instanceOf(And.class));
        And and = (And) or.right();
        assertThat(and.left(), instanceOf(GreaterThanOrEqual.class));
        GreaterThanOrEqual gte = (GreaterThanOrEqual) and.left();
        assertThat(gte.left(), instanceOf(UnresolvedAttribute.class));
        assertThat(((UnresolvedAttribute) gte.left()).name(), equalTo("b"));
        assertThat(gte.right(), instanceOf(Div.class));
        Div div = (Div) gte.right();
        assertThat(div.right(), instanceOf(Literal.class));
        assertThat(((Literal) div.right()).value(), equalTo(2));
        assertThat(div.left(), instanceOf(Literal.class));
        assertThat(((Literal) div.left()).value(), equalTo(5));

        assertThat(and.right(), instanceOf(Not.class));
        assertThat(((Not) and.right()).field(), instanceOf(Equals.class));
        Equals e = (Equals) ((Not) and.right()).field();
        assertThat(e.left(), instanceOf(UnresolvedAttribute.class));
        assertThat(((UnresolvedAttribute) e.left()).name(), equalTo("c"));
        assertThat(e.right(), instanceOf(Literal.class));
        assertThat(((Literal) e.right()).value(), equalTo(5));
    }

    /*
     * not a == 1 or not b >= 5 and c == 5 => (not (a == 1)) or ((not (b >= 5)) and c == 5)
     */
    public void testOperatorsPrecedenceWithNegation() {
        Expression expression = whereExpression("not a == 1 or not b >= 5 and c == 5");
        assertThat(expression, instanceOf(Or.class));
        Or or = (Or) expression;

        assertThat(or.left(), instanceOf(Not.class));
        assertThat(((Not) or.left()).field(), instanceOf(Equals.class));
        Equals e = (Equals) ((Not) or.left()).field();
        assertThat(e.left(), instanceOf(UnresolvedAttribute.class));
        assertThat(((UnresolvedAttribute) e.left()).name(), equalTo("a"));
        assertThat(e.right(), instanceOf(Literal.class));
        assertThat(((Literal) e.right()).value(), equalTo(1));

        assertThat(or.right(), instanceOf(And.class));
        And and = (And) or.right();
        assertThat(and.left(), instanceOf(Not.class));
        assertThat(((Not) and.left()).field(), instanceOf(GreaterThanOrEqual.class));
        GreaterThanOrEqual gte = (GreaterThanOrEqual) ((Not) and.left()).field();
        assertThat(gte.right(), instanceOf(Literal.class));
        assertThat(((Literal) gte.right()).value(), equalTo(5));

        assertThat(and.right(), instanceOf(Equals.class));
        e = (Equals) and.right();
        assertThat(e.left(), instanceOf(UnresolvedAttribute.class));
        assertThat(((UnresolvedAttribute) e.left()).name(), equalTo("c"));
        assertThat(e.right(), instanceOf(Literal.class));
        assertThat(((Literal) e.right()).value(), equalTo(5));
    }

    public void testOperatorsPrecedenceExpressionsEquality() {
        assertThat(whereExpression("a-1>2 or b>=5 and c-1>=5"), equalTo(whereExpression("((a-1)>2 or (b>=5 and (c-1)>=5))")));
        assertThat(
            whereExpression("a*5==25 and b>5 and c%4>=1 or true or false"),
            equalTo(whereExpression("(((((a*5)==25) and (b>5) and ((c%4)>=1)) or true) or false)"))
        );
        assertThat(
            whereExpression("a*4-b*5<100 and b/2+c*6>=50 or c%5+x>=5"),
            equalTo(whereExpression("((((a*4)-(b*5))<100) and (((b/2)+(c*6))>=50)) or (((c%5)+x)>=5)"))
        );
        assertThat(
            whereExpression("true and false or true and c/12+x*5-y%2>=50"),
            equalTo(whereExpression("((true and false) or (true and (((c/12)+(x*5)-(y%2))>=50)))"))
        );
        assertThat(
            whereExpression("10 days > 5 hours and 1/5 minutes > 8 seconds * 3 and -1 minutes > foo"),
            equalTo(whereExpression("((10 days) > (5 hours)) and ((1/(5 minutes) > ((8 seconds) * 3))) and (-(1 minute) > foo)"))
        );
        assertThat(
            whereExpression("10 DAYS > 5 HOURS and 1/5 MINUTES > 8 SECONDS * 3 and -1 MINUTES > foo"),
            equalTo(whereExpression("((10 days) > (5 hours)) and ((1/(5 minutes) > ((8 seconds) * 3))) and (-(1 minute) > foo)"))
        );
    }

    public void testFunctionExpressions() {
        assertEquals(new UnresolvedFunction(EMPTY, "fn", DEFAULT, new ArrayList<>()), whereExpression("fn()"));
        assertEquals(
            new UnresolvedFunction(
                EMPTY,
                "invoke",
                DEFAULT,
                new ArrayList<>(
                    List.of(
                        new UnresolvedAttribute(EMPTY, "a"),
                        new Add(EMPTY, new UnresolvedAttribute(EMPTY, "b"), new UnresolvedAttribute(EMPTY, "c"))
                    )
                )
            ),
            whereExpression("invoke(a, b + c)")
        );
        assertEquals(whereExpression("(invoke((a + b)))"), whereExpression("invoke(a+b)"));
        assertEquals(whereExpression("((fn()) + fn(fn()))"), whereExpression("fn() + fn(fn())"));
    }

    public void testUnquotedIdentifiers() {
        for (String identifier : List.of("a", "_a", "a_b", "a9", "abc123", "a_____9", "__a_b", "@a", "_1", "@2")) {
            assertEquals(new UnresolvedAttribute(EMPTY, identifier), whereExpression(identifier));
        }
    }

    public void testDurationLiterals() {
        int value = randomInt(Integer.MAX_VALUE);

        assertEquals(l(Duration.ZERO, TIME_DURATION), whereExpression("0 millisecond"));
        assertEquals(l(Duration.ofMillis(value), TIME_DURATION), whereExpression(value + "millisecond"));
        assertEquals(l(Duration.ofMillis(value), TIME_DURATION), whereExpression(value + " milliseconds"));

        assertEquals(l(Duration.ZERO, TIME_DURATION), whereExpression("0 second"));
        assertEquals(l(Duration.ofSeconds(value), TIME_DURATION), whereExpression(value + "second"));
        assertEquals(l(Duration.ofSeconds(value), TIME_DURATION), whereExpression(value + " seconds"));

        assertEquals(l(Duration.ZERO, TIME_DURATION), whereExpression("0 minute"));
        assertEquals(l(Duration.ofMinutes(value), TIME_DURATION), whereExpression(value + "minute"));
        assertEquals(l(Duration.ofMinutes(value), TIME_DURATION), whereExpression(value + " minutes"));

        assertEquals(l(Duration.ZERO, TIME_DURATION), whereExpression("0 hour"));
        assertEquals(l(Duration.ofHours(value), TIME_DURATION), whereExpression(value + "hour"));
        assertEquals(l(Duration.ofHours(value), TIME_DURATION), whereExpression(value + " hours"));

        assertEquals(new Neg(EMPTY, l(Duration.ofHours(value), TIME_DURATION)), whereExpression("-" + value + " hours"));
    }

    public void testDatePeriodLiterals() {
        int value = randomInt(Integer.MAX_VALUE);

        assertEquals(l(Period.ZERO, DATE_PERIOD), whereExpression("0 day"));
        assertEquals(l(Period.ofDays(value), DATE_PERIOD), whereExpression(value + "day"));
        assertEquals(l(Period.ofDays(value), DATE_PERIOD), whereExpression(value + " days"));

        assertEquals(l(Period.ZERO, DATE_PERIOD), whereExpression("0week"));
        assertEquals(l(Period.ofDays(value * 7), DATE_PERIOD), whereExpression(value + "week"));
        assertEquals(l(Period.ofDays(value * 7), DATE_PERIOD), whereExpression(value + " weeks"));

        assertEquals(l(Period.ZERO, DATE_PERIOD), whereExpression("0 month"));
        assertEquals(l(Period.ofMonths(value), DATE_PERIOD), whereExpression(value + "month"));
        assertEquals(l(Period.ofMonths(value), DATE_PERIOD), whereExpression(value + " months"));

        assertEquals(l(Period.ZERO, DATE_PERIOD), whereExpression("0year"));
        assertEquals(l(Period.ofYears(value), DATE_PERIOD), whereExpression(value + "year"));
        assertEquals(l(Period.ofYears(value), DATE_PERIOD), whereExpression(value + " years"));

        assertEquals(new Neg(EMPTY, l(Period.ofYears(value), DATE_PERIOD)), whereExpression("-" + value + " years"));
    }

    public void testUnknownNumericQualifier() {
        assertParsingException(() -> whereExpression("1 decade"), "Unexpected numeric qualifier 'decade'");
    }

    public void testQualifiedDecimalLiteral() {
        assertParsingException(() -> whereExpression("1.1 hours"), "extraneous input 'hours' expecting <EOF>");
    }

    public void testWildcardProjectKeepPatterns() {
        String[] exp = new String[] {
            "a*",
            "*a",
            "a.*",
            "a.a.*.*.a",
            "*.a.a.a.*",
            "*abc.*",
            "a*b*c",
            "*a*",
            "*a*b",
            "a*b*",
            "*a*b*c*",
            "a*b*c*",
            "*a*b*c",
            "a*b*c*a.b*",
            "a*b*c*a.b.*",
            "*a.b.c*b*c*a.b.*" };
        List<?> projections;
        Project p;
        for (String e : exp) {
            p = projectExpression(e);
            projections = p.projections();
            assertThat(projections.size(), equalTo(1));
            assertThat("Projection [" + e + "] has an unexpected type", projections.get(0), instanceOf(UnresolvedAttribute.class));
            UnresolvedAttribute ua = (UnresolvedAttribute) projections.get(0);
            assertThat(ua.name(), equalTo(e));
            assertThat(ua.unresolvedMessage(), equalTo("Unknown column [" + e + "]"));
        }
    }

    public void testWildcardProjectKeep() {
        Project p = projectExpression("*");
        List<?> projections = p.projections();
        assertThat(projections.size(), equalTo(1));
        assertThat(projections.get(0), instanceOf(UnresolvedStar.class));
        UnresolvedStar us = (UnresolvedStar) projections.get(0);
        assertThat(us.qualifier(), equalTo(null));
        assertThat(us.unresolvedMessage(), equalTo("Cannot determine columns for [*]"));
    }

    public void testWildcardProjectAwayPatterns() {
        String[] exp = new String[] {
            "a*",
            "*a",
            "a.*",
            "a.a.*.*.a",
            "*.a.a.a.*",
            "*abc.*",
            "a*b*c",
            "*a*",
            "*a*b",
            "a*b*",
            "*a*b*c*",
            "a*b*c*",
            "*a*b*c",
            "a*b*c*a.b*",
            "a*b*c*a.b.*",
            "*a.b.c*b*c*a.b.*" };
        List<?> removals;
        for (String e : exp) {
            Drop d = dropExpression(e);
            removals = d.removals();
            assertThat(removals.size(), equalTo(1));
            assertThat("Projection [" + e + "] has an unexpected type", removals.get(0), instanceOf(UnresolvedAttribute.class));
            UnresolvedAttribute ursa = (UnresolvedAttribute) removals.get(0);
            assertThat(ursa.name(), equalTo(e));
            assertThat(ursa.unresolvedMessage(), equalTo("Unknown column [" + e + "]"));
        }
    }

    public void testForbidWildcardProjectAway() {
        assertParsingException(() -> dropExpression("foo, *"), "line 1:21: Removing all fields is not allowed [*]");
    }

    public void testForbidMultipleIncludeStar() {
        var errorMsg = "Cannot specify [*] more than once";
        assertParsingException(() -> projectExpression("a, *, *, b"), errorMsg);
        assertParsingException(() -> projectExpression("a, *, b, *, c"), errorMsg);
        assertParsingException(() -> projectExpression("a, b, *, c, d, *"), errorMsg);
    }

    public void testProjectKeepPatterns() {
        String[] exp = new String[] { "abc", "abc.xyz", "a.b.c.d.e" };
        List<?> projections;
        for (String e : exp) {
            Project p = projectExpression(e);
            projections = p.projections();
            assertThat(projections.size(), equalTo(1));
            assertThat(projections.get(0), instanceOf(UnresolvedAttribute.class));
            assertThat(((UnresolvedAttribute) projections.get(0)).name(), equalTo(e));
        }
    }

    public void testProjectAwayPatterns() {
        String[] exp = new String[] { "abc", "abc.xyz", "a.b.c.d.e" };
        for (String e : exp) {
            Drop d = dropExpression(e);
            List<?> removals = d.removals();
            assertThat(removals.size(), equalTo(1));
            assertThat(removals.get(0), instanceOf(UnresolvedAttribute.class));
            assertThat(((UnresolvedAttribute) removals.get(0)).name(), equalTo(e));
        }
    }

    public void testProjectRename() {
        String[] newName = new String[] { "a", "a.b", "a", "x.y" };
        String[] oldName = new String[] { "b", "a.c", "x.y", "a" };
        List<?> renamings;
        for (int i = 0; i < newName.length; i++) {
            Rename r = renameExpression(newName[i] + "=" + oldName[i]);
            renamings = r.renamings();
            assertThat(renamings.size(), equalTo(1));
            assertThat(renamings.get(0), instanceOf(Alias.class));
            Alias a = (Alias) renamings.get(0);
            assertThat(a.child(), instanceOf(UnresolvedAttribute.class));
            UnresolvedAttribute ua = (UnresolvedAttribute) a.child();
            assertThat(a.name(), equalTo(newName[i]));
            assertThat(ua.name(), equalTo(oldName[i]));
        }
    }

    public void testMultipleProjectPatterns() {
        LogicalPlan plan = parse("from a | rename x = y | project abc, xyz*, x, *");
        Project p = as(plan, Project.class);
        List<?> projections = p.projections();
        assertThat(projections.size(), equalTo(4));
        assertThat(projections.get(0), instanceOf(UnresolvedAttribute.class));
        assertThat(((UnresolvedAttribute) projections.get(0)).name(), equalTo("abc"));
        assertThat(projections.get(1), instanceOf(UnresolvedAttribute.class));
        assertThat(((UnresolvedAttribute) projections.get(1)).name(), equalTo("xyz*"));
        assertThat(projections.get(2), instanceOf(UnresolvedAttribute.class));
        assertThat(projections.get(3), instanceOf(UnresolvedStar.class));
    }

    public void testForbidWildcardProjectRename() {
        assertParsingException(
            () -> renameExpression("a*=b*"),
            "line 1:18: Using wildcards (*) in renaming projections is not allowed [a*=b*]"
        );
    }

    private Expression whereExpression(String e) {
        return ((Filter) parse("from a | where " + e)).condition();
    }

    private Drop dropExpression(String e) {
        return (Drop) parse("from a | drop " + e);
    }

    private Rename renameExpression(String e) {
        return (Rename) parse("from a | rename " + e);
    }

    private Project projectExpression(String e) {
        return (Project) parse("from a | project " + e);
    }

    private LogicalPlan parse(String s) {
        return parser.createStatement(s);
    }

    private Literal l(Object value, DataType type) {
        return new Literal(null, value, type);
    }

    private void assertParsingException(ThrowingRunnable expression, String expectedError) {
        ParsingException e = expectThrows(ParsingException.class, "Expected syntax error", expression);
        assertThat(e.getMessage(), containsString(expectedError));
    }
}
