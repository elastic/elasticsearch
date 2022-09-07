/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.UnresolvedAttribute;
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
import org.elasticsearch.xpack.ql.type.DataType;

import static org.elasticsearch.xpack.ql.type.DataTypes.DOUBLE;
import static org.elasticsearch.xpack.ql.type.DataTypes.INTEGER;
import static org.elasticsearch.xpack.ql.type.DataTypes.KEYWORD;
import static org.elasticsearch.xpack.ql.type.DataTypes.LONG;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;

public class ExpressionTests extends ESTestCase {
    private final EsqlParser parser = new EsqlParser();

    public void testBooleanLiterals() {
        assertEquals(Literal.TRUE, expression("true"));
        assertEquals(Literal.FALSE, expression("false"));
        assertEquals(Literal.NULL, expression("null"));
    }

    public void testNumberLiterals() {
        assertEquals(l(123, INTEGER), expression("123"));
        assertEquals(l(123, INTEGER), expression("+123"));
        assertEquals(new Neg(null, l(123, INTEGER)), expression("-123"));
        assertEquals(l(123.123, DOUBLE), expression("123.123"));
        assertEquals(l(123.123, DOUBLE), expression("+123.123"));
        assertEquals(new Neg(null, l(123.123, DOUBLE)), expression("-123.123"));
        assertEquals(l(0.123, DOUBLE), expression(".123"));
        assertEquals(l(0.123, DOUBLE), expression("0.123"));
        assertEquals(l(0.123, DOUBLE), expression("+0.123"));
        assertEquals(new Neg(null, l(0.123, DOUBLE)), expression("-0.123"));
        assertEquals(l(12345678901L, LONG), expression("12345678901"));
        assertEquals(l(12345678901L, LONG), expression("+12345678901"));
        assertEquals(new Neg(null, l(12345678901L, LONG)), expression("-12345678901"));
        assertEquals(l(123e12, DOUBLE), expression("123e12"));
        assertEquals(l(123e-12, DOUBLE), expression("123e-12"));
        assertEquals(l(123E12, DOUBLE), expression("123E12"));
        assertEquals(l(123E-12, DOUBLE), expression("123E-12"));
    }

    public void testMinusSign() {
        assertEquals(new Neg(null, l(123, INTEGER)), expression("+(-123)"));
        assertEquals(new Neg(null, l(123, INTEGER)), expression("+(+(-123))"));
        // we could do better here. ES SQL is smarter and accounts for the number of minuses
        assertEquals(new Neg(null, new Neg(null, l(123, INTEGER))), expression("-(-123)"));
    }

    public void testStringLiterals() {
        assertEquals(l("abc", KEYWORD), expression("\"abc\""));
        assertEquals(l("123.123", KEYWORD), expression("\"123.123\""));

        assertEquals(l("hello\"world", KEYWORD), expression("\"hello\\\"world\""));
        assertEquals(l("hello'world", KEYWORD), expression("\"hello'world\""));
        assertEquals(l("\"hello\"world\"", KEYWORD), expression("\"\\\"hello\\\"world\\\"\""));
        assertEquals(l("\"hello\nworld\"", KEYWORD), expression("\"\\\"hello\\nworld\\\"\""));
        assertEquals(l("hello\nworld", KEYWORD), expression("\"hello\\nworld\""));
        assertEquals(l("hello\\world", KEYWORD), expression("\"hello\\\\world\""));
        assertEquals(l("hello\rworld", KEYWORD), expression("\"hello\\rworld\""));
        assertEquals(l("hello\tworld", KEYWORD), expression("\"hello\\tworld\""));
        assertEquals(l("C:\\Program Files\\Elastic", KEYWORD), expression("\"C:\\\\Program Files\\\\Elastic\""));

        assertEquals(l("C:\\Program Files\\Elastic", KEYWORD), expression("\"\"\"C:\\Program Files\\Elastic\"\"\""));
        assertEquals(l("\"\"hello world\"\"", KEYWORD), expression("\"\"\"\"\"hello world\"\"\"\"\""));
        assertEquals(l("hello \"\"\" world", KEYWORD), expression("\"hello \\\"\\\"\\\" world\""));
        assertEquals(l("hello\\nworld", KEYWORD), expression("\"\"\"hello\\nworld\"\"\""));
        assertEquals(l("hello\\tworld", KEYWORD), expression("\"\"\"hello\\tworld\"\"\""));
        assertEquals(l("hello world\\", KEYWORD), expression("\"\"\"hello world\\\"\"\""));
        assertEquals(l("hello            world\\", KEYWORD), expression("\"\"\"hello            world\\\"\"\""));
        assertEquals(l("\t \n \r \" \\ ", KEYWORD), expression("\"\\t \\n \\r \\\" \\\\ \""));
    }

    public void testStringLiteralsExceptions() {
        assertParsingException(() -> expression("\"\"\"\"\"\"foo\"\""), "line 1:7: mismatched input 'foo' expecting {<EOF>,");
        assertParsingException(() -> expression("\"foo\" == \"\"\"\"\"\"bar\"\"\""), "line 1:16: mismatched input 'bar' expecting {<EOF>,");
        assertParsingException(
            () -> expression("\"\"\"\"\"\\\"foo\"\"\"\"\"\" != \"\"\"bar\"\"\""),
            "line 1:16: mismatched input '\" != \"' expecting {<EOF>,"
        );
        assertParsingException(
            () -> expression("\"\"\"\"\"\\\"foo\"\"\\\"\"\"\" == \"\"\"\"\"\\\"bar\\\"\\\"\"\"\"\"\""),
            "line 1:40: token recognition error at: '\"'"
        );
        assertParsingException(() -> expression("\"\"\"\"\"\" foo \"\"\"\" == abc"), "line 1:8: mismatched input 'foo' expecting {<EOF>,");
    }

    public void testBooleanLiteralsCondition() {
        Expression expression = expression("true and false");
        assertThat(expression, instanceOf(And.class));
        And and = (And) expression;
        assertThat(and.left(), equalTo(Literal.TRUE));
        assertThat(and.right(), equalTo(Literal.FALSE));
    }

    public void testArithmeticOperationCondition() {
        Expression expression = expression("-a-b*c == 123");
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
        Expression expression = expression("not aaa and b or c");
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
        Expression expression = expression("((a and ((b and c))) or (((x or y))))");
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

    /*
     * a > 1 and b > 1 + 2 => (a > 1) and (b > (1 + 2))
     */
    public void testOperatorsPrecedenceWithConjunction() {
        Expression expression = expression("a > 1 and b > 1 + 2");
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
        Expression expression = expression("a <= 1 or b >= 5 / 2 and c != 5");
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
        Expression expression = expression("not a == 1 or not b >= 5 and c == 5");
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
        assertThat(expression("a-1>2 or b>=5 and c-1>=5"), equalTo(expression("((a-1)>2 or (b>=5 and (c-1)>=5))")));
        assertThat(
            expression("a*5==25 and b>5 and c%4>=1 or true or false"),
            equalTo(expression("(((((a*5)==25) and (b>5) and ((c%4)>=1)) or true) or false)"))
        );
        assertThat(
            expression("a*4-b*5<100 and b/2+c*6>=50 or c%5+x>=5"),
            equalTo(expression("((((a*4)-(b*5))<100) and (((b/2)+(c*6))>=50)) or (((c%5)+x)>=5)"))
        );
        assertThat(
            expression("true and false or true and c/12+x*5-y%2>=50"),
            equalTo(expression("((true and false) or (true and (((c/12)+(x*5)-(y%2))>=50)))"))
        );
    }

    private Expression expression(String e) {
        return parser.createExpression(e);
    }

    private Literal l(Object value, DataType type) {
        return new Literal(null, value, type);
    }

    private void assertParsingException(ThrowingRunnable expression, String expectedError) {
        ParsingException e = expectThrows(ParsingException.class, "Expected syntax error", expression);
        assertThat(e.getMessage(), startsWith(expectedError));
    }
}
