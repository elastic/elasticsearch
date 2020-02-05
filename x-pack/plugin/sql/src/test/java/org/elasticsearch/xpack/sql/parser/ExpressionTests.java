/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.parser;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.function.UnresolvedFunction;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.Neg;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.NullEquals;
import org.elasticsearch.xpack.sql.SqlTestUtils;
import org.elasticsearch.xpack.sql.expression.function.scalar.Cast;
import org.elasticsearch.xpack.sql.expression.literal.interval.Interval;
import org.elasticsearch.xpack.sql.expression.predicate.conditional.Case;
import org.elasticsearch.xpack.sql.expression.predicate.conditional.IfConditional;
import org.elasticsearch.xpack.sql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.sql.expression.predicate.operator.arithmetic.Mul;
import org.elasticsearch.xpack.sql.expression.predicate.operator.arithmetic.Sub;

import java.time.Duration;
import java.time.Period;
import java.time.temporal.TemporalAmount;
import java.util.Locale;

import static java.lang.String.format;
import static org.elasticsearch.xpack.ql.type.DataTypes.BOOLEAN;
import static org.elasticsearch.xpack.ql.type.DataTypes.DOUBLE;
import static org.elasticsearch.xpack.ql.type.DataTypes.INTEGER;
import static org.elasticsearch.xpack.ql.type.DataTypes.LONG;
import static org.hamcrest.Matchers.startsWith;

public class ExpressionTests extends ESTestCase {

    private final SqlParser parser = new SqlParser();

    public void testTokenFunctionName() {
        Expression lt = parser.createExpression("LEFT()");
        assertEquals(UnresolvedFunction.class, lt.getClass());
        UnresolvedFunction uf = (UnresolvedFunction) lt;
        assertEquals("LEFT()", uf.sourceText());
    }

    public void testLiteralBoolean() {
        Expression lt = parser.createExpression("TRUE");
        assertEquals(Literal.class, lt.getClass());
        Literal l = (Literal) lt;
        assertEquals(Boolean.TRUE, l.value());
        assertEquals(BOOLEAN, l.dataType());
    }

    public void testLiteralDouble() {
        Expression lt = parser.createExpression(String.valueOf(Double.MAX_VALUE));
        assertEquals(Literal.class, lt.getClass());
        Literal l = (Literal) lt;
        assertEquals(Double.MAX_VALUE, l.value());
        assertEquals(DOUBLE, l.dataType());
    }

    public void testLiteralDoubleNegative() {
        Expression lt = parser.createExpression(String.valueOf(Double.MIN_VALUE));
        assertEquals(Literal.class, lt.getClass());
        Literal l = (Literal) lt;
        assertEquals(Double.MIN_VALUE, l.value());
        assertEquals(DOUBLE, l.dataType());
    }

    public void testLiteralDoublePositive() {
        Expression lt = parser.createExpression("+" + Double.MAX_VALUE);
        assertEquals(Literal.class, lt.getClass());
        Literal l = (Literal) lt;
        assertEquals(Double.MAX_VALUE, l.value());
        assertEquals(DOUBLE, l.dataType());
    }

    public void testLiteralLong() {
        Expression lt = parser.createExpression(String.valueOf(Long.MAX_VALUE));
        assertEquals(Literal.class, lt.getClass());
        Literal l = (Literal) lt;
        assertEquals(Long.MAX_VALUE, l.value());
        assertEquals(LONG, l.dataType());
    }

    public void testLiteralLongNegative() {
        Expression lt = parser.createExpression(String.valueOf(Long.MIN_VALUE));
        assertTrue(lt.foldable());
        assertEquals(Long.MIN_VALUE, lt.fold());
        assertEquals(LONG, lt.dataType());
    }

    public void testLiteralLongPositive() {
        Expression lt = parser.createExpression("+" + Long.MAX_VALUE);
        assertEquals(Literal.class, lt.getClass());
        Literal l = (Literal) lt;
        assertEquals(Long.MAX_VALUE, l.value());
        assertEquals(LONG, l.dataType());
    }

    public void testLiteralInteger() {
        Expression lt = parser.createExpression(String.valueOf(Integer.MAX_VALUE));
        assertEquals(Literal.class, lt.getClass());
        Literal l = (Literal) lt;
        assertEquals(Integer.MAX_VALUE, l.value());
        assertEquals(INTEGER, l.dataType());
    }

    public void testLiteralIntegerWithShortValue() {
        Expression lt = parser.createExpression(String.valueOf(Short.MAX_VALUE));
        assertEquals(Literal.class, lt.getClass());
        Literal l = (Literal) lt;
        assertEquals((int) Short.MAX_VALUE, l.value());
        assertEquals(INTEGER, l.dataType());
    }

    public void testLiteralIntegerWithByteValue() {
        Expression lt = parser.createExpression(String.valueOf(Byte.MAX_VALUE));
        assertEquals(Literal.class, lt.getClass());
        Literal l = (Literal) lt;
        assertEquals((int) Byte.MAX_VALUE, l.value());
        assertEquals(INTEGER, l.dataType());
    }

    public void testLiteralIntegerInvalid() {
        ParsingException ex = expectThrows(ParsingException.class, () -> parser.createExpression("123456789098765432101"));
        assertEquals("Number [123456789098765432101] is too large", ex.getErrorMessage());
    }

    public void testLiteralDecimalTooBig() {
        ParsingException ex = expectThrows(ParsingException.class, () -> parser.createExpression("1.9976931348623157e+308"));
        assertEquals("Number [1.9976931348623157e+308] is too large", ex.getErrorMessage());
    }

    public void testExactDayTimeInterval() {
        int number = randomIntBetween(-100, 100);
        assertEquals(Duration.ofDays(number), intervalOf("INTERVAL " + number + " DAY"));
        number = randomIntBetween(-100, 100);
        assertEquals(Duration.ofHours(number), intervalOf("INTERVAL " + number + " HOUR"));
        number = randomIntBetween(-100, 100);
        assertEquals(Duration.ofMinutes(number), intervalOf("INTERVAL " + number + " MINUTE"));
        number = randomIntBetween(-100, 100);
        assertEquals(Duration.ofSeconds(number), intervalOf("INTERVAL " + number + " SECOND"));
    }

    public void testExactYearMonthInterval() {
        int number = randomIntBetween(-100, 100);
        assertEquals(Period.ofYears(number), intervalOf("INTERVAL " + number + " YEAR"));
        number = randomIntBetween(-100, 100);
        assertEquals(Period.ofMonths(number), intervalOf("INTERVAL " + number + " MONTH"));
    }

    public void testStringInterval() {
        int randomDay = randomInt(1024);
        int randomHour = randomInt(23);
        int randomMinute = randomInt(59);
        int randomSecond = randomInt(59);
        int randomMilli = randomInt(999);

        String value = format(Locale.ROOT, "INTERVAL '%d %d:%d:%d.%03d' DAY TO SECOND", randomDay, randomHour, randomMinute, randomSecond,
                randomMilli);
        assertEquals(Duration.ofDays(randomDay).plusHours(randomHour).plusMinutes(randomMinute).plusSeconds(randomSecond)
                .plusMillis(randomMilli), intervalOf(value));
    }

    public void testNegativeStringInterval() {
        int randomDay = randomInt(1024);
        int randomHour = randomInt(23);
        int randomMinute = randomInt(59);
        int randomSecond = randomInt(59);
        int randomMilli = randomInt(999);

        String value = format(Locale.ROOT, "INTERVAL -'%d %d:%d:%d.%03d' DAY TO SECOND", randomDay, randomHour, randomMinute, randomSecond,
                randomMilli);
        assertEquals(Duration.ofDays(randomDay).plusHours(randomHour).plusMinutes(randomMinute).plusSeconds(randomSecond)
                .plusMillis(randomMilli).negated(), intervalOf(value));
    }

    private TemporalAmount intervalOf(String query) {
        Expression lt = parser.createExpression(query);
        assertEquals(Literal.class, lt.getClass());
        Literal l = (Literal) lt;
        Object value = l.value();
        assertTrue(Interval.class.isAssignableFrom(value.getClass()));
        return ((Interval<?>) value).interval();
    }

    public void testLiteralTimesLiteral() {
        Expression expr = parser.createExpression("10 *2");
        assertEquals(Mul.class, expr.getClass());
        Mul mul = (Mul) expr;
        assertEquals("10 *2", mul.sourceText());
        assertEquals(INTEGER, mul.dataType());
    }

    public void testFunctionTimesLiteral() {
        Expression expr = parser.createExpression("PI()*2");
        assertEquals(Mul.class, expr.getClass());
        Mul mul = (Mul) expr;
        assertEquals("PI()*2", mul.sourceText());
    }

    public void testNegativeLiteral() {
        Expression expr = parser.createExpression("- 6");
        assertEquals(Literal.class, expr.getClass());
        assertEquals("- 6", expr.sourceText());
        assertEquals(-6, expr.fold());

        expr = parser.createExpression("- ( 6.134 )");
        assertEquals(Literal.class, expr.getClass());
        assertEquals("- ( 6.134 )", expr.sourceText());
        assertEquals(-6.134, expr.fold());

        expr = parser.createExpression("- ( ( (1.25) )    )");
        assertEquals(Literal.class, expr.getClass());
        assertEquals("- ( ( (1.25) )    )", expr.sourceText());
        assertEquals(-1.25, expr.fold());

        int numberOfParentheses = randomIntBetween(3, 10);
        double value = randomDouble();
        StringBuilder sb = new StringBuilder("-");
        for (int i = 0; i < numberOfParentheses; i++) {
            sb.append("(").append(SqlTestUtils.randomWhitespaces());
        }
        sb.append(value);
        for (int i = 0; i < numberOfParentheses; i++) {
            sb.append(")");
            if (i < numberOfParentheses - 1) {
                sb.append(SqlTestUtils.randomWhitespaces());
            }
        }
        expr = parser.createExpression(sb.toString());
        assertEquals(Literal.class, expr.getClass());
        assertEquals(sb.toString(), expr.sourceText());
        assertEquals(- value, expr.fold());
    }

    public void testComplexArithmetic() {
        String sql = "-(((a-2)- (( - ( (  3)  )) )  )+ b)";
        Expression expr = parser.createExpression(sql);
        assertEquals(Neg.class, expr.getClass());
        Neg neg = (Neg) expr;
        assertEquals(sql, neg.sourceText());
        assertEquals(1, neg.children().size());
        assertEquals(Add.class, neg.children().get(0).getClass());
        Add add = (Add) neg.children().get(0);
        assertEquals("((a-2)- (( - ( (  3)  )) )  )+ b", add.sourceText());
        assertEquals(2, add.children().size());
        assertEquals("?b", add.children().get(1).toString());
        assertEquals(Sub.class, add.children().get(0).getClass());
        Sub sub1 = (Sub) add.children().get(0);
        assertEquals("(a-2)- (( - ( (  3)  )) )", sub1.sourceText());
        assertEquals(2, sub1.children().size());
        assertEquals(Literal.class, sub1.children().get(1).getClass());
        assertEquals("- ( (  3)  )", sub1.children().get(1).sourceText());
        assertEquals(Sub.class, sub1.children().get(0).getClass());
        Sub sub2 = (Sub) sub1.children().get(0);
        assertEquals(2, sub2.children().size());
        assertEquals("?a", sub2.children().get(0).toString());
        assertEquals(Literal.class, sub2.children().get(1).getClass());
        assertEquals("2", sub2.children().get(1).sourceText());
    }

    public void testEquals() {
        Expression expr = parser.createExpression("a = 10");
        assertEquals(Equals.class, expr.getClass());
        Equals eq = (Equals) expr;
        assertEquals("a = 10", eq.sourceText());
        assertEquals(2, eq.children().size());
    }

    public void testNullEquals() {
        Expression expr = parser.createExpression("a <=> 10");
        assertEquals(NullEquals.class, expr.getClass());
        NullEquals nullEquals = (NullEquals) expr;
        assertEquals("a <=> 10", nullEquals.sourceText());
        assertEquals(2, nullEquals.children().size());
    }

    public void testNotEquals() {
        Expression expr = parser.createExpression("a != 10");
        assertEquals(NotEquals.class, expr.getClass());
        NotEquals neq = (NotEquals) expr;
        assertEquals("a != 10", neq.sourceText());
        assertEquals(2, neq.children().size());
    }

    public void testCastWithUnquotedDataType() {
        Expression expr = parser.createExpression("CAST(10* 2 AS long)");
        assertEquals(Cast.class, expr.getClass());
        Cast cast = (Cast) expr;
        assertEquals(INTEGER, cast.from());
        assertEquals(LONG, cast.to());
        assertEquals(LONG, cast.dataType());
        assertEquals(Mul.class, cast.field().getClass());
        Mul mul = (Mul) cast.field();
        assertEquals("10* 2", mul.sourceText());
        assertEquals(INTEGER, mul.dataType());
    }

    public void testCastWithQuotedDataType() {
        Expression expr = parser.createExpression("CAST(10*2 AS \"LonG\")");
        assertEquals(Cast.class, expr.getClass());
        Cast cast = (Cast) expr;
        assertEquals(INTEGER, cast.from());
        assertEquals(LONG, cast.to());
        assertEquals(LONG, cast.dataType());
        assertEquals(Mul.class, cast.field().getClass());
        Mul mul = (Mul) cast.field();
        assertEquals("10*2", mul.sourceText());
        assertEquals(INTEGER, mul.dataType());
    }

    public void testCastWithInvalidDataType() {
        ParsingException ex = expectThrows(ParsingException.class, () -> parser.createExpression("CAST(1 AS InVaLiD)"));
        assertEquals("line 1:12: Does not recognize type [InVaLiD]", ex.getMessage());
    }

    public void testCastOperatorPrecedence() {
        Expression expr = parser.createExpression("(10* 2::long)");
        assertEquals(Mul.class, expr.getClass());
        Mul mul = (Mul) expr;
        assertEquals(LONG, mul.dataType());
        assertEquals(INTEGER, mul.left().dataType());
        assertEquals(Cast.class, mul.right().getClass());
        Cast cast = (Cast) mul.right();
        assertEquals(INTEGER, cast.from());
        assertEquals(LONG, cast.to());
        assertEquals(LONG, cast.dataType());
    }

    public void testCastOperatorWithUnquotedDataType() {
        Expression expr = parser.createExpression("(10* 2)::long");
        assertEquals(Cast.class, expr.getClass());
        Cast cast = (Cast) expr;
        assertEquals(INTEGER, cast.from());
        assertEquals(LONG, cast.to());
        assertEquals(LONG, cast.dataType());
        assertEquals(Mul.class, cast.field().getClass());
        Mul mul = (Mul) cast.field();
        assertEquals("10* 2", mul.sourceText());
        assertEquals(INTEGER, mul.dataType());
    }

    public void testCastOperatorWithQuotedDataType() {
        Expression expr = parser.createExpression("(10*2)::\"LonG\"");
        assertEquals(Cast.class, expr.getClass());
        Cast cast = (Cast) expr;
        assertEquals(INTEGER, cast.from());
        assertEquals(LONG, cast.to());
        assertEquals(LONG, cast.dataType());
        assertEquals(Mul.class, cast.field().getClass());
        Mul mul = (Mul) cast.field();
        assertEquals("10*2", mul.sourceText());
        assertEquals(INTEGER, mul.dataType());
    }

    public void testCastOperatorWithInvalidDataType() {
        ParsingException ex = expectThrows(ParsingException.class, () -> parser.createExpression("-1::InVaLiD"));
        assertEquals("line 1:6: Does not recognize type [InVaLiD]", ex.getMessage());
    }

    public void testConvertWithUnquotedDataType() {
        Expression expr = parser.createExpression("CONVERT(10*2, long)");
        assertEquals(Cast.class, expr.getClass());
        Cast cast = (Cast) expr;
        assertEquals(INTEGER, cast.from());
        assertEquals(LONG, cast.to());
        assertEquals(LONG, cast.dataType());
        assertEquals(Mul.class, cast.field().getClass());
        Mul mul = (Mul) cast.field();
        assertEquals("10*2", mul.sourceText());
        assertEquals(INTEGER, mul.dataType());
    }

    public void testConvertWithQuotedDataType() {
        String e = "CONVERT(10*2, \"LonG\")";
        Expression expr = parser.createExpression(e);
        assertEquals(Cast.class, expr.getClass());
        Cast cast = (Cast) expr;
        assertEquals(e, cast.sourceText());
        assertEquals(INTEGER, cast.from());
        assertEquals(LONG, cast.to());
        assertEquals(LONG, cast.dataType());
        assertEquals(Mul.class, cast.field().getClass());
        Mul mul = (Mul) cast.field();
        assertEquals("10*2", mul.sourceText());
        assertEquals(INTEGER, mul.dataType());
    }

    public void testConvertWithUnquotedODBCDataType() {
        Expression expr = parser.createExpression("CONVERT(1, Sql_BigInt)");
        assertEquals(Cast.class, expr.getClass());
        Cast cast = (Cast) expr;
        assertEquals(INTEGER, cast.from());
        assertEquals(LONG, cast.to());
        assertEquals(LONG, cast.dataType());
    }

    public void testConvertWithQuotedODBCDataType() {
        Expression expr = parser.createExpression("CONVERT(1, \"sql_BIGint\")");
        assertEquals(Cast.class, expr.getClass());
        Cast cast = (Cast) expr;
        assertEquals(INTEGER, cast.from());
        assertEquals(LONG, cast.to());
        assertEquals(LONG, cast.dataType());
    }

    public void testConvertWithInvalidODBCDataType() {
        ParsingException ex = expectThrows(ParsingException.class, () -> parser.createExpression("CONVERT(1, SQL_INVALID)"));
        assertEquals("line 1:13: Does not recognize type [SQL_INVALID]", ex.getMessage());
    }

    public void testConvertWithInvalidESDataType() {
        ParsingException ex = expectThrows(ParsingException.class, () -> parser.createExpression("CONVERT(1, INVALID)"));
        assertEquals("line 1:13: Does not recognize type [INVALID]", ex.getMessage());
    }

    public void testCurrentTimestamp() {
        Expression expr = parser.createExpression("CURRENT_TIMESTAMP");
        assertEquals(UnresolvedFunction.class, expr.getClass());
        UnresolvedFunction ur = (UnresolvedFunction) expr;
        assertEquals("CURRENT_TIMESTAMP", ur.sourceText());
        assertEquals(0, ur.children().size());
    }

    public void testCurrentTimestampPrecision() {
        Expression expr = parser.createExpression("CURRENT_TIMESTAMP(4)");
        assertEquals(UnresolvedFunction.class, expr.getClass());
        UnresolvedFunction ur = (UnresolvedFunction) expr;
        assertEquals("CURRENT_TIMESTAMP(4)", ur.sourceText());
        assertEquals(1, ur.children().size());
        Expression child = ur.children().get(0);
        assertEquals(Literal.class, child.getClass());
        assertEquals(4, child.fold());
    }

    public void testCurrentDate() {
        Expression expr = parser.createExpression("CURRENT_DATE");
        assertEquals(UnresolvedFunction.class, expr.getClass());
        UnresolvedFunction ur = (UnresolvedFunction) expr;
        assertEquals("CURRENT_DATE", ur.sourceText());
        assertEquals(0, ur.children().size());
    }

    public void testCurrentDateWithParentheses() {
        Expression expr = parser.createExpression("CURRENT_DATE(  )");
        assertEquals(UnresolvedFunction.class, expr.getClass());
        UnresolvedFunction ur = (UnresolvedFunction) expr;
        assertEquals("CURRENT_DATE(  )", ur.sourceText());
        assertEquals(0, ur.children().size());
    }

    public void testCurrentTime() {
        Expression expr = parser.createExpression("CURRENT_TIME");
        assertEquals(UnresolvedFunction.class, expr.getClass());
        UnresolvedFunction ur = (UnresolvedFunction) expr;
        assertEquals("CURRENT_TIME", ur.sourceText());
        assertEquals(0, ur.children().size());
    }

    public void testCurrentTimePrecision() {
        Expression expr = parser.createExpression("CURRENT_TIME(7)");
        assertEquals(UnresolvedFunction.class, expr.getClass());
        UnresolvedFunction ur = (UnresolvedFunction) expr;
        assertEquals("CURRENT_TIME(7)", ur.sourceText());
        assertEquals(1, ur.children().size());
        Expression child = ur.children().get(0);
        assertEquals(Literal.class, child.getClass());
        assertEquals(7, child.fold());
    }

    public void testSourceKeyword() {
        String s = "CUrrENT_timestamP";
        Expression expr = parser.createExpression(s);
        assertEquals(s, expr.sourceText());
    }

    public void testSourceFunction() {
        String s = "PerCentile_RaNK(fOO,    12 )";
        Expression expr = parser.createExpression(s);
        assertEquals(s, expr.sourceText());
    }

    public void testCaseWithoutOperand() {
        Expression expr = parser.createExpression(
            "CASE WHEN a = 1 THEN 'one'" +
            "     WHEN a > 2 THEN 'a few'" +
            "     WHEN a > 10 THEN 'many' " +
            "END");

        assertEquals(Case.class, expr.getClass());
        Case c = (Case) expr;
        assertEquals(3, c.conditions().size());
        IfConditional ifc = c.conditions().get(0);
        assertEquals("WHEN a = 1 THEN 'one'", ifc.sourceText());
        assertThat(ifc.condition().toString(), startsWith("a = 1"));
        assertEquals("one", ifc.result().toString());
        assertEquals(Literal.NULL, c.elseResult());

        expr = parser.createExpression(
            "CASE WHEN a = 1 THEN 'one'" +
            "     WHEN a <= 2 THEN 'a few'" +
            "ELSE 'many' " +
            "END");

        assertEquals(Case.class, expr.getClass());
        c = (Case) expr;
        assertEquals(2, c.conditions().size());
        ifc = c.conditions().get(0);
        assertEquals("WHEN a = 1 THEN 'one'", ifc.sourceText());
        assertEquals("many", c.elseResult().toString());
    }

    public void testCaseWithOperand() {
        Expression expr = parser.createExpression(
            "CASE a WHEN 1 THEN 'one'" +
            "       WHEN 2 THEN 'two'" +
            "       WHEN 3 THEN 'three' " +
            "END");

        assertEquals(Case.class, expr.getClass());
        Case c = (Case) expr;
        assertEquals(3, c.conditions().size());
        IfConditional ifc = c.conditions().get(0);
        assertEquals("WHEN 1 THEN 'one'", ifc.sourceText());
        assertThat(ifc.condition().toString(), startsWith("WHEN 1 THEN 'one'"));
        assertEquals("one", ifc.result().toString());
        assertEquals(Literal.NULL, c.elseResult());

        expr = parser.createExpression(
            "CASE a WHEN 1 THEN 'one'" +
            "       WHEN 2 THEN 'two'" +
            "ELSE 'many' " +
            "END");
        assertEquals(Case.class, expr.getClass());
        c = (Case) expr;
        assertEquals(2, c.conditions().size());
        ifc = c.conditions().get(0);
        assertEquals("WHEN 1 THEN 'one'", ifc.sourceText());
        assertEquals("many", c.elseResult().toString());
    }
}
