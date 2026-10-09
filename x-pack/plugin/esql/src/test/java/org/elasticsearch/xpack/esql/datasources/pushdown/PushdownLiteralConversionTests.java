/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.pushdown;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.predicate.Range;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;

import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_CFG;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class PushdownLiteralConversionTests extends ESTestCase {

    private static final Source SRC = Source.EMPTY;

    public void testDateLiteralOnDateNanosWidensExactly() {
        long millis = 1_700_000_000_000L;
        Expression rewritten = PushdownLiteralConversion.rewrite(
            new Equals(SRC, field("ts", DataType.DATE_NANOS), new Literal(SRC, millis, DataType.DATETIME), null)
        );
        Equals eq = asInstanceOf(Equals.class, rewritten);
        assertThat(eq.right().dataType(), equalTo(DataType.DATE_NANOS));
        assertThat(((Number) ((Literal) eq.right()).value()).longValue(), equalTo(millis * 1_000_000L));
    }

    public void testDateNanosLiteralOnDateEqualsExactMillis() {
        long nanos = 1_700_000_000_000_000_000L;
        Expression rewritten = PushdownLiteralConversion.rewrite(
            new Equals(SRC, field("ts", DataType.DATETIME), new Literal(SRC, nanos, DataType.DATE_NANOS), null)
        );
        Equals eq = asInstanceOf(Equals.class, rewritten);
        assertThat(eq.right().dataType(), equalTo(DataType.DATETIME));
        assertThat(((Number) ((Literal) eq.right()).value()).longValue(), equalTo(nanos / 1_000_000L));
    }

    public void testDateNanosLiteralOnDateEqualsSubMillisIsContradiction() {
        long nanos = 1_700_000_000_000_000_001L;
        Expression rewritten = PushdownLiteralConversion.rewrite(
            new Equals(SRC, field("ts", DataType.DATETIME), new Literal(SRC, nanos, DataType.DATE_NANOS), null)
        );
        // i < Long.MIN_VALUE style domain contradiction
        assertThat(rewritten, instanceOf(LessThan.class));
        LessThan lt = (LessThan) rewritten;
        assertThat(((Number) ((Literal) lt.right()).value()).longValue(), equalTo(Long.MIN_VALUE));
    }

    public void testIntegerLessThanDoubleWidensToLteFloor() {
        Expression rewritten = PushdownLiteralConversion.rewrite(
            new LessThan(SRC, field("id", DataType.INTEGER), new Literal(SRC, 5.5, DataType.DOUBLE), null)
        );
        LessThanOrEqual lte = asInstanceOf(LessThanOrEqual.class, rewritten);
        assertThat(lte.right().dataType(), equalTo(DataType.INTEGER));
        assertThat(((Number) ((Literal) lte.right()).value()).intValue(), equalTo(5));
    }

    public void testIntegerLessThanOrEqualOutOfRangeIsTautology() {
        Expression rewritten = PushdownLiteralConversion.rewrite(
            new LessThanOrEqual(SRC, field("id", DataType.INTEGER), new Literal(SRC, 3_000_000_000L, DataType.LONG), null)
        );
        LessThanOrEqual lte = asInstanceOf(LessThanOrEqual.class, rewritten);
        assertThat(((Number) ((Literal) lte.right()).value()).intValue(), equalTo(Integer.MAX_VALUE));
    }

    public void testIntegerGreaterThanOutOfRangeIsContradiction() {
        Expression rewritten = PushdownLiteralConversion.rewrite(
            new GreaterThan(SRC, field("id", DataType.INTEGER), new Literal(SRC, 3_000_000_000L, DataType.LONG), null)
        );
        LessThan lt = asInstanceOf(LessThan.class, rewritten);
        assertThat(((Number) ((Literal) lt.right()).value()).intValue(), equalTo(Integer.MIN_VALUE));
    }

    public void testLongEqualsIntegerWidens() {
        Expression rewritten = PushdownLiteralConversion.rewrite(
            new Equals(SRC, field("id", DataType.LONG), new Literal(SRC, 5, DataType.INTEGER), null)
        );
        Equals eq = asInstanceOf(Equals.class, rewritten);
        assertThat(eq.right().dataType(), equalTo(DataType.LONG));
        assertThat(((Number) ((Literal) eq.right()).value()).longValue(), equalTo(5L));
    }

    public void testMatchingTypesUnchanged() {
        Equals original = new Equals(SRC, field("id", DataType.INTEGER), new Literal(SRC, 5, DataType.INTEGER), null);
        assertSame(original, PushdownLiteralConversion.rewrite(original));
    }

    public void testIntegerGreaterThanDoubleWidensToGteCeil() {
        Expression rewritten = PushdownLiteralConversion.rewrite(
            new GreaterThan(SRC, field("id", DataType.INTEGER), new Literal(SRC, 5.5, DataType.DOUBLE), null)
        );
        GreaterThanOrEqual gte = asInstanceOf(GreaterThanOrEqual.class, rewritten);
        assertThat(((Number) ((Literal) gte.right()).value()).intValue(), equalTo(6));
    }

    public void testIntegerLessThanOrEqualDoubleFloors() {
        Expression rewritten = PushdownLiteralConversion.rewrite(
            new LessThanOrEqual(SRC, field("id", DataType.INTEGER), new Literal(SRC, 5.5, DataType.DOUBLE), null)
        );
        LessThanOrEqual lte = asInstanceOf(LessThanOrEqual.class, rewritten);
        assertThat(((Number) ((Literal) lte.right()).value()).intValue(), equalTo(5));
    }

    public void testIntegerInDropsUnreachableDouble() {
        Expression rewritten = PushdownLiteralConversion.rewrite(
            new In(
                SRC,
                field("id", DataType.INTEGER),
                List.of(new Literal(SRC, 5, DataType.INTEGER), new Literal(SRC, 5.5, DataType.DOUBLE))
            )
        );
        Equals eq = asInstanceOf(Equals.class, rewritten);
        assertThat(((Number) ((Literal) eq.right()).value()).intValue(), equalTo(5));
    }

    public void testSwappedLiteralOpFieldConverts() {
        Expression rewritten = PushdownLiteralConversion.rewrite(
            new GreaterThan(SRC, new Literal(SRC, 5.5, DataType.DOUBLE), field("id", DataType.INTEGER), null)
        );
        // 5.5 > id → id < 5.5 → id <= 5
        LessThanOrEqual lte = asInstanceOf(LessThanOrEqual.class, rewritten);
        assertThat(((Number) ((Literal) lte.right()).value()).intValue(), equalTo(5));
    }

    public void testLongEqualsDoubleAtOrAboveTwoToFiftyThreeDeclines() {
        // (double)(2^53+1) == (double)2^53 — boundary is not an injective preimage; decline |d| >= 2^53.
        double atBoundary = 0x1p53;
        Equals boundaryEq = new Equals(SRC, field("id", DataType.LONG), new Literal(SRC, atBoundary, DataType.DOUBLE), null);
        assertSame(boundaryEq, PushdownLiteralConversion.rewrite(boundaryEq));

        double above = Math.nextUp(0x1p53);
        Equals aboveEq = new Equals(SRC, field("id", DataType.LONG), new Literal(SRC, above, DataType.DOUBLE), null);
        assertSame(aboveEq, PushdownLiteralConversion.rewrite(aboveEq));

        // Just inside the injective range still converts (2^53-1 is exact in double).
        double justBelow = Math.nextDown(0x1p53);
        Equals below = new Equals(SRC, field("id", DataType.LONG), new Literal(SRC, justBelow, DataType.DOUBLE), null);
        Equals rewrittenBelow = asInstanceOf(Equals.class, PushdownLiteralConversion.rewrite(below));
        assertThat(((Number) ((Literal) rewrittenBelow.right()).value()).longValue(), equalTo((1L << 53) - 1));
    }

    public void testDateNanosLessThanOnDateColumnRoundsOutward() {
        // nanos = millis*1e6 + 1 → LT uses ceilDiv → bound millis+1
        long nanos = 1_700_000_000_000_000_001L;
        Expression rewritten = PushdownLiteralConversion.rewrite(
            new LessThan(SRC, field("ts", DataType.DATETIME), new Literal(SRC, nanos, DataType.DATE_NANOS), null)
        );
        LessThan lt = asInstanceOf(LessThan.class, rewritten);
        assertThat(lt.right().dataType(), equalTo(DataType.DATETIME));
        assertThat(((Number) ((Literal) lt.right()).value()).longValue(), equalTo(1_700_000_000_001L));
    }

    public void testDateNanosNotEqualsSubMillisIsTautology() {
        long nanos = 1_700_000_000_000_000_001L;
        Expression rewritten = PushdownLiteralConversion.rewrite(
            new NotEquals(SRC, field("ts", DataType.DATETIME), new Literal(SRC, nanos, DataType.DATE_NANOS), null)
        );
        LessThanOrEqual lte = asInstanceOf(LessThanOrEqual.class, rewritten);
        assertThat(((Number) ((Literal) lte.right()).value()).longValue(), equalTo(Long.MAX_VALUE));
    }

    public void testNegativeNanosLiteralDeclines() {
        Equals original = new Equals(SRC, field("ts", DataType.DATETIME), new Literal(SRC, -1L, DataType.DATE_NANOS), null);
        assertSame(original, PushdownLiteralConversion.rewrite(original));
    }

    public void testNullLiteralDeclines() {
        Equals original = new Equals(SRC, field("id", DataType.INTEGER), new Literal(SRC, null, DataType.DOUBLE), null);
        assertSame(original, PushdownLiteralConversion.rewrite(original));
    }

    public void testNotOverMixedConvertsChild() {
        Expression rewritten = PushdownLiteralConversion.rewrite(
            new Not(SRC, new LessThan(SRC, field("id", DataType.INTEGER), new Literal(SRC, 5.5, DataType.DOUBLE), null))
        );
        Not not = asInstanceOf(Not.class, rewritten);
        LessThanOrEqual lte = asInstanceOf(LessThanOrEqual.class, not.field());
        assertThat(((Number) ((Literal) lte.right()).value()).intValue(), equalTo(5));
    }

    public void testRangeMixedConvertedAndMatchingBound() {
        Expression rewritten = PushdownLiteralConversion.rewrite(
            new Range(
                SRC,
                field("id", DataType.INTEGER),
                new Literal(SRC, 0, DataType.INTEGER),
                true,
                new Literal(SRC, 5.5, DataType.DOUBLE),
                true,
                ZoneOffset.UTC
            )
        );
        // Lower stays 0 (agreeing); upper 5.5 → <= 5
        And and = asInstanceOf(And.class, rewritten);
        GreaterThanOrEqual lo = asInstanceOf(GreaterThanOrEqual.class, and.left());
        LessThanOrEqual hi = asInstanceOf(LessThanOrEqual.class, and.right());
        assertThat(((Number) ((Literal) lo.right()).value()).intValue(), equalTo(0));
        assertThat(((Number) ((Literal) hi.right()).value()).intValue(), equalTo(5));
    }

    public void testRangeNonLiteralBoundUnchanged() {
        // Foldable non-Literal mixed bound must decline, not throw inside literalValueOf.
        Expression foldableDouble = new Add(SRC, new Literal(SRC, 2.0, DataType.DOUBLE), new Literal(SRC, 3.5, DataType.DOUBLE), TEST_CFG);
        Range original = new Range(
            SRC,
            field("id", DataType.INTEGER),
            new Literal(SRC, 0, DataType.INTEGER),
            true,
            foldableDouble,
            true,
            ZoneOffset.UTC
        );
        assertSame(original, PushdownLiteralConversion.rewrite(original));
    }

    public void testMultiValueNumericLiteralDeclinesWithoutClassCast() {
        // MV array literals are still typed INTEGER/LONG/DOUBLE but hold a List — must decline,
        // not ClassCastException on (Number) value. Same-type MV never enters convertNumeric
        // (columnType == literalType), so use mixed types that used to throw.
        Equals original = new Equals(SRC, field("id", DataType.LONG), new Literal(SRC, List.of(1, 2), DataType.INTEGER), null);
        assertSame(original, PushdownLiteralConversion.rewrite(original));

        LessThan mixed = new LessThan(SRC, field("id", DataType.INTEGER), new Literal(SRC, List.of(1.5, 2.5), DataType.DOUBLE), null);
        assertSame(mixed, PushdownLiteralConversion.rewrite(mixed));
    }

    public void testRangeAgreeingNonLiteralAndMixedLiteralUnchanged() {
        // Reverse of testRangeNonLiteralBoundUnchanged: agreeing lower is foldable non-Literal,
        // upper is a mixed Literal. Must decline rather than ClassCastException on the cast.
        Expression foldableInt = new Add(SRC, new Literal(SRC, 1, DataType.INTEGER), new Literal(SRC, 2, DataType.INTEGER), TEST_CFG);
        Range original = new Range(
            SRC,
            field("id", DataType.INTEGER),
            foldableInt,
            true,
            new Literal(SRC, 5.5, DataType.DOUBLE),
            true,
            ZoneOffset.UTC
        );
        assertSame(original, PushdownLiteralConversion.rewrite(original));
    }

    private static FieldAttribute field(String name, DataType type) {
        return new FieldAttribute(SRC, name, new EsField(name, type, Map.of(), false, EsField.TimeSeriesFieldType.NONE));
    }
}
