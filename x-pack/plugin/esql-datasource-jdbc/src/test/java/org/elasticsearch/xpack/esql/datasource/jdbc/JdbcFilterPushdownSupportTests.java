/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.jdbc;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.WildcardPattern;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.datasources.spi.FilterPushdownSupport;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Contains;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.EndsWith;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.StartsWith;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.WildcardLike;
import org.elasticsearch.xpack.esql.expression.predicate.Range;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;

import java.util.List;
import java.util.Map;

/**
 * Unit tests for {@link JdbcFilterPushdownSupport}: builds ESQL {@link Expression}s by hand and asserts the resulting
 * {@link SqlPredicate} tree (and {@code RECHECK} flag for the keyword path). Renderer behavior is covered separately;
 * here we only care that translation lands on the right node type and refuses what it should refuse.
 */
public class JdbcFilterPushdownSupportTests extends ESTestCase {

    private static final Source SRC = Source.EMPTY;

    private final JdbcFilterPushdownSupport support = new JdbcFilterPushdownSupport(GenericDialect.INSTANCE);
    private final SqlRenderer renderer = new SqlRenderer(GenericDialect.INSTANCE);

    // -- Comparisons --

    public void testTranslateEqualsColumnFirst() {
        Expression eq = new Equals(SRC, intField("a"), intLiteral(7), null);
        SqlPredicate.Comparison c = (SqlPredicate.Comparison) pushOne(eq).filter();
        assertEquals("a", c.column());
        assertEquals(CompOp.EQ, c.op());
        assertEquals(7, c.param().value());
    }

    public void testTranslateEqualsLiteralFirstCommutes() {
        Expression eq = new Equals(SRC, intLiteral(7), intField("a"), null);
        SqlPredicate.Comparison c = (SqlPredicate.Comparison) pushOne(eq).filter();
        assertEquals("a", c.column());
        assertEquals(CompOp.EQ, c.op());
    }

    public void testTranslateLessThanCommutesCorrectly() {
        // 7 < a ===> a > 7
        Expression lt = new LessThan(SRC, intLiteral(7), intField("a"), null);
        SqlPredicate.Comparison c = (SqlPredicate.Comparison) pushOne(lt).filter();
        assertEquals(CompOp.GT, c.op());
    }

    public void testTranslateNotEquals() {
        Expression ne = new NotEquals(SRC, intField("a"), intLiteral(1), null);
        SqlPredicate.Comparison c = (SqlPredicate.Comparison) pushOne(ne).filter();
        assertEquals(CompOp.NEQ, c.op());
    }

    public void testRejectsComparisonAgainstNullLiteral() {
        Expression eq = new Equals(SRC, intField("a"), new Literal(SRC, null, DataType.INTEGER), null);
        FilterPushdownSupport.PushdownResult r = support.pushFilters(List.of(eq));
        assertFalse(r.hasPushedFilter());
        assertEquals(List.of(eq), r.remainder());
    }

    public void testRejectsTwoColumnsInComparison() {
        Expression eq = new Equals(SRC, intField("a"), intField("b"), null);
        FilterPushdownSupport.PushdownResult r = support.pushFilters(List.of(eq));
        assertFalse(r.hasPushedFilter());
    }

    public void testRejectsOverflowingNarrowing() {
        // a is INTEGER, literal is Long.MAX_VALUE -- cannot narrow without loss.
        FieldAttribute a = intField("a");
        Expression eq = new Equals(SRC, a, new Literal(SRC, Long.MAX_VALUE, DataType.LONG), null);
        FilterPushdownSupport.PushdownResult r = support.pushFilters(List.of(eq));
        assertFalse("out-of-range narrowing must refuse pushdown", r.hasPushedFilter());
    }

    public void testRejectsFractionalDoubleIntoIntegerColumn() {
        // INTEGER column, DOUBLE 1.9 literal: longValue() would silently truncate to 1, producing rows ESQL would
        // never match. Must refuse since INTEGER is non-RECHECK and the engine wouldn't reapply.
        Expression eq = new Equals(SRC, intField("a"), new Literal(SRC, 1.9, DataType.DOUBLE), null);
        FilterPushdownSupport.PushdownResult r = support.pushFilters(List.of(eq));
        assertFalse("fractional double -> integer must refuse pushdown", r.hasPushedFilter());
    }

    public void testAllowsIntegralDoubleIntoIntegerColumn() {
        // 1.0 is exactly representable; coercion is lossless.
        Expression eq = new Equals(SRC, intField("a"), new Literal(SRC, 1.0, DataType.DOUBLE), null);
        SqlPredicate.Comparison c = (SqlPredicate.Comparison) pushOne(eq).filter();
        assertEquals(1, c.param().value());
    }

    public void testRejectsDoubleAtLongMaxValueBoundary() {
        // (double) Long.MAX_VALUE rounds up to 2^63 (one more than Long.MAX_VALUE). A naive range check would
        // accept this value, but Double.longValue() saturates to Long.MAX_VALUE -- silent precision loss.
        // Verify we refuse rather than silently push a wrong predicate.
        FieldAttribute longCol = new FieldAttribute(
            SRC,
            "l",
            new EsField("l", DataType.LONG, java.util.Map.of(), false, EsField.TimeSeriesFieldType.NONE)
        );
        Expression eq = new Equals(SRC, longCol, new Literal(SRC, (double) Long.MAX_VALUE, DataType.DOUBLE), null);
        FilterPushdownSupport.PushdownResult r = support.pushFilters(List.of(eq));
        assertFalse("double at the (rounded-up) Long.MAX_VALUE boundary must refuse pushdown", r.hasPushedFilter());
    }

    public void testRejectsLargeLongIntoFloatColumnWhenLossy() {
        // Long that doesn't survive the round-trip via Float/Double. 16777217 is the smallest positive int that
        // can't be represented exactly as a 32-bit float.
        FieldAttribute floatCol = new FieldAttribute(
            SRC,
            "f",
            new EsField("f", DataType.FLOAT, java.util.Map.of(), false, EsField.TimeSeriesFieldType.NONE)
        );
        Expression eq = new Equals(SRC, floatCol, new Literal(SRC, 16777217L, DataType.LONG), null);
        FilterPushdownSupport.PushdownResult r = support.pushFilters(List.of(eq));
        assertFalse("long that loses precision in float must refuse pushdown", r.hasPushedFilter());
    }

    // -- Collation safety on KEYWORD --

    public void testRefusesKeywordNotEquals() {
        // Under case-insensitive collation, SQL <> 'foo' excludes 'Foo' but ESQL byte-exact would keep it. RECHECK
        // can't recover. Refuse.
        Expression p = new NotEquals(SRC, kwField("name"), keywordLiteral("foo"), null);
        FilterPushdownSupport.PushdownResult r = support.pushFilters(List.of(p));
        assertFalse("keyword <> must NOT push under GenericDialect", r.hasPushedFilter());
        assertEquals(List.of(p), r.remainder());
    }

    public void testRefusesKeywordOrderingComparison() {
        // Ordering on KEYWORD is collation-defined; subset semantics under at least one collation -> refuse.
        Expression lt = new LessThan(SRC, kwField("name"), keywordLiteral("foo"), null);
        FilterPushdownSupport.PushdownResult r = support.pushFilters(List.of(lt));
        assertFalse(r.hasPushedFilter());
    }

    public void testRefusesKeywordRange() {
        Expression range = new Range(SRC, kwField("name"), keywordLiteral("a"), true, keywordLiteral("z"), true, null);
        FilterPushdownSupport.PushdownResult r = support.pushFilters(List.of(range));
        assertFalse(r.hasPushedFilter());
    }

    public void testRefusesNotAroundKeywordEquals() {
        // NOT(name == 'foo') is structurally NOT(RECHECK), which under arbitrary collation can DROP rows ESQL would
        // keep. Refuse.
        Expression p = new Not(SRC, new Equals(SRC, kwField("name"), keywordLiteral("foo"), null));
        FilterPushdownSupport.PushdownResult r = support.pushFilters(List.of(p));
        assertFalse("NOT(keyword =) must NOT push under GenericDialect", r.hasPushedFilter());
        assertEquals(List.of(p), r.remainder());
    }

    public void testNotAroundExactPredicateIsAllowed() {
        // NOT(int_col IS NULL) -- inner is exact, NOT stays exact.
        Expression p = new Not(SRC, new IsNull(SRC, intField("a")));
        SqlPredicate.Not n = (SqlPredicate.Not) pushOne(p).filter();
        assertTrue(n.inner() instanceof SqlPredicate.IsNull);
    }

    public void testKeywordIsNullAllowedNonRecheck() {
        // IS NULL is collation-independent; safe to push as exact (no remainder needed).
        Expression p = new IsNull(SRC, kwField("k"));
        FilterPushdownSupport.PushdownResult r = support.pushFilters(List.of(p));
        assertTrue(r.hasPushedFilter());
        assertTrue("IS NULL on keyword is exact, not RECHECK", r.remainder().isEmpty());
    }

    // -- IS NULL / IS NOT NULL --

    public void testTranslateIsNull() {
        Expression p = new IsNull(SRC, intField("a"));
        assertEquals(new SqlPredicate.IsNull("a"), pushOne(p).filter());
    }

    public void testTranslateIsNotNull() {
        Expression p = new IsNotNull(SRC, intField("a"));
        assertEquals(new SqlPredicate.IsNotNull("a"), pushOne(p).filter());
    }

    // -- IN --

    public void testTranslateInList() {
        Expression in = new In(SRC, intField("a"), List.of(intLiteral(1), intLiteral(2), intLiteral(3)));
        SqlPredicate.InList list = (SqlPredicate.InList) pushOne(in).filter();
        assertEquals(3, list.values().size());
    }

    public void testRejectsInWithNullLiteral() {
        Expression in = new In(SRC, intField("a"), List.of(intLiteral(1), new Literal(SRC, null, DataType.INTEGER)));
        FilterPushdownSupport.PushdownResult r = support.pushFilters(List.of(in));
        assertFalse("IN with null literal must refuse pushdown", r.hasPushedFilter());
    }

    // -- Range --

    public void testTranslateRangeInclusiveBoth() {
        Expression range = new Range(SRC, intField("a"), intLiteral(1), true, intLiteral(10), true, null);
        SqlPredicate.Range r = (SqlPredicate.Range) pushOne(range).filter();
        assertTrue(r.lowerInclusive());
        assertTrue(r.upperInclusive());
    }

    public void testTranslateRangeHalfOpen() {
        Expression range = new Range(SRC, intField("a"), intLiteral(1), true, intLiteral(10), false, null);
        SqlPredicate.Range r = (SqlPredicate.Range) pushOne(range).filter();
        assertTrue(r.lowerInclusive());
        assertFalse(r.upperInclusive());
    }

    // -- Like family --

    public void testTranslateStartsWith() {
        Expression sw = new StartsWith(SRC, kwField("name"), keywordLiteral("foo"));
        FilterPushdownSupport.PushdownResult r = support.pushFilters(List.of(sw));
        SqlPredicate.Like like = (SqlPredicate.Like) ((JdbcPushedQuery) r.pushedFilter()).filter();
        assertEquals("foo%", like.pattern());
        assertEquals(List.of(sw), r.remainder()); // string-like is always RECHECK under GenericDialect
    }

    public void testTranslateEndsWith() {
        Expression ew = new EndsWith(SRC, kwField("name"), keywordLiteral("foo"));
        SqlPredicate.Like like = (SqlPredicate.Like) pushOne(ew).filter();
        assertEquals("%foo", like.pattern());
    }

    public void testTranslateContains() {
        Expression c = new Contains(SRC, kwField("name"), keywordLiteral("foo"));
        SqlPredicate.Like like = (SqlPredicate.Like) pushOne(c).filter();
        assertEquals("%foo%", like.pattern());
    }

    public void testStartsWithEscapesJdbcMetacharsInLiteral() {
        Expression sw = new StartsWith(SRC, kwField("name"), keywordLiteral("a%b_c"));
        SqlPredicate.Like like = (SqlPredicate.Like) pushOne(sw).filter();
        assertEquals("a\\%b\\_c%", like.pattern());
    }

    public void testTranslateWildcardLike() {
        // ESQL wildcards: * -> %, ? -> _
        Expression p = new WildcardLike(SRC, kwField("name"), new WildcardPattern("foo*bar?"), false);
        SqlPredicate.Like like = (SqlPredicate.Like) pushOne(p).filter();
        assertEquals("foo%bar_", like.pattern());
    }

    public void testWildcardLikeEscapesLiteralJdbcMetacharsInPattern() {
        // The wildcard pattern itself contains a literal % (not a wildcard in ESQL's Lucene-style syntax; only *
        // and ? are wildcards there). On the JDBC side that % must be escaped so the database treats it as a literal.
        Expression p = new WildcardLike(SRC, kwField("name"), new WildcardPattern("a%b*"), false);
        SqlPredicate.Like like = (SqlPredicate.Like) pushOne(p).filter();
        assertEquals("a\\%b%", like.pattern());
    }

    public void testWildcardLikeCaseInsensitiveRefused() {
        Expression p = new WildcardLike(SRC, kwField("name"), new WildcardPattern("foo*"), true);
        FilterPushdownSupport.PushdownResult r = support.pushFilters(List.of(p));
        assertFalse(r.hasPushedFilter());
        assertEquals(List.of(p), r.remainder());
    }

    // -- Logical operators --

    public void testTranslateAndFlattens() {
        Expression p1 = new GreaterThan(SRC, intField("a"), intLiteral(1), null);
        Expression p2 = new LessThan(SRC, intField("a"), intLiteral(10), null);
        Expression p3 = new IsNotNull(SRC, intField("b"));
        Expression and = new And(SRC, new And(SRC, p1, p2), p3);
        SqlPredicate.And flat = (SqlPredicate.And) pushOne(and).filter();
        assertEquals("nested ANDs should flatten to one group of 3", 3, flat.parts().size());
    }

    public void testTranslateOrPartialRefuses() {
        // OR with one non-pushable side: cannot push at all (would widen results).
        Expression pushable = new Equals(SRC, intField("a"), intLiteral(1), null);
        Expression notPushable = new Equals(SRC, intField("a"), intField("b"), null);
        Expression or = new Or(SRC, pushable, notPushable);
        FilterPushdownSupport.PushdownResult r = support.pushFilters(List.of(or));
        assertFalse("OR with one non-pushable side must refuse entirely", r.hasPushedFilter());
    }

    public void testTranslateNot() {
        Expression p = new Not(SRC, new IsNull(SRC, intField("a")));
        SqlPredicate.Not n = (SqlPredicate.Not) pushOne(p).filter();
        assertTrue(n.inner() instanceof SqlPredicate.IsNull);
    }

    // -- Multi-conjunct pushFilters --

    public void testPushMultipleConjuncts() {
        Expression p1 = new GreaterThan(SRC, intField("a"), intLiteral(1), null);
        Expression p2 = new IsNotNull(SRC, intField("b"));
        FilterPushdownSupport.PushdownResult r = support.pushFilters(List.of(p1, p2));
        assertTrue(r.hasPushedFilter());
        SqlPredicate combined = ((JdbcPushedQuery) r.pushedFilter()).filter();
        assertTrue("multi-conjunct should AND together", combined instanceof SqlPredicate.And);
        assertEquals(2, ((SqlPredicate.And) combined).parts().size());
    }

    public void testPartialPushKeepsNonPushableInRemainder() {
        Expression pushable = new Equals(SRC, intField("a"), intLiteral(1), null);
        Expression notPushable = new Equals(SRC, intField("a"), intField("b"), null);
        FilterPushdownSupport.PushdownResult r = support.pushFilters(List.of(pushable, notPushable));
        assertTrue(r.hasPushedFilter());
        assertEquals(List.of(notPushable), r.remainder());
    }

    public void testKeywordPushIsRecheck() {
        // Keyword equality: pushed AND retained in remainder so engine guarantees byte-exact semantics.
        Expression eq = new Equals(SRC, kwField("name"), keywordLiteral("foo"), null);
        FilterPushdownSupport.PushdownResult r = support.pushFilters(List.of(eq));
        assertTrue(r.hasPushedFilter());
        assertEquals("keyword pushdown is RECHECK", List.of(eq), r.remainder());
    }

    public void testIntegerPushIsNotRecheck() {
        Expression eq = new Equals(SRC, intField("a"), intLiteral(1), null);
        FilterPushdownSupport.PushdownResult r = support.pushFilters(List.of(eq));
        assertTrue(r.hasPushedFilter());
        assertTrue("integer pushdown must NOT recheck", r.remainder().isEmpty());
    }

    public void testCanPush() {
        // Numeric comparison: exact pushdown is safe.
        assertEquals(FilterPushdownSupport.Pushability.YES, support.canPush(new Equals(SRC, intField("a"), intLiteral(1), null)));
        // IS NOT NULL is collation-independent even on KEYWORD: YES, not RECHECK.
        assertEquals(FilterPushdownSupport.Pushability.YES, support.canPush(new IsNotNull(SRC, kwField("k"))));
        // KEYWORD equality may differ under vendor collation: RECHECK.
        assertEquals(
            FilterPushdownSupport.Pushability.RECHECK,
            support.canPush(new Equals(SRC, kwField("k"), keywordLiteral("foo"), null))
        );
        // Two-column comparison: cannot push.
        assertEquals(FilterPushdownSupport.Pushability.NO, support.canPush(new Equals(SRC, intField("a"), intField("b"), null)));
    }

    // -- Wildcard helpers --

    public void testEsqlWildcardToJdbcLikeMaps() {
        assertEquals("foo%bar_", JdbcFilterPushdownSupport.esqlWildcardToJdbcLike("foo*bar?"));
    }

    public void testEsqlWildcardToJdbcLikeEscapesLiteralPercent() {
        assertEquals("a\\%b\\_c", JdbcFilterPushdownSupport.esqlWildcardToJdbcLike("a%b_c"));
    }

    public void testEsqlWildcardToJdbcLikeHandlesEscapes() {
        // ESQL backslash-escape only applies to wildcard metacharacters. \* is a literal star (no JDBC escape needed),
        // \\ is a literal backslash (must escape in JDBC since \ is the LIKE escape char).
        assertEquals("*", JdbcFilterPushdownSupport.esqlWildcardToJdbcLike("\\*"));
        assertEquals("\\\\", JdbcFilterPushdownSupport.esqlWildcardToJdbcLike("\\\\"));
    }

    public void testEscapeLikeLiteralEscapesAllMetachars() {
        assertEquals("a\\%b\\_c\\\\d", JdbcFilterPushdownSupport.escapeLikeLiteral("a%b_c\\d"));
    }

    // -- Integration with renderer (smoke) --

    public void testEndToEndComparisonRenders() {
        Expression eq = new Equals(SRC, intField("a"), intLiteral(7), null);
        FilterPushdownSupport.PushdownResult r = support.pushFilters(List.of(eq));
        SqlRenderer.Rendered rendered = renderer.render(((JdbcPushedQuery) r.pushedFilter()).filter());
        assertEquals("\"a\" = ?", rendered.sql());
        assertEquals(1, rendered.params().size());
    }

    // -- helpers --

    private JdbcPushedQuery pushOne(Expression expr) {
        FilterPushdownSupport.PushdownResult r = support.pushFilters(List.of(expr));
        assertTrue("expected [" + expr + "] to be pushable", r.hasPushedFilter());
        return (JdbcPushedQuery) r.pushedFilter();
    }

    private static FieldAttribute intField(String name) {
        return new FieldAttribute(SRC, name, new EsField(name, DataType.INTEGER, Map.of(), false, EsField.TimeSeriesFieldType.NONE));
    }

    private static FieldAttribute kwField(String name) {
        return new FieldAttribute(SRC, name, new EsField(name, DataType.KEYWORD, Map.of(), false, EsField.TimeSeriesFieldType.NONE));
    }

    private static Literal intLiteral(int value) {
        return new Literal(SRC, value, DataType.INTEGER);
    }

    private static Literal keywordLiteral(String value) {
        return new Literal(SRC, new BytesRef(value), DataType.KEYWORD);
    }
}
