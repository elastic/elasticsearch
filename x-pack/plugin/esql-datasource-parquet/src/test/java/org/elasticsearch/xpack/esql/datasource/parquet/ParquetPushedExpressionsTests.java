/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.WildcardPattern;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.WildcardLike;
import org.elasticsearch.xpack.esql.expression.predicate.Range;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;

import java.time.ZoneOffset;
import java.util.List;
import java.util.Set;

import static org.apache.parquet.schema.LogicalTypeAnnotation.dateType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.timestampType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT96;
import static org.hamcrest.Matchers.containsString;

/**
 * Tests for {@link ParquetPushedExpressions#toFilterPredicate(MessageType)} verifying
 * schema-aware translation of DATETIME columns across different Parquet physical types.
 */
public class ParquetPushedExpressionsTests extends ESTestCase {

    private BlockFactory blockFactory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();
    }

    // --- TIMESTAMP_MILLIS (INT64) ---

    public void testToFilterPredicateTimestampMillis() {
        MessageType schema = Types.buildMessage()
            .required(INT64)
            .as(timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS))
            .named("ts")
            .named("test");

        long millis = 1700000000000L;
        Expression expr = eq("ts", DataType.DATETIME, millis);
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(expr));

        FilterPredicate fp = pushed.toFilterPredicate(schema);
        assertNotNull(fp);
        assertThat(fp.toString(), containsString("1700000000000"));
    }

    // --- TIMESTAMP_MICROS (INT64) ---

    public void testToFilterPredicateTimestampMicros() {
        MessageType schema = Types.buildMessage()
            .required(INT64)
            .as(timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS))
            .named("ts")
            .named("test");

        long millis = 1700000000000L;
        Expression expr = eq("ts", DataType.DATETIME, millis);
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(expr));

        FilterPredicate fp = pushed.toFilterPredicate(schema);
        assertNotNull(fp);
        assertThat(fp.toString(), containsString(String.valueOf(millis * 1000)));
    }

    // --- TIMESTAMP_NANOS (INT64) ---

    public void testToFilterPredicateTimestampNanos() {
        MessageType schema = Types.buildMessage()
            .required(INT64)
            .as(timestampType(true, LogicalTypeAnnotation.TimeUnit.NANOS))
            .named("ts")
            .named("test");

        long millis = 1700000000000L;
        Expression expr = eq("ts", DataType.DATETIME, millis);
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(expr));

        FilterPredicate fp = pushed.toFilterPredicate(schema);
        assertNotNull(fp);
        assertThat(fp.toString(), containsString(String.valueOf(millis * 1_000_000)));
    }

    // --- DATE (INT32) ---

    public void testToFilterPredicateDateInt32() {
        MessageType schema = Types.buildMessage().required(INT32).as(dateType()).named("d").named("test");

        long millis = 86400000L * 19723; // some date
        int expectedDays = (int) (millis / ParquetPushedExpressions.MILLIS_PER_DAY);
        Expression expr = eq("d", DataType.DATETIME, millis);
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(expr));

        FilterPredicate fp = pushed.toFilterPredicate(schema);
        assertNotNull(fp);
        assertThat(fp.toString(), containsString(String.valueOf(expectedDays)));
    }

    // --- INT96 (skip pushdown) ---

    public void testToFilterPredicateInt96SkipsPushdown() {
        MessageType schema = Types.buildMessage().required(INT96).named("ts").named("test");

        Expression expr = eq("ts", DataType.DATETIME, 1700000000000L);
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(expr));

        FilterPredicate fp = pushed.toFilterPredicate(schema);
        assertNull(fp);
    }

    // --- Column not in schema ---

    public void testToFilterPredicateColumnNotInSchema() {
        MessageType schema = Types.buildMessage().required(INT32).named("id").named("test");

        Expression expr = eq("missing_col", DataType.DATETIME, 1700000000000L);
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(expr));

        FilterPredicate fp = pushed.toFilterPredicate(schema);
        assertNull(fp);
    }

    // --- Mixed types (DATETIME + INTEGER) ---

    public void testToFilterPredicateMixedTypes() {
        MessageType schema = Types.buildMessage()
            .required(INT64)
            .as(timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS))
            .named("ts")
            .required(INT32)
            .named("id")
            .named("test");

        Expression tsExpr = new GreaterThan(Source.EMPTY, attr("ts", DataType.DATETIME), datetimeLit(1000L), null);
        Expression idExpr = eq("id", DataType.INTEGER, 42);
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(tsExpr, idExpr));

        FilterPredicate fp = pushed.toFilterPredicate(schema);
        assertNotNull(fp);
        String repr = fp.toString();
        assertThat(repr, containsString("ts"));
        assertThat(repr, containsString("id"));
    }

    // --- Range on DATE schema ---

    public void testToFilterPredicateRangeOnDate() {
        MessageType schema = Types.buildMessage().required(INT32).as(dateType()).named("d").named("test");

        long lowerMillis = 86400000L * 100;
        long upperMillis = 86400000L * 200;
        int expectedLowerDays = 100;
        int expectedUpperDays = 200;

        Expression range = new Range(
            Source.EMPTY,
            attr("d", DataType.DATETIME),
            datetimeLit(lowerMillis),
            true,
            datetimeLit(upperMillis),
            true,
            ZoneOffset.UTC
        );
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(range));

        FilterPredicate fp = pushed.toFilterPredicate(schema);
        assertNotNull(fp);
        String repr = fp.toString();
        assertThat(repr, containsString(String.valueOf(expectedLowerDays)));
        assertThat(repr, containsString(String.valueOf(expectedUpperDays)));
    }

    // --- IN list on TIMESTAMP_MICROS ---

    public void testToFilterPredicateInListOnTimestampMicros() {
        MessageType schema = Types.buildMessage()
            .required(INT64)
            .as(timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS))
            .named("ts")
            .named("test");

        long millis1 = 1000L;
        long millis2 = 2000L;

        Expression inExpr = new In(Source.EMPTY, attr("ts", DataType.DATETIME), List.of(datetimeLit(millis1), datetimeLit(millis2)));
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(inExpr));

        FilterPredicate fp = pushed.toFilterPredicate(schema);
        assertNotNull(fp);
        String repr = fp.toString();
        assertThat(repr, containsString(String.valueOf(millis1 * 1000)));
        assertThat(repr, containsString(String.valueOf(millis2 * 1000)));
    }

    // --- Overflow protection ---

    public void testToFilterPredicateOverflowProtection() {
        MessageType schema = Types.buildMessage()
            .required(INT64)
            .as(timestampType(true, LogicalTypeAnnotation.TimeUnit.NANOS))
            .named("ts")
            .named("test");

        // A millis value that would overflow when multiplied by 1_000_000
        long hugeMillis = Long.MAX_VALUE / 1000;
        Expression expr = eq("ts", DataType.DATETIME, hugeMillis);
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(expr));

        // Should gracefully skip (return null) instead of throwing
        FilterPredicate fp = pushed.toFilterPredicate(schema);
        assertNull(fp);
    }

    // --- Non-DATETIME types pass through unchanged ---

    public void testToFilterPredicateIntegerPassthrough() {
        MessageType schema = Types.buildMessage().required(INT32).named("id").named("test");

        Expression expr = eq("id", DataType.INTEGER, 42);
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(expr));

        FilterPredicate fp = pushed.toFilterPredicate(schema);
        assertNotNull(fp);
        assertThat(fp.toString(), containsString("42"));
    }

    public void testToFilterPredicateLessThanOrEqualDatetime() {
        MessageType schema = Types.buildMessage()
            .required(INT64)
            .as(timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS))
            .named("ts")
            .named("test");

        long millis = 1700000000000L;
        Expression expr = new LessThanOrEqual(Source.EMPTY, attr("ts", DataType.DATETIME), datetimeLit(millis), null);
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(expr));

        FilterPredicate fp = pushed.toFilterPredicate(schema);
        assertNotNull(fp);
        assertThat(fp.toString(), containsString("lteq"));
    }

    // --- convertMillisToPhysical unit tests ---

    public void testConvertMillisToPhysicalMillis() {
        LogicalTypeAnnotation ts = timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS);
        assertEquals(1234L, ParquetPushedExpressions.convertMillisToPhysical(1234L, ts));
    }

    public void testConvertMillisToPhysicalMicros() {
        LogicalTypeAnnotation ts = timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS);
        assertEquals(1234000L, ParquetPushedExpressions.convertMillisToPhysical(1234L, ts));
    }

    public void testConvertMillisToPhysicalNanos() {
        LogicalTypeAnnotation ts = timestampType(true, LogicalTypeAnnotation.TimeUnit.NANOS);
        assertEquals(1234000000L, ParquetPushedExpressions.convertMillisToPhysical(1234L, ts));
    }

    public void testConvertMillisToPhysicalNanosOverflow() {
        LogicalTypeAnnotation ts = timestampType(true, LogicalTypeAnnotation.TimeUnit.NANOS);
        expectThrows(ArithmeticException.class, () -> ParquetPushedExpressions.convertMillisToPhysical(Long.MAX_VALUE / 1000, ts));
    }

    public void testConvertMillisToPhysicalNoAnnotation() {
        assertEquals(5678L, ParquetPushedExpressions.convertMillisToPhysical(5678L, null));
    }

    // --- predicateColumnNames tests ---

    public void testPredicateColumnNamesSingleComparison() {
        Expression expr = new GreaterThan(Source.EMPTY, attr("status", DataType.LONG), lit(200L, DataType.LONG), null);
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(expr));
        assertEquals(Set.of("status"), pushed.predicateColumnNames());
    }

    public void testPredicateColumnNamesAndTwoColumns() {
        Expression left = new GreaterThan(Source.EMPTY, attr("age", DataType.LONG), lit(18L, DataType.LONG), null);
        Expression right = new LessThan(Source.EMPTY, attr("score", DataType.LONG), lit(100L, DataType.LONG), null);
        Expression and = new And(Source.EMPTY, left, right);
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(and));
        assertEquals(Set.of("age", "score"), pushed.predicateColumnNames());
    }

    public void testPredicateColumnNamesDeduplicated() {
        Expression expr1 = new GreaterThan(Source.EMPTY, attr("x", DataType.LONG), lit(1L, DataType.LONG), null);
        Expression expr2 = new LessThan(Source.EMPTY, attr("x", DataType.LONG), lit(10L, DataType.LONG), null);
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(expr1, expr2));
        assertEquals(Set.of("x"), pushed.predicateColumnNames());
    }

    public void testPredicateColumnNamesNestedAndOrNot() {
        Expression a = new GreaterThan(Source.EMPTY, attr("col_a", DataType.LONG), lit(1L, DataType.LONG), null);
        Expression b = new LessThan(Source.EMPTY, attr("col_b", DataType.LONG), lit(5L, DataType.LONG), null);
        Expression c = new Equals(Source.EMPTY, attr("col_c", DataType.LONG), lit(0L, DataType.LONG), null);
        Expression and = new And(Source.EMPTY, a, b);
        Expression not = new Not(Source.EMPTY, c);
        Expression or = new Or(Source.EMPTY, and, not);
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(or));
        assertEquals(Set.of("col_a", "col_b", "col_c"), pushed.predicateColumnNames());
    }

    public void testPredicateColumnNamesEmpty() {
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of());
        assertEquals(Set.of(), pushed.predicateColumnNames());
    }

    // --- hasYesConjunctOutsideFilterPredicate tests ---
    //
    // Important: these tests exist because the Pushability.YES promotion of WildcardLike (commit
    // that added testWildcardLikeKeywordPushedAsYes etc.) introduced a latent bug where the
    // OptimizedParquetColumnIterator's trivially-passes shortcut would silently bypass late-mat
    // for row groups whose stats prove the (non-LIKE) FilterPredicate, leaking rows that don't
    // match the LIKE conjunct (FilterExec was dropped for it). The fix relies on this method
    // returning true exactly when there is a YES conjunct outside the FilterPredicate. DO NOT
    // weaken these tests — they are the unit-level guard for the integration regression test
    // OptimizedFilteredReaderTests.testPushedExpressionsLikeWithStatsTrivialEqDoesNotLeak.

    public void testHasYesConjunctOutsideFilterPredicateLikeAlone() {
        // Bare LIKE: YES, doesn't translate. Helper must return true.
        MessageType schema = Types.buildMessage()
            .required(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("url")
            .named("schema");
        Expression like = new WildcardLike(Source.EMPTY, attr("url", DataType.KEYWORD), new WildcardPattern("*google*"));
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(like));
        assertTrue("bare LIKE is YES-eligible and untranslatable", pushed.hasYesConjunctOutsideFilterPredicate(schema));
    }

    public void testHasYesConjunctOutsideFilterPredicateLikeAndEquals() {
        // The exact realistic shape that caused the trivially-passes leak: LIKE (YES, untranslatable)
        // AND-d with a comparator (RECHECK, translatable). Helper must return true.
        MessageType schema = Types.buildMessage()
            .required(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("url")
            .required(INT64)
            .named("status")
            .named("schema");
        Expression like = new WildcardLike(Source.EMPTY, attr("url", DataType.KEYWORD), new WildcardPattern("*google*"));
        Expression statusEq = new Equals(Source.EMPTY, attr("status", DataType.LONG), lit(200L, DataType.LONG), null);
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(like, statusEq));
        assertTrue("YES LIKE is silently absent from the FilterPredicate", pushed.hasYesConjunctOutsideFilterPredicate(schema));
    }

    public void testHasYesConjunctOutsideFilterPredicateAllTranslatable() {
        // Only translatable comparators: helper must return false so the trivially-passes
        // shortcut still fires for the common single-conjunct/all-comparator case.
        MessageType schema = Types.buildMessage().required(INT64).named("status").named("schema");
        Expression statusEq = new Equals(Source.EMPTY, attr("status", DataType.LONG), lit(200L, DataType.LONG), null);
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(statusEq));
        assertFalse(
            "all-translatable filters must still benefit from the trivially-passes shortcut",
            pushed.hasYesConjunctOutsideFilterPredicate(schema)
        );
    }

    public void testHasYesConjunctOutsideFilterPredicateRecheckOnlyUntranslatable() {
        // INT96 datetime: canConvert=true but translateExpression returns null. Pushability is
        // RECHECK (not YES) because isFullyEvaluable returns false for non-LIKE comparators —
        // FilterExec re-applies it. The shortcut is therefore safe to take, so the helper must
        // return false. This is the case where translatability alone would over-reject.
        MessageType schema = Types.buildMessage().required(INT96).named("ts").named("schema");
        Expression tsEq = new Equals(Source.EMPTY, attr("ts", DataType.DATETIME), lit(1700000000000L, DataType.DATETIME), null);
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(tsEq));
        assertFalse(
            "RECHECK conjuncts that fail to translate are still safe under the shortcut " + "(FilterExec catches the over-inclusion)",
            pushed.hasYesConjunctOutsideFilterPredicate(schema)
        );
    }

    public void testHasYesConjunctOutsideFilterPredicateNotLike() {
        // Not(WildcardLike) is YES-eligible per isFullyEvaluable and untranslatable. Same trap as
        // bare LIKE: FilterExec is dropped, late-mat must run.
        MessageType schema = Types.buildMessage()
            .required(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("url")
            .named("schema");
        Expression like = new WildcardLike(Source.EMPTY, attr("url", DataType.KEYWORD), new WildcardPattern("*google*"));
        Expression notLike = new Not(Source.EMPTY, like);
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(notLike));
        assertTrue("Not(LIKE) is YES-eligible and untranslatable", pushed.hasYesConjunctOutsideFilterPredicate(schema));
    }

    public void testHasYesConjunctOutsideFilterPredicateEmpty() {
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of());
        MessageType schema = Types.buildMessage().required(INT64).named("status").named("schema");
        assertFalse("empty expression list has no YES conjuncts", pushed.hasYesConjunctOutsideFilterPredicate(schema));
    }

    // --- helpers ---

    private static Expression eq(String name, DataType type, Object value) {
        return new Equals(Source.EMPTY, attr(name, type), lit(value, type), null);
    }

    private static Attribute attr(String name, DataType type) {
        return new ReferenceAttribute(Source.EMPTY, name, type);
    }

    private static Literal lit(Object value, DataType type) {
        return new Literal(Source.EMPTY, value, type);
    }

    private static Literal datetimeLit(long millis) {
        return new Literal(Source.EMPTY, millis, DataType.DATETIME);
    }
}
