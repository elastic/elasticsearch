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
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.predicate.Range;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;

import java.time.ZoneOffset;
import java.util.List;

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
