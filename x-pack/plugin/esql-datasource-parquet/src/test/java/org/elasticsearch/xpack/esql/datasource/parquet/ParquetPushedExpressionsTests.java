/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.lucene.util.BytesRef;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
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
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;

import java.time.ZoneOffset;
import java.util.List;
import java.util.Set;

import static org.apache.parquet.schema.LogicalTypeAnnotation.dateType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.decimalType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.float16Type;
import static org.apache.parquet.schema.LogicalTypeAnnotation.timestampType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT96;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

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

    // --- Declared LONG over a TIMESTAMP column (unit mismatch) ---

    /**
     * A column DECLARED as {@code long} over a physical {@code TIMESTAMP(MICROS)} decodes through the temporal
     * path, which scales micros to epoch-nanos — so the block value is 1000x the raw value the row-group
     * statistics hold. Pushing that nanos literal against the raw micros stats prunes row groups that genuinely
     * match, losing rows (RECHECK cannot resurrect a row group that was never read). We decline instead.
     */
    public void testDeclaredLongOverTimestampMicrosDeclinesPushdown() {
        MessageType schema = Types.buildMessage()
            .required(INT64)
            .as(timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS))
            .named("ts")
            .named("test");

        // The value a declared-long read of a 1_600_000_000_000_000-micros row actually yields.
        long nanos = 1_600_000_000_000_000_000L;
        Expression expr = eq("ts", DataType.LONG, nanos);
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(expr));

        assertNull("declared long over TIMESTAMP(MICROS) must not push a raw predicate", pushed.toFilterPredicate(schema));
    }

    /** A declared {@code long} over a plain (un-annotated) INT64 still pushes — the decline is scoped to timestamps. */
    public void testDeclaredLongOverPlainInt64StillPushes() {
        MessageType schema = Types.buildMessage().required(INT64).named("n").named("test");

        Expression expr = eq("n", DataType.LONG, 42L);
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(expr));

        FilterPredicate fp = pushed.toFilterPredicate(schema);
        assertNotNull("a plain INT64 column must still push", fp);
        assertThat(fp.toString(), containsString("42"));
    }

    /** The IN path has the same unit hazard as the comparison path: a declared long over TIMESTAMP(MICROS) must decline. */
    public void testDeclaredLongInOverTimestampMicrosDeclinesPushdown() {
        MessageType schema = Types.buildMessage()
            .required(INT64)
            .as(timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS))
            .named("ts")
            .named("test");

        Expression inExpr = new In(Source.EMPTY, attr("ts", DataType.LONG), List.of(lit(1600000000000000000L, DataType.LONG)));
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(inExpr));

        assertNull("declared long IN over TIMESTAMP(MICROS) must not push a raw predicate", pushed.toFilterPredicate(schema));
    }

    /**
     * A declared {@code long} over a physical {@code DATE} (INT32, days) decodes to epoch-millis (days x 86_400_000)
     * while the stats are day-valued — same 1000x-class unit mismatch. Comparison and IN must both decline.
     */
    public void testDeclaredLongOverDateDeclinesPushdown() {
        MessageType schema = Types.buildMessage().required(INT32).as(dateType()).named("d").named("test");

        // A sub-2^31 millis literal (~1970-01-01T12:00) DOES narrow to int32, so without the guard it would push
        // `d == 43200000` against day-valued stats and mis-prune — the value must be small enough to reach the push.
        Expression cmp = eq("d", DataType.LONG, 43200000L);
        assertNull("declared long over DATE must not push", new ParquetPushedExpressions(List.of(cmp)).toFilterPredicate(schema));

        Expression inExpr = new In(Source.EMPTY, attr("d", DataType.LONG), List.of(lit(43200000L, DataType.LONG)));
        assertNull("declared long IN over DATE must not push", new ParquetPushedExpressions(List.of(inExpr)).toFilterPredicate(schema));
    }

    /**
     * A physical {@code TIME(MICROS)} column infers to {@code long} and decodes x1000 to nanos-of-day while its
     * stats stay in micros — the same unit-mismatch class, so a LONG predicate over it must decline. {@code TIME(MILLIS)}
     * stays pushable (identity widen; see {@code testTimeMillisAnalogousInt32WidenToLongIsPushed}).
     */
    public void testLongOverTimeMicrosDeclinesPushdown() {
        MessageType schema = Types.buildMessage()
            .required(INT64)
            .as(LogicalTypeAnnotation.timeType(true, LogicalTypeAnnotation.TimeUnit.MICROS))
            .named("t")
            .named("test");

        Expression cmp = eq("t", DataType.LONG, 43_200_000_000L);
        assertNull("long over TIME(MICROS) must not push", new ParquetPushedExpressions(List.of(cmp)).toFilterPredicate(schema));

        Expression inExpr = new In(Source.EMPTY, attr("t", DataType.LONG), List.of(lit(43_200_000_000L, DataType.LONG)));
        assertNull("long IN over TIME(MICROS) must not push", new ParquetPushedExpressions(List.of(inExpr)).toFilterPredicate(schema));
    }

    /**
     * Only the scaling {@code MICROS} unit needs the decline: a declared {@code long} over {@code TIMESTAMP(MILLIS)}
     * or {@code TIMESTAMP(NANOS)} is an identity read (block value == raw stat), so those still push — declining them
     * would be a needless lost-pruning cost (reviewer note on the guard's over-declining).
     */
    public void testTimestampMillisAndNanosDeclaredLongStillPush() {
        for (LogicalTypeAnnotation.TimeUnit unit : new LogicalTypeAnnotation.TimeUnit[] {
            LogicalTypeAnnotation.TimeUnit.MILLIS,
            LogicalTypeAnnotation.TimeUnit.NANOS }) {
            MessageType schema = Types.buildMessage().required(INT64).as(timestampType(true, unit)).named("t").named("test");
            FilterPredicate fp = new ParquetPushedExpressions(List.of(eq("t", DataType.LONG, 1_600_000_000_000L))).toFilterPredicate(
                schema
            );
            assertNotNull("declared long over TIMESTAMP(" + unit + ") is identity and must still push", fp);
            assertThat(fp.toString(), containsString("1600000000000"));
        }
    }

    /** A plain INT64 IN still pushes — the decline is scoped to temporal-annotated columns. */
    public void testPlainInt64InStillPushes() {
        MessageType schema = Types.buildMessage().required(INT64).named("n").named("test");

        Expression inExpr = new In(Source.EMPTY, attr("n", DataType.LONG), List.of(lit(7L, DataType.LONG), lit(9L, DataType.LONG)));
        FilterPredicate fp = new ParquetPushedExpressions(List.of(inExpr)).toFilterPredicate(schema);
        assertNotNull("a plain INT64 IN must still push", fp);
    }

    // --- Declared LONG/INTEGER over a DECIMAL(scale>0) column (scale mismatch) ---

    /**
     * A declared {@code long} over a physical {@code DECIMAL(scale=2)} INT64 decodes to {@code unscaled / 100} (a
     * double rounded to long) while the raw footer statistics hold the unscaled integer — an implicit ÷100. Pushing a
     * raw long literal against those stats mis-prunes, so comparison and IN must both decline.
     */
    public void testDeclaredLongOverDecimalScaledDeclinesPushdown() {
        MessageType schema = Types.buildMessage().required(INT64).as(decimalType(2, 18)).named("amt").named("test");

        Expression cmp = eq("amt", DataType.LONG, 12L);
        assertNull(
            "declared long over DECIMAL(scale>0) must not push",
            new ParquetPushedExpressions(List.of(cmp)).toFilterPredicate(schema)
        );

        Expression inExpr = new In(Source.EMPTY, attr("amt", DataType.LONG), List.of(lit(12L, DataType.LONG)));
        assertNull(
            "declared long IN over DECIMAL(scale>0) must not push",
            new ParquetPushedExpressions(List.of(inExpr)).toFilterPredicate(schema)
        );
    }

    /** A {@code DECIMAL(scale=0)} is an integer decode (block == raw), so the guard must NOT decline — pruning stays correct. */
    public void testDeclaredLongOverDecimalScaleZeroStillPushes() {
        MessageType schema = Types.buildMessage().required(INT64).as(decimalType(0, 18)).named("amt").named("test");

        FilterPredicate fp = new ParquetPushedExpressions(List.of(eq("amt", DataType.LONG, 12L))).toFilterPredicate(schema);
        assertNotNull("declared long over DECIMAL(scale=0) is identity and must still push", fp);
        assertThat(fp.toString(), containsString("12"));
    }

    // --- Declared INTEGER over a unit-transformed INT32 column ---

    /**
     * A declared {@code integer} over a physical {@code DATE}(INT32) column decodes days→millis (×86_400_000) then
     * narrows to int, while the stats stay day-valued — the same class of mismatch the LONG path guards. The INTEGER
     * comparison and IN arms must consult the guard and decline. A sub-2^31 millis literal is used so it would
     * otherwise reach the push.
     */
    public void testDeclaredIntegerOverDateInt32DeclinesPushdown() {
        MessageType schema = Types.buildMessage().required(INT32).as(dateType()).named("d").named("test");

        Expression cmp = eq("d", DataType.INTEGER, 86_400_000); // one day in millis, fits int32
        assertNull("declared integer over DATE must not push", new ParquetPushedExpressions(List.of(cmp)).toFilterPredicate(schema));

        Expression inExpr = new In(Source.EMPTY, attr("d", DataType.INTEGER), List.of(lit(86_400_000, DataType.INTEGER)));
        assertNull("declared integer IN over DATE must not push", new ParquetPushedExpressions(List.of(inExpr)).toFilterPredicate(schema));
    }

    /** The INTEGER decline is scoped to unit-transformed columns; a plain INT32 still pushes. */
    public void testDeclaredIntegerOverPlainInt32StillPushes() {
        MessageType schema = Types.buildMessage().required(INT32).named("n").named("test");

        FilterPredicate fp = new ParquetPushedExpressions(List.of(eq("n", DataType.INTEGER, 42))).toFilterPredicate(schema);
        assertNotNull("declared integer over a plain INT32 must still push", fp);
        assertThat(fp.toString(), containsString("42"));
    }

    /** A declared {@code integer} over a physical {@code DECIMAL(INT32, scale>0)} column decodes ÷10^scale, so it declines. */
    public void testDeclaredIntegerOverDecimalInt32ScaledDeclinesPushdown() {
        MessageType schema = Types.buildMessage().required(INT32).as(decimalType(2, 9)).named("amt").named("test");

        assertNull(
            "declared integer over DECIMAL(INT32, scale>0) must not push",
            new ParquetPushedExpressions(List.of(eq("amt", DataType.INTEGER, 12))).toFilterPredicate(schema)
        );
    }

    /** {@code IN} over a declared integer routes through translateIntIn: it declines over DATE and pushes over a plain INT32. */
    public void testDeclaredIntegerInConsultsGuard() {
        MessageType dateSchema = Types.buildMessage().required(INT32).as(dateType()).named("d").named("test");
        Expression inOverDate = new In(
            Source.EMPTY,
            attr("d", DataType.INTEGER),
            List.of(lit(86_400_000, DataType.INTEGER), lit(172_800_000, DataType.INTEGER))
        );
        assertNull(
            "declared integer IN over DATE must not push",
            new ParquetPushedExpressions(List.of(inOverDate)).toFilterPredicate(dateSchema)
        );

        MessageType plainSchema = Types.buildMessage().required(INT32).named("n").named("test");
        Expression inOverPlain = new In(
            Source.EMPTY,
            attr("n", DataType.INTEGER),
            List.of(lit(7, DataType.INTEGER), lit(9, DataType.INTEGER))
        );
        FilterPredicate fp = new ParquetPushedExpressions(List.of(inOverPlain)).toFilterPredicate(plainSchema);
        assertNotNull("declared integer IN over a plain INT32 must still push", fp);
    }

    /**
     * Direct pin for the single-home decline inventory {@link ParquetColumnDecoding#integralDecodeScalesRelativeToRawStats}
     * that {@code pushDeclinedForUnitMismatch} delegates to: it must flag exactly the annotations whose integral decode
     * scales relative to the raw footer stats, and nothing else.
     */
    public void testIntegralDecodeScalesInventory() {
        assertTrue(ParquetColumnDecoding.integralDecodeScalesRelativeToRawStats(dateType()));
        assertTrue(
            ParquetColumnDecoding.integralDecodeScalesRelativeToRawStats(timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS))
        );
        assertTrue(
            ParquetColumnDecoding.integralDecodeScalesRelativeToRawStats(
                LogicalTypeAnnotation.timeType(true, LogicalTypeAnnotation.TimeUnit.MICROS)
            )
        );
        assertTrue(ParquetColumnDecoding.integralDecodeScalesRelativeToRawStats(decimalType(2, 9)));

        assertFalse(
            ParquetColumnDecoding.integralDecodeScalesRelativeToRawStats(timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS))
        );
        assertFalse(
            ParquetColumnDecoding.integralDecodeScalesRelativeToRawStats(timestampType(true, LogicalTypeAnnotation.TimeUnit.NANOS))
        );
        assertFalse(ParquetColumnDecoding.integralDecodeScalesRelativeToRawStats(decimalType(0, 9)));
        assertFalse(ParquetColumnDecoding.integralDecodeScalesRelativeToRawStats(null));
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

    // --- DATE_NANOS pushdown (production path for timestamp[us]/[ns]) ---

    public void testToFilterPredicateDateNanosOnNanosColumnIsExact() {
        MessageType schema = Types.buildMessage()
            .required(INT64)
            .as(timestampType(true, LogicalTypeAnnotation.TimeUnit.NANOS))
            .named("ts")
            .named("test");

        long nanos = 1_700_000_000_123_456_789L;
        Expression expr = eq("ts", DataType.DATE_NANOS, nanos);
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(expr));

        FilterPredicate fp = pushed.toFilterPredicate(schema);
        assertNotNull(fp);
        assertThat(fp.toString(), containsString(String.valueOf(nanos)));
    }

    public void testToFilterPredicateDateNanosOnMicrosColumnScalesToMicros() {
        MessageType schema = Types.buildMessage()
            .required(INT64)
            .as(timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS))
            .named("ts")
            .named("test");

        long nanos = 1_700_000_000_123_456_000L; // exact multiple of 1_000 ns
        long expectedMicros = nanos / 1_000;
        Expression expr = eq("ts", DataType.DATE_NANOS, nanos);
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(expr));

        FilterPredicate fp = pushed.toFilterPredicate(schema);
        assertNotNull(fp);
        assertThat(fp.toString(), containsString(String.valueOf(expectedMicros)));
    }

    public void testToFilterPredicateDateNanosMicrosGreaterThanRoundsOutward() {
        MessageType schema = Types.buildMessage()
            .required(INT64)
            .as(timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS))
            .named("ts")
            .named("test");

        long nanos = 1_700_000_000_123_456_789L; // not a multiple of 1_000 ns
        // GT widens downward (floorDiv) so no matching micro is excluded (pushdown is RECHECK).
        long expectedMicros = Math.floorDiv(nanos, 1_000L);
        Expression expr = new GreaterThan(Source.EMPTY, attr("ts", DataType.DATE_NANOS), lit(nanos, DataType.DATE_NANOS), null);
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(expr));

        FilterPredicate fp = pushed.toFilterPredicate(schema);
        assertNotNull(fp);
        assertThat(fp.toString(), containsString(String.valueOf(expectedMicros)));
    }

    public void testToFilterPredicateDateNanosMicrosEqNonDivisibleSkipsPushdown() {
        MessageType schema = Types.buildMessage()
            .required(INT64)
            .as(timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS))
            .named("ts")
            .named("test");

        long nanos = 1_700_000_000_123_456_789L; // not a multiple of 1_000 ns: no micro equals it exactly
        Expression expr = eq("ts", DataType.DATE_NANOS, nanos);
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(expr));

        // No predicate is pushed; the scan + FilterExec recheck still yields the correct (empty) result.
        assertNull(pushed.toFilterPredicate(schema));
    }

    public void testToFilterPredicateInListOnDateNanosMicros() {
        MessageType schema = Types.buildMessage()
            .required(INT64)
            .as(timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS))
            .named("ts")
            .named("test");

        long nanos1 = 1_000_000L; // 1_000 micros
        long nanos2 = 2_000_000L; // 2_000 micros
        Expression inExpr = new In(
            Source.EMPTY,
            attr("ts", DataType.DATE_NANOS),
            List.of(lit(nanos1, DataType.DATE_NANOS), lit(nanos2, DataType.DATE_NANOS))
        );
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(inExpr));

        FilterPredicate fp = pushed.toFilterPredicate(schema);
        assertNotNull(fp);
        String repr = fp.toString();
        assertThat(repr, containsString(String.valueOf(nanos1 / 1_000)));
        assertThat(repr, containsString(String.valueOf(nanos2 / 1_000)));
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

    /**
     * A DATE column stores whole days; {@code d < <non-midnight millis>} must round the day bound UP (ceilDiv), not
     * down. With floorDiv the predicate becomes {@code day < floor(millis)} and prunes the row group holding the very
     * day the literal falls in — silent data loss. Reachable with a plain inferred date column, no declared schema.
     */
    public void testDateColumnLessThanNonMidnightRoundsBoundUp() {
        MessageType schema = Types.buildMessage().required(INT32).as(dateType()).named("d").named("test");

        long day = 19723L;
        long nonMidnight = day * ParquetPushedExpressions.MILLIS_PER_DAY + 1; // 1ms past midnight of day 19723
        Expression lt = new LessThan(Source.EMPTY, attr("d", DataType.DATETIME), datetimeLit(nonMidnight), null);

        FilterPredicate fp = new ParquetPushedExpressions(List.of(lt)).toFilterPredicate(schema);
        assertNotNull(fp);
        // ceilDiv(nonMidnight) == day + 1, so lt keeps day 19723; floorDiv would push lt(day 19723) and prune it.
        assertThat(fp.toString(), containsString(String.valueOf(day + 1)));
    }

    /**
     * {@code d != <non-midnight millis>} is true for every day (no day equals a non-midnight instant), so pushing a
     * {@code notEq(day)} — which parquet-mr uses to prune an all-that-day row group — would drop matching rows. A
     * non-midnight {@code !=} must decline pushdown (only a midnight literal is exactly representable).
     */
    public void testDateColumnNotEqualsNonMidnightDeclinesPushdown() {
        MessageType schema = Types.buildMessage().required(INT32).as(dateType()).named("d").named("test");

        long nonMidnight = 19723L * ParquetPushedExpressions.MILLIS_PER_DAY + 1;
        Expression ne = new NotEquals(Source.EMPTY, attr("d", DataType.DATETIME), datetimeLit(nonMidnight), null);

        assertNull(
            "d != <non-midnight> must not push a notEq day predicate",
            new ParquetPushedExpressions(List.of(ne)).toFilterPredicate(schema)
        );
    }

    /** {@code d <= <non-midnight>} rounds DOWN (floorDiv) — that direction is correct, so it must still push. */
    public void testDateColumnLessThanOrEqualNonMidnightStillPushes() {
        MessageType schema = Types.buildMessage().required(INT32).as(dateType()).named("d").named("test");

        long day = 19723L;
        long nonMidnight = day * ParquetPushedExpressions.MILLIS_PER_DAY + 1;
        Expression lte = new LessThanOrEqual(Source.EMPTY, attr("d", DataType.DATETIME), datetimeLit(nonMidnight), null);

        FilterPredicate fp = new ParquetPushedExpressions(List.of(lte)).toFilterPredicate(schema);
        assertNotNull(fp);
        assertThat(fp.toString(), containsString(String.valueOf(day))); // floorDiv == day 19723, correct for <=
    }

    /** {@code d == <non-midnight>} matches no day, so it must decline (only a midnight literal is exactly representable). */
    public void testDateColumnEqualsNonMidnightDeclinesPushdown() {
        MessageType schema = Types.buildMessage().required(INT32).as(dateType()).named("d").named("test");

        long nonMidnight = 19723L * ParquetPushedExpressions.MILLIS_PER_DAY + 1;
        assertNull(
            "d == <non-midnight> must not push an eq day predicate",
            new ParquetPushedExpressions(List.of(eq("d", DataType.DATETIME, nonMidnight))).toFilterPredicate(schema)
        );
    }

    /** {@code d > <non-midnight>} rounds DOWN (floorDiv) — the correct direction for {@code >} — so it still pushes. */
    public void testDateColumnGreaterThanNonMidnightStillPushes() {
        MessageType schema = Types.buildMessage().required(INT32).as(dateType()).named("d").named("test");

        long day = 19723L;
        long nonMidnight = day * ParquetPushedExpressions.MILLIS_PER_DAY + 1;
        Expression gt = new GreaterThan(Source.EMPTY, attr("d", DataType.DATETIME), datetimeLit(nonMidnight), null);

        FilterPredicate fp = new ParquetPushedExpressions(List.of(gt)).toFilterPredicate(schema);
        assertNotNull(fp);
        assertThat(fp.toString(), containsString(String.valueOf(day))); // floorDiv == day 19723, correct for >
    }

    /** {@code d >= <non-midnight>} rounds UP (ceilDiv), which only over-includes the boundary day — safe, still pushes. */
    public void testDateColumnGreaterThanOrEqualNonMidnightStillPushes() {
        MessageType schema = Types.buildMessage().required(INT32).as(dateType()).named("d").named("test");

        long day = 19723L;
        long nonMidnight = day * ParquetPushedExpressions.MILLIS_PER_DAY + 1;
        Expression gte = new GreaterThanOrEqual(Source.EMPTY, attr("d", DataType.DATETIME), datetimeLit(nonMidnight), null);

        FilterPredicate fp = new ParquetPushedExpressions(List.of(gte)).toFilterPredicate(schema);
        assertNotNull(fp);
        assertThat(fp.toString(), containsString(String.valueOf(day + 1))); // ceilDiv == day 19724
    }

    /** {@code d == <midnight>} is exactly representable and must still push the eq day predicate. */
    public void testDateColumnEqualsMidnightStillPushes() {
        MessageType schema = Types.buildMessage().required(INT32).as(dateType()).named("d").named("test");

        long day = 19723L;
        long midnight = day * ParquetPushedExpressions.MILLIS_PER_DAY;
        FilterPredicate fp = new ParquetPushedExpressions(List.of(eq("d", DataType.DATETIME, midnight))).toFilterPredicate(schema);
        assertNotNull("d == <midnight> is exact and must still push", fp);
        assertThat(fp.toString(), containsString(String.valueOf(day)));
    }

    /** {@code d IN (midnight, non-midnight)} keeps only the midnight day; the non-midnight literal can equal no day. */
    public void testDateColumnInDropsNonMidnightElement() {
        MessageType schema = Types.buildMessage().required(INT32).as(dateType()).named("d").named("test");

        long midnightDay = 19723L;
        long midnight = midnightDay * ParquetPushedExpressions.MILLIS_PER_DAY;
        long nonMidnight = 20000L * ParquetPushedExpressions.MILLIS_PER_DAY + 5;
        Expression in = new In(Source.EMPTY, attr("d", DataType.DATETIME), List.of(datetimeLit(midnight), datetimeLit(nonMidnight)));

        FilterPredicate fp = new ParquetPushedExpressions(List.of(in)).toFilterPredicate(schema);
        assertNotNull(fp);
        String repr = fp.toString();
        assertThat("the midnight day is pushed", repr, containsString(String.valueOf(midnightDay)));
        assertThat("the non-midnight day must be dropped, not floorDiv'd in", repr, not(containsString(String.valueOf(20000L))));
    }

    // --- Declared LONG over an UNSIGNED_64 column (signed/unsigned comparator mismatch) ---

    /**
     * A {@code uint64} column DECLARED as {@code long} decodes via signed sign-wrap (raws >= 2^63 read as negative),
     * so signed-block ordering disagrees with parquet-mr's UNSIGNED row-group comparator for EVERY ordered op and
     * either literal sign. {@code u < 100} would prune the negative-block groups (large unsigned) that genuinely match
     * (a false negative RECHECK cannot undo); {@code u > 5}, though over-including on its own, mis-prunes once wrapped
     * in {@code NOT}. So every ordered comparison must decline, whatever the literal sign.
     */
    public void testDeclaredLongOverUnsignedInt64OrderedDeclinesPushdown() {
        MessageType schema = Types.buildMessage().required(INT64).as(LogicalTypeAnnotation.intType(64, false)).named("u").named("test");

        for (long literal : new long[] { -5L, 100L }) {
            Expression gt = new GreaterThan(Source.EMPTY, attr("u", DataType.LONG), lit(literal, DataType.LONG), null);
            assertNull(
                "u > " + literal + " over uint64 must not push",
                new ParquetPushedExpressions(List.of(gt)).toFilterPredicate(schema)
            );

            Expression lt = new LessThan(Source.EMPTY, attr("u", DataType.LONG), lit(literal, DataType.LONG), null);
            assertNull(
                "u < " + literal + " over uint64 must not push",
                new ParquetPushedExpressions(List.of(lt)).toFilterPredicate(schema)
            );

            Expression lte = new LessThanOrEqual(Source.EMPTY, attr("u", DataType.LONG), lit(literal, DataType.LONG), null);
            assertNull(
                "u <= " + literal + " over uint64 must not push",
                new ParquetPushedExpressions(List.of(lte)).toFilterPredicate(schema)
            );
        }
    }

    /** {@code eq}/{@code notEq} are bit-pattern exact regardless of sign, so an equality literal over uint64 still pushes. */
    public void testDeclaredLongOverUnsignedInt64EqStillPushes() {
        MessageType schema = Types.buildMessage().required(INT64).as(LogicalTypeAnnotation.intType(64, false)).named("u").named("test");

        assertNotNull(
            "u == -5 over uint64 is bit-exact and must still push",
            new ParquetPushedExpressions(List.of(eq("u", DataType.LONG, -5L))).toFilterPredicate(schema)
        );
        assertNotNull(
            "u == 5 over uint64 is bit-exact and must still push",
            new ParquetPushedExpressions(List.of(eq("u", DataType.LONG, 5L))).toFilterPredicate(schema)
        );
    }

    /** A plain (signed) INT64 still pushes ordered comparisons — the decline is scoped to the unsigned annotation. */
    public void testDeclaredLongOverSignedInt64OrderedStillPushes() {
        MessageType schema = Types.buildMessage().required(INT64).named("n").named("test");

        Expression lt = new LessThan(Source.EMPTY, attr("n", DataType.LONG), lit(100L, DataType.LONG), null);
        FilterPredicate fp = new ParquetPushedExpressions(List.of(lt)).toFilterPredicate(schema);
        assertNotNull("a plain signed INT64 must still push ordered comparisons", fp);
        assertThat(fp.toString(), containsString("100"));
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

    // --- Declared-retype physical-type guard (F-PUSH-KW) ---

    /**
     * A declared retype ({@code keyword}/{@code integer}/{@code boolean} over a physical {@code
     * INT64}) is a supported coercion, so the predicate reaches the INTEGER/KEYWORD/BOOLEAN arms.
     * Without the physical-type guard those arms mint a BINARY/INT32/BOOLEAN predicate against an
     * INT64 column — a declared-type mismatch parquet-mr rejects or mis-prunes. They must decline
     * (the conjunct stays RECHECK, {@code FilterExec} re-applies the real semantics).
     */
    public void testDeclaredRetypeOverInt64DeclinesPushdown() {
        MessageType schema = Types.buildMessage().required(INT64).named("code").named("test");

        assertNull(
            "keyword over int64 declines",
            new ParquetPushedExpressions(List.of(eq("code", DataType.KEYWORD, new BytesRef("42")))).toFilterPredicate(schema)
        );
        assertNull(
            "integer over int64 declines",
            new ParquetPushedExpressions(List.of(eq("code", DataType.INTEGER, 42))).toFilterPredicate(schema)
        );
        assertNull(
            "boolean over int64 declines",
            new ParquetPushedExpressions(List.of(eq("code", DataType.BOOLEAN, true))).toFilterPredicate(schema)
        );
        assertNull(
            "keyword IN over int64 declines",
            new ParquetPushedExpressions(
                List.of(new In(Source.EMPTY, attr("code", DataType.KEYWORD), List.of(lit(new BytesRef("42"), DataType.KEYWORD))))
            ).toFilterPredicate(schema)
        );
        assertNull(
            "STARTS_WITH over int64 declines",
            new ParquetPushedExpressions(
                List.of(new StartsWith(Source.EMPTY, attr("code", DataType.KEYWORD), lit(new BytesRef("4"), DataType.KEYWORD)))
            ).toFilterPredicate(schema)
        );
    }

    /**
     * Positive control for the guard: a genuine keyword/boolean column (physical BINARY/BOOLEAN)
     * still pushes — the guard passes on a matching physical, so no pushdown is lost.
     */
    public void testGenuineKeywordAndBooleanStillPush() {
        MessageType kwSchema = Types.buildMessage()
            .required(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("s")
            .named("test");
        assertNotNull(
            "keyword over BINARY still pushes",
            new ParquetPushedExpressions(List.of(eq("s", DataType.KEYWORD, new BytesRef("x")))).toFilterPredicate(kwSchema)
        );

        MessageType boolSchema = Types.buildMessage()
            .required(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN)
            .named("b")
            .named("test");
        assertNotNull(
            "boolean over BOOLEAN still pushes",
            new ParquetPushedExpressions(List.of(eq("b", DataType.BOOLEAN, true))).toFilterPredicate(boolSchema)
        );
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

    public void testHasYesConjunctOutsideFilterPredicateBareStartsWith() {
        // Bare StartsWith is YES-eligible AND translates to a prefix-range FilterPredicate, so
        // it is NOT "outside" the predicate — the shortcut may still fire.
        MessageType schema = Types.buildMessage()
            .required(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("url")
            .named("schema");
        Expression sw = new StartsWith(Source.EMPTY, attr("url", DataType.KEYWORD), lit(new BytesRef("https://"), DataType.KEYWORD));
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(sw));
        assertFalse("bare StartsWith translates to a range FilterPredicate", pushed.hasYesConjunctOutsideFilterPredicate(schema));
    }

    public void testHasYesConjunctOutsideFilterPredicateNotStartsWith() {
        // Not(StartsWith) is YES-eligible and exactly translatable (StartsWith is a leaf with a
        // pure prefix-range FilterPredicate), so it pushes as FilterApi.not(range) and the
        // trivially-passes shortcut stays enabled.
        MessageType schema = Types.buildMessage()
            .required(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("url")
            .named("schema");
        Expression sw = new StartsWith(Source.EMPTY, attr("url", DataType.KEYWORD), lit(new BytesRef("https://"), DataType.KEYWORD));
        Expression notSw = new Not(Source.EMPTY, sw);
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(notSw));
        assertFalse(
            "Not(StartsWith) is exactly translatable (pure leaf, no silent-drop hazard)",
            pushed.hasYesConjunctOutsideFilterPredicate(schema)
        );
    }

    public void testNotStartsWithTranslatesToFilterPredicate() {
        // Asserts the actual FilterPredicate emitted for Not(StartsWith) is non-null and is a
        // Not over the StartsWith range. Apache-mr internally expands NOT(range) into the
        // canonical OR-of-comparators when applying row-group / page stats.
        MessageType schema = Types.buildMessage()
            .required(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("url")
            .named("schema");
        Expression sw = new StartsWith(Source.EMPTY, attr("url", DataType.KEYWORD), lit(new BytesRef("/admin/"), DataType.KEYWORD));
        Expression notSw = new Not(Source.EMPTY, sw);
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(notSw));
        FilterPredicate fp = pushed.toFilterPredicate(schema);
        assertNotNull("Not(StartsWith) must translate so row-group pruning can apply", fp);
        assertThat(fp.toString(), containsString("not("));
    }

    // -----------------------------------------------------------------------------------
    // DOUBLE pushdown is unsafe when the physical column is NOT a native FLOAT/DOUBLE:
    // parquet-mr's SchemaCompatibilityValidator rejects a doubleColumn predicate against an
    // INT32/INT64/FIXED_LEN_BYTE_ARRAY/BINARY physical column at RowGroupFilter time (throws
    // IllegalArgumentException), so any DECIMAL- or Float16-encoded column read as ESQL DOUBLE
    // would crash a stats-based read. Build*Predicate / translateIn must peek at the file
    // schema and refuse to translate those shapes, leaving the row to RECHECK / late-mat.
    // These tests fail today (the predicate is built unconditionally) and pass after the fix.
    // -----------------------------------------------------------------------------------

    public void testDoubleEqAgainstDecimalInt32IsNotPushed() {
        MessageType schema = Types.buildMessage().required(INT32).as(decimalType(2, 9)).named("price").named("test");
        Expression expr = eq("price", DataType.DOUBLE, 100.0);
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(expr));

        assertNull("DOUBLE predicate against INT32+DECIMAL must be suppressed", pushed.toFilterPredicate(schema));
    }

    public void testDoubleEqAgainstDecimalInt64IsNotPushed() {
        MessageType schema = Types.buildMessage().required(INT64).as(decimalType(2, 18)).named("price").named("test");
        Expression expr = eq("price", DataType.DOUBLE, 100.0);
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(expr));

        assertNull("DOUBLE predicate against INT64+DECIMAL must be suppressed", pushed.toFilterPredicate(schema));
    }

    public void testDoubleEqAgainstDecimalFixedLenBinaryIsNotPushed() {
        MessageType schema = Types.buildMessage()
            .required(FIXED_LEN_BYTE_ARRAY)
            .length(16)
            .as(decimalType(2, 30))
            .named("price")
            .named("test");
        Expression expr = eq("price", DataType.DOUBLE, 100.0);
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(expr));

        assertNull("DOUBLE predicate against FIXED_LEN_BYTE_ARRAY+DECIMAL must be suppressed", pushed.toFilterPredicate(schema));
    }

    public void testDoubleEqAgainstFloatIsNotPushed() {
        MessageType schema = Types.buildMessage().required(FLOAT).named("ratio").named("test");
        Expression expr = eq("ratio", DataType.DOUBLE, 0.5);
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(expr));

        assertNull("DOUBLE predicate against FLOAT physical must be suppressed", pushed.toFilterPredicate(schema));
    }

    public void testDoubleEqAgainstFloat16IsNotPushed() {
        MessageType schema = Types.buildMessage().required(FIXED_LEN_BYTE_ARRAY).length(2).as(float16Type()).named("ratio").named("test");
        Expression expr = eq("ratio", DataType.DOUBLE, 0.5);
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(expr));

        assertNull("DOUBLE predicate against FIXED_LEN_BYTE_ARRAY(2)+Float16 must be suppressed", pushed.toFilterPredicate(schema));
    }

    public void testDoubleInAgainstDecimalInt32IsNotPushed() {
        MessageType schema = Types.buildMessage().required(INT32).as(decimalType(2, 9)).named("price").named("test");
        Expression inExpr = new In(
            Source.EMPTY,
            attr("price", DataType.DOUBLE),
            List.of(lit(100.0, DataType.DOUBLE), lit(200.0, DataType.DOUBLE))
        );
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(inExpr));

        assertNull("IN over DOUBLE against INT32+DECIMAL must be suppressed", pushed.toFilterPredicate(schema));
    }

    public void testDoubleRangeAgainstDecimalInt32IsNotPushed() {
        // Range routes through buildPredicate twice (lower + upper); both must return null so
        // the And built around them collapses and the whole Range is suppressed.
        MessageType schema = Types.buildMessage().required(INT32).as(decimalType(2, 9)).named("price").named("test");
        Expression range = new Range(
            Source.EMPTY,
            attr("price", DataType.DOUBLE),
            lit(10.0, DataType.DOUBLE),
            true,
            lit(100.0, DataType.DOUBLE),
            true,
            ZoneOffset.UTC
        );
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(range));

        assertNull("Range over DOUBLE against INT32+DECIMAL must be suppressed", pushed.toFilterPredicate(schema));
    }

    public void testDoubleEqAgainstNativeDoubleColumnIsPushed() {
        // No-regression: native DOUBLE physical must keep going through pushdown unchanged.
        MessageType schema = Types.buildMessage().required(DOUBLE).named("score").named("test");
        Expression expr = eq("score", DataType.DOUBLE, 0.5);
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(expr));

        FilterPredicate fp = pushed.toFilterPredicate(schema);
        assertNotNull(fp);
        assertThat(fp.toString(), containsString("score"));
    }

    // -----------------------------------------------------------------------------------
    // esql-planning#1030: a Parquet UINT_32 column (physical INT32) widens to ESQL LONG
    // because unsigned 32-bit values can exceed signed int range. Pushing a longColumn
    // predicate against it makes parquet-mr reject the predicate as a declared-type
    // mismatch against the file's INT32 schema — every comparator on such a column 500s.
    // The fix dispatches on the physical primitive (buildLongPredicate / translateLongIn),
    // narrowing the literal to int when it is safely representable and skipping pushdown
    // (returning null, safe because LONG comparisons are always RECHECK) otherwise.
    // -----------------------------------------------------------------------------------

    private static MessageType uint32Schema() {
        return Types.buildMessage().required(INT32).as(LogicalTypeAnnotation.intType(32, false)).named("u32").named("test");
    }

    public void testUint32GreaterThanPushesAsIntColumn() {
        // The issue's exact reproducer: FROM <dataset> | WHERE u32 > 100000 | STATS COUNT(*).
        MessageType schema = uint32Schema();
        Expression expr = new GreaterThan(Source.EMPTY, attr("u32", DataType.LONG), lit(100_000L, DataType.LONG), null);
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(expr));

        FilterPredicate fp = pushed.toFilterPredicate(schema);
        assertNotNull("uint32 > k must still push (as an intColumn, not a longColumn)", fp);
        assertThat(fp.toString(), containsString("u32"));
        assertThat(fp.toString(), containsString("100000"));
    }

    public void testUint32EqualsPushesAsIntColumn() {
        MessageType schema = uint32Schema();
        // A value that overflows signed int32 (matches the issue's 3,000,000,000-style example);
        // (int) 3_000_000_000L == -1_294_967_296, the raw bit pattern the file actually stores.
        long value = 3_000_000_000L;
        Expression expr = eq("u32", DataType.LONG, value);
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(expr));

        FilterPredicate fp = pushed.toFilterPredicate(schema);
        assertNotNull("uint32 == k (k > Integer.MAX_VALUE) must still push", fp);
        assertThat(fp.toString(), containsString(String.valueOf((int) value)));
    }

    public void testUint32InPushesAsIntColumn() {
        MessageType schema = uint32Schema();
        Expression inExpr = new In(
            Source.EMPTY,
            attr("u32", DataType.LONG),
            List.of(lit(100_000L, DataType.LONG), lit(3_000_000_000L, DataType.LONG))
        );
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(inExpr));

        FilterPredicate fp = pushed.toFilterPredicate(schema);
        assertNotNull("uint32 IN (...) must still push", fp);
        String repr = fp.toString();
        assertThat(repr, containsString("in("));
        assertThat(repr, containsString("100000"));
        assertThat(repr, containsString(String.valueOf((int) 3_000_000_000L)));
    }

    public void testUint32CombinedAndPushesAsIntColumn() {
        // Matches the issue's "combined (> AND >)" reproduction shape.
        MessageType schema = Types.buildMessage()
            .required(INT32)
            .as(LogicalTypeAnnotation.intType(32, false))
            .named("u32")
            .required(INT32)
            .as(LogicalTypeAnnotation.intType(16, false))
            .named("u16")
            .named("test");
        Expression u32Gt = new GreaterThan(Source.EMPTY, attr("u32", DataType.LONG), lit(100_000L, DataType.LONG), null);
        Expression u16Gt = new GreaterThan(Source.EMPTY, attr("u16", DataType.INTEGER), lit(100, DataType.INTEGER), null);
        Expression and = new And(Source.EMPTY, u32Gt, u16Gt);
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(and));

        FilterPredicate fp = pushed.toFilterPredicate(schema);
        assertNotNull("uint32 > k AND uint16 > k must still push", fp);
        String repr = fp.toString();
        assertThat(repr, containsString("u32"));
        assertThat(repr, containsString("u16"));
        assertThat(repr, containsString("and("));
    }

    public void testUint32RangePushesAsIntColumn() {
        MessageType schema = uint32Schema();
        Expression range = new Range(
            Source.EMPTY,
            attr("u32", DataType.LONG),
            lit(100_000L, DataType.LONG),
            true,
            lit(200_000L, DataType.LONG),
            true,
            ZoneOffset.UTC
        );
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(range));

        FilterPredicate fp = pushed.toFilterPredicate(schema);
        assertNotNull("Range over uint32 must still push", fp);
        String repr = fp.toString();
        assertThat(repr, containsString("100000"));
        assertThat(repr, containsString("200000"));
    }

    public void testUint32LiteralAboveUnsignedRangeIsNotPushed() {
        // No 32-bit value (signed or unsigned) can ever equal a literal beyond 2^32-1; the
        // literal cannot be safely narrowed, so pushdown is skipped rather than mistranslated.
        // Correctness is preserved regardless — LONG comparators are always RECHECK.
        MessageType schema = uint32Schema();
        Expression expr = eq("u32", DataType.LONG, 0xFFFFFFFFL + 1);
        assertNull(
            "out-of-unsigned-range literal must not be pushed",
            new ParquetPushedExpressions(List.of(expr)).toFilterPredicate(schema)
        );
    }

    public void testUint32NegativeLiteralIsNotPushed() {
        MessageType schema = uint32Schema();
        Expression expr = new GreaterThan(Source.EMPTY, attr("u32", DataType.LONG), lit(-5L, DataType.LONG), null);
        assertNull(
            "negative literal against an unsigned column must not be pushed",
            new ParquetPushedExpressions(List.of(expr)).toFilterPredicate(schema)
        );
    }

    public void testUint32InDropsOutOfRangeLiteralButKeepsInRangeOnes() {
        // One literal is unrepresentable (dropped) but the other is in range: the narrowed IN
        // predicate must still push for the representable value rather than aborting entirely.
        MessageType schema = uint32Schema();
        Expression inExpr = new In(
            Source.EMPTY,
            attr("u32", DataType.LONG),
            List.of(lit(100_000L, DataType.LONG), lit(-1L, DataType.LONG))
        );
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(inExpr));

        FilterPredicate fp = pushed.toFilterPredicate(schema);
        assertNotNull("the in-range literal must still push even though -1 is dropped", fp);
        assertThat(fp.toString(), containsString("100000"));
    }

    public void testTimeMillisAnalogousInt32WidenToLongIsPushed() {
        // TIME_MILLIS is the other Parquet shape that is physical INT32 but widens to ESQL
        // LONG (ESQL has no distinct "time of day" type). Values are always small and signed,
        // so the plain signed round trip in narrowLongToPhysicalInt32 applies.
        MessageType schema = Types.buildMessage()
            .required(INT32)
            .as(LogicalTypeAnnotation.timeType(true, LogicalTypeAnnotation.TimeUnit.MILLIS))
            .named("tod")
            .named("test");
        long millisOfDay = 3_600_000L; // 01:00:00.000
        Expression expr = eq("tod", DataType.LONG, millisOfDay);
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(expr));

        FilterPredicate fp = pushed.toFilterPredicate(schema);
        assertNotNull("TIME_MILLIS (INT32 physical, LONG ESQL type) must still push", fp);
        assertThat(fp.toString(), containsString(String.valueOf(millisOfDay)));
    }

    public void testLongEqAgainstNativeInt64ColumnIsPushedUnchanged() {
        // No-regression control: a real 64-bit LONG column (no widening) must keep pushing
        // as a longColumn exactly as before.
        MessageType schema = Types.buildMessage().required(INT64).named("counter").named("test");
        long value = 9_000_000_000L; // exceeds int32 range entirely — only valid as a real long
        Expression expr = eq("counter", DataType.LONG, value);
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(expr));

        FilterPredicate fp = pushed.toFilterPredicate(schema);
        assertNotNull(fp);
        assertThat(fp.toString(), containsString(String.valueOf(value)));
    }

    public void testToFilterPredicateLongMissingPathReturnsNull() {
        // LONG now also resolves the physical primitive via resolveNestedPrimitive (esql-planning#1030);
        // a missing/unresolvable path must return null just like the DATETIME case above.
        MessageType schema = uint32Schema();
        Expression expr = eq("does_not_exist", DataType.LONG, 100_000L);
        assertNull(new ParquetPushedExpressions(List.of(expr)).toFilterPredicate(schema));
    }

    // -----------------------------------------------------------------------------------
    // Nested STRUCT pushdown — dotted column names (e.g. event.action) flow through the
    // same FilterPredicate translation as top-level names. The resolveNestedPrimitive
    // helper walks the dotted path; FilterApi.binaryColumn / intColumn / longColumn etc.
    // internally store the name as a multi-segment ColumnPath via ColumnPath.fromDotString,
    // which is then compared against the multi-segment ColumnDescriptor.getPath() at row
    // group filter time — so the dotted name must reappear verbatim in the predicate's
    // toString. Per the prior PR's D2 rule, a literal top-level field literally named
    // "event.action" wins over a nested resolution of the same dotted path.
    // -----------------------------------------------------------------------------------
    // Nested STRUCT type-shape helpers: each builds a single event-group schema with the
    // requested leaf primitive and logical-type annotation.

    private static MessageType nestedTimestamp(LogicalTypeAnnotation.TimeUnit unit) {
        return Types.buildMessage().requiredGroup().required(INT64).as(timestampType(true, unit)).named("ts").named("event").named("test");
    }

    private static MessageType nestedDate() {
        return Types.buildMessage().requiredGroup().required(INT32).as(dateType()).named("d").named("event").named("test");
    }

    private static MessageType nestedKeyword() {
        return Types.buildMessage()
            .requiredGroup()
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("action")
            .named("event")
            .named("test");
    }

    public void testToFilterPredicateNestedTimestampMillis() {
        MessageType schema = nestedTimestamp(LogicalTypeAnnotation.TimeUnit.MILLIS);
        long millis = 1700000000000L;
        Expression expr = eq("event.ts", DataType.DATETIME, millis);
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(expr));

        FilterPredicate fp = pushed.toFilterPredicate(schema);
        assertNotNull(fp);
        String repr = fp.toString();
        assertThat(repr, containsString("event.ts"));
        assertThat(repr, containsString(String.valueOf(millis)));
    }

    public void testToFilterPredicateNestedTimestampMicros() {
        MessageType schema = nestedTimestamp(LogicalTypeAnnotation.TimeUnit.MICROS);
        long millis = 1700000000000L;
        Expression expr = eq("event.ts", DataType.DATETIME, millis);

        FilterPredicate fp = new ParquetPushedExpressions(List.of(expr)).toFilterPredicate(schema);
        assertNotNull(fp);
        assertThat(fp.toString(), containsString("event.ts"));
        assertThat(fp.toString(), containsString(String.valueOf(millis * 1000)));
    }

    public void testToFilterPredicateNestedTimestampNanos() {
        MessageType schema = nestedTimestamp(LogicalTypeAnnotation.TimeUnit.NANOS);
        long millis = 1700000000000L;
        Expression expr = eq("event.ts", DataType.DATETIME, millis);

        FilterPredicate fp = new ParquetPushedExpressions(List.of(expr)).toFilterPredicate(schema);
        assertNotNull(fp);
        assertThat(fp.toString(), containsString("event.ts"));
        assertThat(fp.toString(), containsString(String.valueOf(millis * 1_000_000L)));
    }

    public void testToFilterPredicateNestedDate() {
        MessageType schema = nestedDate();
        long millis = 86400000L * 19723;
        int expectedDays = (int) (millis / ParquetPushedExpressions.MILLIS_PER_DAY);
        Expression expr = eq("event.d", DataType.DATETIME, millis);

        FilterPredicate fp = new ParquetPushedExpressions(List.of(expr)).toFilterPredicate(schema);
        assertNotNull(fp);
        assertThat(fp.toString(), containsString("event.d"));
        assertThat(fp.toString(), containsString(String.valueOf(expectedDays)));
    }

    public void testToFilterPredicateNestedKeyword() {
        MessageType schema = nestedKeyword();
        Expression expr = eq("event.action", DataType.KEYWORD, new BytesRef("login"));

        FilterPredicate fp = new ParquetPushedExpressions(List.of(expr)).toFilterPredicate(schema);
        assertNotNull(fp);
        // parquet-mr renders the column path with dots in toString — verify the dotted name
        // round-trips end-to-end through FilterApi.binaryColumn -> ColumnPath.fromDotString.
        // Note: the BytesRef literal is rendered as raw byte values (Binary{N constant bytes,
        // [108, ...]}) rather than the ASCII text, so we cannot assert on "login" here.
        assertThat(fp.toString(), containsString("event.action"));
    }

    public void testToFilterPredicateNestedLongRange() {
        MessageType schema = Types.buildMessage().requiredGroup().required(INT64).named("id").named("event").named("test");
        Expression range = new Range(
            Source.EMPTY,
            attr("event.id", DataType.LONG),
            lit(10L, DataType.LONG),
            true,
            lit(99L, DataType.LONG),
            true,
            ZoneOffset.UTC
        );

        FilterPredicate fp = new ParquetPushedExpressions(List.of(range)).toFilterPredicate(schema);
        assertNotNull(fp);
        assertThat(fp.toString(), containsString("event.id"));
    }

    public void testToFilterPredicateNestedDoubleSkippedForNonDouble() {
        // Nested physical INT64+DECIMAL is read as ESQL DOUBLE but parquet-mr rejects a
        // doubleColumn predicate against an INT64 — must skip pushdown, mirror of top-level
        // testDoubleEqAgainstDecimalInt64IsNotPushed.
        MessageType schema = Types.buildMessage()
            .requiredGroup()
            .required(INT64)
            .as(decimalType(2, 18))
            .named("price")
            .named("event")
            .named("test");
        Expression expr = eq("event.price", DataType.DOUBLE, 100.0);
        assertNull(new ParquetPushedExpressions(List.of(expr)).toFilterPredicate(schema));
    }

    public void testToFilterPredicateNestedDoublePushed() {
        // Native DOUBLE under a struct must keep going through pushdown unchanged.
        MessageType schema = Types.buildMessage().requiredGroup().required(DOUBLE).named("score").named("event").named("test");
        Expression expr = eq("event.score", DataType.DOUBLE, 0.5);

        FilterPredicate fp = new ParquetPushedExpressions(List.of(expr)).toFilterPredicate(schema);
        assertNotNull(fp);
        assertThat(fp.toString(), containsString("event.score"));
    }

    public void testToFilterPredicateNestedIsNull() {
        MessageType schema = nestedKeyword();
        Expression expr = new IsNull(Source.EMPTY, attr("event.action", DataType.KEYWORD));

        FilterPredicate fp = new ParquetPushedExpressions(List.of(expr)).toFilterPredicate(schema);
        assertNotNull(fp);
        // IsNull lowers to eq(col, null). The dotted name must still appear.
        assertThat(fp.toString(), containsString("event.action"));
        assertThat(fp.toString(), containsString("null"));
    }

    public void testToFilterPredicateNestedIsNotNull() {
        MessageType schema = nestedKeyword();
        Expression expr = new IsNotNull(Source.EMPTY, attr("event.action", DataType.KEYWORD));

        FilterPredicate fp = new ParquetPushedExpressions(List.of(expr)).toFilterPredicate(schema);
        assertNotNull(fp);
        assertThat(fp.toString(), containsString("event.action"));
    }

    public void testToFilterPredicateNestedInList() {
        MessageType schema = nestedKeyword();
        Expression inExpr = new In(
            Source.EMPTY,
            attr("event.action", DataType.KEYWORD),
            List.of(lit(new BytesRef("login"), DataType.KEYWORD), lit(new BytesRef("logout"), DataType.KEYWORD))
        );

        FilterPredicate fp = new ParquetPushedExpressions(List.of(inExpr)).toFilterPredicate(schema);
        assertNotNull(fp);
        String repr = fp.toString();
        assertThat(repr, containsString("in("));
        assertThat(repr, containsString("event.action"));
    }

    public void testToFilterPredicateNestedAnd() {
        // Two conjuncts sharing the same parent path AND one top-level conjunct — verifies
        // the dotted path resolves correctly per-leaf, alongside an exact top-level name.
        MessageType schema = Types.buildMessage()
            .required(INT64)
            .named("id")
            .requiredGroup()
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("action")
            .required(INT64)
            .named("ts")
            .named("event")
            .named("test");

        Expression action = eq("event.action", DataType.KEYWORD, new BytesRef("login"));
        Expression ts = new GreaterThan(Source.EMPTY, attr("event.ts", DataType.LONG), lit(0L, DataType.LONG), null);
        Expression id = eq("id", DataType.LONG, 1L);
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(action, ts, id));

        FilterPredicate fp = pushed.toFilterPredicate(schema);
        assertNotNull(fp);
        String repr = fp.toString();
        assertThat(repr, containsString("event.action"));
        assertThat(repr, containsString("event.ts"));
        assertThat(repr, containsString("id"));
    }

    public void testToFilterPredicateNestedOrAndNot() {
        MessageType schema = nestedKeyword();
        Expression eq1 = eq("event.action", DataType.KEYWORD, new BytesRef("login"));
        Expression eq2 = eq("event.action", DataType.KEYWORD, new BytesRef("logout"));
        Expression or = new Or(Source.EMPTY, eq1, eq2);
        Expression notEq = new Not(
            Source.EMPTY,
            new NotEquals(Source.EMPTY, attr("event.action", DataType.KEYWORD), lit(new BytesRef("admin"), DataType.KEYWORD), null)
        );
        Expression and = new And(Source.EMPTY, or, notEq);

        FilterPredicate fp = new ParquetPushedExpressions(List.of(and)).toFilterPredicate(schema);
        assertNotNull(fp);
        String repr = fp.toString();
        // The dotted name appears on every leaf; verify And/Or/Not nesting all preserve it.
        assertThat(repr, containsString("event.action"));
        assertThat(repr, containsString("and("));
        assertThat(repr, containsString("or("));
        assertThat(repr, containsString("not("));
    }

    public void testToFilterPredicateNestedDeepPath() {
        // Four-level path a.b.c.d — verifies the walker does not stop after the first dot.
        MessageType schema = Types.buildMessage()
            .requiredGroup()
            .requiredGroup()
            .requiredGroup()
            .required(INT64)
            .named("d")
            .named("c")
            .named("b")
            .named("a")
            .named("test");
        Expression expr = eq("a.b.c.d", DataType.LONG, 42L);

        FilterPredicate fp = new ParquetPushedExpressions(List.of(expr)).toFilterPredicate(schema);
        assertNotNull(fp);
        assertThat(fp.toString(), containsString("a.b.c.d"));
        assertThat(fp.toString(), containsString("42"));
    }

    public void testToFilterPredicateNestedPathMissingReturnsNull() {
        // Schema has event.ts (DATETIME), but predicate references event.does_not_exist; the
        // DATETIME path resolves the leaf via resolveNestedPrimitive and returns null when
        // missing. KEYWORD/INT predicates bypass schema resolution and forward the dotted name
        // straight to FilterApi.binaryColumn — parquet-mr's RowGroupFilter tolerates unknown
        // columns at filter time, so they are not the right shape for testing the resolver.
        // DATETIME exercises the resolver because it has to map epoch millis to the correct
        // physical encoding. LONG also resolves via resolveNestedPrimitive since
        // ParquetPushedExpressions.buildLongPredicate (esql-planning#1030) — see
        // testToFilterPredicateLongMissingPathReturnsNull below for that case.
        MessageType schema = nestedTimestamp(LogicalTypeAnnotation.TimeUnit.MILLIS);
        Expression expr = eq("event.does_not_exist", DataType.DATETIME, 1700000000000L);
        assertNull(new ParquetPushedExpressions(List.of(expr)).toFilterPredicate(schema));
    }

    public void testToFilterPredicateNestedPathLandsOnGroupReturnsNull() {
        // The name resolves to an intermediate group ("event") rather than a primitive leaf
        // — for DATETIME (which routes through resolveNestedPrimitive), the translator must
        // return null since pushdown is meaningless against a group.
        MessageType schema = nestedTimestamp(LogicalTypeAnnotation.TimeUnit.MILLIS);
        Expression expr = eq("event", DataType.DATETIME, 1700000000000L);
        assertNull(new ParquetPushedExpressions(List.of(expr)).toFilterPredicate(schema));
    }

    public void testToFilterPredicateLiteralDottedNameWinsOverPath() {
        // Schema has BOTH a literal top-level "event.action" and a nested struct event.action.
        // Per the prior PR's D2 rule (preserved by resolveNestedPrimitive), the literal wins —
        // the predicate type-shape is decided from the literal's primitive (BINARY here), not
        // from the nested INT64 "event.action" leaf.
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("event.action")
            .requiredGroup()
            .required(INT64)
            .named("action")
            .named("event")
            .named("test");

        // KEYWORD predicate succeeds because the literal field's primitive is BINARY.
        Expression keywordEq = eq("event.action", DataType.KEYWORD, new BytesRef("login"));
        FilterPredicate keywordPred = new ParquetPushedExpressions(List.of(keywordEq)).toFilterPredicate(schema);
        assertNotNull("literal BINARY 'event.action' wins → KEYWORD pushdown valid", keywordPred);
        assertThat(keywordPred.toString(), containsString("event.action"));
    }

    public void testResolveNestedPrimitiveLiteralWinsOverPath() {
        // Directly assert the helper's D2 contract.
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("event.action")
            .requiredGroup()
            .required(INT64)
            .named("action")
            .named("event")
            .named("test");

        PrimitiveType resolved = ParquetPushedExpressions.resolveNestedPrimitive(schema, "event.action");
        assertNotNull(resolved);
        assertEquals(PrimitiveType.PrimitiveTypeName.BINARY, resolved.getPrimitiveTypeName());
    }

    public void testResolveNestedPrimitiveDottedPathFallback() {
        MessageType schema = nestedKeyword();
        PrimitiveType resolved = ParquetPushedExpressions.resolveNestedPrimitive(schema, "event.action");
        assertNotNull(resolved);
        assertEquals(PrimitiveType.PrimitiveTypeName.BINARY, resolved.getPrimitiveTypeName());
    }

    public void testResolveNestedPrimitiveMissingPathReturnsNull() {
        MessageType schema = nestedKeyword();
        assertNull(ParquetPushedExpressions.resolveNestedPrimitive(schema, "event.nope"));
        assertNull(ParquetPushedExpressions.resolveNestedPrimitive(schema, "nope"));
        assertNull(ParquetPushedExpressions.resolveNestedPrimitive(schema, "nope.also.missing"));
    }

    public void testResolveNestedPrimitiveLandsOnGroupReturnsNull() {
        // "event" alone resolves to a GroupType, not a primitive — helper returns null.
        MessageType schema = nestedKeyword();
        assertNull(ParquetPushedExpressions.resolveNestedPrimitive(schema, "event"));
    }

    // --- helpers ---

    // IS NULL / IS NOT NULL over a top-level list must NOT push a predicate (esql-planning#1056): the
    // attribute name resolves to a LIST group, so notEq(column("tags"), null) names a leaf-absent
    // column that parquet-mr drops. They decline so the multivalue-safe null-mask evaluator answers.
    // (Value predicates — comparisons/IN/LIKE — are NOT declined here; their evaluator is not MV-safe.)

    private static MessageType intListSchema() {
        return new MessageType("test", Types.optionalList().optionalElement(INT32).named("ints"));
    }

    private static MessageType stringListSchema() {
        return new MessageType(
            "test",
            Types.optionalList()
                .optionalElement(PrimitiveType.PrimitiveTypeName.BINARY)
                .as(LogicalTypeAnnotation.stringType())
                .named("tags")
        );
    }

    public void testTopLevelListIsNotNullDeclines() {
        Expression expr = new IsNotNull(Source.EMPTY, attr("ints", DataType.INTEGER));
        assertNull(new ParquetPushedExpressions(List.of(expr)).toFilterPredicate(intListSchema()));
    }

    public void testTopLevelStringListIsNotNullDeclines() {
        Expression expr = new IsNotNull(Source.EMPTY, attr("tags", DataType.KEYWORD));
        assertNull(new ParquetPushedExpressions(List.of(expr)).toFilterPredicate(stringListSchema()));
    }

    public void testFlatColumnStillPushesControl() {
        // The list guard must not regress flat columns: a plain INT32 still pushes.
        MessageType schema = new MessageType("test", Types.optional(INT32).named("flat"));
        Expression expr = new IsNotNull(Source.EMPTY, attr("flat", DataType.INTEGER));
        FilterPredicate fp = new ParquetPushedExpressions(List.of(expr)).toFilterPredicate(schema);
        assertNotNull(fp);
        assertThat(fp.toString(), containsString("flat"));
    }

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
