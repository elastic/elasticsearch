/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.expression.function.UnresolvedFunction;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.EsqlBinaryComparison;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.esql.plan.QuerySettings;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.session.ConfigurationBuilder;

import java.time.Instant;
import java.time.LocalDate;
import java.time.Period;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_CFG;
import static org.elasticsearch.xpack.esql.core.type.EsField.TimeSeriesFieldType;

public class DateFunctionComparisonRewriterTests extends ESTestCase {

    private static final Source SRC = new Source(1, 0, "DATE_EXTRACT(\"YEAR\", \"2026-07-13\")");
    private static final EsqlFunctionRegistry REGISTRY = new EsqlFunctionRegistry();
    private static final long JULY_13_2026_UTC = Instant.parse("2026-07-13T00:00:00Z").toEpochMilli();
    private static final long YEAR_2024 = Instant.parse("2024-01-01T00:00:00Z").toEpochMilli();
    private static final long YEAR_2025 = Instant.parse("2025-01-01T00:00:00Z").toEpochMilli();
    private static final long JUNE_2024 = Instant.parse("2024-06-01T00:00:00Z").toEpochMilli();

    public void testDateExtractDatetimeLiteral() {
        Literal folded = foldLiteral("DATE_EXTRACT", List.of(keyword("YEAR"), datetime(JULY_13_2026_UTC)), utc());
        assertEquals(DataType.LONG, folded.dataType());
        assertEquals(2026L, folded.value());
        assertEquals(SRC, folded.source());
    }

    public void testDateExtractKeywordIso() {
        Literal folded = foldLiteral("date_extract", List.of(keyword("YEAR"), keyword("2026-07-13T00:00:00Z")), utc());
        assertEquals(DataType.LONG, folded.dataType());
        assertEquals(2026L, folded.value());
    }

    public void testDateExtractIsoOffsetVsQueryTimeZone() {
        // Instant is 2025-12-31T19:00Z; YEAR is taken in the query zone, not from the calendar date in the string.
        Literal iso = keyword("2026-01-01T00:30:00+05:30");
        Literal chrono = keyword("YEAR");
        assertEquals(2025L, foldLiteral("DATE_EXTRACT", List.of(chrono, iso), utc()).value());
        assertEquals(2026L, foldLiteral("DATE_EXTRACT", List.of(chrono, iso), config(ZoneOffset.ofHoursMinutes(5, 30))).value());
    }

    public void testDateExtractChronoCase() {
        Literal date = datetime(JULY_13_2026_UTC);
        assertEquals(2026L, foldLiteral("DATE_EXTRACT", List.of(keyword("YEAR"), date), utc()).value());
        assertEquals(2026L, foldLiteral("DATE_EXTRACT", List.of(keyword("year"), date), utc()).value());
    }

    public void testDateExtractMonthOfYear() {
        Literal folded = foldLiteral("DATE_EXTRACT", List.of(keyword("MONTH_OF_YEAR"), datetime(JULY_13_2026_UTC)), utc());
        assertEquals(DataType.LONG, folded.dataType());
        assertEquals(7L, folded.value());
    }

    public void testUnresolvedFieldChildUnchanged() {
        Expression field = new UnresolvedAttribute(SRC, "start");
        assertUnchanged("DATE_EXTRACT", List.of(keyword("YEAR"), field), utc());
    }

    public void testDateTruncHiveShapedIntFieldUnchanged() {
        Literal interval = new Literal(SRC, Period.ofYears(1), DataType.DATE_PERIOD);
        Literal yearInt = new Literal(SRC, 2026, DataType.INTEGER);
        assertUnchanged("DATE_TRUNC", List.of(interval, yearInt), utc());
    }

    public void testDateTruncQuotedIntervalUnchanged() {
        // DATE_TRUNC("1 day", ...) stays KEYWORD until ImplicitCasting; listing does not parse it.
        assertUnchanged("DATE_TRUNC", List.of(keyword("1 day"), datetime(JULY_13_2026_UTC)), utc());
    }

    public void testDateTruncQuotedIntervalNotInverted() {
        // Same listing gap: invert sees a resolved node, but the interval is still KEYWORD.
        // Analysis ImplicitCasting turns "1 year" into Period; optimizer tests cover that path.
        FieldAttribute ts = datetimeField();
        assertNull(invert(eq(trunc(keyword("1 year"), ts, utc()), datetime(YEAR_2024))));
    }

    public void testBadIsoUnchanged() {
        assertUnchanged("DATE_EXTRACT", List.of(keyword("YEAR"), keyword("not-a-date")), utc());
    }

    public void testWrongArityAndUnknownFunctionUnchanged() {
        assertUnchanged("DATE_EXTRACT", List.of(keyword("YEAR")), utc());
        assertUnchanged("concat", List.of(keyword("a"), keyword("b")), utc());
    }

    public void testDispatchUsesRegisteredClassNotHardcodedName() {
        assertEquals("date_extract", REGISTRY.functionName(DateExtract.class));
        assertEquals("date_trunc", REGISTRY.functionName(DateTrunc.class));
        assertEquals(REGISTRY.functionName(DateExtract.class), REGISTRY.resolveAlias("DATE_EXTRACT"));
        assertEquals(REGISTRY.functionName(DateTrunc.class), REGISTRY.resolveAlias("DATE_TRUNC"));
    }

    public void testDateExtractDateNanosLiteral() {
        Literal nanos = new Literal(SRC, DateUtils.toNanoSeconds(JULY_13_2026_UTC), DataType.DATE_NANOS);
        Literal folded = foldLiteral("DATE_EXTRACT", List.of(keyword("YEAR"), nanos), utc());
        assertEquals(DataType.LONG, folded.dataType());
        assertEquals(2026L, folded.value());
        assertEquals(SRC, folded.source());
    }

    public void testDateTruncKeywordIso() {
        Literal interval = new Literal(SRC, Period.ofDays(1), DataType.DATE_PERIOD);
        Literal folded = foldLiteral("DATE_TRUNC", List.of(interval, keyword("2026-07-13T12:34:56Z")), utc());
        assertEquals(DataType.DATETIME, folded.dataType());
        assertEquals(JULY_13_2026_UTC, folded.value());
        assertEquals(SRC, folded.source());
    }

    public void testDateTruncAllLiteralDatetime() {
        Literal interval = new Literal(SRC, Period.ofDays(1), DataType.DATE_PERIOD);
        Literal noon = datetime(Instant.parse("2026-07-13T12:34:56Z").toEpochMilli());
        Literal folded = foldLiteral("DATE_TRUNC", List.of(interval, noon), utc());
        assertEquals(DataType.DATETIME, folded.dataType());
        assertEquals(JULY_13_2026_UTC, folded.value());
    }

    public void testDateTruncAlignedEqualsBecomesHalfOpenRange() {
        FieldAttribute ts = datetimeField();
        Expression rewritten = invert(eq(trunc(yearInterval(), ts, utc()), datetime(YEAR_2024)));
        assertGteLt(asAnd(rewritten), ts, YEAR_2024, YEAR_2025, DataType.DATETIME);
    }

    public void testDateTruncNonAlignedEqualsIsEmptyRange() {
        FieldAttribute ts = datetimeField();
        Expression rewritten = invert(eq(trunc(yearInterval(), ts, utc()), datetime(JUNE_2024)));
        assertGteLt(asAnd(rewritten), ts, YEAR_2024, YEAR_2024, DataType.DATETIME);
    }

    public void testDateTruncAlignedNotEqualsBecomesOutsideRange() {
        FieldAttribute ts = datetimeField();
        Expression rewritten = invert(neq(trunc(yearInterval(), ts, utc()), datetime(YEAR_2024)));
        assertLtGte((Or) rewritten, ts, YEAR_2024, YEAR_2025, DataType.DATETIME);
    }

    public void testDateTruncNonAlignedNotEqualsLeftAlone() {
        FieldAttribute ts = datetimeField();
        assertNull(invert(neq(trunc(yearInterval(), ts, utc()), datetime(JUNE_2024))));
    }

    public void testDateTruncInequalitiesUseBucketBounds() {
        FieldAttribute ts = datetimeField();
        Literal interval = yearInterval();
        Configuration cfg = utc();
        assertGte(invert(gt(trunc(interval, ts, cfg), datetime(YEAR_2024))), ts, YEAR_2025, DataType.DATETIME);
        assertGte(invert(gte(trunc(interval, ts, cfg), datetime(YEAR_2024))), ts, YEAR_2024, DataType.DATETIME);
        assertLt(invert(lt(trunc(interval, ts, cfg), datetime(YEAR_2024))), ts, YEAR_2024, DataType.DATETIME);
        assertLt(invert(lte(trunc(interval, ts, cfg), datetime(YEAR_2024))), ts, YEAR_2025, DataType.DATETIME);
        assertGte(invert(gt(trunc(interval, ts, cfg), datetime(JUNE_2024))), ts, YEAR_2025, DataType.DATETIME);
        assertGte(invert(gte(trunc(interval, ts, cfg), datetime(JUNE_2024))), ts, YEAR_2025, DataType.DATETIME);
        assertLt(invert(lt(trunc(interval, ts, cfg), datetime(JUNE_2024))), ts, YEAR_2025, DataType.DATETIME);
        assertLt(invert(lte(trunc(interval, ts, cfg), datetime(JUNE_2024))), ts, YEAR_2025, DataType.DATETIME);
    }

    public void testDateTruncKeywordIsoLiteral() {
        FieldAttribute ts = datetimeField();
        Expression rewritten = invert(eq(trunc(yearInterval(), ts, utc()), keyword("2024-01-01T00:00:00Z")));
        assertGteLt(asAnd(rewritten), ts, YEAR_2024, YEAR_2025, DataType.DATETIME);
    }

    public void testDateTruncBoundsMatchFieldDateNanos() {
        FieldAttribute ts = nanosField();
        long startNanos = DateUtils.toNanoSeconds(YEAR_2024);
        long nextNanos = DateUtils.toNanoSeconds(YEAR_2025);
        Expression rewritten = invert(eq(trunc(yearInterval(), ts, utc()), new Literal(SRC, startNanos, DataType.DATE_NANOS)));
        assertGteLt(asAnd(rewritten), ts, startNanos, nextNanos, DataType.DATE_NANOS);
    }

    public void testDateTruncEvalAliasInverts() {
        ReferenceAttribute alias = new ReferenceAttribute(SRC, "ts", DataType.DATETIME);
        Expression rewritten = invert(eq(trunc(yearInterval(), alias, utc()), datetime(YEAR_2024)));
        assertGteLt(asAnd(rewritten), alias, YEAR_2024, YEAR_2025, DataType.DATETIME);
    }

    public void testDateExtractYearEqualsBecomesRange() {
        FieldAttribute ts = datetimeField();
        Expression rewritten = invert(eq(extract(keyword("year"), ts, utc()), longLit(2024L)));
        assertGteLt(asAnd(rewritten), ts, YEAR_2024, YEAR_2025, DataType.DATETIME);
    }

    public void testDateExtractYearInequalitiesAndNotEquals() {
        FieldAttribute ts = datetimeField();
        DateExtract year = extract(keyword("year"), ts, utc());
        assertGte(invert(gt(year, longLit(2024L))), ts, YEAR_2025, DataType.DATETIME);
        assertGte(invert(gte(year, longLit(2024L))), ts, YEAR_2024, DataType.DATETIME);
        assertLt(invert(lt(year, longLit(2024L))), ts, YEAR_2024, DataType.DATETIME);
        assertLt(invert(lte(year, longLit(2024L))), ts, YEAR_2025, DataType.DATETIME);
        assertLtGte((Or) invert(neq(year, longLit(2024L))), ts, YEAR_2024, YEAR_2025, DataType.DATETIME);
    }

    public void testDateExtractYearUsesQueryTimeZone() {
        FieldAttribute ts = datetimeField();
        ZoneId plus530 = ZoneOffset.ofHoursMinutes(5, 30);
        Configuration cfg = config(plus530);
        long start = ZonedDateTime.of(2024, 1, 1, 0, 0, 0, 0, plus530).toInstant().toEpochMilli();
        long next = ZonedDateTime.of(2025, 1, 1, 0, 0, 0, 0, plus530).toInstant().toEpochMilli();
        Expression rewritten = invert(eq(extract(keyword("YEAR"), ts, cfg), longLit(2024L)));
        assertGteLt(asAnd(rewritten), ts, start, next, DataType.DATETIME);
    }

    public void testDateExtractYearUsesNamedTimeZone() {
        FieldAttribute ts = datetimeField();
        ZoneId ny = ZoneId.of("America/New_York");
        long start = LocalDate.of(2024, 1, 1).atStartOfDay(ny).toInstant().toEpochMilli();
        long next = LocalDate.of(2025, 1, 1).atStartOfDay(ny).toInstant().toEpochMilli();
        Expression rewritten = invert(eq(extract(keyword("YEAR"), ts, config(ny)), longLit(2024L)));
        assertGteLt(asAnd(rewritten), ts, start, next, DataType.DATETIME);
    }

    public void testDateTruncUsesQueryTimeZone() {
        FieldAttribute ts = datetimeField();
        ZoneId plus530 = ZoneOffset.ofHoursMinutes(5, 30);
        Configuration cfg = config(plus530);
        long zoneYearStart = ZonedDateTime.of(2024, 1, 1, 0, 0, 0, 0, plus530).toInstant().toEpochMilli();
        long zoneYearNext = ZonedDateTime.of(2025, 1, 1, 0, 0, 0, 0, plus530).toInstant().toEpochMilli();
        Expression unaligned = invert(eq(trunc(yearInterval(), ts, cfg), datetime(YEAR_2024)));
        assertGteLt(asAnd(unaligned), ts, zoneYearStart, zoneYearStart, DataType.DATETIME);
        Expression aligned = invert(eq(trunc(yearInterval(), ts, cfg), datetime(zoneYearStart)));
        assertGteLt(asAnd(aligned), ts, zoneYearStart, zoneYearNext, DataType.DATETIME);
    }

    public void testDateTruncWeekAndDay() {
        FieldAttribute ts = datetimeField();
        // 2024-01-01 is Monday; WEEK_OF_WEEKYEAR, not a 7-day epoch duration.
        Expression week = invert(eq(trunc(weekInterval(), ts, utc()), datetime(YEAR_2024)));
        assertGteLt(asAnd(week), ts, YEAR_2024, Instant.parse("2024-01-08T00:00:00Z").toEpochMilli(), DataType.DATETIME);
        Expression midweek = invert(eq(trunc(weekInterval(), ts, utc()), datetime(Instant.parse("2024-01-03T00:00:00Z").toEpochMilli())));
        assertGteLt(asAnd(midweek), ts, YEAR_2024, YEAR_2024, DataType.DATETIME);
        Expression day = invert(eq(trunc(dayInterval(), ts, utc()), datetime(YEAR_2024)));
        assertGteLt(asAnd(day), ts, YEAR_2024, Instant.parse("2024-01-02T00:00:00Z").toEpochMilli(), DataType.DATETIME);
    }

    public void testDateTruncDayUsesNamedZoneDst() {
        FieldAttribute ts = datetimeField();
        ZoneId ny = ZoneId.of("America/New_York");
        long start = LocalDate.of(2024, 3, 10).atStartOfDay(ny).toInstant().toEpochMilli();
        long next = LocalDate.of(2024, 3, 11).atStartOfDay(ny).toInstant().toEpochMilli();
        assertEquals("spring-forward day is 23 hours", 23 * 3_600_000L, next - start);
        Expression rewritten = invert(eq(trunc(dayInterval(), ts, config(ny)), datetime(start)));
        assertGteLt(asAnd(rewritten), ts, start, next, DataType.DATETIME);
    }

    public void testDateExtractProlepticMonthAndEpochDay() {
        FieldAttribute ts = datetimeField();
        long jan2024 = Instant.parse("2024-01-01T00:00:00Z").toEpochMilli();
        long feb2024 = Instant.parse("2024-02-01T00:00:00Z").toEpochMilli();
        long prolepticMonth = 2024L * 12 + 1 - 1;
        Expression month = invert(eq(extract(keyword("proleptic_month"), ts, utc()), longLit(prolepticMonth)));
        assertGteLt(asAnd(month), ts, jan2024, feb2024, DataType.DATETIME);

        long epochDay = LocalDate.of(2024, 1, 1).toEpochDay();
        long nextDay = Instant.parse("2024-01-02T00:00:00Z").toEpochMilli();
        Expression day = invert(eq(extract(keyword("epoch_day"), ts, utc()), longLit(epochDay)));
        assertGteLt(asAnd(day), ts, jan2024, nextDay, DataType.DATETIME);
    }

    public void testDateExtractCyclicChronoLeftAlone() {
        FieldAttribute ts = datetimeField();
        Configuration cfg = utc();
        assertNull(invert(eq(extract(keyword("month_of_year"), ts, cfg), longLit(7L))));
        assertNull(invert(eq(extract(keyword("hour_of_day"), ts, cfg), longLit(9L))));
        assertNull(invert(eq(extract(keyword("day_of_month"), ts, cfg), longLit(13L))));
        assertNull(invert(eq(extract(keyword("year_of_era"), ts, cfg), longLit(2024L))));
    }

    public void testDateTruncRefusesNanosLiteralOnDatetimeField() {
        FieldAttribute ts = datetimeField();
        Literal nanos = new Literal(SRC, DateUtils.toNanoSeconds(YEAR_2024), DataType.DATE_NANOS);
        assertNull(invert(eq(trunc(yearInterval(), ts, utc()), nanos)));
    }

    public void testDateExtractYearOnDateNanos() {
        FieldAttribute ts = nanosField();
        long startNanos = DateUtils.toNanoSeconds(YEAR_2024);
        long nextNanos = DateUtils.toNanoSeconds(YEAR_2025);
        Expression rewritten = invert(eq(extract(keyword("year"), ts, utc()), longLit(2024L)));
        assertGteLt(asAnd(rewritten), ts, startNanos, nextNanos, DataType.DATE_NANOS);
    }

    public void testDateTruncDatetimeLiteralOnNanosField() {
        FieldAttribute ts = nanosField();
        long startNanos = DateUtils.toNanoSeconds(YEAR_2024);
        long nextNanos = DateUtils.toNanoSeconds(YEAR_2025);
        Expression rewritten = invert(eq(trunc(yearInterval(), ts, utc()), datetime(YEAR_2024)));
        assertGteLt(asAnd(rewritten), ts, startNanos, nextNanos, DataType.DATE_NANOS);
    }

    public void testDateTruncLeftoverNanosIsEmptyRange() {
        FieldAttribute ts = nanosField();
        long startNanos = DateUtils.toNanoSeconds(YEAR_2024);
        Literal leftover = new Literal(SRC, startNanos + 1L, DataType.DATE_NANOS);
        Expression rewritten = invert(eq(trunc(yearInterval(), ts, utc()), leftover));
        assertGteLt(asAnd(rewritten), ts, startNanos, startNanos, DataType.DATE_NANOS);
    }

    public void testDateExtractIntegerLiteral() {
        FieldAttribute ts = datetimeField();
        Expression rewritten = invert(eq(extract(keyword("year"), ts, utc()), intLit(2024)));
        assertGteLt(asAnd(rewritten), ts, YEAR_2024, YEAR_2025, DataType.DATETIME);
    }

    public void testInvertRefusesNonDatetimeFieldAndNullLiteral() {
        FieldAttribute yearInt = field("year", DataType.INTEGER);
        FieldAttribute ts = datetimeField();
        assertNull(invert(eq(trunc(yearInterval(), yearInt, utc()), datetime(YEAR_2024))));
        assertNull(invert(eq(trunc(yearInterval(), ts, utc()), new Literal(SRC, null, DataType.DATETIME))));
        assertNull(invert(eq(new UnresolvedAttribute(SRC, "x"), longLit(2024L))));
    }

    private static Literal foldLiteral(String name, List<Expression> args, Configuration config) {
        UnresolvedFunction call = new UnresolvedFunction(SRC, name, args);
        Expression folded = DateFunctionComparisonRewriter.tryFoldCall(call, config, REGISTRY);
        assertNotSame(call, folded);
        assertEquals(Literal.class, folded.getClass());
        assertEquals(SRC, folded.source());
        return (Literal) folded;
    }

    private static void assertUnchanged(String name, List<Expression> args, Configuration config) {
        UnresolvedFunction call = new UnresolvedFunction(SRC, name, args);
        assertSame(call, DateFunctionComparisonRewriter.tryFoldCall(call, config, REGISTRY));
    }

    private static Configuration utc() {
        return config(ZoneOffset.UTC);
    }

    private static Configuration config(ZoneId zone) {
        return new ConfigurationBuilder(TEST_CFG).setting(QuerySettings.TIME_ZONE, zone).build();
    }

    private static Literal keyword(String value) {
        return Literal.keyword(SRC, value);
    }

    private static Literal datetime(long millis) {
        return new Literal(SRC, millis, DataType.DATETIME);
    }

    private static Expression invert(EsqlBinaryComparison cmp) {
        return DateFunctionComparisonRewriter.tryRewriteComparison(cmp, FoldContext.small());
    }

    private static DateTrunc trunc(Expression interval, Expression field, Configuration config) {
        return new DateTrunc(SRC, interval, field, config);
    }

    private static DateExtract extract(Expression chrono, Expression field, Configuration config) {
        return new DateExtract(SRC, chrono, field, config);
    }

    private static And asAnd(Expression expression) {
        assertNotNull(expression);
        return (And) expression;
    }

    private static void assertGteLt(And and, Expression field, long start, long next, DataType type) {
        GreaterThanOrEqual gte = (GreaterThanOrEqual) and.left();
        LessThan lt = (LessThan) and.right();
        assertSame(field, gte.left());
        assertSame(field, lt.left());
        assertEquals(type, gte.right().dataType());
        assertEquals(type, lt.right().dataType());
        assertEquals(start, ((Literal) gte.right()).value());
        assertEquals(next, ((Literal) lt.right()).value());
    }

    private static void assertLtGte(Or or, Expression field, long start, long next, DataType type) {
        LessThan lt = (LessThan) or.left();
        GreaterThanOrEqual gte = (GreaterThanOrEqual) or.right();
        assertSame(field, lt.left());
        assertSame(field, gte.left());
        assertEquals(type, lt.right().dataType());
        assertEquals(type, gte.right().dataType());
        assertEquals(start, ((Literal) lt.right()).value());
        assertEquals(next, ((Literal) gte.right()).value());
    }

    private static void assertGte(Expression expression, Expression field, long bound, DataType type) {
        GreaterThanOrEqual gte = (GreaterThanOrEqual) expression;
        assertSame(field, gte.left());
        assertEquals(type, gte.right().dataType());
        assertEquals(bound, ((Literal) gte.right()).value());
    }

    private static void assertLt(Expression expression, Expression field, long bound, DataType type) {
        LessThan lt = (LessThan) expression;
        assertSame(field, lt.left());
        assertEquals(type, lt.right().dataType());
        assertEquals(bound, ((Literal) lt.right()).value());
    }

    private static Equals eq(Expression left, Expression right) {
        return new Equals(SRC, left, right, null);
    }

    private static NotEquals neq(Expression left, Expression right) {
        return new NotEquals(SRC, left, right, null);
    }

    private static GreaterThan gt(Expression left, Expression right) {
        return new GreaterThan(SRC, left, right, null);
    }

    private static GreaterThanOrEqual gte(Expression left, Expression right) {
        return new GreaterThanOrEqual(SRC, left, right, null);
    }

    private static LessThan lt(Expression left, Expression right) {
        return new LessThan(SRC, left, right, null);
    }

    private static LessThanOrEqual lte(Expression left, Expression right) {
        return new LessThanOrEqual(SRC, left, right, null);
    }

    private static Literal yearInterval() {
        return new Literal(SRC, Period.ofYears(1), DataType.DATE_PERIOD);
    }

    private static Literal weekInterval() {
        return new Literal(SRC, Period.ofDays(7), DataType.DATE_PERIOD);
    }

    private static Literal dayInterval() {
        return new Literal(SRC, Period.ofDays(1), DataType.DATE_PERIOD);
    }

    private static Literal longLit(long value) {
        return new Literal(SRC, value, DataType.LONG);
    }

    private static Literal intLit(int value) {
        return new Literal(SRC, value, DataType.INTEGER);
    }

    private static FieldAttribute datetimeField() {
        return field("ts", DataType.DATETIME);
    }

    private static FieldAttribute nanosField() {
        return field("ts", DataType.DATE_NANOS);
    }

    private static FieldAttribute field(String name, DataType type) {
        return new FieldAttribute(SRC, name, new EsField(name, type, Map.of(), true, TimeSeriesFieldType.NONE));
    }
}
