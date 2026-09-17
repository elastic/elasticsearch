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
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.expression.function.UnresolvedFunction;
import org.elasticsearch.xpack.esql.plan.QuerySettings;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.session.ConfigurationBuilder;

import java.time.Instant;
import java.time.Period;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_CFG;

public class DateFunctionComparisonRewriterTests extends ESTestCase {

    private static final Source SRC = new Source(1, 0, "DATE_EXTRACT(\"YEAR\", \"2026-07-13\")");
    private static final EsqlFunctionRegistry REGISTRY = new EsqlFunctionRegistry();
    private static final long JULY_13_2026_UTC = Instant.parse("2026-07-13T00:00:00Z").toEpochMilli();

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
}
