/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.datasources.PartitionFilterHintExtractor.Operator;
import org.elasticsearch.xpack.esql.datasources.PartitionFilterHintExtractor.PartitionFilterHint;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvInRange;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItem;

public class PartitionSpecProjectorTests extends ESTestCase {

    private static final Source SRC = Source.EMPTY;

    /** 2024-03-15T00:00:00Z */
    private static final Instant MARCH_15_2024 = Instant.parse("2024-03-15T00:00:00Z");

    public void testCrossYearRangeKeepsNextJanuaryDropsPriorFebruary() {
        PartitionSpec spec = PartitionSpec.parse("year(ts), month(ts), day(ts)");
        List<PartitionFilterHint> hints = List.of(hint("ts", Operator.GREATER_THAN, MARCH_15_2024));

        assertTrue("2025-01-01 is after the bound", spec.overlaps(folder(2025, 1, 1), hints));
        assertFalse("2024-02-15 is before the bound", spec.overlaps(folder(2024, 2, 15), hints));
        assertTrue("2024-03-15 still overlaps the exclusive start", spec.overlaps(folder(2024, 3, 15), hints));
        assertFalse("2024-03-14 is entirely before the bound", spec.overlaps(folder(2024, 3, 14), hints));
    }

    public void testOverlapsExpressionsGreaterThanDatetimeMatchesHintOverlap() {
        PartitionSpec spec = PartitionSpec.parse("year(ts), month(ts), day(ts)");
        Expression filter = new GreaterThan(SRC, datetimeField("ts"), datetimeLiteral(MARCH_15_2024.toEpochMilli()));

        assertTrue("2025-01-01 is after the bound", spec.overlapsExpressions(folder(2025, 1, 1), List.of(filter)));
        assertFalse("2024-02-15 is before the bound", spec.overlapsExpressions(folder(2024, 2, 15), List.of(filter)));
        assertTrue("2024-03-15 still overlaps the exclusive start", spec.overlapsExpressions(folder(2024, 3, 15), List.of(filter)));
        assertFalse("2024-03-14 is entirely before the bound", spec.overlapsExpressions(folder(2024, 3, 14), List.of(filter)));
    }

    public void testOverlapsExpressionsMvInRangeOneDay() {
        PartitionSpec spec = PartitionSpec.parse("year(ts), month(ts), day(ts)");
        Instant dayStart = Instant.parse("2024-06-15T00:00:00Z");
        Instant dayEnd = Instant.parse("2024-06-15T23:59:59.999Z");
        Expression filter = new MvInRange(
            SRC,
            datetimeField("ts"),
            datetimeLiteral(dayStart.toEpochMilli()),
            datetimeLiteral(dayEnd.toEpochMilli())
        );
        assertTrue(spec.overlapsExpressions(folder(2024, 6, 15), List.of(filter)));
        assertFalse(spec.overlapsExpressions(folder(2024, 6, 14), List.of(filter)));
        assertFalse(spec.overlapsExpressions(folder(2024, 6, 16), List.of(filter)));
    }

    public void testOverlapsExpressionsDatetimeLongIsNotScaledAsSeconds() {
        PartitionSpec spec = PartitionSpec.parse("year(ts, epoch_second), month(ts, epoch_second)");
        Expression filter = new GreaterThan(SRC, datetimeField("ts"), datetimeLiteral(MARCH_15_2024.toEpochMilli()));
        assertTrue("datetime millis must not be scaled as unix seconds", spec.overlapsExpressions(folder(2024, 3, null), List.of(filter)));
        assertFalse("2023 is entirely before March 2024", spec.overlapsExpressions(folder(2023, 3, null), List.of(filter)));
    }

    public void testOverlapsExpressionsDateNanosLongIsInstantNotUnix() {
        PartitionSpec spec = PartitionSpec.parse("year(ts), month(ts)");
        long nanos = MARCH_15_2024.getEpochSecond() * 1_000_000_000L;
        Expression filter = new GreaterThan(SRC, dateNanosField("ts"), new Literal(SRC, nanos, DataType.DATE_NANOS));
        assertTrue(spec.overlapsExpressions(folder(2024, 3, null), List.of(filter)));
        assertFalse(spec.overlapsExpressions(folder(2024, 2, null), List.of(filter)));
    }

    public void testBoundColumnsIncludesIdentityAndTemporalSources() {
        PartitionSpec spec = PartitionSpec.parse("aws-region=region, year(ts), month(ts)");
        assertEquals(Set.of("region", "ts"), spec.boundColumns());
    }

    public void testJointOverlapIsNotIndependentYearAndMonth() {
        PartitionSpec spec = PartitionSpec.parse("year(ts), month(ts)");
        List<PartitionFilterHint> hints = List.of(hint("ts", Operator.GREATER_THAN, MARCH_15_2024));

        // Independent year>=2024 AND month>=3 would drop January 2025. Joint overlap keeps it.
        assertTrue(spec.overlaps(folder(2025, 1, null), hints));
        assertFalse(spec.overlaps(folder(2024, 2, null), hints));
        assertFalse("March 2023 is entirely before the bound", spec.overlaps(folder(2023, 3, null), hints));
        assertTrue(spec.overlaps(folder(2024, 3, null), hints));
        assertTrue(spec.overlaps(folder(2024, 4, null), hints));
    }

    public void testExclusiveEndOnGrainBoundaryDropsNextFolder() {
        PartitionSpec spec = PartitionSpec.parse("year(ts), month(ts)");
        Instant april1 = Instant.parse("2024-04-01T00:00:00Z");
        List<PartitionFilterHint> hints = List.of(hint("ts", Operator.LESS_THAN, april1));

        assertTrue(spec.overlaps(folder(2024, 3, null), hints));
        assertFalse(spec.overlaps(folder(2024, 4, null), hints));
        assertTrue(
            "inclusive end on the April boundary keeps April",
            spec.overlaps(folder(2024, 4, null), List.of(hint("ts", Operator.LESS_THAN_OR_EQUAL, april1)))
        );
    }

    public void testYearInListingHintsCoverOnlyOverlappingYears() {
        PartitionSpec spec = PartitionSpec.parse("year(ts), month(ts), day(ts)");
        Instant start = Instant.parse("2024-03-15T00:00:00Z");
        Instant end = Instant.parse("2026-01-01T00:00:00Z");
        List<PartitionFilterHint> hints = List.of(hint("ts", Operator.GREATER_THAN_OR_EQUAL, start), hint("ts", Operator.LESS_THAN, end));

        assertEquals(List.of(hint("year", Operator.IN, 2024, 2025)), spec.projectListingHints(hints));
    }

    public void testFifteenMinuteWindowEmitsOneValuePerGrain() {
        PartitionSpec spec = PartitionSpec.parse("year(ts), month(ts), day(ts), hour(ts)");
        Instant start = Instant.parse("2026-10-13T10:00:00Z");
        Instant end = Instant.parse("2026-10-13T10:15:00Z");
        List<PartitionFilterHint> hints = List.of(hint("ts", Operator.GREATER_THAN_OR_EQUAL, start), hint("ts", Operator.LESS_THAN, end));
        List<PartitionFilterHint> projected = spec.projectListingHints(hints);
        assertEquals(List.of(2026), inValues(projected, "year"));
        assertEquals(List.of(10), inValues(projected, "month"));
        assertEquals(List.of(13), inValues(projected, "day"));
        assertEquals(List.of(10), inValues(projected, "hour"));
    }

    public void testThreeDayWindowSkipsCompleteHourIn() {
        PartitionSpec spec = PartitionSpec.parse("year(ts), month(ts), day(ts), hour(ts)");
        Instant start = Instant.parse("2026-07-13T00:00:00Z");
        Instant end = Instant.parse("2026-07-16T00:00:00Z");
        List<PartitionFilterHint> hints = List.of(hint("ts", Operator.GREATER_THAN_OR_EQUAL, start), hint("ts", Operator.LESS_THAN, end));
        List<PartitionFilterHint> projected = spec.projectListingHints(hints);
        assertEquals(List.of(2026), inValues(projected, "year"));
        assertEquals(List.of(7), inValues(projected, "month"));
        assertEquals(List.of(13, 14, 15), inValues(projected, "day"));
        assertNull("72 hour folders but 24 unique hours of day is complete", inValues(projected, "hour"));
    }

    public void testLagKeepsNextHourAndDayInListing() {
        PartitionSpec spec = PartitionSpec.parse("year(ts), month(ts), day(ts), hour(ts), lag(ts, 15m)");
        Instant start = Instant.parse("2024-06-15T23:50:00Z");
        Instant end = Instant.parse("2024-06-15T23:55:00Z");
        List<PartitionFilterHint> hints = List.of(hint("ts", Operator.GREATER_THAN_OR_EQUAL, start), hint("ts", Operator.LESS_THAN, end));
        List<PartitionFilterHint> projected = spec.projectListingHints(hints);
        assertEquals(List.of(2024), inValues(projected, "year"));
        assertEquals(List.of(6), inValues(projected, "month"));
        assertEquals(List.of(15, 16), inValues(projected, "day"));
        assertEquals(List.of(23, 0), inValues(projected, "hour"));
        PartitionSpec noLag = PartitionSpec.parse("year(ts), month(ts), day(ts), hour(ts)");
        assertEquals(List.of(15), inValues(noLag.projectListingHints(hints), "day"));
        assertEquals(List.of(23), inValues(noLag.projectListingHints(hints), "hour"));
    }

    public void testLagDoesNotInventExtraYear() {
        PartitionSpec spec = PartitionSpec.parse("year(ts), month(ts), day(ts), hour(ts), lag(ts, 15m)");
        Instant start = Instant.parse("2024-06-15T10:00:00Z");
        Instant end = Instant.parse("2024-06-15T10:15:00Z");
        List<PartitionFilterHint> hints = List.of(hint("ts", Operator.GREATER_THAN_OR_EQUAL, start), hint("ts", Operator.LESS_THAN, end));
        assertEquals(List.of(2024), inValues(spec.projectListingHints(hints), "year"));
        assertEquals(List.of(6), inValues(spec.projectListingHints(hints), "month"));
        assertEquals(List.of(15), inValues(spec.projectListingHints(hints), "day"));
        assertEquals(List.of(10), inValues(spec.projectListingHints(hints), "hour"));
    }

    public void testUnboundedRangeDoesNotEmitInfiniteYearIn() {
        PartitionSpec spec = PartitionSpec.parse("year(ts)");
        List<PartitionFilterHint> hints = List.of(hint("ts", Operator.GREATER_THAN, MARCH_15_2024));
        assertEquals(hints, spec.projectListingHints(hints));
    }

    public void testMonthOnlySpecDoesNotInventYearIn() {
        PartitionSpec spec = PartitionSpec.parse("month(ts)");
        Instant start = Instant.parse("2024-03-15T00:00:00Z");
        Instant end = Instant.parse("2026-01-01T00:00:00Z");
        List<PartitionFilterHint> hints = List.of(hint("ts", Operator.GREATER_THAN_OR_EQUAL, start), hint("ts", Operator.LESS_THAN, end));
        assertEquals(hints, spec.projectListingHints(hints));
    }

    public void testIdentityRemapRewritesHintColumn() {
        PartitionSpec spec = PartitionSpec.parse("aws-region=region");
        List<PartitionFilterHint> projected = spec.projectListingHints(List.of(hint("region", Operator.EQUALS, "eu")));
        assertEquals(1, projected.size());
        assertEquals("aws-region", projected.get(0).columnName());
        assertEquals(Operator.EQUALS, projected.get(0).operator());
        assertEquals(List.of("eu"), projected.get(0).values());
    }

    public void testIdentityAndTransformOnSameKeyStayAnd() {
        PartitionSpec spec = PartitionSpec.parse("year=year(ts)");
        Instant start = Instant.parse("2024-01-01T00:00:00Z");
        Instant end = Instant.parse("2026-01-01T00:00:00Z");
        List<PartitionFilterHint> hints = List.of(
            hint("year", Operator.EQUALS, 2024),
            hint("ts", Operator.GREATER_THAN_OR_EQUAL, start),
            hint("ts", Operator.LESS_THAN, end)
        );
        assertEquals(List.of(hint("year", Operator.EQUALS, 2024), hint("year", Operator.IN, 2024, 2025)), spec.projectListingHints(hints));
    }

    public void testEmptySpecIsIdentity() {
        List<PartitionFilterHint> hints = List.of(hint("year", Operator.EQUALS, 2024));
        assertEquals(hints, PartitionSpec.EMPTY.projectListingHints(hints));
        assertTrue(PartitionSpec.EMPTY.overlaps(folder(1999, 1, 1), hints));
    }

    public void testNoSourceColumnHintSkipsOverlap() {
        PartitionSpec spec = PartitionSpec.parse("year(ts), month(ts)");
        assertTrue(spec.overlaps(folder(1999, 1, 1), List.of(hint("year", Operator.EQUALS, 2024))));
    }

    public void testUnmatchedBindWarnsWhenDetectionFoundNoKeys() {
        PartitionSpec spec = PartitionSpec.parse("year(ts)");
        List<String> notices = new ArrayList<>();
        spec.emitListingNotices(Set.of(), List.of(), notices::add);
        assertThat(notices, hasItem(containsString("not detected")));
    }

    public void testUnmatchedBindIsSkippedAndWarned() {
        PartitionSpec spec = PartitionSpec.parse("yyy=year(ts)");
        Instant start = Instant.parse("2024-01-01T00:00:00Z");
        Instant end = Instant.parse("2025-01-01T00:00:00Z");
        List<PartitionFilterHint> hints = List.of(hint("ts", Operator.GREATER_THAN_OR_EQUAL, start), hint("ts", Operator.LESS_THAN, end));

        assertEquals(hints, spec.projectListingHints(hints, Set.of("year")));
        assertEquals(List.of(hint("yyy", Operator.IN, 2024)), spec.projectListingHints(hints, Set.of("yyy")));

        List<String> notices = new ArrayList<>();
        spec.emitListingNotices(Set.of("year"), hints, notices::add);
        assertThat(notices, hasItem(containsString(PartitionSpec.CONFIG_PARTITION_SPEC)));
        assertThat(notices, hasItem(containsString("yyy")));
        assertThat(notices, hasItem(containsString("not detected")));
    }

    public void testWrongUnitWarnsForNumericSecondsReadAsMillis() {
        PartitionSpec spec = PartitionSpec.parse("year(start)");
        // 1_710_000_000 as millis is 1970-01-20.
        List<PartitionFilterHint> hints = List.of(hint("start", Operator.GREATER_THAN, 1_710_000_000L));
        List<String> notices = new ArrayList<>();
        spec.emitListingNotices(Set.of("year"), hints, notices::add);
        assertThat(notices, hasItem(containsString("calendar year [1970]")));
        assertThat(notices, hasItem(containsString("epoch_second")));
        assertThat(notices, hasItem(containsString("epoch_millis")));
    }

    public void testWrongUnitWarnsForMillisReadAsSeconds() {
        PartitionSpec spec = PartitionSpec.parse("year(start, epoch_second)");
        List<String> notices = new ArrayList<>();
        spec.emitListingNotices(Set.of("year"), List.of(hint("start", Operator.GREATER_THAN, 1_710_000_000_000L)), notices::add);
        assertThat(notices, hasItem(containsString("calendar year [")));
        assertThat(notices, hasItem(containsString("epoch_second")));
        assertThat(notices, hasItem(containsString("epoch_millis")));
        String yearNotice = notices.stream().filter(n -> n.contains("calendar year [")).findFirst().orElseThrow();
        int year = Integer.parseInt(yearNotice.replaceAll(".*calendar year \\[(-?\\d+)].*", "$1"));
        assertTrue(year > PartitionSpec.WRONG_UNIT_YEAR_MAX);
    }

    public void testWrongUnitLessThanDoesNotDropFolders() {
        PartitionSpec spec = PartitionSpec.parse("year(start), month(start)");
        List<PartitionFilterHint> hints = List.of(hint("start", Operator.LESS_THAN, 1_710_000_000L));
        assertTrue("wrong-unit < must not empty the 2024 folder", spec.overlaps(folder(2024, 6, null), hints));
        assertEquals(hints, spec.projectListingHints(hints));
        List<String> notices = new ArrayList<>();
        spec.emitListingNotices(Set.of("year", "month"), spec.projectListingHints(hints), notices::add);
        assertThat(notices, hasItem(containsString("the unit is likely wrong")));
    }

    public void testWrongUnitDoesNotWarnForDatetimeLiteral() {
        PartitionSpec spec = PartitionSpec.parse("year(ts)");
        List<String> notices = new ArrayList<>();
        spec.emitListingNotices(Set.of("year"), List.of(hint("ts", Operator.GREATER_THAN, MARCH_15_2024)), notices::add);
        assertThat(notices, empty());
    }

    public void testSecondsUnitConvertsNumericBound() {
        PartitionSpec spec = PartitionSpec.parse("year(start, epoch_second), month(start, epoch_second)");
        long march15Seconds = MARCH_15_2024.getEpochSecond();
        List<PartitionFilterHint> hints = List.of(hint("start", Operator.GREATER_THAN, march15Seconds));

        assertTrue(spec.overlaps(folder(2025, 1, null), hints));
        assertFalse(spec.overlaps(folder(2024, 2, null), hints));
    }

    public void testPaddedFolderStrings() {
        PartitionSpec spec = PartitionSpec.parse("year(ts), month(ts), day(ts)");
        List<PartitionFilterHint> hints = List.of(hint("ts", Operator.GREATER_THAN, MARCH_15_2024));
        assertTrue(spec.overlaps(Map.of("year", "2025", "month", "01", "day", "01"), hints));
        assertFalse(spec.overlaps(Map.of("year", "2024", "month", "02", "day", "15"), hints));
    }

    public void testProjectDoesNotInventExtractorDroppedColumn() {
        PartitionSpec spec = PartitionSpec.parse("year(ts)");
        // Extractor already dropped the shadowed ts; projector only sees year.
        List<PartitionFilterHint> projected = spec.projectListingHints(List.of(hint("year", Operator.EQUALS, 2024)));
        assertEquals(List.of(hint("year", Operator.EQUALS, 2024)), projected);
    }

    public void testAliasIdentityValuesCopiesPathKeyOntoQueryColumn() {
        PartitionSpec spec = PartitionSpec.parse("aws-region=region");
        Map<String, Object> values = new HashMap<>(Map.of("aws-region", "eu"));
        spec.aliasIdentityValues(values);
        assertEquals("eu", values.get("region"));
        assertEquals("eu", values.get("aws-region"));
    }

    public void testHourFolderDropsTheHourBeforeTheBound() {
        PartitionSpec spec = PartitionSpec.parse("year(ts), month(ts), day(ts), hour(ts)");
        Instant bound = Instant.parse("2024-06-15T10:30:00Z");
        List<PartitionFilterHint> hints = List.of(hint("ts", Operator.GREATER_THAN_OR_EQUAL, bound));

        assertFalse(spec.overlaps(hourFolder(9), hints));
        assertTrue(spec.overlaps(hourFolder(10), hints));
        assertTrue(spec.overlaps(hourFolder(11), hints));
    }

    public void testRenamedMonthWithoutYearWarnsAndKeepsFolders() {
        PartitionSpec spec = PartitionSpec.parse("mo=month(ts)");
        List<String> notices = new ArrayList<>();
        spec.emitListingNotices(Set.of("yyy", "mo"), List.of(hint("ts", Operator.GREATER_THAN, MARCH_15_2024)), notices::add);
        assertThat(notices, hasItem(containsString("needs a [year]")));
        assertTrue(spec.overlaps(Map.of("yyy", 2024, "mo", 1), List.of(hint("ts", Operator.GREATER_THAN, MARCH_15_2024))));
    }

    public void testHiveMonthWithoutYearBindStillUsesYearFolder() {
        PartitionSpec spec = PartitionSpec.parse("month(ts)");
        List<String> notices = new ArrayList<>();
        spec.emitListingNotices(Set.of("year", "month"), List.of(hint("ts", Operator.GREATER_THAN, MARCH_15_2024)), notices::add);
        assertThat(notices, empty());
        assertFalse(spec.overlaps(Map.of("year", 2024, "month", 2), List.of(hint("ts", Operator.GREATER_THAN, MARCH_15_2024))));
    }

    public void testTimestampBoundsBecomeYearInForAtTimestamp() {
        Instant start = Instant.parse("2024-06-01T00:00:00Z");
        Instant end = Instant.parse("2025-01-01T00:00:00Z");
        PartitionSpec spec = PartitionSpec.parse("year(@timestamp), month(@timestamp)");
        Map<String, List<PartitionFilterHint>> hints = PartitionSpec.addTimestampBounds(
            Map.of(),
            Map.of("s3://logs/**", Map.of(PartitionSpec.CONFIG_PARTITION_SPEC, "year(@timestamp), month(@timestamp)")),
            start,
            end
        );
        List<PartitionFilterHint> projected = spec.projectListingHints(hints.get("s3://logs/**"));
        assertEquals(List.of(2024, 2025), inValues(projected, "year"));
        assertEquals(List.of(6, 7, 8, 9, 10, 11, 12, 1), inValues(projected, "month"));
        assertTrue(
            PartitionSpec.addTimestampBounds(
                Map.of(),
                Map.of("s3://logs/**", Map.of(PartitionSpec.CONFIG_PARTITION_SPEC, "year(ts)")),
                start,
                end
            ).isEmpty()
        );
    }

    public void testClosedRangeEndingAtYearStartStillIncludesThatYear() {
        Instant start = Instant.parse("2024-06-01T00:00:00Z");
        Instant end = Instant.parse("2026-01-01T00:00:00Z");
        PartitionSpec spec = PartitionSpec.parse("year(@timestamp), month(@timestamp)");
        Map<String, List<PartitionFilterHint>> hints = PartitionSpec.addTimestampBounds(
            Map.of(),
            Map.of("s3://logs/**", Map.of(PartitionSpec.CONFIG_PARTITION_SPEC, "year(@timestamp), month(@timestamp)")),
            start,
            end
        );
        assertEquals(List.of(2024, 2025, 2026), inValues(spec.projectListingHints(hints.get("s3://logs/**")), "year"));
    }

    public void testDateNanosExclusiveGreaterThanKeepsContainingMillisecondFolder() {
        PartitionSpec spec = PartitionSpec.parse("year(ts), month(ts), day(ts)");
        Instant bound = Instant.parse("2024-06-15T23:59:59.999000001Z");
        List<PartitionFilterHint> hints = List.of(hint("ts", Operator.GREATER_THAN, bound));
        assertTrue("June 15 still has (bound, midnight)", spec.overlaps(folder(2024, 6, 15), hints));
        assertFalse("June 14 is entirely before the bound", spec.overlaps(folder(2024, 6, 14), hints));
        long nanos = bound.getEpochSecond() * 1_000_000_000L + bound.getNano();
        Expression filter = new GreaterThan(SRC, dateNanosField("ts"), new Literal(SRC, nanos, DataType.DATE_NANOS));
        assertTrue(spec.overlapsExpressions(folder(2024, 6, 15), List.of(filter)));
        assertFalse(spec.overlapsExpressions(folder(2024, 6, 14), List.of(filter)));
    }

    public void testDateNanosExclusiveLessThanKeepsContainingMillisecondFolder() {
        PartitionSpec spec = PartitionSpec.parse("year(ts), month(ts), day(ts)");
        Instant bound = Instant.parse("2024-06-16T00:00:00.000000001Z");
        List<PartitionFilterHint> hints = List.of(hint("ts", Operator.LESS_THAN, bound));
        assertTrue("June 16 still has [midnight, bound)", spec.overlaps(folder(2024, 6, 16), hints));
        assertFalse("June 17 is entirely after the bound", spec.overlaps(folder(2024, 6, 17), hints));
        long nanos = bound.getEpochSecond() * 1_000_000_000L + bound.getNano();
        Expression filter = new LessThan(SRC, dateNanosField("ts"), new Literal(SRC, nanos, DataType.DATE_NANOS));
        assertTrue(spec.overlapsExpressions(folder(2024, 6, 16), List.of(filter)));
        assertFalse(spec.overlapsExpressions(folder(2024, 6, 17), List.of(filter)));
    }

    public void testAliasIdentityValuesDoesNotOverwriteExistingColumn() {
        PartitionSpec spec = PartitionSpec.parse("aws-region=region");
        Map<String, Object> values = new HashMap<>(Map.of("aws-region", "eu", "region", "us"));
        spec.aliasIdentityValues(values);
        assertEquals("us", values.get("region"));
    }

    public void testIdentityOnTimestampWarns() {
        PartitionSpec spec = PartitionSpec.parse("dt=@timestamp");
        List<String> notices = new ArrayList<>();
        spec.emitListingNotices(Set.of("dt"), List.of(), notices::add);
        assertThat(notices, hasItem(containsString("binds [dt] with identity to the date column [@timestamp]")));
        assertThat(notices, hasItem(containsString("year/month/day/hour")));
    }

    public void testIdentityOnMappedDateWarns() {
        PartitionSpec spec = PartitionSpec.parse("dt=event_time");
        List<String> notices = new ArrayList<>();
        spec.emitListingNotices(Set.of("dt"), List.of(), Map.of("event_time", DataType.DATETIME), notices::add);
        assertThat(notices, hasItem(containsString("binds [dt] with identity to the date column [event_time]")));
    }

    public void testIdentityOnKeywordDoesNotWarn() {
        PartitionSpec spec = PartitionSpec.parse("aws-region=region");
        List<String> notices = new ArrayList<>();
        spec.emitListingNotices(Set.of("aws-region"), List.of(), notices::add);
        assertThat(notices, empty());
    }

    public void testPathRenameWarnsWithoutRewriting() {
        PartitionSpec spec = PartitionSpec.parse("year(start)");
        List<String> notices = new ArrayList<>();
        spec.emitListingNotices(Set.of("year"), List.of(), null, Map.of("start", "@timestamp"), notices::add);
        assertThat(notices, hasItem(containsString("binds [start]")));
        assertThat(notices, hasItem(containsString("mapping field [@timestamp]")));
        assertThat(notices, hasItem(containsString("renames with path")));
        assertThat(notices, hasItem(containsString("bind [@timestamp]")));
    }

    public void testLagKeepsNextHourAndDay() {
        PartitionSpec noLag = PartitionSpec.parse("year(ts), month(ts), day(ts), hour(ts)");
        PartitionSpec lag = PartitionSpec.parse("year(ts), month(ts), day(ts), hour(ts), lag(ts, 15m)");
        Instant ten = Instant.parse("2024-06-15T10:00:00Z");
        Instant eleven = Instant.parse("2024-06-15T11:00:00Z");
        List<PartitionFilterHint> hourHints = List.of(
            hint("ts", Operator.GREATER_THAN_OR_EQUAL, ten),
            hint("ts", Operator.LESS_THAN, eleven)
        );
        assertTrue(noLag.overlaps(hourFolder(10), hourHints));
        assertFalse("next hour is after the window without lag", noLag.overlaps(hourFolder(11), hourHints));
        assertTrue("lag 15m reaches into hour 11", lag.overlaps(hourFolder(11), hourHints));

        Instant dayStart = Instant.parse("2024-06-15T00:00:00Z");
        Instant dayEnd = Instant.parse("2024-06-16T00:00:00Z");
        List<PartitionFilterHint> dayHints = List.of(
            hint("ts", Operator.GREATER_THAN_OR_EQUAL, dayStart),
            hint("ts", Operator.LESS_THAN, dayEnd)
        );
        assertTrue(noLag.overlaps(folder(2024, 6, 15), dayHints));
        assertFalse(noLag.overlaps(folder(2024, 6, 16), dayHints));
        assertTrue(lag.overlaps(folder(2024, 6, 16), dayHints));
    }

    public void testLeadKeepsPreviousHour() {
        PartitionSpec lead = PartitionSpec.parse("year(ts), month(ts), day(ts), hour(ts), lead(ts, 15m)");
        Instant eleven = Instant.parse("2024-06-15T11:00:00Z");
        Instant noon = Instant.parse("2024-06-15T12:00:00Z");
        List<PartitionFilterHint> hints = List.of(hint("ts", Operator.GREATER_THAN_OR_EQUAL, eleven), hint("ts", Operator.LESS_THAN, noon));
        PartitionSpec none = PartitionSpec.parse("year(ts), month(ts), day(ts), hour(ts)");
        assertFalse(none.overlaps(hourFolder(10), hints));
        assertTrue(lead.overlaps(hourFolder(10), hints));
        assertTrue(lead.overlaps(hourFolder(11), hints));
    }

    public void testLagIntoNextYearIsListed() {
        PartitionSpec spec = PartitionSpec.parse("year(ts), month(ts), day(ts), hour(ts), lag(ts, 15m)");
        Instant start = Instant.parse("2024-12-31T23:50:00Z");
        Instant end = Instant.parse("2024-12-31T23:51:00Z");
        List<PartitionFilterHint> hints = List.of(hint("ts", Operator.GREATER_THAN_OR_EQUAL, start), hint("ts", Operator.LESS_THAN, end));
        List<PartitionFilterHint> projected = spec.projectListingHints(hints);
        assertEquals(List.of(2024, 2025), inValues(projected, "year"));
        assertEquals(List.of(12, 1), inValues(projected, "month"));
        assertEquals(List.of(31, 1), inValues(projected, "day"));
        assertEquals(List.of(23, 0), inValues(projected, "hour"));
        PartitionSpec noLag = PartitionSpec.parse("year(ts), month(ts), day(ts), hour(ts)");
        assertEquals(List.of(2024), inValues(noLag.projectListingHints(hints), "year"));
        assertEquals(List.of(12), inValues(noLag.projectListingHints(hints), "month"));
        assertEquals(List.of(31), inValues(noLag.projectListingHints(hints), "day"));
        assertEquals(List.of(23), inValues(noLag.projectListingHints(hints), "hour"));
    }

    public void testPointEqualsWithLeadWidensPreviousFolder() {
        PartitionSpec spec = PartitionSpec.parse("year(ts), month(ts), day(ts), hour(ts), lead(ts, 15m)");
        Instant midnight = Instant.parse("2024-06-15T00:00:00Z");
        List<PartitionFilterHint> hints = List.of(hint("ts", Operator.EQUALS, midnight));
        assertTrue(spec.overlaps(hourFolder(0), hints));
        Map<String, Object> prevHour = Map.of("year", 2024, "month", 6, "day", 14, "hour", 23);
        assertTrue("lead 15m from midnight reaches 23:45 of the previous day", spec.overlaps(prevHour, hints));
        PartitionSpec none = PartitionSpec.parse("year(ts), month(ts), day(ts), hour(ts)");
        assertFalse(none.overlaps(prevHour, hints));
    }

    public void testTwoColumnsYearInIntersects() {
        PartitionSpec spec = PartitionSpec.parse("year(start), year(end)");
        Instant startLo = Instant.parse("2024-01-01T00:00:00Z");
        Instant startHi = Instant.parse("2026-01-01T00:00:00Z");
        Instant endLo = Instant.parse("2025-01-01T00:00:00Z");
        Instant endHi = Instant.parse("2027-01-01T00:00:00Z");
        List<PartitionFilterHint> hints = List.of(
            hint("start", Operator.GREATER_THAN_OR_EQUAL, startLo),
            hint("start", Operator.LESS_THAN, startHi),
            hint("end", Operator.GREATER_THAN_OR_EQUAL, endLo),
            hint("end", Operator.LESS_THAN, endHi)
        );
        List<PartitionFilterHint> projected = spec.projectListingHints(hints);
        PartitionFilterHint yearIn = projected.stream()
            .filter(h -> "year".equals(h.columnName()) && h.operator() == Operator.IN)
            .findFirst()
            .orElseThrow();
        assertEquals(List.of(2025), yearIn.values());
    }

    public void testPerColumnLagIsIndependent() {
        PartitionSpec spec = PartitionSpec.parse(
            "year(start), month(start), day(start), hour(start), year(end), month(end), day(end), hour(end), lag(start, 20m), lag(end, 10m)"
        );
        Instant ten = Instant.parse("2024-06-15T10:00:00Z");
        Instant eleven = Instant.parse("2024-06-15T11:00:00Z");
        List<PartitionFilterHint> startHour = List.of(
            hint("start", Operator.GREATER_THAN_OR_EQUAL, ten),
            hint("start", Operator.LESS_THAN, eleven)
        );
        assertTrue("start lag 20m keeps hour 11", spec.overlaps(hourFolder(11), startHour));
        List<PartitionFilterHint> endHour = List.of(
            hint("end", Operator.GREATER_THAN_OR_EQUAL, ten),
            hint("end", Operator.LESS_THAN, eleven)
        );
        assertTrue("end lag 10m keeps hour 11", spec.overlaps(hourFolder(11), endHour));
        Map<String, Object> hour12 = Map.of("year", 2024, "month", 6, "day", 15, "hour", 12);
        assertFalse("end lag 10m from 11:00 does not reach hour 12", spec.overlaps(hour12, endHour));
        List<PartitionFilterHint> startTight = List.of(
            hint("start", Operator.GREATER_THAN_OR_EQUAL, Instant.parse("2024-06-15T10:50:00Z")),
            hint("start", Operator.LESS_THAN, eleven)
        );
        assertTrue("start lag 20m from 11:00 keeps hour 11", spec.overlaps(hourFolder(11), startTight));
        assertFalse("start lag 20m from 11:00 does not reach hour 12", spec.overlaps(hour12, startTight));
    }

    public void testLagWithEpochSecondAndDatetimeNanosHints() {
        PartitionSpec seconds = PartitionSpec.parse("year(start, epoch_second), month(start, epoch_second), lag(start, 15m)");
        // 2024-06-15T10:50:00Z as epoch seconds.
        long tenFifty = Instant.parse("2024-06-15T10:50:00Z").getEpochSecond();
        long eleven = Instant.parse("2024-06-15T11:00:00Z").getEpochSecond();
        List<PartitionFilterHint> numeric = List.of(
            hint("start", Operator.GREATER_THAN_OR_EQUAL, tenFifty),
            hint("start", Operator.LESS_THAN, eleven)
        );
        assertTrue(seconds.overlaps(folder(2024, 6, null), numeric));
        assertTrue("lag 15m from 11:00 keeps June", seconds.overlaps(folder(2024, 6, null), numeric));

        PartitionSpec nanos = PartitionSpec.parse("year(ts), month(ts), day(ts), hour(ts), lag(ts, 15m)");
        Instant ten = Instant.parse("2024-06-15T10:00:00Z");
        Instant elevenInstant = Instant.parse("2024-06-15T11:00:00Z");
        List<PartitionFilterHint> datetime = List.of(
            hint("ts", Operator.GREATER_THAN_OR_EQUAL, ten),
            hint("ts", Operator.LESS_THAN, elevenInstant)
        );
        assertTrue(nanos.overlaps(hourFolder(11), datetime));
    }

    private static PartitionFilterHint hint(String column, Operator op, Object... values) {
        return new PartitionFilterHint(column, op, List.of(values));
    }

    private static FieldAttribute datetimeField(String name) {
        return new FieldAttribute(SRC, name, new EsField(name, DataType.DATETIME, Map.of(), false, EsField.TimeSeriesFieldType.NONE));
    }

    private static FieldAttribute dateNanosField(String name) {
        return new FieldAttribute(SRC, name, new EsField(name, DataType.DATE_NANOS, Map.of(), false, EsField.TimeSeriesFieldType.NONE));
    }

    private static Literal datetimeLiteral(long millis) {
        return new Literal(SRC, millis, DataType.DATETIME);
    }

    private static List<Object> inValues(List<PartitionFilterHint> hints, String key) {
        for (PartitionFilterHint hint : hints) {
            if (key.equals(hint.columnName()) && hint.operator() == Operator.IN) {
                return hint.values();
            }
        }
        return null;
    }

    private static Map<String, Object> hourFolder(int hour) {
        return Map.of("year", 2024, "month", 6, "day", 15, "hour", hour);
    }

    private static Map<String, Object> folder(int year, int month, Integer day) {
        if (day == null) {
            return Map.of("year", year, "month", month);
        }
        return Map.of("year", year, "month", month, "day", day);
    }
}
