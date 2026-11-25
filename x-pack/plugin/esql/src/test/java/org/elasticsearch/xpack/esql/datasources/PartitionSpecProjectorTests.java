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
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.datasources.PartitionFilterHintExtractor.BoundRange;
import org.elasticsearch.xpack.esql.datasources.PartitionFilterHintExtractor.Operator;
import org.elasticsearch.xpack.esql.datasources.PartitionFilterHintExtractor.PartitionFilterHint;
import org.elasticsearch.xpack.esql.datasources.glob.FolderRangePredicate;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvInRange;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
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

    /**
     * The merge branch of the single-spec form, which the map form's own tests never reach because all three pass
     * empty hints. The two bounds come first, in GTE-then-LTE order, with the query's own hints after them.
     */
    public void testWithTimestampBoundsPrependsBoundsToExistingHints() {
        Instant start = Instant.parse("2024-06-01T00:00:00Z");
        Instant end = Instant.parse("2025-01-01T00:00:00Z");
        PartitionSpec spec = PartitionSpec.parse("year(@timestamp)");
        PartitionFilterHint existing = hint("region", Operator.EQUALS, "emea");

        List<PartitionFilterHint> merged = spec.withTimestampBounds(List.of(existing), start, end);

        assertEquals(3, merged.size());
        assertEquals(MetadataAttribute.TIMESTAMP_FIELD, merged.get(0).columnName());
        assertEquals(Operator.GREATER_THAN_OR_EQUAL, merged.get(0).operator());
        assertEquals(List.of(start), merged.get(0).values());
        assertEquals(MetadataAttribute.TIMESTAMP_FIELD, merged.get(1).columnName());
        assertEquals(Operator.LESS_THAN_OR_EQUAL, merged.get(1).operator());
        assertEquals(List.of(end), merged.get(1).values());
        assertEquals(existing, merged.get(2));
    }

    /** A spec that does not bind {@code @timestamp} gets its hints back by identity, bounds or no bounds. */
    public void testWithTimestampBoundsLeavesAnUnboundSpecAlone() {
        List<PartitionFilterHint> hints = List.of(hint("region", Operator.EQUALS, "emea"));
        PartitionSpec spec = PartitionSpec.parse("year(ts)");

        Instant start = Instant.parse("2024-06-01T00:00:00Z");
        Instant end = Instant.parse("2025-01-01T00:00:00Z");

        assertSame(hints, spec.withTimestampBounds(hints, start, end));
        assertNull(spec.withTimestampBounds(null, start, end));
        // The absent-bounds arm, which is what EsqlSession passes for every spec when the request carries no filter.
        assertSame(hints, spec.withTimestampBounds(hints, null, null));
        assertSame(hints, spec.withTimestampBounds(hints, end, start));
    }

    public void testTimestampBoundsBecomeYearInForAtTimestamp() {
        Instant start = Instant.parse("2024-06-01T00:00:00Z");
        Instant end = Instant.parse("2025-01-01T00:00:00Z");
        PartitionSpec spec = PartitionSpec.parse("year(@timestamp), month(@timestamp)");
        List<PartitionFilterHint> projected = spec.projectListingHints(spec.withTimestampBounds(null, start, end));
        assertEquals(List.of(2024, 2025), inValues(projected, "year"));
        assertEquals(List.of(6, 7, 8, 9, 10, 11, 12, 1), inValues(projected, "month"));
        // A spec binding some other column earns no bounds at all, so its hints come back exactly as they went in.
        assertNull(PartitionSpec.parse("year(ts)").withTimestampBounds(null, start, end));
    }

    public void testClosedRangeEndingAtYearStartStillIncludesThatYear() {
        Instant start = Instant.parse("2024-06-01T00:00:00Z");
        Instant end = Instant.parse("2026-01-01T00:00:00Z");
        PartitionSpec spec = PartitionSpec.parse("year(@timestamp), month(@timestamp)");
        List<PartitionFilterHint> bounds = spec.withTimestampBounds(null, start, end);
        assertEquals(List.of(2024, 2025, 2026), inValues(spec.projectListingHints(bounds), "year"));
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

    public void testFolderRangeIdentityStableWithinUtcDayDiffersAcrossDays() {
        PartitionSpec spec = PartitionSpec.parse("year(ts), month(ts), day(ts)");
        Instant morning = Instant.parse("2026-10-14T01:00:00Z");
        Instant afternoon = Instant.parse("2026-10-14T23:00:00Z");
        Instant nextDay = Instant.parse("2026-10-15T01:00:00Z");
        var a = spec.folderRangePredicate(List.of(hint("ts", Operator.GREATER_THAN_OR_EQUAL, morning)));
        var b = spec.folderRangePredicate(List.of(hint("ts", Operator.GREATER_THAN_OR_EQUAL, afternoon)));
        var c = spec.folderRangePredicate(List.of(hint("ts", Operator.GREATER_THAN_OR_EQUAL, nextDay)));
        assertNotNull(a);
        assertEquals(a.identity(), b.identity());
        assertNotEquals(a.identity(), c.identity());
        assertEquals(List.of("year", "month", "day"), a.bindKeys());
        assertTrue(a.mayContain(Map.of("year", "2026", "month", "10", "day", "14")));
        assertFalse(a.mayContain(Map.of("year", "2026", "month", "10", "day", "13")));
    }

    public void testFolderRangeIdentityDistinguishesInListsThatShareAHull() {
        PartitionSpec spec = PartitionSpec.parse("year(ts), month(ts), day(ts)");
        Instant jan1 = Instant.parse("2024-01-01T00:00:00Z");
        Instant jun15 = Instant.parse("2024-06-15T00:00:00Z");
        Instant dec31 = Instant.parse("2024-12-31T00:00:00Z");
        var ends = spec.folderRangePredicate(List.of(hint("ts", Operator.IN, jan1, dec31)));
        var mid = spec.folderRangePredicate(List.of(hint("ts", Operator.IN, jan1, jun15, dec31)));
        assertNotNull(ends);
        assertNotNull(mid);
        assertNotEquals("IN lists with the same min-max must not share a listing cache entry", ends.identity(), mid.identity());
        assertTrue(ends.mayContain(Map.of("year", "2024", "month", "1", "day", "1")));
        assertFalse(ends.mayContain(Map.of("year", "2024", "month", "6", "day", "15")));
        assertTrue(mid.mayContain(Map.of("year", "2024", "month", "6", "day", "15")));
    }

    /**
     * ES|QL reads {@code "2024-06"} as 2024-06-01, but the projection cannot parse it. An {@code IN} holding it must not
     * narrow to the values that do parse, or June's folders drop out. {@code ts == "2024-06"} alone already restricts
     * nothing, and the {@code IN} must do the same.
     */
    public void testInWithAValueThatDoesNotConvertRestrictsNothing() {
        PartitionSpec spec = PartitionSpec.parse("year(ts), month(ts), day(ts)");
        List<PartitionFilterHint> hints = List.of(hint("ts", Operator.IN, "2024-01-15", "2024-06"));

        assertTrue(spec.overlaps(folder(2024, 6, 1), hints));
        assertTrue(spec.overlaps(folder(2024, 1, 15), hints));
        assertNull(spec.folderRangePredicate(hints));
        assertEquals("no year, month or day hint may be derived", hints, spec.projectListingHints(hints));
    }

    public void testQuantizeRangeAgreesWithRawOnGrainFoldersIncludingLagLead() {
        Instant min = Instant.parse("2000-01-01T00:00:00Z");
        Instant max = Instant.parse("2030-12-31T23:00:00Z");
        for (int i = 0; i < 200; i++) {
            PartitionSpec.Transform grain = randomFrom(
                PartitionSpec.Transform.YEAR,
                PartitionSpec.Transform.MONTH,
                PartitionSpec.Transform.DAY,
                PartitionSpec.Transform.HOUR
            );
            long folderStart = randomAlignedFolderStart(grain, min, max);
            long folderEnd = alignedFolderEnd(folderStart, grain);
            Long start = randomBoolean() ? randomLongBetween(min.toEpochMilli(), max.toEpochMilli()) : null;
            Long end = randomBoolean() ? randomLongBetween(min.toEpochMilli(), max.toEpochMilli()) : null;
            if (start != null && end != null && start > end) {
                long swap = start;
                start = end;
                end = swap;
            }
            long lead = randomBoolean() ? 0L : randomLongBetween(0, 48L * 3_600_000L);
            long lag = randomBoolean() ? 0L : randomLongBetween(0, 48L * 3_600_000L);
            Long widenedStart = start;
            Long widenedEnd = end;
            if (widenedStart != null && lead != 0) {
                try {
                    widenedStart = Math.subtractExact(widenedStart, lead);
                } catch (ArithmeticException overflow) {
                    widenedStart = null;
                }
            }
            if (widenedEnd != null && lag != 0) {
                try {
                    widenedEnd = Math.addExact(widenedEnd, lag);
                } catch (ArithmeticException overflow) {
                    widenedEnd = null;
                }
            }
            PartitionSpec.InstantRange raw = new PartitionSpec.InstantRange(widenedStart, widenedEnd);
            PartitionSpec.InstantRange quantized = PartitionSpec.quantizeRange(raw, grain);
            assertEquals(
                "grain=" + grain + " folder=[" + folderStart + "," + folderEnd + ") raw=" + raw,
                raw.overlapsFolder(folderStart, folderEnd),
                quantized.overlapsFolder(folderStart, folderEnd)
            );
        }
    }

    private static long randomAlignedFolderStart(PartitionSpec.Transform grain, Instant min, Instant max) {
        long millis = randomLongBetween(min.toEpochMilli(), max.toEpochMilli());
        return PartitionSpec.quantizeRange(new PartitionSpec.InstantRange(millis, millis + 1), grain).startInclusiveMillis();
    }

    private static long alignedFolderEnd(long folderStart, PartitionSpec.Transform grain) {
        return PartitionSpec.quantizeRange(new PartitionSpec.InstantRange(folderStart + 1, folderStart + 1), grain).endExclusiveMillis();
    }

    /**
     * Two disjoint {@code @timestamp} windows keep the four hour folders they overlap, not the gap
     * and not the 72 hours of a 3-day layout. The extract hull ({@code >=} first start / {@code <=}
     * last end) covers the gap; {@code folderRangePredicate} must ignore it and match RANGE-only.
     */
    public void testOrOfTwoTimestampWindowsKeepsFourHourFolders() {
        PartitionSpec spec = PartitionSpec.parse("year(ts), month(ts), day(ts), hour(ts)");
        Instant start1 = Instant.parse("2026-11-17T22:00:00Z");
        Instant end1 = Instant.parse("2026-11-18T00:00:00Z");
        Instant start2 = Instant.parse("2026-11-19T04:00:00Z");
        Instant end2 = Instant.parse("2026-11-19T06:00:00Z");
        PartitionFilterHint range = new PartitionFilterHint(
            "ts",
            Operator.RANGE,
            List.of(new BoundRange(start1, true, end1, false), new BoundRange(start2, true, end2, false))
        );
        List<PartitionFilterHint> rangeOnly = List.of(range);
        List<PartitionFilterHint> rangeAndHull = List.of(
            range,
            hint("ts", Operator.GREATER_THAN_OR_EQUAL, start1),
            hint("ts", Operator.LESS_THAN_OR_EQUAL, end2)
        );
        var withHull = spec.folderRangePredicate(rangeAndHull);
        var withoutHull = spec.folderRangePredicate(rangeOnly);
        assertNotNull(withHull);
        assertNotNull(withoutHull);
        assertEquals("coveringRange must not fold disjuncts into the hull", withoutHull.identity(), withHull.identity());
        List<Map<String, String>> expected = List.of(
            Map.of("year", "2026", "month", "11", "day", "17", "hour", "22"),
            Map.of("year", "2026", "month", "11", "day", "17", "hour", "23"),
            Map.of("year", "2026", "month", "11", "day", "19", "hour", "4"),
            Map.of("year", "2026", "month", "11", "day", "19", "hour", "5")
        );
        List<Map<String, String>> kept = hourFoldersIn(2026, 11, 17, 19, withHull);
        assertEquals(expected, kept);
        assertEquals(kept, hourFoldersIn(2026, 11, 17, 19, withoutHull));
        List<PartitionFilterHint> projected = spec.projectListingHints(rangeAndHull);
        assertNull("RANGE is not a listing glob operator", rangeHint(projected, "ts"));
        assertEquals(List.of(2026), inValues(projected, "year"));
        assertEquals(List.of(11), inValues(projected, "month"));
        // Grain INs are a superset: hour IN (22,23,4,5) × day IN (17,19) is 8 glob combos.
        // The folder-range predicate, not the IN, is the keep-set. Do not hull the INs.
        assertEquals(List.of(17, 19), inValues(projected, "day"));
        assertEquals(List.of(22, 23, 4, 5), inValues(projected, "hour"));
    }

    /**
     * Nov 17 22:00–Nov 18 02:00 OR Dec 3 22:00–Dec 4 02:00: eight hour folders, not the hull (~408 h)
     * and not the grain-IN cross-product as a keep set. Scan Nov 17–20 and Dec 1–5 so a too-open
     * predicate cannot pass by matching only the eight positives.
     */
    public void testOrOfNovAndDecWindowsKeepsEightHourFolders() {
        PartitionSpec spec = PartitionSpec.parse("year(ts), month(ts), day(ts), hour(ts)");
        Instant start1 = Instant.parse("2026-11-17T22:00:00Z");
        Instant end1 = Instant.parse("2026-11-18T02:00:00Z");
        Instant start2 = Instant.parse("2026-12-03T22:00:00Z");
        Instant end2 = Instant.parse("2026-12-04T02:00:00Z");
        List<PartitionFilterHint> hints = List.of(
            new PartitionFilterHint(
                "ts",
                Operator.RANGE,
                List.of(new BoundRange(start1, true, end1, false), new BoundRange(start2, true, end2, false))
            )
        );
        var predicate = spec.folderRangePredicate(hints);
        assertNotNull(predicate);
        List<Map<String, String>> kept = new ArrayList<>();
        kept.addAll(hourFoldersIn(2026, 11, 17, 20, predicate));
        kept.addAll(hourFoldersIn(2026, 12, 1, 5, predicate));
        assertEquals(
            List.of(
                Map.of("year", "2026", "month", "11", "day", "17", "hour", "22"),
                Map.of("year", "2026", "month", "11", "day", "17", "hour", "23"),
                Map.of("year", "2026", "month", "11", "day", "18", "hour", "0"),
                Map.of("year", "2026", "month", "11", "day", "18", "hour", "1"),
                Map.of("year", "2026", "month", "12", "day", "3", "hour", "22"),
                Map.of("year", "2026", "month", "12", "day", "3", "hour", "23"),
                Map.of("year", "2026", "month", "12", "day", "4", "hour", "0"),
                Map.of("year", "2026", "month", "12", "day", "4", "hour", "1")
            ),
            kept
        );
        List<PartitionFilterHint> projected = spec.projectListingHints(hints);
        // Grain INs still LIST the independent cross-product (month × day × hour). Keep-set is
        // the predicate. A temporal hull of the INs would list the Nov–Dec gap; do not add one.
        assertEquals(List.of(11, 12), inValues(projected, "month"));
        assertEquals(List.of(17, 18, 3, 4), inValues(projected, "day"));
        assertEquals(List.of(22, 23, 0, 1), inValues(projected, "hour"));
    }

    public void testOrOfTimestampWindowsSameIdentityEitherBranchOrder() {
        PartitionSpec spec = PartitionSpec.parse("year(ts), month(ts), day(ts), hour(ts)");
        Instant start1 = Instant.parse("2026-11-17T22:00:00Z");
        Instant end1 = Instant.parse("2026-11-18T00:00:00Z");
        Instant start2 = Instant.parse("2026-11-19T04:00:00Z");
        Instant end2 = Instant.parse("2026-11-19T06:00:00Z");
        var a = spec.folderRangePredicate(
            List.of(
                new PartitionFilterHint(
                    "ts",
                    Operator.RANGE,
                    List.of(new BoundRange(start1, true, end1, false), new BoundRange(start2, true, end2, false))
                )
            )
        );
        var b = spec.folderRangePredicate(
            List.of(
                new PartitionFilterHint(
                    "ts",
                    Operator.RANGE,
                    List.of(new BoundRange(start2, true, end2, false), new BoundRange(start1, true, end1, false))
                )
            )
        );
        assertNotNull(a);
        assertEquals(a.identity(), b.identity());
    }

    /**
     * Lag on two live windows keeps the lag margin of each, not the gap between them.
     */
    public void testOrOfTimestampWindowsLagKeepsMarginNotGap() {
        PartitionSpec spec = PartitionSpec.parse("year(ts), month(ts), day(ts), hour(ts), lag(ts, 15m)");
        Instant start1 = Instant.parse("2026-11-17T22:00:00Z");
        Instant end1 = Instant.parse("2026-11-18T00:00:00Z");
        Instant start2 = Instant.parse("2026-11-19T04:00:00Z");
        Instant end2 = Instant.parse("2026-11-19T06:00:00Z");
        List<PartitionFilterHint> hints = List.of(
            new PartitionFilterHint(
                "ts",
                Operator.RANGE,
                List.of(new BoundRange(start1, true, end1, false), new BoundRange(start2, true, end2, false))
            )
        );
        var predicate = spec.folderRangePredicate(hints);
        assertNotNull(predicate);
        assertTrue("lag 15m from 00:00 reaches hour 00 of the 18th", predicate.mayContain(hourBinding(2026, 11, 18, 0)));
        assertFalse("gap hour is not a lag margin", predicate.mayContain(hourBinding(2026, 11, 18, 3)));
        assertTrue(predicate.mayContain(hourBinding(2026, 11, 17, 23)));
        assertTrue(predicate.mayContain(hourBinding(2026, 11, 19, 4)));
    }

    /**
     * An inverted {@code BoundRange} is an empty clip. Lag must not put it in the cache key or
     * turn it into a live window; identity matches the live interval alone.
     */
    public void testLagDoesNotWidenAnEmptyDisjunct() {
        PartitionSpec spec = PartitionSpec.parse("year(ts), month(ts), day(ts), hour(ts), lag(ts, 15m)");
        Instant start1 = Instant.parse("2026-11-17T22:00:00Z");
        Instant end1 = Instant.parse("2026-11-18T00:00:00Z");
        Instant invertedLower = Instant.parse("2026-11-19T04:00:00Z");
        Instant invertedUpper = Instant.parse("2026-11-17T22:00:00Z");
        var withEmpty = spec.folderRangePredicate(
            List.of(
                new PartitionFilterHint(
                    "ts",
                    Operator.RANGE,
                    List.of(new BoundRange(start1, true, end1, false), new BoundRange(invertedLower, true, invertedUpper, false))
                )
            )
        );
        var liveOnly = spec.folderRangePredicate(
            List.of(new PartitionFilterHint("ts", Operator.RANGE, List.of(new BoundRange(start1, true, end1, false))))
        );
        assertNotNull(withEmpty);
        assertEquals(liveOnly.identity(), withEmpty.identity());
        assertFalse(withEmpty.mayContain(hourBinding(2026, 11, 19, 4)));
    }

    /**
     * Two AND'd RANGE hints (each one OR) intersect. Union would keep Nov 17 22:00 even though
     * the second conjunct excludes it.
     */
    public void testAndOfTwoRangeHintsIntersectsDisjuncts() {
        PartitionSpec spec = PartitionSpec.parse("year(ts), month(ts), day(ts), hour(ts)");
        Instant start1 = Instant.parse("2026-11-17T22:00:00Z");
        Instant end1 = Instant.parse("2026-11-18T00:00:00Z");
        Instant start2 = Instant.parse("2026-11-19T04:00:00Z");
        Instant end2 = Instant.parse("2026-11-19T06:00:00Z");
        Instant start3 = Instant.parse("2026-11-18T00:00:00Z");
        Instant end3 = Instant.parse("2026-11-18T02:00:00Z");
        List<PartitionFilterHint> hints = List.of(
            new PartitionFilterHint(
                "ts",
                Operator.RANGE,
                List.of(new BoundRange(start1, true, end1, false), new BoundRange(start2, true, end2, false))
            ),
            new PartitionFilterHint(
                "ts",
                Operator.RANGE,
                List.of(new BoundRange(start3, true, end3, false), new BoundRange(start2, true, end2, false))
            )
        );
        var predicate = spec.folderRangePredicate(hints);
        assertNotNull(predicate);
        assertEquals(
            List.of(
                Map.of("year", "2026", "month", "11", "day", "19", "hour", "4"),
                Map.of("year", "2026", "month", "11", "day", "19", "hour", "5")
            ),
            hourFoldersIn(2026, 11, 17, 19, predicate)
        );
    }

    /**
     * Quoted DATE_NANOS exclusive upper inside a millisecond must keep the 22:00 folder.
     */
    public void testQuotedNanosExclusiveUpperKeepsBoundaryHour() {
        PartitionSpec spec = PartitionSpec.parse("year(ts), month(ts), day(ts), hour(ts)");
        Instant start = Instant.parse("2026-11-17T22:00:00Z");
        List<PartitionFilterHint> hints = List.of(
            new PartitionFilterHint("ts", Operator.RANGE, List.of(new BoundRange(start, true, "2026-11-17T22:00:00.000000001Z", false)))
        );
        var predicate = spec.folderRangePredicate(hints);
        assertNotNull(predicate);
        assertTrue(predicate.mayContain(hourBinding(2026, 11, 17, 22)));
        assertFalse(predicate.mayContain(hourBinding(2026, 11, 17, 21)));
        assertFalse(predicate.mayContain(hourBinding(2026, 11, 17, 23)));
    }

    /**
     * One value that is not a {@link BoundRange} discards the whole {@code RANGE} hint. Keeping only
     * the convertible branch would hide folders the omitted interval still matches.
     */
    public void testRangeHintWithANonBoundRangeValueRestrictsNothing() {
        PartitionSpec spec = PartitionSpec.parse("year(ts), month(ts), day(ts), hour(ts)");
        Instant start1 = Instant.parse("2026-11-17T22:00:00Z");
        Instant end1 = Instant.parse("2026-11-18T00:00:00Z");
        List<PartitionFilterHint> hints = List.of(
            new PartitionFilterHint("ts", Operator.RANGE, List.of(new BoundRange(start1, true, end1, false), "not-a-range"))
        );
        assertNull(spec.folderRangePredicate(hints));
        assertTrue(spec.overlaps(Map.of("year", 2026, "month", 11, "day", 18, "hour", 3), hints));
    }

    public void testOrOfTimestampWindowsFromConjunctsMatchesManualRange() {
        PartitionSpec spec = PartitionSpec.parse("year(ts), month(ts), day(ts), hour(ts)");
        Instant start1 = Instant.parse("2026-11-17T22:00:00Z");
        Instant end1 = Instant.parse("2026-11-18T00:00:00Z");
        Instant start2 = Instant.parse("2026-11-19T04:00:00Z");
        Instant end2 = Instant.parse("2026-11-19T06:00:00Z");
        Expression filter = new Or(
            SRC,
            new And(
                SRC,
                new GreaterThanOrEqual(SRC, datetimeField("ts"), datetimeLiteral(start1.toEpochMilli())),
                new LessThan(SRC, datetimeField("ts"), datetimeLiteral(end1.toEpochMilli()))
            ),
            new And(
                SRC,
                new GreaterThanOrEqual(SRC, datetimeField("ts"), datetimeLiteral(start2.toEpochMilli())),
                new LessThan(SRC, datetimeField("ts"), datetimeLiteral(end2.toEpochMilli()))
            )
        );
        List<PartitionFilterHint> extracted = PartitionFilterHintExtractor.fromConjuncts(
            List.of(filter),
            Set.of(),
            spec.boundColumns(),
            PartitionFilterHintExtractor.TEMPORAL
        );
        var fromExtracted = spec.folderRangePredicate(extracted);
        var manual = spec.folderRangePredicate(
            List.of(
                new PartitionFilterHint(
                    "ts",
                    Operator.RANGE,
                    List.of(new BoundRange(start1, true, end1, false), new BoundRange(start2, true, end2, false))
                )
            )
        );
        assertNotNull(fromExtracted);
        assertEquals(manual.identity(), fromExtracted.identity());
        assertEquals(hourFoldersIn(2026, 11, 17, 19, manual), hourFoldersIn(2026, 11, 17, 19, fromExtracted));
    }

    /** Plain identity remap: the hull listing {@code eu}…{@code us-east-1} is the allowed gap. */
    public void testIdentityRemapOrOfClosedRangesProjectsHull() {
        PartitionSpec spec = PartitionSpec.parse("aws-region=region");
        List<PartitionFilterHint> hints = List.of(
            new PartitionFilterHint(
                "region",
                Operator.RANGE,
                List.of(new BoundRange("eu", true, "eu-west-1", true), new BoundRange("us", true, "us-east-1", true))
            ),
            hint("region", Operator.GREATER_THAN_OR_EQUAL, "eu"),
            hint("region", Operator.LESS_THAN_OR_EQUAL, "us-east-1")
        );
        List<PartitionFilterHint> projected = spec.projectListingHints(hints);
        assertNull(rangeHint(projected, "region"));
        assertNull(rangeHint(projected, "aws-region"));
        assertEquals("aws-region", projected.get(0).columnName());
        assertEquals(Operator.GREATER_THAN_OR_EQUAL, projected.get(0).operator());
        assertEquals(List.of("eu"), projected.get(0).values());
        assertEquals(Operator.LESS_THAN_OR_EQUAL, projected.get(1).operator());
        assertEquals(List.of("us-east-1"), projected.get(1).values());
    }

    public void testWraparoundPlainColumnProjectsNoHull() {
        PartitionSpec spec = PartitionSpec.EMPTY;
        List<PartitionFilterHint> hints = List.of(
            new PartitionFilterHint(
                "hour",
                Operator.RANGE,
                List.of(new BoundRange(22, true, null, true), new BoundRange(null, true, 1, true))
            )
        );
        assertEquals(List.of(), spec.projectListingHints(hints));
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

    private static PartitionFilterHint rangeHint(List<PartitionFilterHint> hints, String key) {
        for (PartitionFilterHint hint : hints) {
            if (key.equals(hint.columnName()) && hint.operator() == Operator.RANGE) {
                return hint;
            }
        }
        return null;
    }

    private static List<Map<String, String>> hourFoldersIn(int year, int month, int fromDay, int toDay, FolderRangePredicate predicate) {
        List<Map<String, String>> kept = new ArrayList<>();
        for (int day = fromDay; day <= toDay; day++) {
            for (int hour = 0; hour < 24; hour++) {
                Map<String, String> folder = hourBinding(year, month, day, hour);
                if (predicate.mayContain(folder)) {
                    kept.add(folder);
                }
            }
        }
        return kept;
    }

    private static Map<String, String> hourBinding(int year, int month, int day, int hour) {
        return Map.of(
            "year",
            Integer.toString(year),
            "month",
            Integer.toString(month),
            "day",
            Integer.toString(day),
            "hour",
            Integer.toString(hour)
        );
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
