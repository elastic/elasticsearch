/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.PartitionFilterHintExtractor.Operator;
import org.elasticsearch.xpack.esql.datasources.PartitionFilterHintExtractor.PartitionFilterHint;

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

        assertEquals(
            List.of(
                hint("ts", Operator.GREATER_THAN_OR_EQUAL, start),
                hint("ts", Operator.LESS_THAN, end),
                hint("year", Operator.IN, 2024, 2025)
            ),
            spec.projectListingHints(hints)
        );
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
        assertEquals(
            List.of(
                hint("year", Operator.EQUALS, 2024),
                hint("ts", Operator.GREATER_THAN_OR_EQUAL, start),
                hint("ts", Operator.LESS_THAN, end),
                hint("year", Operator.IN, 2024, 2025)
            ),
            spec.projectListingHints(hints)
        );
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
        assertEquals(
            List.of(hint("ts", Operator.GREATER_THAN_OR_EQUAL, start), hint("ts", Operator.LESS_THAN, end), hint("yyy", Operator.IN, 2024)),
            spec.projectListingHints(hints, Set.of("yyy"))
        );

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
        assertThat(notices, hasItem(containsString("second")));
        assertThat(notices, hasItem(containsString("millis")));
    }

    public void testWrongUnitWarnsForMillisReadAsSeconds() {
        PartitionSpec spec = PartitionSpec.parse("year(start, second)");
        List<String> notices = new ArrayList<>();
        spec.emitListingNotices(Set.of("year"), List.of(hint("start", Operator.GREATER_THAN, 1_710_000_000_000L)), notices::add);
        assertThat(notices, hasItem(containsString("calendar year [")));
        assertThat(notices, hasItem(containsString("second")));
        String yearNotice = notices.stream().filter(n -> n.contains("calendar year [")).findFirst().orElseThrow();
        int year = Integer.parseInt(yearNotice.replaceAll(".*calendar year \\[(-?\\d+)].*", "$1"));
        assertTrue(year > PartitionSpec.WRONG_UNIT_YEAR_MAX);
    }

    public void testWrongUnitLessThanDoesNotDropFolders() {
        PartitionSpec spec = PartitionSpec.parse("year(start), month(start)");
        List<PartitionFilterHint> hints = List.of(hint("start", Operator.LESS_THAN, 1_710_000_000L));
        assertTrue("wrong-unit < must not empty the 2024 folder", spec.overlaps(folder(2024, 6, null), hints));
        assertEquals(hints, spec.projectListingHints(hints));
    }

    public void testWrongUnitDoesNotWarnForDatetimeLiteral() {
        PartitionSpec spec = PartitionSpec.parse("year(ts)");
        List<String> notices = new ArrayList<>();
        spec.emitListingNotices(Set.of("year"), List.of(hint("ts", Operator.GREATER_THAN, MARCH_15_2024)), notices::add);
        assertThat(notices, empty());
    }

    public void testSecondsUnitConvertsNumericBound() {
        PartitionSpec spec = PartitionSpec.parse("year(start, second), month(start, second)");
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

    public void testAliasIdentityValuesDoesNotOverwriteExistingColumn() {
        PartitionSpec spec = PartitionSpec.parse("aws-region=region");
        Map<String, Object> values = new HashMap<>(Map.of("aws-region", "eu", "region", "us"));
        spec.aliasIdentityValues(values);
        assertEquals("us", values.get("region"));
    }

    private static PartitionFilterHint hint(String column, Operator op, Object... values) {
        return new PartitionFilterHint(column, op, List.of(values));
    }

    private static Map<String, Object> folder(int year, int month, Integer day) {
        if (day == null) {
            return Map.of("year", year, "month", month);
        }
        return Map.of("year", year, "month", month, "day", day);
    }
}
