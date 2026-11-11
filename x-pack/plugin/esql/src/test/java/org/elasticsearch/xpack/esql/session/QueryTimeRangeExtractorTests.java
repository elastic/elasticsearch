/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.session.QueryTimeRangeExtractor.TimeRange;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_CFG;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_PARSER;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.equalTo;

public class QueryTimeRangeExtractorTests extends ESTestCase {

    private static final long NOW = TEST_CFG.now().toEpochMilli();
    private static final long MINUTES_15 = Duration.ofMinutes(15).toMillis();
    private static final long T1 = Instant.parse("2024-05-01T00:00:00Z").toEpochMilli();
    private static final long T2 = Instant.parse("2024-05-02T00:00:00Z").toEpochMilli();

    public void testNoTimeRange() {
        assertRanges("FROM logs", Map.of("logs", TimeRange.UNBOUNDED));
        assertRanges("FROM logs | WHERE level == \"error\"", Map.of("logs", TimeRange.UNBOUNDED));
        assertRanges("ROW a = 1", Map.of());
        assertThat(QueryTimeRangeExtractor.extract(TEST_PARSER.parseQuery("FROM logs | LIMIT 1"), TEST_CFG), anEmptyMap());
    }

    public void testComparisons() {
        assertRange("@timestamp >= \"2024-05-01T00:00:00Z\"", T1, null);
        assertRange("@timestamp > \"2024-05-01T00:00:00Z\"", T1, null);
        assertRange("@timestamp <= \"2024-05-01T00:00:00Z\"", null, T1);
        assertRange("@timestamp < \"2024-05-01T00:00:00Z\"", null, T1);
        assertRange("@timestamp == \"2024-05-01T00:00:00Z\"", T1, T1);
        // With the constant on the left the comparison reads the other way around.
        assertRange("\"2024-05-01T00:00:00Z\" <= @timestamp", T1, null);
        assertRange("\"2024-05-01T00:00:00Z\" > @timestamp", null, T1);
    }

    public void testNow() {
        assertRange("@timestamp >= NOW() - 15 minutes AND @timestamp <= NOW()", NOW - MINUTES_15, NOW);
        assertRange("@timestamp >= now() - 15 minutes", NOW - MINUTES_15, null);
        assertRange("@timestamp >= NOW() - 1 hour - 15 minutes", NOW - Duration.ofMinutes(75).toMillis(), null);
        assertRange("@timestamp <= NOW() + 15 minutes", null, NOW + MINUTES_15);
        assertRange("@timestamp <= 15 minutes + NOW()", null, NOW + MINUTES_15);
        assertRange("@timestamp >= NOW() - 1 month", TEST_CFG.now().atZone(ZoneOffset.UTC).minusMonths(1).toInstant().toEpochMilli(), null);
    }

    public void testDateArithmeticOnStrings() {
        assertRange("@timestamp >= \"2024-05-02T00:00:00Z\" - 1 day", T1, null);
    }

    public void testCasts() {
        for (String date : new String[] {
            "\"2024-05-01T00:00:00Z\"::datetime",
            "\"2024-05-01T00:00:00Z\"::date",
            "TO_DATETIME(\"2024-05-01T00:00:00Z\")",
            "to_dt(\"2024-05-01T00:00:00Z\")",
            T1 + "::datetime",
            "TO_DATETIME(" + T1 + ")",
            "\"2024-05-01T00:00:00Z\"::datetime::datetime",
            // Nanoseconds are rounded down to the millisecond.
            "\"2024-05-01T00:00:00.000999999Z\"::date_nanos",
            "TO_DATE_NANOS(\"2024-05-01T00:00:00.000000001Z\")",
            "TO_DATE_NANOS(" + T1 + "000001)",
            "\"2024-05-01T00:00:00Z\"::datetime::date_nanos",
            "(\"2024-05-02T00:00:00Z\"::datetime - 1 day)::date_nanos" }) {
            assertRange("@timestamp >= " + date, T1, null);
            assertRange("@timestamp <= " + date, null, T1);
        }
        assertRange("@timestamp >= NOW()::datetime - 15 minutes", NOW - MINUTES_15, null);
        // Nanoseconds do not go back beyond the epoch.
        assertRange("@timestamp >= TO_DATE_NANOS(-1)", null, null);
    }

    public void testDateTrunc() {
        long today = TEST_CFG.now().atZone(ZoneOffset.UTC).truncatedTo(ChronoUnit.DAYS).toInstant().toEpochMilli();
        assertRange("@timestamp >= DATE_TRUNC(1 day, NOW())", today, null);
        assertRange("@timestamp >= date_trunc(1 day, NOW()) - 1 day", today - Duration.ofDays(1).toMillis(), null);
        assertRange("@timestamp >= DATE_TRUNC(1 day, NOW() - 1 day)", today - Duration.ofDays(1).toMillis(), null);
        assertRange("@timestamp >= DATE_TRUNC(1 day, \"2024-05-01T13:14:15Z\")", T1, null);
        assertRange("@timestamp < DATE_TRUNC(1 hour, \"2024-05-01T00:59:59Z\"::datetime)", null, T1);
        assertRange("@timestamp >= DATE_TRUNC(1 month, \"2024-05-17T13:14:15Z\")", T1, null);
        assertRange("@timestamp >= DATE_TRUNC(1 day, TO_DATE_NANOS(\"2024-05-01T13:14:15.123456789Z\"))", T1, null);
    }

    public void testDateParse() {
        assertRange("@timestamp >= DATE_PARSE(\"2024-05-01T00:00:00Z\")", T1, null);
        assertRange("@timestamp >= DATE_PARSE(\"yyyy-MM-dd\", \"2024-05-01\")", T1, null);
        assertRange("@timestamp <= date_parse(\"dd/MM/yyyy HH:mm\", \"02/05/2024 00:00\")", null, T2);
        assertRange("@timestamp >= DATE_PARSE(\"yyyy-MM-dd\", \"2024-05-02\") - 1 day", T1, null);
        assertRange("@timestamp >= DATE_TRUNC(1 month, DATE_PARSE(\"yyyy-MM-dd\", \"2024-05-17\"))", T1, null);
        // Neither a date that does not match the pattern, nor one that is not a constant, nor the options are understood.
        assertRange("@timestamp >= DATE_PARSE(\"yyyy-MM-dd\", \"01/05/2024\")", null, null);
        assertRange("@timestamp >= DATE_PARSE(\"not a pattern\", \"2024-05-01\")", null, null);
        assertRange("@timestamp >= DATE_PARSE(\"yyyy-MM-dd\", other_string)", null, null);
        assertRange("@timestamp >= DATE_PARSE(other_pattern, \"2024-05-01\")", null, null);
        assertRange("@timestamp >= DATE_PARSE(\"yyyy-MM-dd\", \"2024-05-01\", {\"time_zone\": \"Europe/Athens\"})", null, null);
    }

    /**
     * {@code DATE_TRUNC(interval, @timestamp)} is never after {@code @timestamp}, and at most to the end of its bucket before.
     */
    public void testTruncatedTimestamp() {
        long endOfT1Day = T2 - 1;
        long noon = T1 + Duration.ofHours(12).toMillis();
        assertRange("DATE_TRUNC(1 day, @timestamp) == \"2024-05-01T00:00:00Z\"", T1, endOfT1Day);
        assertRange("\"2024-05-01T00:00:00Z\" == date_trunc(1 day, @timestamp)", T1, endOfT1Day);
        assertRange("DATE_TRUNC(1 day, @timestamp) >= \"2024-05-01T00:00:00Z\"", T1, null);
        assertRange("DATE_TRUNC(1 day, @timestamp) > \"2024-05-01T00:00:00Z\"", T1, null);
        assertRange("DATE_TRUNC(1 day, @timestamp) <= \"2024-05-01T00:00:00Z\"", null, endOfT1Day);
        assertRange("DATE_TRUNC(1 day, @timestamp) < \"2024-05-01T00:00:00Z\"", null, endOfT1Day);
        assertRange("\"2024-05-01T00:00:00Z\" >= DATE_TRUNC(1 day, @timestamp)", null, endOfT1Day);
        assertRange("\"2024-05-01T00:00:00Z\" <= DATE_TRUNC(1 day, @timestamp)", T1, null);
        // A constant within a bucket: no truncated date equals it, the range just must not be narrower than that.
        assertRange("DATE_TRUNC(1 day, @timestamp) <= \"2024-05-01T12:00:00Z\"", null, endOfT1Day);
        assertRange("DATE_TRUNC(1 day, @timestamp) >= \"2024-05-01T12:00:00Z\"", noon, null);
        assertRange("DATE_TRUNC(1 day, @timestamp) == \"2024-05-01T12:00:00Z\"", noon, endOfT1Day);
        assertRange(
            "DATE_TRUNC(1 month, @timestamp) == \"2024-05-01T00:00:00Z\"",
            T1,
            Instant.parse("2024-06-01T00:00:00Z").toEpochMilli() - 1
        );
        assertRange(
            "DATE_TRUNC(1 hour, @timestamp) >= DATE_TRUNC(1 hour, NOW() - 1 hour) AND level == \"error\"",
            TEST_CFG.now().atZone(ZoneOffset.UTC).truncatedTo(ChronoUnit.HOURS).minusHours(1).toInstant().toEpochMilli(),
            null
        );

        assertRange("DATE_TRUNC(1 day, other_date) == \"2024-05-01T00:00:00Z\"", null, null);
        assertRange("DATE_TRUNC(other_span, @timestamp) == \"2024-05-01T00:00:00Z\"", null, null);
        assertRange("DATE_TRUNC(1 day, @timestamp) == DATE_TRUNC(1 day, other_date)", null, null);
        assertRange("DATE_TRUNC(1 day, @timestamp) != \"2024-05-01T00:00:00Z\"", null, null);
        assertRange("DATE_TRUNC(1 day, DATE_TRUNC(1 hour, @timestamp)) == \"2024-05-01T00:00:00Z\"", null, null);
    }

    public void testTRange() {
        assertRange("TRANGE(15 minutes)", NOW - MINUTES_15, NOW);
        assertRange("TRANGE(\"2024-05-01T00:00:00Z\", \"2024-05-02T00:00:00Z\")", T1, T2);
        assertRange("TRANGE(" + T1 + ", " + T2 + ")", T1, T2);
        assertRange("TRANGE(15 minutes) AND level == \"error\"", NOW - MINUTES_15, NOW);
    }

    public void testConjunctions() {
        // Any other condition in a conjunction only narrows the result further.
        assertRange("@timestamp >= \"2024-05-01T00:00:00Z\" AND level == \"error\"", T1, null);
        assertRange("level == \"error\" AND (host LIKE \"a*\" AND @timestamp <= \"2024-05-02T00:00:00Z\")", null, T2);
        assertRange("@timestamp >= \"2024-05-01T00:00:00Z\" AND @timestamp >= \"2024-05-02T00:00:00Z\"", T2, null);
    }

    public void testDisjunctions() {
        assertRange("@timestamp >= \"2024-05-01T00:00:00Z\" OR @timestamp >= \"2024-05-02T00:00:00Z\"", T1, null);
        assertRange(
            "(@timestamp >= \"2024-05-01T00:00:00Z\" AND @timestamp <= \"2024-05-01T00:00:00Z\")"
                + " OR (@timestamp >= \"2024-05-02T00:00:00Z\" AND @timestamp <= \"2024-05-02T00:00:00Z\")",
            T1,
            T2
        );
        // One side open on an end leaves that end open.
        assertRange("@timestamp >= \"2024-05-01T00:00:00Z\" OR @timestamp <= \"2024-05-02T00:00:00Z\"", null, null);
        assertRange("@timestamp >= \"2024-05-01T00:00:00Z\" OR level == \"error\"", null, null);
    }

    public void testUnsupportedConditions() {
        assertRange("NOT (@timestamp >= \"2024-05-01T00:00:00Z\")", null, null);
        assertRange("@timestamp != \"2024-05-01T00:00:00Z\"", null, null);
        assertRange("@timestamp >= \"not a date\"", null, null);
        assertRange("@timestamp >= other_date", null, null);
        assertRange("@timestamp >= DATE_TRUNC(1 day, other_date)", null, null);
        assertRange("@timestamp >= DATE_TRUNC(other_span, NOW())", null, null);
        assertRange("@timestamp >= other_date::datetime", null, null);
        assertRange("@timestamp >= \"not a date\"::datetime", null, null);
        assertRange("@timestamp >= TO_LONG(\"2024-05-01T00:00:00Z\")", null, null);
        assertRange("@timestamp >= NOW() - other_span", null, null);
        assertRange("other_date >= NOW() - 15 minutes", null, null);
        assertRange("@timestamp IS NOT NULL", null, null);
    }

    public void testSources() {
        TimeRange from = new TimeRange(T1, null);
        String filter = " | WHERE @timestamp >= \"2024-05-01T00:00:00Z\"";
        assertRanges("FROM logs-*" + filter, Map.of("logs-*", from));
        // The patterns of one FROM are resolved together, and so are filtered together.
        assertRanges("FROM logs-*, metrics-*,-metrics-old" + filter, Map.of("logs-*,metrics-*,-metrics-old", from));
        assertRanges("FROM remote:logs-*, *:metrics" + filter, Map.of("remote:logs-*,*:metrics", from));
        assertRanges("FROM <logs-{now/d}>" + filter, Map.of("<logs-{now/d}>", from));
        assertRanges("FROM logs METADATA _id, _index" + filter, Map.of("logs", from));
        assertRanges("TS metrics" + filter, Map.of("metrics", from));
        assertRanges("ROW @timestamp = NOW()" + filter, Map.of());
        assertRanges("SHOW INFO | WHERE version IS NOT NULL", Map.of());
    }

    public void testNestedSubqueries() {
        assertRanges(
            "FROM (FROM (FROM logs | WHERE @timestamp >= \"2024-05-01T00:00:00Z\") | WHERE level == \"error\"), "
                + "(FROM metrics, (FROM traces | WHERE @timestamp >= \"2024-05-02T00:00:00Z\" | LIMIT 5) | DROP lost.*) "
                + "| WHERE @timestamp <= \"2024-05-02T00:00:00Z\"",
            Map.of("logs", new TimeRange(T1, T2), "metrics", new TimeRange(null, T2), "traces", new TimeRange(T2, null))
        );
        // Whatever the subquery does to its rows after its own filter, that filter still applies to the index.
        assertRanges(
            "FROM (FROM logs | WHERE @timestamp >= \"2024-05-01T00:00:00Z\" | STATS c = COUNT(*) BY @timestamp), metrics "
                + "| WHERE @timestamp <= \"2024-05-02T00:00:00Z\"",
            Map.of("logs", new TimeRange(T1, null), "metrics", new TimeRange(null, T2))
        );
        assertRanges(
            "FROM (FROM logs | EVAL @timestamp = other_date), (FROM metrics | RENAME other_date AS @timestamp) "
                + "| WHERE @timestamp >= \"2024-05-01T00:00:00Z\"",
            Map.of("logs", TimeRange.UNBOUNDED, "metrics", TimeRange.UNBOUNDED)
        );
        // The same pattern read by a subquery and by the query itself.
        assertRanges(
            "FROM logs, (FROM logs | WHERE @timestamp >= \"2024-05-02T00:00:00Z\") | WHERE @timestamp >= \"2024-05-01T00:00:00Z\"",
            Map.of("logs", new TimeRange(T1, null))
        );
    }

    public void testNestedConditions() {
        String from = "@timestamp >= \"2024-05-01T00:00:00Z\"";
        String to = "@timestamp <= \"2024-05-02T00:00:00Z\"";
        String other = "level == \"error\"";
        assertRange("(" + from + " AND (" + other + " AND (" + to + " AND host IS NOT NULL)))", T1, T2);
        assertRange(from + " AND (" + other + " OR (" + to + " AND " + other + "))", T1, null);
        assertRange("(" + from + " OR " + other + ") AND (" + to + " OR " + other + ")", null, null);
        assertRange("(" + from + " OR " + other + ") AND " + to, null, T2);
        assertRange("(" + from + " AND " + other + ") OR (" + from + " AND " + to + ")", T1, null);
        assertRange("(" + from + " AND " + to + ") OR (" + to + " AND " + from + " AND " + other + ")", T1, T2);
        assertRange(from + " AND NOT (" + to + ")", T1, null);
        assertRange("NOT (" + from + " AND " + to + ") AND " + to, null, T2);
        assertRange("(TRANGE(15 minutes) OR " + from + ") AND " + to, Math.min(T1, NOW - MINUTES_15), T2);
        // Ranges that do not overlap leave an empty range, which no index matches.
        assertRange("@timestamp >= \"2024-05-02T00:00:00Z\" AND @timestamp <= \"2024-05-01T00:00:00Z\"", T2, T1);
    }

    public void testFiltersAcrossCommands() {
        assertRanges(
            "FROM logs | WHERE level == \"error\" | EVAL a = 1 | WHERE @timestamp >= \"2024-05-01T00:00:00Z\" | DROP lost.* "
                + "| RENAME level AS severity | SORT @timestamp | WHERE @timestamp <= \"2024-05-02T00:00:00Z\" | KEEP @timestamp "
                + "| LIMIT 10 | WHERE @timestamp >= \"2024-05-02T00:00:00Z\"",
            Map.of("logs", new TimeRange(T1, T2))
        );
        // The filter below the command that resets the range still counts, the one above does not.
        assertRanges(
            "FROM logs | WHERE @timestamp >= \"2024-05-01T00:00:00Z\" | EVAL @timestamp = other_date "
                + "| WHERE @timestamp <= \"2024-05-02T00:00:00Z\"",
            Map.of("logs", new TimeRange(T1, null))
        );
    }

    public void testConsecutiveFilters() {
        assertRanges(
            "FROM logs | WHERE @timestamp >= \"2024-05-01T00:00:00Z\" | WHERE level == \"error\" "
                + "| WHERE @timestamp <= \"2024-05-02T00:00:00Z\"",
            Map.of("logs", new TimeRange(T1, T2))
        );
    }

    public void testCommandsThatKeepTheRange() {
        for (String command : new String[] {
            "EVAL a = level",
            "KEEP @timestamp, level",
            "DROP lost.*",
            "RENAME level AS severity",
            "SORT level" }) {
            assertRanges(
                "FROM logs | " + command + " | WHERE @timestamp >= \"2024-05-01T00:00:00Z\"",
                Map.of("logs", new TimeRange(T1, null))
            );
        }
    }

    /**
     * A filter that comes after one of these commands does not filter the rows of the index: the command either depends on
     * the other rows it reads or may have redefined {@code @timestamp}.
     */
    public void testCommandsThatResetTheRange() {
        for (String command : new String[] {
            "LIMIT 10",
            "SORT level | LIMIT 10",
            "STATS @timestamp = MAX(@timestamp)",
            "INLINE STATS m = MAX(@timestamp)",
            "EVAL @timestamp = other_date",
            "EVAL a = 1, @timestamp = other_date",
            "RENAME other_date AS @timestamp",
            "MV_EXPAND @timestamp",
            "LOOKUP JOIN lookup_index ON level",
            "ENRICH policy ON level",
            "GROK message \"%{WORD:level}\"" }) {
            assertRanges("FROM logs | " + command + " | WHERE @timestamp >= \"2024-05-01T00:00:00Z\"", Map.of("logs", TimeRange.UNBOUNDED));
        }
    }

    public void testFilterBelowOtherCommands() {
        assertRanges(
            "FROM logs | WHERE @timestamp >= \"2024-05-01T00:00:00Z\" | STATS c = COUNT(*) BY level | SORT c DESC | LIMIT 3",
            Map.of("logs", new TimeRange(T1, null))
        );
    }

    public void testLookupIndexIsLeftAlone() {
        assertRanges(
            "FROM logs | WHERE @timestamp >= \"2024-05-01T00:00:00Z\" | LOOKUP JOIN lookup_index ON level",
            Map.of("logs", new TimeRange(T1, null))
        );
    }

    public void testSubqueries() {
        // The filter on the union applies to each branch, on top of the filters of the branch itself.
        assertRanges(
            "FROM logs, (FROM metrics | WHERE @timestamp <= \"2024-05-02T00:00:00Z\"), (FROM traces | STATS BY @timestamp) "
                + "| WHERE @timestamp >= \"2024-05-01T00:00:00Z\"",
            Map.of("logs", new TimeRange(T1, null), "metrics", new TimeRange(T1, T2), "traces", TimeRange.UNBOUNDED)
        );
    }

    public void testPatternReadMoreThanOnce() {
        assertRanges(
            "FROM (FROM logs | WHERE @timestamp >= \"2024-05-02T00:00:00Z\"), "
                + "(FROM logs | WHERE @timestamp >= \"2024-05-01T00:00:00Z\" AND @timestamp <= \"2024-05-02T00:00:00Z\")",
            Map.of("logs", new TimeRange(T1, null))
        );
        assertRanges(
            "FROM (FROM logs | WHERE @timestamp >= \"2024-05-02T00:00:00Z\"), (FROM logs | WHERE level == \"error\")",
            Map.of("logs", TimeRange.UNBOUNDED)
        );
    }

    public void testFork() {
        assertRanges(
            "FROM logs | FORK (WHERE level == \"error\") (WHERE level == \"warn\") | WHERE @timestamp >= \"2024-05-01T00:00:00Z\"",
            Map.of("logs", TimeRange.UNBOUNDED)
        );
        assertRanges(
            "FROM logs | WHERE @timestamp >= \"2024-05-01T00:00:00Z\" | FORK (WHERE level == \"error\") (WHERE level == \"warn\")",
            Map.of("logs", new TimeRange(T1, null))
        );
    }

    public void testTimeSeries() {
        assertRanges(
            "TS metrics | WHERE @timestamp >= \"2024-05-01T00:00:00Z\" | STATS MAX(RATE(requests)) BY host",
            Map.of("metrics", new TimeRange(T1, null))
        );
    }

    /**
     * A PromQL query reads the samples between its start and its end, and as far before the start as it looks back for:
     * the range of a range selector, the lookback of an instant selector, a step, and the offsets of its selectors.
     */
    public void testPromql() {
        String ends = "start=\"2024-05-01T00:00:00Z\" end=\"2024-05-02T00:00:00Z\" ";
        long minutes = Duration.ofMinutes(1).toMillis();
        assertPromqlRange("step=1m " + ends + "(avg(foo))", T1 - 5 * minutes, T2);
        assertPromqlRange("step=10m " + ends + "(avg(foo))", T1 - 10 * minutes, T2);
        assertPromqlRange("step=1m " + ends + "(sum(rate(foo[30m])))", T1 - 30 * minutes, T2);
        assertPromqlRange("step=1m " + ends + "(sum(rate(foo[30m])) / sum(rate(bar[2h])))", T1 - 120 * minutes, T2);
        // An instant query, whose start and end are the same.
        assertPromqlRange("time=\"2024-05-01T00:00:00Z\" (avg(foo))", T1 - 5 * minutes, T1);
        assertPromqlRange("time=\"2024-05-01T00:00:00Z\" (foo[30m])", T1 - 30 * minutes, T1);
    }

    public void testPromqlOffsets() {
        String ends = "step=1m start=\"2024-05-01T00:00:00Z\" end=\"2024-05-02T00:00:00Z\" ";
        long minutes = Duration.ofMinutes(1).toMillis();
        // The range covers every selector, so it reaches back for the largest offset and ends with the smallest.
        assertPromqlRange(ends + "(avg(foo offset 1h))", T1 - 65 * minutes, T2);
        assertPromqlRange(ends + "(foo - foo offset 1d)", T1 - (24 * 60 + 5) * minutes, T2);
        assertPromqlRange(ends + "(sum(rate(foo[30m] offset 1h)))", T1 - 90 * minutes, T2);
        assertPromqlRange(ends + "(avg(foo offset -1h))", T1 - 5 * minutes, T2 + 60 * minutes);
    }

    public void testPromqlWithoutRange() {
        String ends = "step=1m start=\"2024-05-01T00:00:00Z\" end=\"2024-05-02T00:00:00Z\" ";
        // Without both ends the range comes from the request filter.
        assertRanges("PROMQL index=metrics step=1m (avg(foo))", Map.of("metrics", TimeRange.UNBOUNDED));
        // A selector evaluated at a fixed time, and a subquery, read samples that the ends do not bound this simply.
        assertRanges("PROMQL index=metrics " + ends + "(avg(foo @ 1714521600))", Map.of("metrics", TimeRange.UNBOUNDED));
        assertRanges("PROMQL index=metrics " + ends + "(avg(foo @ start()))", Map.of("metrics", TimeRange.UNBOUNDED));
        assertRanges("PROMQL index=metrics " + ends + "(max_over_time(rate(foo[5m])[30m:1m]))", Map.of("metrics", TimeRange.UNBOUNDED));
    }

    /** A filter on the output of a PromQL query is not one on the samples that it reads. */
    public void testPromqlWithFilterOnItsOutput() {
        assertRanges(
            "PROMQL index=metrics step=1m start=\"2024-05-01T00:00:00Z\" end=\"2024-05-02T00:00:00Z\" v=(avg(foo)) "
                + "| WHERE step >= \"2024-05-01T12:00:00Z\" AND v > 1",
            Map.of("metrics", new TimeRange(T1 - Duration.ofMinutes(5).toMillis(), T2))
        );
    }

    /** Without a step, it follows from the number of buckets: an hour here, which is more than the lookback. */
    public void testPromqlWithBuckets() {
        assertPromqlRange(
            "start=\"2024-05-01T00:00:00Z\" end=\"2024-05-02T00:00:00Z\" buckets=24 avg(foo)",
            T1 - Duration.ofHours(1).toMillis(),
            T2
        );
    }

    private static void assertPromqlRange(String query, long from, long to) {
        assertRanges("PROMQL index=metrics " + query, Map.of("metrics", new TimeRange(from, to)));
    }

    /**
     * The walk of the plan is capped. A plan beyond the cap gets no range for any pattern, including those that the walk did
     * reach: a pattern may be read again, without a time range, by the part of the plan that was not walked.
     */
    public void testPlanBeyondCallLimit() {
        String filtered = "FROM logs | WHERE @timestamp >= \"2024-05-01T00:00:00Z\"";
        assertRanges(filtered + " | EVAL a = 1".repeat(50), Map.of("logs", new TimeRange(T1, null)));
        assertRanges(filtered + " | EVAL a = 1".repeat(100), Map.of());
        assertRanges(
            "FROM (" + filtered + "), (FROM logs" + " | EVAL a = 1".repeat(100) + ") | WHERE @timestamp >= \"2024-05-02T00:00:00Z\"",
            Map.of()
        );
    }

    /** The walk of a condition is capped as well. What is left of the condition by then is taken to allow any time. */
    public void testConditionBeyondCallLimit() {
        String others = " AND level == \"error\"".repeat(200);
        assertRange("@timestamp >= \"2024-05-01T00:00:00Z\"" + " AND level == \"error\"".repeat(20), T1, null);
        assertRange("@timestamp >= \"2024-05-01T00:00:00Z\"" + others, null, null);
        // The side of the disjunction that is not walked to its end must not leave the other side to bound the range.
        assertRange("@timestamp >= \"2024-05-02T00:00:00Z\" OR (@timestamp >= \"2024-05-01T00:00:00Z\"" + others + ")", null, null);
        assertRange("@timestamp >= \"2024-05-01T00:00:00Z\"" + " - 1 millisecond + 1 millisecond".repeat(100), null, null);
    }

    /** The upper end is sent as the exclusive start of the next millisecond, which keeps the nanoseconds within the last one. */
    public void testQuery() {
        assertThat(new TimeRange(T1, T2).toQuery(), equalTo(new RangeQueryBuilder("@timestamp").format("epoch_millis").gte(T1).lt(T2 + 1)));
        assertThat(new TimeRange(T1, null).toQuery(), equalTo(new RangeQueryBuilder("@timestamp").format("epoch_millis").gte(T1)));
        assertThat(new TimeRange(null, T2).toQuery(), equalTo(new RangeQueryBuilder("@timestamp").format("epoch_millis").lt(T2 + 1)));

        Map<IndexPattern, QueryBuilder> filters = QueryTimeRangeExtractor.extract(
            TEST_PARSER.parseQuery("FROM logs, (FROM metrics | LIMIT 1) | WHERE @timestamp >= \"2024-05-01T00:00:00Z\""),
            TEST_CFG
        );
        Map<String, QueryBuilder> byName = new HashMap<>();
        filters.forEach((pattern, filter) -> byName.put(pattern.indexPattern(), filter));
        assertThat(byName, equalTo(Map.of("logs", new RangeQueryBuilder("@timestamp").format("epoch_millis").gte(T1))));
    }

    private static void assertRange(String condition, Long from, Long to) {
        assertRanges("FROM logs | WHERE " + condition, Map.of("logs", new TimeRange(from, to)));
    }

    private static void assertRanges(String query, Map<String, TimeRange> expected) {
        Map<String, TimeRange> ranges = new HashMap<>();
        QueryTimeRangeExtractor.ranges(TEST_PARSER.parseQuery(query), TEST_CFG)
            .forEach((pattern, range) -> ranges.put(pattern.indexPattern(), range));
        assertThat(query, ranges, equalTo(expected));
    }
}
