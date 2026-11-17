/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.ml;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.datafeed.esql.spi.TimeBucketing;
import org.elasticsearch.xpack.ml.datafeed.esql.spi.TimeBucketing.BucketSource;
import org.elasticsearch.xpack.ml.datafeed.esql.spi.TimeBucketing.FixedTimeBucket;
import org.elasticsearch.xpack.ml.datafeed.esql.spi.TimeBucketing.NoTimeBucket;
import org.elasticsearch.xpack.ml.datafeed.esql.spi.TimeBucketing.Reason;
import org.elasticsearch.xpack.ml.datafeed.esql.spi.TimeBucketing.UnsupportedTimeBucket;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class EsqlDatafeedTimeBucketingTests extends ESTestCase {

    private static final TimeValue HOUR = TimeValue.timeValueHours(1);
    private final EsqlDatafeedQueryParser parser = new EsqlDatafeedQueryParser();

    public void testBucketOverSourceFieldShouldReportFieldSpanAndColumn() {
        assertThat(
            bucketing("FROM x | STATS c = COUNT(*) BY b = BUCKET(@timestamp, 1 hour)"),
            equalTo(new FixedTimeBucket(HOUR, new BucketSource.SourceField("@timestamp"), Set.of("b")))
        );
    }

    public void testBinAliasShouldBeResolvedThroughTheFunctionRegistry() {
        assertThat(
            bucketing("FROM x | STATS c = COUNT(*) BY b = BIN(@timestamp, 1 hour)"),
            equalTo(new FixedTimeBucket(HOUR, new BucketSource.SourceField("@timestamp"), Set.of("b")))
        );
    }

    public void testDateTruncShouldReportBucketWithEvalDefinedGrouping() {
        assertThat(
            bucketing("FROM x | EVAL h = DATE_TRUNC(1 hour, @timestamp) | STATS c = COUNT(*) BY h, host"),
            equalTo(new FixedTimeBucket(HOUR, new BucketSource.SourceField("@timestamp"), Set.of("h")))
        );
    }

    public void testStringSpanShouldBeParsedAsEsqlDoes() {
        assertThat(fixed("FROM x | STATS c = COUNT(*) BY b = BUCKET(@timestamp, \"1 hour\")").span(), equalTo(HOUR));
        assertUnsupported("FROM x | STATS c = COUNT(*) BY b = BUCKET(@timestamp, \"1 day\")", Reason.CALENDAR_SPAN);
    }

    public void testCalendarAndAutoSpansShouldBeUnsupported() {
        assertUnsupported("FROM x | STATS c = COUNT(*) BY b = BUCKET(@timestamp, 1 month)", Reason.CALENDAR_SPAN);
        assertUnsupported("FROM x | STATS c = COUNT(*) BY b = DATE_TRUNC(1 year, @timestamp)", Reason.CALENDAR_SPAN);
        assertUnsupported("FROM x | STATS c = COUNT(*) BY b = BUCKET(@timestamp, 20, \"2024-01-01\", \"2024-01-02\")", Reason.AUTO_SPAN);
    }

    public void testSeveralBucketsShouldBeUnsupported() {
        assertUnsupported(
            "FROM x | STATS c = COUNT(*) BY a = BUCKET(@timestamp, 1 hour), b = BUCKET(other, 1 minute)",
            Reason.SEVERAL_BUCKETS
        );
    }

    public void testQueriesWithoutTimeBucketShouldReportNoBucket() {
        for (String query : List.of(
            "FROM x | STATS c = COUNT(*) BY host",
            "FROM x | STATS c = COUNT(*) BY b = BUCKET(salary, 1000)",
            "FROM x | KEEP a",
            "ROW a = 1"
        )) {
            assertThat(query, bucketing(query), instanceOf(NoTimeBucket.class));
        }
    }

    public void testChainedEvalAndRenameShouldReportSourceField() {
        for (String query : List.of(
            "FROM x | EVAL t = @timestamp | EVAL h = BUCKET(t, 1 hour) | STATS c = COUNT(*) BY h",
            "FROM x | RENAME @timestamp AS t | STATS c = COUNT(*) BY h = BUCKET(t, 1 hour)",
            "FROM x | EVAL h = BUCKET(@timestamp, 1 hour) | RENAME h AS g | STATS c = COUNT(*) BY g"
        )) {
            assertThat(query, fixed(query).source(), equalTo(new BucketSource.SourceField("@timestamp")));
        }
    }

    public void testShadowedTimestampShouldReportTheFieldItIsDefinedFrom() {
        assertThat(
            fixed("FROM x | EVAL @timestamp = event.ingested | STATS c = COUNT(*) BY BUCKET(@timestamp, 1 hour)").source(),
            equalTo(new BucketSource.SourceField("event.ingested"))
        );
    }

    public void testComputedBucketFieldShouldBeReportedAsComputed() {
        for (String query : List.of(
            "FROM x | EVAL t = TO_DATETIME(raw) | STATS c = COUNT(*) BY BUCKET(t, 1 hour)",
            "FROM x | EVAL t = @timestamp + 1 hour | RENAME t AS u | STATS c = COUNT(*) BY BUCKET(u, 1 hour)",
            "FROM x | STATS c = COUNT(*) BY BUCKET(TO_DATETIME(raw), 1 hour)"
        )) {
            assertThat(query, fixed(query).source(), instanceOf(BucketSource.ComputedValue.class));
        }
    }

    public void testTbucketShouldReportImplicitTimestamp() {
        assertThat(fixed("TS m | STATS max(cpu) BY TBUCKET(1 hour)").source(), instanceOf(BucketSource.ImplicitTimestamp.class));
    }

    public void testGroupingNamedByAliasOrSourceText() {
        assertThat(
            fixed("FROM x | STATS c = COUNT(*) BY BUCKET(@timestamp, 1 hour)").outputColumns(),
            equalTo(Set.of("BUCKET(@timestamp, 1 hour)"))
        );
        assertThat(fixed("FROM x | STATS c = COUNT(*) BY bucket = BUCKET(@timestamp, 1 hour)").outputColumns(), equalTo(Set.of("bucket")));
    }

    public void testOutputColumnsShouldFollowCommandsAfterStats() {
        String stats = "FROM x | STATS c = COUNT(*) BY b = BUCKET(@timestamp, 1 hour)";
        assertThat(fixed(stats + " | RENAME b AS ts").outputColumns(), equalTo(Set.of("ts")));
        assertThat(fixed(stats + " | EVAL ts = b + 1 hour - 1 millisecond").outputColumns(), equalTo(Set.of("b", "ts")));
        assertThat(fixed(stats + " | EVAL b = c, d = b + c").outputColumns(), empty());
        assertThat(fixed(stats + ", host | KEEP c, host").outputColumns(), empty());
        assertThat(fixed(stats + " | EVAL ts = b | KEEP t*").outputColumns(), equalTo(Set.of("ts")));
        assertThat(fixed(stats + " | EVAL ts = b | DROP b").outputColumns(), equalTo(Set.of("ts")));
        assertThat(fixed(stats + " | EVAL ts = b | KEEP *").outputColumns(), equalTo(Set.of("b", "ts")));
    }

    public void testRegroupingLowerStatsBucketShouldBeReportedOnTopStats() {
        FixedTimeBucket bucket = fixed(
            "FROM x | STATS c = COUNT(*) BY b = BUCKET(@timestamp, 1 hour), host | STATS m = MAX(c) BY b | WHERE m > 0 | SORT b"
        );
        assertThat(bucket, equalTo(new FixedTimeBucket(HOUR, new BucketSource.SourceField("@timestamp"), Set.of("b"))));
    }

    public void testTopStatsWithoutBucketShouldReportNoBucket() {
        assertThat(
            bucketing("FROM x | STATS c = COUNT(*) BY b = BUCKET(@timestamp, 1 hour) | STATS n = SUM(c)"),
            instanceOf(NoTimeBucket.class)
        );
        assertThat(bucketing("FROM x | STATS b = MAX(@timestamp) BY host | STATS c = COUNT(*) BY b"), instanceOf(NoTimeBucket.class));
    }

    public void testNestedTimeFunctionGroupingShouldNotBeReported() {
        for (String query : List.of(
            "FROM x | STATS c = COUNT(*) BY s = TO_STRING(DATE_TRUNC(1 hour, @timestamp))",
            "FROM x | STATS c = COUNT(*) BY b = BUCKET(@timestamp, 1 hour) + 1 hour",
            "FROM x | EVAL h = BUCKET(@timestamp, 1 hour) + 1 hour | STATS c = COUNT(*) BY h"
        )) {
            assertThat(query, bucketing(query), instanceOf(NoTimeBucket.class));
        }
    }

    public void testBucketBehindForkJoinOrInlineStatsShouldNotBeReported() {
        for (String query : List.of(
            "FROM x | EVAL h = DATE_TRUNC(1 hour, @timestamp) | FORK (WHERE a > 1) (WHERE a < 1) | STATS c = COUNT(*) BY h",
            "FROM x | EVAL h = DATE_TRUNC(1 hour, @timestamp) | LOOKUP JOIN l ON k | STATS c = COUNT(*) BY h",
            "FROM x | EVAL h = DATE_TRUNC(1 hour, @timestamp) | INLINE STATS m = MAX(a) BY h | STATS c = COUNT(*) BY h"
        )) {
            assertThat(query, bucketing(query), instanceOf(NoTimeBucket.class));
        }
    }

    public void testRenamedAndKeptColumnShouldTraceToSourceField() {
        assertThat(sourceFieldOf("FROM x | RENAME @timestamp AS ts | KEEP ts, host", "ts"), equalTo(Optional.of("@timestamp")));
        assertThat(
            sourceFieldOf("FROM x | EVAL ts = @timestamp | WHERE host == \"a\" | KEEP ts", "ts"),
            equalTo(Optional.of("@timestamp"))
        );
        assertThat(sourceFieldOf("FROM x | WHERE a > 1", "@timestamp"), equalTo(Optional.of("@timestamp")));
    }

    public void testComputedOrRemovedColumnShouldHaveNoSourceField() {
        assertThat(sourceFieldOf("FROM x | EVAL ts = TO_DATETIME(raw) | KEEP ts", "ts"), equalTo(Optional.empty()));
        assertThat(sourceFieldOf("FROM x | RENAME @timestamp AS ts | KEEP host", "ts"), equalTo(Optional.empty()));
        assertThat(sourceFieldOf("FROM x | DROP @timestamp", "@timestamp"), equalTo(Optional.empty()));
        assertThat(sourceFieldOf("FROM x | FORK (WHERE a > 1) (WHERE a < 1)", "@timestamp"), equalTo(Optional.empty()));
    }

    private TimeBucketing bucketing(String query) {
        return parser.parse(query).timeBucketing();
    }

    private FixedTimeBucket fixed(String query) {
        TimeBucketing bucketing = bucketing(query);
        assertThat(query, bucketing, instanceOf(FixedTimeBucket.class));
        return (FixedTimeBucket) bucketing;
    }

    private void assertUnsupported(String query, Reason reason) {
        assertThat(query, bucketing(query), equalTo(new UnsupportedTimeBucket(reason)));
    }

    private Optional<String> sourceFieldOf(String query, String outputColumn) {
        return parser.parse(query).sourceFieldOf(outputColumn);
    }
}
