/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.plan.QuerySettings;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.time.Duration;
import java.time.Period;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_CFG;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_PARSER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.configuration;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

public class TimestampIndexFilterExtractorTests extends ESTestCase {

    public void testExtractNowMinusIntervalAboveDrop() {
        Configuration configuration = configuration("FROM logs*");
        LogicalPlan plan = TEST_PARSER.parseQuery("""
            FROM logs*
            | DROP lost.*
            | WHERE @timestamp >= NOW() - 15 minutes AND @timestamp <= NOW()
            | LIMIT 1
            """);

        QueryBuilder filter = TimestampIndexFilterExtractor.extract(plan, configuration);
        assertThat(filter, instanceOf(BoolQueryBuilder.class));
        BoolQueryBuilder bool = (BoolQueryBuilder) filter;
        assertThat(bool.filter().size(), equalTo(2));

        RangeQueryBuilder lower = (RangeQueryBuilder) bool.filter().get(0);
        RangeQueryBuilder upper = (RangeQueryBuilder) bool.filter().get(1);
        assertDateMathRange(lower, "now-15m", true, null, false, configuration);
        assertDateMathRange(upper, null, false, "now", true, configuration);
    }

    public void testExtractDateStringLiteral() {
        LogicalPlan plan = TEST_PARSER.parseQuery("""
            FROM logs* | WHERE @timestamp >= "2024-01-01T00:00:00Z" AND @timestamp < "2024-02-01T00:00:00Z"
            """);
        QueryBuilder filter = TimestampIndexFilterExtractor.extract(plan, TEST_CFG);
        assertThat(filter, instanceOf(BoolQueryBuilder.class));
        BoolQueryBuilder bool = (BoolQueryBuilder) filter;
        RangeQueryBuilder lower = (RangeQueryBuilder) bool.filter().get(0);
        RangeQueryBuilder upper = (RangeQueryBuilder) bool.filter().get(1);
        assertThat(lower.fieldName(), equalTo("@timestamp"));
        assertThat(lower.from(), equalTo("2024-01-01T00:00:00Z"));
        assertThat(lower.includeLower(), equalTo(true));
        assertThat(lower.timeZone(), nullValue());
        assertThat(upper.to(), equalTo("2024-02-01T00:00:00Z"));
        assertThat(upper.includeUpper(), equalTo(false));
    }

    public void testExtractPastKeepAndDrop() {
        Configuration configuration = configuration("FROM logs*");
        LogicalPlan plan = TEST_PARSER.parseQuery("""
            FROM logs*
            | WHERE @timestamp >= NOW() - 1 hour
            | DROP lost.*
            | KEEP @timestamp, message
            """);
        QueryBuilder filter = TimestampIndexFilterExtractor.extract(plan, configuration);
        assertThat(filter, instanceOf(RangeQueryBuilder.class));
        assertDateMathRange((RangeQueryBuilder) filter, "now-1h", true, null, false, configuration);
    }

    public void testExtractNowMinusDaySetsTimeZone() {
        Configuration configuration = configuration("FROM logs*");
        LogicalPlan plan = TEST_PARSER.parseQuery("FROM logs* | WHERE @timestamp >= NOW() - 1 day");
        QueryBuilder filter = TimestampIndexFilterExtractor.extract(plan, configuration);
        assertThat(filter, instanceOf(RangeQueryBuilder.class));
        assertDateMathRange((RangeQueryBuilder) filter, "now-1d", true, null, false, configuration);
    }

    public void testNoExtractAboveLimit() {
        LogicalPlan plan = TEST_PARSER.parseQuery("""
            FROM logs*
            | LIMIT 10
            | WHERE @timestamp >= "2024-01-01"
            """);
        assertThat(TimestampIndexFilterExtractor.extract(plan, TEST_CFG), nullValue());
    }

    public void testNoExtractWhenTimestampShadowedByEval() {
        LogicalPlan plan = TEST_PARSER.parseQuery("""
            FROM logs*
            | EVAL @timestamp = 1
            | WHERE @timestamp >= "2024-01-01"
            """);
        assertThat(TimestampIndexFilterExtractor.extract(plan, TEST_CFG), nullValue());
    }

    public void testExtractWhenTimestampFilterBelowEvalShadow() {
        LogicalPlan plan = TEST_PARSER.parseQuery("""
            FROM logs*
            | WHERE @timestamp >= "2024-01-01"
            | EVAL @timestamp = 1
            """);
        QueryBuilder filter = TimestampIndexFilterExtractor.extract(plan, TEST_CFG);
        assertThat(filter, instanceOf(RangeQueryBuilder.class));
        assertThat(((RangeQueryBuilder) filter).from(), equalTo("2024-01-01"));
    }

    public void testIgnoresNonTimestampPredicates() {
        LogicalPlan plan = TEST_PARSER.parseQuery("""
            FROM logs*
            | WHERE status == "error" AND @timestamp >= "2024-01-01"
            """);
        QueryBuilder filter = TimestampIndexFilterExtractor.extract(plan, TEST_CFG);
        assertThat(filter, instanceOf(RangeQueryBuilder.class));
        assertThat(((RangeQueryBuilder) filter).from(), equalTo("2024-01-01"));
    }

    public void testNoExtractWithoutTimestamp() {
        LogicalPlan plan = TEST_PARSER.parseQuery("FROM logs* | WHERE status == \"error\"");
        assertThat(TimestampIndexFilterExtractor.extract(plan, TEST_CFG), nullValue());
    }

    public void testForkKeepsCommonTimestampFilter() {
        LogicalPlan plan = TEST_PARSER.parseQuery("""
            FROM logs*
            | FORK
                (WHERE @timestamp >= "2024-01-01" AND status == "a")
                (WHERE @timestamp >= "2024-01-01" AND status == "b")
            | LIMIT 1
            """);
        QueryBuilder filter = TimestampIndexFilterExtractor.extract(plan, TEST_CFG);
        assertThat(filter, instanceOf(RangeQueryBuilder.class));
        assertThat(((RangeQueryBuilder) filter).from(), equalTo("2024-01-01"));
    }

    public void testForkKeepsPartialCommonTimestampFilters() {
        LogicalPlan plan = TEST_PARSER.parseQuery("""
            FROM logs*
            | FORK
                (WHERE @timestamp >= "2024-01-01" AND @timestamp < "2024-06-01")
                (WHERE @timestamp >= "2024-01-01" AND @timestamp < "2025-01-01")
            | LIMIT 1
            """);
        QueryBuilder filter = TimestampIndexFilterExtractor.extract(plan, TEST_CFG);
        assertThat(filter, instanceOf(RangeQueryBuilder.class));
        assertThat(((RangeQueryBuilder) filter).from(), equalTo("2024-01-01"));
        assertThat(((RangeQueryBuilder) filter).to(), nullValue());
    }

    public void testForkSkipsWhenTimestampFiltersDiffer() {
        LogicalPlan plan = TEST_PARSER.parseQuery("""
            FROM logs*
            | FORK
                (WHERE @timestamp >= "2024-01-01")
                (WHERE @timestamp >= "2023-01-01")
            | LIMIT 1
            """);
        assertThat(TimestampIndexFilterExtractor.extract(plan, TEST_CFG), nullValue());
    }

    public void testForkSkipsWhenOneBranchHasNoTimestampFilter() {
        LogicalPlan plan = TEST_PARSER.parseQuery("""
            FROM logs*
            | FORK
                (WHERE @timestamp >= "2024-01-01")
                (WHERE status == "error")
            | LIMIT 1
            """);
        assertThat(TimestampIndexFilterExtractor.extract(plan, TEST_CFG), nullValue());
    }

    public void testToDateMathUnit() {
        assertThat(TimestampIndexFilterExtractor.toDateMathUnit(Duration.ofMinutes(15)), equalTo("15m"));
        assertThat(TimestampIndexFilterExtractor.toDateMathUnit(Duration.ofHours(1)), equalTo("1h"));
        assertThat(TimestampIndexFilterExtractor.toDateMathUnit(Duration.ofHours(24)), equalTo("24h"));
        assertThat(TimestampIndexFilterExtractor.toDateMathUnit(Duration.ofHours(48)), equalTo("48h"));
        assertThat(TimestampIndexFilterExtractor.toDateMathUnit(Duration.ofSeconds(30)), equalTo("30s"));
        assertThat(TimestampIndexFilterExtractor.toDateMathUnit(Period.ofDays(1)), equalTo("1d"));
        assertThat(TimestampIndexFilterExtractor.toDateMathUnit(Period.ofWeeks(2)), equalTo("2w"));
        assertThat(TimestampIndexFilterExtractor.toDateMathUnit(Period.ofMonths(3)), equalTo("3M"));
        assertThat(TimestampIndexFilterExtractor.toDateMathUnit(Period.ofYears(1)), equalTo("1y"));
        assertThat(TimestampIndexFilterExtractor.toDateMathUnit(Duration.ofMillis(500)), nullValue());
    }

    private static void assertDateMathRange(
        RangeQueryBuilder range,
        String from,
        boolean includeLower,
        String to,
        boolean includeUpper,
        Configuration configuration
    ) {
        assertThat(range.fieldName(), equalTo("@timestamp"));
        assertThat(range.format(), nullValue());
        assertThat(range.timeZone(), equalTo(QuerySettings.TIME_ZONE.get(configuration.resolvedSettings()).getId()));
        if (from != null) {
            assertThat(range.from(), equalTo(from));
            assertThat(range.includeLower(), equalTo(includeLower));
        }
        if (to != null) {
            assertThat(range.to(), equalTo(to));
            assertThat(range.includeUpper(), equalTo(includeUpper));
        }
    }
}
