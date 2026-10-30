/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.xpack.esql.TestAnalyzer;
import org.elasticsearch.xpack.esql.VersionMode;
import org.elasticsearch.xpack.esql.analysis.AnalyzerTestCase;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MetricsInfo;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

public class MetricsInfoTimeRangeRewriterTests extends AnalyzerTestCase {
    private static final Instant NOW = Instant.parse("2026-10-29T12:00:00Z");
    private static final TimeValue CAP = TimeValue.timeValueHours(1);
    private static final String WARNING = "METRICS_INFO and TS_INFO are limited to the most recent [1h]";
    private static final List<String> INFO_COMMANDS = List.of("METRICS_INFO", "TS_INFO");
    /**
     * A range of a day, which a cap of an hour narrows if it is on and the plan has an info command.
     */
    private static final QueryBuilder WIDE_RANGE = range(NOW.minus(1, ChronoUnit.DAYS), NOW);

    public MetricsInfoTimeRangeRewriterTests(VersionMode versionMode) {
        super(versionMode);
    }

    public void testDisabled() {
        for (String command : INFO_COMMANDS) {
            LogicalPlan plan = analyze(k8s(), "TS k8s | " + command);
            assertSame(plan, MetricsInfoTimeRangeRewriter.capTimeRange(plan, TimeValue.MINUS_ONE, WIDE_RANGE, NOW));
            assertSame(plan, MetricsInfoTimeRangeRewriter.capTimeRange(plan, TimeValue.ZERO, WIDE_RANGE, NOW));
        }
    }

    public void testNoRequestedRangeReadsTheMostRecentCap() {
        for (String command : INFO_COMMANDS) {
            LogicalPlan plan = analyze(k8s(), "TS k8s | " + command);
            Instant from = NOW.minus(1, ChronoUnit.HOURS);
            assertLimitedFrom(from, MetricsInfoTimeRangeRewriter.capTimeRange(plan, CAP, null, NOW));
            assertWarnings(WARNING);
        }
    }

    public void testRangeWiderThanTheCapIsNarrowedToItsMostRecentPart() {
        for (String command : INFO_COMMANDS) {
            LogicalPlan plan = analyze(k8s(), "TS k8s | " + command);
            QueryBuilder requested = range(NOW.minus(5, ChronoUnit.HOURS), NOW.minus(1, ChronoUnit.HOURS));
            // [max(t1, t2 - cap), t2] where t2 is the end of the requested range, not the start of the query
            assertLimitedFrom(NOW.minus(2, ChronoUnit.HOURS), MetricsInfoTimeRangeRewriter.capTimeRange(plan, CAP, requested, NOW));
            assertWarnings(WARNING);
        }
    }

    public void testRangeWithinTheCapIsKept() {
        for (String command : INFO_COMMANDS) {
            LogicalPlan plan = analyze(k8s(), "TS k8s | " + command);
            QueryBuilder requested = range(NOW.minus(30, ChronoUnit.MINUTES), NOW);
            assertSame(plan, MetricsInfoTimeRangeRewriter.capTimeRange(plan, CAP, requested, NOW));
            // a range as wide as the cap is within it too
            QueryBuilder asWideAsTheCap = range(NOW.minus(1, ChronoUnit.HOURS), NOW);
            assertSame(plan, MetricsInfoTimeRangeRewriter.capTimeRange(plan, CAP, asWideAsTheCap, NOW));
        }
    }

    /**
     * The range query rounds {@code lte} up, so {@code "lte": "2026-10-29"} ends at the end of that day. Reading it as the start of the
     * day would make the range look empty, and so within the cap, although it covers a whole day.
     */
    public void testDateOnlyUpperBoundEndsAtTheEndOfTheDay() {
        LogicalPlan plan = analyze(k8s(), "TS k8s | METRICS_INFO");
        QueryBuilder requested = QueryBuilders.rangeQuery("@timestamp").gte("2026-10-29").lte("2026-10-29");
        Instant endOfDay = Instant.parse("2026-10-29T23:59:59.999Z");
        assertLimitedFrom(endOfDay.minus(1, ChronoUnit.HOURS), MetricsInfoTimeRangeRewriter.capTimeRange(plan, CAP, requested, NOW));
        assertWarnings(WARNING);
    }

    /**
     * Only LEGACY applies a time zone, so the range is read with it if there is one.
     */
    public void testRangeWithTimeZone() {
        LogicalPlan plan = analyze(k8s(), "TS k8s | METRICS_INFO");
        QueryBuilder requested = QueryBuilders.rangeQuery("@timestamp")
            .gte("2026-10-28T00:00:00")
            .lte("2026-10-29T22:00:00")
            .timeZone("+01:00");
        // the range ends at 2026-10-29T21:00:00Z
        Instant from = Instant.parse("2026-10-29T20:00:00Z");
        assertLimitedFrom(from, MetricsInfoTimeRangeRewriter.capTimeRange(plan, CAP, requested, NOW));
        assertWarnings(WARNING);
    }

    public void testOtherCommandsAreNotLimited() {
        LogicalPlan plan = analyze(k8s(), "TS k8s | LIMIT 10");
        assertSame(plan, MetricsInfoTimeRangeRewriter.capTimeRange(plan, CAP, WIDE_RANGE, NOW));
        assertSame(plan, MetricsInfoTimeRangeRewriter.capTimeRange(plan, CAP, null, NOW));
    }

    /**
     * Only the relations the info command reads are capped. The other FORK branch reads the same index and keeps its range.
     */
    public void testOnlyTheInfoCommandIsLimited() {
        LogicalPlan plan = analyze(k8s(), "TS k8s | FORK (METRICS_INFO) (LIMIT 1)");
        LogicalPlan limited = MetricsInfoTimeRangeRewriter.capTimeRange(plan, CAP, null, NOW);
        assertLimitedFrom(NOW.minus(1, ChronoUnit.HOURS), limited);
        MetricsInfo metricsInfo = limited.collect(MetricsInfo.class).getFirst();
        assertTrue("the bound sits under the info command", metricsInfo.anyMatch(Filter.class::isInstance));
        assertWarnings(WARNING);
    }

    public void testDateNanosTimestamp() {
        LogicalPlan plan = analyze(analyzer().addK8sDateNanos(), "TS datenanos-k8s | METRICS_INFO");
        LogicalPlan limited = MetricsInfoTimeRangeRewriter.capTimeRange(plan, CAP, null, NOW);
        Literal bound = lowerBound(limited);
        assertThat(bound.value(), equalTo(ChronoUnit.NANOS.between(Instant.EPOCH, NOW.minus(1, ChronoUnit.HOURS))));
        assertWarnings(WARNING);
    }

    /**
     * A range between two instants, as the exact epoch millis that the range query uses for a number.
     */
    private static QueryBuilder range(Instant from, Instant to) {
        return QueryBuilders.rangeQuery("@timestamp").gte(from.toEpochMilli()).lte(to.toEpochMilli());
    }

    private TestAnalyzer k8s() {
        return analyzer().addK8s();
    }

    private LogicalPlan analyze(TestAnalyzer analyzer, String query) {
        LogicalPlan plan = analyzer.query(query);
        // The analyzer warns about the missing LIMIT, which has nothing to do with the time range
        takeResponseWarnings();
        return plan;
    }

    private static void assertLimitedFrom(Instant from, LogicalPlan limited) {
        assertThat(lowerBound(limited).value(), equalTo(from.toEpochMilli()));
    }

    /**
     * The bound that the limit added on {@code @timestamp}, sitting right above the source of the info command.
     */
    private static Literal lowerBound(LogicalPlan limited) {
        List<Filter> filters = limited.collect(Filter.class);
        assertThat(filters, hasSize(1));
        Filter filter = filters.get(0);
        assertThat(filter.child(), instanceOf(EsRelation.class));
        assertThat(filter.condition(), instanceOf(GreaterThanOrEqual.class));
        GreaterThanOrEqual condition = (GreaterThanOrEqual) filter.condition();
        assertThat(((NamedExpression) condition.left()).name(), equalTo(MetadataAttribute.TIMESTAMP_FIELD));
        assertThat(condition.right(), instanceOf(Literal.class));
        return (Literal) condition.right();
    }
}
