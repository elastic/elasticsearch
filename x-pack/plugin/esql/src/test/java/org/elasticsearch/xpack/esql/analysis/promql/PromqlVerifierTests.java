/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis.promql;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.TestAnalyzer;
import org.elasticsearch.xpack.esql.core.querydsl.QueryDslTimestampBoundsExtractor.TimestampBounds;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class PromqlVerifierTests extends ESTestCase {

    private final TestAnalyzer tsdb = analyzer().addIndex("test", "tsdb-mapping.json").stripErrorPrefix(true);

    public void testPromqlRangeVector() {
        assertThat(
            tsdb.error("PROMQL index=test step=5m network.bytes_in[5m]"),
            equalTo("1:27: invalid expression type \"range vector\" for range query, must be scalar or instant vector")
        );
    }

    public void testPromqlRangeVectorBinaryExpression() {
        assertThat(
            tsdb.error("PROMQL index=test step=5m max(network.bytes_in[5m] / network.bytes_in[5m])"),
            equalTo("1:31: binary expression must contain only scalar and instant vector types")
        );
    }

    public void testPromqlIllegalNameLabelMatcher() {
        assertThat(tsdb.error("PROMQL index=test step=5m (avg({__name__=~\"*.foo.*\"}))"), containsString("Unknown column [__name__]"));
    }

    public void testPromqlSubquery() {
        assertThat(
            tsdb.error("PROMQL index=test step=5m (avg(rate(network.bytes_in[5m:])))"),
            equalTo("1:37: Subquery queries are not supported at this time [network.bytes_in[5m:]]")
        );
        assertThat(
            tsdb.error("PROMQL index=test step=5m (avg(rate(network.bytes_in[5m:1m])))"),
            equalTo("1:37: Subquery queries are not supported at this time [network.bytes_in[5m:1m]]")
        );
    }

    @AwaitsFix(
        bugUrl = "Doesn't parse: line 1:27: Invalid query 'method_code_http_errors_rate5m{code=\"500\"}'"
            + "[ValueExpressionContext] given; expected Expression but found InstantSelector"
    )
    public void testPromqlVectorMatching() {
        assertThat(
            tsdb.error(
                "PROMQL index=test step=5m (method_code_http_errors_rate5m{code=\"500\"} / ignoring(code) method_http_requests_rate5m)"
            ),
            equalTo("")
        );
        assertThat(
            tsdb.error(
                "PROMQL index=test step=5m (method_code_http_errors_rate5m / ignoring(code) group_left method_http_requests_rate5m)"
            ),
            equalTo("")
        );
    }

    public void testPromqlModifier() {
        assertThat(
            tsdb.error("PROMQL index=test step=5m (avg(rate(network.bytes_in[5m] offset 5m)))"),
            equalTo("1:37: offset modifiers are not supported at this time [network.bytes_in[5m] offset 5m]")
        );
        assertThat(
            tsdb.error("PROMQL index=test step=5m start=0 end=1 (avg(foo @ start()))"),
            equalTo("1:46: @ modifiers are not supported at this time [foo @ start()]")
        );
    }

    public void testLogicalSetBinaryOperators() {
        List.of("and", "or", "unless").forEach(op -> {
            assertThat(
                tsdb.error("PROMQL index=test step=5m foo " + op + " bar"),
                containsString("set operators are not supported at this time")
            );
        });
    }

    public void testPromqlInstantQuery() {
        assertThat(
            tsdb.error("PROMQL index=test time=\"2025-10-31T00:00:00Z\" (avg(foo))"),
            containsString("unable to create a bucket; provide either [step] or all of [start], [end], and [buckets]")
        );
    }

    public void testPromqlMissingBucketParameters() {
        assertThat(
            tsdb.error("PROMQL index=test avg(foo)"),
            containsString("unable to create a bucket; provide either [step] or all of [start], [end], and [buckets]")
        );
    }

    public void testPromqlBucketsWithoutRange() {
        assertThat(
            tsdb.error("PROMQL index=test buckets=10 avg(foo)"),
            containsString("unable to create a bucket; provide either [step] or all of [start], [end], and [buckets]")
        );
    }

    public void testPromqlBucketsWithTimestampBoundsFromContext() {
        var now = Instant.now();
        var bounds = new TimestampBounds(now.minus(1, ChronoUnit.HOURS), now);
        var plan = analyzer().addIndex("test", "tsdb-mapping.json")
            .timestampBounds(bounds)
            .query("PROMQL index=test buckets=10 avg(network.bytes_in)");
        assertTrue("Plan should be resolved after timestamp bounds injection", plan.resolved());
    }

    public void testNoMetricNameMatcherNotSupported() {
        assertThat(
            tsdb.error("PROMQL index=test step=5m {foo=\"bar\"}"),
            containsString("__name__ label selector is required at this time [{foo=\"bar\"}]")
        );
    }

    public void testWithoutNotSupported() {
        assertThat(
            tsdb.error("PROMQL index=test step=5m avg(foo) without (bar)"),
            containsString("'without' grouping is not supported at this time")
        );
    }

    public void testGroupModifiersNotSupported() {
        assertThat(
            tsdb.error("PROMQL index=test step=5m foo / on(bar) baz"),
            containsString("queries with group modifiers are not supported at this time")
        );
    }

    public void testNonScalarComparison() {
        assertThat(
            tsdb.error("PROMQL index=test step=5m foo > bar"),
            containsString("comparison operators with non-literal right-hand side are not supported at this time")
        );
    }

    public void testNestedComparisons() {
        assertThat(
            tsdb.error("PROMQL index=test step=5m avg(foo > 5)"),
            containsString("comparison operators are only supported at the top-level at this time")
        );
    }

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }
}
