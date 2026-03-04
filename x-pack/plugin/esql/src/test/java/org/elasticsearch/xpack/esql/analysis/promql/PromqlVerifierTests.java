/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis.promql;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.elasticsearch.xpack.esql.analysis.VerifierTests.error;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class PromqlVerifierTests extends ESTestCase {

    private final Analyzer tsdb = AnalyzerTestUtils.analyzer(AnalyzerTestUtils.tsdbIndexResolution());

    public void testPromqlRangeVector() {
        assertThat(
            error("PROMQL index=test step=5m network.bytes_in[5m]", tsdb),
            equalTo("1:27: invalid expression type \"range vector\" for range query, must be scalar or instant vector")
        );
    }

    public void testPromqlRangeVectorBinaryExpression() {
        assertThat(
            error("PROMQL index=test step=5m max(network.bytes_in[5m] / network.bytes_in[5m])", tsdb),
            equalTo("1:31: binary expression must contain only scalar and instant vector types")
        );
    }

    public void testPromqlIllegalNameLabelMatcher() {
        assertThat(error("PROMQL index=test step=5m (avg({__name__=~\"*.foo.*\"}))", tsdb), containsString("Unknown column [__name__]"));
    }

    public void testPromqlSubquery() {
        assertThat(
            error("PROMQL index=test step=5m (avg(rate(network.bytes_in[5m:])))", tsdb),
            equalTo("1:37: Subquery queries are not supported at this time [network.bytes_in[5m:]]")
        );
        assertThat(
            error("PROMQL index=test step=5m (avg(rate(network.bytes_in[5m:1m])))", tsdb),
            equalTo("1:37: Subquery queries are not supported at this time [network.bytes_in[5m:1m]]")
        );
    }

    @AwaitsFix(
        bugUrl = "Doesn't parse: line 1:27: Invalid query 'method_code_http_errors_rate5m{code=\"500\"}'"
            + "[ValueExpressionContext] given; expected Expression but found InstantSelector"
    )
    public void testPromqlVectorMatching() {
        assertThat(
            error(
                "PROMQL index=test step=5m (method_code_http_errors_rate5m{code=\"500\"} / ignoring(code) method_http_requests_rate5m)",
                tsdb
            ),
            equalTo("")
        );
        assertThat(
            error(
                "PROMQL index=test step=5m (method_code_http_errors_rate5m / ignoring(code) group_left method_http_requests_rate5m)",
                tsdb
            ),
            equalTo("")
        );
    }

    public void testPromqlModifier() {
        assertThat(
            error("PROMQL index=test step=5m (avg(rate(network.bytes_in[5m] offset 5m)))", tsdb),
            equalTo("1:37: offset modifiers are not supported at this time [network.bytes_in[5m] offset 5m]")
        );
        assertThat(
            error("PROMQL index=test step=5m start=0 end=1 (avg(foo @ start()))", tsdb),
            equalTo("1:46: @ modifiers are not supported at this time [foo @ start()]")
        );
    }

    public void testLogicalSetBinaryOperators() {
        List.of("and", "or", "unless").forEach(op -> {
            assertThat(
                error("PROMQL index=test step=5m foo " + op + " bar", tsdb),
                containsString("set operators are not supported at this time")
            );
        });
    }

    public void testPromqlInstantQuery() {
        assertThat(
            error("PROMQL index=test time=\"2025-10-31T00:00:00Z\" (avg(foo))", tsdb),
            containsString("unable to create a bucket; provide either [step] or all of [start], [end], and [buckets]")
        );
    }

    public void testPromqlMissingBucketParameters() {
        assertThat(
            error("PROMQL index=test avg(foo)", tsdb),
            containsString("unable to create a bucket; provide either [step] or all of [start], [end], and [buckets]")
        );
    }

    public void testPromqlBucketsWithoutRange() {
        assertThat(
            error("PROMQL index=test buckets=10 avg(foo)", tsdb),
            containsString("unable to create a bucket; provide either [step] or all of [start], [end], and [buckets]")
        );
    }

    public void testNoMetricNameMatcherNotSupported() {
        assertThat(
            error("PROMQL index=test step=5m {foo=\"bar\"}", tsdb),
            containsString("__name__ label selector is required at this time [{foo=\"bar\"}]")
        );
    }

    public void testWithoutNotSupported() {
        assertThat(
            error("PROMQL index=test step=5m avg(foo) without (bar)", tsdb),
            containsString("'without' grouping is not supported at this time")
        );
    }

    public void groupModifiersNotSupported() {
        assertThat(
            error("PROMQL index=test step=5m foo / on(bar) baz", tsdb),
            containsString("queries with group modifiers are not supported at this time")
        );
    }

    public void testNonScalarComparison() {
        assertThat(
            error("PROMQL index=test step=5m foo > bar", tsdb),
            containsString("comparison operators with non-literal right-hand side are not supported at this time")
        );
    }

    public void testNestedComparisons() {
        assertThat(
            error("PROMQL index=test step=5m avg(foo > 5)", tsdb),
            containsString("comparison operators are only supported at the top-level at this time")
        );
    }

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }
}
