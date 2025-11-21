/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis.promql;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.action.PromqlFeatures;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils;
import org.junit.BeforeClass;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.elasticsearch.xpack.esql.analysis.VerifierTests.error;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assume.assumeTrue;

public class PromqlVerifierTests extends ESTestCase {

    private final Analyzer tsdb = AnalyzerTestUtils.analyzer(AnalyzerTestUtils.tsdbIndexResolution());

    @BeforeClass
    public static void checkPromqlEnabled() {
        assumeTrue("requires snapshot build with promql feature enabled", PromqlFeatures.isEnabled());
    }

    public void testPromqlMissingAcrossSeriesAggregation() {
        assertThat(
            error("""
                TS test | PROMQL step 5m (
                  rate(network.bytes_in[5m])
                )""", tsdb),
            equalTo("2:3: only aggregations across timeseries are supported at this time (found [rate(network.bytes_in[5m])])")
        );
    }

    public void testPromqlStepAndRangeMisaligned() {
        assertThat(
            error("""
                TS test | PROMQL step 1m (
                  avg(rate(network.bytes_in[5m]))
                )""", tsdb),
            equalTo("2:29: the duration for range vector selector [5m] must be equal to the query's step for range queries at this time")
        );
    }

    public void testPromqlIllegalNameLabelMatcher() {
        assertThat(
            error("TS test | PROMQL step 5m (avg({__name__=~\"*.foo.*\"}))", tsdb),
            equalTo("1:31: regex label selectors on __name__ are not supported at this time [{__name__=~\"*.foo.*\"}]")
        );
    }

    public void testPromqlSubquery() {
        assertThat(
            error("TS test | PROMQL step 5m (avg(rate(network.bytes_in[5m:])))", tsdb),
            equalTo("1:36: subqueries are not supported at this time [network.bytes_in[5m:]]")
        );
        assertThat(
            error("TS test | PROMQL step 5m (avg(rate(network.bytes_in[5m:1m])))", tsdb),
            equalTo("1:36: subqueries are not supported at this time [network.bytes_in[5m:1m]]")
        );
    }

    @AwaitsFix(
        bugUrl = "Doesn't parse: line 1:27: Invalid query '1+1'[ArithmeticBinaryContext] given; "
            + "expected LogicalPlan but found VectorBinaryArithmetic"
    )
    public void testPromqlArithmetricOperators() {
        assertThat(
            error("TS test | PROMQL step 5m (1+1)", tsdb),
            equalTo("1:27: arithmetic operators are not supported at this time [foo]")
        );
        assertThat(
            error("TS test | PROMQL step 5m (foo+1)", tsdb),
            equalTo("1:27: arithmetic operators are not supported at this time [foo]")
        );
        assertThat(
            error("TS test | PROMQL step 5m (1+foo)", tsdb),
            equalTo("1:27: arithmetic operators are not supported at this time [foo]")
        );
        assertThat(
            error("TS test | PROMQL step 5m (foo+bar)", tsdb),
            equalTo("1:27: arithmetic operators are not supported at this time [foo]")
        );
    }

    @AwaitsFix(
        bugUrl = "Doesn't parse: line 1:27: Invalid query 'method_code_http_errors_rate5m{code=\"500\"}'"
            + "[ValueExpressionContext] given; expected Expression but found InstantSelector"
    )
    public void testPromqlVectorMatching() {
        assertThat(
            error(
                "TS test | PROMQL step 5m (method_code_http_errors_rate5m{code=\"500\"} / ignoring(code) method_http_requests_rate5m)",
                tsdb
            ),
            equalTo("")
        );
        assertThat(
            error(
                "TS test | PROMQL step 5m (method_code_http_errors_rate5m / ignoring(code) group_left method_http_requests_rate5m)",
                tsdb
            ),
            equalTo("")
        );
    }

    public void testPromqlModifier() {
        assertThat(
            error("TS test | PROMQL step 5m (avg(rate(network.bytes_in[5m] offset 5m)))", tsdb),
            equalTo("1:36: offset modifiers are not supported at this time [network.bytes_in[5m] offset 5m]")
        );
        /* TODO
        assertThat(
            error("TS test | PROMQL step 5m (foo @ start())", tsdb),
            equalTo("1:27: @ modifiers are not supported at this time [foo @ start()]")
        );*/
    }

    @AwaitsFix(
        bugUrl = "Doesn't parse: line 1:27: Invalid query 'foo and bar'[LogicalBinaryContext] given; "
            + "expected Expression but found InstantSelector"
    )
    public void testLogicalSetBinaryOperators() {
        assertThat(error("TS test | PROMQL step 5m (foo and bar)", tsdb), equalTo(""));
        assertThat(error("TS test | PROMQL step 5m (foo or bar)", tsdb), equalTo(""));
        assertThat(error("TS test | PROMQL step 5m (foo unless bar)", tsdb), equalTo(""));
    }

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }
}
