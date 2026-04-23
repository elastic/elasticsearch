/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis.promql;

import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.TestAnalyzer;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.core.querydsl.QueryDslTimestampBoundsExtractor.TimestampBounds;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.parser.promql.PromqlAstTests;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class PromqlVerifierTests extends ESTestCase {

    private final TestAnalyzer tsdb = analyzer().addIndex("test", "tsdb-mapping.json").stripErrorPrefix(true);

    public void testPromqlRangeVector() {
        tsdb.error(
            "PROMQL index=test step=5m network.bytes_in[5m]",
            equalTo("1:27: invalid expression type \"range vector\" for range query, must be scalar or instant vector")
        );
    }

    public void testPromqlRangeVectorBinaryExpression() {
        tsdb.error(
            "PROMQL index=test step=5m max(network.bytes_in[5m] / network.bytes_in[5m])",
            equalTo("1:31: binary expression must contain only scalar and instant vector types")
        );
    }

    public void testPromqlIllegalNameLabelMatcher() {
        tsdb.error(
            "PROMQL index=test step=5m (avg({__name__=~\"*.foo.*\"}))",
            containsString("regex label selectors on __name__ are not supported at this time")
        );
    }

    public void testPromqlSubquery() {
        tsdb.error(
            "PROMQL index=test step=5m (avg(rate(network.bytes_in[5m:])))",
            equalTo("1:37: Subquery queries are not supported at this time [network.bytes_in[5m:]]")
        );
        tsdb.error(
            "PROMQL index=test step=5m (avg(rate(network.bytes_in[5m:1m])))",
            equalTo("1:37: Subquery queries are not supported at this time [network.bytes_in[5m:1m]]")
        );
    }

    @AwaitsFix(
        bugUrl = "Doesn't parse: line 1:27: Invalid query 'method_code_http_errors_rate5m{code=\"500\"}'"
            + "[ValueExpressionContext] given; expected Expression but found InstantSelector"
    )
    public void testPromqlVectorMatching() {
        tsdb.error(
            "PROMQL index=test step=5m (method_code_http_errors_rate5m{code=\"500\"} / ignoring(code) method_http_requests_rate5m)",
            equalTo("")
        );
        tsdb.error(
            "PROMQL index=test step=5m (method_code_http_errors_rate5m / ignoring(code) group_left method_http_requests_rate5m)",
            equalTo("")
        );
    }

    public void testPromqlModifier() {
        tsdb.error(
            "PROMQL index=test step=5m (avg(rate(network.bytes_in[5m] offset 5m)))",
            equalTo("1:37: offset modifiers are not supported at this time [network.bytes_in[5m] offset 5m]")
        );
        tsdb.error(
            "PROMQL index=test step=5m start=0 end=1 (avg(foo @ start()))",
            equalTo("1:46: @ modifiers are not supported at this time [foo @ start()]")
        );
    }

    public void testLogicalSetBinaryOperators() {
        List.of("and", "or", "unless").forEach(op -> {
            // metric op metric
            tsdb.error("PROMQL index=test step=5m foo " + op + " bar", containsString("set operators are not supported at this time"));
            // scalar op scalar
            tsdb.error("PROMQL index=test step=5m 1 " + op + " 1", containsString("set operators are not supported at this time"));
            // metric op scalar and scalar op metric
            tsdb.error(
                "PROMQL index=test step=5m network.bytes_in " + op + " 1",
                containsString("set operators are not supported at this time")
            );
            tsdb.error(
                "PROMQL index=test step=5m 1 " + op + " network.bytes_in",
                containsString("set operators are not supported at this time")
            );
        });
    }

    public void testPromqlInstantQuery() {
        tsdb.error(
            "PROMQL index=test time=\"2025-10-31T00:00:00Z\" (avg(foo))",
            containsString("unable to create a bucket; provide either [step] or all of [start], [end], and [buckets]")
        );
    }

    public void testPromqlMissingBucketParameters() {
        tsdb.error(
            "PROMQL index=test avg(foo)",
            containsString("unable to create a bucket; provide either [step] or all of [start], [end], and [buckets]")
        );
    }

    public void testPromqlBucketsWithoutRange() {
        tsdb.error(
            "PROMQL index=test buckets=10 avg(foo)",
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

    public void testQueryOnEmptyIndexReturnsEmptyLocalRelation() {
        // When the index pattern resolves to zero concrete indices (e.g. the data stream hasn't been created yet),
        // the PROMQL command should be short-circuited to a Limit(0) -> LocalRelation rather than leaving
        // series attributes unresolved, which would cause a VerificationException.
        var plan = analyzer().addEmptyIndex().query("PROMQL index=empty_index step=5m test_metric");
        var localRelations = plan.collect(LocalRelation.class);
        assertThat(localRelations, hasSize(1));
        assertThat(localRelations.get(0).supplier(), equalTo(EmptyLocalSupplier.EMPTY));
    }

    public void testQueryOnEmptyIndexWithGroupingReturnsEmptyLocalRelation() {
        // A `by` clause would normally add dimension columns to the PROMQL output. With an empty index
        // those columns are absent from the short-circuited LocalRelation, but should not cause errors.
        var plan = analyzer().addEmptyIndex().query("PROMQL index=empty_index step=5m avg(test_metric) by (job)");
        var localRelations = plan.collect(LocalRelation.class);
        assertThat(localRelations, hasSize(1));
        assertThat(localRelations.get(0).supplier(), equalTo(EmptyLocalSupplier.EMPTY));
    }

    public void testAbsentMetricWithSimilarNameReturnsEmptyResult() {
        // Prometheus returns empty results for non-existent metrics, not errors.
        // It uses the load_unmapped="nullify" functionality to do that.
        // There was a bug in this mechanism where it would throw an exception if the metric name was similar enough to an existing field,
        // due to a "did you mean" message being left in the plan after resolution.
        // This test ensures that the fix for that bug is working correctly.
        var plan = tsdb.query("PROMQL index=test step=5m network.bites_in");
        assertTrue("Plan should be resolved even when the metric is absent", plan.resolved());
    }

    public void testSimilarFieldInNonPromqlQueryFailsWithDidYouMean() {
        // Showcases the did you mean message for non PROMQL queries.
        tsdb.error(
            "FROM test | WHERE network.bites_in > 0",
            allOf(containsString("Unknown column [network.bites_in], did you mean any of ["), containsString("network.bytes_in"))
        );
    }

    public void testCounterMetricWithUnsupportedFunction() {
        // network.bytes_in is a counter metric - avg_over_time doesn't support counters
        tsdb.error(
            "PROMQL index=test step=5m avg_over_time(network.bytes_in[5m])",
            containsString("function [avg_over_time] does not support counter metric [network.bytes_in]")
        );
    }

    public void testCounterMetricWithAcrossSeriesAggregateIsValid() {
        // sum(counter) works because the implicit LastOverTime on the InstantSelector
        // converts the counter type to its numeric base type before the aggregate sees it
        var plan = tsdb.query("PROMQL index=test step=5m sum(network.bytes_in)");
        assertTrue("sum() on a counter should be valid (implicit last_over_time converts the type)", plan.resolved());
    }

    public void testCounterMetricWithValueTransformationIsValid() {
        // ceil(counter) works for the same reason — implicit LastOverTime on InstantSelector
        var plan = tsdb.query("PROMQL index=test step=5m ceil(network.bytes_in)");
        assertTrue("ceil() on a counter should be valid (implicit last_over_time converts the type)", plan.resolved());
    }

    public void testCounterMetricWithRateIsValid() {
        // rate() accepts counter metrics - this should succeed
        var plan = tsdb.query("PROMQL index=test step=5m rate(network.bytes_in[5m])");
        assertTrue("rate() on a counter should be valid", plan.resolved());
    }

    public void testCounterMetricWithSumOfRateIsValid() {
        // sum(rate(...)) is the standard pattern for counter metrics
        var plan = tsdb.query("PROMQL index=test step=5m sum(rate(network.bytes_in[5m]))");
        assertTrue("sum(rate()) on a counter should be valid", plan.resolved());
    }

    public void testGaugeMetricWithCounterOnlyFunction() {
        // network.connections is a gauge - rate() requires counter metrics
        tsdb.error(
            "PROMQL index=test step=5m rate(network.connections[5m])",
            containsString("function [rate] requires a counter metric, but [network.connections] has type [long]")
        );
    }

    public void testRateOnNonNumericField() {
        // host is a keyword dimension field, not a numeric metric - should get a clear 4xx-style error
        tsdb.error(
            "PROMQL index=test step=5m rate(host[5m])",
            containsString("field [host] of type [keyword] cannot be used as a metric; it is a dimension field")
        );
    }

    public void testAggregationOnNonNumericField() {
        // metricset is a keyword dimension field, not a numeric metric
        tsdb.error(
            "PROMQL index=test step=5m sum(metricset)",
            containsString("field [metricset] of type [keyword] cannot be used as a metric; it is a dimension field")
        );
    }

    public void testNoMetricNameMatcherNotSupported() {
        tsdb.error(
            "PROMQL index=test step=5m {foo=\"bar\"}",
            containsString("__name__ label selector is required at this time [{foo=\"bar\"}]")
        );
    }

    public void testGroupModifiersNotSupported() {
        tsdb.error(
            "PROMQL index=test step=5m foo / on(bar) baz",
            containsString("queries with group modifiers are not supported at this time")
        );
    }

    public void testNonScalarComparison() {
        tsdb.error(
            "PROMQL index=test step=5m foo > bar",
            containsString("comparison operators with non-literal right-hand side are not supported at this time")
        );
    }

    public void testNestedComparisons() {
        tsdb.error(
            "PROMQL index=test step=5m avg(foo > 5)",
            containsString("comparison operators are only supported at the top-level at this time")
        );
    }

    public void testUnknownFunction() {
        tsdb.error(
            "PROMQL index=test step=5m result=(non_existent_function(network.bytes_in))",
            containsString("Unknown PromQL function [non_existent_function]")
        );
    }

    public void testNonLiteralQuantileParameter() {
        // quantile() requires a literal scalar for φ; time() is a scalar but not a literal
        tsdb.error(
            "PROMQL index=test step=5m quantile(time(), network.connections)",
            containsString("expected literal parameter in call to function [quantile]")
        );
    }

    public void testScalarComparisonRequiresBool() {
        // time() returns a scalar; comparing two scalars without the bool modifier is invalid
        tsdb.error("PROMQL index=test step=5m time() > 1", containsString("Comparisons [>] between scalars must use the BOOL modifier"));
    }

    public void testUnaryNegationOfRangeVector() {
        // -(foo[5m]) is invalid: the negation expands to 0 - foo[5m] which has a range vector operand
        tsdb.error(
            "PROMQL index=test step=5m sum(-network.bytes_in[5m])",
            containsString("binary expression must contain only scalar and instant vector types")
        );
    }

    public void testInstantVectorExpected() {
        // avg expects an instant vector, but a range selector produces a range vector
        tsdb.error(
            "PROMQL index=test step=5m avg(network.bytes_in[5m])",
            containsString("expected type instant_vector in call to function [avg], got range_vector")
        );
    }

    public void testInstantVectorExpectedWithGrouping() {
        tsdb.error(
            "PROMQL index=test step=5m avg by (pod) (network.bytes_in[5m])",
            containsString("expected type instant_vector in call to function [avg], got range_vector")
        );
    }

    public void testRangeVectorExpectedRejectsNonSelectorInstantVectors() {
        // rate() requires a range vector; avg() returns an instant vector, so rate(avg(...)) is invalid
        tsdb.error(
            "PROMQL index=test step=5m rate(avg(network.bytes_in))",
            containsString("expected type range_vector in call to function [rate], got instant_vector")
        );
    }

    /**
     * Batch test for analysis-level invalid queries. Each bare PromQL expression is wrapped in
     * "PROMQL index=test step=5m (%s)" and expected to throw during analysis (either a
     * {@link org.elasticsearch.xpack.esql.parser.ParsingException} for arity errors or a
     * {@link org.elasticsearch.xpack.esql.VerificationException} for type/semantic errors).
     */
    public void testUnsupportedQueries() throws Exception {
        List<Tuple<String, Integer>> lines = PromqlAstTests.readQueries("/promql/grammar/queries-invalid-verifier.promql");
        for (Tuple<String, Integer> line : lines) {
            String q = line.v1();
            String promqlQuery = String.format(java.util.Locale.ROOT, "PROMQL index=test step=5m (%s)", q);
            try {
                tsdb.query(promqlQuery);
                fail("Expected exception for query on line " + line.v2() + ": [" + q + "] but none was thrown");
            } catch (ParsingException | VerificationException e) {
                // Expected — analysis should reject this query
            }
        }
    }

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }
}
