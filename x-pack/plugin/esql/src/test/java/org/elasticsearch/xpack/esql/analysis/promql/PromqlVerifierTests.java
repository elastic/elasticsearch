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
import org.elasticsearch.xpack.esql.analysis.UnmappedResolution;
import org.elasticsearch.xpack.esql.core.querydsl.QueryDslTimestampBoundsExtractor.TimestampBounds;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.parser.promql.PromqlAstTests;
import org.elasticsearch.xpack.esql.plan.logical.Row;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class PromqlVerifierTests extends ESTestCase {

    private final TestAnalyzer tsdb = analyzer().addIndex("test", "tsdb-mapping.json")
        .stripErrorPrefix(true)
        .unmappedResolution(UnmappedResolution.NULLIFY);

    public void testPromqlRangeVector() {
        tsdb.error(
            "PROMQL index=test step=5m network.bytes_in[5m]",
            equalTo("1:27: invalid expression type \"range vector\" for range query, must be scalar or instant vector")
        );
    }

    public void testPromqlRangeVectorBinaryExpression() {
        tsdb.error(
            "PROMQL index=test step=5m max(network.bytes_in[5m] / network.bytes_in[10m])",
            equalTo(
                "1:31: binary expression must contain only scalar and instant vector types\n"
                    + "line 1:54: binary expression must contain only scalar and instant vector types"
            )
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
        // Offset modifiers are now supported via a constant time shift; only the `@` modifier remains unsupported.
        tsdb.error(
            "PROMQL index=test step=5m start=0 end=1 (avg(foo @ start()))",
            equalTo("1:46: @ modifiers are not supported at this time [foo @ start()]")
        );
    }

    public void testPromqlHeterogeneousOffsetBinaryExpression() {
        // Both operands are source-backed and get merged into a single time-series aggregate, which cannot carry
        // two different offsets. `or` (UNION) translates to independent branches and is therefore allowed.
        tsdb.error(
            "PROMQL index=test step=5m (network.bytes_in - network.bytes_in offset 1d)",
            containsString("binary expressions with different offsets are not supported at this time")
        );
    }

    public void testLogicalSetBinaryOperators() {
        List.of("and", "unless").forEach(op -> {
            // metric op metric: and/unless (INTERSECT/SUBTRACT) are not supported yet.
            tsdb.error(
                "PROMQL index=test step=5m foo " + op + " bar",
                containsString("set operator [" + op + "] is not supported at this time")
            );
            // Any scalar operand is illegal in PromQL itself; this takes precedence over the unsupported-op message.
            // scalar op scalar
            tsdb.error(
                "PROMQL index=test step=5m 1 " + op + " 1",
                containsString("set operator \"" + op + "\" not allowed in binary scalar expression")
            );
            // metric op scalar and scalar op metric
            tsdb.error(
                "PROMQL index=test step=5m network.bytes_in " + op + " 1",
                containsString("set operator \"" + op + "\" not allowed in binary scalar expression")
            );
            tsdb.error(
                "PROMQL index=test step=5m 1 " + op + " network.bytes_in",
                containsString("set operator \"" + op + "\" not allowed in binary scalar expression")
            );
        });
    }

    public void testUnionBetweenInstantVectorsIsSupported() {
        // Top-level `or` (UNION) between two instant vectors is supported.
        assertTrue(tsdb.query("PROMQL index=test step=5m network.bytes_in or network.connections").resolved());
        // Left-associative chain of unions is also supported.
        assertTrue(tsdb.query("PROMQL index=test step=5m network.bytes_in or network.bytes_out or network.connections").resolved());
        // Common fallback idioms.
        assertTrue(tsdb.query("PROMQL index=test step=5m rate(network.bytes_in[5m]) or irate(network.bytes_in[5m])").resolved());
        assertTrue(tsdb.query("PROMQL index=test step=5m sum(rate(network.bytes_in[5m])) or vector(0)").resolved());
    }

    public void testUnionWithScalarOperandIsRejected() {
        // Scalar operands are illegal for set operators in PromQL itself (not just our implementation), so the
        // message mirrors Prometheus and does not imply the shape might be supported later.
        tsdb.error("PROMQL index=test step=5m 1 or 1", containsString("set operator \"or\" not allowed in binary scalar expression"));
        tsdb.error(
            "PROMQL index=test step=5m network.bytes_in or 1",
            containsString("set operator \"or\" not allowed in binary scalar expression")
        );
        tsdb.error(
            "PROMQL index=test step=5m 1 or network.bytes_in",
            containsString("set operator \"or\" not allowed in binary scalar expression")
        );
        // expr or 0 must still fail (0 is a scalar), while expr or vector(0) is allowed.
        tsdb.error(
            "PROMQL index=test step=5m network.bytes_in or 0",
            containsString("set operator \"or\" not allowed in binary scalar expression")
        );
    }

    public void testNestedUnionIsRejected() {
        // `or` is only supported at the top level; nested inside an aggregation it is rejected.
        tsdb.error(
            "PROMQL index=test step=5m sum(network.bytes_in or network.connections)",
            containsString("set operator [or] is only supported at the top-level at this time")
        );
    }

    public void testUnionBranchLimit() {
        // A union chain is translated into a single UnionAll, which supports up to Fork.MAX_BRANCHES (8) branches.
        String maxOperands = String.join(" or ", Collections.nCopies(8, "network.bytes_in"));
        assertTrue(tsdb.query("PROMQL index=test step=5m " + maxOperands).resolved());

        String tooManyOperands = String.join(" or ", Collections.nCopies(9, "network.bytes_in"));
        tsdb.error(
            "PROMQL index=test step=5m " + tooManyOperands,
            containsString("PromQL set operator [or] supports up to [8] operands, got [9]")
        );
    }

    public void testPromqlInstantQuery() {
        assertNotNull(tsdb.query("PROMQL index=test time=\"2025-10-31T00:00:00Z\" (avg(foo))"));
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

    public void testSourcelessQueryOnEmptyIndexDoesNotShortCircuitToEmptyLocalRelation() {
        var plan = analyzer().addEmptyIndex().query("PROMQL index=empty_index time=\"2025-01-01T00:00:00Z\" result=(time())");
        int emptyLocalRelations = 0;
        for (LocalRelation localRelation : plan.collect(LocalRelation.class)) {
            if (localRelation.supplier() == EmptyLocalSupplier.EMPTY) {
                emptyLocalRelations++;
            }
        }
        assertThat(emptyLocalRelations, equalTo(0));
        assertThat(plan.collect(Row.class), hasSize(1));
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
        tsdb.unmappedResolution(UnmappedResolution.DEFAULT)
            .error(
                "FROM test | WHERE network.bites_in > 0",
                allOf(containsString("Unknown column [network.bites_in], did you mean any of ["), containsString("network.bytes_in"))
            );
    }

    // PROMQL collapses to an aggregate, so a field after the pipe isn't nullified.
    public void testNullifyMissingFieldOutsidePromqlFails() {
        tsdb.error(
            "PROMQL index=test step=5m v=(sum(network.bytes_in)) | EVAL x = does_not_exist",
            containsString("Unknown column [does_not_exist]")
        );
    }

    // A mapped field collapsed by PROMQL is just as unreferenceable after the pipe, so nullify treats missing no worse.
    public void testMappedFieldOutsidePromqlFailsUnderNullify() {
        tsdb.error(
            "PROMQL index=test step=5m v=(sum(network.bytes_in)) | EVAL x = network.bytes_in",
            containsString("Unknown column [network.bytes_in]")
        );
    }

    // Same failure in default mode, so nullify changes nothing.
    public void testMissingFieldOutsidePromqlFailsInDefaultMode() {
        tsdb.unmappedResolution(UnmappedResolution.DEFAULT)
            .error(
                "PROMQL index=test step=5m v=(sum(network.bytes_in)) | EVAL x = does_not_exist",
                containsString("Unknown column [does_not_exist]")
            );
    }

    // nullify doesn't affect PROMQL's own handling of fields inside the command.
    public void testNullifyMissingFieldInsidePromqlResolves() {
        assertTrue(tsdb.query("PROMQL index=test step=5m sum(does_not_exist)").resolved());
    }

    public void testCounterMetricWithUnsupportedFunction() {
        // network.bytes_in is a counter metric; avg_over_time auto-wraps counters with to_gauge()
        var plan = tsdb.query("PROMQL index=test step=5m avg_over_time(network.bytes_in[5m])");
        assertTrue("avg_over_time() on a counter should be valid (implicit to_gauge wrap)", plan.resolved());
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
        // network.connections is a gauge; rate() auto-wraps plain numerics with to_counter()
        var plan = tsdb.query("PROMQL index=test step=5m rate(network.connections[5m])");
        assertTrue("rate() on a plain numeric gauge should be valid (implicit to_counter wrap)", plan.resolved());
    }

    public void testRateOnNonNumericField() {
        // host is a keyword dimension field, not a numeric metric - should get a clear 4xx-style error
        tsdb.error(
            "PROMQL index=test step=5m rate(host[5m])",
            containsString(
                "argument of [rate(host[5m])] must be [counter_double or counter_integer or counter_long or double or integer or long], "
                    + "found value [host] type [keyword]"
            )
        );
    }

    public void testRateOnHistogramField() {
        tsdb.error(
            "PROMQL index=test step=5m histogram_count(rate(request_duration[5m]))",
            ParsingException.class,
            containsString("rate() is not supported yet on native histograms; if possible, use increase() instead")
        );
    }

    public void testHistogramCountOnCounter() {
        tsdb.error(
            "PROMQL index=test step=5m histogram_count(network.bytes_in)",
            containsString("must be [exponential_histogram or tdigest]")
        );
    }

    public void testHistogramSumOnCounter() {
        tsdb.error(
            "PROMQL index=test step=5m histogram_sum(network.bytes_in)",
            containsString("must be [exponential_histogram or tdigest]")
        );
    }

    public void testHistogramAvgOnCounter() {
        tsdb.error(
            "PROMQL index=test step=5m histogram_avg(network.bytes_in)",
            containsString("must be [exponential_histogram or tdigest]")
        );
    }

    public void testAggregationOnNonNumericField() {
        // metricset is a keyword dimension field, not a numeric metric
        tsdb.error(
            "PROMQL index=test step=5m sum(metricset)",
            containsString(
                "1:27: argument of [sum(metricset)] must be [aggregate_metric_double, exponential_histogram, tdigest "
                    + "or numeric except unsigned_long or counter types], found value [metricset] type [keyword]"
            )
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
