/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis.promql;

import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.TestAnalyzer;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.analysis.InSubqueryResolver;
import org.elasticsearch.xpack.esql.analysis.UnmappedResolution;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.querydsl.QueryDslTimestampBoundsExtractor.TimestampBounds;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.TranslatePromqlToEsqlPlan;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.parser.promql.PromqlAstTests;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Row;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.promql.AcrossSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.promql.MetadataManipulationFunction;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCommand;
import org.elasticsearch.xpack.esql.rule.Rule;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

public class PromqlVerifierTests extends ESTestCase {

    private final TestAnalyzer tsdb = analyzer().addIndex("test", "tsdb-mapping.json", IndexMode.TIME_SERIES)
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
        var plan = analyzer().addIndex("test", "tsdb-mapping.json", IndexMode.TIME_SERIES)
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

    public void testVectorMatchingRequiresInstantVectors() {
        // Mirrors Prometheus: on/ignoring describe how two labelsets match, and a scalar operand has no labelset.
        tsdb.error("PROMQL index=test step=5m foo / on(bar) 1", containsString("vector matching only allowed between instant vectors"));
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

    public void testLabelReplaceWrongArity() {
        tsdb.error(
            "PROMQL index=test step=5m sum by (dst) (label_replace(network.bytes_in, \"dst\", \"x\", \"host\"))",
            ParsingException.class,
            containsString("Invalid number of parameters for function [label_replace], required [5], found [4]")
        );
    }

    public void testLabelJoinTooFewArguments() {
        tsdb.error(
            "PROMQL index=test step=5m sum by (dst) (label_join(network.bytes_in, \"dst\"))",
            ParsingException.class,
            containsString("Invalid number of parameters for function [label_join], required [3], found [2]")
        );
    }

    public void testLabelReplaceRangeVectorChildRejected() {
        tsdb.error(
            "PROMQL index=test step=5m sum by (dst) (label_replace(network.bytes_in[5m], \"dst\", \"x\", \"host\", \".*\"))",
            containsString("expected type instant_vector in call to function [label_replace], got range_vector")
        );
    }

    public void testLabelReplaceMalformedRegexRejected() {
        tsdb.error(
            "PROMQL index=test step=5m sum by (dst) (label_replace(network.bytes_in, \"dst\", \"x\", \"host\", \"(\"))",
            containsString("invalid regular expression [(] in call to function [label_replace]")
        );
    }

    public void testLabelReplaceInvalidDestinationLabelRejected() {
        // Label-name validation mirrors Prometheus's UTF-8 scheme, under which the only invalid name is the empty string.
        tsdb.error(
            "PROMQL index=test step=5m sum by (dst) (label_replace(network.bytes_in, \"\", \"x\", \"host\", \".*\"))",
            containsString("invalid destination label name [] in call to function [label_replace]")
        );
    }

    public void testLabelJoinInvalidDestinationLabelRejected() {
        tsdb.error(
            "PROMQL index=test step=5m sum by (dst) (label_join(network.bytes_in, \"\", \"-\", \"host\"))",
            containsString("invalid destination label name [] in call to function [label_join]")
        );
    }

    public void testLabelReplaceGroupingRejected() {
        // label_replace is not an aggregation, so a by(...) grouping clause is invalid PromQL and must be rejected rather
        // than silently dropped (mirroring the guard that rejects grouping on other non-aggregation functions).
        tsdb.error(
            "PROMQL index=test step=5m label_replace by (host) (network.bytes_in, \"dst\", \"x\", \"host\", \".*\")",
            containsString("[by] clause not allowed on non-aggregation function [label_replace]")
        );
    }

    public void testLabelJoinGroupingRejected() {
        tsdb.error(
            "PROMQL index=test step=5m label_join without (host) (network.bytes_in, \"dst\", \"-\", \"host\")",
            containsString("[without] clause not allowed on non-aggregation function [label_join]")
        );
    }

    public void testLabelReplaceOverwritingNameResolves() {
        // A destination of __name__ overwrites the metric name as a derived column; the enclosing by(__name__) binds to the
        // derived destination (which shadows the stored __name__), so the query resolves.
        assertTrue(
            tsdb.query(
                "PROMQL index=test step=5m sum by (__name__) (label_replace(network.bytes_in, \"__name__\", \"renamed\", \"host\", \".*\"))"
            ).resolved()
        );
    }

    public void testLabelReplaceOverwritingExistingDimensionResolves() {
        // host is a stored dimension; the derived host destination shadows it in the resolution scope, so by(host) binds to
        // the derived destination unambiguously (rather than colliding with the stored host) and the query resolves.
        assertTrue(
            tsdb.query(
                "PROMQL index=test step=5m sum by (host) (label_replace(network.bytes_in, \"host\", \"$1\", \"metricset\", \"(.+)\"))"
            ).resolved()
        );
    }

    public void testLabelJoinOverwritingExistingDimensionResolves() {
        // label_join into a stored dimension resolves the same way: the derived destination shadows the stored label.
        assertTrue(
            tsdb.query("PROMQL index=test step=5m sum by (host) (label_join(network.bytes_in, \"host\", \"-\", \"host\", \"metricset\"))")
                .resolved()
        );
    }

    public void testLabelReplaceBareCallRejected() {
        // A relabel must be consumed by an enclosing by(...) aggregation; a bare (non-aggregated) call is rejected because
        // the derived label is materialized as a column rather than written into the series-identity blob.
        tsdb.error(
            "PROMQL index=test step=5m label_replace(network.bytes_in, \"dst\", \"$1\", \"host\", \"(.+)\")",
            containsString("[label_replace] is only supported inside a `by(...)` aggregation")
        );
    }

    public void testLabelReplaceUnderWithoutRejected() {
        tsdb.error(
            "PROMQL index=test step=5m sum without (dst) (label_replace(network.bytes_in, \"dst\", \"$1\", \"host\", \"(.+)\"))",
            containsString("[label_replace] is only supported inside a `by(...)` aggregation, but was used with a `without(...)` grouping")
        );
    }

    public void testLabelReplaceAsBinaryOperandRejected() {
        tsdb.error(
            "PROMQL index=test step=5m label_replace(network.bytes_in, \"dst\", \"$1\", \"host\", \"(.+)\") + network.connections",
            containsString(
                "[label_replace] is only supported inside a `by(...)` aggregation, but was used as an operand of a binary operator"
            )
        );
    }

    public void testLabelReplaceUnderGroupAllAggregateRejected() {
        // A group-all aggregation (no by/without) collapses all series, so there is no by(dst) to bind the derived label.
        tsdb.error(
            "PROMQL index=test step=5m sum (label_replace(network.bytes_in, \"dst\", \"$1\", \"host\", \"(.+)\"))",
            containsString("[label_replace] is only supported inside a `by(...)` aggregation, but was used without a `by(...)` grouping")
        );
    }

    public void testLabelReplaceUnderReductionRejected() {
        // A topk/bottomk reduction consumes the series identity directly rather than through a by(dst) grouping.
        tsdb.error(
            "PROMQL index=test step=5m topk(3, label_replace(network.bytes_in, \"dst\", \"$1\", \"host\", \"(.+)\"))",
            containsString("[label_replace] is only supported inside a `by(...)` aggregation, but was used under [topk]")
        );
    }

    public void testLabelReplaceUnderByGroupingResolves() {
        // The supported shape: a derived destination label consumed by an enclosing by(...) aggregation (here a new label).
        assertTrue(
            tsdb.query("PROMQL index=test step=5m sum by (dst) (label_replace(network.bytes_in, \"dst\", \"$1\", \"host\", \"(.+)\"))")
                .resolved()
        );
    }

    public void testLabelJoinUnderByGroupingResolves() {
        assertTrue(
            tsdb.query("PROMQL index=test step=5m sum by (dst) (label_join(network.bytes_in, \"dst\", \"-\", \"host\", \"metricset\"))")
                .resolved()
        );
    }

    public void testLabelReplaceDottedDestinationResolves() {
        // OpenTelemetry-style dimension names contain dots (for example `service.name`). Prometheus validates the
        // label_replace destination with the permissive UTF-8 scheme and accepts such names, and the ES|QL pipeline
        // handles dotted names throughout - the grammar's label-name token allows dots, `by(...)` binds to them, and a
        // derived column may be named with dots - so a dotted destination consumed by an enclosing by(...) must resolve.
        assertTrue(
            tsdb.query(
                "PROMQL index=test step=5m sum by (service.name) "
                    + "(label_replace(network.bytes_in, \"service.name\", \"$1\", \"host\", \"(.+)\"))"
            ).resolved()
        );
    }

    public void testLabelJoinDottedSourceAndDestinationResolves() {
        // label_join validates every source label name in addition to the destination. Both must accept dotted
        // (OpenTelemetry-style) names, matching Prometheus; a dotted source that is absent simply contributes the empty
        // string. Here `service.name` is a dotted destination and `k8s.pod.name` a dotted source.
        assertTrue(
            tsdb.query(
                "PROMQL index=test step=5m sum by (service.name) "
                    + "(label_join(network.bytes_in, \"service.name\", \"-\", \"host\", \"k8s.pod.name\"))"
            ).resolved()
        );
    }

    public void testNestedRelabelsSameDestinationResolves() {
        // Two relabels deriving the same destination label is valid PromQL: the outer/last relabel wins. Today this fails
        // with a confusing internal "ambiguous reference" error because every relabel destination is flattened into one
        // global resolution scope (Analyzer#resolvePromql), so the enclosing by(dst) matches two same-named attributes.
        // The same happens when two relabels in separate `or` (union) branches share a destination. It should resolve.
        assertTrue(
            tsdb.query(
                "PROMQL index=test step=5m sum by (dst) "
                    + "(label_replace(label_replace(network.bytes_in, \"dst\", \"$1\", \"host\", \"(.+)\"), "
                    + "\"dst\", \"$1\", \"metricset\", \"(.+)\"))"
            ).resolved()
        );
    }

    public void testGroupingBelowRelabelBindsToStoredLabelNotDerived() {
        // A label_replace overwriting the stored `host` dimension, with an inner `sum by (host, metricset)` BELOW the relabel.
        // The inner grouping operates on the vector before relabeling, so its `host` must bind to the stored dimension - not to
        // the derived destination the relabel mints above it. Today the analyzer resolves the whole PromQL plan against one flat
        // scope in which the derived destination shadows the stored label everywhere, so the inner grouping wrongly binds to the
        // derived destination. Translation later re-links it to the stored column by name, masking this at execution, so the
        // defect is only observable on the resolved-but-not-yet-translated plan - which is what this test inspects.
        LogicalPlan resolved = resolvePromqlWithoutTranslation(
            "PROMQL index=test step=5m sum by (host) "
                + "(label_replace(sum by (host, metricset) (network.bytes_in), \"host\", \"h-$1\", \"metricset\", \"(.+)\"))"
        );
        PromqlCommand command = resolved.collect(PromqlCommand.class).getFirst();
        MetadataManipulationFunction relabel = command.promqlPlan().collect(MetadataManipulationFunction.class).getFirst();
        AcrossSeriesAggregate innerGrouping = (AcrossSeriesAggregate) relabel.child();
        Attribute innerHost = innerGrouping.groupings().stream().filter(g -> g.name().equals("host")).findFirst().orElseThrow();
        // The stored dimension is a FieldAttribute; the derived destination is a ReferenceAttribute. A grouping below the relabel
        // must bind to the stored label, and so must not share the derived destination's id.
        assertThat(innerHost, instanceOf(FieldAttribute.class));
        assertThat(innerHost.id(), not(equalTo(relabel.destination().id())));
    }

    public void testLabelJoinInvalidSourceLabelNameRejected() {
        // label_join validates every source label name in addition to the destination, mirroring Prometheus. Under the
        // UTF-8 scheme the only invalid name is the empty string, so an empty source must be rejected at analysis time.
        tsdb.error(
            "PROMQL index=test step=5m sum by (dst) (label_join(network.bytes_in, \"dst\", \"-\", \"\"))",
            containsString("invalid source label name [] in call to function [label_join]")
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

    /**
     * Analyzes a PromQL query up to (and including) PromQL reference resolution and verification, but stops before the PromQL
     * plan is lowered to ESQL. This exposes the resolved-but-not-translated {@link PromqlCommand} so tests can assert how
     * references bound - which translation would otherwise erase (it re-links columns by name, masking mis-bindings).
     */
    private LogicalPlan resolvePromqlWithoutTranslation(String query) {
        LogicalPlan parsed = InSubqueryResolver.resolve(EsqlTestUtils.TEST_PARSER.parseQuery(query));
        return new ResolveOnlyAnalyzer(tsdb.buildContext()).resolve(parsed);
    }

    /**
     * An {@link Analyzer} that runs only the first analyzer batch, with the PromQL→ESQL {@link TranslatePromqlToEsqlPlan}
     * translation rule removed. The result keeps the PromQL plan resolved and verified but un-lowered.
     */
    private static final class ResolveOnlyAnalyzer extends Analyzer {
        ResolveOnlyAnalyzer(AnalyzerContext context) {
            super(context, EsqlTestUtils.TEST_VERIFIER);
        }

        LogicalPlan resolve(LogicalPlan plan) {
            return execute(plan);
        }

        @Override
        protected List<Batch<LogicalPlan>> batches() {
            Batch<LogicalPlan> first = super.batches().iterator().next();
            @SuppressWarnings({ "rawtypes", "unchecked" })
            Rule<?, LogicalPlan>[] withoutTranslation = Arrays.stream(first.rules())
                .filter(rule -> rule instanceof TranslatePromqlToEsqlPlan == false)
                .toArray(Rule[]::new);
            return List.of(first.with(withoutTranslation));
        }
    }

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }
}
