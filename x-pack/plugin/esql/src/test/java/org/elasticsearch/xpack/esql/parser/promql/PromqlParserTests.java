/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser.promql;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.parser.QueryParams;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.promql.AcrossSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCommand;
import org.elasticsearch.xpack.esql.plan.logical.promql.UnresolvedPromqlFunction;
import org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorBinaryArithmetic;
import org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorBinaryComparison;
import org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorBinarySet;
import org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorMatch;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.InstantSelector;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LabelMatcher;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LiteralSelector;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.RangeSelector;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_PARSER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.paramAsConstant;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.paramsAsConstant;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

public class PromqlParserTests extends ESTestCase {

    public void testNoParenthesis() {
        Stream.of(
            parse("PROMQL index=test step=5m avg(foo)"),
            parse("PROMQL index=test step=5m avg by (host) (foo)"),
            parse("PROMQL index=test step=5m avg by (pod) (avg_over_time(network.bytes_in{pod=~\"host-0|host-1|host-2\"}[1h]))")
        ).map(PromqlCommand::step).forEach(step -> {
            assertThat(step.value(), equalTo(Duration.ofMinutes(5)));
        });
    }

    public void testSpaceBetweenAssignParams() {
        Stream.of(
            parse("PROMQL index=test step=5m (avg(foo))"),
            parse("PROMQL index=test step= 5m (avg(foo))"),
            parse("PROMQL index=test step =5m (avg(foo))"),
            parse("PROMQL index=test step = 5m (avg(foo))")
        ).map(PromqlCommand::step).forEach(step -> {
            assertThat(step.value(), equalTo(Duration.ofMinutes(5)));
        });
    }

    public void testValidRangeQuery() {
        PromqlCommand promql = parse("PROMQL index=test start=\"2025-10-31T00:00:00Z\" end=\"2025-10-31T01:00:00Z\" step=1m (avg(foo))");
        assertThat(promql.start().value(), equalTo(Instant.parse("2025-10-31T00:00:00Z").toEpochMilli()));
        assertThat(promql.end().value(), equalTo(Instant.parse("2025-10-31T01:00:00Z").toEpochMilli()));
        assertThat(promql.step().value(), equalTo(Duration.ofMinutes(1)));
        assertThat(promql.scrapeInterval().value(), equalTo(Duration.ofMinutes(1)));
        assertThat(promql.isRangeQuery(), equalTo(true));
        assertThat(promql.isInstantQuery(), equalTo(false));
    }

    public void testValidRangeQueryParams() {
        PromqlCommand promql = EsqlTestUtils.as(
            TEST_PARSER.parseQuery(
                "PROMQL index=test start=?_tstart end=?_tend step=?_step (avg(foo))",
                new QueryParams(
                    List.of(
                        paramAsConstant("_tstart", "2025-10-31T00:00:00Z"),
                        paramAsConstant("_tend", "2025-10-31T01:00:00Z"),
                        paramAsConstant("_step", "1m")
                    )
                )
            ),
            PromqlCommand.class
        );
        assertThat(promql.start().value(), equalTo(Instant.parse("2025-10-31T00:00:00Z").toEpochMilli()));
        assertThat(promql.end().value(), equalTo(Instant.parse("2025-10-31T01:00:00Z").toEpochMilli()));
        assertThat(promql.step().value(), equalTo(Duration.ofMinutes(1)));
        assertThat(promql.scrapeInterval().value(), equalTo(Duration.ofMinutes(1)));
        assertThat(promql.isRangeQuery(), equalTo(true));
        assertThat(promql.isInstantQuery(), equalTo(false));
    }

    public void testValidRangeQueryOnlyStep() {
        PromqlCommand promql = parse("PROMQL index=test `step`=\"1\" (avg(foo))");
        assertThat(promql.start().value(), nullValue());
        assertThat(promql.end().value(), nullValue());
        assertThat(promql.step().value(), equalTo(Duration.ofSeconds(1)));
        assertThat(promql.scrapeInterval().value(), equalTo(Duration.ofMinutes(1)));
        assertThat(promql.isRangeQuery(), equalTo(true));
        assertThat(promql.isInstantQuery(), equalTo(false));
    }

    public void testValidInstantQuery() {
        PromqlCommand promql = parse("PROMQL index=test time=\"2025-10-31T00:00:00Z\" (avg(foo))");
        assertThat(promql.start().value(), equalTo(Instant.parse("2025-10-31T00:00:00Z").toEpochMilli()));
        assertThat(promql.end().value(), equalTo(Instant.parse("2025-10-31T00:00:00Z").toEpochMilli()));
        assertThat(promql.step().value(), nullValue());
        assertThat(promql.scrapeInterval().value(), equalTo(Duration.ofMinutes(1)));
        assertThat(promql.isInstantQuery(), equalTo(true));
        assertThat(promql.isRangeQuery(), equalTo(false));
    }

    public void testValidRangeQueryWithScrapeInterval() {
        PromqlCommand promql = parse("PROMQL index=test step=10s scrape_interval=45s (avg(foo))");
        assertThat(promql.step().value(), equalTo(Duration.ofSeconds(10)));
        assertThat(promql.scrapeInterval().value(), equalTo(Duration.ofSeconds(45)));
    }

    public void testValidRangeQueryInvalidQuotedIdentifierValue() {
        ParsingException e = assertThrows(ParsingException.class, () -> parse("PROMQL index=test step=`1m` (avg(foo))"));
        assertThat(e.getMessage(), containsString("1:24: Parameter value [`1m`] must not be a quoted identifier"));
    }

    public void testMissingParams() {
        Stream.of(
            parse("PROMQL foo / bar"),
            parse("PROMQL avg(foo)"),
            parse("PROMQL foo{host=\"host-1\"}"),
            parse("PROMQL avg by (host) (foo)"),
            parse("PROMQL avg by (pod) (avg_over_time(network.bytes_in{pod=~\"host-0|host-1|host-2\"}[1h]))")
        ).forEach(cmd -> {
            assertThat(cmd.start().value(), nullValue());
            assertThat(cmd.end().value(), nullValue());
            assertThat(cmd.step().value(), nullValue());
            assertThat(cmd.buckets().value(), equalTo(100));
        });
    }

    public void testZeroStep() {
        ParsingException e = assertThrows(ParsingException.class, () -> parse("PROMQL index=test step=0 (avg(foo))"));
        assertThat(e.getMessage(), containsString("Invalid value [0] for parameter [step], expected a positive duration"));
    }

    public void testNegativeStep() {
        ParsingException e = assertThrows(ParsingException.class, () -> parse("PROMQL index=test step=\"-1\" (avg(foo))"));
        assertThat(e.getMessage(), containsString("Invalid value [-1] for parameter [step], expected a positive duration"));
    }

    public void testZeroScrapeInterval() {
        ParsingException e = assertThrows(ParsingException.class, () -> parse("PROMQL index=test step=1m scrape_interval=0 (avg(foo))"));
        assertThat(e.getMessage(), containsString("Invalid value [0] for parameter [scrape_interval], expected a positive duration"));
    }

    public void testNegativeScrapeInterval() {
        ParsingException e = assertThrows(
            ParsingException.class,
            () -> parse("PROMQL index=test step=1m scrape_interval=\"-1\" (avg(foo))")
        );
        assertThat(e.getMessage(), containsString("Invalid value [-1] for parameter [scrape_interval], expected a positive duration"));
    }

    public void testEndBeforeStart() {
        ParsingException e = assertThrows(
            ParsingException.class,
            () -> parse("PROMQL index=test start=\"2025-10-31T01:00:00Z\" end=\"2025-10-31T00:00:00Z\" step=1m (avg(foo))")
        );
        assertThat(e.getMessage(), containsString("1:1: invalid parameter \"end\": end timestamp must not be before start time"));
    }

    public void testInstantAndRangeParams() {
        ParsingException e = assertThrows(ParsingException.class, () -> parse("""
            PROMQL index=test start="2025-10-31T00:00:00Z" end="2025-10-31T01:00:00Z" step=1m time="2025-10-31T00:00:00Z" (
                 avg(foo)
               )"""));
        assertThat(
            e.getMessage(),
            containsString("1:1: Specify either [time] for instant query or any of [step], [buckets], [start], [end]")
        );
    }

    public void testDuplicateParameter() {
        ParsingException e = assertThrows(ParsingException.class, () -> parse("PROMQL index=test step=1 step=2 (avg(foo))"));
        assertThat(e.getMessage(), containsString("[step] already specified"));
    }

    public void testUnknownParameter() {
        ParsingException e = assertThrows(ParsingException.class, () -> parse("PROMQL index=test stp=1 (avg(foo))"));
        assertThat(e.getMessage(), containsString("Unknown parameter [stp], did you mean [step]?"));
    }

    public void testUnknownParameterNoSuggestion() {
        ParsingException e = assertThrows(ParsingException.class, () -> parse("PROMQL index=test foo=1 (avg(foo))"));
        assertThat(e.getMessage(), containsString("Unknown parameter [foo]"));
    }

    public void testInvalidDateFormat() {
        ParsingException e = assertThrows(
            ParsingException.class,
            () -> parse("PROMQL index=test start=\"not-a-date\" end=\"2025-10-31T01:00:00Z\" step=1m (avg(foo))")
        );
        assertThat(e.getMessage(), containsString("1:25: Invalid date format [not-a-date]"));
    }

    public void testOnlyStartSpecified() {
        ParsingException e = assertThrows(
            ParsingException.class,
            () -> parse("PROMQL index=test start=\"2025-10-31T00:00:00Z\" step=1m (avg(foo))")
        );
        assertThat(
            e.getMessage(),
            containsString("Parameters [start] and [end] must either both be specified or both be omitted for a range query")
        );
    }

    public void testOnlyEndSpecified() {
        ParsingException e = assertThrows(
            ParsingException.class,
            () -> parse("PROMQL index=test end=\"2025-10-31T01:00:00Z\" step=1m (avg(foo))")
        );
        assertThat(
            e.getMessage(),
            containsString("Parameters [start] and [end] must either both be specified or both be omitted for a range query")
        );
    }

    public void testRangeQueryMissingStepUsesDefaultBuckets() {
        PromqlCommand promql = parse("PROMQL index=test start=\"2025-10-31T00:00:00Z\" end=\"2025-10-31T01:00:00Z\" (avg(foo))");
        assertThat(promql.step().value(), nullValue());
        assertThat(promql.buckets().value(), equalTo(100));
        assertThat(promql.isRangeQuery(), equalTo(true));
    }

    public void testRangeQueryMissingStepWithBuckets() {
        PromqlCommand promql = parse("PROMQL index=test start=\"2025-10-31T00:00:00Z\" end=\"2025-10-31T01:00:00Z\" buckets=6 (avg(foo))");
        assertThat(promql.step().value(), nullValue());
        assertThat(promql.buckets().value(), equalTo(6));
        assertThat(promql.isRangeQuery(), equalTo(true));
    }

    public void testRangeQueryBucketsWithoutRangeBoundsWhenStepMissing() {
        PromqlCommand promql = parse("PROMQL index=test buckets=6 (avg(foo))");
        assertThat(promql.step().value(), nullValue());
        assertThat(promql.buckets().value(), equalTo(6));
        assertThat(promql.start().value(), nullValue());
        assertThat(promql.end().value(), nullValue());
    }

    public void testRangeQueryWithoutParams() {
        PromqlCommand promql = parse("PROMQL index=test avg(foo)");
        assertThat(promql.step().value(), nullValue());
        assertThat(promql.buckets().value(), equalTo(100));
        assertThat(promql.start().value(), nullValue());
        assertThat(promql.end().value(), nullValue());
    }

    public void testRangeQueryWithoutIndexUsesDefaultMetricsPattern() {
        PromqlCommand promql = parse("PROMQL step=5m avg(foo)");
        List<UnresolvedRelation> unresolvedRelations = promql.collect(UnresolvedRelation.class);
        assertThat(unresolvedRelations, hasSize(1));
        assertThat(unresolvedRelations.getFirst().indexPattern().indexPattern(), equalTo(PromqlCommand.DEFAULT_PROMQL_INDEX_PATTERN));
    }

    public void testRangeQueryBucketsRequiresPositiveInteger() {
        ParsingException e = assertThrows(ParsingException.class, () -> parse("PROMQL index=test buckets=0 (avg(foo))"));
        assertThat(e.getMessage(), containsString("Invalid value [0] for parameter [buckets], expected a positive integer"));
    }

    public void testRangeQueryBucketsRequiresNumericInteger() {
        ParsingException e = assertThrows(ParsingException.class, () -> parse("PROMQL index=test buckets=not-a-number (avg(foo))"));
        assertThat(e.getMessage(), containsString("Invalid value [not-a-number] for parameter [buckets], expected a positive integer"));
    }

    public void testRangeQueryStepAndBucketsMutuallyExclusive() {
        ParsingException e = assertThrows(
            ParsingException.class,
            () -> parse("PROMQL index=test start=\"2025-10-31T00:00:00Z\" end=\"2025-10-31T01:00:00Z\" step=1m buckets=10 (avg(foo))")
        );
        assertThat(e.getMessage(), containsString("Parameters [step] and [buckets] are mutually exclusive for a range query"));
    }

    public void testParseMultipleIndices() {
        PromqlCommand promqlCommand = parse("PROMQL index=foo, bar step=5m (avg(foo))");
        List<UnresolvedRelation> unresolvedRelations = promqlCommand.collect(UnresolvedRelation.class);
        assertThat(unresolvedRelations, hasSize(1));
        assertThat(unresolvedRelations.getFirst().indexPattern().indexPattern(), equalTo("foo,bar"));
    }

    public void testParseRemoteIndices() {
        PromqlCommand promqlCommand = parse("PROMQL index=*:foo,foo step=5m (avg(foo))");
        List<UnresolvedRelation> unresolvedRelations = promqlCommand.collect(UnresolvedRelation.class);
        assertThat(unresolvedRelations, hasSize(1));
        assertThat(unresolvedRelations.getFirst().indexPattern().indexPattern(), equalTo("*:foo,foo"));
    }

    public void testNamedParameterInDuration() {
        PromqlCommand promql = as(
            TEST_PARSER.parseQuery(
                "PROMQL index=test step=10m rate(http_requests_total[?_duration])",
                paramsAsConstant("_duration", "10m")
            ),
            PromqlCommand.class
        );
        assertThat(promql.step().value(), equalTo(Duration.ofMinutes(10)));
        List<RangeSelector> rangeSelectors = promql.promqlPlan().collect(RangeSelector.class);
        assertThat(rangeSelectors, hasSize(1));
        assertThat(rangeSelectors.getFirst().range().fold(null), equalTo(Duration.ofMinutes(10)));
    }

    public void testPositionalParameterInDuration() {
        PromqlCommand promql = as(
            TEST_PARSER.parseQuery("PROMQL index=test step=15m rate(http_requests_total[?1])", paramsAsConstant(null, "15m")),
            PromqlCommand.class
        );
        assertThat(promql.step().value(), equalTo(Duration.ofMinutes(15)));
        List<RangeSelector> rangeSelectors = promql.promqlPlan().collect(RangeSelector.class);
        assertThat(rangeSelectors, hasSize(1));
        assertThat(rangeSelectors.getFirst().range().fold(null), equalTo(Duration.ofMinutes(15)));
    }

    public void testSameParameterUsedMultipleTimes() {
        PromqlCommand promql = as(
            TEST_PARSER.parseQuery("PROMQL index=test step=?_step rate(foo[?_step]) + rate(bar[?_step])", paramsAsConstant("_step", "5m")),
            PromqlCommand.class
        );
        assertThat(promql.step().value(), equalTo(Duration.ofMinutes(5)));
        List<RangeSelector> rangeSelectors = promql.promqlPlan().collect(RangeSelector.class);
        assertThat(rangeSelectors, hasSize(2));
        for (RangeSelector rs : rangeSelectors) {
            assertThat(rs.range().fold(null), equalTo(Duration.ofMinutes(5)));
        }
    }

    public void testUnknownParameterInDurationError() {
        ParsingException e = assertThrows(
            ParsingException.class,
            () -> TEST_PARSER.parseQuery("PROMQL index=test step=5m rate(foo[?_unknown])", new QueryParams(List.of()))
        );
        assertThat(e.getMessage(), containsString("No value found for parameter [?_unknown]"));
    }

    public void testParameterWithInvalidDurationValue() {
        ParsingException e = assertThrows(
            ParsingException.class,
            () -> TEST_PARSER.parseQuery("PROMQL index=test step=5m rate(foo[?_bad])", paramsAsConstant("_bad", "not_a_duration"))
        );
        assertThat(e.getMessage(), containsString("Invalid time duration"));
    }

    public void testParameterWithListType() {
        ParsingException e = assertThrows(
            ParsingException.class,
            () -> TEST_PARSER.parseQuery("PROMQL index=test step=5m rate(foo[?_bad])", paramsAsConstant("_bad", List.of("1m", "5m")))
        );
        assertThat(e.getMessage(), containsString("Invalid time duration"));
    }

    public void testParameterWithInvalidType() {
        ParsingException e = assertThrows(
            ParsingException.class,
            () -> TEST_PARSER.parseQuery("PROMQL index=test step=5m rate(foo[?_bad])", paramsAsConstant("_bad", 42))
        );
        assertThat(e.getMessage(), containsString("Expected parameter [?_bad] to be of type string, but found [INTEGER]"));
    }

    public void testRangeVectorExpectedSupportsInstantSelector() {
        // rate() expects a range vector; an instant selector should be implicitly promoted at analysis time
        PromqlCommand promql = parse("PROMQL index=test step=5m rate(foo)");
        assertThat(promql.promqlPlan(), instanceOf(UnresolvedPromqlFunction.class));
        assertThat(((UnresolvedPromqlFunction) promql.promqlPlan()).functionName(), equalTo("rate"));
    }

    public void testCaseInsensitivityOperators() {
        var promql = parse("PROMQL index=test step=5m foo OR bar");
        assertThat(as(promql.promqlPlan(), VectorBinarySet.class).op(), equalTo(VectorBinarySet.SetOp.UNION));

        promql = parse("PROMQL index=test step=5m foo And bar");
        assertThat(as(promql.promqlPlan(), VectorBinarySet.class).op(), equalTo(VectorBinarySet.SetOp.INTERSECT));

        promql = parse("PROMQL index=test step=5m foo unless bar");
        assertThat(as(promql.promqlPlan(), VectorBinarySet.class).op(), equalTo(VectorBinarySet.SetOp.SUBTRACT));
    }

    public void testCaseInsensitivityAggregators() {
        // The parser lowercases function names in visitFunction; this verifies that behaviour directly.
        List.of("Sum", "Avg", "Count", "Min", "Max", "Stddev", "Stdvar").forEach(func -> {
            var promql = parse("promql index=test step=5m " + func.toUpperCase(Locale.ROOT) + "(foo)");
            String upper = as(promql.promqlPlan(), UnresolvedPromqlFunction.class).functionName();

            promql = parse("promql index=test step=5m " + func.toLowerCase(Locale.ROOT) + "(foo)");
            String lower = as(promql.promqlPlan(), UnresolvedPromqlFunction.class).functionName();

            promql = parse("promql index=test step=5m " + func + "(foo)");
            String camel = as(promql.promqlPlan(), UnresolvedPromqlFunction.class).functionName();

            assertThat(upper, equalTo(func.toLowerCase(Locale.ROOT)));
            assertThat(lower, equalTo(func.toLowerCase(Locale.ROOT)));
            assertThat(camel, equalTo(func.toLowerCase(Locale.ROOT)));
        });
    }

    public void testCaseInsensitivityKeywords() {
        var promql = parse("PROMQL index=test step=5m avg(foo) BY (pod)");
        assertThat(as(promql.promqlPlan(), UnresolvedPromqlFunction.class).grouping(), equalTo(AcrossSeriesAggregate.Grouping.BY));

        promql = parse("PROMQL index=test step=5m foo OfFsEt 5m");
        assertThat(as(promql.promqlPlan(), InstantSelector.class).evaluation().offset().value(), equalTo(Duration.ofMinutes(5)));
    }

    public void testCaseInsensitivityVectorMatchingKeywords() {

        var promql = parse("PROMQL index=test step=5m foo > Bool On(job) bar");
        VectorBinaryComparison binaryComp = as(promql.promqlPlan(), VectorBinaryComparison.class);
        assertThat(binaryComp.boolMode(), equalTo(true));
        assertThat(binaryComp.match().filter(), equalTo(VectorMatch.Filter.ON));

        promql = parse("PROMQL index=test step=5m foo < IGNORING (job) bar");
        binaryComp = as(promql.promqlPlan(), VectorBinaryComparison.class);
        assertThat(binaryComp.boolMode(), equalTo(false));
        assertThat(binaryComp.match().filter(), equalTo(VectorMatch.Filter.IGNORING));

        promql = parse("PROMQL index=test step=5m foo / ON (job) GROUP_LEFT bar");
        VectorBinaryArithmetic binaryArith = as(promql.promqlPlan(), VectorBinaryArithmetic.class);
        assertThat(binaryArith.match().grouping(), equalTo(VectorMatch.Joining.LEFT));
        assertThat(binaryArith.match().filter(), equalTo(VectorMatch.Filter.ON));

        promql = parse("PROMQL index=test step=5m foo / ON (job) GROUP_RIGHT bar");
        binaryArith = as(promql.promqlPlan(), VectorBinaryArithmetic.class);
        assertThat(binaryArith.match().grouping(), equalTo(VectorMatch.Joining.RIGHT));
        assertThat(binaryArith.match().filter(), equalTo(VectorMatch.Filter.ON));
    }

    public void testCaseInsensitivityModifier() {
        // @ start()
        var promql = parse("PROMQL index=test step=5m start=\"2026-01-01T00:00:00Z\" end=\"2026-01-01T01:00:00Z\" foo @ START()");
        assertThat(as(promql.promqlPlan(), InstantSelector.class).evaluation().at().value(), equalTo(promql.start().value()));

        // @ end()
        promql = parse("PROMQL index=test step=5m start=\"2026-01-01T00:00:00Z\" end=\"2026-01-01T01:00:00Z\" foo @ END()");
        assertThat(as(promql.promqlPlan(), InstantSelector.class).evaluation().at().value(), equalTo(promql.end().value()));
    }

    public void testCaseInsensitivityScalars() {
        var promql = parse("PROMQL index=test step=5m nan");
        assertThat(as(promql.promqlPlan(), LiteralSelector.class).literal().value(), equalTo(Double.NaN));

        promql = parse("PROMQL index=test step=5m Inf");
        assertThat(as(promql.promqlPlan(), LiteralSelector.class).literal().value(), equalTo(Double.POSITIVE_INFINITY));
    }

    public void testMatchSameLabelMultipleTimesSuccess() {
        var plan = parse("PROMQL index=test step=5m foo{host!=\"host-1\", host!=\"host-2\"}");
        List<LabelMatcher> matchers = as(plan.promqlPlan(), InstantSelector.class).labelMatchers().matchers();
        assertThat(matchers, hasSize(3));
        assertThat(matchers.get(0).name(), equalTo("__name__"));
        assertThat(matchers.get(0).value(), equalTo("foo"));
        assertThat(matchers.get(0).isNegation(), equalTo(false));

        assertThat(matchers.get(1).name(), equalTo("host"));
        assertThat(matchers.get(1).value(), equalTo("host-1"));
        assertThat(matchers.get(1).isNegation(), equalTo(true));

        assertThat(matchers.get(2).name(), equalTo("host"));
        assertThat(matchers.get(2).value(), equalTo("host-2"));
        assertThat(matchers.get(2).isNegation(), equalTo(true));
    }

    public void testMatchMetricNameMultipleTimesError() {
        ParsingException e = assertThrows(ParsingException.class, () -> parse("PROMQL index=test step=5m foo{__name__=\"bar\"}"));
        assertThat(e.getMessage(), containsString("Metric name must not be defined twice: [foo] or [bar]"));
    }

    // ---- query-as-param tests ----

    public void testQueryAsNamedParam() {
        PromqlCommand promql = as(
            TEST_PARSER.parseQuery("PROMQL index=test step=5m (?query)", paramsAsConstant("query", "foo")),
            PromqlCommand.class
        );
        assertThat(promql.step().value(), equalTo(Duration.ofMinutes(5)));
        assertThat(promql.promqlPlan(), instanceOf(InstantSelector.class));
    }

    public void testQueryAsPositionalParam() {
        PromqlCommand promql = as(
            TEST_PARSER.parseQuery("PROMQL index=test step=5m (?1)", paramsAsConstant(null, "foo")),
            PromqlCommand.class
        );
        assertThat(promql.step().value(), equalTo(Duration.ofMinutes(5)));
        assertThat(promql.promqlPlan(), instanceOf(InstantSelector.class));
    }

    public void testQueryAsParamWithExplicitValueName() {
        PromqlCommand promql = as(
            TEST_PARSER.parseQuery("PROMQL index=test step=5m value=(?query)", paramsAsConstant("query", "foo")),
            PromqlCommand.class
        );
        assertThat(promql.step().value(), equalTo(Duration.ofMinutes(5)));
        assertThat(promql.promqlPlan(), instanceOf(InstantSelector.class));
    }

    public void testQueryAsParamWithAllParamsAsNamedParams() {
        PromqlCommand promql = as(
            TEST_PARSER.parseQuery(
                "PROMQL step=?step start=?start end=?end index=?index value=(?query)",
                new QueryParams(
                    List.of(
                        paramAsConstant("query", "avg(foo)"),
                        paramAsConstant("step", "1m"),
                        paramAsConstant("start", "2025-10-31T00:00:00Z"),
                        paramAsConstant("end", "2025-10-31T01:00:00Z"),
                        paramAsConstant("index", "my-metrics")
                    )
                )
            ),
            PromqlCommand.class
        );
        assertThat(promql.step().value(), equalTo(Duration.ofMinutes(1)));
        assertThat(promql.start().value(), equalTo(Instant.parse("2025-10-31T00:00:00Z").toEpochMilli()));
        assertThat(promql.end().value(), equalTo(Instant.parse("2025-10-31T01:00:00Z").toEpochMilli()));
        List<UnresolvedRelation> unresolvedRelations = promql.collect(UnresolvedRelation.class);
        assertThat(unresolvedRelations.getFirst().indexPattern().indexPattern(), equalTo("my-metrics"));
    }

    public void testQueryAsNamedParamWithoutParens() {
        PromqlCommand promql = as(
            TEST_PARSER.parseQuery("PROMQL index=test step=5m ?query", paramsAsConstant("query", "foo")),
            PromqlCommand.class
        );
        assertThat(promql.step().value(), equalTo(Duration.ofMinutes(5)));
        assertThat(promql.promqlPlan(), instanceOf(InstantSelector.class));
    }

    public void testQueryAsPositionalParamWithoutParens() {
        PromqlCommand promql = as(
            TEST_PARSER.parseQuery("PROMQL index=test step=5m ?1", paramsAsConstant(null, "foo")),
            PromqlCommand.class
        );
        assertThat(promql.step().value(), equalTo(Duration.ofMinutes(5)));
        assertThat(promql.promqlPlan(), instanceOf(InstantSelector.class));
    }

    public void testQueryAsParamNonStringError() {
        ParsingException e = assertThrows(
            ParsingException.class,
            () -> TEST_PARSER.parseQuery("PROMQL index=test step=5m (?query)", paramsAsConstant("query", 42))
        );
        assertThat(e.getMessage(), containsString("Parameter [?query] in PromQL expression must be a string"));
    }

    public void testQueryAsParamUnknownParamError() {
        ParsingException e = assertThrows(
            ParsingException.class,
            () -> TEST_PARSER.parseQuery("PROMQL index=test step=5m (?unknown)", new QueryParams(List.of()))
        );
        assertThat(e.getMessage(), containsString("No value found for parameter [?unknown]"));
    }

    public void testQueryAsParamUnknownNamedParamError() {
        ParsingException e = assertThrows(
            ParsingException.class,
            () -> TEST_PARSER.parseQuery("PROMQL index=test step=5m (?_typo)", paramsAsConstant("_correct", "foo"))
        );
        assertThat(e.getMessage(), containsString("No value found for parameter [?_typo]"));
    }

    public void testQueryAsParamBlankStringError() {
        ParsingException e = assertThrows(
            ParsingException.class,
            () -> TEST_PARSER.parseQuery("PROMQL index=test step=5m (?query)", paramsAsConstant("query", ""))
        );
        assertThat(e.getMessage(), containsString("PromQL expression cannot be empty"));
    }

    public void testQueryAsNamedParamWithValueName() {
        PromqlCommand promql = as(
            TEST_PARSER.parseQuery("PROMQL index=test step=5m value=?query", paramsAsConstant("query", "foo")),
            PromqlCommand.class
        );
        assertThat(promql.step().value(), equalTo(Duration.ofMinutes(5)));
        assertThat(promql.promqlPlan(), instanceOf(InstantSelector.class));
        assertThat(promql.valueColumnName(), equalTo("value"));
    }

    // ---- index-as-param tests ----

    public void testIndexAsNamedParam() {
        PromqlCommand promql = as(
            TEST_PARSER.parseQuery("PROMQL index=?idx step=5m avg(foo)", paramsAsConstant("idx", "my-metrics")),
            PromqlCommand.class
        );
        List<UnresolvedRelation> unresolvedRelations = promql.collect(UnresolvedRelation.class);
        assertThat(unresolvedRelations.getFirst().indexPattern().indexPattern(), equalTo("my-metrics"));
    }

    public void testIndexAsPositionalParam() {
        PromqlCommand promql = as(
            TEST_PARSER.parseQuery("PROMQL index=?1 step=5m avg(foo)", paramsAsConstant(null, "my-metrics")),
            PromqlCommand.class
        );
        List<UnresolvedRelation> unresolvedRelations = promql.collect(UnresolvedRelation.class);
        assertThat(unresolvedRelations.getFirst().indexPattern().indexPattern(), equalTo("my-metrics"));
    }

    public void testIndexAsParamUnknownParamError() {
        ParsingException e = assertThrows(
            ParsingException.class,
            () -> TEST_PARSER.parseQuery("PROMQL index=?unknown step=5m avg(foo)", new QueryParams(List.of()))
        );
        assertThat(e.getMessage(), containsString("No value found for parameter [?unknown]"));
    }

    public void testIndexAsParamNonStringError() {
        ParsingException e = assertThrows(
            ParsingException.class,
            () -> TEST_PARSER.parseQuery("PROMQL index=?idx step=5m avg(foo)", paramsAsConstant("idx", 42))
        );
        assertThat(e.getMessage(), containsString("Parameter [?idx] for index must be a string"));
    }

    private static PromqlCommand parse(String query) {
        return as(TEST_PARSER.parseQuery(query), PromqlCommand.class);
    }

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }

}
