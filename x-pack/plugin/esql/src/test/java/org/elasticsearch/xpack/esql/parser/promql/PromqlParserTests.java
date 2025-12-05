/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser.promql;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.action.PromqlFeatures;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.parser.QueryParams;
import org.elasticsearch.xpack.esql.plan.logical.Explain;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.promql.AcrossSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCommand;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.InstantSelector;
import org.junit.BeforeClass;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.paramAsConstant;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

public class PromqlParserTests extends ESTestCase {

    private static final EsqlParser parser = EsqlParser.INSTANCE;

    @BeforeClass
    public static void checkPromqlEnabled() {
        assumeTrue("requires snapshot build with promql feature enabled", PromqlFeatures.isEnabled());
    }

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
        assertThat(promql.isRangeQuery(), equalTo(true));
        assertThat(promql.isInstantQuery(), equalTo(false));
    }

    public void testValidRangeQueryParams() {
        PromqlCommand promql = EsqlTestUtils.as(
            parser.parseQuery(
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
        assertThat(promql.isRangeQuery(), equalTo(true));
        assertThat(promql.isInstantQuery(), equalTo(false));
    }

    public void testValidRangeQueryOnlyStep() {
        PromqlCommand promql = parse("PROMQL index=test `step`=\"1\" (avg(foo))");
        assertThat(promql.start().value(), nullValue());
        assertThat(promql.end().value(), nullValue());
        assertThat(promql.step().value(), equalTo(Duration.ofSeconds(1)));
        assertThat(promql.isRangeQuery(), equalTo(true));
        assertThat(promql.isInstantQuery(), equalTo(false));
    }

    public void testValidInstantQuery() {
        PromqlCommand promql = parse("PROMQL index=test time=\"2025-10-31T00:00:00Z\" (avg(foo))");
        assertThat(promql.start().value(), equalTo(Instant.parse("2025-10-31T00:00:00Z").toEpochMilli()));
        assertThat(promql.end().value(), equalTo(Instant.parse("2025-10-31T00:00:00Z").toEpochMilli()));
        assertThat(promql.step().value(), nullValue());
        assertThat(promql.isInstantQuery(), equalTo(true));
        assertThat(promql.isRangeQuery(), equalTo(false));
    }

    public void testValidRangeQueryInvalidQuotedIdentifierValue() {
        ParsingException e = assertThrows(ParsingException.class, () -> parse("PROMQL index=test step=`1m` (avg(foo))"));
        assertThat(e.getMessage(), containsString("1:24: Parameter value [`1m`] must not be a quoted identifier"));
    }

    // TODO nicer error messages for missing params
    public void testMissingParams() {
        assertThrows(ParsingException.class, () -> parse("PROMQL index=test (avg(foo))"));
    }

    public void testZeroStep() {
        ParsingException e = assertThrows(ParsingException.class, () -> parse("PROMQL index=test step=0 (avg(foo))"));
        assertThat(
            e.getMessage(),
            containsString(
                "1:1: invalid parameter \"step\": zero or negative query resolution step widths are not accepted. "
                    + "Try a positive integer"
            )
        );
    }

    public void testNegativeStep() {
        ParsingException e = assertThrows(ParsingException.class, () -> parse("PROMQL index=test step=\"-1\" (avg(foo))"));
        assertThat(
            e.getMessage(),
            containsString("invalid parameter \"step\": zero or negative query resolution step widths are not accepted")
        );
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
            containsString("1:1: Specify either [time] for instant query or [step], [start] or [end] for a range query")
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

    public void testRangeQueryMissingStep() {
        ParsingException e = assertThrows(
            ParsingException.class,
            () -> parse("PROMQL index=test start=\"2025-10-31T00:00:00Z\" end=\"2025-10-31T01:00:00Z\" (avg(foo))")
        );
        assertThat(e.getMessage(), containsString("Parameter [step] or [time] is required"));
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

    public void testExplain() {
        assertExplain("""
            PROMQL index=k8s step=5m ( avg by (pod) (avg_over_time(network.bytes_in{pod=~"host-0|host-1|host-2"}[1h])) )
            | LIMIT 1000
            """, AcrossSeriesAggregate.class);
        assertExplain("""
            PROMQL index=k8s step=5m avg by (pod) (avg_over_time(network.bytes_in{pod=~"host-0|host-1|host-2"}[1h]))
            | LIMIT 1000
            """, AcrossSeriesAggregate.class);
        assertExplain(
            "PROMQL index=k8s step=5m avg by (pod) (avg_over_time(network.bytes_in{pod=~\"host-0|host-1|host-2\"}[1h]))",
            AcrossSeriesAggregate.class
        );
        assertExplain("PROMQL index=k8s step=5m foo", InstantSelector.class);
    }

    public void assertExplain(String query, Class<? extends UnaryPlan> promqlCommandClass) {
        var plan = parser.parseQuery("EXPLAIN ( " + query + " )");
        Explain explain = plan.collect(Explain.class).getFirst();
        PromqlCommand promqlCommand = explain.query().collect(PromqlCommand.class).getFirst();
        assertThat(promqlCommand.promqlPlan(), instanceOf(promqlCommandClass));
    }

    private static PromqlCommand parse(String query) {
        return as(parser.parseQuery(query), PromqlCommand.class);
    }

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }

}
