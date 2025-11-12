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
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCommand;
import org.junit.BeforeClass;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.paramAsConstant;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assume.assumeTrue;

public class PromqlParamsTests extends ESTestCase {

    private static final EsqlParser parser = new EsqlParser();

    @BeforeClass
    public static void checkPromqlEnabled() {
        assumeTrue("requires snapshot build with promql feature enabled", PromqlFeatures.isEnabled());
    }

    public void testValidRangeQuery() {
        PromqlCommand promql = parse("TS test | PROMQL start \"2025-10-31T00:00:00Z\" end \"2025-10-31T01:00:00Z\" step 1m (avg(foo))");
        assertThat(promql.start().value(), equalTo(Instant.parse("2025-10-31T00:00:00Z")));
        assertThat(promql.end().value(), equalTo(Instant.parse("2025-10-31T01:00:00Z")));
        assertThat(promql.step().value(), equalTo(Duration.ofMinutes(1)));
        assertThat(promql.isRangeQuery(), equalTo(true));
        assertThat(promql.isInstantQuery(), equalTo(false));
    }

    public void testValidRangeQueryParams() {
        PromqlCommand promql = EsqlTestUtils.as(
            parser.createStatement(
                "TS test | PROMQL start ?_tstart end ?_tend step ?_step (avg(foo))",
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
        assertThat(promql.start().value(), equalTo(Instant.parse("2025-10-31T00:00:00Z")));
        assertThat(promql.end().value(), equalTo(Instant.parse("2025-10-31T01:00:00Z")));
        assertThat(promql.step().value(), equalTo(Duration.ofMinutes(1)));
        assertThat(promql.isRangeQuery(), equalTo(true));
        assertThat(promql.isInstantQuery(), equalTo(false));
    }

    public void testValidRangeQueryOnlyStep() {
        PromqlCommand promql = parse("TS test | PROMQL `step` \"1\" (avg(foo))");
        assertThat(promql.start().value(), nullValue());
        assertThat(promql.end().value(), nullValue());
        assertThat(promql.step().value(), equalTo(Duration.ofSeconds(1)));
        assertThat(promql.isRangeQuery(), equalTo(true));
        assertThat(promql.isInstantQuery(), equalTo(false));
    }

    public void testValidInstantQuery() {
        PromqlCommand promql = parse("TS test | PROMQL time \"2025-10-31T00:00:00Z\" (avg(foo))");
        assertThat(promql.start().value(), equalTo(Instant.parse("2025-10-31T00:00:00Z")));
        assertThat(promql.end().value(), equalTo(Instant.parse("2025-10-31T00:00:00Z")));
        assertThat(promql.step().value(), nullValue());
        assertThat(promql.isInstantQuery(), equalTo(true));
        assertThat(promql.isRangeQuery(), equalTo(false));
    }

    public void testValidRangeQueryInvalidQuotedIdentifierValue() {
        ParsingException e = assertThrows(ParsingException.class, () -> parse("TS test | PROMQL step `1m` (avg(foo))"));
        assertThat(e.getMessage(), containsString("1:23: Parameter value [`1m`] must not be a quoted identifier"));
    }

    // TODO nicer error messages for missing params
    public void testMissingParams() {
        assertThrows(ParsingException.class, () -> parse("TS test | PROMQL (avg(foo))"));
    }

    public void testZeroStep() {
        ParsingException e = assertThrows(ParsingException.class, () -> parse("TS test | PROMQL step 0 (avg(foo))"));
        assertThat(
            e.getMessage(),
            containsString(
                "1:11: invalid parameter \"step\": zero or negative query resolution step widths are not accepted. "
                    + "Try a positive integer"
            )
        );
    }

    public void testNegativeStep() {
        ParsingException e = assertThrows(ParsingException.class, () -> parse("TS test | PROMQL step \"-1\" (avg(foo))"));
        assertThat(
            e.getMessage(),
            containsString("invalid parameter \"step\": zero or negative query resolution step widths are not accepted")
        );
    }

    public void testEndBeforeStart() {
        ParsingException e = assertThrows(
            ParsingException.class,
            () -> parse("TS test | PROMQL start \"2025-10-31T01:00:00Z\" end \"2025-10-31T00:00:00Z\" step 1m (avg(foo))")
        );
        assertThat(e.getMessage(), containsString("1:11: invalid parameter \"end\": end timestamp must not be before start time"));
    }

    public void testInstantAndRangeParams() {
        ParsingException e = assertThrows(ParsingException.class, () -> parse("""
            TS test
             | PROMQL start "2025-10-31T00:00:00Z" end "2025-10-31T01:00:00Z" step 1m time "2025-10-31T00:00:00Z" (
                 avg(foo)
               )"""));
        assertThat(
            e.getMessage(),
            containsString("2:4: Specify either [time] for instant query or [step], [start] or [end] for a range query")
        );
    }

    public void testDuplicateParameter() {
        ParsingException e = assertThrows(ParsingException.class, () -> parse("TS test | PROMQL step 1 step 2 (avg(foo))"));
        assertThat(e.getMessage(), containsString("[step] already specified"));
    }

    public void testUnknownParameter() {
        ParsingException e = assertThrows(ParsingException.class, () -> parse("TS test | PROMQL stp 1 (avg(foo))"));
        assertThat(e.getMessage(), containsString("Unknown parameter [stp], did you mean [step]?"));
    }

    public void testUnknownParameterNoSuggestion() {
        ParsingException e = assertThrows(ParsingException.class, () -> parse("TS test | PROMQL foo 1 (avg(foo))"));
        assertThat(e.getMessage(), containsString("Unknown parameter [foo]"));
    }

    public void testInvalidDateFormat() {
        ParsingException e = assertThrows(
            ParsingException.class,
            () -> parse("TS test | PROMQL start \"not-a-date\" end \"2025-10-31T01:00:00Z\" step 1m (avg(foo))")
        );
        assertThat(e.getMessage(), containsString("1:24: Invalid date format [not-a-date]"));
    }

    public void testOnlyStartSpecified() {
        ParsingException e = assertThrows(
            ParsingException.class,
            () -> parse("TS test | PROMQL start \"2025-10-31T00:00:00Z\" step 1m (avg(foo))")
        );
        assertThat(
            e.getMessage(),
            containsString("Parameters [start] and [end] must either both be specified or both be omitted for a range query")
        );
    }

    public void testOnlyEndSpecified() {
        ParsingException e = assertThrows(
            ParsingException.class,
            () -> parse("TS test | PROMQL end \"2025-10-31T01:00:00Z\" step 1m (avg(foo))")
        );
        assertThat(
            e.getMessage(),
            containsString("Parameters [start] and [end] must either both be specified or both be omitted for a range query")
        );
    }

    public void testRangeQueryMissingStep() {
        ParsingException e = assertThrows(
            ParsingException.class,
            () -> parse("TS test | PROMQL start \"2025-10-31T00:00:00Z\" end \"2025-10-31T01:00:00Z\" (avg(foo))")
        );
        assertThat(e.getMessage(), containsString("Parameter [step] or [time] is required"));
    }

    private static PromqlCommand parse(String query) {
        return as(parser.createStatement(query), PromqlCommand.class);
    }

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }

}
