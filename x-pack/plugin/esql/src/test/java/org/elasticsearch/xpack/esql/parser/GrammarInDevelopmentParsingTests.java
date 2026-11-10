/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_FUNCTION_REGISTRY;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class GrammarInDevelopmentParsingTests extends ESTestCase {

    public void testDevelopmentInline() throws Exception {
        LogicalPlan plan = parser().parseQuery("row a = 1 | inline stats b = min(a) by c, d.e");
        assertNotNull(plan);
    }

    public void testDevelopmentLookup() throws Exception {
        parse("row a = 1 | lookup_\uD83D\uDC14 \"foo\" on j", "lookup_\uD83D\uDC14");
    }

    public void testDevelopmentMatch() throws Exception {
        parse("row a = 1 | match foo", "match");
    }

    public void testDevelopmentLambda() throws Exception {
        // outside of dev mode, `->` lexes as ARROW but the lambda parser rule is predicate-guarded
        expectThrows(ParsingException.class, () -> parser().parseQuery("row a = 1 | eval b = invoke(a, x -> x)"));
    }

    /**
     * A release build must not merely refuse a call, it must refuse it the way it refused one before table
     * functions existed — as a stray {@code (} after an index pattern. Asserting the exact message is what makes
     * this discriminate: both predicates can be removed and a {@link ParsingException} still arrives, because the
     * other one still refuses, so "some parse error" is satisfied by a build that has silently gained the syntax.
     */
    public void testDevelopmentTableFunction() throws Exception {
        parse("from dataset(a, {\"resource\": \"x\"})", "(", "line 1:13");
        parse("from te()st", "(", "line 1:8");
    }

    /**
     * The token is only reachable behind that predicate, but the expected-token set a syntax error prints is
     * computed from the ATN, which ignores predicates — so the token's name would reach a production user's
     * error message if it were not DEV_-prefixed for the error listener to strip.
     */
    public void testTableFunctionTokenIsNotNamedInProductionErrors() throws Exception {
        ParsingException pe = expectThrows(ParsingException.class, () -> parser().parseQuery("from ::::data"));
        assertThat(pe.getMessage(), containsString("expecting {QUOTED_STRING, '(', UNQUOTED_SOURCE}"));
        assertThat(pe.getMessage(), not(containsString("DEV_")));
    }

    void parse(String query, String errorMessage) {
        ParsingException pe = expectThrows(ParsingException.class, () -> parser().parseQuery(query));
        assertThat(pe.getMessage(), containsString("mismatched input '" + errorMessage + "'"));
        // check the parser eliminated the DEV_ tokens from the message
        assertThat(pe.getMessage(), not(containsString("DEV_")));
    }

    /** As above, and at the position a release build reports — which is what a gained-syntax build would move. */
    void parse(String query, String errorMessage, String position) {
        ParsingException pe = expectThrows(ParsingException.class, () -> parser().parseQuery(query));
        assertThat(
            pe.getMessage(),
            equalTo(position + ": mismatched input '" + errorMessage + "' expecting {<EOF>, '|', '::', ',', 'metadata'}")
        );
        assertThat(pe.getMessage(), not(containsString("DEV_")));
    }

    private EsqlParser parser() {
        assumeTrue(" requires snapshot builds", new EsqlConfig(TEST_FUNCTION_REGISTRY).isDevVersion());

        // manually disable dev mode (make it production)
        EsqlConfig config = new EsqlConfig(false, TEST_FUNCTION_REGISTRY);
        return new EsqlParser(config);
    }
}
