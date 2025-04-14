/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

public class GrammarInDevelopmentParsingTests extends ESTestCase {

    public void testDevelopmentInline() throws Exception {
        parse("row a = 1 | inlinestats b = min(a) by c, d.e", "inlinestats");
    }

    public void testDevelopmentLookup() throws Exception {
        parse("row a = 1 | lookup_\uD83D\uDC14 \"foo\" on j", "lookup_\uD83D\uDC14");
    }

    public void testDevelopmentMetrics() throws Exception {
        parse("TS foo", "TS");
    }

    public void testDevelopmentMatch() throws Exception {
        parse("row a = 1 | match foo", "match");
    }

    public void testDevelopmentRerank() {
        parse("row a = 1 | rerank \"foo\" on title with reranker", "rerank");
    }

    public void testDevelopmentCompletion() {
        parse("row a = 1 | completion concat(\"test\", \"a\") with inferenceId as fieldName", "completion");
    }

    void parse(String query, String errorMessage) {
        ParsingException pe = expectThrows(ParsingException.class, () -> parser().createStatement(query));
        assertThat(pe.getMessage(), containsString("mismatched input '" + errorMessage + "'"));
        // check the parser eliminated the DEV_ tokens from the message
        assertThat(pe.getMessage(), not(containsString("DEV_")));
    }

    private EsqlParser parser() {
        EsqlParser parser = new EsqlParser();
        EsqlConfig config = parser.config();
        assumeTrue(" requires snapshot builds", config.devVersion);

        // manually disable dev mode (make it production)
        config.setDevVersion(false);
        return parser;
    }
}
