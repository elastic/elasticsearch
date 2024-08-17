/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.containsString;

public class GrammarInDevelopmentParsingTest extends ESTestCase {

    public void testDevelopmentInline() throws Exception {
        parse("row a = 1 | inlinestats b = min(a) by c, d.e", "inlinestats");
    }

    public void testDevelopmentLookup() throws Exception {
        parse("row a = 1 | lookup \"foo\" on j", "lookup");
    }

    public void testDevelopmentMetrics() throws Exception {
        parse("metrics foo", "metrics");
    }

    public void testDevelopmentMatch() throws Exception {
        parse("row a = 1 | match foo", "match");
    }

    void parse(String query, String errorMessage) {
        ParsingException pe = expectThrows(ParsingException.class, () -> parser().createStatement(query));
        // System.out.println(pe.getMessage());
        assertThat(pe.getMessage(), containsString("mismatched input '" + errorMessage + "'"));
    }

    private EsqlParser parser() {
        EsqlParser parser = new EsqlParser();
        assumeTrue(" requires snapshot builds", parser.config().devVersion);

        EsqlConfig config = new EsqlConfig();
        // manually disable dev mode (make it production)
        config.setDevVersion(false);
        parser.setEsqlConfig(config);
        return parser;
    }
}
