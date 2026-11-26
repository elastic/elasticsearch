/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql.selector;

import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.elasticsearch.xpack.esql.plan.logical.promql.selector.AutomatonUtils.literalAlternatives;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class AutomatonUtilsTests extends ESTestCase {

    public void testLiteralAlternatives() {
        assertThat(literalAlternatives("api"), equalTo(List.of("api")));
        assertThat(literalAlternatives("api|web|service"), equalTo(List.of("api", "web", "service")));
        // whitespace and a dash are literals in RE2
        assertThat(literalAlternatives("host-0| foo"), equalTo(List.of("host-0", " foo")));
    }

    /** An empty alternative matches the empty value wherever it is. */
    public void testEmptyAlternatives() {
        assertThat(literalAlternatives(""), equalTo(List.of("")));
        assertThat(literalAlternatives("foo|"), equalTo(List.of("foo", "")));
        assertThat(literalAlternatives("|foo"), equalTo(List.of("", "foo")));
        assertThat(literalAlternatives("a||b"), equalTo(List.of("a", "", "b")));
    }

    /**
     * A regex with a metacharacter anywhere is matched as a whole, so the split never needs to know whether a | is inside a
     * class, a group or a quote.
     */
    public void testMetacharacterKeepsTheRegexWhole() {
        for (String regex : List.of(
            "prod-.*|staging",
            "api|[0-9]+",
            "^prod|staging$",
            "(a|b)",
            "a\\|b",
            "\\Qa|b\\E",
            "[\\pL|]",
            "a|[]|]b",
            "[\\p{Greek}A-[:|:]",
            "a{2}|b",
            "a?|b"
        )) {
            assertThat(regex, literalAlternatives(regex), nullValue());
        }
    }

    public void testNullPattern() {
        assertThat(literalAlternatives(null), nullValue());
    }

    public void testTooManyAlternatives() {
        StringBuilder pattern = new StringBuilder("a0");
        for (int i = 1; i < 300; i++) {
            pattern.append("|a").append(i);
        }
        assertThat(literalAlternatives(pattern.toString()), nullValue());
    }
}
