/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.expression.predicate.regex;

import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class RLikePatternSerializationTests extends ESTestCase {

    private static final NamedWriteableRegistry REGISTRY = new NamedWriteableRegistry(List.of());

    public void testRoundTripKeepsSyntax() throws IOException {
        RLikePattern pattern = new RLikePattern("(?i)a<1-3>|~b", RLikePattern.Syntax.RE2);
        assertThat(copyWriteable(pattern, REGISTRY, RLikePattern::new), equalTo(pattern));
    }

    /** A node that predates RE2 patterns reads Lucene syntax, so only Lucene patterns reach it. */
    public void testOlderNodes() throws IOException {
        TransportVersion old = TransportVersionUtils.randomVersionNotSupporting(RLikePattern.ESQL_RLIKE_RE2_SYNTAX);

        RLikePattern pattern = new RLikePattern("a<1-3>|~b");
        assertThat(copyWriteable(pattern, REGISTRY, RLikePattern::new, old), equalTo(pattern));

        RLikePattern re2 = new RLikePattern("a<1-3>|~b", RLikePattern.Syntax.RE2);
        expectThrows(UnsupportedOperationException.class, () -> copyWriteable(re2, REGISTRY, RLikePattern::new, old));
    }

    public void testSyntaxChangesTheLanguage() {
        assertThat(new RLikePattern("a&b").exactMatch(), equalTo(null));
        assertThat(new RLikePattern("a&b", RLikePattern.Syntax.RE2).exactMatch(), equalTo("a&b"));
        assertThat(new RLikePattern("\"q\"").exactMatch(), equalTo("q"));
        assertThat(new RLikePattern("\"q\"", RLikePattern.Syntax.RE2).exactMatch(), equalTo("\"q\""));
        assertNotEquals(new RLikePattern("a&b"), new RLikePattern("a&b", RLikePattern.Syntax.RE2));
    }

    public void testRe2IgnoreCase() {
        RLikePattern pattern = new RLikePattern("ab(?-i)c", RLikePattern.Syntax.RE2);
        assertTrue(Operations.run(pattern.createAutomaton(true), "ABc"));
        assertFalse(Operations.run(pattern.createAutomaton(true), "ABC"));
        assertFalse(Operations.run(pattern.createAutomaton(false), "ABc"));
    }
}
