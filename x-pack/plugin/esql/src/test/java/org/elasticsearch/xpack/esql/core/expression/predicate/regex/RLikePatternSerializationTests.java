/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.expression.predicate.regex;

import org.apache.lucene.util.automaton.RegExp;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class RLikePatternSerializationTests extends ESTestCase {

    private static final NamedWriteableRegistry REGISTRY = new NamedWriteableRegistry(List.of());

    public void testRoundTripKeepsSyntaxFlags() throws IOException {
        RLikePattern pattern = new RLikePattern("a<1-3>|~b", RegExp.NONE);
        assertThat(copyWriteable(pattern, REGISTRY, RLikePattern::new), equalTo(pattern));
    }

    /** A node that predates syntax flags reads the default ones, so only patterns that use the default reach it. */
    public void testOlderNodes() throws IOException {
        TransportVersion old = TransportVersionUtils.randomVersionNotSupporting(RLikePattern.ESQL_RLIKE_SYNTAX_FLAGS);

        RLikePattern pattern = new RLikePattern("a<1-3>|~b");
        assertThat(copyWriteable(pattern, REGISTRY, RLikePattern::new, old), equalTo(pattern));

        RLikePattern withFlags = new RLikePattern("a<1-3>|~b", RegExp.NONE);
        expectThrows(UnsupportedOperationException.class, () -> copyWriteable(withFlags, REGISTRY, RLikePattern::new, old));
    }

    public void testSyntaxFlagsChangeTheLanguage() {
        assertThat(new RLikePattern("a&b").exactMatch(), equalTo(null));
        assertThat(new RLikePattern("a&b", RegExp.NONE).exactMatch(), equalTo("a&b"));
        assertNotEquals(new RLikePattern("a&b"), new RLikePattern("a&b", RegExp.NONE));
    }
}
