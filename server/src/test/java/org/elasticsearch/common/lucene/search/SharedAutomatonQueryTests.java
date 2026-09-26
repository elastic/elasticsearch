/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.lucene.search;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.automaton.Automaton;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.containsString;

/**
 * Equality drives query cache hits, so a clause must never match another that resolves to a different automaton,
 * field or rewrite method.
 */
public class SharedAutomatonQueryTests extends ESTestCase {

    private static SharedAutomaton automaton(String field, String pattern) {
        return SharedAutomaton.compile(
            AutomatonQueries.toWildcardAutomaton(new Term(field, pattern), new NoopCircuitBreaker("test")),
            new NoopCircuitBreaker("test"),
            "wildcard"
        );
    }

    private static SharedAutomatonQuery query(String field, String pattern, SharedAutomaton shared, MultiTermQuery.RewriteMethod rewrite) {
        Term term = new Term(field, pattern);
        return new SharedAutomatonQuery(term, shared, SharedAutomatonQuery.fieldPrefixed(term, pattern), rewrite);
    }

    public void testCompileRejectsNonDeterministicAutomaton() {
        Automaton nfa = AutomatonQueries.toWildcardNFA(new Term("field", "a*b*c"));
        assumeTrue("pattern must yield an NFA for this to be meaningful", nfa.isDeterministic() == false);

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> SharedAutomaton.compile(nfa, new NoopCircuitBreaker("test"), "wildcard")
        );
        assertThat(e.getMessage(), containsString("require a determinized automaton"));
    }

    public void testEqualsAndHashCode() {
        SharedAutomaton shared = automaton("field", "foo*");
        SharedAutomatonQuery query = query("field", "foo*", shared, MultiTermQuery.CONSTANT_SCORE_BLENDED_REWRITE);
        SharedAutomatonQuery same = query("field", "foo*", shared, MultiTermQuery.CONSTANT_SCORE_BLENDED_REWRITE);

        assertEquals(query, same);
        assertEquals(same, query);
        assertEquals(query.hashCode(), same.hashCode());

        // Two clauses that share one automaton but sit on different fields must stay distinct.
        assertNotEquals(query, query("other", "foo*", shared, MultiTermQuery.CONSTANT_SCORE_BLENDED_REWRITE));
        assertNotEquals(query, query("field", "foo*", shared, MultiTermQuery.DOC_VALUES_REWRITE));
        assertNotEquals(query, query("field", "bar*", automaton("field", "bar*"), MultiTermQuery.CONSTANT_SCORE_BLENDED_REWRITE));
    }

    public void testNotEqualToOtherTypes() {
        SharedAutomatonQuery query = query("field", "foo*", automaton("field", "foo*"), MultiTermQuery.CONSTANT_SCORE_BLENDED_REWRITE);

        // assertNotEquals calls equals on its first argument, so the query under test has to stay there.
        assertNotEquals(query, new TermQuery(new Term("field", "foo*")));
    }

    public void testToStringUsesTheSuppliedRenderer() {
        Term term = new Term("field", "foo*");
        SharedAutomatonQuery prefixed = new SharedAutomatonQuery(
            term,
            automaton("field", "foo*"),
            SharedAutomatonQuery.fieldPrefixed(term, "foo*"),
            MultiTermQuery.CONSTANT_SCORE_BLENDED_REWRITE
        );
        assertEquals("foo*", prefixed.toString("field"));
        assertEquals("field:foo*", prefixed.toString(""));

        // Renderers that ignore the field keep their own shape, as the case-insensitive wildcard does.
        SharedAutomatonQuery custom = new SharedAutomatonQuery(
            term,
            automaton("field", "foo*"),
            f -> "Custom{" + f + ":foo*}",
            MultiTermQuery.CONSTANT_SCORE_BLENDED_REWRITE
        );
        assertEquals("Custom{:foo*}", custom.toString());
    }
}
