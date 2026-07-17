/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.fulltext;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.expression.ConstantEvaluators;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.junit.Before;

import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;
import static org.hamcrest.Matchers.instanceOf;

/**
 * End-to-end execution tests for runtime {@code match_phrase}, where the field is not a Lucene-mapped index field.
 * Unlike {@link MatchPhraseTests}, which only checks type resolution and serialization, this builds the actual
 * runtime evaluators and runs them over real {@link Block}s.
 * <p>
 * This first chunk of runtime {@code match_phrase} only supports {@code text} expressions (the {@code to_text(...)}
 * case). Unlike runtime {@code match}, which succeeds when <em>any</em> analyzed query token appears in the value,
 * a phrase only matches when all query tokens appear <em>in order and in consecutive positions</em> (slop 0).
 * Multivalue (any-value match) and null/missing positions are exercised too.
 */
public class MatchPhraseRuntimeSearchEvaluatorTests extends AbstractRuntimeSearchEvaluatorTests {

    @Before
    public void assumeRuntimeMatchPhraseEnabled() {
        // Runtime match_phrase is gated behind a snapshot-only capability; there is nothing to test in release builds.
        assumeTrue("requires runtime match_phrase", MatchPhrase.runtimeSearchEnabled());
    }

    private static MatchPhrase runtimeMatchPhrase(String queryValue) {
        ReferenceAttribute field = new ReferenceAttribute(Source.EMPTY, "field", TEXT);
        Literal query = new Literal(Source.EMPTY, new BytesRef(queryValue), KEYWORD);
        MatchPhrase matchPhrase = new MatchPhrase(Source.EMPTY, field, query, null);
        assertTrue("expected a runtime search, not a pushed-down query", matchPhrase.isRuntimeSearch());
        return matchPhrase;
    }

    private Boolean[] evaluatePhrase(String query, String... values) {
        return evaluate(runtimeMatchPhrase(query), factory -> bytesRefBlock(factory, builder -> {
            for (String value : values) {
                builder.appendBytesRef(new BytesRef(value));
            }
        }));
    }

    public void testPhraseMatchesConsecutiveTokens() {
        Boolean[] result = evaluatePhrase(
            "brown fox",
            "This is a brown fox",
            "This is a brown dog",
            "The quick brown fox jumps over the lazy dog"
        );
        assertArrayEquals(new Boolean[] { true, false, true }, result);
    }

    public void testPhraseOrderMatters() {
        // Unlike runtime match, both tokens being present is not enough: they must appear in query order.
        Boolean[] result = evaluatePhrase("fox brown", "This is a brown fox", "fox is brown");
        assertArrayEquals(new Boolean[] { false, false }, result);
    }

    public void testPhraseRequiresAdjacentPositions() {
        // Slop is 0: intervening tokens break the phrase.
        Boolean[] result = evaluatePhrase("brown fox", "brown quick fox", "brown fox");
        assertArrayEquals(new Boolean[] { false, true }, result);
    }

    public void testPhraseIsAnalyzed() {
        // The standard analyzer lowercases and strips punctuation, keeping positions consecutive.
        Boolean[] result = evaluatePhrase("brown fox", "a Brown FOX!", "One brown, fox again");
        assertArrayEquals(new Boolean[] { true, true }, result);
    }

    public void testSingleTermPhrase() {
        // A single-term phrase degrades to simple (analyzed) term presence.
        Boolean[] result = evaluatePhrase("fox", "This is a brown fox", "The cat sat on the mat");
        assertArrayEquals(new Boolean[] { true, false }, result);
    }

    public void testLongerPhrase() {
        Boolean[] result = evaluatePhrase(
            "quick brown fox jumps",
            "The quick brown fox jumps over the lazy dog",
            "The quick brown fox sleeps"
        );
        assertArrayEquals(new Boolean[] { true, false }, result);
    }

    public void testPhraseWithRepeatedTokens() {
        Boolean[] result = evaluatePhrase("dog dog", "dog dog", "dog brown dog", "dog");
        assertArrayEquals(new Boolean[] { true, false, false }, result);
    }

    public void testPhraseMatchesAtValueBoundaries() {
        // Phrase at the very start and very end of the value.
        Boolean[] result = evaluatePhrase("brown fox", "brown fox runs", "he saw a brown fox");
        assertArrayEquals(new Boolean[] { true, true }, result);
    }

    public void testMultiValueAndNull() {
        // Matches if any single value in the position matches; the phrase cannot span values; nulls never match.
        Boolean[] result = evaluate(runtimeMatchPhrase("brown fox"), factory -> bytesRefBlock(factory, builder -> {
            builder.beginPositionEntry();
            builder.appendBytesRef(new BytesRef("white cat"));
            builder.appendBytesRef(new BytesRef("a brown fox"));
            builder.endPositionEntry();
            builder.beginPositionEntry();
            builder.appendBytesRef(new BytesRef("this is brown"));
            builder.appendBytesRef(new BytesRef("fox and more"));
            builder.endPositionEntry();
            builder.appendNull();
        }));
        assertArrayEquals(new Boolean[] { true, false, false }, result);
    }

    public void testValuesThatAnalyzeToZeroTerms() {
        Boolean[] result = evaluatePhrase("brown fox", "! !", "");
        assertArrayEquals(new Boolean[] { false, false }, result);
    }

    public void testQueryWithZeroTermsUsesConstantBlock() {
        // The default zero_terms_query is "none", so a query that analyzes to no tokens matches nothing.
        MatchPhrase matchPhrase = runtimeMatchPhrase("! ! !");
        assertThat(matchPhrase.toEvaluator(toEvaluator()), instanceOf(ConstantEvaluators.CONSTANT_FALSE_FACTORY.getClass()));
    }
}
