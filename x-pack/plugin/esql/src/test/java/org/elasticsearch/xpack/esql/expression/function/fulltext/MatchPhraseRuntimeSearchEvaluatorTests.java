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
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.junit.Before;

import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.NULL;
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;
import static org.hamcrest.Matchers.instanceOf;

/**
 * End-to-end execution tests for runtime {@code match_phrase}, where the field is not a Lucene-mapped index field.
 * Unlike {@link MatchPhraseTests}, which only checks type resolution and serialization, this builds the actual
 * runtime evaluators and runs them over real {@link Block}s.
 * <p>
 * Runtime {@code match_phrase} covers the two behaviors of its pushed-down counterpart: on {@code text} expressions
 * (the {@code to_text(...)} case) the value is analyzed and matches only when all query tokens appear <em>in order
 * and in consecutive positions</em> (slop 0) — unlike runtime {@code match}, which succeeds when <em>any</em>
 * analyzed query token appears. On {@code keyword} expressions the pushed-down query rewrites to a term query, so
 * the runtime path preserves that: exact, unanalyzed value equality. Multivalue (any-value match) and null/missing
 * positions are exercised too.
 */
public class MatchPhraseRuntimeSearchEvaluatorTests extends AbstractRuntimeSearchEvaluatorTests {

    @Before
    public void assumeRuntimeMatchPhraseEnabled() {
        // Runtime match_phrase is gated behind a snapshot-only capability; there is nothing to test in release builds.
        assumeTrue("requires runtime match_phrase", MatchPhrase.runtimeSearchEnabled());
    }

    private static MatchPhrase runtimeMatchPhrase(String queryValue) {
        return runtimeMatchPhrase(TEXT, queryValue);
    }

    private static MatchPhrase runtimeMatchPhrase(DataType fieldType, String queryValue) {
        ReferenceAttribute field = new ReferenceAttribute(Source.EMPTY, "field", fieldType);
        Literal query = new Literal(Source.EMPTY, new BytesRef(queryValue), KEYWORD);
        MatchPhrase matchPhrase = new MatchPhrase(Source.EMPTY, field, query, null);
        assertTrue("expected a runtime search, not a pushed-down query", matchPhrase.isRuntimeSearch());
        return matchPhrase;
    }

    private Boolean[] evaluatePhrase(String query, String... values) {
        return evaluatePhrase(TEXT, query, values);
    }

    private Boolean[] evaluatePhrase(DataType fieldType, String query, String... values) {
        return evaluate(runtimeMatchPhrase(fieldType, query), factory -> bytesRefBlock(factory, builder -> {
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

    // ---- keyword: exact (unanalyzed) matching, mirroring the term query a pushed-down match_phrase rewrites to ----

    public void testKeywordIsExactAndCaseSensitive() {
        // Unlike text, keyword compares the whole value byte-for-byte: only the exact "hello" matches.
        Boolean[] result = evaluatePhrase(KEYWORD, "hello", "Hello", "hello", "hell");
        assertArrayEquals(new Boolean[] { false, true, false }, result);
    }

    public void testKeywordIsNotAnalyzed() {
        // A multi-word query on keyword is a single unanalyzed term: it matches the identical value only, with no
        // phrase (substring) or case-normalized matching.
        Boolean[] result = evaluatePhrase(KEYWORD, "brown fox", "brown fox", "a brown fox", "Brown Fox", "brown  fox");
        assertArrayEquals(new Boolean[] { true, false, false, false }, result);
    }

    public void testUnexpectedFieldTypeThrows() {
        // A field type without a dedicated runtime evaluator must fail loudly instead of silently falling through
        // to exact keyword matching. NULL is such a type: in a real plan a null field folds the whole function away
        // before evaluation, so it can only get here through a bug.
        MatchPhrase matchPhrase = runtimeMatchPhrase(NULL, "brown fox");
        expectThrows(EsqlIllegalArgumentException.class, () -> matchPhrase.toEvaluator(toEvaluator()));
    }

    public void testKeywordMultiValueAndNull() {
        // Matches if any value in the position equals the query exactly; nulls never match.
        Boolean[] result = evaluate(runtimeMatchPhrase(KEYWORD, "brown fox"), factory -> bytesRefBlock(factory, builder -> {
            builder.beginPositionEntry();
            builder.appendBytesRef(new BytesRef("white cat"));
            builder.appendBytesRef(new BytesRef("brown fox"));
            builder.endPositionEntry();
            builder.beginPositionEntry();
            builder.appendBytesRef(new BytesRef("a brown fox"));
            builder.appendBytesRef(new BytesRef("fox"));
            builder.endPositionEntry();
            builder.appendNull();
        }));
        assertArrayEquals(new Boolean[] { true, false, false }, result);
    }
}
