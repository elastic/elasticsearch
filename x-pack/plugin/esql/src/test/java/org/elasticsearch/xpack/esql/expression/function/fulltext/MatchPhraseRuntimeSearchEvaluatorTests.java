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
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

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

    private static MatchPhrase runtimeMatchPhraseWithOptions(String queryValue, MapExpression options) {
        ReferenceAttribute field = new ReferenceAttribute(Source.EMPTY, "field", TEXT);
        Literal query = new Literal(Source.EMPTY, new BytesRef(queryValue), KEYWORD);
        MatchPhrase matchPhrase = new MatchPhrase(Source.EMPTY, field, query, options);
        assertTrue("expected a runtime search, not a pushed-down query", matchPhrase.isRuntimeSearch());
        return matchPhrase;
    }

    private Boolean[] evaluatePhraseWithOptions(String query, MapExpression options, String... values) {
        return evaluate(runtimeMatchPhraseWithOptions(query, options), factory -> bytesRefBlock(factory, builder -> {
            for (String value : values) {
                builder.appendBytesRef(new BytesRef(value));
            }
        }));
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

    // ---- text with options: Lucene-query evaluator path ----

    public void testTextWithoutOptionsUsesOptimizedEvaluator() {
        MatchPhrase matchPhrase = runtimeMatchPhrase("brown fox");
        assertThat(matchPhrase.toEvaluator(toEvaluator()), instanceOf(RuntimeSearchTextEvaluator.Factory.class));
    }

    public void testTextWithOptionsUsesLuceneQueryEvaluator() {
        MatchPhrase matchPhrase = runtimeMatchPhraseWithOptions("brown fox", mapOptions("slop", "1"));
        assertThat(matchPhrase.toEvaluator(toEvaluator()), instanceOf(RuntimeSearchTextWithLuceneQueryEvaluator.Factory.class));
    }

    public void testTextWithSlopAllowsInterveningToken() {
        Boolean[] result = evaluatePhraseWithOptions(
            "brown fox",
            mapOptions("slop", "1"),
            "a brown fox",
            "a brown quick fox",
            "a brown very quick fox"
        );
        assertArrayEquals(new Boolean[] { true, true, false }, result);
    }

    public void testTextWithExplicitSlopZeroRequiresAdjacentPositions() {
        Boolean[] result = evaluatePhraseWithOptions("brown fox", mapOptions("slop", "0"), "a brown fox", "a brown quick fox");
        assertArrayEquals(new Boolean[] { true, false }, result);
    }

    public void testTextWithSlopTwoMatchesTransposedTerms() {
        // Transposed terms have a slop of 2: "fox brown" matches "brown fox" with slop 2 but not slop 1.
        Boolean[] result = evaluatePhraseWithOptions("fox brown", mapOptions("slop", "2"), "a brown fox");
        assertArrayEquals(new Boolean[] { true }, result);
        result = evaluatePhraseWithOptions("fox brown", mapOptions("slop", "1"), "a brown fox");
        assertArrayEquals(new Boolean[] { false }, result);
    }

    public void testTextWithOptionsPhraseCannotSpanValues() {
        Boolean[] result = evaluate(runtimeMatchPhraseWithOptions("brown fox", mapOptions("slop", "0")), factory -> {
            return bytesRefBlock(factory, builder -> {
                builder.beginPositionEntry();
                builder.appendBytesRef(new BytesRef("a brown")); // "brown" ends this value...
                builder.appendBytesRef(new BytesRef("fox b"));   // ..."fox" starts the next: not a phrase
                builder.endPositionEntry();
                builder.beginPositionEntry();
                builder.appendBytesRef(new BytesRef("a brown fox"));
                builder.appendBytesRef(new BytesRef("nothing here"));
                builder.endPositionEntry();
            });
        });
        assertArrayEquals(new Boolean[] { false, true }, result);
    }

    public void testTextWithZeroTermsQueryNoneAndNoTokensUsesConstantFalse() {
        MatchPhrase matchPhrase = runtimeMatchPhraseWithOptions("! ! !", mapOptions("zero_terms_query", "none"));
        assertThat(matchPhrase.toEvaluator(toEvaluator()), instanceOf(ConstantEvaluators.CONSTANT_FALSE_FACTORY.getClass()));
    }

    public void testTextWithZeroTermsQueryAllAndNoTokensUsesConstantTrue() {
        MatchPhrase matchPhrase = runtimeMatchPhraseWithOptions("! ! !", mapOptions("zero_terms_query", "all"));
        assertThat(matchPhrase.toEvaluator(toEvaluator()), instanceOf(ConstantEvaluators.CONSTANT_TRUE_FACTORY.getClass()));
    }

    public void testTextWithZeroTermsQueryAllMatchesEverything() {
        Boolean[] result = evaluatePhraseWithOptions("", mapOptions("zero_terms_query", "all"), "a brown fox", "the lazy dog");
        assertArrayEquals(new Boolean[] { true, true }, result);
    }

    public void testTextWithWhitespaceAnalyzerIsCaseSensitive() {
        // The whitespace analyzer does not lowercase, unlike the standard analyzer.
        Boolean[] result = evaluatePhraseWithOptions(
            "Brown Fox",
            mapOptions("analyzer", "whitespace"),
            "the Brown Fox runs",
            "the brown fox runs"
        );
        assertArrayEquals(new Boolean[] { true, false }, result);
    }

    public void testTextWithKeywordAnalyzerMatchesWholeValueOnly() {
        // The keyword analyzer emits the whole value as a single token, so the phrase must equal the entire value.
        Boolean[] result = evaluatePhraseWithOptions("brown fox", mapOptions("analyzer", "keyword"), "brown fox", "a brown fox");
        assertArrayEquals(new Boolean[] { true, false }, result);
    }

    public void testTextWithAnalyzerAndSlopCombined() {
        Boolean[] result = evaluatePhraseWithOptions(
            "Brown Fox",
            mapOptions("analyzer", "whitespace", "slop", "1"),
            "the Brown quick Fox",
            "the brown quick fox"
        );
        assertArrayEquals(new Boolean[] { true, false }, result);
    }

    public void testTextWithBoostDoesNotChangeMatching() {
        // Boost only affects scoring, not matching.
        Boolean[] result = evaluatePhraseWithOptions("brown fox", mapOptions("boost", "2.5"), "a brown fox", "a brown quick fox");
        assertArrayEquals(new Boolean[] { true, false }, result);
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

    // ---- scoring: runtime match_phrase contributes the boost (1.0 by default) to _score on match ----

    private Double[] scorePhrase(DataType fieldType, String query, String... values) {
        return score(runtimeMatchPhrase(fieldType, query), factory -> bytesRefBlock(factory, builder -> {
            for (String value : values) {
                builder.appendBytesRef(new BytesRef(value));
            }
        }));
    }

    private Double[] scorePhraseWithOptions(String query, MapExpression options, String... values) {
        return score(runtimeMatchPhraseWithOptions(query, options), factory -> bytesRefBlock(factory, builder -> {
            for (String value : values) {
                builder.appendBytesRef(new BytesRef(value));
            }
        }));
    }

    public void testScorePhrase() {
        Double[] result = scorePhrase(TEXT, "brown fox", "a brown fox", "fox brown a");
        assertArrayEquals(new Double[] { 1.0, 0.0 }, result);
    }

    public void testScorePhraseWithSlop() {
        Double[] result = scorePhraseWithOptions("fox brown", mapOptions("slop", "2"), "a brown fox");
        assertArrayEquals(new Double[] { 1.0 }, result);
    }

    public void testScorePhraseWithBoost() {
        Double[] result = scorePhraseWithOptions("brown fox", mapOptions("boost", "2.5"), "a brown fox", "nothing here");
        assertArrayEquals(new Double[] { 2.5, 0.0 }, result);
    }

    public void testScorePhraseWithAnalyzer() {
        Double[] result = scorePhraseWithOptions("Brown Fox", mapOptions("analyzer", "whitespace"), "a Brown Fox", "a brown fox");
        assertArrayEquals(new Double[] { 1.0, 0.0 }, result);
    }

    public void testScorePhraseWithZeroTermsQuery() {
        Double[] all = scorePhraseWithOptions("! !", mapOptions("zero_terms_query", "all"), "a brown fox", "nothing here");
        assertArrayEquals(new Double[] { 1.0, 1.0 }, all);
        Double[] none = scorePhraseWithOptions("! !", mapOptions("zero_terms_query", "none"), "a brown fox", "nothing here");
        assertArrayEquals(new Double[] { 0.0, 0.0 }, none);
    }

    /**
     * The Lucene-query scorer (options path) builds its position-increment-gap analyzer independently of the
     * boolean evaluator. This test ensures that scoring does not span multiple values.
     */
    public void testScorePhraseWithOptionsCannotSpanValues() {
        Double[] result = score(runtimeMatchPhraseWithOptions("brown fox", mapOptions("slop", "0")), factory -> {
            return bytesRefBlock(factory, builder -> {
                builder.beginPositionEntry();
                builder.appendBytesRef(new BytesRef("a brown")); // "brown" ends this value...
                builder.appendBytesRef(new BytesRef("fox b"));   // ..."fox" starts the next: not a phrase
                builder.endPositionEntry();
                builder.beginPositionEntry();
                builder.appendBytesRef(new BytesRef("a brown fox"));
                builder.appendBytesRef(new BytesRef("nothing here"));
                builder.endPositionEntry();
            });
        });
        assertArrayEquals(new Double[] { 0.0, 1.0 }, result);
    }

    public void testScorePhraseMultiValueAndNull() {
        // A phrase never spans a value boundary; any single value containing it scores the row; nulls score 0.0.
        Double[] result = score(runtimeMatchPhrase(TEXT, "brown fox"), factory -> bytesRefBlock(factory, builder -> {
            builder.beginPositionEntry();
            builder.appendBytesRef(new BytesRef("a brown"));
            builder.appendBytesRef(new BytesRef("fox b"));
            builder.endPositionEntry();
            builder.beginPositionEntry();
            builder.appendBytesRef(new BytesRef("white cat"));
            builder.appendBytesRef(new BytesRef("a brown fox"));
            builder.endPositionEntry();
            builder.appendNull();
        }));
        assertArrayEquals(new Double[] { 0.0, 1.0, 0.0 }, result);
    }

    public void testScoreKeywordExact() {
        Double[] result = scorePhrase(KEYWORD, "brown fox", "brown fox", "a brown fox");
        assertArrayEquals(new Double[] { 1.0, 0.0 }, result);
    }
}
