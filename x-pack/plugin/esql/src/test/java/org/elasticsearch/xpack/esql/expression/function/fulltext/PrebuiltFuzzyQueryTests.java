/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.fulltext;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.synonym.SynonymGraphFilter;
import org.apache.lucene.analysis.synonym.SynonymMap;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.BooleanSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.query.support.QueryParsers;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.planner.RuntimeSearchExecutionContext;
import org.elasticsearch.xpack.esql.querydsl.query.MatchPhraseQuery;
import org.elasticsearch.xpack.esql.querydsl.query.MatchQuery;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;

/**
 * Checks that {@link PrebuiltFuzzyQuery} matches and scores exactly like the {@link FuzzyQuery} it is built from on a
 * single-document {@link MemoryIndex}, the way runtime {@code match} evaluates it, and that
 * {@link RuntimeSearch#prebuildFuzzyAutomata} reaches every fuzzy clause of a compiled {@code match} query.
 */
public class PrebuiltFuzzyQueryTests extends ESTestCase {

    private static final String FIELD = "f";
    private static final int[] ASCII_ALPHABET = "abcd".codePoints().toArray();
    /** One-, two-, three- and four-byte UTF-8 code points, the last a surrogate pair in UTF-16. */
    private static final int[] UNICODE_ALPHABET = "abcé中𝄞".codePoints().toArray();

    /**
     * Rows mix typos of a base word with random words from a four-letter alphabet, so many terms fall within the edit
     * distance, often more than {@code max_expansions}.
     */
    public void testMatchesFuzzyQuery() throws IOException {
        assertMatchesFuzzyQuery(ASCII_ALPHABET, Lucene.STANDARD_ANALYZER);
    }

    /**
     * Like {@link #testMatchesFuzzyQuery} with multi-byte code points, where edit distances, prefix lengths and boosts
     * count code points rather than bytes. The whitespace analyzer keeps CJK characters in one token.
     */
    public void testMatchesFuzzyQueryUnicode() throws IOException {
        assertMatchesFuzzyQuery(UNICODE_ALPHABET, new WhitespaceAnalyzer());
    }

    private static void assertMatchesFuzzyQuery(int[] alphabet, Analyzer analyzer) throws IOException {
        MemoryIndex memoryIndex = new MemoryIndex();
        for (int i = 0; i < 500; i++) {
            String base = randomWord(alphabet, between(3, 8));
            StringBuilder row = new StringBuilder();
            int words = between(1, 300);
            for (int w = 0; w < words; w++) {
                row.append(randomBoolean() ? typo(alphabet, base) : randomWord(alphabet, between(1, 9))).append(' ');
            }
            memoryIndex.reset();
            memoryIndex.addField(FIELD, row.toString(), analyzer);

            int maxEdits = between(1, 2);
            int maxExpansions = randomBoolean() ? FuzzyQuery.defaultMaxExpansions : between(1, 10);
            Term term = new Term(FIELD, typo(alphabet, base));
            int prefixLength = between(0, 2);
            boolean transpositions = randomBoolean();
            MultiTermQuery.RewriteMethod rewriteMethod = randomRewriteMethod(maxExpansions);
            FuzzyQuery query = rewriteMethod == null
                ? new FuzzyQuery(term, maxEdits, prefixLength, maxExpansions, transpositions)
                : new FuzzyQuery(term, maxEdits, prefixLength, maxExpansions, transpositions, rewriteMethod);
            assertSameHitsAndScores(memoryIndex, query);
        }
    }

    /**
     * Nine terms within one edit of {@code abcd} and {@code max_expansions: 3}: only the top three terms may count,
     * which shows in the score.
     */
    public void testMoreMatchingTermsThanMaxExpansions() throws IOException {
        MemoryIndex memoryIndex = new MemoryIndex();
        memoryIndex.addField(FIELD, "abcd abca abcb abcc bbcd cbcd dbcd abd acbd", Lucene.STANDARD_ANALYZER);
        Term term = new Term(FIELD, "abcd");

        TermsEnum matching = memoryIndex.createSearcher()
            .getIndexReader()
            .leaves()
            .get(0)
            .reader()
            .terms(FIELD)
            .intersect(FuzzyQuery.getFuzzyAutomaton(term.text(), 1, 0, true), null);
        int matchingTerms = 0;
        while (matching.next() != null) {
            matchingTerms++;
        }
        assertEquals(9, matchingTerms);

        FuzzyQuery limited = new FuzzyQuery(term, 1, 0, 3, true);
        FuzzyQuery unlimited = new FuzzyQuery(term, 1, 0, 50, true);
        assertThat(score(memoryIndex, limited, new BooleanSimilarity()), lessThan(score(memoryIndex, unlimited, new BooleanSimilarity())));
        assertSameHitsAndScores(memoryIndex, limited);
        assertSameHitsAndScores(memoryIndex, unlimited);
        assertEquals(limited.toString(), new PrebuiltFuzzyQuery(limited).toString());
    }

    public void testRewriteSingleTerm() {
        assertFuzzyClausesPrebuilt(compile("bron", Lucene.STANDARD_ANALYZER, "fuzziness", "1"));
    }

    public void testRewriteMultipleTerms() {
        assertFuzzyClausesPrebuilt(compile("bron foxx", Lucene.STANDARD_ANALYZER, "fuzziness", "AUTO"));
    }

    public void testRewriteOperatorAnd() {
        assertFuzzyClausesPrebuilt(compile("bron foxx", Lucene.STANDARD_ANALYZER, "fuzziness", "AUTO", "operator", "AND"));
    }

    public void testRewriteMinimumShouldMatch() {
        assertFuzzyClausesPrebuilt(compile("bron foxx lazzy", Lucene.STANDARD_ANALYZER, "fuzziness", "AUTO", "minimum_should_match", "2"));
    }

    public void testRewriteBoost() {
        assertFuzzyClausesPrebuilt(compile("bron", Lucene.STANDARD_ANALYZER, "fuzziness", "1", "boost", 2.0f));
        assertFuzzyClausesPrebuilt(compile("bron foxx", Lucene.STANDARD_ANALYZER, "fuzziness", "1", "boost", 2.0f, "operator", "AND"));
    }

    /**
     * {@code fuzziness: AUTO} gives a two-letter term zero edits; that clause stays a plain {@link FuzzyQuery}.
     */
    public void testRewriteLeavesZeroEditsUntouched() {
        Query compiled = compile("ab cdef", Lucene.STANDARD_ANALYZER, "fuzziness", "AUTO");
        assertFuzzyClausesPrebuilt(compiled);
        List<FuzzyQuery> zeroEdits = fuzzyLeaves(RuntimeSearch.prebuildFuzzyAutomata(compiled)).stream()
            .filter(fuzzy -> fuzzy.getMaxEdits() == 0)
            .toList();
        assertEquals(1, zeroEdits.size());
        assertEquals(FuzzyQuery.class, zeroEdits.get(0).getClass());
    }

    /**
     * Queries without a fuzzy clause with edits come back as the same instance, so {@code match_phrase} and
     * non-fuzzy {@code match} queries pass through untouched.
     */
    public void testRewriteKeepsQueriesWithoutFuzzyEdits() throws IOException {
        for (Query compiled : List.of(
            compile("quick", Lucene.STANDARD_ANALYZER),
            compile("quick fox", Lucene.STANDARD_ANALYZER, "operator", "AND"),
            compile("quick fox", Lucene.STANDARD_ANALYZER, "boost", 2.0f, "minimum_should_match", "1"),
            compile("ab cd", Lucene.STANDARD_ANALYZER, "fuzziness", "AUTO", "boost", 2.0f),
            new MatchPhraseQuery(Source.EMPTY, RuntimeSearch.CONTENT_FIELD, "quick fox", Map.of("boost", 2.0f)).toQueryBuilder()
                .toQuery(RuntimeSearchExecutionContext.create(List.of(RuntimeSearch.CONTENT_FIELD), Lucene.STANDARD_ANALYZER))
        )) {
            assertSame(compiled, RuntimeSearch.prebuildFuzzyAutomata(compiled));
        }
    }

    /**
     * A multi-word synonym without phrase generation nests a boolean query of fuzzy clauses for the synonym's words.
     */
    public void testRewriteSynonymGraph() throws IOException {
        SynonymMap.Builder synonyms = new SynonymMap.Builder(true);
        synonyms.add(new CharsRef("ny"), SynonymMap.Builder.join(new String[] { "new", "york" }, new CharsRefBuilder()), true);
        SynonymMap map = synonyms.build();
        Analyzer analyzer = new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                WhitespaceTokenizer tokenizer = new WhitespaceTokenizer();
                return new TokenStreamComponents(tokenizer, new SynonymGraphFilter(tokenizer, map, true));
            }
        };
        NamedAnalyzer named = new NamedAnalyzer("synonyms", AnalyzerScope.GLOBAL, analyzer);
        Query compiled = compile("ny foxx", named, "fuzziness", "1", "operator", "AND", "auto_generate_synonyms_phrase_query", false);
        assertEquals(4, fuzzyLeaves(compiled).size());
        assertFuzzyClausesPrebuilt(compiled);
    }

    /**
     * Compiles a runtime {@code match} the way {@link RuntimeSearch} does, with the {@code lenient: true} that
     * {@link Match} always adds.
     */
    private static Query compile(String query, NamedAnalyzer analyzer, Object... options) {
        Map<String, Object> opts = new HashMap<>();
        opts.put("lenient", true);
        for (int i = 0; i < options.length; i += 2) {
            opts.put((String) options[i], options[i + 1]);
        }
        try {
            return new MatchQuery(Source.EMPTY, RuntimeSearch.CONTENT_FIELD, query, opts).toQueryBuilder()
                .toQuery(RuntimeSearchExecutionContext.create(List.of(RuntimeSearch.CONTENT_FIELD), analyzer));
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * Every fuzzy clause with edits ends up a {@link PrebuiltFuzzyQuery}, and the structure (occurs, minimum should
     * match, boosts) is unchanged, which shows in {@link Query#toString}.
     */
    private static void assertFuzzyClausesPrebuilt(Query compiled) {
        List<FuzzyQuery> before = fuzzyLeaves(compiled);
        assertThat(before, not(empty()));
        assertThat(before, everyItem(not(instanceOf(PrebuiltFuzzyQuery.class))));

        Query rewritten = RuntimeSearch.prebuildFuzzyAutomata(compiled);
        assertEquals(compiled.toString(), rewritten.toString());
        List<FuzzyQuery> after = fuzzyLeaves(rewritten);
        assertEquals(before.size(), after.size());
        for (FuzzyQuery fuzzy : after) {
            if (fuzzy.getMaxEdits() > 0) {
                assertThat(fuzzy, instanceOf(PrebuiltFuzzyQuery.class));
            } else {
                assertEquals(FuzzyQuery.class, fuzzy.getClass());
            }
        }
    }

    private static List<FuzzyQuery> fuzzyLeaves(Query query) {
        List<FuzzyQuery> leaves = new ArrayList<>();
        query.visit(new QueryVisitor() {
            @Override
            public void consumeTerms(Query query, Term... terms) {
                if (query instanceof FuzzyQuery fuzzy) {
                    leaves.add(fuzzy);
                }
            }

            @Override
            public void consumeTermsMatching(Query query, String field, Supplier<ByteRunAutomaton> automaton) {
                if (query instanceof FuzzyQuery fuzzy) {
                    leaves.add(fuzzy);
                }
            }

            @Override
            public QueryVisitor getSubVisitor(BooleanClause.Occur occur, Query parent) {
                return this;
            }
        });
        return leaves;
    }

    private static void assertSameHitsAndScores(MemoryIndex memoryIndex, FuzzyQuery query) throws IOException {
        PrebuiltFuzzyQuery prebuilt = new PrebuiltFuzzyQuery(query);
        for (Similarity similarity : new Similarity[] { new BooleanSimilarity(), new BM25Similarity() }) {
            TopDocs expected;
            try {
                expected = search(memoryIndex, query, similarity);
            } catch (RuntimeException e) {
                // e.g. a scoring boolean rewrite turns a candidate shorter than its edit distance into a negative boost
                RuntimeException actual = expectThrows(e.getClass(), () -> search(memoryIndex, prebuilt, similarity));
                assertEquals(e.getMessage(), actual.getMessage());
                continue;
            }
            TopDocs actual = search(memoryIndex, prebuilt, similarity);
            assertEquals(query + " " + similarity, expected.scoreDocs.length, actual.scoreDocs.length);
            if (expected.scoreDocs.length > 0) {
                assertEquals(query + " " + similarity, expected.scoreDocs[0].score, actual.scoreDocs[0].score, 0f);
            }
        }
    }

    private static float score(MemoryIndex memoryIndex, Query query, Similarity similarity) throws IOException {
        TopDocs topDocs = search(memoryIndex, query, similarity);
        assertEquals(1, topDocs.scoreDocs.length);
        return topDocs.scoreDocs[0].score;
    }

    private static TopDocs search(MemoryIndex memoryIndex, Query query, Similarity similarity) throws IOException {
        IndexSearcher searcher = memoryIndex.createSearcher();
        searcher.setSimilarity(similarity);
        return searcher.search(query, 1);
    }

    /**
     * A rewrite method as {@code fuzzy_rewrite} parses it; {@code null} leaves the {@link FuzzyQuery} default, as
     * {@code FuzzyQueries#create} does.
     */
    private static MultiTermQuery.RewriteMethod randomRewriteMethod(int maxExpansions) {
        String name = randomFrom(
            new String[] {
                null,
                "constant_score",
                "constant_score_blended",
                "constant_score_boolean",
                "scoring_boolean",
                "top_terms_" + maxExpansions,
                "top_terms_boost_" + maxExpansions,
                "top_terms_blended_freqs_" + maxExpansions }
        );
        return QueryParsers.parseRewriteMethod(name, null, LoggingDeprecationHandler.INSTANCE);
    }

    private static String randomWord(int[] alphabet, int length) {
        StringBuilder word = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            word.appendCodePoint(randomCodePoint(alphabet));
        }
        return word.toString();
    }

    /** Applies up to two random deletions, insertions, substitutions or transpositions of code points. */
    private static String typo(int[] alphabet, String word) {
        List<Integer> codePoints = new ArrayList<>(word.codePoints().boxed().toList());
        int edits = between(0, 2);
        for (int e = 0; e < edits && codePoints.size() > 1; e++) {
            int p = between(0, codePoints.size() - 1);
            switch (between(0, 3)) {
                case 0 -> codePoints.remove(p);
                case 1 -> codePoints.add(p, randomCodePoint(alphabet));
                case 2 -> codePoints.set(p, randomCodePoint(alphabet));
                case 3 -> {
                    if (p + 1 < codePoints.size()) {
                        Collections.swap(codePoints, p, p + 1);
                    }
                }
                default -> throw new AssertionError();
            }
        }
        StringBuilder sb = new StringBuilder();
        codePoints.forEach(sb::appendCodePoint);
        return sb.toString();
    }

    private static int randomCodePoint(int[] alphabet) {
        return alphabet[between(0, alphabet.length - 1)];
    }
}
