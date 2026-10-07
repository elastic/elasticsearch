/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.fulltext;

import org.apache.lucene.analysis.Analyzer;
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
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.planner.RuntimeSearchExecutionContext;
import org.elasticsearch.xpack.esql.querydsl.query.MatchQuery;

import java.io.IOException;
import java.util.ArrayList;
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
    private static final String ALPHABET = "abcd";

    /**
     * Rows mix typos of a base word with random words from a four-letter alphabet, so many terms fall within the edit
     * distance, often more than {@code max_expansions}.
     */
    public void testMatchesFuzzyQuery() throws IOException {
        MemoryIndex memoryIndex = new MemoryIndex();
        for (int i = 0; i < 500; i++) {
            String base = randomWord(between(3, 8));
            StringBuilder row = new StringBuilder();
            int words = between(1, 300);
            for (int w = 0; w < words; w++) {
                row.append(randomBoolean() ? typo(base) : randomWord(between(1, 9))).append(' ');
            }
            memoryIndex.reset();
            memoryIndex.addField(FIELD, row.toString(), Lucene.STANDARD_ANALYZER);

            int maxEdits = between(0, 2);
            int maxExpansions = randomBoolean() ? FuzzyQuery.defaultMaxExpansions : between(1, 10);
            FuzzyQuery query = new FuzzyQuery(
                new Term(FIELD, typo(base)),
                maxEdits,
                between(0, 2),
                maxExpansions,
                randomBoolean(),
                randomRewriteMethod(maxExpansions)
            );
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
            } catch (IllegalArgumentException e) {
                // A scoring boolean rewrite turns a candidate shorter than its edit distance into a negative boost
                IllegalArgumentException actual = expectThrows(
                    IllegalArgumentException.class,
                    () -> search(memoryIndex, prebuilt, similarity)
                );
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

    private static MultiTermQuery.RewriteMethod randomRewriteMethod(int maxExpansions) {
        return switch (between(0, 4)) {
            case 0 -> FuzzyQuery.defaultRewriteMethod(maxExpansions);
            case 1 -> new MultiTermQuery.TopTermsScoringBooleanQueryRewrite(maxExpansions);
            case 2 -> new MultiTermQuery.TopTermsBoostOnlyBooleanQueryRewrite(maxExpansions);
            case 3 -> MultiTermQuery.SCORING_BOOLEAN_REWRITE;
            case 4 -> MultiTermQuery.CONSTANT_SCORE_BLENDED_REWRITE;
            default -> throw new AssertionError();
        };
    }

    private static String randomWord(int length) {
        StringBuilder word = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            word.append(randomAlphabetChar());
        }
        return word.toString();
    }

    /** Applies up to two random deletions, insertions, substitutions or transpositions. */
    private static String typo(String word) {
        StringBuilder sb = new StringBuilder(word);
        int edits = between(0, 2);
        for (int e = 0; e < edits && sb.length() > 1; e++) {
            int p = between(0, sb.length() - 1);
            switch (between(0, 3)) {
                case 0 -> sb.deleteCharAt(p);
                case 1 -> sb.insert(p, randomAlphabetChar());
                case 2 -> sb.setCharAt(p, randomAlphabetChar());
                case 3 -> {
                    if (p + 1 < sb.length()) {
                        char c = sb.charAt(p);
                        sb.setCharAt(p, sb.charAt(p + 1));
                        sb.setCharAt(p + 1, c);
                    }
                }
                default -> throw new AssertionError();
            }
        }
        return sb.toString();
    }

    private static char randomAlphabetChar() {
        return ALPHABET.charAt(between(0, ALPHABET.length() - 1));
    }
}
