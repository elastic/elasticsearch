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
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.query.support.QueryParsers;
import org.elasticsearch.lucene.search.FuzzyQueries;
import org.elasticsearch.search.internal.MaxClauseCountQueryVisitor;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.planner.RuntimeSearchExecutionContext;
import org.elasticsearch.xpack.esql.querydsl.query.MatchPhraseQuery;
import org.elasticsearch.xpack.esql.querydsl.query.MatchQuery;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

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
        Query compiled = compile(
            "ny foxx",
            synonymAnalyzer(),
            "fuzziness",
            "1",
            "operator",
            "AND",
            "auto_generate_synonyms_phrase_query",
            false
        );
        assertEquals(4, fuzzyLeaves(compiled).size());
        assertFuzzyClausesPrebuilt(compiled);
    }

    /**
     * Random runtime {@code match} queries with fuzziness and the options that shape the boolean query around the fuzzy
     * clauses (operator, {@code minimum_should_match}, boost, a multi-word synonym), checked by hits and scores against
     * the compiled query on random rows. Half the time the budget is random, so some clauses stay plain.
     */
    public void testPrebuiltMatchesCompiledQuery() throws IOException {
        NamedAnalyzer synonyms = synonymAnalyzer();
        MemoryIndex memoryIndex = new MemoryIndex();
        for (int i = 0; i < 100; i++) {
            List<String> words = new ArrayList<>(randomList(1, 5, () -> randomWord(ASCII_ALPHABET, between(2, 8))));
            boolean useSynonyms = randomBoolean();
            if (useSynonyms) {
                words.add(between(0, words.size()), "ny");
            }
            Map<String, Object> options = new HashMap<>();
            options.put("fuzziness", randomFrom("1", "2", "AUTO"));
            if (randomBoolean()) {
                options.put("operator", randomFrom("AND", "OR"));
            }
            if (randomBoolean()) {
                options.put("minimum_should_match", randomFrom("1", "2", "-1", "50%"));
            }
            if (randomBoolean()) {
                options.put("boost", randomFrom(0.5f, 2.0f, 3.5f));
            }
            if (randomBoolean()) {
                options.put("prefix_length", between(0, 2));
            }
            if (randomBoolean()) {
                options.put("max_expansions", between(1, 10));
            }
            if (randomBoolean()) {
                options.put("fuzzy_transpositions", randomBoolean());
            }
            String rewrite = randomRewriteName(between(1, 10));
            if (rewrite != null) {
                options.put("fuzzy_rewrite", rewrite);
            }
            if (useSynonyms) {
                options.put("auto_generate_synonyms_phrase_query", false);
            }
            Query compiled = compile(String.join(" ", words), useSynonyms ? synonyms : Lucene.STANDARD_ANALYZER, options);

            long budget = randomBoolean() ? RuntimeSearch.PREBUILT_FUZZY_AUTOMATA_BUDGET_BYTES : randomLongBetween(0, 2 << 20);
            Query prebuilt = RuntimeSearch.prebuildFuzzyAutomata(compiled, budget);
            long retained = distinctPrebuilt(prebuilt).stream().mapToLong(PrebuiltFuzzyQuery::ramBytesUsed).sum();
            assertThat(retained, lessThanOrEqualTo(budget));

            for (int r = 0; r < 5; r++) {
                StringBuilder row = new StringBuilder();
                int rowWords = between(1, 50);
                for (int w = 0; w < rowWords; w++) {
                    String word = switch (between(0, 2)) {
                        case 0 -> typo(ASCII_ALPHABET, randomFrom(words));
                        case 1 -> randomWord(ASCII_ALPHABET, between(1, 9));
                        case 2 -> typo(ASCII_ALPHABET, randomFrom("new", "york"));
                        default -> throw new AssertionError();
                    };
                    row.append(word).append(' ');
                }
                memoryIndex.reset();
                memoryIndex.addField(RuntimeSearch.CONTENT_FIELD, row.toString(), Lucene.STANDARD_ANALYZER);
                assertSameHitsAndScores(memoryIndex, compiled, prebuilt);
            }
        }
    }

    /**
     * {@link RuntimeSearch#compileQuery} is the single compile path of both runtime evaluator factories, so this fails
     * if it stops prebuilding.
     */
    public void testCompileQueryPrebuildsFuzzyClauses() {
        Map<String, Object> options = Map.of("lenient", true, "fuzziness", "AUTO", "operator", "AND");
        Query compiled = RuntimeSearch.compileQuery(
            new MatchQuery(Source.EMPTY, RuntimeSearch.CONTENT_FIELD, "brwon foxx", options),
            Lucene.STANDARD_ANALYZER
        );
        List<FuzzyQuery> leaves = fuzzyLeaves(compiled);
        assertEquals(2, leaves.size());
        assertThat(leaves, everyItem(instanceOf(PrebuiltFuzzyQuery.class)));
    }

    /**
     * A compiled {@code match} gets one fuzzy clause per occurrence of a word; the occurrences share one prebuilt query.
     */
    public void testRepeatedTermsSharePrebuiltQuery() {
        Query compiled = compile("bron foxx bron lazy bron foxx", Lucene.STANDARD_ANALYZER, "fuzziness", "1");
        Query rewritten = RuntimeSearch.prebuildFuzzyAutomata(compiled);
        List<FuzzyQuery> leaves = fuzzyLeaves(rewritten);
        assertEquals(6, leaves.size());
        assertThat(leaves, everyItem(instanceOf(PrebuiltFuzzyQuery.class)));
        assertEquals(3, distinctPrebuilt(rewritten).size());
        Map<String, List<FuzzyQuery>> byTerm = leaves.stream().collect(Collectors.groupingBy(fuzzy -> fuzzy.getTerm().text()));
        assertEquals(
            Map.of("bron", 3, "foxx", 2, "lazy", 1),
            byTerm.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().size()))
        );
        for (List<FuzzyQuery> occurrences : byTerm.values()) {
            assertThat(occurrences, everyItem(sameInstance(occurrences.get(0))));
        }
    }

    /**
     * With a budget that only fits the estimate of the first clause, {@code bron}, the longer clauses after it stay the
     * original plain {@link FuzzyQuery}s, and the query still matches and scores the same.
     */
    public void testClausesPastBudgetStayPlain() throws IOException {
        Query compiled = compile("bron quick brown jumps", Lucene.STANDARD_ANALYZER, "fuzziness", "1", "minimum_should_match", "2");
        List<FuzzyQuery> plain = fuzzyLeaves(compiled);
        assertEquals(4, plain.size());
        FuzzyQuery bron = plain.stream().filter(fuzzy -> fuzzy.getTerm().text().equals("bron")).findFirst().orElseThrow();

        Query rewritten = RuntimeSearch.prebuildFuzzyAutomata(compiled, FuzzyQueries.estimateBytes(bron, 1));
        for (FuzzyQuery leaf : fuzzyLeaves(rewritten)) {
            if (leaf.getTerm().text().equals("bron")) {
                assertThat(leaf, instanceOf(PrebuiltFuzzyQuery.class));
            } else {
                assertThat(plain, hasItem(sameInstance(leaf)));
            }
        }
        assertSame(compiled, RuntimeSearch.prebuildFuzzyAutomata(compiled, 0));

        MemoryIndex memoryIndex = new MemoryIndex();
        for (int r = 0; r < 50; r++) {
            List<String> row = randomList(1, 10, () -> typo(ASCII_ALPHABET, randomFrom("bron", "quick", "brown", "jumps", "abcd")));
            memoryIndex.reset();
            memoryIndex.addField(RuntimeSearch.CONTENT_FIELD, String.join(" ", row), Lucene.STANDARD_ANALYZER);
            assertSameHitsAndScores(memoryIndex, compiled, rewritten);
        }
    }

    /**
     * {@link FuzzyQuery#visit} and {@link FuzzyQuery#getAutomata} build the automaton again on every call; the prebuilt
     * query hands out the one it holds, which accepts the same terms.
     */
    public void testVisitAndGetAutomataReusePrebuiltAutomaton() {
        String text = randomWord(UNICODE_ALPHABET, between(3, 10));
        FuzzyQuery plain = new FuzzyQuery(new Term(FIELD, text), between(1, 2), between(0, 2), 50, randomBoolean());
        PrebuiltFuzzyQuery prebuilt = new PrebuiltFuzzyQuery(plain);
        CompiledAutomaton automaton = prebuilt.getAutomata();
        assertSame(automaton, prebuilt.getAutomata());

        List<ByteRunAutomaton> visited = new ArrayList<>();
        QueryVisitor visitor = new QueryVisitor() {
            @Override
            public void consumeTermsMatching(Query query, String field, Supplier<ByteRunAutomaton> supplier) {
                assertSame(prebuilt, query);
                assertEquals(FIELD, field);
                visited.add(supplier.get());
            }
        };
        prebuilt.visit(visitor);
        prebuilt.visit(visitor);
        assertEquals(2, visited.size());
        assertSame(automaton.runAutomaton, visited.get(0));
        assertSame(automaton.runAutomaton, visited.get(1));

        ByteRunAutomaton expected = plain.getAutomata().runAutomaton;
        for (int i = 0; i < 200; i++) {
            BytesRef candidate = new BytesRef(
                randomBoolean() ? typo(UNICODE_ALPHABET, text) : randomWord(UNICODE_ALPHABET, between(1, 12))
            );
            assertEquals(
                candidate.utf8ToString(),
                expected.run(candidate.bytes, candidate.offset, candidate.length),
                automaton.runAutomaton.run(candidate.bytes, candidate.offset, candidate.length)
            );
        }
    }

    /**
     * {@link PrebuiltFuzzyQuery#ramBytesUsed} counts the automata it holds, and implementing {@link Accountable} doesn't
     * change what {@link MaxClauseCountQueryVisitor} estimates, since it checks for {@link FuzzyQuery} first.
     */
    public void testRamBytesUsed() {
        String text = randomWord(ASCII_ALPHABET, between(3, 20));
        FuzzyQuery plain = new FuzzyQuery(new Term(FIELD, text), between(1, 2), between(0, 2), 50, randomBoolean());
        PrebuiltFuzzyQuery prebuilt = new PrebuiltFuzzyQuery(plain);
        long automata = 0;
        for (int k = 1; k <= plain.getMaxEdits(); k++) {
            automata += FuzzyQuery.getFuzzyAutomaton(text, k, plain.getPrefixLength(), plain.getTranspositions()).ramBytesUsed();
        }
        assertThat(prebuilt.ramBytesUsed(), greaterThan(automata + prebuilt.getTerm().ramBytesUsed()));
        assertThat(prebuilt.ramBytesUsed(), lessThan(automata + prebuilt.getTerm().ramBytesUsed() + 256));

        MaxClauseCountQueryVisitor visitor = new MaxClauseCountQueryVisitor(IndexSearcher.getMaxClauseCount(), null, null, 1);
        prebuilt.visit(visitor);
        assertEquals(FuzzyQueries.estimateBytes(prebuilt, 1), visitor.getEstimatedBytes());
        assertNotEquals(prebuilt.ramBytesUsed(), visitor.getEstimatedBytes());
    }

    private static NamedAnalyzer synonymAnalyzer() throws IOException {
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
        return new NamedAnalyzer("synonyms", AnalyzerScope.GLOBAL, analyzer);
    }

    private static Set<PrebuiltFuzzyQuery> distinctPrebuilt(Query query) {
        Set<PrebuiltFuzzyQuery> distinct = Collections.newSetFromMap(new IdentityHashMap<>());
        for (FuzzyQuery fuzzy : fuzzyLeaves(query)) {
            if (fuzzy instanceof PrebuiltFuzzyQuery prebuilt) {
                distinct.add(prebuilt);
            }
        }
        return distinct;
    }

    private static Query compile(String query, NamedAnalyzer analyzer, Object... options) {
        Map<String, Object> opts = new HashMap<>();
        for (int i = 0; i < options.length; i += 2) {
            opts.put((String) options[i], options[i + 1]);
        }
        return compile(query, analyzer, opts);
    }

    /**
     * Compiles a runtime {@code match} the way {@link RuntimeSearch} does, without prebuilding, and with the
     * {@code lenient: true} that {@link Match} always adds.
     */
    private static Query compile(String query, NamedAnalyzer analyzer, Map<String, Object> options) {
        Map<String, Object> opts = new HashMap<>(options);
        opts.put("lenient", true);
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
        assertSameHitsAndScores(memoryIndex, query, new PrebuiltFuzzyQuery(query));
    }

    private static void assertSameHitsAndScores(MemoryIndex memoryIndex, Query query, Query prebuilt) throws IOException {
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
        return QueryParsers.parseRewriteMethod(randomRewriteName(maxExpansions), null, LoggingDeprecationHandler.INSTANCE);
    }

    private static String randomRewriteName(int maxExpansions) {
        return randomFrom(
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
