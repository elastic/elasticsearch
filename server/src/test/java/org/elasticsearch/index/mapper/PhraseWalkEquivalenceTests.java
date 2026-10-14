/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SynonymQuery;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static org.hamcrest.Matchers.greaterThan;

/**
 * The phrase walk has to report the frequency an index of the same values would, since that frequency is the score.
 * This holds it against that index over random values and phrases.
 */
public class PhraseWalkEquivalenceTests extends ESTestCase {

    private static final String FIELD = "body";
    private static final String[] WORDS = { "a", "b", "c", "the", "quick", "brown", "syn" };

    /** Drops {@code the}, so a position increment of more than one reaches the walk. */
    private static Analyzer withStopWords() {
        return new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                final Tokenizer source = new WhitespaceTokenizer();
                return new TokenStreamComponents(source, new StopFilter(source, new CharArraySet(List.of("the"), false)));
            }
        };
    }

    /** Emits {@code syn} beside every {@code quick}, so two tokens reach the walk at one position. */
    private static Analyzer withSynonyms() {
        return new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                final Tokenizer source = new WhitespaceTokenizer();
                final TokenStream filtered = new TokenFilter(source) {
                    private final CharTermAttribute term = addAttribute(CharTermAttribute.class);
                    private final PositionIncrementAttribute increment = addAttribute(PositionIncrementAttribute.class);
                    private State pending;

                    @Override
                    public boolean incrementToken() throws IOException {
                        if (pending != null) {
                            restoreState(pending);
                            pending = null;
                            term.setEmpty().append("syn");
                            increment.setPositionIncrement(0);
                            return true;
                        }
                        if (input.incrementToken() == false) {
                            return false;
                        }
                        if (term.toString().equals("quick")) {
                            pending = captureState();
                        }
                        return true;
                    }

                    @Override
                    public void reset() throws IOException {
                        super.reset();
                        pending = null;
                    }
                };
                return new TokenStreamComponents(source, filtered);
            }
        };
    }

    /** The similarity ReanalyzingTextQuery scores with, so a search returns the frequency itself. */
    private static final Similarity FREQ = new Similarity() {
        @Override
        public long computeNorm(FieldInvertState state) {
            return 1L;
        }

        @Override
        public SimScorer scorer(float boost, CollectionStatistics collectionStats, TermStatistics... termStats) {
            return new SimScorer() {
                @Override
                public float score(float freq, long norm) {
                    return freq;
                }
            };
        }
    };

    /**
     * A boolean query of clauses that name terms is answered by a walk counting the clauses answered, which is the
     * score a small index of the same values reports where a clause a document answers counts one.
     */
    public void testClausesReportTheSameScore() throws IOException {
        final Analyzer analyzer = new NamedAnalyzer("test", AnalyzerScope.INDEX, new WhitespaceAnalyzer(), 0);
        int answered = 0;
        for (int iter = 0; iter < 2000; iter++) {
            final List<Object> values = new ArrayList<>();
            for (int value = 0; value < randomIntBetween(1, 3); value++) {
                final StringBuilder text = new StringBuilder();
                for (int word = 0; word < randomIntBetween(1, 12); word++) {
                    text.append(randomFrom(WORDS)).append(' ');
                }
                values.add(text.toString().trim());
            }

            final BooleanQuery.Builder builder = new BooleanQuery.Builder();
            final Set<String> named = new HashSet<>();
            int added = 0;
            int optional = 0;
            for (int clause = randomIntBetween(1, 4); clause > 0; clause--) {
                final List<String> words = new ArrayList<>();
                for (int term = randomIntBetween(1, 3); term > 0; term--) {
                    words.add(randomFrom(WORDS));
                }
                // Clauses naming the same terms are answered once, which the walk leaves to an index; those are
                // the subject of testClausesNamingTheSameTermsAreIndexed rather than of this comparison.
                if (named.add(new TreeSet<>(words).toString()) == false) {
                    continue;
                }
                final Query of = clauseOf(words);
                final boolean must = randomBoolean();
                builder.add(of, must ? Occur.MUST : Occur.SHOULD);
                added++;
                optional += must ? 0 : 1;
            }
            if (added == 0) {
                continue;
            }
            if (optional > 0) {
                builder.setMinimumNumberShouldMatch(randomIntBetween(0, optional));
            }
            final BooleanQuery query = builder.build();

            final ReanalyzingTextQuery.WalkableClauses clauses = ReanalyzingTextQuery.walkableClauses(query, FIELD);
            assertNotNull("clauses naming terms are walkable: " + query, clauses);
            final float expected = clauseCountFreq(values, query, analyzer);
            if (expected > 0) {
                answered++;
            }
            assertEquals(
                "values=" + values + " query=" + query,
                (int) expected,
                ReanalyzingTextQuery.walkClauseFreq(clauses, FIELD, analyzer, values)
            );
        }
        assertThat("and some of them answer, so the comparison means something", answered, greaterThan(0));
    }

    /** A query asking for the same terms twice is answered once when it is rewritten, so a walk leaves it alone. */
    public void testClausesNamingTheSameTermsAreIndexed() {
        final Term term = new Term(FIELD, "quick");
        for (Occur occur : new Occur[] { Occur.MUST, Occur.SHOULD }) {
            final BooleanQuery twice = new BooleanQuery.Builder().add(new TermQuery(term), Occur.SHOULD)
                .add(new TermQuery(term), occur)
                .build();
            assertNull(twice.toString(), ReanalyzingTextQuery.walkableClauses(twice, FIELD));
        }
        // However each of them asks: a term, a set holding it, and a synonym of it alone all name the same term.
        final BooleanQuery eachWay = new BooleanQuery.Builder().add(new TermQuery(term), Occur.SHOULD)
            .add(new TermInSetQuery(FIELD, Set.of(term.bytes())), Occur.SHOULD)
            .build();
        assertNull(eachWay.toString(), ReanalyzingTextQuery.walkableClauses(eachWay, FIELD));
    }

    /** One clause naming {@code words}: a term where it is one, and otherwise a set or a synonym of them. */
    private Query clauseOf(List<String> words) {
        if (words.size() == 1 && randomBoolean()) {
            return new TermQuery(new Term(FIELD, words.get(0)));
        }
        if (randomBoolean()) {
            final SynonymQuery.Builder synonyms = new SynonymQuery.Builder(FIELD);
            for (String word : new TreeSet<>(words)) {
                synonyms.addTerm(new Term(FIELD, word));
            }
            return synonyms.build();
        }
        final Set<BytesRef> terms = new LinkedHashSet<>();
        for (String word : words) {
            terms.add(new BytesRef(word));
        }
        return new TermInSetQuery(FIELD, terms);
    }

    /** What a small index of the values scores where a clause the document answers counts one. */
    private float clauseCountFreq(List<Object> values, Query query, Analyzer analyzer) {
        final MemoryIndex index = new MemoryIndex(true, false);
        index.setSimilarity(MATCHED_CLAUSES);
        for (Object value : values) {
            index.addField(FIELD, (String) value, analyzer);
        }
        return index.search(query);
    }

    /** As the query of a field that indexes no terms counts them: one point for a clause the document answers. */
    private static final Similarity MATCHED_CLAUSES = new Similarity() {
        @Override
        public long computeNorm(FieldInvertState state) {
            return 1L;
        }

        @Override
        public SimScorer scorer(float boost, CollectionStatistics collectionStats, TermStatistics... termStats) {
            return new SimScorer() {
                @Override
                public float score(float freq, long norm) {
                    return freq > 0 ? 1f : 0f;
                }
            };
        }
    };

    private float memoryIndexFreq(List<Object> values, Query query, Analyzer analyzer) {
        final MemoryIndex index = new MemoryIndex(true, false);
        index.setSimilarity(FREQ);
        for (Object value : values) {
            index.addField(FIELD, (String) value, analyzer);
        }
        return index.search(query);
    }

    public void testWalkReportsTheSameFrequency() throws IOException {
        check(new WhitespaceAnalyzer(), "whitespace");
    }

    /**
     * A term is walked as the phrase of one it is, and reports what an index of the same values reports for it. Its
     * frequency is the score where the field indexes terms, and the clause it answers where the field indexes none.
     */
    public void testATermReportsTheSameFrequency() throws IOException {
        final Analyzer analyzer = new NamedAnalyzer("test", AnalyzerScope.INDEX, new WhitespaceAnalyzer(), 0);
        for (int iter = 0; iter < 1000; iter++) {
            final List<Object> values = new ArrayList<>();
            for (int value = 0; value < randomIntBetween(1, 3); value++) {
                final StringBuilder text = new StringBuilder();
                for (int word = 0; word < randomIntBetween(1, 12); word++) {
                    text.append(randomFrom(WORDS)).append(' ');
                }
                values.add(text.toString().trim());
            }
            final Term term = new Term(FIELD, randomFrom(WORDS));
            final Term[] walkable = ReanalyzingTextQuery.walkablePhrase(new TermQuery(term));
            assertNotNull("a term is walkable", walkable);
            assertArrayEquals(new Term[] { term }, walkable);
            assertEquals(
                "values=" + values + " term=" + term,
                (int) memoryIndexFreq(values, new TermQuery(term), analyzer),
                ReanalyzingTextQuery.walkPhraseFreq(walkable, FIELD, analyzer, values)
            );
        }
    }

    public void testWalkReportsTheSameFrequencyWithPositionGaps() throws IOException {
        check(withStopWords(), "stop words");
    }

    public void testWalkReportsTheSameFrequencyWithTokensSharingAPosition() throws IOException {
        check(withSynonyms(), "synonyms");
    }

    /**
     * Over both gaps an index can put between two values: none, and the default a text field's analyzer carries.
     */
    private void check(Analyzer analyzer, String what) throws IOException {
        for (int gap : new int[] { 0, TextFieldMapper.Defaults.POSITION_INCREMENT_GAP }) {
            check(new NamedAnalyzer("test", AnalyzerScope.INDEX, analyzer, gap), what + ", gap " + gap, 1000);
        }
    }

    private void check(Analyzer analyzer, String what, int iterations) throws IOException {
        int multiValued = 0;
        int nonZero = 0;
        for (int iter = 0; iter < iterations; iter++) {
            final int valueCount = randomIntBetween(1, 3);
            final List<Object> values = new ArrayList<>();
            for (int v = 0; v < valueCount; v++) {
                final StringBuilder text = new StringBuilder();
                for (int w = 0; w < randomIntBetween(1, 12); w++) {
                    text.append(randomFrom(WORDS)).append(' ');
                }
                values.add(text.toString().trim());
            }
            if (valueCount > 1) {
                multiValued++;
            }

            final PhraseQuery.Builder builder = new PhraseQuery.Builder();
            final int phraseLength = randomIntBetween(1, 3);
            final Term[] terms = new Term[phraseLength];
            for (int t = 0; t < phraseLength; t++) {
                terms[t] = new Term(FIELD, randomFrom(WORDS));
                builder.add(terms[t], t);
            }
            final PhraseQuery query = builder.build();

            assertNotNull("exact consecutive phrase", ReanalyzingTextQuery.walkablePhrase(query));
            final float expected = memoryIndexFreq(values, query, analyzer);
            final int actual = ReanalyzingTextQuery.walkPhraseFreq(terms, FIELD, analyzer, values);
            if (expected > 0) {
                nonZero++;
            }
            assertEquals("values=" + values + " phrase=" + List.of(terms), (int) expected, actual);
        }
        logger.info("{}: {} iterations, {} multi-valued, {} with a match", what, iterations, multiValued, nonZero);
    }
}
