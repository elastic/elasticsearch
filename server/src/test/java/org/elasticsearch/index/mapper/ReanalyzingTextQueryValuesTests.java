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
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.en.PorterStemFilter;
import org.apache.lucene.analysis.miscellaneous.KeywordRepeatFilter;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * How a query over a field's values reads one of those values, and how often it counts a phrase it holds.
 */
public class ReanalyzingTextQueryValuesTests extends ESTestCase {

    /** The walk reads a value whichever way it arrives, so a phrase is found in bytes as it is in a string. */
    public void testAPhraseIsFoundInAValueHeldAsBytes() throws IOException {
        final Term[] phrase = { new Term("body", "quick"), new Term("body", "brown") };
        assertEquals(1, walk(phrase, List.of("the quick brown fox")));
        assertEquals("the same value, held as bytes", 1, walk(phrase, List.of(new BytesRef("the quick brown fox"))));
        assertEquals(0, walk(phrase, List.of(new BytesRef("brown quick"))));
    }

    /** A phrase the value holds twice is two, and one ending twice at a position is one, as an index counts them. */
    public void testHowOftenAPhraseIsCounted() throws IOException {
        final Term[] phrase = { new Term("body", "quick"), new Term("body", "brown") };
        assertEquals(2, walk(phrase, List.of("quick brown and quick brown")));
        assertEquals(1, walk(phrase, List.of("quick brown")));
    }

    /**
     * What the walk counts is what an index of the same values counts, whichever position an analyzer leaves a
     * repeated token at. A repeated token is not one phrase or many by itself: a phrase starts once on each copy of
     * its first term, and a copy of any later term is the same phrase reaching the position it already reached.
     */
    public void testTheWalkCountsWhatAnIndexCounts() throws IOException {
        final Term[] xCat = { new Term(FIELD, "x"), new Term(FIELD, "cat") };
        final Term[] catX = { new Term(FIELD, "cat"), new Term(FIELD, "x") };
        final Term[] catXCat = { new Term(FIELD, "cat"), new Term(FIELD, "x"), new Term(FIELD, "cat") };
        final Term[] aCatB = { new Term(FIELD, "a"), new Term(FIELD, "cat"), new Term(FIELD, "b") };

        // Every token repeated, so the first term of the phrase is repeated too.
        assertAgreesWithAnIndex(keepsEveryStem(), xCat, List.of("x cat"));
        assertAgreesWithAnIndex(keepsEveryStem(), xCat, List.of("x cat and x cat"));
        assertAgreesWithAnIndex(keepsEveryStem(), catXCat, List.of("cat x cat"));
        assertAgreesWithAnIndex(keepsEveryStem(), xCat, List.of("x cat", "x cat"));

        // Only one token repeated, so where it falls in the phrase is what decides.
        assertAgreesWithAnIndex(repeats("cat"), xCat, List.of("x cat"));
        assertAgreesWithAnIndex(repeats("cat"), catX, List.of("cat x"));
        assertAgreesWithAnIndex(repeats("cat"), aCatB, List.of("a cat b"));
        assertAgreesWithAnIndex(repeats("cat"), xCat, List.of("x cat", "x cat"));

        // Nothing repeated, and a phrase that does not reach across the gap between two values.
        assertAgreesWithAnIndex(repeats("nothing"), xCat, List.of("x cat"));
        assertAgreesWithAnIndex(repeats("cat"), xCat, List.of("x", "cat"));
    }

    /**
     * A phrase whose terms repeat can start again where the last one ended, and a term repeated three times at one
     * position starts three. Both are counted as an index counts them.
     */
    public void testPhrasesThatOverlapOrRepeatMoreThanTwice() throws IOException {
        final Term[] catCat = { new Term(FIELD, "cat"), new Term(FIELD, "cat") };
        final Term[] xCat = { new Term(FIELD, "x"), new Term(FIELD, "cat") };
        final Term[] catX = { new Term(FIELD, "cat"), new Term(FIELD, "x") };
        final Term[] aBC = { new Term(FIELD, "a"), new Term(FIELD, "b"), new Term(FIELD, "c") };

        // One phrase begins where the one before it ended.
        assertAgreesWithAnIndex(repeats("nothing"), catCat, List.of("cat cat cat"));
        assertAgreesWithAnIndex(repeats("cat"), catCat, List.of("cat cat"));
        assertAgreesWithAnIndex(keepsEveryStem(), catCat, List.of("cat cat cat"));

        // Three of one token at one position, as its first term and as its last.
        assertAgreesWithAnIndex(repeats("cat", 3), catX, List.of("cat x"));
        assertAgreesWithAnIndex(repeats("cat", 3), xCat, List.of("x cat"));

        // Three terms, with the repeat at the start, in the middle and at the end in turn.
        assertAgreesWithAnIndex(repeats("a", 2), aBC, List.of("a b c"));
        assertAgreesWithAnIndex(repeats("b", 2), aBC, List.of("a b c"));
        assertAgreesWithAnIndex(repeats("c", 2), aBC, List.of("a b c"));
        assertAgreesWithAnIndex(keepsEveryStem(), aBC, List.of("a b c and a b c"));

        // A phrase whose first term is also one of its later terms, repeated, so the count carried forward and the
        // count held back are the same token's.
        final Term[] catCatX = { new Term(FIELD, "cat"), new Term(FIELD, "cat"), new Term(FIELD, "x") };
        assertAgreesWithAnIndex(repeats("cat", 2), catCatX, List.of("cat cat x"));
        assertAgreesWithAnIndex(repeats("cat", 3), catCatX, List.of("cat cat x"));
        assertAgreesWithAnIndex(keepsEveryStem(), catCatX, List.of("cat cat x"));
        assertAgreesWithAnIndex(repeats("cat", 2), catCatX, List.of("cat cat cat x"));

        // A phrase the values do not hold, and one that would only hold across the gap between two of them.
        assertAgreesWithAnIndex(repeats("cat"), xCat, List.of("cat x"));
        assertAgreesWithAnIndex(keepsEveryStem(), aBC, List.of("a b", "c"));
    }

    /** A single term is counted as often as it occurs, repeats and all, as an index counts it. */
    public void testASingleTermIsCountedAsOftenAsItOccurs() throws IOException {
        assertAgreesWithAnIndex(repeats("cat"), new Term[] { new Term(FIELD, "cat") }, List.of("x cat"));
        assertAgreesWithAnIndex(keepsEveryStem(), new Term[] { new Term(FIELD, "cat") }, List.of("x cat"));
    }

    /** Asserts the walk over {@code values} counts the phrase as often as an index of those same values does. */
    private static void assertAgreesWithAnIndex(Analyzer analyzer, Term[] phrase, List<String> values) throws IOException {
        final int indexed = indexFreq(analyzer, phrase, values);
        final int walked = ReanalyzingTextQuery.walkPhraseFreq(phrase, FIELD, analyzer, List.copyOf(values));
        assertEquals("phrase " + Arrays.toString(phrase) + " over " + values, indexed, walked);
    }

    /**
     * How often an index of {@code values} holds the phrase, taken from a search of one: the similarity scores a
     * match by its frequency, so the score of the one document is the frequency itself.
     */
    private static int indexFreq(Analyzer analyzer, Term[] phrase, List<String> values) throws IOException {
        try (Directory dir = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(analyzer))) {
                final Document doc = new Document();
                for (String value : values) {
                    doc.add(new TextField(FIELD, value, Field.Store.NO));
                }
                writer.addDocument(doc);
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final IndexSearcher searcher = new IndexSearcher(reader);
                searcher.setSimilarity(FREQ_AS_SCORE);
                final PhraseQuery.Builder builder = new PhraseQuery.Builder();
                for (int i = 0; i < phrase.length; i++) {
                    builder.add(phrase[i], i);
                }
                final TopDocs hits = searcher.search(builder.build(), 1);
                return hits.scoreDocs.length == 0 ? 0 : Math.round(hits.scoreDocs[0].score);
            }
        }
    }

    /** Scores a match by its frequency, so a score read back is the frequency an index counted. */
    private static final Similarity FREQ_AS_SCORE = new Similarity() {
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

    /** Keeps every token beside its stem, which leaves two of them at every position whose stem is the token. */
    private static Analyzer keepsEveryStem() {
        return new GapAnalyzer() {
            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                final Tokenizer source = new StandardTokenizer();
                return new TokenStreamComponents(source, new PorterStemFilter(new KeywordRepeatFilter(source)));
            }
        };
    }

    /** Leaves two of one chosen token at its position, and every other token alone. */
    private static Analyzer repeats(String repeated) {
        return repeats(repeated, 2);
    }

    /** Leaves {@code copies} of one chosen token at its position, and every other token alone. */
    private static Analyzer repeats(String repeated, int copies) {
        return new GapAnalyzer() {
            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                final Tokenizer source = new StandardTokenizer();
                return new TokenStreamComponents(source, new RepeatOneTokenFilter(source, repeated, copies));
            }
        };
    }

    /** An analyzer whose values are joined by a gap, as a field holding several of them is indexed. */
    private abstract static class GapAnalyzer extends Analyzer {
        @Override
        public int getPositionIncrementGap(String fieldName) {
            return 100;
        }
    }

    /** Emits a second copy of one chosen token at the position the first was left at. */
    private static class RepeatOneTokenFilter extends TokenFilter {
        private final CharTermAttribute term = addAttribute(CharTermAttribute.class);
        private final PositionIncrementAttribute increment = addAttribute(PositionIncrementAttribute.class);
        private final String repeated;
        private State held;

        private final int copies;
        private int left;

        RepeatOneTokenFilter(TokenStream input, String repeated, int copies) {
            super(input);
            this.repeated = repeated;
            this.copies = copies;
        }

        @Override
        public boolean incrementToken() throws IOException {
            if (held != null && left > 0) {
                restoreState(held);
                left--;
                if (left == 0) {
                    held = null;
                }
                increment.setPositionIncrement(0);
                return true;
            }
            held = null;
            if (input.incrementToken() == false) {
                return false;
            }
            if (repeated.contentEquals(term)) {
                held = captureState();
                left = copies - 1;
            }
            return true;
        }

        @Override
        public void reset() throws IOException {
            super.reset();
            held = null;
            left = 0;
        }
    }

    private static final String FIELD = "body";

    private static int walk(Term[] phrase, List<Object> values) throws IOException {
        return ReanalyzingTextQuery.walkPhraseFreq(phrase, FIELD, new StandardAnalyzer(), values);
    }
}
