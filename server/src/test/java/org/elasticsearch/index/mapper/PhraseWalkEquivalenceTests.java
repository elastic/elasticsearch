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
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.similarities.Similarity;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * The phrase walk has to report the frequency an index of the same values would, since that frequency is the score.
 * This holds it against that index over random values and phrases.
 */
public class PhraseWalkEquivalenceTests extends ESTestCase {

    private static final String FIELD = "body";
    private static final String[] WORDS = { "a", "b", "c", "the", "quick", "brown" };

    /** The similarity SourceConfirmedTextQuery scores with, so a search returns the frequency itself. */
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

    private float memoryIndexFreq(List<Object> values, PhraseQuery query, Analyzer analyzer) {
        final MemoryIndex index = new MemoryIndex(true, false);
        index.setSimilarity(FREQ);
        for (Object value : values) {
            index.addField(FIELD, (String) value, analyzer);
        }
        return index.search(query);
    }

    public void testWalkReportsTheSameFrequency() throws IOException {
        final Analyzer analyzer = new WhitespaceAnalyzer();
        int multiValued = 0;
        int nonZero = 0;
        for (int iter = 0; iter < 2000; iter++) {
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

            assertNotNull("exact consecutive phrase", SourceConfirmedTextQuery.walkablePhrase(query));
            final float expected = memoryIndexFreq(values, query, analyzer);
            final int actual = SourceConfirmedTextQuery.walkPhraseFreq(terms, FIELD, analyzer, values);
            if (expected > 0) {
                nonZero++;
            }
            assertEquals("values=" + values + " phrase=" + List.of(terms), (int) expected, actual);
        }
        logger.info("{} iterations, {} multi-valued, {} with a match", 2000, multiValued, nonZero);
    }
}
