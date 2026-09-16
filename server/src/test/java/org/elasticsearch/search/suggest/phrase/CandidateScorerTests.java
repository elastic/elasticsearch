/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.suggest.phrase;

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.codecs.TermStats;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MultiTerms;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.search.suggest.phrase.DirectCandidateGenerator.Candidate;
import org.elasticsearch.search.suggest.phrase.DirectCandidateGenerator.CandidateSet;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class CandidateScorerTests extends ESTestCase {

    /**
     * A scorer that assigns every candidate the same probability. These tests only care about how many scoring steps
     * {@link CandidateScorer} performs, not about the resulting ranking, so avoiding real term lookups keeps them fast
     * and deterministic. A tiny real index is still required because {@link WordScorer} needs a {@code Terms} instance.
     */
    private static WordScorer constantScorer(DirectoryReader reader) throws IOException {
        return new WordScorer(reader, MultiTerms.getTerms(reader, "body"), "body", 0.95d, new BytesRef(" ")) {
            @Override
            protected double scoreUnigram(Candidate word) {
                return 0.5d;
            }
        };
    }

    private static CandidateSet[] candidateSets(int positions, int alternativesPerPosition) {
        CandidateSet[] sets = new CandidateSet[positions];
        for (int i = 0; i < positions; i++) {
            Candidate original = new Candidate(new BytesRef("w" + i), new TermStats(1, 1), 1.0d, 1.0d, true);
            Candidate[] alternatives = new Candidate[alternativesPerPosition];
            for (int j = 0; j < alternativesPerPosition; j++) {
                alternatives[j] = new Candidate(new BytesRef("w" + i + "_" + j), new TermStats(1, 1), 0.5d, 0.5d, false);
            }
            sets[i] = new CandidateSet(alternatives, original);
        }
        return sets;
    }

    private static DirectoryReader openReader(Directory dir) throws IOException {
        try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(new WhitespaceAnalyzer()))) {
            Document doc = new Document();
            doc.add(new TextField("body", "some indexed words", Field.Store.NO));
            writer.addDocument(doc);
            writer.commit();
        }
        return DirectoryReader.open(dir);
    }

    public void testWithinBudgetReturnsCorrections() throws IOException {
        try (Directory dir = new ByteBuffersDirectory(); DirectoryReader reader = openReader(dir)) {
            CandidateScorer scorer = new CandidateScorer(constantScorer(reader), 5, 1);
            // 5 positions with 2 alternatives each and every position correctable: 3^5 = 243 combinations
            Correction[] corrections = scorer.findBestCandiates(candidateSets(5, 2), 5.0f, Double.MIN_VALUE);
            assertThat(corrections.length, lessThanOrEqualTo(5));
        }
    }

    public void testExceedingBudgetThrows() throws IOException {
        try (Directory dir = new ByteBuffersDirectory(); DirectoryReader reader = openReader(dir)) {
            CandidateScorer scorer = new CandidateScorer(constantScorer(reader), 5, 1, 100L);
            // 10 positions with 3 alternatives each and every position correctable: 4^10 combinations, far beyond the budget
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> scorer.findBestCandiates(candidateSets(10, 3), 10.0f, Double.MIN_VALUE)
            );
            assertThat(e.getMessage(), containsString("too complex"));
        }
    }

    /**
     * Alternatives at the final position are scored in a loop rather than through a further recursive call. They must
     * still count toward the budget, otherwise a single position with many candidates could perform unbounded work.
     */
    public void testAlternativesAtLastPositionCountTowardBudget() throws IOException {
        try (Directory dir = new ByteBuffersDirectory(); DirectoryReader reader = openReader(dir)) {
            CandidateScorer scorer = new CandidateScorer(constantScorer(reader), 5, 1, 100L);
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> scorer.findBestCandiates(candidateSets(1, 1000), 1.0f, Double.MIN_VALUE)
            );
            assertThat(e.getMessage(), containsString("too complex"));
        }
    }
}
