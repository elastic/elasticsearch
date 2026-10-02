/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

/**
 * Unit tests for {@link PostFilterKnnQuery#applyFilter}, which tests each kNN candidate against the
 * user filter by random access (via {@code Lucene.asSequentialAccessBits}). The doc-values range case
 * is the important one: consuming the two-phase scorer through {@code iterator().advance()} would scan
 * from an out-of-range candidate to the end of the segment looking for the next match, so these tests
 * include candidates past the range upper bound and past the last matching doc.
 */
public class PostFilterKnnQueryApplyFilterTests extends ESTestCase {

    private static final String VECTOR_FIELD = "vector";
    private static final String NUM_FIELD = "num";
    private static final String TEXT_FIELD = "text";
    private static final String TAG_FIELD = "tag";
    private static final int NUM_DOCS = 100;
    private static final long RANGE_UPPER = 54;

    /**
     * Two-phase path: an {@code IndexOrDocValuesQuery} range picks the doc-values scorer for the small
     * candidate leadCost. Candidates past {@code upper} (55, 70, and the last doc 99) must be rejected
     * without affecting the partition of the in-range ones.
     */
    public void testDocValuesRangeFilter() throws IOException {
        Query range = new IndexOrDocValuesQuery(
            LongPoint.newRangeQuery(NUM_FIELD, 0, RANGE_UPPER),
            NumericDocValuesField.newSlowRangeQuery(NUM_FIELD, 0, RANGE_UPPER)
        );
        assertPartition(range, new int[] { 99, 10, 55, 54, 70 }, new int[] { 10, 54 }, new int[] { 55, 70, 99 });
    }

    /**
     * Two-phase path with positional rejection: decoy docs contain both phrase terms reversed, so the
     * approximation (terms conjunction) contains them but {@code matches()} must reject them.
     */
    public void testPhraseFilterRejectsReversedDecoys() throws IOException {
        // doc % 10 < 5 -> "alpha bravo" (matches); doc % 10 in [5,7] -> "bravo alpha" (decoy); else unrelated
        Query phrase = new PhraseQuery(TEXT_FIELD, "alpha", "bravo");
        // candidates: 3,13 match; 5,16,97 are decoys (terms present, wrong order); 8,99 unrelated
        assertPartition(phrase, new int[] { 97, 3, 8, 5, 13, 99, 16 }, new int[] { 3, 13 }, new int[] { 5, 8, 16, 97, 99 });
    }

    /** No-two-phase path: a postings-backed TermQuery goes through the plain iterator. */
    public void testTermFilter() throws IOException {
        // doc % 2 == 0 -> tag=even
        Query term = new TermQuery(new Term(TAG_FIELD, "even"));
        assertPartition(term, new int[] { 7, 0, 42, 99, 96 }, new int[] { 0, 42, 96 }, new int[] { 7, 99 });
    }

    /** A filter matching nothing yields a null scorer supplier; every candidate is filtered out. */
    public void testMatchNothingFilter() throws IOException {
        Query none = new TermQuery(new Term(TAG_FIELD, "absent"));
        assertPartition(none, new int[] { 0, 50, 99 }, new int[] {}, new int[] { 0, 50, 99 });
    }

    private void assertPartition(Query filter, int[] candidateDocs, int[] expectedMatching, int[] expectedFilteredOut) throws IOException {
        try (Directory dir = newDirectory()) {
            writeSingleSegment(dir);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                assertEquals("test relies on a single segment", 1, reader.leaves().size());
                IndexSearcher searcher = new IndexSearcher(reader);
                Weight filterWeight = KnnQueryUtils.createFilterWeight(searcher, filter, VECTOR_FIELD).weight();
                assertNotNull(filterWeight);

                ScoreDoc[] cands = new ScoreDoc[candidateDocs.length];
                for (int i = 0; i < candidateDocs.length; i++) {
                    cands[i] = new ScoreDoc(candidateDocs[i], /* score irrelevant to partitioning */ 1f / (i + 1));
                }
                ScoreDoc[][] perLeaf = new ScoreDoc[][] { cands };

                PostFilterKnnQuery.FilteredCandidates result = PostFilterKnnQuery.applyFilter(perLeaf, filterWeight, reader.leaves());

                assertArrayEquals(expectedMatching, docIds(result.matchingPerLeaf()[0]));
                int[] filteredOut = result.filteredOutPerLeaf()[0];
                assertArrayEquals(expectedFilteredOut, filteredOut == null ? new int[0] : filteredOut);
            }
        }
    }

    private static int[] docIds(ScoreDoc[] docs) {
        if (docs == null) {
            return new int[0];
        }
        int[] ids = new int[docs.length];
        for (int i = 0; i < docs.length; i++) {
            ids[i] = docs[i].doc;
        }
        return ids;
    }

    private static void writeSingleSegment(Directory dir) throws IOException {
        try (IndexWriter iw = new IndexWriter(dir, new IndexWriterConfig())) {
            for (int i = 0; i < NUM_DOCS; i++) {
                Document doc = new Document();
                doc.add(new KnnFloatVectorField(VECTOR_FIELD, new float[] { i }));
                doc.add(new LongPoint(NUM_FIELD, i));
                doc.add(new NumericDocValuesField(NUM_FIELD, i));
                final String text;
                if (i % 10 < 5) {
                    text = "alpha bravo";
                } else if (i % 10 < 8) {
                    text = "bravo alpha";
                } else {
                    text = "charlie delta";
                }
                doc.add(new TextField(TEXT_FIELD, text, Field.Store.NO));
                doc.add(new StringField(TAG_FIELD, i % 2 == 0 ? "even" : "odd", Field.Store.NO));
                iw.addDocument(doc);
            }
            iw.commit();
        }
    }
}
