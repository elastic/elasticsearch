/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.internal;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.LongRange;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.search.internal.BreakerWeightTestUtils.TrackingCircuitBreaker;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.stream.LongStream;

import static org.elasticsearch.search.internal.BreakerWeightTestUtils.conjunction;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Searches with the queries sized by {@link PointsExecutionEstimator} ({@link org.apache.lucene.search.PointInSetQuery} and
 * {@link org.apache.lucene.document.RangeFieldQuery}) through a {@link ContextIndexSearcher} with a request breaker, and checks
 * what is charged and that it is released.
 */
public class PointsExecutionBreakerTests extends ESTestCase {

    private static final String POINT_FIELD = "p";
    private static final String RANGE_FIELD = "r";
    private static final String KEYWORD_FIELD = "k";
    private static final String RARE_TERM = "rare";
    private static final int RARE_DOCS = 5;
    private static final int NUM_DOCS = 2000;

    private Directory directory;
    private DirectoryReader reader;

    @Before
    public void initDirectoryAndReader() throws Exception {
        directory = newDirectory();
        try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig(null))) {
            for (int docId = 0; docId < NUM_DOCS; docId++) {
                Document doc = new Document();
                doc.add(new LongPoint(POINT_FIELD, docId));
                doc.add(new SortedNumericDocValuesField(POINT_FIELD, docId));
                doc.add(new LongRange(RANGE_FIELD, new long[] { docId }, new long[] { docId + 10 }));
                // A rare term gives a cheap lead clause that drives IndexOrDocValuesQuery onto its doc-values side.
                if (docId < RARE_DOCS) {
                    doc.add(new StringField(KEYWORD_FIELD, RARE_TERM, Field.Store.NO));
                }
                writer.addDocument(doc);
            }
            writer.forceMerge(1);
        }
        reader = DirectoryReader.open(directory);
    }

    @After
    public void closeDirectoryAndReader() throws Exception {
        IOUtils.close(reader, directory);
    }

    public void testSetQueryChargesAndReleases() throws IOException {
        assertChargesThenReleases(denseSetQuery());
    }

    public void testRangeFieldQueryChargesAndReleases() throws IOException {
        assertChargesThenReleases(LongRange.newIntersectsQuery(RANGE_FIELD, new long[] { 100 }, new long[] { 1500 }));
    }

    public void testIndexOrDocValuesSetQueryChargesWhenPointsSideSelected() throws IOException {
        assertChargesThenReleases(new IndexOrDocValuesQuery(denseSetQuery(), denseDocValuesSetQuery()));
    }

    public void testIndexOrDocValuesSetQuerySkipsChargeWhenDocValuesSideSelected() throws IOException {
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker(-1L);
        Query rareLead = new TermQuery(new Term(KEYWORD_FIELD, RARE_TERM));
        int hits = runSearch(conjunction(rareLead, new IndexOrDocValuesQuery(denseSetQuery(), denseDocValuesSetQuery())), breaker);
        assertThat("the selective lead clause must still match documents so the scorer runs", hits, greaterThan(0));
        assertThat("the doc-values side allocates no doc id set, so nothing is charged", breaker.peak(), equalTo(0L));
    }

    public void testUnmatchedSetQueryChargesNothing() throws IOException {
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker(-1L);
        assertThat(runSearch(LongPoint.newSetQuery(POINT_FIELD, -1, NUM_DOCS + 1), breaker), equalTo(0));
        assertThat("a set that matches no point grows no doc id set", breaker.peak(), equalTo(0L));
    }

    public void testExpensiveSetQueryTripsBreakerWithoutLeaking() throws IOException {
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker(1L);
        expectThrows(CircuitBreakingException.class, () -> runSearch(denseSetQuery(), breaker));
        assertThat("a tripped reservation must not leak onto the breaker", breaker.getUsed(), equalTo(0L));
    }

    private void assertChargesThenReleases(Query query) throws IOException {
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker(-1L);
        int hits = runSearch(query, breaker);
        assertThat("the query must match documents so its scorer actually runs", hits, greaterThan(0));
        assertThat("the points scorer must charge execution RAM while it runs", breaker.peak(), greaterThan(0L));
        assertThat("the per-leaf execution charge must be released once the leaf is scored", breaker.getUsed(), equalTo(0L));
    }

    private int runSearch(Query query, TrackingCircuitBreaker breaker) throws IOException {
        return BreakerWeightTestUtils.runSearch(reader, query, breaker);
    }

    private static Query denseSetQuery() {
        return LongPoint.newSetQuery(POINT_FIELD, denseSet());
    }

    private static Query denseDocValuesSetQuery() {
        return SortedNumericDocValuesField.newSlowSetQuery(POINT_FIELD, denseSet());
    }

    private static long[] denseSet() {
        return LongStream.range(0, NUM_DOCS).filter(v -> v % 2 == 0).toArray();
    }
}
