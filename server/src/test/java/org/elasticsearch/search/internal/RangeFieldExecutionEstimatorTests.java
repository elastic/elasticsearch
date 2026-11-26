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
import org.apache.lucene.document.LongRange;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.search.internal.BreakerWeightTestUtils.assertChargeIsCeilingOnRetainedSet;
import static org.elasticsearch.search.internal.BreakerWeightTestUtils.isDocIdRange;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Checks when {@link RangeFieldExecutionEstimator} charges nothing for the leaves Lucene matches in full without building a doc id
 * set. Each indexed range is {@code [docId - v, numDocs + docId]}, so the leaf's ranges all contain {@code [numDocs - 1, numDocs]}
 * and the intersects, within and contains queries of {@link #coveringQueries} hold for the leaf's whole value range.
 */
public class RangeFieldExecutionEstimatorTests extends ESTestCase {

    private static final String FIELD = "r";

    private Directory directory;
    private int numDocs;

    @Before
    public void initDirectory() {
        directory = newDirectory();
        numDocs = between(2, 5000);
    }

    @After
    public void closeDirectory() throws IOException {
        directory.close();
    }

    public void testCoveringRangeOnDenseSingleValuedLeafChargesNothing() throws IOException {
        indexRanges(1, false);
        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            for (Query query : coveringQueries()) {
                assertLeafCharge(reader, query, true);
            }
        }
    }

    public void testRangeNotCoveringDenseSingleValuedLeafIsCharged() throws IOException {
        indexRanges(1, false);
        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            Query query = LongRange.newIntersectsQuery(FIELD, new long[] { -numDocs }, new long[] { numDocs / 2 - 1 });
            assertLeafCharge(reader, query, false);
            assertChargeIsCeilingOnRetainedSet(searcherWithoutCache(reader), query);
        }
    }

    /** Lucene matches this leaf in full too, but a multivalued doc count estimate cannot tell it apart, so it is over-charged. */
    public void testCoveringRangeOnMultiValuedLeafIsCharged() throws IOException {
        indexRanges(between(2, 4), false);
        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            for (Query query : coveringQueries()) {
                assertLeafCharge(reader, query, false);
            }
        }
    }

    /** Lucene's match-all path needs every document to have a value, so a covering range on a sparse leaf builds a set. */
    public void testCoveringRangeOnSparseLeafIsCharged() throws IOException {
        indexRanges(1, true);
        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            for (Query query : coveringQueries()) {
                assertLeafCharge(reader, query, false);
                assertChargeIsCeilingOnRetainedSet(searcherWithoutCache(reader), query);
            }
        }
    }

    private List<Query> coveringQueries() {
        long[] wideMin = { -numDocs };
        long[] wideMax = { 3L * numDocs };
        return List.of(
            LongRange.newIntersectsQuery(FIELD, wideMin, wideMax),
            LongRange.newWithinQuery(FIELD, wideMin, wideMax),
            LongRange.newContainsQuery(FIELD, new long[] { numDocs - 1 }, new long[] { numDocs })
        );
    }

    /** Indexes {@code valuesPerDoc} ranges on every document, or on every other one if {@code sparse}, into a single segment. */
    private void indexRanges(int valuesPerDoc, boolean sparse) throws IOException {
        try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig(null))) {
            for (int docId = 0; docId < numDocs; docId++) {
                Document doc = new Document();
                if (sparse == false || docId % 2 == 0) {
                    for (int v = 0; v < valuesPerDoc; v++) {
                        doc.add(new LongRange(FIELD, new long[] { docId - v }, new long[] { numDocs + docId }));
                    }
                }
                writer.addDocument(doc);
            }
            writer.forceMerge(1);
        }
    }

    private static void assertLeafCharge(DirectoryReader reader, Query query, boolean expectNoCharge) throws IOException {
        IndexSearcher searcher = searcherWithoutCache(reader);
        Weight weight = searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1f);
        LeafReaderContext leaf = reader.leaves().getFirst();
        ScorerSupplier supplier = weight.scorerSupplier(leaf);
        long bytes = ExecutionCharge.forQuery(query).estimator().bytesForLeaf(leaf, supplier, Long.MAX_VALUE);
        if (expectNoCharge) {
            assertTrue(
                "Lucene must match every document without a set for " + query,
                isDocIdRange(supplier.get(Long.MAX_VALUE).iterator())
            );
            assertThat("must not be charged for " + query, bytes, equalTo(0L));
        } else {
            assertThat("must be charged for " + query, bytes, greaterThan(0L));
        }
    }

    private static IndexSearcher searcherWithoutCache(DirectoryReader reader) {
        IndexSearcher searcher = new IndexSearcher(reader);
        searcher.setQueryCache(null);
        return searcher;
    }
}
