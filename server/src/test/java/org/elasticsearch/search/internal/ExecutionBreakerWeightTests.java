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
import org.apache.lucene.document.LatLonPoint;
import org.apache.lucene.geo.Polygon;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.RamUsageTester;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.DocIdSetBuilder;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.lucene.search.cost.FixedBitSetCost;
import org.elasticsearch.search.internal.BreakerWeightTestUtils.TrackingCircuitBreaker;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.search.internal.BreakerWeightTestUtils.chargeAgainst;
import static org.elasticsearch.search.internal.BreakerWeightTestUtils.newContextIndexSearcher;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;

/**
 * Checks that {@link ExecutionBreakerWeight} settles its reservation down to the doc id set the scorer keeps, and keeps the whole
 * reservation when it cannot tell what that is.
 */
public class ExecutionBreakerWeightTests extends ESTestCase {

    private static final String GEO_FIELD = "g";
    private static final int GRID_SIDE = 64;

    private Directory directory;
    private DirectoryReader reader;
    private LeafReaderContext leaf;
    private TrackingCircuitBreaker breaker;

    @Before
    public void initDirectoryAndReader() throws IOException {
        breaker = new TrackingCircuitBreaker(-1L);
        directory = newDirectory();
        try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig(null))) {
            for (int docId = 0; docId < GRID_SIDE * GRID_SIDE; docId++) {
                Document doc = new Document();
                doc.add(new LatLonPoint(GEO_FIELD, gridCoordinate(docId % GRID_SIDE), gridCoordinate(docId / GRID_SIDE)));
                writer.addDocument(doc);
            }
            writer.forceMerge(1);
        }
        reader = DirectoryReader.open(directory);
        leaf = reader.leaves().getFirst();
    }

    @After
    public void closeDirectoryAndReader() throws IOException {
        IOUtils.close(reader, directory);
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    /** The spatial reservation covers two bit sets, but the scorer keeps one set at most, so the settle refunds the rest. */
    public void testSpatialQuerySettlesToRetainedSet() throws IOException {
        Query query = polygonQuery();
        try (ContextIndexSearcher searcher = newContextIndexSearcher(reader)) {
            searcher.setCircuitBreaker(breaker);
            chargeAgainst(searcher.createWeight(query, ScoreMode.COMPLETE_NO_SCORES, 1f), leaf);
            long reserved = SpatialExecutionEstimator.peakBytes(leaf.reader().maxDoc());
            assertThat(breaker.charges(), equalTo(List.of(reserved)));
            assertThat(breaker.getUsed(), equalTo(retainedBytes(query)));
            assertThat("the settle must refund the second bit set", breaker.getUsed(), lessThan(reserved));
        }
    }

    public void testSettleInScopeReleasesFromScope() throws IOException {
        Query query = polygonQuery();
        try (ContextIndexSearcher searcher = newContextIndexSearcher(reader)) {
            searcher.setCircuitBreaker(breaker);
            Weight weight = searcher.createWeight(query, ScoreMode.COMPLETE_NO_SCORES, 1f);
            LeafExecutionScope scope = new LeafExecutionScope();
            scope.capture(() -> {
                chargeAgainst(weight, leaf);
                return null;
            });
            assertThat(breaker.getUsed(), equalTo(retainedBytes(query)));
            scope.release();
            assertThat("the scope must release what is left after the settle", breaker.getUsed(), equalTo(0L));
        }
    }

    /** For {@code TOP_SCORES} the scorer hides its doc id set behind a wrapper, so the reservation is kept. */
    public void testTopScoresKeepsReservation() throws IOException {
        try (ContextIndexSearcher searcher = newContextIndexSearcher(reader)) {
            searcher.setCircuitBreaker(breaker);
            chargeAgainst(searcher.createWeight(polygonQuery(), ScoreMode.TOP_SCORES, 1f), leaf);
            assertThat(breaker.getUsed(), equalTo(SpatialExecutionEstimator.peakBytes(leaf.reader().maxDoc())));
        }
    }

    /** A bulk scorer does not expose the scorer it iterates, so the reservation is kept. */
    public void testBulkScorerKeepsReservation() throws IOException {
        try (ContextIndexSearcher searcher = newContextIndexSearcher(reader)) {
            searcher.setCircuitBreaker(breaker);
            Weight weight = searcher.createWeight(polygonQuery(), ScoreMode.COMPLETE_NO_SCORES, 1f);
            weight.scorerSupplier(leaf).bulkScorer();
            assertThat(breaker.getUsed(), equalTo(SpatialExecutionEstimator.peakBytes(leaf.reader().maxDoc())));
        }
    }

    public void testDocIdRangeRetainsNothing() {
        assertThat(ExecutionBreakerWeight.retainedBytes(DocIdSetIterator.all(between(0, 1000)), 1000), equalTo(0L));
        assertThat(ExecutionBreakerWeight.retainedBytes(DocIdSetIterator.empty(), 1000), equalTo(0L));
    }

    public void testUnknownIteratorKeepsReservation() {
        DocIdSetIterator wrapped = new ConstantScoreScorer(1f, ScoreMode.TOP_SCORES, DocIdSetIterator.all(1000)).iterator();
        assertThat(ExecutionBreakerWeight.retainedBytes(wrapped, 1000), equalTo(Long.MAX_VALUE));
    }

    /** Whatever is grown and added, the set {@link DocIdSetBuilder} builds is no larger than what it is settled to. */
    public void testRetainedBytesIsCeilingOnDocIdSetBuilder() throws IOException {
        for (int iter = 0; iter < 100; iter++) {
            int maxDoc = between(1, 1 << 17);
            DocIdSetBuilder builder = new DocIdSetBuilder(maxDoc);
            int grows = between(0, 20);
            for (int g = 0; g < grows; g++) {
                int grown = between(1, randomBoolean() ? 512 : Math.max(1, maxDoc >>> 6));
                DocIdSetBuilder.BulkAdder adder = builder.grow(grown);
                for (int added = between(0, grown); added > 0; added--) {
                    adder.add(between(0, maxDoc - 1));
                }
            }
            assertRetainedBytesIsCeiling(builder.build().iterator(), maxDoc);
        }
    }

    /**
     * A builder that grows a whole leaf block but keeps only a few of its documents, as an intersection does on a block that crosses
     * the query, reuses the large buffer: its iterator keeps far more than {@code 4 * cost} bytes.
     */
    public void testSparseBuilderRetainsItsLargestBuffer() throws IOException {
        int maxDoc = 1 << 17;
        int blockSize = 512;
        DocIdSetBuilder builder = new DocIdSetBuilder(maxDoc);
        builder.grow(blockSize).add(randomIntBetween(0, maxDoc - 1));
        DocIdSetIterator iterator = builder.build().iterator();
        assertThat(iterator.getClass(), equalTo(ExecutionBreakerWeight.INT_ARRAY_ITERATOR_CLASS));
        assertThat(RamUsageTester.ramUsed(iterator), greaterThan((long) Integer.BYTES * blockSize));
        assertRetainedBytesIsCeiling(iterator, maxDoc);
    }

    public void testBitSetIteratorRetainsItsBitSet() {
        int maxDoc = between(1, 1 << 20);
        BitSetIterator iterator = new BitSetIterator(new FixedBitSet(maxDoc), 0);
        assertThat(
            ExecutionBreakerWeight.retainedBytes(iterator, maxDoc),
            equalTo(iterator.getBitSet().ramBytesUsed() + FixedBitSetCost.ITERATOR_BYTES)
        );
        assertRetainedBytesIsCeiling(iterator, maxDoc);
    }

    private static void assertRetainedBytesIsCeiling(DocIdSetIterator iterator, int maxDoc) {
        assertThat(
            "a builder's set must be sized, not kept whole",
            ExecutionBreakerWeight.retainedBytes(iterator, maxDoc),
            lessThan(Long.MAX_VALUE)
        );
        assertThat(
            "maxDoc=" + maxDoc + ", cost=" + iterator.cost(),
            ExecutionBreakerWeight.retainedBytes(iterator, maxDoc),
            greaterThanOrEqualTo(RamUsageTester.ramUsed(iterator))
        );
    }

    /** What the settle lowers the reservation for {@code query} to: the set a scorer built the same way keeps. */
    private long retainedBytes(Query query) throws IOException {
        IndexSearcher plain = new IndexSearcher(reader);
        plain.setQueryCache(null);
        Weight weight = plain.createWeight(query, ScoreMode.COMPLETE_NO_SCORES, 1f);
        DocIdSetIterator iterator = weight.scorerSupplier(leaf).get(Long.MAX_VALUE).iterator();
        long retained = ExecutionBreakerWeight.retainedBytes(iterator, leaf.reader().maxDoc());
        assertThat("the polygon must be settled to a set it can size, not kept whole", retained, lessThan(Long.MAX_VALUE));
        return retained;
    }

    /** A square around the grid center that matches some of the grid points but not all of them. */
    private static Query polygonQuery() {
        double halfSide = randomDoubleBetween(0.05, gridCoordinate(GRID_SIDE - 1) / 2, true);
        return LatLonPoint.newPolygonQuery(
            GEO_FIELD,
            new Polygon(
                new double[] { -halfSide, -halfSide, halfSide, halfSide, -halfSide },
                new double[] { -halfSide, halfSide, halfSide, -halfSide, -halfSide }
            )
        );
    }

    private static double gridCoordinate(int index) {
        return (index - GRID_SIDE / 2) * 0.1;
    }
}
