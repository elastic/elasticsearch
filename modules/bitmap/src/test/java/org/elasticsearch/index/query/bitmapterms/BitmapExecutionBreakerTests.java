/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query.bitmapterms;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.PointsFormat;
import org.apache.lucene.codecs.PointsReader;
import org.apache.lucene.codecs.PointsWriter;
import org.apache.lucene.codecs.lucene90.Lucene90PointsReader;
import org.apache.lucene.codecs.lucene90.Lucene90PointsWriter;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.TopScoreDocCollectorManager;
import org.apache.lucene.search.TotalHitCountCollectorManager;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.search.DummyTotalHitCountCollector;
import org.apache.lucene.tests.util.RamUsageTester;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.bkd.BKDConfig;
import org.apache.lucene.util.bkd.BKDWriter;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.TrackingCircuitBreaker;
import org.elasticsearch.common.lucene.search.function.FunctionScoreQuery;
import org.elasticsearch.common.lucene.search.function.WeightFactorFunction;
import org.elasticsearch.lucene.search.cost.DocIdSetBuilderCostEstimator;
import org.elasticsearch.lucene.search.cost.FixedBitSetCost;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;

/**
 * Searches with both bitmap queries through a {@link ContextIndexSearcher} with a request breaker, and checks that the
 * {@code DocIdSetBuilder} of the collecting path is charged under {@code bitmap_terms} as it grows and released, that the points
 * streaming path is charged for the buffers it reads leaves with, and that the terms streaming path charges nothing.
 */
public class BitmapExecutionBreakerTests extends ESTestCase {

    private static final String POINT_FIELD = "p";
    private static final String TERM_FIELD = "t";
    private static final int NUM_DOCS = 4096;
    private static final int SEGMENTS = 4;
    /**
     * Enough documents that the bound for growing the builder by one 512-point leaf block stays sparse: it does while four times the
     * total, plus slack, is within {@code maxDoc >>> 7}.
     */
    private static final int DISTINCT_DOCS = 300_000;

    private static final FieldType INDEX_TERMS_TYPE;

    static {
        INDEX_TERMS_TYPE = new FieldType();
        INDEX_TERMS_TYPE.setIndexOptions(IndexOptions.DOCS);
        INDEX_TERMS_TYPE.setOmitNorms(true);
        INDEX_TERMS_TYPE.setTokenized(false);
        INDEX_TERMS_TYPE.freeze();
    }

    public void testPointsCollectingPathChargesAndReleases() throws IOException {
        assertChargesThenReleases(null, pointsQuery());
    }

    public void testTermsCollectingPathChargesAndReleases() throws IOException {
        assertChargesThenReleases(null, termsQuery());
    }

    /** Sorted on the other field, the segment is not in the queried field's order and the query still collects. */
    public void testCollectingPathOnSegmentSortedByAnotherField() throws IOException {
        assertChargesThenReleases(TERM_FIELD, pointsQuery());
        assertChargesThenReleases(POINT_FIELD, termsQuery());
    }

    /** {@link BoostQuery} creates its child's weight itself and returns it as is, so the searcher still sees the charging weight. */
    public void testChargedUnderBoostQuery() throws IOException {
        assertChargesThenReleases(null, new BoostQuery(randomFrom(pointsQuery(), termsQuery()), 2f));
    }

    /**
     * Scored, {@link FunctionScoreQuery} creates its child's weight itself and returns one of its own, so the searcher never sees the
     * bitmap weight, which is charged all the same through the searcher it was created with.
     */
    public void testChargedUnderScoringFunctionScore() throws IOException {
        assertChargesThenReleases(
            null,
            new FunctionScoreQuery(randomFrom(pointsQuery(), termsQuery()), new WeightFactorFunction(2f)),
            true
        );
    }

    /**
     * Each leaf's scorer is charged for the point tree cursor's two leaf-sized arrays, and for the buffer the matching doc ids are read
     * into, grown to the one leaf block of 0s. The leaves release their charges one after another, so the peak is one leaf's.
     */
    public void testPointsStreamingPathChargesItsBuffers() throws IOException {
        int leafSize = BKDConfig.DEFAULT_MAX_POINTS_IN_LEAF_NODE;
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker(-1L);
        try (Directory directory = newDirectory(); DirectoryReader reader = index(directory, POINT_FIELD)) {
            try (ContextIndexSearcher searcher = newSearcher(reader, breaker)) {
                assertThat(searcher.search(pointsQuery(), DummyTotalHitCountCollector.createManager()), equalTo(NUM_DOCS / 2));
                assertThat("each leaf must release its charge once it is scored, not on close", breaker.getUsed(), equalTo(0L));
            }
        }
        long buffer = RamUsageEstimator.sizeOf(new int[ArrayUtil.oversize(leafSize, Integer.BYTES)]);
        assertThat(breaker.peak(), equalTo(cursorScratch(leafSize) + buffer));
    }

    /**
     * A leaf smaller than the default leaves the cursor's arrays charged at the default leaf size, which is what the codec sizes them
     * by. The buffer keeps its initial size, since the few matching documents fit in it.
     */
    public void testPointsStreamingPathOnASmallLeaf() throws IOException {
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker(-1L);
        try (
            Directory directory = newDirectory();
            DirectoryReader reader = indexSortedOnPoints(directory, 10, TestUtil.getDefaultCodec())
        ) {
            assertThat(count(reader, pointsQuery(), breaker), equalTo(5));
        }
        long buffer = RamUsageEstimator.sizeOf(new int[64]);
        assertThat(breaker.peak(), equalTo(cursorScratch(BKDConfig.DEFAULT_MAX_POINTS_IN_LEAF_NODE) + buffer));
    }

    /**
     * Leaves larger than the default raise the charge for the cursor's arrays to their size once one is visited, here the leaf of 0s,
     * and the buffer grows to hold it.
     */
    public void testPointsStreamingPathOnLargerLeaves() throws IOException {
        int leafSize = 4 * BKDConfig.DEFAULT_MAX_POINTS_IN_LEAF_NODE;
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker(-1L);
        try (Directory directory = newDirectory(); DirectoryReader reader = indexSortedOnPoints(directory, 2 * leafSize, codec(leafSize))) {
            assertThat(count(reader, pointsQuery(), breaker), equalTo(leafSize));
        }
        long buffer = RamUsageEstimator.sizeOf(new int[ArrayUtil.oversize(leafSize, Integer.BYTES)]);
        assertThat(breaker.peak(), equalTo(cursorScratch(leafSize) + buffer));
    }

    public void testTermsStreamingPathChargesNothing() throws IOException {
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker(-1L);
        try (Directory directory = newDirectory(); DirectoryReader reader = index(directory, TERM_FIELD)) {
            assertThat(count(reader, termsQuery(), breaker), equalTo(NUM_DOCS / 2));
        }
        assertFalse("the terms streaming path builds no doc id set", breaker.wasCalled());
    }

    /** Unsorted, both queries collect; sorted on the points field, the points query streams and the terms query still collects. */
    public void testExpensiveQueryTripsBreakerWithoutLeaking() throws IOException {
        for (String sortField : new String[] { null, POINT_FIELD }) {
            for (Query query : List.of(pointsQuery(), termsQuery())) {
                TrackingCircuitBreaker breaker = new TrackingCircuitBreaker(1L);
                try (Directory directory = newDirectory(); DirectoryReader reader = index(directory, sortField)) {
                    CircuitBreakingException e = expectThrows(CircuitBreakingException.class, () -> count(reader, query, breaker));
                    assertThat(e.getMessage(), containsString(BitmapTermsQueryBuilder.NAME));
                }
                assertThat("a tripped charge must not leak onto the breaker: " + query, breaker.getUsed(), equalTo(0L));
            }
        }
    }

    /**
     * One value, on a field where every document has its own, matches one document. Each clause must be charged for the sparse set
     * it builds rather than for a full bit set: the terms clause for exactly that document's posting, the points clause for the one
     * leaf block the value falls in. What the scorer keeps must still fit in the charge.
     */
    public void testSelectiveClauseIsChargedForASparseSet() throws IOException {
        int value = between(0, DISTINCT_DOCS - 1);
        try (Directory directory = newDirectory(); DirectoryReader reader = indexDistinctValues(directory)) {
            LeafReaderContext leaf = reader.leaves().getFirst();
            long fullBitSet = FixedBitSetCost.bytes(reader.maxDoc());
            Query terms = new BitmapTermsQuery(TERM_FIELD, IntBitmap.bitmapOf(value));
            Query points = new BitmapBKDQuery(POINT_FIELD, IntBitmap.bitmapOf(value));
            for (Query query : List.of(terms, points)) {
                TrackingCircuitBreaker breaker = new TrackingCircuitBreaker(-1L);
                try (ContextIndexSearcher searcher = newSearcher(reader, breaker)) {
                    Weight weight = searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1f);
                    DocIdSetIterator iterator = weight.scorerSupplier(leaf).get(Long.MAX_VALUE).iterator();
                    long charged = breaker.getUsed();
                    assertThat(query.toString(), charged, allOf(greaterThan(0L), lessThan(fullBitSet)));
                    if (query == terms) {
                        assertThat(
                            "the terms clause is charged for its posting alone",
                            charged,
                            equalTo(new DocIdSetBuilderCostEstimator(1, reader.maxDoc()).estimate())
                        );
                    }
                    assertThat("the set stays sparse: " + query, iterator, not(instanceOf(BitSetIterator.class)));
                    assertThat("the set fits in the charge: " + query, RamUsageTester.ramUsed(iterator), lessThanOrEqualTo(charged));
                }
                assertThat(count(reader, query, new TrackingCircuitBreaker(-1L)), equalTo(1));
            }
        }
    }

    /** No term in the dictionary is in the bitmap, so the terms clause builds nothing and is not charged. */
    public void testTermsClauseMatchingNoTermChargesNothing() throws IOException {
        Query query = new BitmapTermsQuery(TERM_FIELD, IntBitmap.bitmapOf(2, 3, 1 << 20));
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker(-1L);
        try (Directory directory = newDirectory(); DirectoryReader reader = index(directory, null)) {
            assertThat(count(reader, query, breaker), equalTo(0));
        }
        assertFalse(breaker.wasCalled());
    }

    /**
     * Every segment collects half its documents, so each leaf's builder turns dense on its first grow and the leaf is charged a full
     * bit set. The searcher scores the leaves one after another, so if each leaf releases its charge once it is scored, the peak is
     * the largest single leaf's charge rather than their sum, and nothing is left for {@link ContextIndexSearcher#close} to release.
     */
    private void assertChargesThenReleases(String sortField, Query query) throws IOException {
        assertChargesThenReleases(sortField, query, false);
    }

    private void assertChargesThenReleases(String sortField, Query query, boolean scored) throws IOException {
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker(-1L);
        try (Directory directory = newDirectory(); DirectoryReader reader = index(directory, sortField)) {
            assertThat("the peak tells leaves apart only if there are several", reader.leaves().size(), equalTo(SEGMENTS));
            long largestLeafCharge = 0;
            for (LeafReaderContext leaf : reader.leaves()) {
                int maxDoc = leaf.reader().maxDoc();
                largestLeafCharge = Math.max(largestLeafCharge, new DocIdSetBuilderCostEstimator(maxDoc, maxDoc).estimate());
            }
            try (ContextIndexSearcher searcher = newSearcher(reader, breaker)) {
                long hits = scored
                    ? searcher.search(query, new TopScoreDocCollectorManager(1, Integer.MAX_VALUE)).totalHits.value()
                    : searcher.search(query, DummyTotalHitCountCollector.createManager());
                assertThat(hits, equalTo((long) NUM_DOCS / 2));
                assertThat("each leaf must release its charge once it is scored, not on close", breaker.getUsed(), equalTo(0L));
            }
            assertThat("a leaf must release its charge before the next leaf charges", breaker.peak(), equalTo(largestLeafCharge));
        }
    }

    /** The point tree cursor's doc ids and decoding scratch, each an {@code int[leafSize]}. */
    private static long cursorScratch(int leafSize) {
        return 2 * RamUsageEstimator.sizeOf(new int[leafSize]);
    }

    /** Matches the documents with value 0, half of them, so the collecting path builds a bit set. */
    private static Query pointsQuery() {
        return new BitmapBKDQuery(POINT_FIELD, IntBitmap.bitmapOf(0));
    }

    private static Query termsQuery() {
        return new BitmapTermsQuery(TERM_FIELD, IntBitmap.bitmapOf(0));
    }

    /**
     * {@link #SEGMENTS} equal segments, unmerged, in which half the documents have the value 0 and half 1 in both fields, each
     * sorted on {@code sortField} if not null. Sorted segments are written with the default codec, so that a BKD leaf holds
     * {@link BKDConfig#DEFAULT_MAX_POINTS_IN_LEAF_NODE} points and the streaming path's charge is known.
     */
    private DirectoryReader index(Directory directory, String sortField) throws IOException {
        IndexWriterConfig config = newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE)
            .setMaxBufferedDocs(NUM_DOCS)
            .setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH);
        if (sortField != null) {
            config.setIndexSort(new Sort(new SortedNumericSortField(sortField, SortField.Type.INT)));
            config.setCodec(TestUtil.getDefaultCodec());
        }
        try (IndexWriter writer = new IndexWriter(directory, config)) {
            for (int docId = 0; docId < NUM_DOCS; docId++) {
                writer.addDocument(doc(docId % 2));
                if ((docId + 1) % (NUM_DOCS / SEGMENTS) == 0) {
                    writer.flush();
                }
            }
        }
        return DirectoryReader.open(directory);
    }

    /** One segment of {@code numDocs} documents, sorted on the points field, in which half have the value 0 and half 1. */
    private DirectoryReader indexSortedOnPoints(Directory directory, int numDocs, Codec codec) throws IOException {
        IndexWriterConfig config = newIndexWriterConfig().setCodec(codec)
            .setIndexSort(new Sort(new SortedNumericSortField(POINT_FIELD, SortField.Type.INT)));
        try (IndexWriter writer = new IndexWriter(directory, config)) {
            for (int docId = 0; docId < numDocs; docId++) {
                writer.addDocument(doc(docId % 2));
            }
            writer.forceMerge(1);
        }
        return DirectoryReader.open(directory);
    }

    /** The default codec, writing BKD leaves of {@code maxPointsInLeafNode} points. */
    private static Codec codec(int maxPointsInLeafNode) {
        Codec codec = TestUtil.getDefaultCodec();
        // Named after the codec it wraps so that the segment reads back with that one, whose points reader reads any leaf size.
        return new FilterCodec(codec.getName(), codec) {
            @Override
            public PointsFormat pointsFormat() {
                return new PointsFormat() {
                    @Override
                    public PointsWriter fieldsWriter(SegmentWriteState state) throws IOException {
                        return new Lucene90PointsWriter(state, maxPointsInLeafNode, BKDWriter.DEFAULT_MAX_MB_SORT_IN_HEAP);
                    }

                    @Override
                    public PointsReader fieldsReader(SegmentReadState state) throws IOException {
                        return new Lucene90PointsReader(state);
                    }
                };
            }
        };
    }

    /**
     * One segment of {@link #DISTINCT_DOCS} documents, each with its own value, its doc id, in both fields. The codec is pinned so that
     * BKD leaf blocks hold 512 points: random codecs use blocks of up to 2048, and one of those would already be charged as dense.
     */
    private DirectoryReader indexDistinctValues(Directory directory) throws IOException {
        try (IndexWriter writer = new IndexWriter(directory, newIndexWriterConfig().setCodec(TestUtil.getDefaultCodec()))) {
            for (int docId = 0; docId < DISTINCT_DOCS; docId++) {
                writer.addDocument(doc(docId));
            }
            writer.forceMerge(1);
        }
        return DirectoryReader.open(directory);
    }

    private static Document doc(int value) {
        Document doc = new Document();
        doc.add(new IntPoint(POINT_FIELD, value));
        doc.add(new SortedNumericDocValuesField(POINT_FIELD, value));
        byte[] term = new byte[Integer.BYTES];
        NumericUtils.intToSortableBytes(value, term, 0);
        doc.add(new Field(TERM_FIELD, new BytesRef(term), INDEX_TERMS_TYPE));
        doc.add(new SortedNumericDocValuesField(TERM_FIELD, value));
        return doc;
    }

    /**
     * Counts by collecting every hit. {@link TotalHitCountCollectorManager} would take {@link BitmapTermsQuery}'s
     * {@code Weight#count} shortcut, which answers from the terms dictionary without building a scorer.
     */
    private static int count(DirectoryReader reader, Query query, TrackingCircuitBreaker breaker) throws IOException {
        try (ContextIndexSearcher searcher = newSearcher(reader, breaker)) {
            return searcher.search(query, DummyTotalHitCountCollector.createManager());
        }
    }

    private static ContextIndexSearcher newSearcher(DirectoryReader reader, TrackingCircuitBreaker breaker) throws IOException {
        ContextIndexSearcher searcher = new ContextIndexSearcher(
            reader,
            IndexSearcher.getDefaultSimilarity(),
            null,
            IndexSearcher.getDefaultQueryCachingPolicy(),
            false
        );
        searcher.setCircuitBreaker(breaker);
        return searcher;
    }
}
