/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query.bitmapterms;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.TotalHitCountCollectorManager;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Searches with both bitmap queries through a {@link ContextIndexSearcher} with a request breaker, and checks that the
 * {@code DocIdSetBuilder} of the collecting path is charged under {@code bitmap_terms} and released, and that the
 * streaming path charges nothing.
 */
public class BitmapExecutionBreakerTests extends ESTestCase {

    private static final String POINT_FIELD = "p";
    private static final String TERM_FIELD = "t";
    private static final int NUM_DOCS = 4096;

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

    public void testPointsStreamingPathChargesNothing() throws IOException {
        assertChargesNothing(POINT_FIELD, pointsQuery());
    }

    public void testTermsStreamingPathChargesNothing() throws IOException {
        assertChargesNothing(TERM_FIELD, termsQuery());
    }

    public void testExpensiveQueryTripsBreakerWithoutLeaking() throws IOException {
        for (Query query : List.of(pointsQuery(), termsQuery())) {
            TrackingCircuitBreaker breaker = new TrackingCircuitBreaker(1L);
            try (Directory directory = newDirectory(); DirectoryReader reader = index(directory, null)) {
                expectThrows(CircuitBreakingException.class, () -> count(reader, query, breaker));
            }
            assertThat(breaker.trippedLabels(), equalTo(List.of(BitmapTermsQueryBuilder.NAME)));
            assertThat("a tripped reservation must not leak onto the breaker", breaker.getUsed(), equalTo(0L));
        }
    }

    private void assertChargesThenReleases(String sortField, Query query) throws IOException {
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker(-1L);
        try (Directory directory = newDirectory(); DirectoryReader reader = index(directory, sortField)) {
            assertThat(count(reader, query, breaker), equalTo(NUM_DOCS / 2));
        }
        assertThat("the collecting path must charge the set it builds: " + query, breaker.peak(), greaterThan(0L));
        assertThat(breaker.labels(), everyItem(equalTo(BitmapTermsQueryBuilder.NAME)));
        assertThat("the per-leaf execution charge must be released once the leaf is scored", breaker.getUsed(), equalTo(0L));
    }

    private void assertChargesNothing(String sortField, Query query) throws IOException {
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker(-1L);
        try (Directory directory = newDirectory(); DirectoryReader reader = index(directory, sortField)) {
            assertThat(count(reader, query, breaker), equalTo(NUM_DOCS / 2));
        }
        assertThat("the streaming path builds no doc id set: " + query, breaker.labels(), empty());
    }

    /** Matches the documents with value 0, half of them, so the collecting path builds a bit set. */
    private static Query pointsQuery() {
        return new BitmapBKDQuery(POINT_FIELD, IntBitmap.bitmapOf(0));
    }

    private static Query termsQuery() {
        return new BitmapTermsQuery(TERM_FIELD, IntBitmap.bitmapOf(0));
    }

    /** One segment in which half the documents have the value 0 and half 1 in both fields, sorted on {@code sortField} if not null. */
    private DirectoryReader index(Directory directory, String sortField) throws IOException {
        IndexWriterConfig config = newIndexWriterConfig();
        if (sortField != null) {
            config.setIndexSort(new Sort(new SortedNumericSortField(sortField, SortField.Type.INT)));
        }
        try (IndexWriter writer = new IndexWriter(directory, config)) {
            for (int docId = 0; docId < NUM_DOCS; docId++) {
                int value = docId % 2;
                Document doc = new Document();
                doc.add(new IntPoint(POINT_FIELD, value));
                doc.add(new SortedNumericDocValuesField(POINT_FIELD, value));
                byte[] term = new byte[Integer.BYTES];
                NumericUtils.intToSortableBytes(value, term, 0);
                doc.add(new Field(TERM_FIELD, new BytesRef(term), INDEX_TERMS_TYPE));
                doc.add(new SortedNumericDocValuesField(TERM_FIELD, value));
                writer.addDocument(doc);
            }
            writer.forceMerge(1);
        }
        return DirectoryReader.open(directory);
    }

    /**
     * Counts by collecting every hit. {@link TotalHitCountCollectorManager} would take {@link BitmapTermsQuery}'s
     * {@code Weight#count} shortcut, which answers from the terms dictionary without building a scorer.
     */
    private static int count(DirectoryReader reader, Query query, TrackingCircuitBreaker breaker) throws IOException {
        try (ContextIndexSearcher searcher = newSearcher(reader, breaker)) {
            return searcher.search(query, new CollectorManager<CountingCollector, Integer>() {
                @Override
                public CountingCollector newCollector() {
                    return new CountingCollector();
                }

                @Override
                public Integer reduce(Collection<CountingCollector> collectors) {
                    return collectors.stream().mapToInt(c -> c.count).sum();
                }
            });
        }
    }

    private static final class CountingCollector extends SimpleCollector {
        private int count;

        @Override
        public void collect(int doc) {
            count++;
        }

        @Override
        public ScoreMode scoreMode() {
            return ScoreMode.COMPLETE_NO_SCORES;
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

    /** Request breaker that keeps track of what is in use, the peak, and the labels it was charged and tripped under. */
    private static final class TrackingCircuitBreaker extends NoopCircuitBreaker {
        private final long limit;
        private final AtomicLong used = new AtomicLong();
        private final AtomicLong peak = new AtomicLong();
        private final List<Long> charges = new CopyOnWriteArrayList<>();
        private final List<String> labels = new CopyOnWriteArrayList<>();
        private final List<String> trippedLabels = new CopyOnWriteArrayList<>();

        TrackingCircuitBreaker(long limit) {
            super("request");
            this.limit = limit;
        }

        @Override
        public void addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {
            long current = used.addAndGet(bytes);
            if (limit >= 0 && current > limit) {
                used.addAndGet(-bytes);
                trippedLabels.add(label);
                throw new CircuitBreakingException("test breaker tripped", bytes, limit, Durability.TRANSIENT);
            }
            peak.accumulateAndGet(current, Math::max);
            charges.add(bytes);
            labels.add(label);
        }

        @Override
        public void addWithoutBreaking(long bytes) {
            used.addAndGet(bytes);
        }

        @Override
        public long getUsed() {
            return used.get();
        }

        @Override
        public long getLimit() {
            return limit;
        }

        long peak() {
            return peak.get();
        }

        List<Long> charges() {
            return charges;
        }

        List<String> labels() {
            return labels;
        }

        List<String> trippedLabels() {
            return trippedLabels;
        }
    }
}
