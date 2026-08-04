/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.aggregations.bucket;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.search.aggregations.AggregationExecutionContext;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.BucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

public class BestBucketsDeferringCollectorTests extends AggregatorTestCase {

    public void testReplay() throws Exception {
        Directory directory = newDirectory();
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
        int numDocs = randomIntBetween(1, 128);
        int maxNumValues = randomInt(16);
        for (int i = 0; i < numDocs; i++) {
            Document document = new Document();
            document.add(new StringField("field", String.valueOf(randomInt(maxNumValues)), Field.Store.NO));
            indexWriter.addDocument(document);
        }

        indexWriter.close();
        IndexReader indexReader = DirectoryReader.open(directory);
        IndexSearcher indexSearcher = newSearcher(indexReader);

        TermQuery termQuery = new TermQuery(new Term("field", String.valueOf(randomInt(maxNumValues))));
        Query rewrittenQuery = indexSearcher.rewrite(termQuery);
        TopDocs topDocs = indexSearcher.search(termQuery, numDocs);

        BestBucketsDeferringCollector collector = new BestBucketsDeferringCollector(rewrittenQuery, indexSearcher, false, bytes -> {}) {
            @Override
            public ScoreMode scoreMode() {
                return ScoreMode.COMPLETE;
            }
        };
        Set<Integer> deferredCollectedDocIds = new HashSet<>();
        collector.setDeferredCollector(Collections.singleton(bla(deferredCollectedDocIds)));
        collector.preCollection();
        indexSearcher.search(termQuery, collector.asCollector());
        collector.postCollection();
        collector.prepareSelectedBuckets(BigArrays.NON_RECYCLING_INSTANCE.newLongArray(1, true));

        assertEquals(topDocs.scoreDocs.length, deferredCollectedDocIds.size());
        for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
            assertTrue("expected docid [" + scoreDoc.doc + "] is missing", deferredCollectedDocIds.contains(scoreDoc.doc));
        }

        topDocs = indexSearcher.search(Queries.ALL_DOCS_INSTANCE, numDocs);
        collector = new BestBucketsDeferringCollector(rewrittenQuery, indexSearcher, true, bytes -> {});
        deferredCollectedDocIds = new HashSet<>();
        collector.setDeferredCollector(Collections.singleton(bla(deferredCollectedDocIds)));
        collector.preCollection();
        indexSearcher.search(Queries.ALL_DOCS_INSTANCE, collector.asCollector());
        collector.postCollection();
        collector.prepareSelectedBuckets(BigArrays.NON_RECYCLING_INSTANCE.newLongArray(1, true));

        assertEquals(topDocs.scoreDocs.length, deferredCollectedDocIds.size());
        for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
            assertTrue("expected docid [" + scoreDoc.doc + "] is missing", deferredCollectedDocIds.contains(scoreDoc.doc));
        }
        indexReader.close();
        directory.close();
    }

    /**
     * Verifies the exact sequence of circuit-breaker events across the full lifecycle.
     * <p>
     * With two segments (5 docs each) the expected event list is:
     * <pre>
     *   +chargeA  — finishLeaf for segment 1 (triggered when segment 2 starts)
     *   +chargeB  — finishLeaf for segment 2 (postCollection)
     *   -chargeA  — prepareSelectedBuckets frees segment 1 entry
     *   -chargeB  — prepareSelectedBuckets frees segment 2 entry
     * </pre>
     * Each return must equal the exact charge for that entry, and the total must be zero.
     */
    public void testCircuitBreakerChargesOneEventPerSegmentAndReleasesSymmetrically() throws IOException {
        try (Directory directory = newDirectory()) {
            try (IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig())) {
                for (int i = 0; i < 5; i++) {
                    indexWriter.addDocument(new Document());
                }
                indexWriter.commit();
                for (int i = 0; i < 5; i++) {
                    indexWriter.addDocument(new Document());
                }
            }

            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                IndexSearcher indexSearcher = newSearcher(indexReader);
                Query query = Queries.ALL_DOCS_INSTANCE;

                List<Long> events = new ArrayList<>();
                BestBucketsDeferringCollector deferringCollector = new BestBucketsDeferringCollector(
                    query,
                    indexSearcher,
                    false,
                    events::add
                );
                deferringCollector.setDeferredCollector(Collections.singleton(BucketCollector.NO_OP_BUCKET_COLLECTOR));
                deferringCollector.preCollection();
                indexSearcher.search(query, new Collector() {
                    @Override
                    public ScoreMode scoreMode() {
                        return ScoreMode.COMPLETE_NO_SCORES;
                    }

                    @Override
                    public LeafBucketCollector getLeafCollector(LeafReaderContext context) throws IOException {
                        LeafBucketCollector delegate = deferringCollector.getLeafCollector(
                            new AggregationExecutionContext(context, null, null, null)
                        );
                        return new LeafBucketCollector() {
                            @Override
                            public void collect(int doc, long bucket) throws IOException {
                                delegate.collect(doc, doc);
                            }
                        };
                    }
                });
                deferringCollector.postCollection();

                // exactly one positive charge per segment
                assertThat(events.size(), equalTo(2));
                assertThat("segment 1 charge must be positive", events.get(0), greaterThan(0L));
                assertThat("segment 2 charge must be positive", events.get(1), greaterThan(0L));

                deferringCollector.prepareSelectedBuckets(toLongArray(0));

                // each entry's return must exactly match its charge
                assertThat(events.size(), equalTo(4));
                assertThat("segment 1 return must equal its charge", events.get(2), equalTo(-events.get(0)));
                assertThat("segment 2 return must equal its charge", events.get(3), equalTo(-events.get(1)));
                assertThat("net balance must be zero", events.stream().mapToLong(Long::longValue).sum(), equalTo(0L));
            }
        }
    }

    /**
     * Verifies the exact circuit-breaker event sequence when {@link BestBucketsDeferringCollector#rewriteBuckets}
     * is called between segments.
     * <p>
     * Expected event list:
     * <pre>
     *   +chargeA     — finishLeaf for segment 1 (triggered when segment 2 starts)
     *   -chargeA     — rewriteBuckets loop 1: return old entry bytes
     *   +chargeA'    — rewriteBuckets loop 2: charge rebuilt entry (all-zero buckets
     *                  compress better, so chargeA' &lt; chargeA)
     *   +chargeB     — finishLeaf for segment 2 (postCollection)
     *   -chargeA'    — prepareSelectedBuckets frees segment 1 (rebuilt) entry
     *   -chargeB     — prepareSelectedBuckets frees segment 2 entry
     * </pre>
     */
    public void testCircuitBreakerRewriteBucketsProducesSymmetricEvents() throws IOException {
        try (Directory directory = newDirectory()) {
            try (IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig())) {
                for (int i = 0; i < 5; i++) {
                    indexWriter.addDocument(new Document());
                }
                indexWriter.commit();
                for (int i = 0; i < 5; i++) {
                    indexWriter.addDocument(new Document());
                }
            }

            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                IndexSearcher indexSearcher = newSearcher(indexReader);
                Query query = Queries.ALL_DOCS_INSTANCE;

                List<Long> events = new ArrayList<>();
                AtomicInteger segmentsSeen = new AtomicInteger();
                BestBucketsDeferringCollector deferringCollector = new BestBucketsDeferringCollector(
                    query,
                    indexSearcher,
                    false,
                    events::add
                );
                deferringCollector.setDeferredCollector(Collections.singleton(BucketCollector.NO_OP_BUCKET_COLLECTOR));
                deferringCollector.preCollection();
                indexSearcher.search(query, new Collector() {
                    @Override
                    public ScoreMode scoreMode() {
                        return ScoreMode.COMPLETE_NO_SCORES;
                    }

                    @Override
                    public LeafBucketCollector getLeafCollector(LeafReaderContext context) throws IOException {
                        // finishLeaf() for the previous segment runs inside getLeafCollector,
                        // so segment 1 is committed before rewriteBuckets is called.
                        LeafBucketCollector delegate = deferringCollector.getLeafCollector(
                            new AggregationExecutionContext(context, null, null, null)
                        );
                        if (segmentsSeen.incrementAndGet() == 2) {
                            // merge all distinct ordinals (0-4) into bucket 0 — the
                            // rebuilt buckets array is all-zero and compresses smaller
                            deferringCollector.rewriteBuckets(oldBucket -> 0);
                        }
                        return new LeafBucketCollector() {
                            @Override
                            public void collect(int doc, long bucket) throws IOException {
                                delegate.collect(doc, doc);
                            }
                        };
                    }
                });
                deferringCollector.postCollection();

                // [+chargeA, -chargeA, +chargeA', +chargeB]
                assertThat(events.size(), equalTo(4));
                long chargeA = events.get(0);
                long returnA = events.get(1);
                long chargeAPrime = events.get(2);
                long chargeB = events.get(3);

                assertThat("segment 1 initial charge must be positive", chargeA, greaterThan(0L));
                assertThat("rewriteBuckets must return the exact original bytes", returnA, equalTo(-chargeA));
                assertThat("rebuilt entry charge must be positive", chargeAPrime, greaterThan(0L));
                assertThat("all-zero buckets must compress smaller than distinct ordinals", chargeAPrime, lessThan(chargeA));
                assertThat("segment 2 charge must be positive", chargeB, greaterThan(0L));

                deferringCollector.prepareSelectedBuckets(toLongArray(0));

                // [+chargeA, -chargeA, +chargeA', +chargeB, -chargeA', -chargeB]
                assertThat(events.size(), equalTo(6));
                assertThat("rebuilt entry return must match its charge", events.get(4), equalTo(-chargeAPrime));
                assertThat("segment 2 return must match its charge", events.get(5), equalTo(-chargeB));
                assertThat("net balance must be zero", events.stream().mapToLong(Long::longValue).sum(), equalTo(0L));
            }
        }
    }

    /**
     * Verifies that a circuit breaker trip during {@code postCollection} propagates correctly.
     */
    public void testCircuitBreakerTripDuringFinishLeaf() throws IOException {
        try (Directory directory = newDirectory()) {
            try (IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig())) {
                for (int i = 0; i < 10; i++) {
                    indexWriter.addDocument(new Document());
                }
            }

            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                IndexSearcher indexSearcher = newSearcher(indexReader);
                Query query = Queries.ALL_DOCS_INSTANCE;

                BestBucketsDeferringCollector deferringCollector = new BestBucketsDeferringCollector(query, indexSearcher, false, bytes -> {
                    if (bytes > 0) {
                        throw new CircuitBreakingException("test trip", CircuitBreaker.Durability.TRANSIENT);
                    }
                });
                deferringCollector.setDeferredCollector(Collections.singleton(BucketCollector.NO_OP_BUCKET_COLLECTOR));
                deferringCollector.preCollection();
                indexSearcher.search(query, new Collector() {
                    @Override
                    public ScoreMode scoreMode() {
                        return ScoreMode.COMPLETE_NO_SCORES;
                    }

                    @Override
                    public LeafBucketCollector getLeafCollector(LeafReaderContext context) throws IOException {
                        return deferringCollector.getLeafCollector(new AggregationExecutionContext(context, null, null, null));
                    }
                });
                expectThrows(CircuitBreakingException.class, deferringCollector::postCollection);
            }
        }
    }

    /**
     * Edge case 1: a segment where no docs match the query creates no entry and no CB events.
     * <p>
     * {@code finishLeaf} guards on {@code aggCtx != null}; without any {@code collect()} calls
     * in a segment, {@code aggCtx} is never initialized, so no bytes are charged for that segment.
     */
    public void testCircuitBreakerNoChargeForSegmentWithNoMatchingDocs() throws IOException {
        try (Directory directory = newDirectory()) {
            try (IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig())) {
                Document d = new Document();
                d.add(new StringField("field", "seg1", Field.Store.NO));
                indexWriter.addDocument(d);
                indexWriter.commit();
                d = new Document();
                d.add(new StringField("field", "seg2", Field.Store.NO));
                indexWriter.addDocument(d);
            }

            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                IndexSearcher indexSearcher = newSearcher(indexReader);
                // query only matches the "seg2" segment
                Query query = new TermQuery(new Term("field", "seg2"));

                List<Long> events = new ArrayList<>();
                BestBucketsDeferringCollector deferringCollector = new BestBucketsDeferringCollector(
                    query,
                    indexSearcher,
                    false,
                    events::add
                );
                deferringCollector.setDeferredCollector(Collections.singleton(BucketCollector.NO_OP_BUCKET_COLLECTOR));
                deferringCollector.preCollection();
                indexSearcher.search(query, new Collector() {
                    @Override
                    public ScoreMode scoreMode() {
                        return ScoreMode.COMPLETE_NO_SCORES;
                    }

                    @Override
                    public LeafBucketCollector getLeafCollector(LeafReaderContext context) throws IOException {
                        LeafBucketCollector delegate = deferringCollector.getLeafCollector(
                            new AggregationExecutionContext(context, null, null, null)
                        );
                        return new LeafBucketCollector() {
                            @Override
                            public void collect(int doc, long bucket) throws IOException {
                                delegate.collect(doc, 0);
                            }
                        };
                    }
                });
                deferringCollector.postCollection();

                // the empty segment creates no entry, so exactly one positive charge (for the matching segment)
                assertThat("empty segment must produce no CB event", events.size(), equalTo(1));
                assertThat("charge for the matching segment must be positive", events.get(0), greaterThan(0L));

                deferringCollector.prepareSelectedBuckets(toLongArray(0));

                assertThat("return event added", events.size(), equalTo(2));
                assertThat("return must negate the charge exactly", events.get(1), equalTo(-events.get(0)));
                assertThat("net balance must be zero", events.stream().mapToLong(Long::longValue).sum(), equalTo(0L));
            }
        }
    }

    /**
     * Edge case 2: {@code prepareSelectedBuckets} with no matching bucket ordinals still returns
     * all entry bytes.
     * <p>
     * The return call in {@code prepareSelectedBuckets} is unconditional — it fires after each
     * entry's replay loop regardless of how many docs were forwarded to sub-collectors.
     */
    public void testCircuitBreakerBytesReturnedEvenWhenNoBucketsMatch() throws IOException {
        try (Directory directory = newDirectory()) {
            try (IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig())) {
                for (int i = 0; i < 5; i++) {
                    indexWriter.addDocument(new Document());
                }
                indexWriter.commit();
                for (int i = 0; i < 5; i++) {
                    indexWriter.addDocument(new Document());
                }
            }

            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                IndexSearcher indexSearcher = newSearcher(indexReader);
                Query query = Queries.ALL_DOCS_INSTANCE;

                List<Long> events = new ArrayList<>();
                BestBucketsDeferringCollector deferringCollector = new BestBucketsDeferringCollector(
                    query,
                    indexSearcher,
                    false,
                    events::add
                );
                deferringCollector.setDeferredCollector(Collections.singleton(BucketCollector.NO_OP_BUCKET_COLLECTOR));
                deferringCollector.preCollection();
                indexSearcher.search(query, new Collector() {
                    @Override
                    public ScoreMode scoreMode() {
                        return ScoreMode.COMPLETE_NO_SCORES;
                    }

                    @Override
                    public LeafBucketCollector getLeafCollector(LeafReaderContext context) throws IOException {
                        LeafBucketCollector delegate = deferringCollector.getLeafCollector(
                            new AggregationExecutionContext(context, null, null, null)
                        );
                        return new LeafBucketCollector() {
                            @Override
                            public void collect(int doc, long bucket) throws IOException {
                                delegate.collect(doc, 0);
                            }
                        };
                    }
                });
                deferringCollector.postCollection();

                assertThat("two segments yield two charge events", events.size(), equalTo(2));
                assertThat("segment 1 charge must be positive", events.get(0), greaterThan(0L));
                assertThat("segment 2 charge must be positive", events.get(1), greaterThan(0L));

                // ordinal 999 was never collected; no docs are replayed, but bytes must still be returned
                deferringCollector.prepareSelectedBuckets(toLongArray(999));

                assertThat("two return events even though no docs were replayed", events.size(), equalTo(4));
                assertThat("segment 1 return must negate its charge", events.get(2), equalTo(-events.get(0)));
                assertThat("segment 2 return must negate its charge", events.get(3), equalTo(-events.get(1)));
                assertThat("net balance must be zero", events.stream().mapToLong(Long::longValue).sum(), equalTo(0L));
            }
        }
    }

    /**
     * Edge case 3: {@code rewriteBuckets} called when {@code entries} is empty and no
     * {@code collect()} has run yet fires no CB events.
     * <p>
     * Both loops inside {@code rewriteBuckets} iterate over an empty list (no-ops) and the
     * in-flight builder check is skipped because {@code bucketsBuilder} is null.
     */
    public void testCircuitBreakerNoEventsWhenRewritingEmptyEntries() throws IOException {
        try (Directory directory = newDirectory()) {
            try (IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig())) {
                for (int i = 0; i < 5; i++) {
                    indexWriter.addDocument(new Document());
                }
            }

            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                IndexSearcher indexSearcher = newSearcher(indexReader);
                Query query = Queries.ALL_DOCS_INSTANCE;

                List<Long> events = new ArrayList<>();
                BestBucketsDeferringCollector deferringCollector = new BestBucketsDeferringCollector(
                    query,
                    indexSearcher,
                    false,
                    events::add
                );
                deferringCollector.setDeferredCollector(Collections.singleton(BucketCollector.NO_OP_BUCKET_COLLECTOR));
                deferringCollector.preCollection();

                // rewrite before any collect(): entries is empty, bucketsBuilder is null
                deferringCollector.rewriteBuckets(oldBucket -> oldBucket);
                assertThat("rewriting empty entries fires no CB events", events.size(), equalTo(0));

                indexSearcher.search(query, new Collector() {
                    @Override
                    public ScoreMode scoreMode() {
                        return ScoreMode.COMPLETE_NO_SCORES;
                    }

                    @Override
                    public LeafBucketCollector getLeafCollector(LeafReaderContext context) throws IOException {
                        LeafBucketCollector delegate = deferringCollector.getLeafCollector(
                            new AggregationExecutionContext(context, null, null, null)
                        );
                        return new LeafBucketCollector() {
                            @Override
                            public void collect(int doc, long bucket) throws IOException {
                                delegate.collect(doc, 0);
                            }
                        };
                    }
                });
                deferringCollector.postCollection();

                assertThat("one charge event at postCollection", events.size(), equalTo(1));
                assertThat("charge must be positive", events.get(0), greaterThan(0L));

                deferringCollector.prepareSelectedBuckets(toLongArray(0));

                assertThat("return event added", events.size(), equalTo(2));
                assertThat("net balance must be zero", events.stream().mapToLong(Long::longValue).sum(), equalTo(0L));
            }
        }
    }

    /**
     * Edge case 4: {@code rewriteBuckets} that maps all ordinals to {@code -1} removes all
     * committed entries and returns their bytes without charging any new bytes.
     * <p>
     * Expected event sequence:
     * <pre>
     *   +chargeA  — finishLeaf for segment 1 (triggered when segment 2 starts)
     *   -chargeA  — rewriteBuckets returns all old entry bytes; no new entries recharged
     *   +chargeB  — finishLeaf for segment 2 (postCollection)
     *   -chargeB  — prepareSelectedBuckets frees segment 2 entry
     * </pre>
     */
    public void testCircuitBreakerReturnsBytesWhenRewriteRemovesAllEntries() throws IOException {
        try (Directory directory = newDirectory()) {
            try (IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig())) {
                for (int i = 0; i < 5; i++) {
                    indexWriter.addDocument(new Document());
                }
                indexWriter.commit();
                for (int i = 0; i < 5; i++) {
                    indexWriter.addDocument(new Document());
                }
            }

            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                IndexSearcher indexSearcher = newSearcher(indexReader);
                Query query = Queries.ALL_DOCS_INSTANCE;

                List<Long> events = new ArrayList<>();
                AtomicInteger segmentsSeen = new AtomicInteger();
                BestBucketsDeferringCollector deferringCollector = new BestBucketsDeferringCollector(
                    query,
                    indexSearcher,
                    false,
                    events::add
                );
                deferringCollector.setDeferredCollector(Collections.singleton(BucketCollector.NO_OP_BUCKET_COLLECTOR));
                deferringCollector.preCollection();
                indexSearcher.search(query, new Collector() {
                    @Override
                    public ScoreMode scoreMode() {
                        return ScoreMode.COMPLETE_NO_SCORES;
                    }

                    @Override
                    public LeafBucketCollector getLeafCollector(LeafReaderContext context) throws IOException {
                        // finishLeaf() for segment 1 runs inside getLeafCollector, committing it before the rewrite
                        LeafBucketCollector delegate = deferringCollector.getLeafCollector(
                            new AggregationExecutionContext(context, null, null, null)
                        );
                        if (segmentsSeen.incrementAndGet() == 2) {
                            // segment 1 is now committed; prune all its entries
                            deferringCollector.rewriteBuckets(oldBucket -> -1L);
                        }
                        return new LeafBucketCollector() {
                            @Override
                            public void collect(int doc, long bucket) throws IOException {
                                delegate.collect(doc, 0);
                            }
                        };
                    }
                });
                deferringCollector.postCollection();

                // [+chargeA, -chargeA, +chargeB]
                assertThat(events.size(), equalTo(3));
                assertThat("segment 1 initial charge must be positive", events.get(0), greaterThan(0L));
                assertThat("rewriteBuckets must return segment 1 bytes exactly", events.get(1), equalTo(-events.get(0)));
                assertThat("segment 2 charge must be positive", events.get(2), greaterThan(0L));

                deferringCollector.prepareSelectedBuckets(toLongArray(0));

                // [+chargeA, -chargeA, +chargeB, -chargeB]
                assertThat(events.size(), equalTo(4));
                assertThat("segment 2 return must negate its charge", events.get(3), equalTo(-events.get(2)));
                assertThat("net balance must be zero", events.stream().mapToLong(Long::longValue).sum(), equalTo(0L));
            }
        }
    }

    /**
     * Edge case 5: two successive calls to {@code rewriteBuckets} maintain correct accounting.
     * <p>
     * Expected event sequence:
     * <pre>
     *   +chargeA   — finishLeaf for segment 1
     *   -chargeA   — first rewriteBuckets (merge all → 0): return old bytes
     *   +chargeA'  — first rewriteBuckets: charge rebuilt entry
     *   -chargeA'  — second rewriteBuckets (prune all → -1): return rebuilt bytes, no recharge
     *   +chargeB   — finishLeaf for segment 2 (postCollection)
     *   -chargeB   — prepareSelectedBuckets frees segment 2 entry
     * </pre>
     */
    public void testCircuitBreakerMultipleRewritesAreSymmetric() throws IOException {
        try (Directory directory = newDirectory()) {
            try (IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig())) {
                for (int i = 0; i < 5; i++) {
                    indexWriter.addDocument(new Document());
                }
                indexWriter.commit();
                for (int i = 0; i < 5; i++) {
                    indexWriter.addDocument(new Document());
                }
            }

            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                IndexSearcher indexSearcher = newSearcher(indexReader);
                Query query = Queries.ALL_DOCS_INSTANCE;

                List<Long> events = new ArrayList<>();
                AtomicInteger segmentsSeen = new AtomicInteger();
                BestBucketsDeferringCollector deferringCollector = new BestBucketsDeferringCollector(
                    query,
                    indexSearcher,
                    false,
                    events::add
                );
                deferringCollector.setDeferredCollector(Collections.singleton(BucketCollector.NO_OP_BUCKET_COLLECTOR));
                deferringCollector.preCollection();
                indexSearcher.search(query, new Collector() {
                    @Override
                    public ScoreMode scoreMode() {
                        return ScoreMode.COMPLETE_NO_SCORES;
                    }

                    @Override
                    public LeafBucketCollector getLeafCollector(LeafReaderContext context) throws IOException {
                        LeafBucketCollector delegate = deferringCollector.getLeafCollector(
                            new AggregationExecutionContext(context, null, null, null)
                        );
                        if (segmentsSeen.incrementAndGet() == 2) {
                            // first rewrite: merge all distinct ordinals → 0
                            deferringCollector.rewriteBuckets(oldBucket -> 0L);
                            // second rewrite: prune all entries → -1
                            deferringCollector.rewriteBuckets(oldBucket -> -1L);
                        }
                        return new LeafBucketCollector() {
                            @Override
                            public void collect(int doc, long bucket) throws IOException {
                                delegate.collect(doc, doc);
                            }
                        };
                    }
                });
                deferringCollector.postCollection();

                // [+chargeA, -chargeA, +chargeA', -chargeA', +chargeB]
                assertThat(events.size(), equalTo(5));
                long chargeA = events.get(0);
                long returnA = events.get(1);
                long chargeAPrime = events.get(2);
                long returnAPrime = events.get(3);
                long chargeB = events.get(4);

                assertThat("initial segment 1 charge must be positive", chargeA, greaterThan(0L));
                assertThat("first rewrite must return segment 1 bytes exactly", returnA, equalTo(-chargeA));
                assertThat("first rewrite rebuilt entry must have positive charge", chargeAPrime, greaterThan(0L));
                assertThat("second rewrite must return rebuilt bytes exactly", returnAPrime, equalTo(-chargeAPrime));
                assertThat("segment 2 charge must be positive", chargeB, greaterThan(0L));

                deferringCollector.prepareSelectedBuckets(toLongArray(0));

                // [+chargeA, -chargeA, +chargeA', -chargeA', +chargeB, -chargeB]
                assertThat(events.size(), equalTo(6));
                assertThat("segment 2 return must negate its charge", events.get(5), equalTo(-chargeB));
                assertThat("net balance must be zero", events.stream().mapToLong(Long::longValue).sum(), equalTo(0L));
            }
        }
    }

    /**
     * Edge case 6: zero-byte heartbeat events (fired every 1024 {@code collect()} calls) are
     * captured in the event list but do not affect the circuit-breaker balance.
     * <p>
     * {@code callCount} is not reset between segments, so with 2048 total collected docs the
     * heartbeat fires exactly twice (at {@code callCount} == 1024 and 2048).
     */
    public void testCircuitBreakerHeartbeatEventsDoNotAffectBalance() throws IOException {
        try (Directory directory = newDirectory()) {
            try (IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig())) {
                for (int i = 0; i < 2048; i++) {
                    indexWriter.addDocument(new Document());
                }
            }

            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                IndexSearcher indexSearcher = newSearcher(indexReader);
                Query query = Queries.ALL_DOCS_INSTANCE;

                List<Long> events = new ArrayList<>();
                BestBucketsDeferringCollector deferringCollector = new BestBucketsDeferringCollector(
                    query,
                    indexSearcher,
                    false,
                    events::add
                );
                deferringCollector.setDeferredCollector(Collections.singleton(BucketCollector.NO_OP_BUCKET_COLLECTOR));
                deferringCollector.preCollection();
                indexSearcher.search(query, new Collector() {
                    @Override
                    public ScoreMode scoreMode() {
                        return ScoreMode.COMPLETE_NO_SCORES;
                    }

                    @Override
                    public LeafBucketCollector getLeafCollector(LeafReaderContext context) throws IOException {
                        LeafBucketCollector delegate = deferringCollector.getLeafCollector(
                            new AggregationExecutionContext(context, null, null, null)
                        );
                        return new LeafBucketCollector() {
                            @Override
                            public void collect(int doc, long bucket) throws IOException {
                                delegate.collect(doc, 0);
                            }
                        };
                    }
                });
                deferringCollector.postCollection();

                long heartbeats = events.stream().filter(e -> e == 0L).count();
                // callCount is shared across segments: 2048 total calls → multiples of 1024 at 1024 and 2048
                assertThat("2048 docs must produce exactly 2 zero-byte heartbeats", heartbeats, equalTo(2L));

                deferringCollector.prepareSelectedBuckets(toLongArray(0));

                assertThat(
                    "heartbeat events (0L) must not affect the net balance",
                    events.stream().mapToLong(Long::longValue).sum(),
                    equalTo(0L)
                );
            }
        }
    }

    private BucketCollector bla(Set<Integer> docIds) {
        return new BucketCollector() {
            @Override
            public LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx) throws IOException {
                return new LeafBucketCollector() {
                    @Override
                    public void collect(int doc, long bucket) throws IOException {
                        docIds.add(aggCtx.getLeafReaderContext().docBase + doc);
                    }
                };
            }

            @Override
            public void preCollection() throws IOException {

            }

            @Override
            public void postCollection() throws IOException {

            }

            @Override
            public ScoreMode scoreMode() {
                return ScoreMode.COMPLETE_NO_SCORES;
            }
        };
    }

    public void testBucketMergeNoDelete() throws Exception {
        testCase((deferringCollector, delegate) -> new LeafBucketCollector() {
            @Override
            public void collect(int doc, long owningBucketOrd) throws IOException {
                assert owningBucketOrd == 0; // Only collects at top level
                delegate.collect(doc, doc);
                if (doc == 7) {
                    deferringCollector.rewriteBuckets(oldBucket -> 0);
                }
            }
        }, (deferringCollector, finalCollector) -> {
            deferringCollector.prepareSelectedBuckets(toLongArray(0, 8, 9));

            equalTo(Map.of(0L, List.of(0, 1, 2, 3, 4, 5, 6, 7), 1L, List.of(8), 2L, List.of(9)));
        });
    }

    public void testBucketMergeAndDelete() throws Exception {
        testCase((deferringCollector, delegate) -> new LeafBucketCollector() {
            @Override
            public void collect(int doc, long owningBucketOrd) throws IOException {
                assert owningBucketOrd == 0; // Only collects at top level
                delegate.collect(doc, doc);
                if (doc == 7) {
                    deferringCollector.rewriteBuckets(oldBucket -> oldBucket > 3 ? 0 : -1);
                }
            }
        }, (deferringCollector, finalCollector) -> {
            deferringCollector.prepareSelectedBuckets(toLongArray(0, 8, 9));

            assertThat(finalCollector.collection, equalTo(Map.of(0L, List.of(4, 5, 6, 7), 1L, List.of(8), 2L, List.of(9))));
        });
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/60021")
    public void testBucketMergeAndDeleteLastEntry() throws Exception {
        testCase((deferringCollector, delegate) -> new LeafBucketCollector() {
            @Override
            public void collect(int doc, long owningBucketOrd) throws IOException {
                assert owningBucketOrd == 0; // Only collects at top level
                delegate.collect(doc, doc);
                if (doc == 7) {
                    deferringCollector.rewriteBuckets(oldBucket -> oldBucket <= 3 ? 0 : -1);
                }
            }
        }, (deferringCollector, finalCollector) -> {
            deferringCollector.prepareSelectedBuckets(toLongArray(0, 8, 9));

            assertThat(finalCollector.collection, equalTo(Map.of(0L, List.of(0, 1, 2, 3), 1L, List.of(8), 2L, List.of(9))));
        });
    }

    private LongArray toLongArray(long... lons) {
        LongArray longArray = BigArrays.NON_RECYCLING_INSTANCE.newLongArray(lons.length);
        for (int i = 0; i < lons.length; i++) {
            longArray.set(i, lons[i]);
        }
        return longArray;
    }

    private void testCase(
        BiFunction<BestBucketsDeferringCollector, LeafBucketCollector, LeafBucketCollector> leafCollector,
        CheckedBiConsumer<BestBucketsDeferringCollector, CollectingBucketCollector, IOException> verify
    ) throws IOException {
        try (Directory directory = newDirectory()) {
            try (IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig())) {
                for (int i = 0; i < 10; i++) {
                    indexWriter.addDocument(new Document());
                }
            }

            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                IndexSearcher indexSearcher = newSearcher(indexReader);

                Query query = Queries.ALL_DOCS_INSTANCE;
                BestBucketsDeferringCollector deferringCollector = new BestBucketsDeferringCollector(
                    query,
                    indexSearcher,
                    false,
                    bytes -> {}
                );

                CollectingBucketCollector finalCollector = new CollectingBucketCollector();
                deferringCollector.setDeferredCollector(Collections.singleton(finalCollector));
                deferringCollector.preCollection();
                indexSearcher.search(query, new Collector() {
                    @Override
                    public ScoreMode scoreMode() {
                        return ScoreMode.COMPLETE_NO_SCORES;
                    }

                    @Override
                    public LeafBucketCollector getLeafCollector(LeafReaderContext context) throws IOException {
                        LeafBucketCollector delegate = deferringCollector.getLeafCollector(
                            new AggregationExecutionContext(context, null, null, null)
                        );
                        return leafCollector.apply(deferringCollector, delegate);
                    }
                });
                deferringCollector.postCollection();
                verify.accept(deferringCollector, finalCollector);
            }
        }
    }

    private class CollectingBucketCollector extends BucketCollector {
        final Map<Long, List<Integer>> collection = new HashMap<>();

        @Override
        public ScoreMode scoreMode() {
            return ScoreMode.COMPLETE_NO_SCORES;
        }

        @Override
        public LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx) throws IOException {
            return new LeafBucketCollector() {
                @Override
                public void collect(int doc, long owningBucketOrd) throws IOException {
                    collection.computeIfAbsent(owningBucketOrd, k -> new ArrayList<>()).add(doc);
                }
            };
        }

        @Override
        public void preCollection() throws IOException {}

        @Override
        public void postCollection() throws IOException {

        }
    }
}
