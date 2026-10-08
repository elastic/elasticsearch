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
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.LongConsumer;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
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

    public void testCrankyBreakerNeverLeaksOnCompletion() throws IOException {
        withSearcher(searcher -> {
            AtomicLong used = new AtomicLong();
            LongConsumer cranky = bytes -> {
                if (bytes > 0 && random().nextInt(20) == 0) {
                    throw new CircuitBreakingException("cranky breaker", CircuitBreaker.Durability.TRANSIENT);
                }
                used.addAndGet(bytes);
            };
            BestBucketsDeferringCollector dc = new BestBucketsDeferringCollector(Queries.ALL_DOCS_INSTANCE, searcher, false, cranky);
            dc.setDeferredCollector(Collections.singleton(BucketCollector.NO_OP_BUCKET_COLLECTOR));
            try {
                dc.preCollection();
                searcher.search(Queries.ALL_DOCS_INSTANCE, delegatingCollector(dc, delegate -> new LeafBucketCollector() {
                    @Override
                    public void collect(int doc, long bucket) throws IOException {
                        delegate.collect(doc, 0);
                    }
                }));
                dc.postCollection();
                dc.prepareSelectedBuckets(toLongArray(0));
                assertThat("completed lifecycle must have zero net balance", used.get(), equalTo(0L));
            } catch (CircuitBreakingException e) {
                assertThat(e.getMessage(), equalTo("cranky breaker"));
                assertThat("must not over-release bytes on trip", used.get(), greaterThanOrEqualTo(0L));
            }
        }, 5, 5);
    }

    public void testCircuitBreakerChargesOneEventPerSegmentAndReleasesSymmetrically() throws IOException {
        runBreakerTest((searcher, dc, events) -> {
            dc.preCollection();
            searcher.search(Queries.ALL_DOCS_INSTANCE, delegatingCollector(dc, delegate -> new LeafBucketCollector() {
                @Override
                public void collect(int doc, long bucket) throws IOException {
                    delegate.collect(doc, doc);
                }
            }));
            dc.postCollection();

            assertThat(events.size(), equalTo(2));
            assertThat(events.get(0), greaterThan(0L));
            assertThat(events.get(1), greaterThan(0L));

            dc.prepareSelectedBuckets(toLongArray(0));

            assertThat(events.size(), equalTo(4));
            assertThat(events.get(2), equalTo(-events.get(0)));
            assertThat(events.get(3), equalTo(-events.get(1)));
        });
    }

    public void testCircuitBreakerRewriteBucketsProducesSymmetricEvents() throws IOException {
        AtomicInteger segmentsSeen = new AtomicInteger();
        runBreakerTest((searcher, dc, events) -> {
            dc.preCollection();
            searcher.search(Queries.ALL_DOCS_INSTANCE, delegatingCollector(dc, delegate -> {
                if (segmentsSeen.incrementAndGet() == 2) {
                    // merge all distinct ordinals into 0 — rebuilt buckets compress smaller
                    dc.rewriteBuckets(oldBucket -> 0);
                }
                return new LeafBucketCollector() {
                    @Override
                    public void collect(int doc, long bucket) throws IOException {
                        delegate.collect(doc, doc);
                    }
                };
            }));
            dc.postCollection();

            // [+chargeA, -chargeA, +chargeA', +chargeB]
            assertThat(events.size(), equalTo(4));
            long chargeA = events.get(0), returnA = events.get(1), chargeAPrime = events.get(2), chargeB = events.get(3);
            assertThat(chargeA, greaterThan(0L));
            assertThat(returnA, equalTo(-chargeA));
            assertThat(chargeAPrime, greaterThan(0L));
            assertThat("all-zero buckets must compress smaller than distinct ordinals", chargeAPrime, lessThan(chargeA));
            assertThat(chargeB, greaterThan(0L));

            dc.prepareSelectedBuckets(toLongArray(0));

            assertThat(events.size(), equalTo(6));
            assertThat(events.get(4), equalTo(-chargeAPrime));
            assertThat(events.get(5), equalTo(-chargeB));
        });
    }

    public void testCircuitBreakerTripDuringFinishLeaf() throws IOException {
        AtomicLong used = new AtomicLong();
        withSearcher(searcher -> {
            BestBucketsDeferringCollector dc = new BestBucketsDeferringCollector(Queries.ALL_DOCS_INSTANCE, searcher, false, bytes -> {
                if (bytes > 0) {
                    throw new CircuitBreakingException("test trip", CircuitBreaker.Durability.TRANSIENT);
                }
                used.addAndGet(bytes);
            });
            dc.setDeferredCollector(Collections.singleton(BucketCollector.NO_OP_BUCKET_COLLECTOR));
            dc.preCollection();
            searcher.search(Queries.ALL_DOCS_INSTANCE, delegatingCollector(dc, delegate -> delegate));
            expectThrows(CircuitBreakingException.class, dc::postCollection);
            assertThat("no bytes should remain charged after trip", used.get(), equalTo(0L));
        }, 10);
    }

    public void testCircuitBreakerNoChargeForSegmentWithNoMatchingDocs() throws IOException {
        try (Directory dir = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
                Document d = new Document();
                d.add(new StringField("field", "seg1", Field.Store.NO));
                writer.addDocument(d);
                writer.commit();
                d = new Document();
                d.add(new StringField("field", "seg2", Field.Store.NO));
                writer.addDocument(d);
            }
            try (IndexReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = newSearcher(reader);
                Query query = new TermQuery(new Term("field", "seg2"));
                List<Long> events = new ArrayList<>();
                BestBucketsDeferringCollector dc = setupCollector(searcher, query, events);
                dc.preCollection();
                searcher.search(query, delegatingCollector(dc, delegate -> new LeafBucketCollector() {
                    @Override
                    public void collect(int doc, long bucket) throws IOException {
                        delegate.collect(doc, 0);
                    }
                }));
                dc.postCollection();

                // the non-matching segment produces no entry and no CB event
                assertThat(events.size(), equalTo(1));
                assertThat(events.get(0), greaterThan(0L));

                dc.prepareSelectedBuckets(toLongArray(0));

                assertThat(events.size(), equalTo(2));
                assertThat(events.get(1), equalTo(-events.get(0)));
                assertThat(events.stream().mapToLong(Long::longValue).sum(), equalTo(0L));
            }
        }
    }

    public void testCircuitBreakerBytesReturnedEvenWhenNoBucketsMatch() throws IOException {
        runBreakerTest((searcher, dc, events) -> {
            dc.preCollection();
            searcher.search(Queries.ALL_DOCS_INSTANCE, delegatingCollector(dc, delegate -> new LeafBucketCollector() {
                @Override
                public void collect(int doc, long bucket) throws IOException {
                    delegate.collect(doc, 0);
                }
            }));
            dc.postCollection();

            assertThat(events.size(), equalTo(2));
            assertThat(events.get(0), greaterThan(0L));
            assertThat(events.get(1), greaterThan(0L));

            // ordinal 999 was never collected; no docs are replayed, but bytes must still be returned
            dc.prepareSelectedBuckets(toLongArray(999));

            assertThat(events.size(), equalTo(4));
            assertThat(events.get(2), equalTo(-events.get(0)));
            assertThat(events.get(3), equalTo(-events.get(1)));
        });
    }

    public void testCircuitBreakerNoEventsWhenRewritingEmptyEntries() throws IOException {
        runBreakerTest((searcher, dc, events) -> {
            dc.preCollection();

            // rewrite before any collect(): entries is empty, bucketsBuilder is null — no CB events
            dc.rewriteBuckets(oldBucket -> oldBucket);
            assertThat(events.size(), equalTo(0));

            searcher.search(Queries.ALL_DOCS_INSTANCE, delegatingCollector(dc, delegate -> new LeafBucketCollector() {
                @Override
                public void collect(int doc, long bucket) throws IOException {
                    delegate.collect(doc, 0);
                }
            }));
            dc.postCollection();

            assertThat(events.size(), equalTo(1));
            assertThat(events.get(0), greaterThan(0L));

            dc.prepareSelectedBuckets(toLongArray(0));

            assertThat(events.size(), equalTo(2));
        }, 5);
    }

    public void testCircuitBreakerReturnsBytesWhenRewriteRemovesAllEntries() throws IOException {
        AtomicInteger segmentsSeen = new AtomicInteger();
        runBreakerTest((searcher, dc, events) -> {
            dc.preCollection();
            searcher.search(Queries.ALL_DOCS_INSTANCE, delegatingCollector(dc, delegate -> {
                if (segmentsSeen.incrementAndGet() == 2) {
                    // prune all committed entries from segment 1
                    dc.rewriteBuckets(oldBucket -> -1L);
                }
                return new LeafBucketCollector() {
                    @Override
                    public void collect(int doc, long bucket) throws IOException {
                        delegate.collect(doc, 0);
                    }
                };
            }));
            dc.postCollection();

            // [+chargeA, -chargeA, +chargeB]
            assertThat(events.size(), equalTo(3));
            assertThat(events.get(0), greaterThan(0L));
            assertThat(events.get(1), equalTo(-events.get(0)));
            assertThat(events.get(2), greaterThan(0L));

            dc.prepareSelectedBuckets(toLongArray(0));

            assertThat(events.size(), equalTo(4));
            assertThat(events.get(3), equalTo(-events.get(2)));
        });
    }

    public void testCircuitBreakerMultipleRewritesAreSymmetric() throws IOException {
        AtomicInteger segmentsSeen = new AtomicInteger();
        runBreakerTest((searcher, dc, events) -> {
            dc.preCollection();
            searcher.search(Queries.ALL_DOCS_INSTANCE, delegatingCollector(dc, delegate -> {
                if (segmentsSeen.incrementAndGet() == 2) {
                    dc.rewriteBuckets(oldBucket -> 0L);  // merge all → 0
                    dc.rewriteBuckets(oldBucket -> -1L); // prune all → removed
                }
                return new LeafBucketCollector() {
                    @Override
                    public void collect(int doc, long bucket) throws IOException {
                        delegate.collect(doc, doc);
                    }
                };
            }));
            dc.postCollection();

            // [+chargeA, -chargeA, +chargeA', -chargeA', +chargeB]
            assertThat(events.size(), equalTo(5));
            long chargeA = events.get(0), returnA = events.get(1);
            long chargeAPrime = events.get(2), returnAPrime = events.get(3);
            long chargeB = events.get(4);
            assertThat(chargeA, greaterThan(0L));
            assertThat(returnA, equalTo(-chargeA));
            assertThat(chargeAPrime, greaterThan(0L));
            assertThat(returnAPrime, equalTo(-chargeAPrime));
            assertThat(chargeB, greaterThan(0L));

            dc.prepareSelectedBuckets(toLongArray(0));

            assertThat(events.size(), equalTo(6));
            assertThat(events.get(5), equalTo(-chargeB));
        });
    }

    public void testCircuitBreakerHeartbeatEventsDoNotAffectBalance() throws IOException {
        runBreakerTest((searcher, dc, events) -> {
            dc.preCollection();
            searcher.search(Queries.ALL_DOCS_INSTANCE, delegatingCollector(dc, delegate -> new LeafBucketCollector() {
                @Override
                public void collect(int doc, long bucket) throws IOException {
                    delegate.collect(doc, 0);
                }
            }));
            dc.postCollection();

            // callCount is shared across segments: 2048 total calls → heartbeats at 1024 and 2048
            long heartbeats = events.stream().filter(e -> e == 0L).count();
            assertThat(heartbeats, equalTo(2L));

            dc.prepareSelectedBuckets(toLongArray(0));
        }, 2048);
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

    @FunctionalInterface
    private interface BreakerTest {
        void run(IndexSearcher searcher, BestBucketsDeferringCollector dc, List<Long> events) throws IOException;
    }

    @FunctionalInterface
    private interface SearcherTest {
        void run(IndexSearcher searcher) throws IOException;
    }

    private static void withSearcher(SearcherTest body, int... docsPerSegment) throws IOException {
        try (Directory dir = buildIndex(docsPerSegment); IndexReader reader = DirectoryReader.open(dir)) {
            body.run(newSearcher(reader));
        }
    }

    /** Runs {@code body} with a two-segment index (5 docs each) and a fresh event-recording collector. */
    private static void runBreakerTest(BreakerTest body, int... docsPerSegment) throws IOException {
        int[] segments = docsPerSegment.length == 0 ? new int[] { 5, 5 } : docsPerSegment;
        try (Directory dir = buildIndex(segments); IndexReader reader = DirectoryReader.open(dir)) {
            IndexSearcher searcher = newSearcher(reader);
            List<Long> events = new ArrayList<>();
            body.run(searcher, setupCollector(searcher, Queries.ALL_DOCS_INSTANCE, events), events);
            assertThat("net byte balance must be zero", events.stream().mapToLong(Long::longValue).sum(), equalTo(0L));
        }
    }

    /** Builds a Lucene directory with documents distributed across segments as specified. */
    private static Directory buildIndex(int... docsPerSegment) throws IOException {
        Directory directory = newDirectory();
        try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
            for (int s = 0; s < docsPerSegment.length; s++) {
                for (int i = 0; i < docsPerSegment[s]; i++) {
                    writer.addDocument(new Document());
                }
                if (s < docsPerSegment.length - 1) {
                    writer.commit();
                }
            }
        }
        return directory;
    }

    /** Creates a {@link BestBucketsDeferringCollector} wired to {@code events} with a NO_OP deferred collector. */
    private static BestBucketsDeferringCollector setupCollector(IndexSearcher searcher, Query query, List<Long> events) {
        BestBucketsDeferringCollector dc = new BestBucketsDeferringCollector(query, searcher, false, events::add);
        dc.setDeferredCollector(Collections.singleton(BucketCollector.NO_OP_BUCKET_COLLECTOR));
        return dc;
    }

    @FunctionalInterface
    private interface LeafFactory {
        LeafBucketCollector create(LeafBucketCollector delegate) throws IOException;
    }

    /** Wraps {@code dc} in a {@link Collector} that creates a leaf per segment using {@code factory}. */
    private static Collector delegatingCollector(BestBucketsDeferringCollector dc, LeafFactory factory) {
        return new Collector() {
            @Override
            public ScoreMode scoreMode() {
                return ScoreMode.COMPLETE_NO_SCORES;
            }

            @Override
            public LeafBucketCollector getLeafCollector(LeafReaderContext context) throws IOException {
                LeafBucketCollector delegate = dc.getLeafCollector(new AggregationExecutionContext(context, null, null, null));
                return factory.create(delegate);
            }
        };
    }

    private static LongArray toLongArray(long... lons) {
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

    private static class CollectingBucketCollector extends BucketCollector {
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
