/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.query;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.lucene.IndexedByShardIdFromList;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.cache.query.QueryCacheStats;
import org.elasticsearch.index.cache.query.TrivialQueryCachingPolicy;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesQueryCache;
import org.elasticsearch.search.internal.ContextIndexSearcher;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntConsumer;

import static org.elasticsearch.indices.IndicesQueryCache.INDICES_QUERIES_CACHE_ALL_SEGMENTS_SETTING;
import static org.hamcrest.Matchers.equalTo;

public class DocPartitioningQueryCacheTests extends ComputeTestCase {

    public void testCacheOnce() throws Exception {
        var dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig());
        final int numDocs = 200;
        for (int d = 0; d < numDocs; d++) {
            writer.addDocument(new Document());
        }
        var reader = DirectoryReader.open(writer);
        ShardId shard = new ShardId("index", "_na_", 0);
        reader = ElasticsearchDirectoryReader.wrap(reader, shard);
        writer.close();
        var indicesQueryCache = new IndicesQueryCache(
            Settings.builder().put(INDICES_QUERIES_CACHE_ALL_SEGMENTS_SETTING.getKey(), true).build()
        );
        var searcher = new ContextIndexSearcher(
            reader,
            IndexSearcher.getDefaultSimilarity(),
            indicesQueryCache,
            TrivialQueryCachingPolicy.ALWAYS,
            false
        );
        CountDownLatch blockedOnFirstRange = new CountDownLatch(1);
        final int blockingDocId = randomIntBetween(10, 90);
        AtomicInteger visited = new AtomicInteger();
        var query = new BlockingQuery(docId -> {
            if (docId == blockingDocId) {
                safeAwait(blockedOnFirstRange, TimeValue.THIRTY_SECONDS);
            }
            visited.incrementAndGet();
        });
        LuceneSliceQueue queue = LuceneSliceQueue.create(
            new IndexedByShardIdFromList<>(List.of(new LuceneSourceOperatorTests.MockShardContext(searcher, 0))),
            c -> List.of(new LuceneSliceQueue.QueryAndTags(query, List.of())),
            DataPartitioning.DOC,
            q -> LuceneSliceQueue.PartitioningStrategy.DOC,
            2,
            s -> ScoreMode.COMPLETE_NO_SCORES
        );
        LuceneSlice slice1 = queue.nextSlice(null);
        assertThat(slice1.numLeaves(), equalTo(1));
        LeafReaderContext singleLeaf = slice1.getLeaf(0).leafReaderContext();
        assertNull(slice1.leafBlockedOnCaching(singleLeaf));
        LuceneSlice slice2 = queue.nextSlice(null);
        assertThat(slice2.numLeaves(), equalTo(1));
        assertNull(slice2.leafBlockedOnCaching(singleLeaf));
        // thread-1 executes the first 100 docs
        CountCollector collector1 = new CountCollector();
        Thread thread1 = new Thread(() -> {
            try {
                BulkScorer scorer = slice1.weight().scorerSupplier(singleLeaf).bulkScorer();
                scorer.score(collector1, null, 0, 100);
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        });
        thread1.start();
        assertBusy(() -> {
            assertNotNull(slice1.leafBlockedOnCaching(singleLeaf));
            assertNotNull(slice2.leafBlockedOnCaching(singleLeaf));
        });
        SubscribableListener<Void> blocked = slice1.leafBlockedOnCaching(singleLeaf);
        assertFalse(blocked.isDone());
        CountCollector collector2 = new CountCollector();
        Thread thread2 = new Thread(() -> {
            try {
                BulkScorer scorer = slice2.weight().scorerSupplier(singleLeaf).bulkScorer();
                scorer.score(collector2, null, 100, 200);
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        });
        blocked.addListener(ActionListener.running(thread2::start));
        blockedOnFirstRange.countDown();
        thread1.join(10_000);
        thread2.join(10_000);
        assertThat(visited.get(), equalTo(200));
        reader.close();
        dir.close();
        QueryCacheStats cacheStats = indicesQueryCache.getStats(shard, () -> 0L);
        assertThat(cacheStats.getHitCount(), equalTo(1L));
        assertThat(cacheStats.getMissCount(), equalTo(1L));
        assertThat(cacheStats.getCacheCount(), equalTo(1L));
        assertThat(collector1.found, equalTo(100));
        assertThat(collector2.found, equalTo(100));
    }

    static class CountCollector implements LeafCollector {
        int found = 0;

        @Override
        public void setScorer(Scorable scorer) {

        }

        @Override
        public void collect(int doc) throws IOException {
            found++;
        }
    }

    static class BlockingQuery extends Query {
        final IntConsumer preVisit;

        BlockingQuery(IntConsumer preVisit) {
            this.preVisit = preVisit;
        }

        @Override
        public String toString(String field) {
            return "BlockingQuery";
        }

        @Override
        public void visit(QueryVisitor visitor) {

        }

        @Override
        public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
            return new BlockingWeight(this, preVisit);
        }

        @Override
        public boolean equals(Object other) {
            return sameClassAs(other);
        }

        @Override
        public int hashCode() {
            return 0;
        }
    }

    static class BlockingWeight extends Weight {
        final IntConsumer preVisit;

        BlockingWeight(Query query, IntConsumer preVisit) {
            super(query);
            this.preVisit = preVisit;
        }

        @Override
        public Explanation explain(LeafReaderContext context, int doc) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ScorerSupplier scorerSupplier(LeafReaderContext context) {
            int maxDoc = context.reader().maxDoc();
            DocIdSetIterator iterator = new DocIdSetIterator() {
                int docId = -1;

                @Override
                public int docID() {
                    return docId;
                }

                @Override
                public int nextDoc() throws IOException {
                    return advance(docId + 1);
                }

                @Override
                public int advance(int target) {
                    if (target < maxDoc) {
                        docId = target;
                        preVisit.accept(docId);
                    } else {
                        docId = DocIdSetIterator.NO_MORE_DOCS;
                    }
                    return docId;
                }

                @Override
                public long cost() {
                    return maxDoc;
                }
            };
            return new ScorerSupplier() {
                @Override
                public Scorer get(long leadCost) {
                    return new ConstantScoreScorer(1f, ScoreMode.COMPLETE_NO_SCORES, iterator);
                }

                @Override
                public long cost() {
                    return maxDoc;
                }
            };
        }

        @Override
        public boolean isCacheable(LeafReaderContext ctx) {
            return true;
        }
    }
}
