/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.cache.query.QueryCacheStats;
import org.elasticsearch.index.cache.query.TrivialQueryCachingPolicy;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class IndicesQueryCacheTests extends ESTestCase {

    private static class DummyQuery extends Query {

        private final int id;

        DummyQuery(int id) {
            this.id = id;
        }

        @Override
        public boolean equals(Object obj) {
            return sameClassAs(obj) && id == ((DummyQuery) obj).id;
        }

        @Override
        public int hashCode() {
            return 31 * classHash() + id;
        }

        @Override
        public void visit(QueryVisitor visitor) {
            visitor.visitLeaf(this);
        }

        @Override
        public String toString(String field) {
            return "dummy";
        }

        @Override
        public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
            return new ConstantScoreWeight(this, boost) {
                @Override
                public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                    Scorer scorer = new ConstantScoreScorer(score(), scoreMode, DocIdSetIterator.all(context.reader().maxDoc()));
                    return new DefaultScorerSupplier(scorer);
                }

                @Override
                public boolean isCacheable(LeafReaderContext ctx) {
                    return true;
                }
            };
        }

    }

    public void testBasics() throws IOException {
        Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
        w.addDocument(new Document());
        DirectoryReader r = DirectoryReader.open(w);
        w.close();
        ShardId shard = new ShardId("index", "_na_", 0);
        r = ElasticsearchDirectoryReader.wrap(r, shard);
        IndexSearcher s = new IndexSearcher(r);
        s.setQueryCachingPolicy(TrivialQueryCachingPolicy.ALWAYS);

        Settings settings = Settings.builder()
            .put(IndicesQueryCache.INDICES_CACHE_QUERY_COUNT_SETTING.getKey(), 10)
            .put(IndicesQueryCache.INDICES_QUERIES_CACHE_ALL_SEGMENTS_SETTING.getKey(), true)
            .build();
        IndicesQueryCache cache = new IndicesQueryCache(settings);
        s.setQueryCache(cache);

        QueryCacheStats stats = cache.getStats(shard);
        assertEquals(0L, stats.getCacheSize());
        assertEquals(0L, stats.getCacheCount());
        assertEquals(0L, stats.getHitCount());
        assertEquals(0L, stats.getMissCount());

        assertEquals(1, s.count(new DummyQuery(0)));

        stats = cache.getStats(shard);
        assertEquals(1L, stats.getCacheSize());
        assertEquals(1L, stats.getCacheCount());
        assertEquals(0L, stats.getHitCount());
        assertEquals(2L, stats.getMissCount());

        for (int i = 1; i < 20; ++i) {
            assertEquals(1, s.count(new DummyQuery(i)));
        }

        stats = cache.getStats(shard);
        assertEquals(10L, stats.getCacheSize());
        assertEquals(20L, stats.getCacheCount());
        assertEquals(0L, stats.getHitCount());
        assertEquals(40L, stats.getMissCount());

        s.count(new DummyQuery(10));

        stats = cache.getStats(shard);
        assertEquals(10L, stats.getCacheSize());
        assertEquals(20L, stats.getCacheCount());
        assertEquals(1L, stats.getHitCount());
        assertEquals(40L, stats.getMissCount());

        IOUtils.close(r, dir);

        // got emptied, but no changes to other metrics
        stats = cache.getStats(shard);
        assertEquals(0L, stats.getCacheSize());
        assertEquals(20L, stats.getCacheCount());
        assertEquals(1L, stats.getHitCount());
        assertEquals(40L, stats.getMissCount());

        cache.onClose(shard);

        // forgot everything
        stats = cache.getStats(shard);
        assertEquals(0L, stats.getCacheSize());
        assertEquals(0L, stats.getCacheCount());
        assertEquals(0L, stats.getHitCount());
        assertEquals(0L, stats.getMissCount());

        cache.close(); // this triggers some assertions
    }

    public void testTwoShards() throws IOException {
        Directory dir1 = newDirectory();
        IndexWriter w1 = new IndexWriter(dir1, newIndexWriterConfig());
        w1.addDocument(new Document());
        DirectoryReader r1 = DirectoryReader.open(w1);
        w1.close();
        ShardId shard1 = new ShardId("index", "_na_", 0);
        r1 = ElasticsearchDirectoryReader.wrap(r1, shard1);
        IndexSearcher s1 = new IndexSearcher(r1);
        s1.setQueryCachingPolicy(TrivialQueryCachingPolicy.ALWAYS);

        Directory dir2 = newDirectory();
        IndexWriter w2 = new IndexWriter(dir2, newIndexWriterConfig());
        w2.addDocument(new Document());
        DirectoryReader r2 = DirectoryReader.open(w2);
        w2.close();
        ShardId shard2 = new ShardId("index", "_na_", 1);
        r2 = ElasticsearchDirectoryReader.wrap(r2, shard2);
        IndexSearcher s2 = new IndexSearcher(r2);
        s2.setQueryCachingPolicy(TrivialQueryCachingPolicy.ALWAYS);

        Settings settings = Settings.builder()
            .put(IndicesQueryCache.INDICES_CACHE_QUERY_COUNT_SETTING.getKey(), 10)
            .put(IndicesQueryCache.INDICES_QUERIES_CACHE_ALL_SEGMENTS_SETTING.getKey(), true)
            .build();
        IndicesQueryCache cache = new IndicesQueryCache(settings);
        s1.setQueryCache(cache);
        s2.setQueryCache(cache);

        assertEquals(1, s1.count(new DummyQuery(0)));

        QueryCacheStats stats1 = cache.getStats(shard1);
        assertEquals(1L, stats1.getCacheSize());
        assertEquals(1L, stats1.getCacheCount());
        assertEquals(0L, stats1.getHitCount());
        assertEquals(2L, stats1.getMissCount());

        QueryCacheStats stats2 = cache.getStats(shard2);
        assertEquals(0L, stats2.getCacheSize());
        assertEquals(0L, stats2.getCacheCount());
        assertEquals(0L, stats2.getHitCount());
        assertEquals(0L, stats2.getMissCount());

        assertEquals(1, s2.count(new DummyQuery(0)));

        stats1 = cache.getStats(shard1);
        assertEquals(1L, stats1.getCacheSize());
        assertEquals(1L, stats1.getCacheCount());
        assertEquals(0L, stats1.getHitCount());
        assertEquals(2L, stats1.getMissCount());

        stats2 = cache.getStats(shard2);
        assertEquals(1L, stats2.getCacheSize());
        assertEquals(1L, stats2.getCacheCount());
        assertEquals(0L, stats2.getHitCount());
        assertEquals(2L, stats2.getMissCount());

        for (int i = 0; i < 20; ++i) {
            assertEquals(1, s2.count(new DummyQuery(i)));
        }

        stats1 = cache.getStats(shard1);
        assertEquals(0L, stats1.getCacheSize()); // evicted
        assertEquals(1L, stats1.getCacheCount());
        assertEquals(0L, stats1.getHitCount());
        assertEquals(2L, stats1.getMissCount());

        stats2 = cache.getStats(shard2);
        assertEquals(10L, stats2.getCacheSize());
        assertEquals(20L, stats2.getCacheCount());
        assertEquals(1L, stats2.getHitCount());
        assertEquals(40L, stats2.getMissCount());

        IOUtils.close(r1, dir1);

        // no changes
        stats1 = cache.getStats(shard1);
        assertEquals(0L, stats1.getCacheSize());
        assertEquals(1L, stats1.getCacheCount());
        assertEquals(0L, stats1.getHitCount());
        assertEquals(2L, stats1.getMissCount());

        stats2 = cache.getStats(shard2);
        assertEquals(10L, stats2.getCacheSize());
        assertEquals(20L, stats2.getCacheCount());
        assertEquals(1L, stats2.getHitCount());
        assertEquals(40L, stats2.getMissCount());

        cache.onClose(shard1);

        // forgot everything about shard1
        stats1 = cache.getStats(shard1);
        assertEquals(0L, stats1.getCacheSize());
        assertEquals(0L, stats1.getCacheCount());
        assertEquals(0L, stats1.getHitCount());
        assertEquals(0L, stats1.getMissCount());

        stats2 = cache.getStats(shard2);
        assertEquals(10L, stats2.getCacheSize());
        assertEquals(20L, stats2.getCacheCount());
        assertEquals(1L, stats2.getHitCount());
        assertEquals(40L, stats2.getMissCount());

        IOUtils.close(r2, dir2);
        cache.onClose(shard2);

        // forgot everything about shard2
        stats1 = cache.getStats(shard1);
        assertEquals(0L, stats1.getCacheSize());
        assertEquals(0L, stats1.getCacheCount());
        assertEquals(0L, stats1.getHitCount());
        assertEquals(0L, stats1.getMissCount());
        assertEquals(0L, stats1.getMemorySizeInBytes());

        stats2 = cache.getStats(shard2);
        assertEquals(0L, stats2.getCacheSize());
        assertEquals(0L, stats2.getCacheCount());
        assertEquals(0L, stats2.getHitCount());
        assertEquals(0L, stats2.getMissCount());
        assertEquals(0L, stats2.getMemorySizeInBytes());

        cache.close(); // this triggers some assertions
    }

    // Make sure the cache behaves correctly when a segment that is associated
    // with an empty cache gets closed. In that particular case, the eviction
    // callback is called with a number of evicted entries equal to 0
    // see https://github.com/elastic/elasticsearch/issues/15043
    public void testStatsOnEviction() throws IOException {
        Directory dir1 = newDirectory();
        IndexWriter w1 = new IndexWriter(dir1, newIndexWriterConfig());
        w1.addDocument(new Document());
        DirectoryReader r1 = DirectoryReader.open(w1);
        w1.close();
        ShardId shard1 = new ShardId("index", "_na_", 0);
        r1 = ElasticsearchDirectoryReader.wrap(r1, shard1);
        IndexSearcher s1 = newSearcher(r1, false);
        s1.setQueryCachingPolicy(TrivialQueryCachingPolicy.ALWAYS);

        Directory dir2 = newDirectory();
        IndexWriter w2 = new IndexWriter(dir2, newIndexWriterConfig());
        w2.addDocument(new Document());
        DirectoryReader r2 = DirectoryReader.open(w2);
        w2.close();
        ShardId shard2 = new ShardId("index", "_na_", 1);
        r2 = ElasticsearchDirectoryReader.wrap(r2, shard2);
        IndexSearcher s2 = newSearcher(r2, false);
        s2.setQueryCachingPolicy(TrivialQueryCachingPolicy.ALWAYS);

        Settings settings = Settings.builder()
            .put(IndicesQueryCache.INDICES_CACHE_QUERY_COUNT_SETTING.getKey(), 10)
            .put(IndicesQueryCache.INDICES_QUERIES_CACHE_ALL_SEGMENTS_SETTING.getKey(), true)
            .build();
        IndicesQueryCache cache = new IndicesQueryCache(settings);
        s1.setQueryCache(cache);
        s2.setQueryCache(cache);

        assertEquals(1, s1.count(new DummyQuery(0)));

        for (int i = 1; i <= 20; ++i) {
            assertEquals(1, s2.count(new DummyQuery(i)));
        }

        QueryCacheStats stats1 = cache.getStats(shard1);
        assertEquals(0L, stats1.getCacheSize());
        assertEquals(1L, stats1.getCacheCount());

        // this used to fail because we were evicting an empty cache on
        // the segment from r1
        IOUtils.close(r1, dir1);
        cache.onClose(shard1);

        IOUtils.close(r2, dir2);
        cache.onClose(shard2);

        cache.close(); // this triggers some assertions
    }

    private static class DummyWeight extends Weight {

        private final Weight weight;
        private boolean scorerCalled;
        private boolean scorerSupplierCalled;

        DummyWeight(Weight weight) {
            super(weight.getQuery());
            this.weight = weight;
        }

        @Override
        public Explanation explain(LeafReaderContext context, int doc) throws IOException {
            return weight.explain(context, doc);
        }

        @Override
        public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
            scorerSupplierCalled = true;
            ScorerSupplier inScorerSupplier = weight.scorerSupplier(context);
            return new ScorerSupplier() {
                @Override
                public Scorer get(long leadCost) throws IOException {
                    scorerCalled = true;
                    return inScorerSupplier.get(leadCost);
                }

                @Override
                public long cost() {
                    return inScorerSupplier.cost();
                }
            };
        }

        @Override
        public boolean isCacheable(LeafReaderContext ctx) {
            return true;
        }

    }

    public void testDelegatesScorerSupplier() throws Exception {
        Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
        w.addDocument(new Document());
        DirectoryReader r = DirectoryReader.open(w);
        w.close();
        ShardId shard = new ShardId("index", "_na_", 0);
        r = ElasticsearchDirectoryReader.wrap(r, shard);
        IndexSearcher s = newSearcher(r, false);
        s.setQueryCachingPolicy(TrivialQueryCachingPolicy.NEVER);

        Settings settings = Settings.builder()
            .put(IndicesQueryCache.INDICES_CACHE_QUERY_COUNT_SETTING.getKey(), 10)
            .put(IndicesQueryCache.INDICES_QUERIES_CACHE_ALL_SEGMENTS_SETTING.getKey(), true)
            .build();
        IndicesQueryCache cache = new IndicesQueryCache(settings);
        s.setQueryCache(cache);
        Query query = new MatchAllDocsQuery();
        final DummyWeight weight = new DummyWeight(s.createWeight(s.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1f));
        final Weight cached = cache.doCache(weight, s.getQueryCachingPolicy());
        assertNotSame(weight, cached);
        assertFalse(weight.scorerCalled);
        assertFalse(weight.scorerSupplierCalled);
        cached.scorerSupplier(s.getIndexReader().leaves().get(0));
        assertFalse(weight.scorerCalled);
        assertTrue(weight.scorerSupplierCalled);
        IOUtils.close(r, dir);
        cache.onClose(shard);
        cache.close();
    }
}
