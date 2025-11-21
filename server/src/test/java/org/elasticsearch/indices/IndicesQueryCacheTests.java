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
import org.apache.lucene.util.Accountable;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.cache.query.QueryCacheStats;
import org.elasticsearch.index.cache.query.TrivialQueryCachingPolicy;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IndicesQueryCacheTests extends ESTestCase {

    private static class DummyQuery extends Query implements Accountable {

        private final String id;
        private final long sizeInCache;

        DummyQuery(int id) {
            this(Integer.toString(id), 10);
        }

        DummyQuery(String id, long sizeInCache) {
            this.id = id;
            this.sizeInCache = sizeInCache;
        }

        @Override
        public boolean equals(Object obj) {
            return sameClassAs(obj) && id.equals(((DummyQuery) obj).id);
        }

        @Override
        public int hashCode() {
            return 31 * classHash() + id.hashCode();
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
        public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
            return new ConstantScoreWeight(this, boost) {
                @Override
                public ScorerSupplier scorerSupplier(LeafReaderContext context) {
                    Scorer scorer = new ConstantScoreScorer(score(), scoreMode, DocIdSetIterator.all(context.reader().maxDoc()));
                    return new DefaultScorerSupplier(scorer);
                }

                @Override
                public boolean isCacheable(LeafReaderContext ctx) {
                    return true;
                }
            };
        }

        @Override
        public long ramBytesUsed() {
            return sizeInCache;
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

        QueryCacheStats stats = cache.getStats(shard, () -> 0L);
        assertEquals(0L, stats.getCacheSize());
        assertEquals(0L, stats.getCacheCount());
        assertEquals(0L, stats.getHitCount());
        assertEquals(0L, stats.getMissCount());

        assertEquals(1, s.count(new DummyQuery(0)));

        stats = cache.getStats(shard, () -> 0L);
        assertEquals(1L, stats.getCacheSize());
        assertEquals(1L, stats.getCacheCount());
        assertEquals(0L, stats.getHitCount());
        assertEquals(2L, stats.getMissCount());

        for (int i = 1; i < 20; ++i) {
            assertEquals(1, s.count(new DummyQuery(i)));
        }

        stats = cache.getStats(shard, () -> 0L);
        assertEquals(10L, stats.getCacheSize());
        assertEquals(20L, stats.getCacheCount());
        assertEquals(0L, stats.getHitCount());
        assertEquals(40L, stats.getMissCount());

        s.count(new DummyQuery(10));

        stats = cache.getStats(shard, () -> 0L);
        assertEquals(10L, stats.getCacheSize());
        assertEquals(20L, stats.getCacheCount());
        assertEquals(1L, stats.getHitCount());
        assertEquals(40L, stats.getMissCount());

        IOUtils.close(r, dir);

        // got emptied, but no changes to other metrics
        stats = cache.getStats(shard, () -> 0L);
        assertEquals(0L, stats.getCacheSize());
        assertEquals(20L, stats.getCacheCount());
        assertEquals(1L, stats.getHitCount());
        assertEquals(40L, stats.getMissCount());

        cache.onClose(shard);

        // forgot everything
        stats = cache.getStats(shard, () -> 0L);
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

        QueryCacheStats stats1 = cache.getStats(shard1, () -> 0L);
        assertEquals(1L, stats1.getCacheSize());
        assertEquals(1L, stats1.getCacheCount());
        assertEquals(0L, stats1.getHitCount());
        assertEquals(2L, stats1.getMissCount());

        QueryCacheStats stats2 = cache.getStats(shard2, () -> 0L);
        assertEquals(0L, stats2.getCacheSize());
        assertEquals(0L, stats2.getCacheCount());
        assertEquals(0L, stats2.getHitCount());
        assertEquals(0L, stats2.getMissCount());

        assertEquals(1, s2.count(new DummyQuery(0)));

        stats1 = cache.getStats(shard1, () -> 0L);
        assertEquals(1L, stats1.getCacheSize());
        assertEquals(1L, stats1.getCacheCount());
        assertEquals(0L, stats1.getHitCount());
        assertEquals(2L, stats1.getMissCount());

        stats2 = cache.getStats(shard2, () -> 0L);
        assertEquals(1L, stats2.getCacheSize());
        assertEquals(1L, stats2.getCacheCount());
        assertEquals(0L, stats2.getHitCount());
        assertEquals(2L, stats2.getMissCount());

        for (int i = 0; i < 20; ++i) {
            assertEquals(1, s2.count(new DummyQuery(i)));
        }

        stats1 = cache.getStats(shard1, () -> 0L);
        assertEquals(0L, stats1.getCacheSize()); // evicted
        assertEquals(1L, stats1.getCacheCount());
        assertEquals(0L, stats1.getHitCount());
        assertEquals(2L, stats1.getMissCount());

        stats2 = cache.getStats(shard2, () -> 0L);
        assertEquals(10L, stats2.getCacheSize());
        assertEquals(20L, stats2.getCacheCount());
        assertEquals(1L, stats2.getHitCount());
        assertEquals(40L, stats2.getMissCount());

        IOUtils.close(r1, dir1);

        // no changes
        stats1 = cache.getStats(shard1, () -> 0L);
        assertEquals(0L, stats1.getCacheSize());
        assertEquals(1L, stats1.getCacheCount());
        assertEquals(0L, stats1.getHitCount());
        assertEquals(2L, stats1.getMissCount());

        stats2 = cache.getStats(shard2, () -> 0L);
        assertEquals(10L, stats2.getCacheSize());
        assertEquals(20L, stats2.getCacheCount());
        assertEquals(1L, stats2.getHitCount());
        assertEquals(40L, stats2.getMissCount());

        cache.onClose(shard1);

        // forgot everything about shard1
        stats1 = cache.getStats(shard1, () -> 0L);
        assertEquals(0L, stats1.getCacheSize());
        assertEquals(0L, stats1.getCacheCount());
        assertEquals(0L, stats1.getHitCount());
        assertEquals(0L, stats1.getMissCount());

        stats2 = cache.getStats(shard2, () -> 0L);
        assertEquals(10L, stats2.getCacheSize());
        assertEquals(20L, stats2.getCacheCount());
        assertEquals(1L, stats2.getHitCount());
        assertEquals(40L, stats2.getMissCount());

        IOUtils.close(r2, dir2);
        cache.onClose(shard2);

        // forgot everything about shard2
        stats1 = cache.getStats(shard1, () -> 0L);
        assertEquals(0L, stats1.getCacheSize());
        assertEquals(0L, stats1.getCacheCount());
        assertEquals(0L, stats1.getHitCount());
        assertEquals(0L, stats1.getMissCount());
        assertEquals(0L, stats1.getMemorySizeInBytes());

        stats2 = cache.getStats(shard2, () -> 0L);
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

        QueryCacheStats stats1 = cache.getStats(shard1, () -> 0L);
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
        cached.scorerSupplier(s.getIndexReader().leaves().getFirst());
        assertFalse(weight.scorerCalled);
        assertTrue(weight.scorerSupplierCalled);
        IOUtils.close(r, dir);
        cache.onClose(shard);
        cache.close();
    }

    private IndicesService getTestIndicesService(List<Tuple<IndexShard, Long>> shardsAndSizes, long sharedRamBytesUsed) {
        IndicesQueryCache queryCache = mock(IndicesQueryCache.class);
        for (Tuple<IndexShard, Long> shardAndSize : shardsAndSizes) {
            when(queryCache.getCacheSizeForShard(shardAndSize.v1().shardId())).thenReturn(shardAndSize.v2());
        }
        when(queryCache.getSharedRamBytesUsed()).thenReturn(sharedRamBytesUsed);
        return getTestIndicesService(shardsAndSizes.stream().map(Tuple::v1).toList(), queryCache);
    }

    private IndicesService getTestIndicesService(List<IndexShard> shards, IndicesQueryCache queryCache) {
        IndexService indexService = mock(IndexService.class);
        when(indexService.iterator()).thenAnswer(ignored -> shards.iterator());
        IndicesService indicesService = mock(IndicesService.class);
        when(indicesService.iterator()).thenAnswer(ignored -> List.of(indexService).iterator());
        when(indicesService.getIndicesQueryCache()).thenReturn(queryCache);
        return indicesService;
    }

    public void testGetStatsMemory() throws Exception {
        /*
         * This test creates 2 shards, one with two segments and one with one. It makes unique queries against all 3 segments (so that each
         * query will be cached, up to the max cache size), and then asserts various things about the cache memory. Most importantly, it
         * asserts that the memory the cache attributes to each shard is proportional to the number of segment-queries for the shard in the
         * cache (and not to the number of documents in the query).
         */
        String indexName = randomIdentifier();
        String uuid = randomUUID();
        ShardId shard1 = new ShardId(indexName, uuid, 0);
        ShardId shard2 = new ShardId(indexName, uuid, 1);
        List<Closeable> closeableList = new ArrayList<>();
        // We're going to create 2 segments for shard1, and 1 segment for shard2:
        int shard1Segment1Docs = randomIntBetween(11, 1000);
        int shard1Segment2Docs = randomIntBetween(1, 10);
        int shard2Segment1Docs = randomIntBetween(1, 10);
        IndexSearcher shard1Segment1Searcher = initializeSegment(shard1, shard1Segment1Docs, closeableList);
        IndexSearcher shard1Segment2Searcher = initializeSegment(shard1, shard1Segment2Docs, closeableList);
        IndexSearcher shard2Searcher = initializeSegment(shard2, shard2Segment1Docs, closeableList);

        final int maxCacheSize = 200;
        Settings settings = Settings.builder()
            .put(IndicesQueryCache.INDICES_CACHE_QUERY_COUNT_SETTING.getKey(), maxCacheSize)
            .put(IndicesQueryCache.INDICES_QUERIES_CACHE_ALL_SEGMENTS_SETTING.getKey(), true)
            .build();
        IndicesQueryCache cache = new IndicesQueryCache(settings);
        shard1Segment1Searcher.setQueryCache(cache);
        shard1Segment2Searcher.setQueryCache(cache);
        shard2Searcher.setQueryCache(cache);

        assertEquals(0L, cache.getStats(shard1, () -> 0L).getMemorySizeInBytes());

        final long largeQuerySize = randomIntBetween(100, 1000);
        final long smallQuerySize = randomIntBetween(10, 50);

        final int shard1Queries = randomIntBetween(20, 50);
        final int shard2Queries = randomIntBetween(5, 10);

        for (int i = 0; i < shard1Queries; ++i) {
            shard1Segment1Searcher.count(new DummyQuery("ingest1-" + i, largeQuerySize));
        }
        IndexShard indexShard1 = mock(IndexShard.class);
        when(indexShard1.shardId()).thenReturn(shard1);
        IndexShard indexShard2 = mock(IndexShard.class);
        when(indexShard2.shardId()).thenReturn(shard2);
        long shard1Segment1CacheMemory = calculateActualCacheMemoryForSegment(shard1Queries, largeQuerySize, shard1Segment1Docs);

        {
            IndicesService indicesService = getTestIndicesService(List.of(indexShard1), cache);
            IndicesQueryCache.CacheTotals cacheTotals = IndicesQueryCache.getCacheTotalsForAllShards(indicesService);
            final long sharedRamSizeShard1 = cache.getSharedRamSizeForShard(shard1, cacheTotals);
            final long sharedRamSizeShard2 = cache.getSharedRamSizeForShard(shard2, cacheTotals);
            assertThat(cache.getStats(shard1, () -> sharedRamSizeShard1).getMemorySizeInBytes(), equalTo(shard1Segment1CacheMemory));
            assertThat(cache.getStats(shard2, () -> sharedRamSizeShard2).getMemorySizeInBytes(), equalTo(0L));
            for (int i = 0; i < shard2Queries; ++i) {
                shard2Searcher.count(new DummyQuery("ingest2-" + i, smallQuerySize));
            }
        }
        double shard1Segment1CacheMemoryShare;
        {
            /*
             * Now that we have cached some smaller things for shard2, the cache memory for shard1 has gone down. This is expected because
             * we report cache memory proportional to the number of segments for each shard, ignoring the number of documents or the actual
             * document sizes. Since the shard2 requests were smaller, the average cache memory size per segment has now gone down.
             */
            IndicesService indicesService = getTestIndicesService(List.of(indexShard1, indexShard2), cache);
            IndicesQueryCache.CacheTotals cacheTotals = IndicesQueryCache.getCacheTotalsForAllShards(indicesService);
            final long sharedRamSizeShard1 = cache.getSharedRamSizeForShard(shard1, cacheTotals);
            final long sharedRamSizeShard2 = cache.getSharedRamSizeForShard(shard2, cacheTotals);
            assertThat(cache.getStats(shard1, () -> sharedRamSizeShard1).getMemorySizeInBytes(), lessThan(shard1Segment1CacheMemory));
            long shard1CacheBytes = cache.getStats(shard1, () -> sharedRamSizeShard1).getMemorySizeInBytes();
            long shard2CacheBytes = cache.getStats(shard2, () -> sharedRamSizeShard2).getMemorySizeInBytes();
            long shard2Segment1CacheMemory = calculateActualCacheMemoryForSegment(shard2Queries, smallQuerySize, shard2Segment1Docs);

            long totalMemory = shard1Segment1CacheMemory + shard2Segment1CacheMemory;
            // Each shard has some fixed overhead that we need to account for:
            long shard1Overhead = calculateOverheadForSegment(shard1Queries, shard1Segment1Docs);
            long shard2Overhead = calculateOverheadForSegment(shard2Queries, shard2Segment1Docs);
            long totalMemoryMinusOverhead = totalMemory - (shard1Overhead + shard2Overhead);
            /*
             * Note that the expected amount of memory we're calculating is based on the proportion of the number of queries to each shard
             * (since each shard currently only has queries to one segment)
             */
            shard1Segment1CacheMemoryShare = ((double) shard1Queries / (shard1Queries + shard2Queries)) * (totalMemoryMinusOverhead)
                + shard1Overhead;
            double shard2Segment1CacheMemoryShare = ((double) shard2Queries / (shard1Queries + shard2Queries)) * (totalMemoryMinusOverhead)
                + shard2Overhead;
            assertThat((double) shard1CacheBytes, closeTo(shard1Segment1CacheMemoryShare, 1)); // accounting for rounding
            assertThat((double) shard2CacheBytes, closeTo(shard2Segment1CacheMemoryShare, 1)); // accounting for rounding

            // Now we cache just more "big" searches on shard1, but on a different segment:
            for (int i = 0; i < shard1Queries; ++i) {
                shard1Segment2Searcher.count(new DummyQuery("ingest3-" + i, largeQuerySize));
            }
            long shard1Segment2CacheMemory = calculateActualCacheMemoryForSegment(shard1Queries, largeQuerySize, shard1Segment2Docs);
            totalMemory = shard1Segment1CacheMemory + shard2Segment1CacheMemory + shard1Segment2CacheMemory;
            // Each shard has some fixed overhead that we need to account for:
            shard1Overhead = shard1Overhead + calculateOverheadForSegment(shard1Queries, shard1Segment2Docs);
            totalMemoryMinusOverhead = totalMemory - (shard1Overhead + shard2Overhead);
            /*
             * Note that the expected amount of memory we're calculating is based on the proportion of the number of queries to each
             * segment. The number of documents and the size of documents is irrelevant (aside from computing the fixed overhead).
             */
            shard1Segment1CacheMemoryShare = ((double) (2 * shard1Queries) / ((2 * shard1Queries) + shard2Queries))
                * (totalMemoryMinusOverhead) + shard1Overhead;
        }
        {
            IndicesService indicesService = getTestIndicesService(List.of(indexShard1, indexShard2), cache);
            IndicesQueryCache.CacheTotals cacheTotals = IndicesQueryCache.getCacheTotalsForAllShards(indicesService);
            final long sharedRamSizeShard1 = cache.getSharedRamSizeForShard(shard1, cacheTotals);
            final long shard1CacheBytes = cache.getStats(shard1, () -> sharedRamSizeShard1).getMemorySizeInBytes();
            assertThat((double) shard1CacheBytes, closeTo(shard1Segment1CacheMemoryShare, 1)); // accounting for rounding
        }
        {
            // Now make sure the cache only has items for shard2:
            for (int i = 0; i < (maxCacheSize * 2); ++i) {
                shard2Searcher.count(new DummyQuery("ingest4-" + i, smallQuerySize));
            }
            IndicesService indicesService = getTestIndicesService(List.of(indexShard2), cache);
            IndicesQueryCache.CacheTotals cacheTotals = IndicesQueryCache.getCacheTotalsForAllShards(indicesService);
            final long sharedRamSizeShard1 = cache.getSharedRamSizeForShard(shard1, cacheTotals);
            final long sharedRamSizeShard2 = cache.getSharedRamSizeForShard(shard2, cacheTotals);
            assertThat(cache.getStats(shard1, () -> sharedRamSizeShard1).getMemorySizeInBytes(), equalTo(0L));
            assertThat(
                cache.getStats(shard2, () -> sharedRamSizeShard2).getMemorySizeInBytes(),
                equalTo(calculateActualCacheMemoryForSegment(maxCacheSize, smallQuerySize, shard2Segment1Docs))
            );
        }

        IOUtils.close(closeableList);
        cache.onClose(shard1);
        cache.onClose(shard2);
        cache.close();
    }

    /*
     * This calculates the memory that actually used by a segment in the IndicesQueryCache. It assumes queryCount queries are made to the
     * segment, and query is querySize bytes in size. It assumes that the shard contains numDocs documents.
     */
    private long calculateActualCacheMemoryForSegment(long queryCount, long querySize, long numDocs) {
        return (queryCount * (querySize + 24)) + calculateOverheadForSegment(queryCount, numDocs);
    }

    /*
     * This computes the part of the recorded IndicesQueryCache memory that is assigned to a segment and *not* divided up proportionally
     * when the cache reports the memory usage of each shard.
     */
    private long calculateOverheadForSegment(long queryCount, long numDocs) {
        return queryCount * (112 + (8 * ((numDocs - 1) / 64)));
    }

    /*
     * This returns an IndexSearcher for a single new segment in the given shard.
     */
    private IndexSearcher initializeSegment(ShardId shard, int numDocs, List<Closeable> closeableList) throws Exception {
        AtomicReference<IndexSearcher> indexSearcherReference = new AtomicReference<>();
        /*
         * Usually creating an IndexWriter like this results in a single segment getting created, but sometimes it results in more. For the
         * sake of keeping the calculations in this test simple we want just a single segment. So we do this in an assertBusy.
         */
        assertBusy(() -> {
            Directory dir = newDirectory();
            IndexWriter indexWriter = new IndexWriter(dir, newIndexWriterConfig());
            for (int i = 0; i < numDocs; i++) {
                indexWriter.addDocument(new Document());
            }
            DirectoryReader directoryReader = DirectoryReader.open(indexWriter);
            indexWriter.close();
            directoryReader = ElasticsearchDirectoryReader.wrap(directoryReader, shard);
            IndexSearcher indexSearcher = new IndexSearcher(directoryReader);
            indexSearcherReference.set(indexSearcher);
            indexSearcher.setQueryCachingPolicy(TrivialQueryCachingPolicy.ALWAYS);
            closeableList.add(directoryReader);
            closeableList.add(dir);
            assertThat(indexSearcher.getLeafContexts().size(), equalTo(1));
        });
        return indexSearcherReference.get();
    }
}
