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
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryCache;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.LimitedBreaker;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.cache.query.QueryCacheStats;
import org.elasticsearch.index.cache.query.TrivialQueryCachingPolicy;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesQueryCache;
import org.elasticsearch.lucene.search.cost.QueryCacheEntryCostEstimator;
import org.elasticsearch.search.internal.BreakerWeightTestUtils.CountingCollectorManager;
import org.elasticsearch.search.internal.BreakerWeightTestUtils.TrackingCircuitBreaker;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;

public class QueryCacheBreakerWeightTests extends ESTestCase {

    private static final String FIELD = "k";
    private static final String TERM = "all";
    private static final int NUM_DOCS = 100;

    private Directory directory;
    private DirectoryReader reader;

    @Before
    public void initDirectoryAndReader() throws Exception {
        directory = newDirectory();
        try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig(null))) {
            for (int docId = 0; docId < NUM_DOCS; docId++) {
                Document doc = new Document();
                doc.add(new StringField(FIELD, TERM, Field.Store.NO));
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

    /**
     * A search that populates the cache reserves the entry's heap on its breaker, sized as Lucene builds the entry, and holds the
     * reservation until it is done with the leaf.
     */
    public void testPopulatingTheCacheChargesTheBreakerUntilTheSearchIsDone() throws IOException {
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker(-1L);
        ShardId shard = new ShardId("index", "_na_", 0);
        IndicesQueryCache cache = newIndicesQueryCache();
        try (DirectoryReader shardReader = ElasticsearchDirectoryReader.wrap(DirectoryReader.open(directory), shard)) {
            ContextIndexSearcher searcher = newSearcher(shardReader, cache);
            searcher.setCircuitBreaker(breaker);
            final long denseBytes = denseEntryBytes(shardReader);

            assertThat(searcher.search(new TermQuery(new Term(FIELD, TERM)), new CountingCollectorManager()), equalTo(NUM_DOCS));
            assertThat("the entry was cached", cache.getStats(shard, () -> 0L).getCacheCount(), equalTo(1L));
            assertThat("the entry was reserved at the size Lucene builds it", breaker.charges(), contains(denseBytes));
            assertThat("the reservation was held while the search ran", breaker.peak(), equalTo(denseBytes));
            assertThat("the reservation is released once the search is done", breaker.getUsed(), equalTo(0L));
        } finally {
            cache.onClose(shard);
            cache.close();
        }
    }

    /**
     * When the request breaker refuses the entry, the cache leaves the leaf uncached: the search still returns its hits, nothing is
     * left on the breaker or in the cache, and the refusal is visible as a trip in the breaker's stats.
     */
    public void testRefusedPopulationRunsTheLeafUncached() throws IOException {
        ShardId shard = new ShardId("index", "_na_", 0);
        IndicesQueryCache cache = newIndicesQueryCache();
        try (DirectoryReader shardReader = ElasticsearchDirectoryReader.wrap(DirectoryReader.open(directory), shard)) {
            LimitedBreaker breaker = new LimitedBreaker("request", ByteSizeValue.ofBytes(denseEntryBytes(shardReader) - 1));
            ContextIndexSearcher searcher = newSearcher(shardReader, cache);
            searcher.setCircuitBreaker(breaker);
            Query query = new TermQuery(new Term(FIELD, TERM));

            assertThat("the search succeeds without the cache", searcher.search(query, new CountingCollectorManager()), equalTo(NUM_DOCS));
            QueryCacheStats stats = cache.getStats(shard, () -> 0L);
            assertThat("the entry was not built", stats.getCacheCount(), equalTo(0L));
            assertThat(stats.getMissCount(), equalTo(1L));
            assertThat(breaker.getUsed(), equalTo(0L));
            assertThat(breaker.getTrippedCount(), equalTo(1L));
        } finally {
            cache.onClose(shard);
            cache.close();
        }
    }

    /**
     * {@link ConstantScoreQuery} without scores hands the searcher its child's weight, which the searcher has already cached, so the
     * searcher caches the same weight twice. {@link IndicesQueryCache#doCache} keeps a single cache layer in that case; the accounting
     * wrapper the searcher adds for the second round must not get in the way of that, or the clause would be looked up and built twice.
     */
    public void testCachingAnAlreadyCachedWeightKeepsOneCacheLayer() throws IOException {
        ShardId shard = new ShardId("index", "_na_", 0);
        IndicesQueryCache cache = newIndicesQueryCache();
        try (DirectoryReader shardReader = ElasticsearchDirectoryReader.wrap(DirectoryReader.open(directory), shard)) {
            ContextIndexSearcher searcher = newSearcher(shardReader, cache);
            searcher.setCircuitBreaker(new TrackingCircuitBreaker(-1L));

            Query query = new ConstantScoreQuery(new TermQuery(new Term(FIELD, TERM)));
            assertThat(searcher.search(query, new CountingCollectorManager()), equalTo(NUM_DOCS));

            QueryCacheStats stats = cache.getStats(shard, () -> 0L);
            assertThat(stats.getCacheCount(), equalTo(1L));
            assertThat("a single cache layer looks the clause up once", stats.getTotalCount(), equalTo(1L));
            assertThat(stats.getMissCount(), equalTo(1L));
        } finally {
            cache.onClose(shard);
            cache.close();
        }
    }

    private static ContextIndexSearcher newSearcher(DirectoryReader reader, QueryCache queryCache) throws IOException {
        return new ContextIndexSearcher(reader, IndexSearcher.getDefaultSimilarity(), queryCache, TrivialQueryCachingPolicy.ALWAYS, false);
    }

    /** A node query cache that caches the test's single small segment. */
    private static IndicesQueryCache newIndicesQueryCache() {
        return new IndicesQueryCache(
            Settings.builder().put(IndicesQueryCache.INDICES_QUERIES_CACHE_ALL_SEGMENTS_SETTING.getKey(), true).build()
        );
    }

    /** The heap the cache reserves for the test term, which matches every document and so gets a dense entry. */
    private static long denseEntryBytes(DirectoryReader reader) {
        int maxDoc = reader.leaves().get(0).reader().maxDoc();
        return new QueryCacheEntryCostEstimator(NUM_DOCS, maxDoc).estimate();
    }
}
