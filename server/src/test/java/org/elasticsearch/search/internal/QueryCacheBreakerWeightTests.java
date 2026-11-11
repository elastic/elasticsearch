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
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryCache;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.cache.query.QueryCacheStats;
import org.elasticsearch.index.cache.query.TrivialQueryCachingPolicy;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesQueryCache;
import org.elasticsearch.indices.breaker.CircuitBreakerMetrics;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.lucene.search.cost.QueryCacheEntryCostEstimator;
import org.elasticsearch.search.internal.BreakerWeightTestUtils.CountingCollectorManager;
import org.elasticsearch.search.internal.BreakerWeightTestUtils.TrackingCircuitBreaker;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

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

    /** Records every weight handed to {@link #doCache} and caches nothing, so the test sees what the searcher gives the cache. */
    private static final class CapturingQueryCache implements QueryCache {
        private final List<Weight> weights = new ArrayList<>();

        @Override
        public Weight doCache(Weight weight, QueryCachingPolicy policy) {
            weights.add(weight);
            return weight;
        }
    }

    public void testSearcherHandsAccountingToTheCacheOnlyWithABreaker() throws IOException {
        CapturingQueryCache cache = new CapturingQueryCache();
        ContextIndexSearcher searcher = newSearcher(reader, cache);
        Query query = new TermQuery(new Term(FIELD, TERM));

        Weight unaccounted = searcher.createWeight(query, ScoreMode.COMPLETE_NO_SCORES, 1f);
        assertThat(cache.weights, contains(sameInstance(unaccounted)));
        assertThat("without a breaker there is nothing to charge", unaccounted, not(instanceOf(QueryCacheBreakerWeight.class)));
        assertFalse(searcher.hasLeafExecutionAccounting());

        searcher.setCircuitBreaker(new TrackingCircuitBreaker(-1L));
        Weight accounted = searcher.createWeight(query, ScoreMode.COMPLETE_NO_SCORES, 1f);
        assertThat(cache.weights, contains(sameInstance(unaccounted), sameInstance(accounted)));
        assertThat(accounted, instanceOf(QueryCacheBreakerWeight.class));
        assertThat("the cache key must not change", accounted.getQuery(), equalTo(query));
        assertTrue("accounting is created up front so that the charge is released with the leaf", searcher.hasLeafExecutionAccounting());

        Weight scoring = searcher.createWeight(query, ScoreMode.COMPLETE, 1f);
        assertThat("a scoring weight bypasses the cache and needs no accounting", scoring, not(instanceOf(QueryCacheBreakerWeight.class)));
        assertThat(cache.weights, contains(sameInstance(unaccounted), sameInstance(accounted)));
    }

    public void testChargeIsReservedUntilTheLeafIsDone() throws IOException {
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker(-1L);
        ContextIndexSearcher searcher = newSearcher(reader, null);
        searcher.setCircuitBreaker(breaker);
        Query query = new TermQuery(new Term(FIELD, TERM));
        QueryCacheBreakerWeight weight = new QueryCacheBreakerWeight(
            searcher,
            searcher.createWeight(query, ScoreMode.COMPLETE_NO_SCORES, 1f)
        );
        LeafReaderContext leaf = reader.leaves().get(0);

        assertTrue(weight.chargeCachePopulation(leaf, 1024L));
        assertThat(breaker.getUsed(), equalTo(1024L));
        assertThat(breaker.charges(), contains(1024L));

        // the search holds the entry it built until it is done with the leaf, so that is when searchLeaf releases the charge
        assertThat(searcher.search(query, new CountingCollectorManager()), equalTo(NUM_DOCS));
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testRefusedChargeIsReportedNotThrown() throws IOException {
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker(100L);
        ContextIndexSearcher searcher = newSearcher(reader, null);
        searcher.setCircuitBreaker(breaker);
        Query query = new TermQuery(new Term(FIELD, TERM));
        QueryCacheBreakerWeight weight = new QueryCacheBreakerWeight(
            searcher,
            searcher.createWeight(query, ScoreMode.COMPLETE_NO_SCORES, 1f)
        );
        LeafReaderContext leaf = reader.leaves().get(0);

        assertFalse("the cache runs the leaf uncached instead of failing the search", weight.chargeCachePopulation(leaf, 1000L));
        assertThat("a refused reservation must not leak onto the breaker", breaker.getUsed(), equalTo(0L));
        assertTrue("a smaller entry still fits", weight.chargeCachePopulation(leaf, 50L));
        assertThat(breaker.getUsed(), equalTo(50L));
    }

    public void testWithoutBreakerTheChargeIsAccepted() throws IOException {
        ContextIndexSearcher searcher = newSearcher(reader, null);
        Query query = new TermQuery(new Term(FIELD, TERM));
        QueryCacheBreakerWeight weight = new QueryCacheBreakerWeight(
            searcher,
            searcher.createWeight(query, ScoreMode.COMPLETE_NO_SCORES, 1f)
        );

        assertTrue("with nothing to charge, the cache populates as before", weight.chargeCachePopulation(reader.leaves().get(0), 1024L));
        assertFalse(searcher.hasLeafExecutionAccounting());
    }

    /**
     * The search that populates the cache reserves the entry's heap on its breaker for as long as it holds the entry, i.e. until it is
     * done with the leaf; a search served from the cache reserves nothing.
     */
    public void testPopulationIsChargedToTheRequestBreakerUntilTheLeafIsDone() throws IOException {
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker(-1L);
        ShardId shard = new ShardId("index", "_na_", 0);
        IndicesQueryCache cache = newIndicesQueryCache();
        try (DirectoryReader shardReader = ElasticsearchDirectoryReader.wrap(DirectoryReader.open(directory), shard)) {
            ContextIndexSearcher searcher = newSearcher(shardReader, cache);
            searcher.setCircuitBreaker(breaker);
            Query query = new TermQuery(new Term(FIELD, TERM));
            final long denseBytes = denseEntryBytes(shardReader);

            assertThat(searcher.search(query, new CountingCollectorManager()), equalTo(NUM_DOCS));
            assertThat("the entry is reserved at the size Lucene builds it", breaker.charges(), contains(denseBytes));
            assertThat(breaker.peak(), equalTo(denseBytes));
            assertThat("the reservation is released with the leaf", breaker.getUsed(), equalTo(0L));
            assertThat(cache.getStats(shard, () -> 0L).getCacheCount(), equalTo(1L));

            assertThat(searcher.search(query, new CountingCollectorManager()), equalTo(NUM_DOCS));
            assertThat("a cache hit builds and reserves nothing", breaker.charges(), contains(denseBytes));
            assertThat(cache.getStats(shard, () -> 0L).getHitCount(), equalTo(1L));
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
            CircuitBreaker breaker = newRequestBreaker(denseEntryBytes(shardReader) - 1);
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

    /** A real request breaker, so that a refused reservation is counted as a trip like in production. */
    private static CircuitBreaker newRequestBreaker(long limitBytes) {
        Settings settings = Settings.builder()
            .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), ByteSizeValue.ofBytes(limitBytes))
            .put(HierarchyCircuitBreakerService.USE_REAL_MEMORY_USAGE_SETTING.getKey(), false)
            .build();
        return new HierarchyCircuitBreakerService(
            CircuitBreakerMetrics.NOOP,
            settings,
            List.of(),
            new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        ).getBreaker(CircuitBreaker.REQUEST);
    }
}
