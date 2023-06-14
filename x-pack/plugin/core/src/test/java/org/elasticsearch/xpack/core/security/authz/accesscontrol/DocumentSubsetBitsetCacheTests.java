/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.accesscontrol;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BitSet;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.MockFieldMapper;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.test.MockLogAppender;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mockito;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DocumentSubsetBitsetCacheTests extends ESTestCase {

    private static final int FIELD_COUNT = 10;
    // This value is based on the internal implementation details of lucene's FixedBitSet
    // If the implementation changes, this can be safely updated to match the new ram usage for a single bitset
    private static final long EXPECTED_BYTES_PER_BIT_SET = 56;
    private ExecutorService singleThreadExecutor;

    @Before
    public void setUpExecutor() {
        singleThreadExecutor = Executors.newSingleThreadExecutor();
    }

    @After
    public void cleanUpExecutor() {
        singleThreadExecutor.shutdown();
    }

    public void testSameBitSetIsReturnedForIdenticalQuery() throws Exception {
        final DocumentSubsetBitsetCache cache = newCache(Settings.EMPTY);
        runTestOnIndex((searchExecutionContext, leafContext) -> {
            final Query query1 = QueryBuilders.termQuery("field-1", "value-1").toQuery(searchExecutionContext);
            final BitSet bitSet1 = cache.getBitSet(query1, leafContext);
            assertThat(bitSet1, notNullValue());

            final Query query2 = QueryBuilders.termQuery("field-1", "value-1").toQuery(searchExecutionContext);
            final BitSet bitSet2 = cache.getBitSet(query2, leafContext);
            assertThat(bitSet2, notNullValue());

            assertThat(bitSet2, Matchers.sameInstance(bitSet1));
        });
    }

    public void testNullBitSetIsReturnedForNonMatchingQuery() throws Exception {
        final DocumentSubsetBitsetCache cache = newCache(Settings.EMPTY);
        runTestOnIndex((searchExecutionContext, leafContext) -> {
            final Query query = QueryBuilders.termQuery("not-mapped", "any-value")
                .rewrite(searchExecutionContext)
                .toQuery(searchExecutionContext);
            final BitSet bitSet = cache.getBitSet(query, leafContext);
            assertThat(bitSet, nullValue());
        });
    }

    public void testNullEntriesAreNotCountedInMemoryUsage() throws Exception {
        final DocumentSubsetBitsetCache cache = newCache(Settings.EMPTY);
        assertThat(cache.ramBytesUsed(), equalTo(0L));

        runTestOnIndex((searchExecutionContext, leafContext) -> {
            for (int i = 1; i <= randomIntBetween(3, 6); i++) {
                final Query query = QueryBuilders.termQuery("dne-" + i, "dne- " + i).toQuery(searchExecutionContext);
                final BitSet bitSet = cache.getBitSet(query, leafContext);
                assertThat(bitSet, nullValue());
                assertThat(cache.ramBytesUsed(), equalTo(0L));
                assertThat(cache.entryCount(), equalTo(i));
            }
        });
    }

    public void testCacheRespectsMemoryLimit() throws Exception {
        // Enough to hold exactly 2 bit-sets in the cache
        final long maxCacheBytes = EXPECTED_BYTES_PER_BIT_SET * 2;
        final Settings settings = Settings.builder()
            .put(DocumentSubsetBitsetCache.CACHE_SIZE_SETTING.getKey(), maxCacheBytes + "b")
            .build();
        final DocumentSubsetBitsetCache cache = newCache(settings);
        assertThat(cache.entryCount(), equalTo(0));
        assertThat(cache.ramBytesUsed(), equalTo(0L));

        runTestOnIndex((searchExecutionContext, leafContext) -> {
            Query previousQuery = null;
            BitSet previousBitSet = null;
            for (int i = 1; i <= 5; i++) {
                final TermQueryBuilder queryBuilder = QueryBuilders.termQuery("field-" + i, "value-" + i);
                final Query query = queryBuilder.toQuery(searchExecutionContext);
                final BitSet bitSet = cache.getBitSet(query, leafContext);
                assertThat(bitSet, notNullValue());
                assertThat(bitSet.ramBytesUsed(), equalTo(EXPECTED_BYTES_PER_BIT_SET));

                // The first time through we have 1 entry, after that we have 2
                final int expectedCount = i == 1 ? 1 : 2;
                assertThat(cache.entryCount(), equalTo(expectedCount));
                assertThat(cache.ramBytesUsed(), equalTo(expectedCount * EXPECTED_BYTES_PER_BIT_SET));

                // Older queries should get evicted, but the query from last iteration should still be cached
                if (previousQuery != null) {
                    assertThat(cache.getBitSet(previousQuery, leafContext), sameInstance(previousBitSet));
                    assertThat(cache.entryCount(), equalTo(expectedCount));
                    assertThat(cache.ramBytesUsed(), equalTo(expectedCount * EXPECTED_BYTES_PER_BIT_SET));
                }
                previousQuery = query;
                previousBitSet = bitSet;

                assertThat(cache.getBitSet(queryBuilder.toQuery(searchExecutionContext), leafContext), sameInstance(bitSet));
                assertThat(cache.entryCount(), equalTo(expectedCount));
                assertThat(cache.ramBytesUsed(), equalTo(expectedCount * EXPECTED_BYTES_PER_BIT_SET));
            }

            assertThat(cache.entryCount(), equalTo(2));
            assertThat(cache.ramBytesUsed(), equalTo(2 * EXPECTED_BYTES_PER_BIT_SET));

            cache.clear("testing");

            assertThat(cache.entryCount(), equalTo(0));
            assertThat(cache.ramBytesUsed(), equalTo(0L));
        });
    }

    public void testLogWarningIfBitSetExceedsCacheSize() throws Exception {
        // Enough to hold less than 1 bit-sets in the cache
        final long maxCacheBytes = EXPECTED_BYTES_PER_BIT_SET - EXPECTED_BYTES_PER_BIT_SET / 3;
        final Settings settings = Settings.builder()
            .put(DocumentSubsetBitsetCache.CACHE_SIZE_SETTING.getKey(), maxCacheBytes + "b")
            .build();
        final DocumentSubsetBitsetCache cache = newCache(settings);
        assertThat(cache.entryCount(), equalTo(0));
        assertThat(cache.ramBytesUsed(), equalTo(0L));

        final Logger cacheLogger = LogManager.getLogger(cache.getClass());
        final MockLogAppender mockAppender = new MockLogAppender();
        mockAppender.start();
        try {
            Loggers.addAppender(cacheLogger, mockAppender);
            mockAppender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "[bitset too big]",
                    cache.getClass().getName(),
                    Level.WARN,
                    "built a DLS BitSet that uses ["
                        + EXPECTED_BYTES_PER_BIT_SET
                        + "] bytes; the DLS BitSet cache has a maximum size of ["
                        + maxCacheBytes
                        + "] bytes; this object cannot be cached and will need to be rebuilt for each use;"
                        + " consider increasing the value of [xpack.security.dls.bitset.cache.size]"
                )
            );

            runTestOnIndex((searchExecutionContext, leafContext) -> {
                final TermQueryBuilder queryBuilder = QueryBuilders.termQuery("field-1", "value-1");
                final Query query = queryBuilder.toQuery(searchExecutionContext);
                final BitSet bitSet = cache.getBitSet(query, leafContext);
                assertThat(bitSet, notNullValue());
                assertThat(bitSet.ramBytesUsed(), equalTo(EXPECTED_BYTES_PER_BIT_SET));
            });

            mockAppender.assertAllExpectationsMatched();
        } finally {
            Loggers.removeAppender(cacheLogger, mockAppender);
            mockAppender.stop();
        }
    }

    public void testLogMessageIfCacheFull() throws Exception {
        // Enough to hold slightly more than 1 bit-sets in the cache
        final long maxCacheBytes = EXPECTED_BYTES_PER_BIT_SET + EXPECTED_BYTES_PER_BIT_SET / 3;
        final Settings settings = Settings.builder()
            .put(DocumentSubsetBitsetCache.CACHE_SIZE_SETTING.getKey(), maxCacheBytes + "b")
            .build();
        final DocumentSubsetBitsetCache cache = newCache(settings);
        assertThat(cache.entryCount(), equalTo(0));
        assertThat(cache.ramBytesUsed(), equalTo(0L));

        final Logger cacheLogger = LogManager.getLogger(cache.getClass());
        final MockLogAppender mockAppender = new MockLogAppender();
        mockAppender.start();
        try {
            Loggers.addAppender(cacheLogger, mockAppender);
            mockAppender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "[cache full]",
                    cache.getClass().getName(),
                    Level.INFO,
                    "the Document Level Security BitSet cache is full which may impact performance;"
                        + " consider increasing the value of [xpack.security.dls.bitset.cache.size]"
                )
            );

            runTestOnIndex((searchExecutionContext, leafContext) -> {
                for (int i = 1; i <= 3; i++) {
                    final TermQueryBuilder queryBuilder = QueryBuilders.termQuery("field-" + i, "value-" + i);
                    final Query query = queryBuilder.toQuery(searchExecutionContext);
                    final BitSet bitSet = cache.getBitSet(query, leafContext);
                    assertThat(bitSet, notNullValue());
                    assertThat(bitSet.ramBytesUsed(), equalTo(EXPECTED_BYTES_PER_BIT_SET));
                }
            });

            mockAppender.assertAllExpectationsMatched();
        } finally {
            Loggers.removeAppender(cacheLogger, mockAppender);
            mockAppender.stop();
        }
    }

    public void testCacheRespectsAccessTimeExpiry() throws Exception {
        final Settings settings = Settings.builder().put(DocumentSubsetBitsetCache.CACHE_TTL_SETTING.getKey(), "10ms").build();
        final DocumentSubsetBitsetCache cache = newCache(settings);
        assertThat(cache.entryCount(), equalTo(0));
        assertThat(cache.ramBytesUsed(), equalTo(0L));

        runTestOnIndex((searchExecutionContext, leafContext) -> {
            final Query query1 = QueryBuilders.termQuery("field-1", "value-1").toQuery(searchExecutionContext);
            final BitSet bitSet1 = cache.getBitSet(query1, leafContext);
            assertThat(bitSet1, notNullValue());

            final Query query2 = QueryBuilders.termQuery("field-2", "value-2").toQuery(searchExecutionContext);
            assertBusy(() -> {
                // Force the cache to perform eviction
                final BitSet bitSet2 = cache.getBitSet(query2, leafContext);
                assertThat(bitSet2, notNullValue());

                // Loop until the cache has less than 2 items, which mean that something we evicted
                assertThat(cache.entryCount(), Matchers.lessThan(2));

            }, 100, TimeUnit.MILLISECONDS);

            // Check that the original bitset is no longer in the cache (a new instance is returned)
            assertThat(cache.getBitSet(query1, leafContext), not(sameInstance(bitSet1)));
        });
    }

    public void testIndexLookupIsClearedWhenBitSetIsEvicted() throws Exception {
        // Enough to hold slightly more than 1 bit-set in the cache
        final long maxCacheBytes = EXPECTED_BYTES_PER_BIT_SET + EXPECTED_BYTES_PER_BIT_SET / 2;
        final Settings settings = Settings.builder()
            .put(DocumentSubsetBitsetCache.CACHE_SIZE_SETTING.getKey(), maxCacheBytes + "b")
            .build();

        final ExecutorService executor = mock(ExecutorService.class);
        final AtomicReference<Runnable> runnableRef = new AtomicReference<>();
        when(executor.submit(any(Runnable.class))).thenAnswer(inv -> {
            final Runnable r = (Runnable) inv.getArguments()[0];
            runnableRef.set(r);
            return null;
        });

        final DocumentSubsetBitsetCache cache = new DocumentSubsetBitsetCache(settings, executor);
        assertThat(cache.entryCount(), equalTo(0));
        assertThat(cache.ramBytesUsed(), equalTo(0L));

        runTestOnIndex((searchExecutionContext, leafContext) -> {
            final Query query1 = QueryBuilders.termQuery("field-1", "value-1").toQuery(searchExecutionContext);
            final BitSet bitSet1 = cache.getBitSet(query1, leafContext);
            assertThat(bitSet1, notNullValue());

            final Query query2 = QueryBuilders.termQuery("field-2", "value-2").toQuery(searchExecutionContext);
            final BitSet bitSet2 = cache.getBitSet(query2, leafContext);
            assertThat(bitSet2, notNullValue());

            // BitSet1 has been evicted now, run the cleanup...
            final Runnable runnable1 = runnableRef.get();
            assertThat(runnable1, notNullValue());
            runnable1.run();
            cache.verifyInternalConsistency();

            // Check that the original bitset is no longer in the cache (a new instance is returned)
            assertThat(cache.getBitSet(query1, leafContext), not(sameInstance(bitSet1)));

            // BitSet2 has been evicted now, run the cleanup...
            final Runnable runnable2 = runnableRef.get();
            assertThat(runnable2, not(sameInstance(runnable1)));
            runnable2.run();
            cache.verifyInternalConsistency();
        });
    }

    public void testCacheUnderConcurrentAccess() throws Exception {
        final int concurrentThreads = randomIntBetween(5, 8);
        final int numberOfIndices = randomIntBetween(3, 8);

        // Force cache evictions by setting the size to be less than the number of distinct queries we search on.
        final int maxCacheCount = randomIntBetween(FIELD_COUNT / 2, FIELD_COUNT * 3 / 4);
        final long maxCacheBytes = EXPECTED_BYTES_PER_BIT_SET * maxCacheCount;
        final Settings settings = Settings.builder()
            .put(DocumentSubsetBitsetCache.CACHE_SIZE_SETTING.getKey(), maxCacheBytes + "b")
            .build();

        final ExecutorService threads = Executors.newFixedThreadPool(concurrentThreads + 1);
        final ExecutorService cleanupExecutor = Mockito.mock(ExecutorService.class);
        when(cleanupExecutor.submit(any(Runnable.class))).thenAnswer(inv -> {
            final Runnable runnable = (Runnable) inv.getArguments()[0];
            return threads.submit(() -> {
                // Sleep for a small (random) length of time.
                // This increases the likelihood that cache could have been modified between the eviction & the cleanup
                Thread.sleep(randomIntBetween(1, 10));
                runnable.run();
                return null;
            });
        });
        try {
            final DocumentSubsetBitsetCache cache = new DocumentSubsetBitsetCache(settings, cleanupExecutor);
            assertThat(cache.entryCount(), equalTo(0));
            assertThat(cache.ramBytesUsed(), equalTo(0L));

            runTestOnIndices(numberOfIndices, contexts -> {
                final CountDownLatch start = new CountDownLatch(concurrentThreads);
                final CountDownLatch end = new CountDownLatch(concurrentThreads);
                final Set<BitSet> uniqueBitSets = Collections.synchronizedSet(Collections.newSetFromMap(new IdentityHashMap<>()));
                final AtomicReference<Throwable> exceptionInThread = new AtomicReference<>();
                for (int thread = 0; thread < concurrentThreads; thread++) {
                    threads.submit(() -> {
                        try {
                            start.countDown();
                            if (false == start.await(100, TimeUnit.MILLISECONDS)) {
                                // We still proceed even when some threads are not ready. All threads being ready increases the chance
                                // of them running concurrently and competing for caching. But this is not guaranteed either way.
                                logger.info("[{}] out of [{}] worker threads are ready", start.getCount(), concurrentThreads);
                            }
                            for (int loop = 0; loop < 5; loop++) {
                                for (int field = 1; field <= FIELD_COUNT; field++) {
                                    final TermQueryBuilder queryBuilder = QueryBuilders.termQuery("field-" + field, "value-" + field);
                                    final TestIndexContext randomContext = randomFrom(contexts);
                                    final Query query = queryBuilder.toQuery(randomContext.searchExecutionContext);
                                    final BitSet bitSet = cache.getBitSet(query, randomContext.leafReaderContext);
                                    assertThat(bitSet, notNullValue());
                                    assertThat(bitSet.ramBytesUsed(), equalTo(EXPECTED_BYTES_PER_BIT_SET));
                                    uniqueBitSets.add(bitSet);
                                }
                            }
                            end.countDown();
                        } catch (Throwable e) {
                            logger.warn("caught exception in worker thread", e);
                            exceptionInThread.compareAndSet(null, e);
                        }
                        return null;
                    });
                }

                if (false == end.await(1, TimeUnit.SECONDS)) {
                    fail("Query threads did not complete in expected time. Possible exception [" + exceptionInThread.get() + "]");
                }

                threads.shutdown();
                assertTrue("Cleanup thread did not complete in expected time", threads.awaitTermination(3, TimeUnit.SECONDS));
                cache.verifyInternalConsistency();

                // Due to cache evictions, we must get more bitsets than fields
                assertThat(uniqueBitSets.size(), Matchers.greaterThan(FIELD_COUNT));
                // Due to cache evictions, we must have seen more bitsets than the cache currently holds
                assertThat(uniqueBitSets.size(), Matchers.greaterThan(cache.entryCount()));
                // Even under concurrent pressure, the cache should hit the expected size
                assertThat(cache.entryCount(), is(maxCacheCount));
                assertThat(cache.ramBytesUsed(), is(maxCacheBytes));
            });
        } finally {
            threads.shutdown();
        }
    }

    public void testCleanupWorksWhenIndexIsClosing() throws Exception {
        // Enough to hold slightly more than 1 bit-set in the cache
        final long maxCacheBytes = EXPECTED_BYTES_PER_BIT_SET + EXPECTED_BYTES_PER_BIT_SET / 2;
        final Settings settings = Settings.builder()
            .put(DocumentSubsetBitsetCache.CACHE_SIZE_SETTING.getKey(), maxCacheBytes + "b")
            .build();
        final ExecutorService threads = Executors.newFixedThreadPool(1);
        final ExecutorService cleanupExecutor = Mockito.mock(ExecutorService.class);
        final CountDownLatch cleanupReadyLatch = new CountDownLatch(1);
        final CountDownLatch cleanupCompleteLatch = new CountDownLatch(1);
        final CountDownLatch indexCloseLatch = new CountDownLatch(1);
        final AtomicReference<Throwable> cleanupException = new AtomicReference<>();
        when(cleanupExecutor.submit(any(Runnable.class))).thenAnswer(inv -> {
            final Runnable runnable = (Runnable) inv.getArguments()[0];
            return threads.submit(() -> {
                try {
                    cleanupReadyLatch.countDown();
                    assertTrue("index close did not completed in expected time", indexCloseLatch.await(1, TimeUnit.SECONDS));
                    runnable.run();
                } catch (Throwable e) {
                    logger.warn("caught error in cleanup thread", e);
                    cleanupException.compareAndSet(null, e);
                } finally {
                    cleanupCompleteLatch.countDown();
                }
                return null;
            });
        });

        final DocumentSubsetBitsetCache cache = new DocumentSubsetBitsetCache(settings, cleanupExecutor);
        assertThat(cache.entryCount(), equalTo(0));
        assertThat(cache.ramBytesUsed(), equalTo(0L));

        try {
            runTestOnIndex((searchExecutionContext, leafContext) -> {
                final Query query1 = QueryBuilders.termQuery("field-1", "value-1").toQuery(searchExecutionContext);
                final BitSet bitSet1 = cache.getBitSet(query1, leafContext);
                assertThat(bitSet1, notNullValue());

                // Second query should trigger a cache eviction
                final Query query2 = QueryBuilders.termQuery("field-2", "value-2").toQuery(searchExecutionContext);
                final BitSet bitSet2 = cache.getBitSet(query2, leafContext);
                assertThat(bitSet2, notNullValue());

                final IndexReader.CacheKey indexKey = leafContext.reader().getCoreCacheHelper().getKey();
                assertTrue("cleanup did not trigger in expected time", cleanupReadyLatch.await(1, TimeUnit.SECONDS));
                cache.onClose(indexKey);
                indexCloseLatch.countDown();
                assertTrue("cleanup did not complete in expected time", cleanupCompleteLatch.await(1, TimeUnit.SECONDS));
                assertThat("caught error in cleanup thread: " + cleanupException.get(), cleanupException.get(), nullValue());
            });
        } finally {
            threads.shutdown();
        }
    }

    public void testCacheIsPerIndex() throws Exception {
        final DocumentSubsetBitsetCache cache = newCache(Settings.EMPTY);
        assertThat(cache.entryCount(), equalTo(0));
        assertThat(cache.ramBytesUsed(), equalTo(0L));

        final int iterations = randomIntBetween(3, 10);
        AtomicInteger counter = new AtomicInteger(0);

        final CheckedBiConsumer<SearchExecutionContext, LeafReaderContext, Exception> consumer = new CheckedBiConsumer<>() {
            @Override
            public void accept(SearchExecutionContext searchExecutionContext, LeafReaderContext leafContext) throws Exception {
                final int count = counter.incrementAndGet();
                final Query query = QueryBuilders.termQuery("field-1", "value-1").toQuery(searchExecutionContext);
                final BitSet bitSet = cache.getBitSet(query, leafContext);

                assertThat(bitSet, notNullValue());
                assertThat(cache.entryCount(), equalTo(count));

                if (count < iterations) {
                    // Need to do this nested, or else the cache will be cleared when the index reader is closed
                    runTestOnIndex(this);
                }
            }
        };
        runTestOnIndex(consumer);
    }

    public void testCacheClearEntriesWhenIndexIsClosed() throws Exception {
        final DocumentSubsetBitsetCache cache = newCache(Settings.EMPTY);
        assertThat(cache.entryCount(), equalTo(0));
        assertThat(cache.ramBytesUsed(), equalTo(0L));

        for (int i = 1; i <= randomIntBetween(2, 5); i++) {
            runTestOnIndex((searchExecutionContext, leafContext) -> {
                for (int j = 1; j <= randomIntBetween(2, 10); j++) {
                    final Query query = QueryBuilders.termQuery("field-" + j, "value-1").toQuery(searchExecutionContext);
                    final BitSet bitSet = cache.getBitSet(query, leafContext);
                    assertThat(bitSet, notNullValue());
                }
                assertThat(cache.entryCount(), not(equalTo(0)));
                assertThat(cache.ramBytesUsed(), not(equalTo(0L)));
            });
            assertThat(cache.entryCount(), equalTo(0));
            assertThat(cache.ramBytesUsed(), equalTo(0L));
        }
    }

    public void testEquivalentMatchAllDocsQuery() {
        assertTrue(DocumentSubsetBitsetCache.isEffectiveMatchAllDocsQuery(new MatchAllDocsQuery()));
        assertTrue(DocumentSubsetBitsetCache.isEffectiveMatchAllDocsQuery(new ConstantScoreQuery(new MatchAllDocsQuery())));
        assertFalse(DocumentSubsetBitsetCache.isEffectiveMatchAllDocsQuery(new TermQuery(new Term("term"))));
    }

    private void runTestOnIndex(CheckedBiConsumer<SearchExecutionContext, LeafReaderContext, Exception> body) throws Exception {
        runTestOnIndices(1, ctx -> {
            final TestIndexContext indexContext = ctx.get(0);
            body.accept(indexContext.searchExecutionContext, indexContext.leafReaderContext);
        });
    }

    private static final class TestIndexContext implements Closeable {
        private final Directory directory;
        private final IndexWriter indexWriter;
        private final DirectoryReader directoryReader;
        private final SearchExecutionContext searchExecutionContext;
        private final LeafReaderContext leafReaderContext;

        private TestIndexContext(
            Directory directory,
            IndexWriter indexWriter,
            DirectoryReader directoryReader,
            SearchExecutionContext searchExecutionContext,
            LeafReaderContext leafReaderContext
        ) {
            this.directory = directory;
            this.indexWriter = indexWriter;
            this.directoryReader = directoryReader;
            this.searchExecutionContext = searchExecutionContext;
            this.leafReaderContext = leafReaderContext;
        }

        @Override
        public void close() throws IOException {
            directoryReader.close();
            indexWriter.close();
            directory.close();
        }
    }

    private TestIndexContext testIndex(MappingLookup mappingLookup, Client client) throws IOException {
        TestIndexContext context = null;

        final long nowInMillis = randomNonNegativeLong();
        final ShardId shardId = new ShardId("idx_" + randomAlphaOfLengthBetween(2, 8), randomAlphaOfLength(12), 0);
        final IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(shardId.getIndex(), Settings.EMPTY);
        final IndexWriterConfig writerConfig = new IndexWriterConfig(new StandardAnalyzer()).setMergePolicy(NoMergePolicy.INSTANCE);

        Directory directory = null;
        IndexWriter iw = null;
        DirectoryReader directoryReader = null;
        try {
            directory = newDirectory();

            iw = new IndexWriter(directory, writerConfig);
            for (int i = 1; i <= 100; i++) {
                Document document = new Document();
                for (int j = 1; j <= FIELD_COUNT; j++) {
                    document.add(new StringField("field-" + j, "value-" + i, Field.Store.NO));
                }
                iw.addDocument(document);
            }
            iw.commit();

            directoryReader = DirectoryReader.open(directory);
            final LeafReaderContext leaf = directoryReader.leaves().get(0);

            final SearchExecutionContext searchExecutionContext = new SearchExecutionContext(
                shardId.id(),
                0,
                indexSettings,
                null,
                null,
                null,
                null,
                mappingLookup,
                null,
                null,
                parserConfig(),
                writableRegistry(),
                client,
                new IndexSearcher(directoryReader),
                () -> nowInMillis,
                null,
                null,
                () -> true,
                null,
                emptyMap()
            );

            context = new TestIndexContext(directory, iw, directoryReader, searchExecutionContext, leaf);
            return context;
        } finally {
            if (context == null) {
                if (directoryReader != null) {
                    directoryReader.close();
                }
                if (iw != null) {
                    iw.close();
                }
                if (directory != null) {
                    directory.close();
                }
            }
        }
    }

    private void runTestOnIndices(int numberIndices, CheckedConsumer<List<TestIndexContext>, Exception> body) throws Exception {
        List<FieldMapper> types = new ArrayList<>();
        for (int i = 0; i < 11; i++) { // the tests use fields 1 to 10.
            // This field has a value.
            types.add(new MockFieldMapper(new KeywordFieldMapper.KeywordFieldType("field-" + i)));
            // This field never has a value
            types.add(new MockFieldMapper(new KeywordFieldMapper.KeywordFieldType("dne-" + i)));
        }

        MappingLookup mappingLookup = MappingLookup.fromMappers(Mapping.EMPTY, types, emptyList(), emptyList());

        final Client client = mock(Client.class);
        when(client.settings()).thenReturn(Settings.EMPTY);

        final List<TestIndexContext> context = new ArrayList<>(numberIndices);
        try {
            for (int i = 0; i < numberIndices; i++) {
                context.add(testIndex(mappingLookup, client));
            }

            body.accept(context);
        } finally {
            for (TestIndexContext indexContext : context) {
                indexContext.close();
            }
        }
    }

    private DocumentSubsetBitsetCache newCache(Settings settings) {
        return new DocumentSubsetBitsetCache(settings, singleThreadExecutor);
    }

}
