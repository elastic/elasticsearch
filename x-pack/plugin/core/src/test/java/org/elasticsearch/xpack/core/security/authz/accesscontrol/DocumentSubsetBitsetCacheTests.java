/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
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
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BitSet;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryShardContext;
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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DocumentSubsetBitsetCacheTests extends ESTestCase {

    private static final int FIELD_COUNT = 10;
    private ExecutorService singleThreadExecutor;

    @Before
    public void setUpExecutor() throws Exception {
        singleThreadExecutor = Executors.newSingleThreadExecutor();
    }

    @After
    public void cleanUpExecutor() throws Exception {
        singleThreadExecutor.shutdown();
    }

    public void testSameBitSetIsReturnedForIdenticalQuery() throws Exception {
        final DocumentSubsetBitsetCache cache = newCache(Settings.EMPTY);
        runTestOnIndex((shardContext, leafContext) -> {
            final Query query1 = QueryBuilders.termQuery("field-1", "value-1").toQuery(shardContext);
            final BitSet bitSet1 = cache.getBitSet(query1, leafContext);
            assertThat(bitSet1, notNullValue());

            final Query query2 = QueryBuilders.termQuery("field-1", "value-1").toQuery(shardContext);
            final BitSet bitSet2 = cache.getBitSet(query2, leafContext);
            assertThat(bitSet2, notNullValue());

            assertThat(bitSet2, Matchers.sameInstance(bitSet1));
        });
    }

    public void testNullBitSetIsReturnedForNonMatchingQuery() throws Exception {
        final DocumentSubsetBitsetCache cache = newCache(Settings.EMPTY);
        runTestOnIndex((shardContext, leafContext) -> {
            final Query query = QueryBuilders.termQuery("does-not-exist", "any-value").toQuery(shardContext);
            final BitSet bitSet = cache.getBitSet(query, leafContext);
            assertThat(bitSet, nullValue());
        });
    }

    public void testNullEntriesAreNotCountedInMemoryUsage() throws Exception {
        final DocumentSubsetBitsetCache cache = newCache(Settings.EMPTY);
        assertThat(cache.ramBytesUsed(), equalTo(0L));

        runTestOnIndex((shardContext, leafContext) -> {
            for (int i = 1; i <= randomIntBetween(3, 6); i++) {
                final Query query = QueryBuilders.termQuery("dne-" + i, "dne- " + i).toQuery(shardContext);
                final BitSet bitSet = cache.getBitSet(query, leafContext);
                assertThat(bitSet, nullValue());
                assertThat(cache.ramBytesUsed(), equalTo(0L));
                assertThat(cache.entryCount(), equalTo(i));
            }
        });
    }

    public void testCacheRespectsMemoryLimit() throws Exception {
        // This value is based on the internal implementation details of lucene's FixedBitSet
        // If the implementation changes, this can be safely updated to match the new ram usage for a single bitset
        final long expectedBytesPerBitSet = 56;

        // Enough to hold exactly 2 bit-sets in the cache
        final long maxCacheBytes = expectedBytesPerBitSet * 2;
        final Settings settings = Settings.builder()
            .put(DocumentSubsetBitsetCache.CACHE_SIZE_SETTING.getKey(), maxCacheBytes + "b")
            .build();
        final DocumentSubsetBitsetCache cache = newCache(settings);
        assertThat(cache.entryCount(), equalTo(0));
        assertThat(cache.ramBytesUsed(), equalTo(0L));

        runTestOnIndex((shardContext, leafContext) -> {
            Query previousQuery = null;
            BitSet previousBitSet = null;
            for (int i = 1; i <= 5; i++) {
                final TermQueryBuilder queryBuilder = QueryBuilders.termQuery("field-" + i, "value-" + i);
                final Query query = queryBuilder.toQuery(shardContext);
                final BitSet bitSet = cache.getBitSet(query, leafContext);
                assertThat(bitSet, notNullValue());
                assertThat(bitSet.ramBytesUsed(), equalTo(expectedBytesPerBitSet));

                // The first time through we have 1 entry, after that we have 2
                final int expectedCount = i == 1 ? 1 : 2;
                assertThat(cache.entryCount(), equalTo(expectedCount));
                assertThat(cache.ramBytesUsed(), equalTo(expectedCount * expectedBytesPerBitSet));

                // Older queries should get evicted, but the query from last iteration should still be cached
                if (previousQuery != null) {
                    assertThat(cache.getBitSet(previousQuery, leafContext), sameInstance(previousBitSet));
                    assertThat(cache.entryCount(), equalTo(expectedCount));
                    assertThat(cache.ramBytesUsed(), equalTo(expectedCount * expectedBytesPerBitSet));
                }
                previousQuery = query;
                previousBitSet = bitSet;

                assertThat(cache.getBitSet(queryBuilder.toQuery(shardContext), leafContext), sameInstance(bitSet));
                assertThat(cache.entryCount(), equalTo(expectedCount));
                assertThat(cache.ramBytesUsed(), equalTo(expectedCount * expectedBytesPerBitSet));
            }

            assertThat(cache.entryCount(), equalTo(2));
            assertThat(cache.ramBytesUsed(), equalTo(2 * expectedBytesPerBitSet));

            cache.clear("testing");

            assertThat(cache.entryCount(), equalTo(0));
            assertThat(cache.ramBytesUsed(), equalTo(0L));
        });
    }

    public void testLogWarningIfBitSetExceedsCacheSize() throws Exception {
        // This value is based on the internal implementation details of lucene's FixedBitSet
        // If the implementation changes, this can be safely updated to match the new ram usage for a single bitset
        final long expectedBytesPerBitSet = 56;

        // Enough to hold less than 1 bit-sets in the cache
        final long maxCacheBytes = expectedBytesPerBitSet - expectedBytesPerBitSet/3;
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
            mockAppender.addExpectation(new MockLogAppender.SeenEventExpectation(
                "[bitset too big]",
                cache.getClass().getName(),
                Level.WARN,
                "built a DLS BitSet that uses [" + expectedBytesPerBitSet + "] bytes; the DLS BitSet cache has a maximum size of [" +
                    maxCacheBytes + "] bytes; this object cannot be cached and will need to be rebuilt for each use;" +
                    " consider increasing the value of [xpack.security.dls.bitset.cache.size]"
            ));

            runTestOnIndex((shardContext, leafContext) -> {
                final TermQueryBuilder queryBuilder = QueryBuilders.termQuery("field-1", "value-1");
                final Query query = queryBuilder.toQuery(shardContext);
                final BitSet bitSet = cache.getBitSet(query, leafContext);
                assertThat(bitSet, notNullValue());
                assertThat(bitSet.ramBytesUsed(), equalTo(expectedBytesPerBitSet));
            });

            mockAppender.assertAllExpectationsMatched();
        } finally {
            Loggers.removeAppender(cacheLogger, mockAppender);
            mockAppender.stop();
        }
    }

    public void testLogMessageIfCacheFull() throws Exception {
        // This value is based on the internal implementation details of lucene's FixedBitSet
        // If the implementation changes, this can be safely updated to match the new ram usage for a single bitset
        final long expectedBytesPerBitSet = 56;

        // Enough to hold slightly more than 1 bit-sets in the cache
        final long maxCacheBytes = expectedBytesPerBitSet + expectedBytesPerBitSet/3;
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
            mockAppender.addExpectation(new MockLogAppender.SeenEventExpectation(
                "[cache full]",
                cache.getClass().getName(),
                Level.INFO,
                "the Document Level Security BitSet cache is full which may impact performance;" +
                    " consider increasing the value of [xpack.security.dls.bitset.cache.size]"
            ));

            runTestOnIndex((shardContext, leafContext) -> {
                for (int i = 1; i <= 3; i++) {
                    final TermQueryBuilder queryBuilder = QueryBuilders.termQuery("field-" + i, "value-" + i);
                    final Query query = queryBuilder.toQuery(shardContext);
                    final BitSet bitSet = cache.getBitSet(query, leafContext);
                    assertThat(bitSet, notNullValue());
                    assertThat(bitSet.ramBytesUsed(), equalTo(expectedBytesPerBitSet));
                }
            });

            mockAppender.assertAllExpectationsMatched();
        } finally {
            Loggers.removeAppender(cacheLogger, mockAppender);
            mockAppender.stop();
        }
    }

    public void testCacheRespectsAccessTimeExpiry() throws Exception {
        final Settings settings = Settings.builder()
            .put(DocumentSubsetBitsetCache.CACHE_TTL_SETTING.getKey(), "10ms")
            .build();
        final DocumentSubsetBitsetCache cache = newCache(settings);
        assertThat(cache.entryCount(), equalTo(0));
        assertThat(cache.ramBytesUsed(), equalTo(0L));

        runTestOnIndex((shardContext, leafContext) -> {
            final Query query1 = QueryBuilders.termQuery("field-1", "value-1").toQuery(shardContext);
            final BitSet bitSet1 = cache.getBitSet(query1, leafContext);
            assertThat(bitSet1, notNullValue());

            final Query query2 = QueryBuilders.termQuery("field-2", "value-2").toQuery(shardContext);
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
        // This value is based on the internal implementation details of lucene's FixedBitSet
        // If the implementation changes, this can be safely updated to match the new ram usage for a single bitset
        final long expectedBytesPerBitSet = 56;

        // Enough to hold slightly more than 1 bit-set in the cache
        final long maxCacheBytes = expectedBytesPerBitSet + expectedBytesPerBitSet/2;
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

        runTestOnIndex((shardContext, leafContext) -> {
            final Query query1 = QueryBuilders.termQuery("field-1", "value-1").toQuery(shardContext);
            final BitSet bitSet1 = cache.getBitSet(query1, leafContext);
            assertThat(bitSet1, notNullValue());

            final Query query2 = QueryBuilders.termQuery("field-2", "value-2").toQuery(shardContext);
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

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/51914")
    public void testCacheUnderConcurrentAccess() throws Exception {
        // This value is based on the internal implementation details of lucene's FixedBitSet
        // If the implementation changes, this can be safely updated to match the new ram usage for a single bitset
        final long expectedBytesPerBitSet = 56;

        final int concurrentThreads = randomIntBetween(5, 15);
        final int numberOfIndices = randomIntBetween(3, 8);

        // Force cache evictions by setting the size to be less than the number of distinct queries we search on.
        final int maxCacheCount = randomIntBetween(FIELD_COUNT / 2, FIELD_COUNT * 3 / 4);
        final long maxCacheBytes = expectedBytesPerBitSet * maxCacheCount;
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
                for (int thread = 0; thread < concurrentThreads; thread++) {
                    threads.submit(() -> {
                        start.countDown();
                        start.await(100, TimeUnit.MILLISECONDS);
                        for (int loop = 0; loop < 15; loop++) {
                            for (int field = 1; field <= FIELD_COUNT; field++) {
                                final TermQueryBuilder queryBuilder = QueryBuilders.termQuery("field-" + field, "value-" + field);
                                final TestIndexContext randomContext = randomFrom(contexts);
                                final Query query = queryBuilder.toQuery(randomContext.queryShardContext);
                                final BitSet bitSet = cache.getBitSet(query, randomContext.leafReaderContext);
                                assertThat(bitSet, notNullValue());
                                assertThat(bitSet.ramBytesUsed(), equalTo(expectedBytesPerBitSet));
                                uniqueBitSets.add(bitSet);
                            }
                        }
                        end.countDown();
                        return null;
                    });
                }

                assertTrue("Query threads did not complete in expected time", end.await(1, TimeUnit.SECONDS));

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

    public void testCacheIsPerIndex() throws Exception {
        final DocumentSubsetBitsetCache cache = newCache(Settings.EMPTY);
        assertThat(cache.entryCount(), equalTo(0));
        assertThat(cache.ramBytesUsed(), equalTo(0L));

        final int iterations = randomIntBetween(3, 10);
        AtomicInteger counter = new AtomicInteger(0);

        final CheckedBiConsumer<QueryShardContext, LeafReaderContext, Exception> consumer = new CheckedBiConsumer<>() {
            @Override
            public void accept(QueryShardContext shardContext, LeafReaderContext leafContext) throws Exception {
                final int count = counter.incrementAndGet();
                final Query query = QueryBuilders.termQuery("field-1", "value-1").toQuery(shardContext);
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
            runTestOnIndex((shardContext, leafContext) -> {
                for (int j = 1; j <= randomIntBetween(2, 10); j++) {
                    final Query query = QueryBuilders.termQuery("field-" + j, "value-1").toQuery(shardContext);
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

    private void runTestOnIndex(CheckedBiConsumer<QueryShardContext, LeafReaderContext, Exception> body) throws Exception {
        runTestOnIndices(1, ctx -> {
            final TestIndexContext indexContext = ctx.get(0);
            body.accept(indexContext.queryShardContext, indexContext.leafReaderContext);
        });
    }

    private static final class TestIndexContext implements Closeable {
        private final Directory directory;
        private final IndexWriter indexWriter;
        private final DirectoryReader directoryReader;
        private final QueryShardContext queryShardContext;
        private final LeafReaderContext leafReaderContext;

        private TestIndexContext(Directory directory, IndexWriter indexWriter, DirectoryReader directoryReader,
                                 QueryShardContext queryShardContext, LeafReaderContext leafReaderContext) {
            this.directory = directory;
            this.indexWriter = indexWriter;
            this.directoryReader = directoryReader;
            this.queryShardContext = queryShardContext;
            this.leafReaderContext = leafReaderContext;
        }

        @Override
        public void close() throws IOException {
            directoryReader.close();
            indexWriter.close();
            directory.close();
        }
    }

    private TestIndexContext testIndex(MapperService mapperService, Client client) throws IOException {
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

            final QueryShardContext shardContext = new QueryShardContext(shardId.id(), indexSettings, BigArrays.NON_RECYCLING_INSTANCE,
                null, null, mapperService, null, null, xContentRegistry(), writableRegistry(),
                client, new IndexSearcher(directoryReader), () -> nowInMillis, null, null);

            context = new TestIndexContext(directory, iw, directoryReader, shardContext, leaf);
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
        final MapperService mapperService = mock(MapperService.class);

        final Client client = mock(Client.class);
        when(client.settings()).thenReturn(Settings.EMPTY);

        final List<TestIndexContext> context = new ArrayList<>(numberIndices);
        try {
            for (int i = 0; i < numberIndices; i++) {
                context.add(testIndex(mapperService, client));
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
