/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.index.engine;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class FrozenEngineTests extends EngineTestCase {

    public void testAcquireReleaseReset() throws IOException {
        IOUtils.close(engine, store);
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        try (Store store = createStore()) {
            CountingRefreshListener listener = new CountingRefreshListener();
            EngineConfig config = config(defaultSettings, store, createTempDir(), newMergePolicy(), null, listener, null,
                globalCheckpoint::get, new NoneCircuitBreakerService());
            try (InternalEngine engine = createEngine(config)) {
                int numDocs = Math.min(10, addDocuments(globalCheckpoint, engine));
                engine.flushAndClose();
                listener.reset();
                try (FrozenEngine frozenEngine = new FrozenEngine(engine.engineConfig)) {
                    assertFalse(frozenEngine.isReaderOpen());
                    Engine.Searcher searcher = frozenEngine.acquireSearcher("test");
                    assertEquals(config.getShardId(), ElasticsearchDirectoryReader.getElasticsearchDirectoryReader(searcher
                        .getDirectoryReader()).shardId());
                    assertTrue(frozenEngine.isReaderOpen());
                    TopDocs search = searcher.search(new MatchAllDocsQuery(), numDocs);
                    assertEquals(search.scoreDocs.length, numDocs);
                    assertEquals(1, listener.afterRefresh.get());
                    FrozenEngine.unwrapLazyReader(searcher.getDirectoryReader()).release();
                    assertFalse(frozenEngine.isReaderOpen());
                    assertEquals(1, listener.afterRefresh.get());
                    expectThrows(AlreadyClosedException.class, () -> searcher.search(new MatchAllDocsQuery(), numDocs));
                    FrozenEngine.unwrapLazyReader(searcher.getDirectoryReader()).reset();
                    assertEquals(2, listener.afterRefresh.get());
                    search = searcher.search(new MatchAllDocsQuery(), numDocs);
                    assertEquals(search.scoreDocs.length, numDocs);
                    searcher.close();
                }
            }
        }
    }

    public void testAcquireReleaseResetTwoSearchers() throws IOException {
        IOUtils.close(engine, store);
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        try (Store store = createStore()) {
            CountingRefreshListener listener = new CountingRefreshListener();
            EngineConfig config = config(defaultSettings, store, createTempDir(), newMergePolicy(), null, listener, null,
                globalCheckpoint::get, new NoneCircuitBreakerService());
            try (InternalEngine engine = createEngine(config)) {
                int numDocs = Math.min(10, addDocuments(globalCheckpoint, engine));
                engine.flushAndClose();
                listener.reset();
                try (FrozenEngine frozenEngine = new FrozenEngine(engine.engineConfig)) {
                    assertFalse(frozenEngine.isReaderOpen());
                    Engine.Searcher searcher1 = frozenEngine.acquireSearcher("test");
                    assertTrue(frozenEngine.isReaderOpen());
                    TopDocs search = searcher1.search(new MatchAllDocsQuery(), numDocs);
                    assertEquals(search.scoreDocs.length, numDocs);
                    assertEquals(1, listener.afterRefresh.get());
                    FrozenEngine.unwrapLazyReader(searcher1.getDirectoryReader()).release();
                    Engine.Searcher searcher2 = frozenEngine.acquireSearcher("test");
                    search = searcher2.search(new MatchAllDocsQuery(), numDocs);
                    assertEquals(search.scoreDocs.length, numDocs);
                    assertTrue(frozenEngine.isReaderOpen());
                    assertEquals(2, listener.afterRefresh.get());
                    expectThrows(AlreadyClosedException.class, () -> searcher1.search(new MatchAllDocsQuery(), numDocs));
                    FrozenEngine.unwrapLazyReader(searcher1.getDirectoryReader()).reset();
                    assertEquals(2, listener.afterRefresh.get());
                    search = searcher1.search(new MatchAllDocsQuery(), numDocs);
                    assertEquals(search.scoreDocs.length, numDocs);
                    searcher1.close();
                    searcher2.close();
                }
            }
        }
    }

    public void testSegmentStats() throws IOException {
        IOUtils.close(engine, store);
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        try (Store store = createStore()) {
            CountingRefreshListener listener = new CountingRefreshListener();
            EngineConfig config = config(defaultSettings, store, createTempDir(), newMergePolicy(), null, listener, null,
                globalCheckpoint::get, new NoneCircuitBreakerService());
            try (InternalEngine engine = createEngine(config)) {
                addDocuments(globalCheckpoint, engine);
                engine.flushAndClose();
                listener.reset();
                try (FrozenEngine frozenEngine = new FrozenEngine(engine.engineConfig)) {
                    Engine.Searcher searcher = frozenEngine.acquireSearcher("test");
                    SegmentsStats segmentsStats = frozenEngine.segmentsStats(randomBoolean(), false);
                    assertEquals(frozenEngine.segments(randomBoolean()).size(), segmentsStats.getCount());
                    FrozenEngine.unwrapLazyReader(searcher.getDirectoryReader()).release();
                    assertEquals(1, listener.afterRefresh.get());
                    segmentsStats = frozenEngine.segmentsStats(randomBoolean(), false);
                    assertEquals(0, segmentsStats.getCount());
                    segmentsStats = frozenEngine.segmentsStats(randomBoolean(), true);
                    assertEquals(frozenEngine.segments(randomBoolean()).size(), segmentsStats.getCount());
                    assertEquals(1, listener.afterRefresh.get());
                    assertFalse(frozenEngine.isReaderOpen());
                    FrozenEngine.unwrapLazyReader(searcher.getDirectoryReader()).reset();
                    segmentsStats = frozenEngine.segmentsStats(randomBoolean(), false);
                    assertEquals(frozenEngine.segments(randomBoolean()).size(), segmentsStats.getCount());
                    searcher.close();
                }
            }
        }
    }

    public void testCircuitBreakerAccounting() throws IOException {
        IOUtils.close(engine, store);
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        try (Store store = createStore()) {
            CountingRefreshListener listener = new CountingRefreshListener();
            EngineConfig config = config(defaultSettings, store, createTempDir(),
                NoMergePolicy.INSTANCE, // we don't merge we want no background merges to happen to ensure we have consistent breaker stats
                null, listener, null, globalCheckpoint::get, new HierarchyCircuitBreakerService(defaultSettings.getSettings(),
                    new ClusterSettings(defaultSettings.getNodeSettings(), ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)));
            CircuitBreaker breaker = config.getCircuitBreakerService().getBreaker(CircuitBreaker.ACCOUNTING);
            final int docs;
            try (InternalEngine engine = createEngine(config)) {
                docs = addDocuments(globalCheckpoint, engine);
                engine.flush(false, true); // first flush to make sure we have a commit that we open in the frozen engine blow.
                engine.refresh("test"); // pull the reader to account for RAM in the breaker.
            }
            final long expectedUse;
            try (ReadOnlyEngine readOnlyEngine = new ReadOnlyEngine(config, null, null, true, i -> i)) {
                expectedUse = breaker.getUsed();
                DocsStats docsStats = readOnlyEngine.docStats();
                assertEquals(docs, docsStats.getCount());
            }
            assertTrue(expectedUse > 0);
            assertEquals(0, breaker.getUsed());
            listener.reset();
            try (FrozenEngine frozenEngine = new FrozenEngine(config)) {
                Engine.Searcher searcher = frozenEngine.acquireSearcher("test");
                assertEquals(expectedUse, breaker.getUsed());
                FrozenEngine.unwrapLazyReader(searcher.getDirectoryReader()).release();
                assertEquals(1, listener.afterRefresh.get());
                assertEquals(0, breaker.getUsed());
                assertFalse(frozenEngine.isReaderOpen());
                FrozenEngine.unwrapLazyReader(searcher.getDirectoryReader()).reset();
                assertEquals(expectedUse, breaker.getUsed());
                searcher.close();
                assertEquals(0, breaker.getUsed());
            }
        }
    }

    private int addDocuments(AtomicLong globalCheckpoint, InternalEngine engine) throws IOException {
        int numDocs = scaledRandomIntBetween(10, 1000);
        int numDocsAdded = 0;
        for (int i = 0; i < numDocs; i++) {
            numDocsAdded++;
            ParsedDocument doc = testParsedDocument(Integer.toString(i), null, testDocument(), new BytesArray("{}"), null);
            engine.index(new Engine.Index(newUid(doc), doc, i, primaryTerm.get(), 1, null, Engine.Operation.Origin.REPLICA,
                System.nanoTime(), -1, false, SequenceNumbers.UNASSIGNED_SEQ_NO, 0));
            if (rarely()) {
                engine.flush();
            }
            globalCheckpoint.set(engine.getProcessedLocalCheckpoint());
        }
        engine.syncTranslog();
        return numDocsAdded;
    }

    public void testSearchConcurrently() throws IOException, InterruptedException {
        // even though we don't want this to be searched concurrently we better make sure we release all resources etc.
        IOUtils.close(engine, store);
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        try (Store store = createStore()) {
            EngineConfig config = config(defaultSettings, store, createTempDir(), newMergePolicy(), null, null, null, globalCheckpoint::get,
                new HierarchyCircuitBreakerService(defaultSettings.getSettings(),
                    new ClusterSettings(defaultSettings.getNodeSettings(), ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)));
            CircuitBreaker breaker = config.getCircuitBreakerService().getBreaker(CircuitBreaker.ACCOUNTING);
            try (InternalEngine engine = createEngine(config)) {
                int numDocsAdded = addDocuments(globalCheckpoint, engine);
                engine.flushAndClose();
                int numIters = randomIntBetween(100, 1000);
                try (FrozenEngine frozenEngine = new FrozenEngine(engine.engineConfig)) {
                    int numThreads = randomIntBetween(2, 4);
                    Thread[] threads = new Thread[numThreads];
                    CyclicBarrier barrier = new CyclicBarrier(numThreads);
                    CountDownLatch latch = new CountDownLatch(numThreads);
                    for (int i = 0; i < numThreads; i++) {
                        threads[i] = new Thread(() -> {
                            try (Engine.Searcher searcher = frozenEngine.acquireSearcher("test")) {
                                barrier.await();
                                FrozenEngine.unwrapLazyReader(searcher.getDirectoryReader()).release();
                                for (int j = 0; j < numIters; j++) {
                                    FrozenEngine.unwrapLazyReader(searcher.getDirectoryReader()).reset();
                                    assertTrue(frozenEngine.isReaderOpen());
                                    TopDocs search = searcher.search(new MatchAllDocsQuery(), Math.min(10, numDocsAdded));
                                    assertEquals(search.scoreDocs.length, Math.min(10, numDocsAdded));
                                    FrozenEngine.unwrapLazyReader(searcher.getDirectoryReader()).release();
                                }
                                if (randomBoolean()) {
                                    FrozenEngine.unwrapLazyReader(searcher.getDirectoryReader()).reset();
                                }
                            } catch (Exception e) {
                                throw new AssertionError(e);
                            } finally {
                                latch.countDown();
                            }
                        });
                        threads[i].start();
                    }
                    latch.await();
                    for (Thread t : threads) {
                        t.join();
                    }
                    assertFalse(frozenEngine.isReaderOpen());
                    assertEquals(0, breaker.getUsed());
                }
            }
        }
    }

    private static void checkOverrideMethods(Class<?> clazz) throws NoSuchMethodException, SecurityException {
        final Class<?> superClazz = clazz.getSuperclass();
        for (Method m : superClazz.getMethods()) {
            final int mods = m.getModifiers();
            if (Modifier.isStatic(mods) || Modifier.isAbstract(mods) || Modifier.isFinal(mods) || m.isSynthetic()
                || m.getName().equals("attributes") || m.getName().equals("getStats")) {
                continue;
            }
            // The point of these checks is to ensure that methods from the super class
            // are overwritten to make sure we never miss a method from FilterLeafReader / FilterDirectoryReader
            final Method subM = clazz.getMethod(m.getName(), m.getParameterTypes());
            if (subM.getDeclaringClass() == superClazz
                && m.getDeclaringClass() != Object.class
                && m.getDeclaringClass() == subM.getDeclaringClass()) {
                fail(clazz + " doesn't override" + m + " although it has been declared by it's superclass");
            }
        }
    }

    // here we make sure we catch any change to their super classes FilterLeafReader / FilterDirectoryReader
    public void testOverrideMethods() throws Exception {
        checkOverrideMethods(FrozenEngine.LazyDirectoryReader.class);
        checkOverrideMethods(FrozenEngine.LazyLeafReader.class);
    }

    private class CountingRefreshListener implements ReferenceManager.RefreshListener {

        final AtomicInteger afterRefresh = new AtomicInteger(0);
        private final AtomicInteger beforeRefresh = new AtomicInteger(0);

        @Override
        public void beforeRefresh() {
            beforeRefresh.incrementAndGet();
        }

        @Override
        public void afterRefresh(boolean didRefresh) {
            afterRefresh.incrementAndGet();
            assertEquals(beforeRefresh.get(), afterRefresh.get());
        }

        void reset() {
            afterRefresh.set(0);
            beforeRefresh.set(0);
        }
    }
    public void testCanMatch() throws IOException {
        IOUtils.close(engine, store);
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        try (Store store = createStore()) {
            CountingRefreshListener listener = new CountingRefreshListener();
            EngineConfig config = config(defaultSettings, store, createTempDir(), newMergePolicy(), null, listener, null,
                globalCheckpoint::get, new NoneCircuitBreakerService());
            try (InternalEngine engine = createEngine(config)) {
                addDocuments(globalCheckpoint, engine);
                engine.flushAndClose();
                listener.reset();
                try (FrozenEngine frozenEngine = new FrozenEngine(engine.engineConfig)) {
                    DirectoryReader reader;
                    try (Engine.Searcher searcher = frozenEngine.acquireSearcher("can_match")) {
                        assertNotNull(ElasticsearchDirectoryReader.getElasticsearchDirectoryReader(searcher.getDirectoryReader()));
                        assertEquals(config.getShardId(), ElasticsearchDirectoryReader.getElasticsearchDirectoryReader(searcher
                            .getDirectoryReader()).shardId());
                        reader = searcher.getDirectoryReader();
                        assertNotEquals(reader, Matchers.instanceOf(FrozenEngine.LazyDirectoryReader.class));
                        assertEquals(0, listener.afterRefresh.get());
                        DirectoryReader unwrap = FilterDirectoryReader.unwrap(searcher.getDirectoryReader());
                        assertThat(unwrap, Matchers.instanceOf(RewriteCachingDirectoryReader.class));
                        assertNotNull(ElasticsearchDirectoryReader.getElasticsearchDirectoryReader(searcher.getDirectoryReader()));
                    }

                    try (Engine.Searcher searcher = frozenEngine.acquireSearcher("can_match")) {
                        assertSame(reader, searcher.getDirectoryReader());
                        assertNotEquals(reader, Matchers.instanceOf(FrozenEngine.LazyDirectoryReader.class));
                        assertEquals(0, listener.afterRefresh.get());
                        DirectoryReader unwrap = FilterDirectoryReader.unwrap(searcher.getDirectoryReader());
                        assertThat(unwrap, Matchers.instanceOf(RewriteCachingDirectoryReader.class));
                    }
                }
            }
        }
    }
}
