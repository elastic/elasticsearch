/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.index.engine;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicLong;

public class FrozenEngineTests extends EngineTestCase {

    public void testAcquireReleaseReset() throws IOException {
        IOUtils.close(engine, store);
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        try (Store store = createStore()) {
            EngineConfig config = config(defaultSettings, store, createTempDir(), newMergePolicy(), null, null, globalCheckpoint::get);
            int numDocs = scaledRandomIntBetween(10, 1000);
            try (InternalEngine engine = createEngine(config)) {
                addDocuments(globalCheckpoint, numDocs, engine);
                engine.flushAndClose();
                try (FrozenEngine frozenEngine = new FrozenEngine(engine.engineConfig)) {
                    assertFalse(frozenEngine.isReaderOpen());
                    Engine.Searcher searcher = frozenEngine.acquireSearcher("test");
                    assertEquals(config.getShardId(), ElasticsearchDirectoryReader.getElasticsearchDirectoryReader(searcher
                        .getDirectoryReader()).shardId());
                    assertTrue(frozenEngine.isReaderOpen());
                    TopDocs search = searcher.searcher().search(new MatchAllDocsQuery(), 10);
                    assertEquals(search.scoreDocs.length, 10);
                    assertEquals(1, frozenEngine.getOpenedReaders());
                    frozenEngine.release(FrozenEngine.unwrapLazyReader(searcher.getDirectoryReader()));
                    assertFalse(frozenEngine.isReaderOpen());
                    assertEquals(1, frozenEngine.getOpenedReaders());
                    expectThrows(AlreadyClosedException.class, () -> searcher.searcher().search(new MatchAllDocsQuery(), 10));
                    frozenEngine.reset(FrozenEngine.unwrapLazyReader(searcher.getDirectoryReader()));
                    assertEquals(2, frozenEngine.getOpenedReaders());
                    search = searcher.searcher().search(new MatchAllDocsQuery(), 10);
                    assertEquals(search.scoreDocs.length, 10);
                    searcher.close();
                }
            }
        }
    }

    public void testAcquireReleaseResetTwoSearchers() throws IOException {
        IOUtils.close(engine, store);
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        try (Store store = createStore()) {
            EngineConfig config = config(defaultSettings, store, createTempDir(), newMergePolicy(), null, null, globalCheckpoint::get);
            int numDocs = scaledRandomIntBetween(10, 1000);
            try (InternalEngine engine = createEngine(config)) {
                addDocuments(globalCheckpoint, numDocs, engine);
                engine.flushAndClose();
                try (FrozenEngine frozenEngine = new FrozenEngine(engine.engineConfig)) {
                    assertFalse(frozenEngine.isReaderOpen());
                    Engine.Searcher searcher1 = frozenEngine.acquireSearcher("test");
                    assertTrue(frozenEngine.isReaderOpen());
                    TopDocs search = searcher1.searcher().search(new MatchAllDocsQuery(), 10);
                    assertEquals(search.scoreDocs.length, 10);
                    assertEquals(1, frozenEngine.getOpenedReaders());
                    frozenEngine.release(FrozenEngine.unwrapLazyReader(searcher1.getDirectoryReader()));
                    Engine.Searcher searcher2 = frozenEngine.acquireSearcher("test");
                    search = searcher2.searcher().search(new MatchAllDocsQuery(), 10);
                    assertEquals(search.scoreDocs.length, 10);
                    assertTrue(frozenEngine.isReaderOpen());
                    assertEquals(2, frozenEngine.getOpenedReaders());
                    expectThrows(AlreadyClosedException.class, () -> searcher1.searcher().search(new MatchAllDocsQuery(), 10));
                    frozenEngine.reset(FrozenEngine.unwrapLazyReader(searcher1.getDirectoryReader()));
                    assertEquals(2, frozenEngine.getOpenedReaders());
                    search = searcher1.searcher().search(new MatchAllDocsQuery(), 10);
                    assertEquals(search.scoreDocs.length, 10);
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
            EngineConfig config = config(defaultSettings, store, createTempDir(), newMergePolicy(), null, null, globalCheckpoint::get);
            int numDocs = scaledRandomIntBetween(10, 1000);
            try (InternalEngine engine = createEngine(config)) {
                addDocuments(globalCheckpoint, numDocs, engine);
                engine.flushAndClose();
                try (FrozenEngine frozenEngine = new FrozenEngine(engine.engineConfig)) {
                    Engine.Searcher searcher = frozenEngine.acquireSearcher("test");
                    SegmentsStats segmentsStats = frozenEngine.segmentsStats(randomBoolean());
                    assertEquals(frozenEngine.segments(randomBoolean()).size(), segmentsStats.getCount());
                    frozenEngine.release(FrozenEngine.unwrapLazyReader(searcher.getDirectoryReader()));
                    assertEquals(1, frozenEngine.getOpenedReaders());
                    segmentsStats = frozenEngine.segmentsStats(randomBoolean());
                    assertEquals(0, segmentsStats.getCount());
                    assertEquals(1, frozenEngine.getOpenedReaders());
                    assertFalse(frozenEngine.isReaderOpen());
                    frozenEngine.reset(FrozenEngine.unwrapLazyReader(searcher.getDirectoryReader()));
                    segmentsStats = frozenEngine.segmentsStats(randomBoolean());
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
            EngineConfig config = config(defaultSettings, store, createTempDir(), newMergePolicy(), null, null, globalCheckpoint::get,
                new HierarchyCircuitBreakerService(defaultSettings.getSettings(),
                    new ClusterSettings(defaultSettings.getNodeSettings(), ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)));
            int numDocs = scaledRandomIntBetween(10, 1000);
            CircuitBreaker breaker = config.getCircuitBreakerService().getBreaker(CircuitBreaker.ACCOUNTING);
            long expectedUse;
            try (InternalEngine engine = createEngine(config)) {
                addDocuments(globalCheckpoint, numDocs, engine);
                engine.refresh("test"); // pull the reader
                expectedUse = breaker.getUsed();
                engine.flushAndClose();
            }
            assertTrue(expectedUse > 0);
            assertEquals(0, breaker.getUsed());
            try (FrozenEngine frozenEngine = new FrozenEngine(config)) {
                Engine.Searcher searcher = frozenEngine.acquireSearcher("test");
                assertEquals(expectedUse, breaker.getUsed());
                frozenEngine.release(FrozenEngine.unwrapLazyReader(searcher.getDirectoryReader()));
                assertEquals(1, frozenEngine.getOpenedReaders());
                assertEquals(0, breaker.getUsed());
                assertFalse(frozenEngine.isReaderOpen());
                frozenEngine.reset(FrozenEngine.unwrapLazyReader(searcher.getDirectoryReader()));
                assertEquals(expectedUse, breaker.getUsed());
                searcher.close();
                assertEquals(0, breaker.getUsed());
            }
        }
    }

    private void addDocuments(AtomicLong globalCheckpoint, int numDocs, InternalEngine engine) throws IOException {
        for (int i = 0; i < numDocs; i++) {
            if (rarely()) {
                continue; // gap in sequence number
            }
            ParsedDocument doc = testParsedDocument(Integer.toString(i), null, testDocument(), new BytesArray("{}"), null);
            engine.index(new Engine.Index(newUid(doc), doc, i, primaryTerm.get(), 1, null, Engine.Operation.Origin.REPLICA,
                System.nanoTime(), -1, false));
            if (rarely()) {
                engine.flush();
            }
            globalCheckpoint.set(randomLongBetween(globalCheckpoint.get(), engine.getLocalCheckpoint()));
        }
        engine.syncTranslog();
    }

    public void testSearchConcurrently() throws IOException, InterruptedException {
        // even though we don't want this to be searched concurrently we better make sure we release all resources etc.
        IOUtils.close(engine, store);
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        try (Store store = createStore()) {
            EngineConfig config = config(defaultSettings, store, createTempDir(), newMergePolicy(), null, null, globalCheckpoint::get,
                new HierarchyCircuitBreakerService(defaultSettings.getSettings(),
                    new ClusterSettings(defaultSettings.getNodeSettings(), ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)));
            int numDocs = scaledRandomIntBetween(10, 1000);
            CircuitBreaker breaker = config.getCircuitBreakerService().getBreaker(CircuitBreaker.ACCOUNTING);
            try (InternalEngine engine = createEngine(config)) {
                addDocuments(globalCheckpoint, numDocs, engine);
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
                                frozenEngine.release(FrozenEngine.unwrapLazyReader(searcher.getDirectoryReader()));
                                for (int j = 0; j < numIters; j++) {
                                    frozenEngine.reset(FrozenEngine.unwrapLazyReader(searcher.getDirectoryReader()));
                                    assertTrue(frozenEngine.isReaderOpen());
                                    TopDocs search = searcher.searcher().search(new MatchAllDocsQuery(), 10);
                                    assertEquals(search.scoreDocs.length, 10);
                                    frozenEngine.release(FrozenEngine.unwrapLazyReader(searcher.getDirectoryReader()));
                                }
                                if (randomBoolean()) {
                                    frozenEngine.reset(FrozenEngine.unwrapLazyReader(searcher.getDirectoryReader()));
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
}
