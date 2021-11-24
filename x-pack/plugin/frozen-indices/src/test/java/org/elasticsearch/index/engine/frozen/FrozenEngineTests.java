/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.index.engine.frozen;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.search.TopDocs;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineTestCase;
import org.elasticsearch.index.engine.InternalEngine;
import org.elasticsearch.index.engine.SegmentsStats;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static org.hamcrest.Matchers.equalTo;

public class FrozenEngineTests extends EngineTestCase {

    public void testAcquireReleaseReset() throws IOException {
        IOUtils.close(engine, store);
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        try (Store store = createStore()) {
            CountingRefreshListener listener = new CountingRefreshListener();
            EngineConfig config = config(
                defaultSettings,
                store,
                createTempDir(),
                newMergePolicy(),
                null,
                listener,
                null,
                globalCheckpoint::get,
                new NoneCircuitBreakerService()
            );
            try (InternalEngine engine = createEngine(config)) {
                int numDocs = Math.min(10, addDocuments(globalCheckpoint, engine));
                engine.flushAndClose();
                listener.reset();
                try (FrozenEngine frozenEngine = new FrozenEngine(engine.getEngineConfig(), true, randomBoolean())) {
                    assertFalse(frozenEngine.isReaderOpen());
                    try (Engine.SearcherSupplier reader = frozenEngine.acquireSearcherSupplier(Function.identity())) {
                        assertFalse(frozenEngine.isReaderOpen());
                        try (Engine.Searcher searcher = reader.acquireSearcher("frozen")) {
                            assertEquals(
                                config.getShardId(),
                                ElasticsearchDirectoryReader.getElasticsearchDirectoryReader(searcher.getDirectoryReader()).shardId()
                            );
                            assertTrue(frozenEngine.isReaderOpen());
                            TopDocs search = searcher.search(new MatchAllDocsQuery(), numDocs);
                            assertEquals(search.scoreDocs.length, numDocs);
                            assertEquals(1, listener.afterRefresh.get());
                        }
                        assertFalse(frozenEngine.isReaderOpen());
                        assertEquals(1, listener.afterRefresh.get());

                        try (Engine.Searcher searcher = reader.acquireSearcher("frozen")) {
                            assertTrue(frozenEngine.isReaderOpen());
                            TopDocs search = searcher.search(new MatchAllDocsQuery(), numDocs);
                            assertEquals(search.scoreDocs.length, numDocs);
                        }
                    }
                }
            }
        }
    }

    public void testAcquireReleaseResetTwoSearchers() throws IOException {
        IOUtils.close(engine, store);
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        try (Store store = createStore()) {
            CountingRefreshListener listener = new CountingRefreshListener();
            EngineConfig config = config(
                defaultSettings,
                store,
                createTempDir(),
                newMergePolicy(),
                null,
                listener,
                null,
                globalCheckpoint::get,
                new NoneCircuitBreakerService()
            );
            try (InternalEngine engine = createEngine(config)) {
                int numDocs = Math.min(10, addDocuments(globalCheckpoint, engine));
                engine.flushAndClose();
                listener.reset();
                try (FrozenEngine frozenEngine = new FrozenEngine(engine.getEngineConfig(), true, randomBoolean())) {
                    assertFalse(frozenEngine.isReaderOpen());
                    Engine.SearcherSupplier reader1 = frozenEngine.acquireSearcherSupplier(Function.identity());
                    try (Engine.Searcher searcher1 = reader1.acquireSearcher("test")) {
                        assertTrue(frozenEngine.isReaderOpen());
                        TopDocs search = searcher1.search(new MatchAllDocsQuery(), numDocs);
                        assertEquals(search.scoreDocs.length, numDocs);
                        assertEquals(1, listener.afterRefresh.get());
                    }
                    assertFalse(frozenEngine.isReaderOpen());
                    Engine.SearcherSupplier reader2 = frozenEngine.acquireSearcherSupplier(Function.identity());
                    try (Engine.Searcher searcher2 = reader2.acquireSearcher("test")) {
                        TopDocs search = searcher2.search(new MatchAllDocsQuery(), numDocs);
                        assertEquals(search.scoreDocs.length, numDocs);
                        assertTrue(frozenEngine.isReaderOpen());
                        assertEquals(2, listener.afterRefresh.get());
                    }
                    assertFalse(frozenEngine.isReaderOpen());
                    assertEquals(2, listener.afterRefresh.get());
                    reader2.close();
                    try (Engine.Searcher searcher1 = reader1.acquireSearcher("test")) {
                        TopDocs search = searcher1.search(new MatchAllDocsQuery(), numDocs);
                        assertEquals(search.scoreDocs.length, numDocs);
                        assertTrue(frozenEngine.isReaderOpen());
                    }
                    reader1.close();
                    assertFalse(frozenEngine.isReaderOpen());
                }
            }
        }
    }

    public void testSegmentStats() throws IOException {
        IOUtils.close(engine, store);
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        try (Store store = createStore()) {
            CountingRefreshListener listener = new CountingRefreshListener();
            EngineConfig config = config(
                defaultSettings,
                store,
                createTempDir(),
                newMergePolicy(),
                null,
                listener,
                null,
                globalCheckpoint::get,
                new NoneCircuitBreakerService()
            );
            try (InternalEngine engine = createEngine(config)) {
                addDocuments(globalCheckpoint, engine);
                engine.flushAndClose();
                listener.reset();
                try (FrozenEngine frozenEngine = new FrozenEngine(engine.getEngineConfig(), true, randomBoolean())) {
                    try (Engine.SearcherSupplier reader = frozenEngine.acquireSearcherSupplier(Function.identity())) {
                        SegmentsStats segmentsStats = frozenEngine.segmentsStats(randomBoolean(), false);
                        try (Engine.Searcher searcher = reader.acquireSearcher("test")) {
                            segmentsStats = frozenEngine.segmentsStats(randomBoolean(), false);
                            assertEquals(frozenEngine.segments().size(), segmentsStats.getCount());
                            assertEquals(1, listener.afterRefresh.get());
                        }
                        segmentsStats = frozenEngine.segmentsStats(randomBoolean(), false);
                        assertEquals(0, segmentsStats.getCount());
                        try (Engine.Searcher searcher = reader.acquireSearcher("test")) {
                            segmentsStats = frozenEngine.segmentsStats(randomBoolean(), true);
                            assertEquals(frozenEngine.segments().size(), segmentsStats.getCount());
                            assertEquals(2, listener.afterRefresh.get());
                        }
                        assertFalse(frozenEngine.isReaderOpen());
                        segmentsStats = frozenEngine.segmentsStats(randomBoolean(), true);
                        assertEquals(frozenEngine.segments().size(), segmentsStats.getCount());
                    }
                }
            }
        }
    }

    private int addDocuments(AtomicLong globalCheckpoint, InternalEngine engine) throws IOException {
        int numDocs = scaledRandomIntBetween(10, 1000);
        int numDocsAdded = 0;
        for (int i = 0; i < numDocs; i++) {
            numDocsAdded++;
            ParsedDocument doc = testParsedDocument(Integer.toString(i), null, testDocument(), new BytesArray("{}"), null);
            engine.index(
                new Engine.Index(
                    newUid(doc),
                    doc,
                    i,
                    primaryTerm.get(),
                    1,
                    null,
                    Engine.Operation.Origin.REPLICA,
                    System.nanoTime(),
                    -1,
                    false,
                    SequenceNumbers.UNASSIGNED_SEQ_NO,
                    0
                )
            );
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
            EngineConfig config = config(
                defaultSettings,
                store,
                createTempDir(),
                newMergePolicy(),
                null,
                null,
                null,
                globalCheckpoint::get,
                new HierarchyCircuitBreakerService(
                    defaultSettings.getSettings(),
                    Collections.emptyList(),
                    new ClusterSettings(defaultSettings.getNodeSettings(), ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
                )
            );
            try (InternalEngine engine = createEngine(config)) {
                int numDocsAdded = addDocuments(globalCheckpoint, engine);
                engine.flushAndClose();
                int numIters = randomIntBetween(100, 1000);
                try (FrozenEngine frozenEngine = new FrozenEngine(engine.getEngineConfig(), true, randomBoolean())) {
                    int numThreads = randomIntBetween(2, 4);
                    Thread[] threads = new Thread[numThreads];
                    CyclicBarrier barrier = new CyclicBarrier(numThreads);
                    CountDownLatch latch = new CountDownLatch(numThreads);
                    for (int i = 0; i < numThreads; i++) {
                        threads[i] = new Thread(() -> {
                            try (Engine.SearcherSupplier reader = frozenEngine.acquireSearcherSupplier(Function.identity())) {
                                barrier.await();
                                for (int j = 0; j < numIters; j++) {
                                    try (Engine.Searcher searcher = reader.acquireSearcher("test")) {
                                        assertTrue(frozenEngine.isReaderOpen());
                                        TopDocs search = searcher.search(new MatchAllDocsQuery(), Math.min(10, numDocsAdded));
                                        assertEquals(search.scoreDocs.length, Math.min(10, numDocsAdded));
                                    }
                                }
                                if (randomBoolean()) {
                                    reader.acquireSearcher("test").close();
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
                }
            }
        }
    }

    private static void checkOverrideMethods(Class<?> clazz) throws NoSuchMethodException, SecurityException {
        final Class<?> superClazz = clazz.getSuperclass();
        for (Method m : superClazz.getMethods()) {
            final int mods = m.getModifiers();
            if (Modifier.isStatic(mods)
                || Modifier.isAbstract(mods)
                || Modifier.isFinal(mods)
                || m.isSynthetic()
                || m.getName().equals("attributes")
                || m.getName().equals("getStats")) {
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
            EngineConfig config = config(
                defaultSettings,
                store,
                createTempDir(),
                newMergePolicy(),
                null,
                listener,
                null,
                globalCheckpoint::get,
                new NoneCircuitBreakerService()
            );
            try (InternalEngine engine = createEngine(config)) {
                addDocuments(globalCheckpoint, engine);
                engine.flushAndClose();
                listener.reset();
                try (FrozenEngine frozenEngine = new FrozenEngine(engine.getEngineConfig(), true, randomBoolean())) {
                    DirectoryReader dirReader;
                    try (Engine.SearcherSupplier reader = frozenEngine.acquireSearcherSupplier(Function.identity())) {
                        try (Engine.Searcher searcher = reader.acquireSearcher(Engine.CAN_MATCH_SEARCH_SOURCE)) {
                            dirReader = searcher.getDirectoryReader();
                            assertNotNull(ElasticsearchDirectoryReader.getElasticsearchDirectoryReader(searcher.getDirectoryReader()));
                            assertEquals(
                                config.getShardId(),
                                ElasticsearchDirectoryReader.getElasticsearchDirectoryReader(searcher.getDirectoryReader()).shardId()
                            );
                            assertEquals(0, listener.afterRefresh.get());
                            DirectoryReader unwrap = FilterDirectoryReader.unwrap(searcher.getDirectoryReader());
                            assertThat(unwrap, Matchers.instanceOf(RewriteCachingDirectoryReader.class));
                            assertNotNull(ElasticsearchDirectoryReader.getElasticsearchDirectoryReader(searcher.getDirectoryReader()));
                        }
                    }

                    try (Engine.SearcherSupplier reader = frozenEngine.acquireSearcherSupplier(Function.identity())) {
                        try (Engine.Searcher searcher = reader.acquireSearcher(Engine.CAN_MATCH_SEARCH_SOURCE)) {
                            assertSame(dirReader, searcher.getDirectoryReader());
                            assertEquals(0, listener.afterRefresh.get());
                            DirectoryReader unwrap = FilterDirectoryReader.unwrap(searcher.getDirectoryReader());
                            assertThat(unwrap, Matchers.instanceOf(RewriteCachingDirectoryReader.class));
                        }
                    }
                }
            }
        }
    }

    public void testSearchers() throws Exception {
        IOUtils.close(engine, store);
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        try (Store store = createStore()) {
            EngineConfig config = config(
                defaultSettings,
                store,
                createTempDir(),
                newMergePolicy(),
                null,
                null,
                null,
                globalCheckpoint::get,
                new NoneCircuitBreakerService()
            );
            final int totalDocs;
            try (InternalEngine engine = createEngine(config)) {
                applyOperations(engine, generateHistoryOnReplica(between(10, 1000), false, randomBoolean(), randomBoolean()));
                globalCheckpoint.set(engine.getProcessedLocalCheckpoint());
                engine.syncTranslog();
                // We need to force flush to make the last commit a safe commit; otherwise, we might fail to open ReadOnlyEngine
                // See TransportVerifyShardBeforeCloseAction#executeShardOperation
                engine.flush(true, true);
                engine.refresh("test");
                try (Engine.SearcherSupplier reader = engine.acquireSearcherSupplier(Function.identity())) {
                    try (Engine.Searcher searcher = reader.acquireSearcher("test")) {
                        totalDocs = searcher.search(new MatchAllDocsQuery(), Integer.MAX_VALUE).scoreDocs.length;
                    }
                }
            }
            try (FrozenEngine frozenEngine = new FrozenEngine(config, true, randomBoolean())) {
                try (Engine.SearcherSupplier reader = frozenEngine.acquireSearcherSupplier(Function.identity())) {
                    try (Engine.Searcher searcher = reader.acquireSearcher("test")) {
                        TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), Integer.MAX_VALUE);
                        assertThat(topDocs.scoreDocs.length, equalTo(totalDocs));
                    }
                }
            }
        }
    }
}
