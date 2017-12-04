/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.shard;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.InternalEngine;
import org.elasticsearch.index.fieldvisitor.SingleFieldsVisitor;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.ParseContext.Document;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.store.DirectoryService;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.TranslogConfig;
import org.elasticsearch.test.DummyShardLock;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.Scheduler.Cancellable;
import org.elasticsearch.threadpool.ThreadPool.Names;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;

/**
 * Tests how {@linkplain RefreshListeners} interacts with {@linkplain InternalEngine}.
 */
public class RefreshListenersTests extends ESTestCase {
    private RefreshListeners listeners;
    private Engine engine;
    private volatile int maxListeners;
    private ThreadPool threadPool;
    private Store store;

    @Before
    public void setupListeners() throws Exception {
        // Setup dependencies of the listeners
        maxListeners = randomIntBetween(1, 1000);
        // Now setup the InternalEngine which is much more complicated because we aren't mocking anything
        threadPool = new TestThreadPool(getTestName());
        listeners = new RefreshListeners(
                () -> maxListeners,
                () -> engine.refresh("too-many-listeners"),
                // Immediately run listeners rather than adding them to the listener thread pool like IndexShard does to simplify the test.
                Runnable::run,
                logger,
                threadPool.getThreadContext());

        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("index", Settings.EMPTY);
        ShardId shardId = new ShardId(new Index("index", "_na_"), 1);
        String allocationId = UUIDs.randomBase64UUID(random());
        Directory directory = newDirectory();
        DirectoryService directoryService = new DirectoryService(shardId, indexSettings) {
            @Override
            public Directory newDirectory() throws IOException {
                return directory;
            }
        };
        store = new Store(shardId, indexSettings, directoryService, new DummyShardLock(shardId));
        IndexWriterConfig iwc = newIndexWriterConfig();
        TranslogConfig translogConfig = new TranslogConfig(shardId, createTempDir("translog"), indexSettings,
            BigArrays.NON_RECYCLING_INSTANCE);
        Engine.EventListener eventListener = new Engine.EventListener() {
            @Override
            public void onFailedEngine(String reason, @Nullable Exception e) {
                // we don't need to notify anybody in this test
            }
        };
        EngineConfig config = new EngineConfig(EngineConfig.OpenMode.CREATE_INDEX_AND_TRANSLOG, shardId, allocationId, threadPool,
                indexSettings, null, store, newMergePolicy(), iwc.getAnalyzer(), iwc.getSimilarity(), new CodecService(null, logger),
                eventListener, IndexSearcher.getDefaultQueryCache(), IndexSearcher.getDefaultQueryCachingPolicy(), false, translogConfig,
                TimeValue.timeValueMinutes(5), Collections.singletonList(listeners), Collections.emptyList(), null, null);
        engine = new InternalEngine(config);
        listeners.setTranslog(engine.getTranslog());
    }

    @After
    public void tearDownListeners() throws Exception {
        IOUtils.close(engine, store);
        terminate(threadPool);
    }

    public void testBeforeRefresh() throws Exception {
        assertEquals(0, listeners.pendingCount());
        Engine.IndexResult index = index("1");
        DummyRefreshListener listener = new DummyRefreshListener();
        assertFalse(listeners.addOrNotify(index.getTranslogLocation(), listener));
        assertNull(listener.forcedRefresh.get());
        assertEquals(1, listeners.pendingCount());
        engine.refresh("I said so");
        assertFalse(listener.forcedRefresh.get());
        listener.assertNoError();
        assertEquals(0, listeners.pendingCount());
    }

    public void testAfterRefresh() throws Exception {
        assertEquals(0, listeners.pendingCount());
        Engine.IndexResult index = index("1");
        engine.refresh("I said so");
        if (randomBoolean()) {
            index(randomFrom("1" /* same document */, "2" /* different document */));
            if (randomBoolean()) {
                engine.refresh("I said so");
            }
        }
        DummyRefreshListener listener = new DummyRefreshListener();
        assertTrue(listeners.addOrNotify(index.getTranslogLocation(), listener));
        assertFalse(listener.forcedRefresh.get());
        listener.assertNoError();
        assertEquals(0, listeners.pendingCount());
    }

    public void testContextIsPreserved() throws IOException, InterruptedException {
        assertEquals(0, listeners.pendingCount());
        Engine.IndexResult index = index("1");
        CountDownLatch latch = new CountDownLatch(1);
        try (ThreadContext.StoredContext ignore = threadPool.getThreadContext().stashContext()) {
            threadPool.getThreadContext().putHeader("test", "foobar");
            assertFalse(listeners.addOrNotify(index.getTranslogLocation(), forced -> {
                assertEquals("foobar", threadPool.getThreadContext().getHeader("test"));
                latch.countDown();
            }));
        }
        assertNull(threadPool.getThreadContext().getHeader("test"));
        assertEquals(1, latch.getCount());
        engine.refresh("I said so");
        latch.await();
    }

    public void testTooMany() throws Exception {
        assertEquals(0, listeners.pendingCount());
        assertFalse(listeners.refreshNeeded());
        Engine.IndexResult index = index("1");

        // Fill the listener slots
        List<DummyRefreshListener> nonForcedListeners = new ArrayList<>(maxListeners);
        for (int i = 0; i < maxListeners; i++) {
            DummyRefreshListener listener = new DummyRefreshListener();
            nonForcedListeners.add(listener);
            listeners.addOrNotify(index.getTranslogLocation(), listener);
            assertTrue(listeners.refreshNeeded());
        }

        // We shouldn't have called any of them
        for (DummyRefreshListener listener : nonForcedListeners) {
            assertNull("Called listener too early!", listener.forcedRefresh.get());
        }
        assertEquals(maxListeners, listeners.pendingCount());

        // Add one more listener which should cause a refresh.
        DummyRefreshListener forcingListener = new DummyRefreshListener();
        listeners.addOrNotify(index.getTranslogLocation(), forcingListener);
        assertTrue("Forced listener wasn't forced?", forcingListener.forcedRefresh.get());
        forcingListener.assertNoError();

        // That forces all the listeners through. It would be on the listener ThreadPool but we've made all of those execute immediately.
        for (DummyRefreshListener listener : nonForcedListeners) {
            assertEquals("Expected listener called with unforced refresh!", Boolean.FALSE, listener.forcedRefresh.get());
            listener.assertNoError();
        }
        assertFalse(listeners.refreshNeeded());
        assertEquals(0, listeners.pendingCount());
    }

    public void testClose() throws Exception {
        assertEquals(0, listeners.pendingCount());
        Engine.IndexResult refreshedOperation = index("1");
        engine.refresh("I said so");
        Engine.IndexResult unrefreshedOperation = index("1");
        {
            /* Closing flushed pending listeners as though they were refreshed. Since this can only happen when the index is closed and no
             * longer useful there doesn't seem much point in sending the listener some kind of "I'm closed now, go away" enum value. */
            DummyRefreshListener listener = new DummyRefreshListener();
            assertFalse(listeners.addOrNotify(unrefreshedOperation.getTranslogLocation(), listener));
            assertNull(listener.forcedRefresh.get());
            listeners.close();
            assertFalse(listener.forcedRefresh.get());
            listener.assertNoError();
            assertFalse(listeners.refreshNeeded());
            assertEquals(0, listeners.pendingCount());
        }
        {
            // If you add a listener for an already refreshed location then it'll just fire even if closed
            DummyRefreshListener listener = new DummyRefreshListener();
            assertTrue(listeners.addOrNotify(refreshedOperation.getTranslogLocation(), listener));
            assertFalse(listener.forcedRefresh.get());
            listener.assertNoError();
            assertFalse(listeners.refreshNeeded());
            assertEquals(0, listeners.pendingCount());
        }
        {
            // But adding a listener to a non-refreshed location will fail
            DummyRefreshListener listener = new DummyRefreshListener();
            Exception e = expectThrows(IllegalStateException.class, () ->
                listeners.addOrNotify(unrefreshedOperation.getTranslogLocation(), listener));
            assertEquals("can't wait for refresh on a closed index", e.getMessage());
            assertNull(listener.forcedRefresh.get());
            assertFalse(listeners.refreshNeeded());
            assertEquals(0, listeners.pendingCount());
        }
    }

    /**
     * Attempts to add a listener at the same time as a refresh occurs by having a background thread force a refresh as fast as it can while
     * adding listeners. This can catch the situation where a refresh happens right as the listener is being added such that the listener
     * misses the refresh and has to catch the next one. If the listener wasn't able to properly catch the next one then this would fail.
     */
    public void testConcurrentRefresh() throws Exception {
        AtomicBoolean run = new AtomicBoolean(true);
        Thread refresher = new Thread(() -> {
            while (run.get()) {
                engine.refresh("test");
            }
        });
        refresher.start();
        try {
            for (int i = 0; i < 1000; i++) {
                Engine.IndexResult index = index("1");
                DummyRefreshListener listener = new DummyRefreshListener();
                boolean immediate = listeners.addOrNotify(index.getTranslogLocation(), listener);
                if (immediate) {
                    assertNotNull(listener.forcedRefresh.get());
                } else {
                    assertBusy(() -> assertNotNull(listener.forcedRefresh.get()));
                }
                assertFalse(listener.forcedRefresh.get());
                listener.assertNoError();
            }
        } finally {
            run.set(false);
            refresher.join();
        }
    }

    /**
     * Uses a bunch of threads to index, wait for refresh, and non-realtime get documents to validate that they are visible after waiting
     * regardless of what crazy sequence of events causes the refresh listener to fire.
     */
    public void testLotsOfThreads() throws Exception {
        int threadCount = between(3, 10);
        maxListeners = between(1, threadCount * 2);

        // This thread just refreshes every once in a while to cause trouble.
        Cancellable refresher = threadPool.scheduleWithFixedDelay(() -> engine.refresh("because test"), timeValueMillis(100), Names.SAME);

        // These threads add and block until the refresh makes the change visible and then do a non-realtime get.
        Thread[] indexers = new Thread[threadCount];
        for (int thread = 0; thread < threadCount; thread++) {
            final String threadId = String.format(Locale.ROOT, "%04d", thread);
            indexers[thread] = new Thread(() -> {
                for (int iteration = 1; iteration <= 50; iteration++) {
                    try {
                        String testFieldValue = String.format(Locale.ROOT, "%s%04d", threadId, iteration);
                        Engine.IndexResult index = index(threadId, testFieldValue);
                        assertEquals(iteration, index.getVersion());

                        DummyRefreshListener listener = new DummyRefreshListener();
                        listeners.addOrNotify(index.getTranslogLocation(), listener);
                        assertBusy(() -> assertNotNull("listener never called", listener.forcedRefresh.get()));
                        if (threadCount < maxListeners) {
                            assertFalse(listener.forcedRefresh.get());
                        }
                        listener.assertNoError();

                        Engine.Get get = new Engine.Get(false, "test", threadId, new Term(IdFieldMapper.NAME, threadId));
                        try (Engine.GetResult getResult = engine.get(get, engine::acquireSearcher)) {
                            assertTrue("document not found", getResult.exists());
                            assertEquals(iteration, getResult.version());
                            SingleFieldsVisitor visitor = new SingleFieldsVisitor("test");
                            getResult.docIdAndVersion().context.reader().document(getResult.docIdAndVersion().docId, visitor);
                            assertEquals(Arrays.asList(testFieldValue), visitor.fields().get("test"));
                        }
                    } catch (Exception t) {
                        throw new RuntimeException("failure on the [" + iteration + "] iteration of thread [" + threadId + "]", t);
                    }
                }
            });
            indexers[thread].start();
        }

        for (Thread indexer: indexers) {
            indexer.join();
        }
        refresher.cancel();
    }

    private Engine.IndexResult index(String id) throws IOException {
        return index(id, "test");
    }

    private Engine.IndexResult index(String id, String testFieldValue) throws IOException {
        Document document = new Document();
        document.add(new TextField("test", testFieldValue, Field.Store.YES));
        Field idField = new Field("_id", id, IdFieldMapper.Defaults.FIELD_TYPE);
        Field versionField = new NumericDocValuesField("_version", Versions.MATCH_ANY);
        SeqNoFieldMapper.SequenceIDFields seqID = SeqNoFieldMapper.SequenceIDFields.emptySeqID();
        document.add(idField);
        document.add(versionField);
        document.add(seqID.seqNo);
        document.add(seqID.seqNoDocValue);
        document.add(seqID.primaryTerm);
        BytesReference source = new BytesArray(new byte[] { 1 });
        ParsedDocument doc = new ParsedDocument(versionField, seqID, id, "test", null, Arrays.asList(document), source, XContentType.JSON,
            null);
        Engine.Index index = new Engine.Index(new Term("_id", doc.id()), doc);
        return engine.index(index);
    }

    private static class DummyRefreshListener implements Consumer<Boolean> {
        /**
         * When the listener is called this captures it's only argument.
         */
        final AtomicReference<Boolean> forcedRefresh = new AtomicReference<>();
        private volatile Exception error;

        @Override
        public void accept(Boolean forcedRefresh) {
            try {
                Boolean oldValue = this.forcedRefresh.getAndSet(forcedRefresh);
                assertNull("Listener called twice", oldValue);
            } catch (Exception e) {
                error = e;
            }
        }

        public void assertNoError() {
            if (error != null) {
                throw new RuntimeException(error);
            }
        }
    }
}
