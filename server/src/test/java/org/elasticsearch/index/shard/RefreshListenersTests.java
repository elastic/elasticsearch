/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.shard;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.metrics.MeanMetric;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineTestCase;
import org.elasticsearch.index.engine.InternalEngine;
import org.elasticsearch.index.engine.ThreadPoolMergeExecutorService;
import org.elasticsearch.index.engine.ThreadPoolMergeScheduler;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.seqno.RetentionLeases;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogConfig;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.plugins.internal.XContentMeteringParserDecorator;
import org.elasticsearch.test.DummyShardLock;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.threadpool.Scheduler.Cancellable;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentType;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

import static org.elasticsearch.core.TimeValue.timeValueMillis;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Tests how {@linkplain RefreshListeners} interacts with {@linkplain InternalEngine}.
 */
public class RefreshListenersTests extends ESTestCase {
    private RefreshListeners listeners;
    private Engine engine;
    private volatile int maxListeners;
    private ThreadPool threadPool;
    private NodeEnvironment nodeEnvironment;
    private ThreadPoolMergeExecutorService threadPoolMergeExecutorService;
    private Store store;

    @Before
    public void setupListeners() throws Exception {
        // Setup dependencies of the listeners
        maxListeners = randomIntBetween(2, 1000);
        // Now setup the InternalEngine which is much more complicated because we aren't mocking anything
        threadPool = new TestThreadPool(getTestName());
        Settings settings = Settings.builder()
            .put(ThreadPoolMergeScheduler.USE_THREAD_POOL_MERGE_SCHEDULER_SETTING.getKey(), randomBoolean())
            .build();
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("index", settings);
        nodeEnvironment = newNodeEnvironment(settings);
        threadPoolMergeExecutorService = ThreadPoolMergeExecutorService.maybeCreateThreadPoolMergeExecutorService(
            threadPool,
            ClusterSettings.createBuiltInClusterSettings(settings),
            nodeEnvironment
        );
        listeners = new RefreshListeners(
            () -> maxListeners,
            () -> engine.refresh("too-many-listeners"),
            logger,
            threadPool.getThreadContext(),
            new MeanMetric()
        );

        ShardId shardId = new ShardId(new Index("index", "_na_"), 1);
        Directory directory = newDirectory();
        store = new Store(shardId, indexSettings, directory, new DummyShardLock(shardId));
        IndexWriterConfig iwc = newIndexWriterConfig();
        TranslogConfig translogConfig = new TranslogConfig(
            shardId,
            createTempDir("translog"),
            indexSettings,
            BigArrays.NON_RECYCLING_INSTANCE
        );
        Engine.EventListener eventListener = new Engine.EventListener() {
            @Override
            public void onFailedEngine(String reason, @Nullable Exception e) {
                // we don't need to notify anybody in this test
            }
        };
        store.createEmpty();
        final long primaryTerm = randomNonNegativeLong();
        final String translogUUID = Translog.createEmptyTranslog(
            translogConfig.getTranslogPath(),
            SequenceNumbers.NO_OPS_PERFORMED,
            shardId,
            primaryTerm
        );
        store.associateIndexWithNewTranslog(translogUUID);
        EngineConfig config = new EngineConfig(
            shardId,
            threadPool,
            threadPoolMergeExecutorService,
            indexSettings,
            null,
            store,
            newMergePolicy(),
            iwc.getAnalyzer(),
            iwc.getSimilarity(),
            new CodecService(null, BigArrays.NON_RECYCLING_INSTANCE),
            eventListener,
            IndexSearcher.getDefaultQueryCache(),
            IndexSearcher.getDefaultQueryCachingPolicy(),
            translogConfig,
            TimeValue.timeValueMinutes(5),
            Collections.singletonList(listeners),
            Collections.emptyList(),
            null,
            new NoneCircuitBreakerService(),
            () -> SequenceNumbers.NO_OPS_PERFORMED,
            () -> RetentionLeases.EMPTY,
            () -> primaryTerm,
            IndexModule.DEFAULT_SNAPSHOT_COMMIT_SUPPLIER,
            null,
            System::nanoTime,
            null,
            true,
            EngineTestCase.createMapperService(),
            new EngineResetLock()
        );
        engine = new InternalEngine(config);
        EngineTestCase.recoverFromTranslog(engine, (e, s) -> 0, Long.MAX_VALUE);
        listeners.setCurrentRefreshLocationSupplier(engine::getTranslogLastWriteLocation);
        listeners.setCurrentProcessedCheckpointSupplier(engine::getProcessedLocalCheckpoint);
        listeners.setMaxIssuedSeqNoSupplier(engine::getMaxSeqNo);
    }

    @After
    public void tearDownListeners() throws Exception {
        IOUtils.close(engine, store, nodeEnvironment, () -> terminate(threadPool));
    }

    public void testBeforeRefresh() throws Exception {
        assertEquals(0, listeners.pendingCount());
        Engine.IndexResult index = index("1");
        TestLocationListener listener = new TestLocationListener();
        assertFalse(listeners.addOrNotify(index.getTranslogLocation(), listener));
        assertNull(listener.forcedRefresh.get());
        assertEquals(1, listeners.pendingCount());

        TestSeqNoListener seqNoListener = new TestSeqNoListener();
        assertFalse(listeners.addOrNotify(index.getSeqNo(), randomBoolean(), seqNoListener));
        assertEquals(2, listeners.pendingCount());
        engine.refresh("I said so");
        assertFalse(listener.forcedRefresh.get());
        assertTrue(seqNoListener.isDone.get());
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
        TestLocationListener listener = new TestLocationListener();
        assertTrue(listeners.addOrNotify(index.getTranslogLocation(), listener));
        assertFalse(listener.forcedRefresh.get());
        TestSeqNoListener seqNoListener = new TestSeqNoListener();
        assertTrue(listeners.addOrNotify(index.getSeqNo(), randomBoolean(), seqNoListener));
        assertTrue(seqNoListener.isDone.get());
        listener.assertNoError();
        assertEquals(0, listeners.pendingCount());
    }

    public void testContextIsPreserved() throws IOException, InterruptedException {
        assertEquals(0, listeners.pendingCount());
        Engine.IndexResult index = index("1");
        CountDownLatch latch = new CountDownLatch(2);
        try (ThreadContext.StoredContext ignore = threadPool.getThreadContext().stashContext()) {
            threadPool.getThreadContext().putHeader("test", "foobar");
            assertFalse(listeners.addOrNotify(index.getTranslogLocation(), forced -> {
                assertEquals("foobar", threadPool.getThreadContext().getHeader("test"));
                latch.countDown();
            }));
            assertFalse(listeners.addOrNotify(index.getSeqNo(), randomBoolean(), ActionTestUtils.assertNoFailureListener(ignored -> {
                assertEquals("foobar", threadPool.getThreadContext().getHeader("test"));
                latch.countDown();
            })));
        }
        assertNull(threadPool.getThreadContext().getHeader("test"));
        assertEquals(2, latch.getCount());
        engine.refresh("I said so");
        latch.await();
    }

    public void testTooMany() throws Exception {
        assertEquals(0, listeners.pendingCount());
        assertFalse(listeners.refreshNeeded());
        Engine.IndexResult index = index("1");

        // Fill the listener slots
        List<TestLocationListener> nonForcedLocationListeners = new ArrayList<>(maxListeners);
        List<TestSeqNoListener> nonSeqNoLocationListeners = new ArrayList<>(maxListeners);
        for (int i = 0; i < maxListeners; i++) {
            if (randomBoolean()) {
                TestLocationListener listener = new TestLocationListener();
                nonForcedLocationListeners.add(listener);
                listeners.addOrNotify(index.getTranslogLocation(), listener);
            } else {
                TestSeqNoListener listener = new TestSeqNoListener();
                nonSeqNoLocationListeners.add(listener);
                listeners.addOrNotify(index.getSeqNo(), randomBoolean(), listener);
            }
            assertTrue(listeners.refreshNeeded());
        }

        // We shouldn't have called any of them
        for (TestLocationListener listener : nonForcedLocationListeners) {
            assertNull("Called listener too early!", listener.forcedRefresh.get());
        }
        for (TestSeqNoListener listener : nonSeqNoLocationListeners) {
            assertFalse("Called listener too early!", listener.isDone.get());
        }
        assertEquals(maxListeners, listeners.pendingCount());

        // Checkpoint version produces error if too many listeners registered
        TestSeqNoListener rejectedListener = new TestSeqNoListener();
        listeners.addOrNotify(index.getSeqNo(), randomBoolean(), rejectedListener);
        Exception error = rejectedListener.error;
        assertThat(error, instanceOf(IllegalStateException.class));

        // Add one more listener which should cause a refresh.
        TestLocationListener forcingListener = new TestLocationListener();
        listeners.addOrNotify(index.getTranslogLocation(), forcingListener);
        assertTrue("Forced listener wasn't forced?", forcingListener.forcedRefresh.get());
        forcingListener.assertNoError();

        // That forces all the listeners through. It would be on the listener ThreadPool but we've made all of those execute immediately.
        for (TestLocationListener listener : nonForcedLocationListeners) {
            assertEquals("Expected listener called with unforced refresh!", Boolean.FALSE, listener.forcedRefresh.get());
            listener.assertNoError();
        }
        for (TestSeqNoListener listener : nonSeqNoLocationListeners) {
            assertEquals("Expected listener called to be called by refresh!", Boolean.TRUE, listener.isDone.get());
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
            TestLocationListener listener = new TestLocationListener();
            assertFalse(listeners.addOrNotify(unrefreshedOperation.getTranslogLocation(), listener));
            assertNull(listener.forcedRefresh.get());

            TestSeqNoListener seqNoListener = new TestSeqNoListener();
            assertFalse(listeners.addOrNotify(unrefreshedOperation.getSeqNo(), randomBoolean(), seqNoListener));
            assertFalse(seqNoListener.isDone.get());

            listeners.close();
            assertFalse(listener.forcedRefresh.get());
            listener.assertNoError();
            assertNotNull(seqNoListener.error);
            assertThat(seqNoListener.error, instanceOf(AlreadyClosedException.class));
            assertFalse(listeners.refreshNeeded());
            assertEquals(0, listeners.pendingCount());
        }
        {
            // If you add a listener for an already refreshed location then it'll just fire even if closed
            TestLocationListener listener = new TestLocationListener();
            assertTrue(listeners.addOrNotify(refreshedOperation.getTranslogLocation(), listener));
            assertFalse(listener.forcedRefresh.get());
            listener.assertNoError();
            TestSeqNoListener seqNoListener = new TestSeqNoListener();
            assertTrue(listeners.addOrNotify(refreshedOperation.getSeqNo(), randomBoolean(), seqNoListener));
            assertTrue(seqNoListener.isDone.get());

            assertFalse(listeners.refreshNeeded());
            assertEquals(0, listeners.pendingCount());
        }
        {
            // But adding a location listener to a non-refreshed location will fail
            TestLocationListener listener = new TestLocationListener();
            Exception e = expectThrows(
                IllegalStateException.class,
                () -> listeners.addOrNotify(unrefreshedOperation.getTranslogLocation(), listener)
            );
            assertEquals("can't wait for refresh on a closed index", e.getMessage());
            assertNull(listener.forcedRefresh.get());

            // But adding a seqNo listener to a non-refreshed location will fail listener
            TestSeqNoListener seqNoListener = new TestSeqNoListener();
            listeners.addOrNotify(unrefreshedOperation.getSeqNo(), randomBoolean(), seqNoListener);
            assertEquals("can't wait for refresh on a closed index", seqNoListener.error.getMessage());

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

                boolean immediate;
                BooleanSupplier doneSupplier;
                if (randomBoolean()) {
                    TestLocationListener listener = new TestLocationListener();
                    immediate = listeners.addOrNotify(index.getTranslogLocation(), listener);
                    doneSupplier = () -> listener.forcedRefresh.get() != null;
                    if (immediate) {
                        assertNotNull(listener.forcedRefresh.get());
                        listener.assertNoError();
                    }
                } else {
                    TestSeqNoListener seqNoListener = new TestSeqNoListener();
                    immediate = listeners.addOrNotify(index.getSeqNo(), randomBoolean(), seqNoListener);
                    doneSupplier = seqNoListener.isDone::get;
                }
                if (immediate == false) {
                    assertBusy(doneSupplier::getAsBoolean, 1, TimeUnit.MINUTES);
                }
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
        Cancellable refresher = threadPool.scheduleWithFixedDelay(
            () -> engine.refresh("because test"),
            timeValueMillis(100),
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );

        // These threads add and block until the refresh makes the change visible and then do a non-realtime get.
        Thread[] indexers = new Thread[threadCount];
        for (int thread = 0; thread < threadCount; thread++) {
            final String threadId = Strings.format("%04d", thread);
            indexers[thread] = new Thread(() -> {
                for (int iteration = 1; iteration <= 50; iteration++) {
                    try {
                        String testFieldValue = Strings.format("%s%04d", threadId, iteration);
                        Engine.IndexResult index = index(threadId, testFieldValue);
                        assertEquals(iteration, index.getVersion());

                        TestLocationListener listener = new TestLocationListener();
                        listeners.addOrNotify(index.getTranslogLocation(), listener);
                        TestSeqNoListener seqNoListener = new TestSeqNoListener();
                        long processedLocalCheckpoint = engine.getProcessedLocalCheckpoint();
                        listeners.addOrNotify(processedLocalCheckpoint, randomBoolean(), seqNoListener);
                        assertBusy(() -> {
                            assertNotNull("location listener never called", listener.forcedRefresh.get());
                            assertTrue("seqNo listener never called", seqNoListener.isDone.get() || seqNoListener.error != null);
                        }, 1, TimeUnit.MINUTES);
                        if ((threadCount * 2) < maxListeners) {
                            assertFalse(listener.forcedRefresh.get());
                        }
                        listener.assertNoError();

                        Engine.Get get = new Engine.Get(false, false, threadId);
                        MapperService mapperService = EngineTestCase.createMapperService();
                        try (
                            Engine.GetResult getResult = engine.get(
                                get,
                                mapperService.mappingLookup(),
                                mapperService.documentParser(),
                                EngineTestCase.randomSearcherWrapper()
                            )
                        ) {
                            assertTrue("document not found", getResult.exists());
                            assertEquals(iteration, getResult.version());
                            org.apache.lucene.document.Document document = getResult.docIdAndVersion().reader.storedFields()
                                .document(getResult.docIdAndVersion().docId);
                            assertThat(document.getValues("test"), arrayContaining(testFieldValue));
                        }
                    } catch (Exception t) {
                        throw new RuntimeException("failure on the [" + iteration + "] iteration of thread [" + threadId + "]", t);
                    }
                }
            });
            indexers[thread].start();
        }

        for (Thread indexer : indexers) {
            indexer.join();
        }
        refresher.cancel();
    }

    public void testDisallowAddListeners() throws Exception {
        assertEquals(0, listeners.pendingCount());
        TestLocationListener listener = new TestLocationListener();
        assertFalse(listeners.addOrNotify(index("1").getTranslogLocation(), listener));
        TestSeqNoListener seqNoListener = new TestSeqNoListener();
        assertFalse(listeners.addOrNotify(index("1").getSeqNo(), randomBoolean(), seqNoListener));
        engine.refresh("I said so");
        assertFalse(listener.forcedRefresh.get());
        listener.assertNoError();
        assertTrue(seqNoListener.isDone.get());

        try (Releasable releaseable1 = listeners.forceRefreshes()) {
            listener = new TestLocationListener();
            assertTrue(listeners.addOrNotify(index("1").getTranslogLocation(), listener));
            assertTrue(listener.forcedRefresh.get());
            listener.assertNoError();
            seqNoListener = new TestSeqNoListener();
            // SeqNo listeners are not forced
            assertFalse(listeners.addOrNotify(index("1").getSeqNo(), randomBoolean(), seqNoListener));
            assertFalse(seqNoListener.isDone.get());
            assertEquals(1, listeners.pendingCount());

            try (Releasable releaseable2 = listeners.forceRefreshes()) {
                listener = new TestLocationListener();
                assertTrue(listeners.addOrNotify(index("1").getTranslogLocation(), listener));
                assertTrue(listener.forcedRefresh.get());
                listener.assertNoError();
                seqNoListener = new TestSeqNoListener();
                // SeqNo listeners are not forced
                assertFalse(listeners.addOrNotify(index("1").getSeqNo(), randomBoolean(), seqNoListener));
                assertFalse(seqNoListener.isDone.get());
                assertEquals(1, listeners.pendingCount());
            }

            listener = new TestLocationListener();
            assertTrue(listeners.addOrNotify(index("1").getTranslogLocation(), listener));
            assertTrue(listener.forcedRefresh.get());
            listener.assertNoError();
            seqNoListener = new TestSeqNoListener();
            // SeqNo listeners are not forced
            assertFalse(listeners.addOrNotify(index("1").getSeqNo(), randomBoolean(), seqNoListener));
            assertFalse(seqNoListener.isDone.get());
            assertEquals(1, listeners.pendingCount());
        }

        assertFalse(listeners.addOrNotify(index("1").getTranslogLocation(), new TestLocationListener()));
        final int expectedPending;
        if (listeners.pendingCount() == maxListeners) {
            // Rejected
            TestSeqNoListener rejected = new TestSeqNoListener();
            assertTrue(listeners.addOrNotify(index("1").getSeqNo(), randomBoolean(), rejected));
            assertNotNull(rejected.error);
            expectedPending = 2;
        } else {
            TestSeqNoListener acceptedListener = new TestSeqNoListener();
            assertFalse(listeners.addOrNotify(index("1").getSeqNo(), randomBoolean(), acceptedListener));
            assertFalse(acceptedListener.isDone.get());
            assertNull(acceptedListener.error);
            expectedPending = 3;
        }
        assertEquals(expectedPending, listeners.pendingCount());
    }

    public void testSequenceNumberMustBeIssued() throws Exception {
        assertEquals(0, listeners.pendingCount());
        TestSeqNoListener seqNoListener = new TestSeqNoListener();
        long issued = index("1").getSeqNo();
        assertTrue(listeners.addOrNotify(issued + 1, false, seqNoListener));
        assertThat(seqNoListener.error, instanceOf(IllegalArgumentException.class));
        String message = "Cannot wait for unissued seqNo checkpoint [wait_for_checkpoint="
            + (issued + 1)
            + ", max_issued_seqNo="
            + issued
            + "]";
        assertThat(seqNoListener.error.getMessage(), equalTo(message));
    }

    public void testSkipSequenceNumberMustBeIssuedCheck() throws Exception {
        assertEquals(0, listeners.pendingCount());
        TestSeqNoListener seqNoListener = new TestSeqNoListener();
        long issued = index("1").getSeqNo();
        assertFalse(listeners.addOrNotify(issued + 1, true, seqNoListener));
        index("2");
        assertFalse(seqNoListener.isDone.get());
        engine.refresh("test");
        assertTrue(seqNoListener.isDone.get());
    }

    private Engine.IndexResult index(String id) throws IOException {
        return index(id, "test");
    }

    private Engine.IndexResult index(String id, String testFieldValue) throws IOException {
        final BytesRef uid = Uid.encodeId(id);
        LuceneDocument document = new LuceneDocument();
        document.add(new TextField("test", testFieldValue, Field.Store.YES));
        Field idField = new StringField(IdFieldMapper.NAME, uid, Field.Store.YES);
        Field versionField = new NumericDocValuesField("_version", Versions.MATCH_ANY);
        var seqID = SeqNoFieldMapper.SequenceIDFields.emptySeqID(engine.config().getIndexSettings().seqNoIndexOptions());
        document.add(idField);
        document.add(versionField);
        seqID.addFields(document);
        BytesReference source = new BytesArray(new byte[] { 1 });
        ParsedDocument doc = new ParsedDocument(
            versionField,
            seqID,
            id,
            null,
            Arrays.asList(document),
            source,
            XContentType.JSON,
            null,
            XContentMeteringParserDecorator.UNKNOWN_SIZE
        );
        Engine.Index index = new Engine.Index(uid, engine.config().getPrimaryTermSupplier().getAsLong(), doc);
        return engine.index(index);
    }

    private static class TestLocationListener implements Consumer<Boolean> {
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

    private static class TestSeqNoListener implements ActionListener<Void> {

        final AtomicReference<Boolean> isDone = new AtomicReference<>(false);
        private volatile Exception error;

        @Override
        public void onResponse(Void unused) {
            isDone.set(true);
        }

        @Override
        public void onFailure(Exception e) {
            error = e;
        }
    }
}
