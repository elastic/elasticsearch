/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.engine;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.Segment;
import org.elasticsearch.index.engine.ThreadPoolMergeExecutorService;
import org.elasticsearch.index.shard.ShardId;
import org.junit.After;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

public class MergedSegmentWarmerTests extends AbstractEngineTestCase {

    private record Notification(String segmentName, BooleanSupplier isAborted, ActionListener<Void> listener) {}

    private static MergedSegmentListener recordingTo(LinkedBlockingQueue<Notification> notifications) {
        return (segment, isAborted, listener) -> notifications.add(new Notification(segment.getSegmentName(), isAborted, listener));
    }

    private boolean engineCreated = false;
    private final List<MergedSegmentWarmer> warmers = new ArrayList<>();

    @After
    public void assertWarningHeaders() {
        // only the engines of AbstractEngineTestCase set the deprecated setting
        if (engineCreated) {
            assertWarnings(
                "[indices.merge.scheduler.use_thread_pool] setting was deprecated in Elasticsearch and will be removed in a future "
                    + "release. See the breaking changes documentation for the next major version."
            );
        }
    }

    @After
    public void assertNoTrackedMerges() {
        for (var warmer : warmers) {
            assertFalse(warmer.hasTrackedMerges());
        }
    }

    private MergedSegmentWarmer newWarmer(ShardId shardId, MergedSegmentListener listener) {
        final var warmer = new MergedSegmentWarmer(shardId, listener);
        warmers.add(warmer);
        return warmer;
    }

    private IndexEngine newEngine(MergedSegmentListener listener) throws IOException {
        engineCreated = true;
        final var mergePolicy = new LogDocMergePolicy();
        mergePolicy.setMergeFactor(2);
        mergePolicy.setMinMergeDocs(1);
        // the thread pool merge scheduler holds merges when the disk of the host is nearly full
        final var nodeSettings = Settings.builder()
            .put(ThreadPoolMergeExecutorService.INDICES_MERGE_DISK_CHECK_INTERVAL_SETTING.getKey(), "0s");
        final var config = indexConfig(Settings.EMPTY, nodeSettings.build(), () -> 1L, mergePolicy);
        return newIndexEngine(EngineConfig.builder(config).mergedSegmentWarmer(newWarmer(config.getShardId(), listener)).build());
    }

    /**
     * Indexes each document in its own segment, which triggers merges.
     */
    private static void indexDocs(IndexEngine engine, int numDocs) throws IOException {
        for (int i = 0; i < numDocs; i++) {
            engine.index(randomDoc(UUIDs.randomBase64UUID()));
            engine.refresh("test");
        }
    }

    private static Set<String> segmentNames(IndexEngine engine) {
        engine.refresh("test");
        return engine.segments().stream().filter(Segment::isSearch).map(Segment::getName).collect(Collectors.toSet());
    }

    private static void forceMerge(IndexEngine engine) throws IOException {
        engine.forceMerge(true, 1, false, UUIDs.randomBase64UUID());
    }

    public void testMergedSegmentsAreNotified() throws Exception {
        final var notified = new LinkedBlockingQueue<String>();
        try (var engine = newEngine((segment, isAborted, listener) -> {
            notified.add(segment.getSegmentName());
            listener.onResponse(null);
        })) {
            indexDocs(engine, between(4, 20));
            safePoll(notified);

            forceMerge(engine);
            final var segments = segmentNames(engine);
            assertThat(segments, hasSize(1));
            assertThat(notified, hasItem(segments.iterator().next()));
        }
    }

    public void testMergedSegmentIsNotVisibleUntilTheListenerIsCompleted() throws Exception {
        final var notifications = new LinkedBlockingQueue<Notification>();
        try (var engine = newEngine(recordingTo(notifications))) {
            Notification notification = null;
            try {
                indexDocs(engine, 2);
                notification = safePoll(notifications);
                assertThat(segmentNames(engine), not(hasItem(notification.segmentName())));

                final var segmentName = notification.segmentName();
                final var completing = notification;
                notification = null;
                completing.listener().onResponse(null);
                assertBusy(() -> assertThat(segmentNames(engine), hasItem(segmentName)));
            } finally {
                // a listener polled but not yet completed must not be left pending, or closing the engine blocks
                if (notification != null) {
                    notification.listener().onResponse(null);
                }
                completePending(notifications);
            }
        }
    }

    public void testEngineCloseWaitsForAPendingListener() throws Exception {
        final var notifications = new LinkedBlockingQueue<Notification>();
        final var engine = newEngine(recordingTo(notifications));
        // completed with whether the listener was completed by the time the engine was closed
        final var closed = new PlainActionFuture<Boolean>();
        final var closeStarted = new AtomicBoolean();
        Notification notification = null;
        try {
            indexDocs(engine, 2);
            notification = safePoll(notifications);
            assertFalse(notification.isAborted().getAsBoolean());

            // the listener is not completed before the engine is closed
            closeStarted.set(true);
            final var listenerCompleted = new AtomicBoolean();
            new Thread(() -> ActionListener.run(closed, l -> {
                engine.close();
                l.onResponse(listenerCompleted.get());
            })).start();

            // closing the engine aborts the merge, but the merge thread keeps waiting for the listener, so the close cannot complete
            final var pending = notification;
            assertBusy(() -> assertTrue(pending.isAborted().getAsBoolean()));
            assertFalse(closed.isDone());

            notification = null;
            listenerCompleted.set(true);
            pending.listener().onResponse(null);
            assertTrue("the engine was closed before the listener was completed", safeGet(closed));
        } finally {
            if (notification != null) {
                notification.listener().onResponse(null);
            }
            completePending(notifications);
            if (closeStarted.get()) {
                safeGet(closed);
            } else {
                engine.close();
            }
        }
    }

    private static void completePending(LinkedBlockingQueue<Notification> notifications) {
        Notification pending;
        while ((pending = notifications.poll()) != null) {
            pending.listener().onResponse(null);
        }
    }

    public void testListenerFailureFailsTheEngine() throws Exception {
        final var failure = new IllegalStateException("simulated");
        final boolean throwsFailure = randomBoolean();
        try (var engine = newEngine((segment, isAborted, listener) -> {
            if (throwsFailure) {
                throw failure;
            }
            listener.onFailure(failure);
        })) {
            indexDocs(engine, 2);
            final var eventListener = (CapturingEngineEventListener) engine.config().getEventListener();
            assertBusy(() -> assertNotNull(eventListener.exception.get()));
            assertSame(failure, ExceptionsHelper.unwrap(eventListener.exception.get(), IllegalStateException.class));
        }
    }

    private record WarmResult(Exception thrown, MergePolicy.OneMerge merge) {}

    /**
     * Warms a segment that is tracked as the one of a merge, on the calling thread, which plays the merge thread.
     */
    private WarmResult warm(MergedSegmentListener listener) throws IOException {
        final var warmer = newWarmer(new ShardId("index", "_na_", 0), listener);
        try (var directory = newDirectory()) {
            try (var writer = new IndexWriter(directory, new IndexWriterConfig())) {
                writer.addDocument(new Document());
            }
            try (var reader = DirectoryReader.open(directory)) {
                final var leaf = reader.leaves().getFirst().reader();
                final var mergedSegment = Lucene.segmentReader(leaf).getSegmentInfo();
                final var merge = new MergePolicy.OneMerge(List.of(mergedSegment));
                warmer.onMergeStarted(mergedSegment, merge);
                try {
                    warmer.warm(leaf);
                    return new WarmResult(null, merge);
                } catch (Exception e) {
                    return new WarmResult(e, merge);
                } finally {
                    warmer.onMergeFinished(mergedSegment, merge);
                }
            }
        }
    }

    public void testMergeAbortedFailureOfTheListenerIsPropagated() throws Exception {
        // a MergeAbortedException is a checked exception, so a listener can only report it by failing
        final var failure = new MergePolicy.MergeAbortedException("simulated");
        final var result = warm((segment, isAborted, listener) -> listener.onFailure(failure));

        assertSame(failure, result.thrown());
    }

    public void testOtherListenerFailuresArePropagated() throws Exception {
        final Exception failure = randomFrom(
            List.<Exception>of(
                new IllegalStateException("simulated"),
                new IOException("simulated"),
                new AlreadyClosedException("simulated"),
                new EsRejectedExecutionException("simulated", true)
            )
        );
        final boolean throwsFailure = failure instanceof RuntimeException && randomBoolean();
        final var result = warm((segment, isAborted, listener) -> {
            if (throwsFailure) {
                throw (RuntimeException) failure;
            }
            listener.onFailure(failure);
        });

        assertSame(failure, result.thrown());
        assertFalse(result.merge().isAborted());
    }

    public void testInterruptWhileWaitingAbortsTheMerge() throws Exception {
        final var pendingListeners = new LinkedBlockingQueue<ActionListener<Void>>();
        final var warmer = newWarmer(new ShardId("index", "_na_", 0), (segment, isAborted, listener) -> pendingListeners.add(listener));
        try (var directory = newDirectory()) {
            try (var writer = new IndexWriter(directory, new IndexWriterConfig())) {
                writer.addDocument(new Document());
            }
            try (var reader = DirectoryReader.open(directory)) {
                final var leaf = reader.leaves().getFirst().reader();
                final var mergedSegment = Lucene.segmentReader(leaf).getSegmentInfo();
                final var merge = new MergePolicy.OneMerge(List.of(mergedSegment));
                warmer.onMergeStarted(mergedSegment, merge);
                try {
                    final var thrown = new AtomicReference<Exception>();
                    final var interrupted = new AtomicBoolean();
                    // plays the merge thread
                    final var mergeThread = new Thread(() -> {
                        try {
                            warmer.warm(leaf);
                        } catch (Exception e) {
                            thrown.set(e);
                        }
                        interrupted.set(Thread.currentThread().isInterrupted());
                    });
                    mergeThread.start();

                    final var listener = safePoll(pendingListeners);
                    mergeThread.interrupt();
                    safeJoin(mergeThread);

                    assertThat(thrown.get(), instanceOf(MergePolicy.MergeAbortedException.class));
                    assertTrue(interrupted.get());
                    assertTrue(merge.isAborted());
                    listener.onResponse(null);
                } finally {
                    warmer.onMergeFinished(mergedSegment, merge);
                }
            }
        }
    }
}
