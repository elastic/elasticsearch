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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.Segment;
import org.elasticsearch.index.engine.ThreadPoolMergeExecutorService;
import org.elasticsearch.index.shard.ShardId;
import org.junit.After;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

public class MergedSegmentWarmerTests extends AbstractEngineTestCase {

    private record Notification(String segmentName, ActionListener<Void> listener) {}

    private boolean engineCreated = false;

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

    private IndexEngine newEngine(MergedSegmentListener listener) throws IOException {
        engineCreated = true;
        final var mergePolicy = new LogDocMergePolicy();
        mergePolicy.setMergeFactor(2);
        mergePolicy.setMinMergeDocs(1);
        // the thread pool merge scheduler holds merges when the disk of the host is nearly full
        final var nodeSettings = Settings.builder()
            .put(ThreadPoolMergeExecutorService.INDICES_MERGE_DISK_CHECK_INTERVAL_SETTING.getKey(), "0s");
        final var config = indexConfig(Settings.EMPTY, nodeSettings.build(), () -> 1L, mergePolicy);
        return newIndexEngine(
            EngineConfig.builder(config).mergedSegmentWarmer(new MergedSegmentWarmer(config.getShardId(), listener)).build()
        );
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
        try (var engine = newEngine((segment, listener) -> {
            notified.add(segment.getSegmentName());
            listener.onResponse(null);
        })) {
            indexDocs(engine, between(4, 20));
            assertNotNull(notified.poll(30, TimeUnit.SECONDS));

            forceMerge(engine);
            final var segments = segmentNames(engine);
            assertThat(segments, hasSize(1));
            assertThat(notified, hasItem(segments.iterator().next()));
        }
    }

    public void testMergedSegmentIsNotVisibleUntilTheListenerIsCompleted() throws Exception {
        final var notifications = new LinkedBlockingQueue<Notification>();
        try (var engine = newEngine((segment, listener) -> notifications.add(new Notification(segment.getSegmentName(), listener)))) {
            try {
                indexDocs(engine, 2);
                final var notification = notifications.poll(30, TimeUnit.SECONDS);
                assertNotNull(notification);
                assertThat(segmentNames(engine), not(hasItem(notification.segmentName())));

                notification.listener().onResponse(null);
                assertBusy(() -> assertThat(segmentNames(engine), hasItem(notification.segmentName())));
            } finally {
                Notification pending;
                while ((pending = notifications.poll()) != null) {
                    pending.listener().onResponse(null);
                }
            }
        }
    }

    public void testFailingListenerDoesNotFailTheMerge() throws Exception {
        final var failure = new IllegalStateException("simulated");
        final boolean throwsFailure = randomBoolean();
        final Set<String> notified = ConcurrentCollections.newConcurrentSet();
        try (var engine = newEngine((segment, listener) -> {
            notified.add(segment.getSegmentName());
            if (throwsFailure) {
                throw failure;
            }
            listener.onFailure(failure);
        })) {
            indexDocs(engine, between(4, 20));
            forceMerge(engine);

            final var segments = segmentNames(engine);
            assertThat(segments, hasSize(1));
            assertThat(notified, hasItem(segments.iterator().next()));
            assertNull(((CapturingEngineEventListener) engine.config().getEventListener()).exception.get());
        }
    }

    public void testInterruptWhileWaitingAbortsTheMerge() throws Exception {
        final var pendingListeners = new LinkedBlockingQueue<ActionListener<Void>>();
        final var warmer = new MergedSegmentWarmer(new ShardId("index", "_na_", 0), (segment, listener) -> pendingListeners.add(listener));
        try (var directory = newDirectory()) {
            try (var writer = new IndexWriter(directory, new IndexWriterConfig())) {
                writer.addDocument(new Document());
            }
            try (var reader = DirectoryReader.open(directory)) {
                final var thrown = new AtomicReference<Exception>();
                final var interrupted = new AtomicBoolean();
                // plays the merge thread
                final var mergeThread = new Thread(() -> {
                    try {
                        warmer.warm(reader.leaves().getFirst().reader());
                    } catch (Exception e) {
                        thrown.set(e);
                    }
                    interrupted.set(Thread.currentThread().isInterrupted());
                });
                mergeThread.start();

                final var listener = pendingListeners.poll(30, TimeUnit.SECONDS);
                assertNotNull(listener);
                mergeThread.interrupt();
                mergeThread.join(TimeUnit.SECONDS.toMillis(30));
                assertFalse(mergeThread.isAlive());

                assertThat(thrown.get(), instanceOf(MergePolicy.MergeAbortedException.class));
                assertTrue(interrupted.get());
                listener.onResponse(null);
            }
        }
    }
}
