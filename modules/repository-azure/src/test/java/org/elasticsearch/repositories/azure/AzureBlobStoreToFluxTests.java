/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.azure;

import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.repositories.azure.executors.ReactorScheduledExecutorService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;
import org.reactivestreams.Subscription;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.empty;

/**
 * Verifies that the {@link Flux} of upload buffers built by {@link AzureBlobStore#toFlux} delivers every buffer exactly once and in order
 * when it is consumed the way reactor-netty consumes a request body: {@code MonoSendMany} requests 128 buffers up front and then, from the
 * Netty event loop, 64 more every time 64 buffers have been written. With the {@code repository_azure} backed scheduler that
 * {@link AzureClientProvider} installs, those refill requests run on a different thread than the one producing the buffers, so the
 * operators in the chain must tolerate concurrent demand. A duplicated or dropped buffer here corresponds to a corrupt blob in the object
 * store, because the body length still matches.
 */
public class AzureBlobStoreToFluxTests extends ESTestCase {

    /** Demand pattern of {@code reactor.netty.channel.MonoSendMany}: {@code MonoSend.MAX_SIZE} up front, then {@code REFILL_SIZE}. */
    private static final int INITIAL_REQUEST = 128;
    private static final int REFILL_REQUEST = 64;

    /** Small buffers keep the test fast; the race window is widest when a buffer is produced quickly. */
    private static final int BUFFER_SIZE = 64;

    private ThreadPool threadPool;

    @Before
    public void createThreadPool() {
        threadPool = new TestThreadPool(
            getTestName(),
            AzureRepositoryPlugin.executorBuilder(Settings.EMPTY),
            AzureRepositoryPlugin.nettyEventLoopExecutorBuilder(Settings.EMPTY)
        );
        AzureClientProvider.installSchedulersFactory(
            new ReactorScheduledExecutorService(threadPool, AzureRepositoryPlugin.REPOSITORY_THREAD_POOL_NAME)
        );
    }

    @After
    public void terminateThreadPool() {
        Schedulers.resetFactory();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    public void testBuffersAreDeliveredOnceAndInOrderUnderConcurrentDemand() throws Exception {
        final int fullBuffers = 2000;
        final long length = (long) fullBuffers * BUFFER_SIZE + randomIntBetween(8, BUFFER_SIZE);
        final int subscriptions = 1000;
        final int concurrentSubscriptions = 8;
        final Executor eventLoop = threadPool.executor(AzureRepositoryPlugin.NETTY_EVENT_LOOP_THREAD_POOL_NAME);

        final List<String> problems = new ArrayList<>();
        for (int first = 0; first < subscriptions; first += concurrentSubscriptions) {
            final CountDownLatch done = new CountDownLatch(concurrentSubscriptions);
            final List<RecordingSubscriber> subscribers = new ArrayList<>();
            for (int i = 0; i < concurrentSubscriptions; i++) {
                final long seed = randomLong();
                final RecordingSubscriber subscriber = new RecordingSubscriber(first + i, eventLoop, done);
                subscribers.add(subscriber);
                AzureBlobStore.toFlux(() -> new SelfDescribingStream(length, seed), length, BUFFER_SIZE).subscribe(subscriber);
            }
            assertTrue("uploads did not complete", done.await(60, TimeUnit.SECONDS));
            for (RecordingSubscriber subscriber : subscribers) {
                subscriber.verify(length, problems);
            }
        }
        assertThat(subscriptions + " subscriptions, corrupt bodies:\n" + String.join("\n", problems), problems, empty());
    }

    /**
     * Consumes the buffers like {@code MonoSendMany}: requests {@link #INITIAL_REQUEST} on subscribe and, from the event loop executor,
     * {@link #REFILL_REQUEST} more every {@link #REFILL_REQUEST} buffers received.
     */
    private static final class RecordingSubscriber extends BaseSubscriber<ByteBuffer> {
        private final int id;
        private final Executor eventLoop;
        private final CountDownLatch done;
        private final List<ByteBuffer> received = Collections.synchronizedList(new ArrayList<>());
        private volatile Throwable error;
        private int sinceRefill;

        RecordingSubscriber(int id, Executor eventLoop, CountDownLatch done) {
            this.id = id;
            this.eventLoop = eventLoop;
            this.done = done;
        }

        @Override
        protected void hookOnSubscribe(Subscription subscription) {
            request(INITIAL_REQUEST);
        }

        @Override
        protected void hookOnNext(ByteBuffer value) {
            received.add(value);
            if (++sinceRefill == REFILL_REQUEST) {
                sinceRefill = 0;
                eventLoop.execute(() -> request(REFILL_REQUEST));
            }
        }

        @Override
        protected void hookOnComplete() {
            done.countDown();
        }

        @Override
        protected void hookOnError(Throwable throwable) {
            error = throwable;
            done.countDown();
        }

        void verify(long length, List<String> problems) {
            if (error != null) {
                problems.add("subscription " + id + " failed: " + error);
                return;
            }
            final int expectedBuffers = Math.toIntExact((length + BUFFER_SIZE - 1) / BUFFER_SIZE);
            final List<String> wrong = new ArrayList<>();
            final List<String> duplicated = new ArrayList<>();
            final List<Long> missing = new ArrayList<>();
            final int[] firstIndexOfChunk = new int[expectedBuffers];
            Arrays.fill(firstIndexOfChunk, -1);
            for (int index = 0; index < received.size(); index++) {
                final ByteBuffer buffer = received.get(index);
                final long expectedOffset = (long) index * BUFFER_SIZE;
                final long carriedOffset = buffer.remaining() >= Long.BYTES ? buffer.getLong(buffer.position()) : -1;
                final int chunk = carriedOffset >= 0 && carriedOffset % BUFFER_SIZE == 0
                    ? Math.toIntExact(carriedOffset / BUFFER_SIZE)
                    : -1;
                if (chunk < 0
                    || chunk >= expectedBuffers
                    || buffer.equals(ByteBuffer.wrap(SelfDescribingStream.chunk(carriedOffset, length))) == false) {
                    wrong.add("buffer " + index + " carries unknown content");
                    continue;
                }
                if (firstIndexOfChunk[chunk] >= 0) {
                    duplicated.add(
                        "chunk at offset " + carriedOffset + " delivered at " + firstIndexOfChunk[chunk] + " and again at " + index
                    );
                } else {
                    firstIndexOfChunk[chunk] = index;
                }
                if (carriedOffset != expectedOffset && wrong.size() < 3) {
                    wrong.add(
                        "buffer "
                            + index
                            + " should carry the chunk at offset "
                            + expectedOffset
                            + " but carries the one at "
                            + carriedOffset
                    );
                }
            }
            for (int chunk = 0; chunk < expectedBuffers; chunk++) {
                if (firstIndexOfChunk[chunk] < 0) {
                    missing.add((long) chunk * BUFFER_SIZE);
                }
            }
            if (received.size() != expectedBuffers
                || wrong.isEmpty() == false
                || duplicated.isEmpty() == false
                || missing.isEmpty() == false) {
                problems.add(
                    "subscription "
                        + id
                        + " received "
                        + received.size()
                        + " of "
                        + expectedBuffers
                        + " buffers; duplicated: "
                        + duplicated
                        + "; missing chunks at offsets: "
                        + missing
                        + "; "
                        + String.join("; ", wrong)
                );
            }
        }
    }

    /**
     * A stream whose content identifies its own position: the first 8 bytes of every {@link #BUFFER_SIZE} aligned chunk are the chunk's
     * offset, the rest is derived from it. Returns short reads like the sliced streams of the stateless plugin do.
     */
    private static final class SelfDescribingStream extends InputStream {
        private final long length;
        private final Random random;
        private long position;

        SelfDescribingStream(long length, long seed) {
            this.length = length;
            this.random = new Random(seed);
        }

        static byte[] chunk(long offset, long length) {
            final byte[] bytes = new byte[(int) Math.min(BUFFER_SIZE, length - offset)];
            for (int i = 0; i < bytes.length; i++) {
                bytes[i] = byteAt(offset + i);
            }
            return bytes;
        }

        static byte byteAt(long offset) {
            final long chunkOffset = offset - offset % BUFFER_SIZE;
            final int indexInChunk = (int) (offset - chunkOffset);
            if (indexInChunk < Long.BYTES) {
                return (byte) (chunkOffset >>> (Long.SIZE - Byte.SIZE * (indexInChunk + 1)));
            }
            final long mixed = (offset + 1) * 0x9E3779B97F4A7C15L;
            return (byte) (mixed >>> 56);
        }

        @Override
        public int read() {
            if (position >= length) {
                return -1;
            }
            return byteAt(position++) & 0xFF;
        }

        @Override
        public int read(byte[] b, int off, int len) {
            // the blocking reads must stay on the repository thread pool, never on the netty event loop that issues the refill requests
            assertTrue(ThreadPool.assertCurrentThreadPool(AzureRepositoryPlugin.REPOSITORY_THREAD_POOL_NAME));
            if (len == 0) {
                return 0;
            }
            if (position >= length) {
                return -1;
            }
            int n = (int) Math.min(len, length - position);
            if (n > 1 && random.nextInt(4) == 0) {
                n = 1 + random.nextInt(n);
            }
            for (int i = 0; i < n; i++) {
                b[off + i] = byteAt(position + i);
            }
            position += n;
            return n;
        }
    }
}
