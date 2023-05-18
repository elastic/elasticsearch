/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.IntStream;

public class AsyncIOProcessorTests extends ESTestCase {

    private ThreadContext threadContext;

    @Before
    public void setUpThreadContext() {
        threadContext = new ThreadContext(Settings.EMPTY);
    }

    public void testPut() throws InterruptedException {
        boolean blockInternal = randomBoolean();
        AtomicInteger received = new AtomicInteger(0);
        AsyncIOProcessor<Object> processor = new AsyncIOProcessor<Object>(logger, scaledRandomIntBetween(1, 2024), threadContext) {
            @Override
            protected void write(List<Tuple<Object, Consumer<Exception>>> candidates) throws IOException {
                if (blockInternal) {
                    synchronized (this) {
                        // TODO: check why we need a loop, can't we just use received.addAndGet(candidates.size())
                        for (int i = 0; i < candidates.size(); i++) {
                            received.incrementAndGet();
                        }
                    }
                } else {
                    received.addAndGet(candidates.size());
                }
            }
        };
        Semaphore semaphore = new Semaphore(Integer.MAX_VALUE);
        final int count = randomIntBetween(1000, 20000);
        Thread[] thread = new Thread[randomIntBetween(3, 10)];
        CountDownLatch latch = new CountDownLatch(thread.length);
        for (int i = 0; i < thread.length; i++) {
            thread[i] = new Thread() {
                @Override
                public void run() {
                    try {
                        latch.countDown();
                        latch.await();
                        for (int i = 0; i < count; i++) {
                            semaphore.acquire();
                            processor.put(new Object(), (ex) -> semaphore.release());
                        }
                    } catch (Exception ex) {
                        throw new RuntimeException(ex);
                    }
                }
            };
            thread[i].start();
        }

        for (int i = 0; i < thread.length; i++) {
            thread[i].join();
        }
        assertTrue(semaphore.tryAcquire(Integer.MAX_VALUE, 10, TimeUnit.SECONDS));
        assertEquals(count * thread.length, received.get());
    }

    public void testRandomFail() throws InterruptedException {
        AtomicInteger received = new AtomicInteger(0);
        AtomicInteger failed = new AtomicInteger(0);
        AtomicInteger actualFailed = new AtomicInteger(0);
        AsyncIOProcessor<Object> processor = new AsyncIOProcessor<Object>(logger, scaledRandomIntBetween(1, 2024), threadContext) {
            @Override
            protected void write(List<Tuple<Object, Consumer<Exception>>> candidates) throws IOException {
                received.addAndGet(candidates.size());
                if (randomBoolean()) {
                    failed.addAndGet(candidates.size());
                    if (randomBoolean()) {
                        throw new IOException();
                    } else {
                        throw new RuntimeException();
                    }
                }
            }
        };
        Semaphore semaphore = new Semaphore(Integer.MAX_VALUE);
        final int count = randomIntBetween(1000, 20000);
        Thread[] thread = new Thread[randomIntBetween(3, 10)];
        CountDownLatch latch = new CountDownLatch(thread.length);
        for (int i = 0; i < thread.length; i++) {
            thread[i] = new Thread() {
                @Override
                public void run() {
                    try {
                        latch.countDown();
                        latch.await();
                        for (int i = 0; i < count; i++) {
                            semaphore.acquire();
                            processor.put(new Object(), (ex) -> {
                                if (ex != null) {
                                    actualFailed.incrementAndGet();
                                }
                                semaphore.release();
                            });
                        }
                    } catch (Exception ex) {
                        throw new RuntimeException(ex);
                    }
                }
            };
            thread[i].start();
        }

        for (int i = 0; i < thread.length; i++) {
            thread[i].join();
        }
        assertTrue(semaphore.tryAcquire(Integer.MAX_VALUE, 10, TimeUnit.SECONDS));
        assertEquals(count * thread.length, received.get());
        assertEquals(actualFailed.get(), failed.get());
    }

    public void testConsumerCanThrowExceptions() {
        AtomicInteger received = new AtomicInteger(0);
        AtomicInteger notified = new AtomicInteger(0);

        AsyncIOProcessor<Object> processor = new AsyncIOProcessor<Object>(logger, scaledRandomIntBetween(1, 2024), threadContext) {
            @Override
            protected void write(List<Tuple<Object, Consumer<Exception>>> candidates) throws IOException {
                received.addAndGet(candidates.size());
            }
        };
        processor.put(new Object(), (e) -> {
            notified.incrementAndGet();
            throw new RuntimeException();
        });
        processor.put(new Object(), (e) -> {
            notified.incrementAndGet();
            throw new RuntimeException();
        });
        assertEquals(2, notified.get());
        assertEquals(2, received.get());
    }

    public void testNullArguments() {
        AsyncIOProcessor<Object> processor = new AsyncIOProcessor<Object>(logger, scaledRandomIntBetween(1, 2024), threadContext) {
            @Override
            protected void write(List<Tuple<Object, Consumer<Exception>>> candidates) throws IOException {}
        };

        expectThrows(NullPointerException.class, () -> processor.put(null, (e) -> {}));
        expectThrows(NullPointerException.class, () -> processor.put(new Object(), null));
    }

    public void testPreserveThreadContext() throws InterruptedException {
        final int threadCount = randomIntBetween(2, 10);
        final String testHeader = "testheader";

        AtomicInteger received = new AtomicInteger(0);
        AtomicInteger notified = new AtomicInteger(0);

        CountDownLatch writeDelay = new CountDownLatch(1);
        AsyncIOProcessor<Object> processor = new AsyncIOProcessor<Object>(
            logger,
            scaledRandomIntBetween(threadCount - 1, 2024),
            threadContext
        ) {
            @Override
            protected void write(List<Tuple<Object, Consumer<Exception>>> candidates) throws IOException {
                try {
                    assertTrue(writeDelay.await(10, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                received.addAndGet(candidates.size());
            }
        };

        // first thread blocks, the rest should be non blocking.
        CountDownLatch nonBlockingDone = new CountDownLatch(randomIntBetween(0, threadCount - 1));
        List<Thread> threads = IntStream.range(0, threadCount).<Thread>mapToObj(i -> new Thread(getTestName() + "_" + i) {
            private final String response = randomAlphaOfLength(10);
            {
                setDaemon(true);
            }

            @Override
            public void run() {
                threadContext.addResponseHeader(testHeader, response);
                processor.put(new Object(), (e) -> {
                    assertEquals(Map.of(testHeader, List.of(response)), threadContext.getResponseHeaders());
                    notified.incrementAndGet();
                });
                nonBlockingDone.countDown();
            }
        }).toList();
        threads.forEach(Thread::start);
        assertTrue(nonBlockingDone.await(10, TimeUnit.SECONDS));
        writeDelay.countDown();
        threads.forEach(t -> {
            try {
                t.join(20000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        assertEquals(threadCount, notified.get());
        assertEquals(threadCount, received.get());
        threads.forEach(t -> assertFalse(t.isAlive()));
    }

    public void testSlowConsumer() {
        AtomicInteger received = new AtomicInteger(0);
        AtomicInteger notified = new AtomicInteger(0);

        AsyncIOProcessor<Object> processor = new AsyncIOProcessor<Object>(logger, scaledRandomIntBetween(1, 2024), threadContext) {
            @Override
            protected void write(List<Tuple<Object, Consumer<Exception>>> candidates) throws IOException {
                received.addAndGet(candidates.size());
            }
        };

        int threadCount = randomIntBetween(2, 10);
        CyclicBarrier barrier = new CyclicBarrier(threadCount);
        Semaphore serializePutSemaphore = new Semaphore(1);
        List<Thread> threads = IntStream.range(0, threadCount).<Thread>mapToObj(i -> new Thread(getTestName() + "_" + i) {
            {
                setDaemon(true);
            }

            @Override
            public void run() {
                try {
                    assertTrue(serializePutSemaphore.tryAcquire(10, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                processor.put(new Object(), (e) -> {
                    serializePutSemaphore.release();
                    try {
                        barrier.await(10, TimeUnit.SECONDS);
                    } catch (InterruptedException | BrokenBarrierException | TimeoutException ex) {
                        throw new RuntimeException(ex);
                    }
                    notified.incrementAndGet();
                });
            }
        }).toList();
        threads.forEach(Thread::start);
        threads.forEach(t -> {
            try {
                t.join(20000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        assertEquals(threadCount, notified.get());
        assertEquals(threadCount, received.get());
        threads.forEach(t -> assertFalse(t.isAlive()));
    }
}
