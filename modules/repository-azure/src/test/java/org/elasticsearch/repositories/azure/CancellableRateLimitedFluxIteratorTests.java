/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories.azure;

import reactor.core.publisher.Flux;

import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class CancellableRateLimitedFluxIteratorTests extends ESTestCase {
    private ThreadPool threadPool;

    @Before
    public void createThreadPool() {
        threadPool = new TestThreadPool(getTestName());
    }

    @After
    public void tearDownThreadPool() {
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    public void testConsumeAllElements() {
        Set<Integer> cleanedElements = new HashSet<>();
        CancellableRateLimitedFluxIterator<Integer> iterator = new CancellableRateLimitedFluxIterator<>(2, cleanedElements::add);

        List<Integer> numbers = randomList(randomIntBetween(1, 20), ESTestCase::randomInt);
        Flux.fromIterable(numbers).subscribe(iterator);

        int consumedElements = 0;
        while (iterator.hasNext()) {
            Integer number = numbers.get(consumedElements++);
            Integer next = iterator.next();
            assertThat(next, equalTo(number));
        }

        assertThat(consumedElements, equalTo(numbers.size()));
        assertThat(cleanedElements, is(empty()));
        assertThat(iterator.getQueue(), is(empty()));
        iterator.cancel();
    }

    public void testItRequestsUpstreamInBatches() {
        final int elementsPerBatch = randomIntBetween(4, 10);
        final Set<Integer> cleanedElements = new HashSet<>();
        final CancellableRateLimitedFluxIterator<Integer> iterator = new CancellableRateLimitedFluxIterator<>(
            elementsPerBatch,
            cleanedElements::add
        );

        final int providedElements = randomIntBetween(0, elementsPerBatch - 1);
        Publisher<Integer> publisher = s -> runOnNewThread(() -> {
            s.onSubscribe(new Subscription() {
                @Override
                public void request(long n) {
                    assertThat(n, equalTo((long) elementsPerBatch));
                    // Provide less elements than requested and complete
                    for (int i = 0; i < providedElements; i++) {
                        s.onNext(i);
                    }
                    s.onComplete();
                }

                @Override
                public void cancel() {

                }
            });
        });
        publisher.subscribe(iterator);

        final List<Integer> consumedElements = new ArrayList<>();
        while (iterator.hasNext()) {
            consumedElements.add(iterator.next());
        }

        assertThat(consumedElements.size(), equalTo(providedElements));
        // Elements are provided in order
        for (int i = 0; i < providedElements; i++) {
            assertThat(consumedElements.get(i), equalTo(i));
        }
        assertThat(cleanedElements, is(empty()));
        assertThat(iterator.getQueue(), is(empty()));
    }

    public void testErrorPath() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        Publisher<Integer> publisher = s -> runOnNewThread(() -> {
            s.onSubscribe(new Subscription() {
                @Override
                public void request(long n) {
                    assertThat(n, equalTo(2L));
                    s.onNext(1);
                    s.onNext(2);

                    try {
                        latch.await();
                    } catch (InterruptedException e) {
                        assert false;
                    }

                    runOnNewThread(() -> s.onError(new IOException("FAILED")));
                }

                @Override
                public void cancel() {

                }
            });
        });

        Set<Integer> cleaning = new HashSet<>();
        CancellableRateLimitedFluxIterator<Integer> iterator = new CancellableRateLimitedFluxIterator<>(2, cleaning::add);
        publisher.subscribe(iterator);

        assertThat(iterator.hasNext(), equalTo(true));
        assertThat(iterator.next(), equalTo(1));

        latch.countDown();
        // noinspection ResultOfMethodCallIgnored
        assertBusy(() -> expectThrows(RuntimeException.class, iterator::hasNext));
        assertThat(cleaning, equalTo(Set.of(2)));
        assertThat(iterator.getQueue(), is(empty()));
        iterator.cancel();
    }

    public void testCancellation() throws Exception {
        int requestedElements = 4;
        Publisher<Integer> publisher = s -> runOnNewThread(() -> {
            s.onSubscribe(new Subscription() {
                final CountDownLatch cancellationLatch = new CountDownLatch(1);

                @Override
                public void request(long n) {
                    assertThat(n, equalTo((long) requestedElements));
                    s.onNext(1);
                    s.onNext(2);
                    try {
                        cancellationLatch.await();
                    } catch (InterruptedException e) {
                        assert false;
                    }

                    runOnNewThread(() -> {
                        // It's possible that extra elements are emitted after cancellation
                        s.onNext(3);
                        s.onNext(4);
                        s.onComplete();
                    });
                }

                @Override
                public void cancel() {
                    cancellationLatch.countDown();
                }
            });
        });

        final Set<Integer> cleanedElements = ConcurrentCollections.newConcurrentSet();
        CancellableRateLimitedFluxIterator<Integer> iterator = new CancellableRateLimitedFluxIterator<>(
            requestedElements,
            cleanedElements::add
        );
        publisher.subscribe(iterator);

        assertThat(iterator.hasNext(), equalTo(true));
        assertThat(iterator.next(), equalTo(1));
        assertThat(iterator.next(), equalTo(2));
        iterator.cancel();
        assertThat(iterator.hasNext(), equalTo(false));

        assertBusy(() -> assertThat(cleanedElements, equalTo(Set.of(3, 4))));
        assertThat(iterator.getQueue(), is(empty()));
    }

    public void testErrorAfterCancellation() throws Exception {
        int requestedElements = 4;
        final AtomicBoolean cancelled = new AtomicBoolean();
        Publisher<Integer> publisher = s -> runOnNewThread(() -> {
            s.onSubscribe(new Subscription() {
                final CountDownLatch cancellationLatch = new CountDownLatch(1);

                @Override
                public void request(long n) {
                    assertThat(n, equalTo((long) requestedElements));
                    s.onNext(1);
                    s.onNext(2);
                    try {
                        cancellationLatch.await();
                    } catch (InterruptedException e) {
                        assert false;
                    }

                    runOnNewThread(() -> {
                        // It's still possible that an error is emitted after cancelling the subscription
                        s.onNext(3);
                        s.onError(new RuntimeException("Error!"));
                    });
                }

                @Override
                public void cancel() {
                    cancelled.set(true);
                    cancellationLatch.countDown();
                }
            });
        });

        Set<Integer> cleanedElements = new HashSet<>();
        CancellableRateLimitedFluxIterator<Integer> iterator = new CancellableRateLimitedFluxIterator<>(
            requestedElements,
            cleanedElements::add
        );
        publisher.subscribe(iterator);

        assertThat(iterator.hasNext(), equalTo(true));
        assertThat(iterator.next(), equalTo(1));
        assertThat(iterator.next(), equalTo(2));
        iterator.cancel();
        // noinspection ResultOfMethodCallIgnored
        assertBusy(() -> expectThrows(RuntimeException.class, iterator::hasNext));
        assertBusy(() -> assertThat(cleanedElements, equalTo(Set.of(3))));
        assertThat(iterator.getQueue(), is(empty()));
    }

    public void runOnNewThread(Runnable runnable) {
        threadPool.executor(ThreadPool.Names.GENERIC).submit(runnable);
    }
}
