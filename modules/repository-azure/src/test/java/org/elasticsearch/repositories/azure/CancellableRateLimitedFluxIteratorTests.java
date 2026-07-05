/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.azure;

import reactor.core.publisher.Flux;

import org.elasticsearch.ExceptionsHelper;
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
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
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
        assertBusy(() -> {
            // noinspection ResultOfMethodCallIgnored
            expectThrows(RuntimeException.class, iterator::hasNext);
            assertThat(cleaning, equalTo(Set.of(2)));
        });
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
        final var runtimeException = assertThrows(RuntimeException.class, iterator::hasNext);
        assertThat(runtimeException.getCause(), instanceOf(CancellationException.class));

        assertBusy(() -> assertThat(cleanedElements, equalTo(Set.of(3, 4))));
        assertThat(iterator.getQueue(), is(empty()));
    }

    public void testErrorAfterCancellation() throws Exception {
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
                        // It's still possible that an error is emitted after cancelling the subscription
                        s.onNext(3);
                        s.onError(new RuntimeException("Error!"));
                    });
                }

                @Override
                public void cancel() {
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
        assertBusy(() -> {
            // noinspection ResultOfMethodCallIgnored
            final var hasNextException = expectThrows(RuntimeException.class, iterator::hasNext);
            assertThat(hasNextException.getCause().getMessage(), equalTo("Error!"));
            assertThat(cleanedElements, equalTo(Set.of(3)));
        });
        assertThat(iterator.getQueue(), is(empty()));
    }

    public void testConcurrentErrorAndHasNext() {
        final var startBarrier = new CyclicBarrier(2);
        final var endBarrier = new CyclicBarrier(3);
        final var running = new AtomicBoolean(true);
        final var outstandingRequests = new AtomicInteger(0);
        for (int i = 0; i < 20; i++) {
            outstandingRequests.set(0);
            running.set(true);
            final var iterator = new CancellableRateLimitedFluxIterator<>(randomIntBetween(1, 10), nextItem -> {});
            final var terminationError = new Exception("This is the end");
            final Subscription subscription = new Subscription() {
                @Override
                public void request(long n) {
                    outstandingRequests.addAndGet((int) n);
                }

                @Override
                public void cancel() {}
            };

            // producer thread
            runOnNewThread(() -> {
                while (running.get()) {
                    if (outstandingRequests.get() > 0) {
                        iterator.onNext(randomInt());
                        outstandingRequests.decrementAndGet();
                    }
                }
                logger.info("--> Sending termination error");
                iterator.onError(terminationError);
                safeAwait(endBarrier);
            });

            logger.info("--> Starting iteration {}", i);

            // consumer thread
            runOnNewThread(() -> {
                safeAwait(startBarrier);
                iterator.onSubscribe(subscription);
                int counter = 0;
                try {
                    while (iterator.hasNext()) {
                        iterator.next();
                        counter++;
                    }
                    ExceptionsHelper.maybeDieOnAnotherThread(new AssertionError("Should never reach the end"));
                } catch (Exception e) {
                    if (e.getCause() != terminationError) {
                        ExceptionsHelper.maybeDieOnAnotherThread(new AssertionError("Got wrong exception", e));
                    }
                }
                logger.info("--> Got {} elements", counter);
                safeAwait(endBarrier);
            });

            safeAwait(startBarrier);
            running.set(false);
            safeAwait(endBarrier);
        }
    }

    public void testCancellationAfterError() {
        final var failure = new RuntimeException("This is the end");
        final var requestLatch = new CountDownLatch(1);
        final var subscription = new Subscription() {
            @Override
            public void request(long n) {
                requestLatch.countDown();
            }

            @Override
            public void cancel() {}
        };
        final var iterator = new CancellableRateLimitedFluxIterator<>(1, nextItem -> {});

        // producer thread
        runOnNewThread(() -> {
            safeAwait(requestLatch);
            iterator.onError(failure);
        });

        // consumer thread
        runOnNewThread(() -> {
            iterator.onSubscribe(subscription);
            assertSame(failure, assertThrows(RuntimeException.class, iterator::hasNext).getCause());

            // After cancellation, we should still see the original error thrown
            iterator.cancel();
            assertSame(failure, assertThrows(RuntimeException.class, iterator::hasNext).getCause());
        });
    }

    public void testCompleteAfterCancellation() {
        final var barrier = new CyclicBarrier(2);
        final var subscription = new Subscription() {
            @Override
            public void request(long n) {
                logger.info("--> Client requesting {} elements", n);
                safeAwait(barrier); // signal request
            }

            @Override
            public void cancel() {
                logger.info("--> Client cancelled subscription");
                safeAwait(barrier); // signal cancellation
            }
        };
        final var iterator = new CancellableRateLimitedFluxIterator<>(randomIntBetween(3, 10), nextItem -> {});

        // producer thread
        runOnNewThread(() -> {
            safeAwait(barrier); // wait for request
            iterator.onNext(randomInt());
            safeAwait(barrier); // wait for cancellation
            iterator.onNext(randomInt());
            logger.info("--> Sending completion");
            iterator.onComplete();
            safeAwait(barrier);
        });

        // consumer thread
        runOnNewThread(() -> {
            iterator.onSubscribe(subscription);
            assertTrue(iterator.hasNext());
            iterator.cancel();
            assertThat(assertThrows(RuntimeException.class, iterator::hasNext).getCause(), instanceOf(CancellationException.class));
            safeAwait(barrier); // wait for after-cancel-complete
            // After completion, we should still see the cancellation exception
            assertThat(assertThrows(RuntimeException.class, iterator::hasNext).getCause(), instanceOf(CancellationException.class));
        });
    }

    public void testCancelAfterCompletion() {
        final var barrier = new CyclicBarrier(2);
        final var subscription = new Subscription() {
            @Override
            public void request(long n) {
                logger.info("--> Client requesting {} elements", n);
                safeAwait(barrier); // signal request
            }

            @Override
            public void cancel() {
                logger.info("--> Client cancelled subscription");
            }
        };
        int elementsPerBatch = randomIntBetween(3, 10);
        final var iterator = new CancellableRateLimitedFluxIterator<>(elementsPerBatch, nextItem -> {});

        final int elementsInStream = randomIntBetween(0, elementsPerBatch);
        // producer thread
        runOnNewThread(() -> {
            safeAwait(barrier); // wait for request
            for (int i = 0; i < elementsInStream; i++) {
                iterator.onNext(randomInt());
            }
            logger.info("--> Sending completion");
            iterator.onComplete();
            safeAwait(barrier); // signal completion
        });

        // consumer thread
        runOnNewThread(() -> {
            iterator.onSubscribe(subscription);
            if (randomBoolean()) {
                assertEquals(elementsInStream > 0, iterator.hasNext());
            }
            safeAwait(barrier); // wait for completion
            logger.info("--> Cancelling subscription");
            iterator.cancel();
            assertThat(assertThrows(RuntimeException.class, iterator::hasNext).getCause(), instanceOf(CancellationException.class));
        });
    }

    public void runOnNewThread(Runnable runnable) {
        threadPool.executor(ThreadPool.Names.GENERIC).submit(runnable);
    }
}
