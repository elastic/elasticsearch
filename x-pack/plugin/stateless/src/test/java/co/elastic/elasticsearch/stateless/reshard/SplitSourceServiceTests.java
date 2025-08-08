/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.reshard;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.equalTo;

public class SplitSourceServiceTests extends ESTestCase {
    // test that a RefCountingAcquirer will only acquire the resource once if multiple acquirers arrive while the resource is held
    public void testRefCountedAcquirerAcquiresAndReleasesOnce() throws Exception {
        final var numAcquirers = randomIntBetween(1, 10);
        AtomicInteger acquired = new AtomicInteger();
        AtomicInteger released = new AtomicInteger();

        var acquirersArrived = new CountDownLatch(numAcquirers);
        // waits for all acquirers to enter RefCountedAcquirer.acquire, then returns the RefCountedAcquirer
        // a releasable that counts the number of times it has ever been fired
        Consumer<ActionListener<Releasable>> acquirer = listener -> new Thread(() -> {
            acquired.incrementAndGet();
            logger.info("acquiring {}", acquired.get());
            try {
                acquirersArrived.await(SAFE_AWAIT_TIMEOUT.seconds(), TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            listener.onResponse(() -> {
                released.incrementAndGet();
                logger.info("releasing {}", released.get());
            });
        }).start();
        var refCountedAcquirer = new SplitSourceService.RefCountedAcquirer(acquirer);

        var threads = new Thread[numAcquirers];
        // creates numAcquirers threads that will enter acquire and then complete
        var acquirersAcquired = new CountDownLatch(numAcquirers);
        for (int i = 0; i < numAcquirers; i++) {
            final int sleepMillis = randomIntBetween(1, 50);
            threads[i] = new Thread(() -> {
                try {
                    Thread.sleep(sleepMillis);
                } catch (InterruptedException ignored) {}
                refCountedAcquirer.acquire(runAndRelease(acquirersAcquired::countDown));
                acquirersArrived.countDown();
            });
            threads[i].start();
        }

        for (int i = 0; i < numAcquirers; i++) {
            threads[i].join();
        }
        acquirersAcquired.await(SAFE_AWAIT_TIMEOUT.seconds(), TimeUnit.SECONDS);

        assertBusy(() -> {
            assertThat(acquired.get(), equalTo(1));
            assertThat(released.get(), equalTo(1));
        });
    }

    // test that a RefCountingAcquirer will acquire the provided resource again after it has released it
    public void testRefCountedAcquirerCanReacquire() throws Exception {
        // counts the number of times it's been acquired and released
        final var acquired = new AtomicInteger();
        final var released = new AtomicInteger();
        var refCountedAcquirer = new SplitSourceService.RefCountedAcquirer(listener -> {
            acquired.incrementAndGet();
            listener.onResponse(released::incrementAndGet);
        });

        var acquiredLatch = new CountDownLatch(1);
        refCountedAcquirer.acquire(runAndRelease(acquiredLatch::countDown));
        acquiredLatch.await(SAFE_AWAIT_TIMEOUT.seconds(), TimeUnit.SECONDS);
        assertBusy(() -> {
            assertThat(acquired.get(), equalTo(1));
            assertThat(released.get(), equalTo(1));
        });

        acquiredLatch = new CountDownLatch(1);
        refCountedAcquirer.acquire(runAndRelease(acquiredLatch::countDown));
        acquiredLatch.await(SAFE_AWAIT_TIMEOUT.seconds(), TimeUnit.SECONDS);
        assertBusy(() -> {
            assertThat(acquired.get(), equalTo(2));
            assertThat(released.get(), equalTo(2));
        });
    }

    // test that a RefCountingAcquirer releases its resource if it acquires it
    public void testRefCountedAcquirer() throws Exception {
        AtomicInteger acquireCount = new AtomicInteger();
        AtomicInteger releaseCount = new AtomicInteger();

        Consumer<ActionListener<Releasable>> acquirer = (listener) -> new Thread(() -> {
            try {
                Thread.sleep(randomIntBetween(1, 100));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }

            acquireCount.incrementAndGet();
            listener.onResponse(releaseCount::incrementAndGet);
        }).start();

        SplitSourceService.RefCountedAcquirer refCountedAcquirer = new SplitSourceService.RefCountedAcquirer(acquirer);

        int numThreads = randomIntBetween(1, 10);
        Thread[] threads = new Thread[numThreads];
        CountDownLatch latch = new CountDownLatch(numThreads);
        for (int i = 0; i < numThreads; i++) {
            final var tid = i;
            final int sleepMillis = randomIntBetween(1, 50);
            threads[i] = new Thread(() -> {
                try {
                    Thread.sleep(sleepMillis);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                logger.info("acquiring {}", tid);
                refCountedAcquirer.acquire(runAndRelease(latch::countDown));
            });
            threads[i].start();
        }
        for (int i = 0; i < numThreads; i++) {
            try {
                threads[i].join();
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }

        latch.await(SAFE_AWAIT_TIMEOUT.seconds(), TimeUnit.SECONDS);
        assertBusy(() -> assertThat(releaseCount.get(), equalTo(acquireCount.get())));
    }

    // verify that if resource acquisition fails, the refcount is still decremented properly
    // we do this by verifying that we attempt to acquire again, which we only do when the refcount is at 0
    public void testReleaseWhenAcquireFails() {
        // increments when acquisition is attempted
        AtomicInteger acquired = new AtomicInteger();
        // will only increment if acquire is called with resource successfully held
        AtomicInteger withResource = new AtomicInteger();
        SplitSourceService.RefCountedAcquirer acquirer = new SplitSourceService.RefCountedAcquirer(listener -> {
            acquired.incrementAndGet();
            throw new IllegalStateException("oops");
        });

        acquirer.acquire(runAndRelease(withResource::incrementAndGet));
        acquirer.acquire(runAndRelease(withResource::incrementAndGet));
        // attempted, then dropped ref, then attempted again
        assertEquals(acquired.get(), 2);
        // never actually obtained the resource
        assertEquals(withResource.get(), 0);
    }

    private ActionListener<Releasable> runAndRelease(Runnable runnable) {
        return new ActionListener<>() {
            @Override
            public void onResponse(Releasable releasable) {
                try (releasable) {
                    runnable.run();
                }
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn("acquiring failed", e);
            }
        };
    }
}
