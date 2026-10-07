/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.AdmissionTracker;
import org.elasticsearch.xpack.esql.datasources.spi.RowGroupIo;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;

public class QueryConcurrencyBudgetTests extends ESTestCase {

    public void testAcquireAndRelease() throws Exception {
        ConcurrencyBudgetAllocator allocator = new ConcurrencyBudgetAllocator(10);
        QueryConcurrencyBudget budget = new QueryConcurrencyBudget(5, 60_000L, allocator);
        assertEquals(5, budget.maxPermits());
        assertEquals(0, budget.inFlight());

        budget.acquire();
        assertEquals(1, budget.inFlight());

        budget.release();
        assertEquals(0, budget.inFlight());
    }

    public void testBlocksAtLimit() throws Exception {
        QueryConcurrencyBudget budget = new QueryConcurrencyBudget(1, 60_000L, null);
        budget.acquire();
        assertEquals(1, budget.inFlight());

        AtomicBoolean acquired = new AtomicBoolean(false);
        CountDownLatch started = new CountDownLatch(1);
        Thread blocker = new Thread(() -> {
            started.countDown();
            try {
                budget.acquire();
                acquired.set(true);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        blocker.start();
        started.await(5, TimeUnit.SECONDS);

        Thread.sleep(100);
        assertFalse(acquired.get());

        budget.release();
        blocker.join(5000);
        assertTrue(acquired.get());
        budget.release();
    }

    public void testTimeoutThrows() throws Exception {
        RecordingTracker tracker = new RecordingTracker();
        ConcurrencyBudgetAllocator allocator = new ConcurrencyBudgetAllocator(10, 50L, tracker, "s3");
        QueryConcurrencyBudget budget = new QueryConcurrencyBudget(1, 50L, allocator, tracker);
        budget.acquire();

        expectThrows(TimeoutException.class, budget::acquire);

        assertEquals("budget/s3", tracker.lastGate);
        assertEquals(0, tracker.outstanding.get());
        assertEquals(0, tracker.grants.get());
        assertEquals(1, tracker.finished.get());
        budget.release();
    }

    public void testDynamicResizeUp() throws Exception {
        QueryConcurrencyBudget budget = new QueryConcurrencyBudget(1, 60_000L, null);
        budget.acquire();

        AtomicBoolean acquired = new AtomicBoolean(false);
        CountDownLatch started = new CountDownLatch(1);
        Thread blocker = new Thread(() -> {
            started.countDown();
            try {
                budget.acquire();
                acquired.set(true);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        blocker.start();
        started.await(5, TimeUnit.SECONDS);
        Thread.sleep(100);
        assertFalse(acquired.get());

        budget.updateMaxPermits(2);
        blocker.join(5000);
        assertTrue(acquired.get());

        budget.release();
        budget.release();
    }

    public void testDynamicResizeDown() throws Exception {
        QueryConcurrencyBudget budget = new QueryConcurrencyBudget(5, 60_000L, null);
        budget.acquire();
        budget.acquire();
        assertEquals(2, budget.inFlight());

        budget.updateMaxPermits(1);
        assertEquals(2, budget.inFlight());
        assertEquals(1, budget.maxPermits());

        budget.release();
        budget.release();
    }

    public void testAcquireOnClosedBudget() throws Exception {
        QueryConcurrencyBudget budget = new QueryConcurrencyBudget(5, 60_000L, null);
        budget.close();
        assertTrue(budget.isClosed());

        expectThrows(TimeoutException.class, budget::acquire);
    }

    public void testCloseWakesBlockedAcquirers() throws Exception {
        QueryConcurrencyBudget budget = new QueryConcurrencyBudget(1, 60_000L, null);
        budget.acquire();

        AtomicReference<Exception> caught = new AtomicReference<>();
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch finished = new CountDownLatch(1);
        Thread blocker = new Thread(() -> {
            started.countDown();
            try {
                budget.acquire();
            } catch (Exception e) {
                caught.set(e);
            }
            finished.countDown();
        });
        blocker.start();
        started.await(5, TimeUnit.SECONDS);
        Thread.sleep(100);

        budget.close();
        assertTrue(finished.await(5, TimeUnit.SECONDS));
        assertNotNull(caught.get());
        assertTrue(caught.get() instanceof TimeoutException);
        assertTrue(caught.get().getMessage().contains("closed"));

        budget.release();
    }

    public void testCloseDeregisters() throws Exception {
        ConcurrencyBudgetAllocator allocator = new ConcurrencyBudgetAllocator(50);
        QueryConcurrencyBudget budget = allocator.register();
        assertEquals(1, allocator.activeQueryCount());

        budget.close();
        assertEquals(0, allocator.activeQueryCount());
    }

    public void testUnlimitedBudget() throws Exception {
        assertFalse(QueryConcurrencyBudget.UNLIMITED.isEnabled());
        assertEquals(0, QueryConcurrencyBudget.UNLIMITED.maxPermits());
        QueryConcurrencyBudget.UNLIMITED.acquire();
        QueryConcurrencyBudget.UNLIMITED.release();
    }

    public void testReleaseWithoutAcquire() {
        QueryConcurrencyBudget budget = new QueryConcurrencyBudget(5, 60_000L, null);
        AssertionError e = expectThrows(AssertionError.class, budget::release);
        assertThat(e.getMessage(), containsString("release() called without a matching acquire()"));
        assertEquals(0, budget.inFlight());
    }

    public void testConcurrentAcquireRelease() throws Exception {
        QueryConcurrencyBudget budget = new QueryConcurrencyBudget(10, 60_000L, null);
        int threadCount = 20;
        int iterations = 100;
        CountDownLatch ready = new CountDownLatch(threadCount);
        CountDownLatch go = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threadCount);
        AtomicReference<Exception> failure = new AtomicReference<>();

        for (int t = 0; t < threadCount; t++) {
            new Thread(() -> {
                ready.countDown();
                try {
                    go.await(10, TimeUnit.SECONDS);
                    for (int i = 0; i < iterations; i++) {
                        budget.acquire();
                        Thread.yield();
                        budget.release();
                    }
                } catch (Exception e) {
                    failure.compareAndSet(null, e);
                }
                done.countDown();
            }).start();
        }
        ready.await(10, TimeUnit.SECONDS);
        go.countDown();
        assertTrue(done.await(30, TimeUnit.SECONDS));
        assertNull(failure.get());
        assertEquals(0, budget.inFlight());
    }

    public void testGapOfTwoPreemptsIncumbent() throws Exception {
        QueryConcurrencyBudget budget = new QueryConcurrencyBudget(1, 60_000L, null);
        RowGroupIo favoured = bound(budget, 6);
        RowGroupIo challenger = bound(budget, 4);
        budget.acquire();
        Thread favouredWaiter = startAcquire(budget, favoured);
        awaitWaiters(budget, 1);
        budget.release();
        favouredWaiter.join(5_000);
        assertEquals(1, budget.inFlight());
        assertSame(favoured, budget.favoured());

        Thread favouredAgain = startAcquire(budget, favoured);
        Thread challengerWaiter = startAcquire(budget, challenger);
        awaitWaiters(budget, 2);
        budget.release();
        challengerWaiter.join(5_000);
        assertTrue(challengerWaiter.isAlive() == false);
        assertTrue("incumbent must stay blocked after a gap-of-two preempt", favouredAgain.isAlive());
        assertSame(challenger, budget.favoured());
        budget.release();
        favouredAgain.join(5_000);
        budget.release();
    }

    public void testGapOfOneDoesNotUnseat() throws Exception {
        QueryConcurrencyBudget budget = new QueryConcurrencyBudget(1, 60_000L, null);
        RowGroupIo challenger = bound(budget, 4);
        RowGroupIo favoured = bound(budget, 5);
        budget.acquire();
        Thread favouredWaiter = startAcquire(budget, favoured);
        awaitWaiters(budget, 1);
        budget.release();
        favouredWaiter.join(5_000);
        assertSame(favoured, budget.favoured());
        assertTrue("challenger was bound first so it has the smaller startSeq", challenger.startSeq() < favoured.startSeq());

        Thread favouredAgain = startAcquire(budget, favoured);
        Thread challengerWaiter = startAcquire(budget, challenger);
        awaitWaiters(budget, 2);
        budget.release();
        favouredAgain.join(5_000);
        assertTrue(favouredAgain.isAlive() == false);
        assertTrue("gap of one must not unseat the incumbent", challengerWaiter.isAlive());
        budget.release();
        challengerWaiter.join(5_000);
        budget.release();
    }

    public void testTieWithNoIncumbentGrantsOldest() throws Exception {
        assertNoIncumbentGrants(5, 5, true);
    }

    public void testGapOfOneWithNoIncumbentGrantsOldest() throws Exception {
        assertNoIncumbentGrants(10, 9, true);
    }

    public void testGapOfTwoWithNoIncumbentGrantsClosest() throws Exception {
        assertNoIncumbentGrants(10, 8, false);
    }

    public void testReleaseOnAnotherThreadUnblocksLaterGroup() throws Exception {
        QueryConcurrencyBudget budget = new QueryConcurrencyBudget(1, 60_000L, null);
        RowGroupIo oldest = bound(budget, 10);
        RowGroupIo closest = bound(budget, 8);
        Thread holder = startAcquire(budget, null);
        holder.join(5_000);
        Thread oldestWaiter = startAcquire(budget, oldest);
        Thread closestWaiter = startAcquire(budget, closest);
        awaitWaiters(budget, 2);
        Thread helper = new Thread(() -> budget.release());
        helper.start();
        helper.join(5_000);
        closestWaiter.join(5_000);
        assertTrue("favoured driver must not need to run again", oldestWaiter.isAlive());
        assertTrue(closestWaiter.isAlive() == false);
        budget.release();
        oldestWaiter.join(5_000);
        budget.release();
    }

    public void testLiveOutstandingPreemptsWhenGapGrowsToTwo() throws Exception {
        QueryConcurrencyBudget budget = new QueryConcurrencyBudget(1, 60_000L, null);
        RowGroupIo oldest = bound(budget, 5);
        RowGroupIo closest = bound(budget, 4);
        budget.acquire();
        Thread oldestWaiter = startAcquire(budget, oldest);
        Thread closestWaiter = startAcquire(budget, closest);
        awaitWaiters(budget, 2);
        closest.onGetComplete();
        assertEquals(3, closest.outstanding());
        budget.release();
        closestWaiter.join(5_000);
        assertTrue(closestWaiter.isAlive() == false);
        assertTrue("grant-time outstanding, not enqueue-time, must decide", oldestWaiter.isAlive());
        budget.release();
        oldestWaiter.join(5_000);
        budget.release();
    }

    public void testFavouredStaysDuringActiveGet() throws Exception {
        QueryConcurrencyBudget budget = new QueryConcurrencyBudget(1, 60_000L, null);
        RowGroupIo favoured = bound(budget, 6);
        RowGroupIo other = bound(budget, 6);
        budget.acquire();
        Thread favouredWaiter = startAcquire(budget, favoured);
        awaitWaiters(budget, 1);
        budget.release();
        favouredWaiter.join(5_000);
        assertSame(favoured, budget.favoured());

        Thread otherWaiter = startAcquire(budget, other);
        awaitWaiters(budget, 1);
        budget.updateMaxPermits(2);
        otherWaiter.join(5_000);
        assertTrue(otherWaiter.isAlive() == false);
        assertSame("mid-GET favoured must not be replaced just because it is not waiting", favoured, budget.favoured());
        budget.release();
        budget.release();
    }

    public void testGapOfTwoStillPreemptsMidGetFavoured() throws Exception {
        QueryConcurrencyBudget budget = new QueryConcurrencyBudget(1, 60_000L, null);
        RowGroupIo favoured = bound(budget, 6);
        RowGroupIo closer = bound(budget, 4);
        budget.acquire();
        Thread favouredWaiter = startAcquire(budget, favoured);
        awaitWaiters(budget, 1);
        budget.release();
        favouredWaiter.join(5_000);
        assertSame(favoured, budget.favoured());

        Thread closerWaiter = startAcquire(budget, closer);
        awaitWaiters(budget, 1);
        budget.updateMaxPermits(2);
        closerWaiter.join(5_000);
        assertTrue(closerWaiter.isAlive() == false);
        assertSame(closer, budget.favoured());
        budget.release();
        budget.release();
    }

    public void testPinnedFavouriteIsNotPreemptedAtGapTwo() throws Exception {
        QueryConcurrencyBudget budget = new QueryConcurrencyBudget(1, 60_000L, null);
        RowGroupIo favoured = bound(budget, 6);
        RowGroupIo challenger = bound(budget, 4);
        budget.acquire();
        Thread favouredWaiter = startAcquire(budget, favoured);
        awaitWaiters(budget, 1);
        budget.release();
        favouredWaiter.join(5_000);
        assertTrue(favoured.tryPinOvershoot());

        Thread favouredAgain = startAcquire(budget, favoured);
        Thread challengerWaiter = startAcquire(budget, challenger);
        awaitWaiters(budget, 2);
        budget.release();
        favouredAgain.join(5_000);
        assertTrue(favouredAgain.isAlive() == false);
        assertTrue("a live pin must keep the favourite even at gap two", challengerWaiter.isAlive());
        budget.release();
        challengerWaiter.join(5_000);
        budget.release();
    }

    public void testFinishClearsFavouredSoNextGroupCanPin() throws Exception {
        QueryConcurrencyBudget budget = new QueryConcurrencyBudget(1, 60_000L, null);
        RowGroupIo first = bound(budget, 5);
        RowGroupIo closer = bound(budget, 1);
        RowGroupIo farther = bound(budget, 10);
        budget.acquire();
        Thread firstWaiter = startAcquire(budget, first);
        awaitWaiters(budget, 1);
        budget.release();
        firstWaiter.join(5_000);
        assertSame(first, budget.favoured());
        first.finish();
        assertNull(budget.favoured());
        assertTrue(closer.tryPinOvershoot());
        assertTrue(closer.isPinned());
        assertFalse("a second live pin must be refused", farther.tryPinOvershoot());
        budget.release();
    }

    public void testFinishRemovesLeaseFromRegistry() {
        QueryConcurrencyBudget budget = new QueryConcurrencyBudget(2, 60_000L, null);
        RowGroupIo a = bound(budget, 1);
        RowGroupIo b = bound(budget, 1);
        assertEquals(2, budget.boundLeaseCount());
        a.finish();
        assertEquals(1, budget.boundLeaseCount());
        assertTrue(a.isFinished());
        assertFalse(b.isFinished());
        b.finish();
        assertEquals(0, budget.boundLeaseCount());
    }

    public void testCloseCancelsLeasesWithoutHoldingBudgetLock() {
        QueryConcurrencyBudget budget = new QueryConcurrencyBudget(2, 60_000L, null);
        RowGroupIo lease = new RowGroupIo();
        AtomicBoolean wakeSawLock = new AtomicBoolean();
        lease.setWake("test", () -> wakeSawLock.set(budget.isLockHeldByCurrentThread()));
        budget.bind(lease);
        budget.close();
        assertTrue(lease.isCancelled());
        assertFalse("cancel() wake must not run under the budget lock", wakeSawLock.get());
    }

    public void testNullLeaseServedBeforeAcquireTimeout() throws Exception {
        QueryConcurrencyBudget budget = new QueryConcurrencyBudget(1, 60_000L, null);
        RowGroupIo real = bound(budget, 3);
        budget.acquire();
        Thread nullWaiter = startAcquire(budget, null);
        Thread realWaiter = startAcquire(budget, real);
        awaitWaiters(budget, 2);
        budget.ageNullWaiters(QueryConcurrencyBudget.NULL_LEASE_MAX_WAIT_MS);
        budget.release();
        nullWaiter.join(5_000);
        assertTrue(nullWaiter.isAlive() == false);
        assertTrue("real leases resume after one aged null grant", realWaiter.isAlive());
        budget.release();
        realWaiter.join(5_000);
        budget.release();
    }

    public void testFinishUnblocksWaiterForThatLease() throws Exception {
        QueryConcurrencyBudget budget = new QueryConcurrencyBudget(1, 60_000L, null);
        RowGroupIo done = bound(budget, 3);
        RowGroupIo live = bound(budget, 3);
        budget.acquire();
        AtomicReference<Exception> doneErr = new AtomicReference<>();
        CountDownLatch doneFinished = new CountDownLatch(1);
        Thread doneWaiter = new Thread(() -> {
            try {
                budget.acquire(done);
            } catch (Exception e) {
                doneErr.set(e);
            } finally {
                doneFinished.countDown();
            }
        });
        doneWaiter.start();
        Thread liveWaiter = startAcquire(budget, live);
        awaitWaiters(budget, 2);
        done.finish();
        assertTrue(doneFinished.await(5, TimeUnit.SECONDS));
        assertTrue(doneErr.get() instanceof TimeoutException);
        assertThat(doneErr.get().getMessage(), containsString("finished"));
        assertTrue("other lease waiters stay queued", liveWaiter.isAlive());
        assertEquals(1, budget.waiterCount());
        budget.release();
        liveWaiter.join(5_000);
        budget.release();
    }

    public void testAcquireOnFinishedLeaseThrows() throws Exception {
        QueryConcurrencyBudget budget = new QueryConcurrencyBudget(1, 60_000L, null);
        RowGroupIo lease = bound(budget, 1);
        lease.finish();
        TimeoutException e = expectThrows(TimeoutException.class, () -> budget.acquire(lease));
        assertThat(e.getMessage(), containsString("finished"));
        assertEquals(0, budget.inFlight());
    }

    public void testUnlimitedAcquireIgnoresLease() throws Exception {
        RowGroupIo lease = new RowGroupIo();
        lease.addUnissued(4);
        QueryConcurrencyBudget.UNLIMITED.acquire(lease, true);
        QueryConcurrencyBudget.UNLIMITED.release(lease, true);
        assertEquals(4, lease.outstanding());
        assertFalse(QueryConcurrencyBudget.UNLIMITED.tryPinOvershoot(lease));
    }

    private static RowGroupIo bound(QueryConcurrencyBudget budget, int unissued) {
        RowGroupIo lease = new RowGroupIo();
        budget.bind(lease);
        lease.addUnissued(unissued);
        return lease;
    }

    private void assertNoIncumbentGrants(int oldestOutstanding, int closestOutstanding, boolean expectOldest) throws Exception {
        QueryConcurrencyBudget budget = new QueryConcurrencyBudget(1, 60_000L, null);
        RowGroupIo oldest = bound(budget, oldestOutstanding);
        RowGroupIo closest = bound(budget, closestOutstanding);
        budget.acquire();
        Thread oldestWaiter = startAcquire(budget, oldest);
        Thread closestWaiter = startAcquire(budget, closest);
        awaitWaiters(budget, 2);
        budget.release();
        if (expectOldest) {
            oldestWaiter.join(5_000);
            assertTrue(oldestWaiter.isAlive() == false);
            assertTrue(closestWaiter.isAlive());
            budget.release();
            closestWaiter.join(5_000);
        } else {
            closestWaiter.join(5_000);
            assertTrue(closestWaiter.isAlive() == false);
            assertTrue(oldestWaiter.isAlive());
            budget.release();
            oldestWaiter.join(5_000);
        }
        budget.release();
    }

    private static Thread startAcquire(QueryConcurrencyBudget budget, RowGroupIo lease) {
        Thread thread = new Thread(() -> {
            try {
                budget.acquire(lease);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        thread.start();
        return thread;
    }

    private static void awaitWaiters(QueryConcurrencyBudget budget, int expected) throws Exception {
        assertBusy(() -> assertEquals(expected, budget.waiterCount()));
    }

    private static final class RecordingTracker implements AdmissionTracker {
        private final AtomicInteger outstanding = new AtomicInteger();
        private final AtomicInteger grants = new AtomicInteger();
        private final AtomicInteger finished = new AtomicInteger();
        private volatile String lastGate;

        @Override
        public Wait waitStarted(String gate, String waiter) {
            lastGate = gate;
            outstanding.incrementAndGet();
            return new Wait() {
                @Override
                public void granted() {
                    outstanding.decrementAndGet();
                    grants.incrementAndGet();
                }

                @Override
                public void finished() {
                    outstanding.decrementAndGet();
                    finished.incrementAndGet();
                }
            };
        }
    }
}
