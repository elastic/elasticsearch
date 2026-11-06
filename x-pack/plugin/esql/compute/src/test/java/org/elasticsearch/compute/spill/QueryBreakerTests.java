/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.spill;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.LocalCircuitBreaker;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.compute.test.MockBlockFactory;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

/**
 * The reservation fixture of the plan's package A: one reservation on a driver's view of the query breaker is charged to
 * the request breaker once and consumed by the driver's blocks, through its local breaker, and by its hashes and arrays,
 * through a {@link BigArrays} over the same view; the local breaker's refills consume it in over-reserve chunks; a failed
 * reservation leaves everything untouched; releases go to the shared counter and never back into the credit; and a block
 * allocated by one driver can be handed to another and released there after the first driver's local breaker and view
 * are closed, with both the query and the request accounts back at baseline.
 */
public class QueryBreakerTests extends ComputeTestCase {

    private static final long OVER = 1024;
    private static final long MAX_OVER = 8 * 1024;

    /**
     * One driver of a query: its view, its arrays over the view, its local breaker over the view, its child factory.
     */
    private record Driver(QueryBreaker.View view, BigArrays arrays, LocalCircuitBreaker local, MockBlockFactory factory) {
        void close() {
            local.close();
            view.close();
        }
    }

    private record Topology(
        BlockFactory nodeFactory,
        BigArrays nodeArrays,
        CircuitBreaker request,
        QueryBreaker query,
        QueryBreaker.View rootView,
        MockBlockFactory queryFactory,
        Driver driver
    ) {
        Driver newDriver() {
            QueryBreaker.View view = query.newView();
            BigArrays arrays = nodeArrays.withBreakerService(view.service());
            LocalCircuitBreaker local = new LocalCircuitBreaker(view, OVER, MAX_OVER);
            MockBlockFactory factory = (MockBlockFactory) queryFactory.newChildFactory(local, arrays);
            return new Driver(view, arrays, local, factory);
        }

        void assertBaseline(Driver... closedDrivers) {
            rootView.close();
            driver.factory.ensureAllBlocksAreReleased();
            for (Driver closed : closedDrivers) {
                closed.factory.ensureAllBlocksAreReleased();
            }
            queryFactory.ensureAllBlocksAreReleased();
            assertThat(query.used(), equalTo(0L));
            assertThat(request.getUsed(), equalTo(0L));
        }
    }

    private Topology topology(ByteSizeValue limit, long budget) {
        BlockFactory nodeFactory = blockFactory(limit);
        CircuitBreaker request = nodeFactory.breaker();
        BigArrays nodeArrays = nodeFactory.bigArrays();
        QueryBreaker query = new QueryBreaker(request, budget);
        QueryBreaker.View rootView = query.newView();
        MockBlockFactory queryFactory = new MockBlockFactory(
            BlockFactory.builder(nodeArrays.withBreakerService(rootView.service())).breaker(rootView)
        );
        Topology partial = new Topology(nodeFactory, nodeArrays, request, query, rootView, queryFactory, null);
        return new Topology(nodeFactory, nodeArrays, request, query, rootView, queryFactory, partial.newDriver());
    }

    private Topology topology() {
        return topology(ByteSizeValue.ofMb(64), ByteSizeValue.ofMb(32).getBytes());
    }

    private static LongBlock longs(BlockFactory factory, int positions) {
        try (LongBlock.Builder builder = factory.newLongBlockBuilder(positions)) {
            for (int i = 0; i < positions; i++) {
                builder.appendLong(i);
            }
            return builder.build();
        }
    }

    public void testReservationIsChargedOnceAndConsumedByBlocksHashesAndArrays() {
        Topology t = topology();
        Driver d = t.driver;
        long reserve = ByteSizeValue.ofMb(4).getBytes();
        d.view.reserve(reserve, "test");
        assertThat(t.request.getUsed(), equalTo(reserve));
        assertThat(t.query.used(), equalTo(reserve));
        assertThat(d.view.credit(), equalTo(reserve));

        List<Releasable> allocations = new ArrayList<>();
        long allocated = 0;
        // gross charges exceed the live total, since a growing hash charges its new arrays before it releases the old ones,
        // so the loop stops while credit remains and the "nothing beyond the reserve" claim is checked only then
        while (allocated < reserve / 2 && d.view.credit() > reserve / 4) {
            switch (between(0, 2)) {
                case 0 -> {
                    LongBlock block = longs(d.factory, between(100, 5000));
                    allocations.add(block);
                    allocated += block.ramBytesUsed();
                }
                case 1 -> {
                    LongArray array = d.arrays.newLongArray(between(100, 5000), false);
                    allocations.add(array);
                    allocated += array.ramBytesUsed();
                }
                case 2 -> {
                    LongHash hash = new LongHash(between(16, 1024), d.arrays);
                    int keys = between(100, 5000);
                    for (int i = 0; i < keys; i++) {
                        hash.add(randomLong());
                    }
                    allocations.add(hash);
                    allocated += keys * Long.BYTES; // a lower bound on what the hash holds
                }
                default -> throw new AssertionError("unreachable");
            }
            // nothing beyond the reservation reaches the request breaker while the credit lasts, and what is live is charged
            if (d.view.credit() > 0) {
                assertThat(t.request.getUsed(), lessThanOrEqualTo(reserve));
            }
            assertThat(t.request.getUsed(), greaterThan(allocated - 1));
            assertThat(t.query.used(), equalTo(t.request.getUsed()));
        }
        // the credit was consumed by the gross charges, the local breaker's reserve chunks included
        long consumed = reserve - d.view.credit();
        assertThat(consumed, greaterThan(allocated - 1));
        assertThat(consumed, lessThanOrEqualTo(reserve));

        long returned = d.view.releaseReserve();
        assertThat(returned, equalTo(reserve - consumed));
        assertThat(t.request.getUsed(), equalTo(t.query.used()));
        assertThat(t.request.getUsed(), greaterThan(allocated - 1));

        Releasables.close(allocations);
        d.close();
        t.assertBaseline();
    }

    public void testRefillsConsumeTheCreditInOverReserveChunks() {
        Topology t = topology();
        Driver d = t.driver;
        long reserve = 10 * MAX_OVER;
        d.view.reserve(reserve, "test");
        long bytes = between(1, (int) OVER);
        d.local.addEstimateBytesAndMaybeBreak(bytes, "block");
        long chunk = reserve - d.view.credit();
        assertThat("the first refill takes the charge plus the over-reserve", chunk, equalTo(bytes + OVER));
        assertThat(t.request.getUsed(), equalTo(reserve));
        d.local.addWithoutBreaking(-bytes);
        // the release went into the local breaker's reserve, not back into the credit
        assertThat(d.view.credit(), equalTo(reserve - chunk));
        assertThat(t.request.getUsed(), equalTo(reserve));
        d.view.releaseReserve();
        d.close();
        t.assertBaseline();
    }

    public void testFailedReservationLeavesEverythingUntouched() {
        Topology t = topology(ByteSizeValue.ofMb(1), ByteSizeValue.ofMb(64).getBytes());
        Driver d = t.driver;
        LongBlock block = longs(d.factory, 100);
        long before = t.request.getUsed();
        long queryBefore = t.query.used();
        expectThrows(CircuitBreakingException.class, () -> d.view.reserve(ByteSizeValue.ofMb(2).getBytes(), "too much"));
        assertThat(d.view.credit(), equalTo(0L));
        assertThat(t.request.getUsed(), equalTo(before));
        assertThat(t.query.used(), equalTo(queryBefore));
        block.close();
        d.close();
        t.assertBaseline();
    }

    public void testAChargeBeyondTheCreditThatTripsRestoresTheCredit() {
        Topology t = topology(ByteSizeValue.ofMb(1), ByteSizeValue.ofMb(64).getBytes());
        Driver d = t.driver;
        long reserve = ByteSizeValue.ofKb(256).getBytes();
        d.view.reserve(reserve, "test");
        expectThrows(
            CircuitBreakingException.class,
            () -> d.view.addEstimateBytesAndMaybeBreak(ByteSizeValue.ofMb(2).getBytes(), "too much")
        );
        assertThat("a failed charge consumes nothing", d.view.credit(), equalTo(reserve));
        assertThat(t.request.getUsed(), equalTo(reserve));
        d.view.releaseReserve();
        d.close();
        t.assertBaseline();
    }

    public void testTwoQueriesCompeteForTheRequestBreaker() {
        BlockFactory nodeFactory = blockFactory(ByteSizeValue.ofMb(10));
        CircuitBreaker request = nodeFactory.breaker();
        QueryBreaker a = new QueryBreaker(request, ByteSizeValue.ofMb(8).getBytes());
        QueryBreaker b = new QueryBreaker(request, ByteSizeValue.ofMb(8).getBytes());
        QueryBreaker.View viewA = a.newView();
        QueryBreaker.View viewB = b.newView();
        viewA.reserve(ByteSizeValue.ofMb(6).getBytes(), "a");
        expectThrows(CircuitBreakingException.class, () -> viewB.reserve(ByteSizeValue.ofMb(6).getBytes(), "b"));
        assertThat(b.used(), equalTo(0L));
        assertThat(a.wouldCrossBudget(ByteSizeValue.ofMb(3).getBytes()), equalTo(true));
        assertThat(a.wouldCrossBudget(ByteSizeValue.ofMb(1).getBytes()), equalTo(false));
        viewA.releaseReserve();
        viewB.reserve(ByteSizeValue.ofMb(6).getBytes(), "b");
        assertThat(request.getUsed(), equalTo(ByteSizeValue.ofMb(6).getBytes()));
        viewB.close();
        assertThat(request.getUsed(), equalTo(0L));
    }

    public void testReleasesGoToTheSharedCounterNotTheCredit() {
        Topology t = topology();
        Driver d = t.driver;
        long reserve = ByteSizeValue.ofMb(2).getBytes();
        d.view.reserve(reserve, "test");
        LongArray array = d.arrays.newLongArray(10_000, false);
        long bytes = array.ramBytesUsed();
        assertThat(d.view.credit(), equalTo(reserve - bytes));
        assertThat(t.request.getUsed(), equalTo(reserve));
        array.close();
        assertThat("the release reduced the shared counts", t.request.getUsed(), equalTo(reserve - bytes));
        assertThat(t.query.used(), equalTo(reserve - bytes));
        assertThat("and left the credit alone", d.view.credit(), equalTo(reserve - bytes));
        d.view.releaseReserve();
        d.close();
        t.assertBaseline();
    }

    /**
     * A block allocated by one driver is handed to a second driver, which releases it on its own thread after the first
     * driver has closed its local breaker and view, while allocating and releasing blocks of its own.
     */
    public void testBlockHandedToAnotherDriverAndReleasedThereAfterTheFirstClosed() throws Exception {
        Topology t = topology();
        Driver first = t.driver;
        Driver second = t.newDriver();
        LongBlock block = longs(first.factory, 5000);
        long bytes = block.ramBytesUsed();
        assertThat(t.query.used(), greaterThan(bytes - 1));
        block.allowPassingToDifferentDriver();
        first.close();
        assertThat("the first driver's reserve chunk went back, the block is still held", t.query.used(), equalTo(bytes));
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread other = new Thread(() -> {
            try {
                assertTrue(second.local.assertBeginRunLoop());
                LongBlock own = longs(second.factory, between(100, 1000));
                block.close();
                own.close();
                assertTrue(second.local.assertEndRunLoop());
                second.close();
            } catch (Throwable e) {
                failure.set(e);
            }
        }, "second-driver");
        other.start();
        other.join();
        assertThat(failure.get(), nullValue());
        t.assertBaseline(second);
    }

    public void testReleaseFromACallbackAfterTheViewClosed() {
        Topology t = topology();
        Driver d = t.driver;
        LongArray array = d.arrays.newLongArray(10_000, false);
        d.view.reserve(ByteSizeValue.ofKb(64).getBytes(), "test");
        d.close();
        Runnable callback = array::close;
        callback.run();
        t.assertBaseline();
    }

    public void testChargesAfterCloseGoToTheQuery() {
        Topology t = topology();
        Driver d = t.driver;
        d.view.reserve(ByteSizeValue.ofKb(64).getBytes(), "test");
        d.close();
        assertThat(d.view.credit(), equalTo(0L));
        assertThat(t.request.getUsed(), equalTo(0L));
        long bytes = between(1, 10_000);
        d.view.addEstimateBytesAndMaybeBreak(bytes, "late");
        assertThat("a closed view holds no credit, so the charge went on", t.request.getUsed(), equalTo(bytes));
        assertThat(t.query.used(), equalTo(bytes));
        d.view.addWithoutBreaking(-bytes);
        // a late reservation on a closed view is returned at once rather than stranded
        d.view.reserve(bytes, "late");
        assertThat(d.view.credit(), equalTo(0L));
        t.assertBaseline();
    }

    public void testConcurrentChargesAndReleasesBalance() throws Exception {
        Topology t = topology(ByteSizeValue.ofGb(1), ByteSizeValue.ofGb(1).getBytes());
        int threads = between(2, 8);
        int rounds = between(100, 1000);
        CyclicBarrier start = new CyclicBarrier(threads);
        List<Thread> workers = new ArrayList<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        for (int i = 0; i < threads; i++) {
            QueryBreaker.View view = t.query.newView();
            final long[] sizes = new long[rounds];
            final boolean[] reserveFirst = new boolean[rounds];
            for (int r = 0; r < rounds; r++) {
                sizes[r] = between(1, 100_000);
                reserveFirst[r] = randomBoolean();
            }
            Thread worker = new Thread(() -> {
                try {
                    start.await();
                    for (int r = 0; r < rounds; r++) {
                        long bytes = sizes[r];
                        if (reserveFirst[r]) {
                            view.reserve(bytes, "r");
                            view.addEstimateBytesAndMaybeBreak(bytes / 2, "c");
                            view.addWithoutBreaking(-(bytes / 2));
                            view.releaseReserve();
                        } else {
                            view.addEstimateBytesAndMaybeBreak(bytes, "c");
                            view.addWithoutBreaking(-bytes);
                        }
                    }
                    view.close();
                } catch (Throwable e) {
                    failure.set(e);
                }
            }, "worker-" + i);
            workers.add(worker);
            worker.start();
        }
        for (Thread worker : workers) {
            worker.join();
        }
        assertThat(failure.get(), nullValue());
        t.driver.close();
        t.assertBaseline();
    }

    public void testViewServiceAnswersTheViewAndTheLimitIsTheParents() {
        Topology t = topology();
        Driver d = t.driver;
        assertThat(d.view.service().getBreaker(CircuitBreaker.REQUEST), sameInstance(d.view));
        assertThat(d.view.getLimit(), equalTo(t.request.getLimit()));
        expectThrows(UnsupportedOperationException.class, () -> t.query.setLimitAndOverhead(1, 1.0));
        assertThat(t.query.used(), equalTo(0L));
        d.close();
        t.assertBaseline();
    }

    public void testChildFactoryRejectsAViewOfAnotherRequestBreaker() {
        Topology t = topology();
        BlockFactory other = blockFactory();
        QueryBreaker foreign = new QueryBreaker(other.breaker(), 1024);
        LocalCircuitBreaker local = new LocalCircuitBreaker(foreign.newView(), OVER, MAX_OVER);
        expectThrows(IllegalStateException.class, () -> t.queryFactory.newChildFactory(local, t.driver.arrays));
        local.close();
        t.driver.close();
        t.assertBaseline();
    }

    /**
     * Two queries on one node share the request breaker; a view of the other query must still be rejected, or its blocks
     * would be counted against one query and released against the other.
     */
    public void testChildFactoryRejectsAViewOfAnotherQueryOnTheSameRequestBreaker() {
        Topology t = topology();
        QueryBreaker other = new QueryBreaker(t.request, 1024);
        QueryBreaker.View otherView = other.newView();
        LocalCircuitBreaker local = new LocalCircuitBreaker(otherView, OVER, MAX_OVER);
        expectThrows(IllegalStateException.class, () -> t.queryFactory.newChildFactory(local, t.driver.arrays));
        local.close();
        otherView.close();
        t.driver.close();
        t.assertBaseline();
    }

    /**
     * A driver factory's breaker is its thread-pinned local breaker; a local breaker straight over the request breaker
     * shares the root but not the account, and the earlier identity check rejected it, so the new check must too.
     */
    public void testChildFactoryRejectsALocalBreakerThatSkipsTheParentFactorysBreaker() {
        Topology t = topology();
        LocalCircuitBreaker skipping = new LocalCircuitBreaker(t.request, OVER, MAX_OVER);
        expectThrows(IllegalStateException.class, () -> t.driver.factory.newChildFactory(skipping, t.driver.arrays));
        skipping.close();
        t.driver.close();
        t.assertBaseline();
    }
}
