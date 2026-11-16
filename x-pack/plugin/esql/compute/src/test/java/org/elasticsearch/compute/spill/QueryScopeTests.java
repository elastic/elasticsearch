/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.spill;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.LocalCircuitBreaker;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.spill.spi.SpillStore;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.compute.test.spill.MemorySpillStore;
import org.elasticsearch.core.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

/**
 * The query scope: leases keep it open and the last release closes it, asynchronously when a backend has to delete;
 * a driver's allocation charges blocks and arrays to the query and the request alike, and a block passed on releases
 * against the query's factory; the spill policy reads the budget and the own-state minimum.
 */
public class QueryScopeTests extends ComputeTestCase {

    private static final long MIN = ByteSizeValue.ofKb(16).getBytes();

    private QueryScope newScope(BlockFactory node, long budget, @Nullable SpillStore store, List<QueryScope> closing) {
        return QueryScope.create("q", node, budget, MIN, store, closing::add);
    }

    private static boolean closed(QueryScope scope) {
        PlainActionFuture<Void> done = new PlainActionFuture<>();
        scope.addCloseListener(done);
        return done.isDone();
    }

    public void testLeasesKeepTheScopeOpenAndTheLastReleaseClosesIt() {
        List<QueryScope> closing = new ArrayList<>();
        QueryScope scope = newScope(blockFactory(), ByteSizeValue.ofMb(1).getBytes(), null, closing);
        QueryScope.Lease first = scope.tryAcquire();
        QueryScope.Lease second = scope.tryAcquire();
        assertThat(scope.leaseCount(), equalTo(2));
        first.close();
        first.close(); // idempotent
        assertThat(scope.leaseCount(), equalTo(1));
        assertFalse(closed(scope));
        assertThat(closing.size(), equalTo(0));
        second.close();
        assertTrue(scope.isClosing());
        assertTrue("no backend, so the close is immediate", closed(scope));
        assertThat(closing, equalTo(List.of(scope)));
        assertThat("a closing scope hands out no lease", scope.tryAcquire(), nullValue());
    }

    public void testTheCloseWaitsForTheBackend() {
        MemorySpillStore store = new MemorySpillStore(
            MemorySpillStore.Completion.DEFERRED,
            null,
            Long.MAX_VALUE,
            "test.limit",
            new Random(random().nextLong())
        );
        List<QueryScope> closing = new ArrayList<>();
        QueryScope scope = newScope(blockFactory(), ByteSizeValue.ofMb(1).getBytes(), store, closing);
        QueryScope.Lease lease = scope.tryAcquire();
        assertThat(scope.spillScope(), sameInstance(scope.spillScope()));
        assertThat(store.liveScopes(), equalTo(1));
        lease.close();
        assertThat("the registry hook runs before the backend is asked", closing, equalTo(List.of(scope)));
        assertFalse("the backend has not deleted yet", closed(scope));
        store.releaseAll();
        assertTrue(closed(scope));
        assertThat(store.liveScopes(), equalTo(0));
    }

    public void testSpillScopeRequiresAStore() {
        QueryScope scope = newScope(blockFactory(), ByteSizeValue.ofMb(1).getBytes(), null, new ArrayList<>());
        assertFalse(scope.spillEnabled());
        expectThrows(IllegalStateException.class, scope::spillScope);
        scope.tryAcquire().close();
    }

    public void testShouldSpillReadsTheBudgetAndTheMinimum() {
        long budget = ByteSizeValue.ofKb(100).getBytes();
        MemorySpillStore store = new MemorySpillStore(
            MemorySpillStore.Completion.INLINE,
            null,
            Long.MAX_VALUE,
            "test.limit",
            new Random(1)
        );
        QueryScope disabled = newScope(blockFactory(), budget, null, new ArrayList<>());
        assertFalse("never without a store", disabled.shouldSpill(MIN * 2, budget * 2));
        disabled.tryAcquire().close();

        QueryScope scope = newScope(blockFactory(), budget, store, new ArrayList<>());
        QueryScope.Lease lease = scope.tryAcquire();
        assertFalse("below the own-state minimum", scope.shouldSpill(MIN - 1, budget * 2));
        assertFalse("not crossing", scope.shouldSpill(MIN, budget / 2));
        assertTrue("crossing, above the minimum", scope.shouldSpill(MIN, budget + 1));
        scope.rootView().reserve(budget / 2, "test");
        assertTrue("the query's current use counts", scope.shouldSpill(MIN, budget / 2 + 1));
        scope.rootView().releaseReserve();
        lease.close();
    }

    public void testDriverAllocationChargesTheQueryAndTheRequest() {
        BlockFactory node = blockFactory();
        CircuitBreaker request = node.breaker();
        QueryScope scope = newScope(node, ByteSizeValue.ofMb(8).getBytes(), null, new ArrayList<>());
        QueryScope.Lease lease = scope.tryAcquire();
        QueryScope.DriverAllocation allocation = scope.newDriverAllocation(new LocalCircuitBreaker.SizeSettings(1024, 4096));
        assertThat(allocation.blockFactory().parent(), sameInstance(scope.blockFactory()));

        LongBlock block;
        try (LongBlock.Builder builder = allocation.blockFactory().newLongBlockBuilder(2000)) {
            for (int i = 0; i < 2000; i++) {
                builder.appendLong(i);
            }
            block = builder.build();
        }
        LongArray array = allocation.bigArrays().newLongArray(5000, false);
        long live = block.ramBytesUsed() + array.ramBytesUsed();
        assertThat(request.getUsed(), greaterThan(live - 1));
        assertThat(scope.breaker().used(), equalTo(request.getUsed()));

        // the block is handed on: it now releases against the query's factory, from any thread, after the driver closed
        block.allowPassingToDifferentDriver();
        assertThat(block.blockFactory(), sameInstance(scope.blockFactory()));
        allocation.close();
        assertThat("the driver's reserve chunk went back, the live bytes stay", request.getUsed(), equalTo(live));
        block.close();
        array.close();
        assertThat(request.getUsed(), equalTo(0L));
        assertThat(scope.breaker().used(), equalTo(0L));
        lease.close();
        assertTrue(closed(scope));
    }

    public void testCloseListenerRunsInlineOnceClosed() {
        QueryScope scope = newScope(blockFactory(), ByteSizeValue.ofMb(1).getBytes(), null, new ArrayList<>());
        scope.tryAcquire().close();
        PlainActionFuture<Void> late = new PlainActionFuture<>();
        scope.addCloseListener(late);
        assertTrue(late.isDone());
        scope.addCloseListener(ActionListener.noop());
    }
}
