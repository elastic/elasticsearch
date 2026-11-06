/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.test;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.LocalCircuitBreaker;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.spill.QueryScope;
import org.elasticsearch.compute.spill.spi.SpillStore;
import org.elasticsearch.compute.test.spill.MemorySpillStore;
import org.elasticsearch.indices.CrankyCircuitBreakerService;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.equalTo;

/**
 * Superclass for testing with blocks and operators
 */
public abstract class ComputeTestCase extends ESTestCase {

    private final List<CircuitBreaker> breakers = new ArrayList<>();
    private final List<BlockFactory> blockFactories = new ArrayList<>();

    /**
     * A {@link BigArrays} that won't throw {@link CircuitBreakingException}.
     * <p>
     * Rather than using the {@link NoneCircuitBreakerService} we use a
     * very large limit so tests can call {@link CircuitBreaker#getUsed()}.
     * </p>
     */
    protected final BigArrays nonBreakingBigArrays() {
        return new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofBytes(Integer.MAX_VALUE)).withCircuitBreaking();
    }

    /**
     * Build a {@link BlockFactory} with a huge limit.
     */
    protected final BlockFactory blockFactory() {
        return blockFactory(ByteSizeValue.ofGb(1));
    }

    /**
     * Build a {@link BlockFactory} with a configured limit.
     */
    protected final BlockFactory blockFactory(ByteSizeValue limit) {
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, limit).withCircuitBreaking();
        CircuitBreaker breaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
        breakers.add(breaker);
        BlockFactory factory = new MockBlockFactory(BlockFactory.builder(bigArrays).breaker(breaker));
        blockFactories.add(factory);
        return factory;
    }

    /**
     * Build a {@link BlockFactory} that randomly fails.
     */
    protected final BlockFactory crankyBlockFactory() {
        CrankyCircuitBreakerService cranky = new CrankyCircuitBreakerService();
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, cranky).withCircuitBreaking();
        breakers.add(bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST));
        BlockFactory blockFactory = new MockBlockFactory(BlockFactory.builder(bigArrays));
        blockFactories.add(blockFactory);
        return blockFactory;
    }

    protected final void testWithCrankyBlockFactory(Consumer<BlockFactory> run) {
        try {
            run.accept(crankyBlockFactory());
            logger.info("cranky let us finish!");
        } catch (CircuitBreakingException e) {
            logger.info("cranky", e);
            assertThat(e.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
        }
    }

    private record SpillingScope(
        QueryScope scope,
        List<QueryScope.Lease> leases,
        List<QueryScope.DriverAllocation> allocations,
        SpillStore store
    ) {}

    private final List<SpillingScope> spillingScopes = new ArrayList<>();

    /**
     * The context of a driver in a query that spills to {@code store} on this node, built the way the planner builds one:
     * a view of the query breaker, a local breaker over it and a child of the query's block factory, so that the
     * driver's blocks and arrays consume one reservation. {@code budget} is the query's soft limit and
     * {@code minSpillBytes} the own-state minimum below which an operator never spills; zero makes every operator
     * spill as soon as the budget is crossed. The scope is closed after the test, which is when its spilled data goes.
     */
    protected DriverContext spillingDriverContext(ByteSizeValue budget, long minSpillBytes, SpillStore store, Executor codecExecutor) {
        BlockFactory node = blockFactory();
        QueryScope scope = QueryScope.create(
            "query-" + spillingScopes.size(),
            node,
            budget.getBytes(),
            minSpillBytes,
            store,
            codecExecutor,
            ignored -> {}
        );
        SpillingScope spilling = new SpillingScope(scope, new ArrayList<>(), new ArrayList<>(), store);
        spillingScopes.add(spilling);
        return newSpillingDriverContext(spilling);
    }

    /**
     * The context of another driver of the query {@code sibling} belongs to: the two share the scope, so what one
     * passes to the other stays in one account, as two drivers of one query on a node do.
     */
    protected DriverContext spillingDriverContext(DriverContext sibling) {
        for (SpillingScope spilling : spillingScopes) {
            if (spilling.scope() == sibling.queryScope()) {
                return newSpillingDriverContext(spilling);
            }
        }
        throw new IllegalArgumentException("not a spilling driver context of this test: " + sibling);
    }

    private static DriverContext newSpillingDriverContext(SpillingScope spilling) {
        QueryScope scope = spilling.scope();
        spilling.leases().add(scope.tryAcquire());
        LocalCircuitBreaker.SizeSettings settings = new LocalCircuitBreaker.SizeSettings(
            ByteSizeValue.ofKb(4).getBytes(),
            ByteSizeValue.ofKb(16).getBytes()
        );
        QueryScope.DriverAllocation allocation = scope.newDriverAllocation(settings);
        spilling.allocations().add(allocation);
        return new DriverContext(allocation.bigArrays(), allocation.blockFactory(), settings, "spilling driver", scope);
    }

    /**
     * Closes the scopes of {@link #spillingDriverContext} and waits for their spilled data to go, before the breakers are
     * checked. A subclass whose scopes complete on a pool of its own calls this before it stops the pool; the call is
     * idempotent.
     */
    public final void closeSpillingScopes() throws Exception {
        List<SpillingScope> closing = new ArrayList<>(spillingScopes);
        spillingScopes.clear();
        for (SpillingScope spilling : closing) {
            for (QueryScope.DriverAllocation allocation : spilling.allocations()) {
                allocation.close();
            }
            PlainActionFuture<Void> closed = new PlainActionFuture<>();
            spilling.scope().addCloseListener(closed);
            for (QueryScope.Lease lease : spilling.leases()) {
                lease.close();
            }
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (closed.isDone() == false) {
                // a store that holds its completions needs them released for the close to land
                if (spilling.store() instanceof MemorySpillStore memory && memory.releaseAll() > 0) {
                    continue;
                }
                assertTrue("the spilling scope did not close in time", System.nanoTime() < deadline);
                Thread.sleep(1);
            }
            closed.get();
        }
    }

    @After
    public final void allBreakersEmpty() throws Exception {
        // the spilling scopes go first: their spilled data and reservations are what the breakers below account
        closeSpillingScopes();
        // first check that all big arrays are released, which can affect breakers
        MockBigArrays.ensureAllArraysAreReleased();
        for (var factory : blockFactories) {
            if (factory instanceof MockBlockFactory mockBlockFactory) {
                mockBlockFactory.ensureAllBlocksAreReleased();
            }
        }
        for (CircuitBreaker breaker : breakers) {
            assertThat("Unexpected used in breaker: " + breaker, breaker.getUsed(), equalTo(0L));
        }
    }
}
