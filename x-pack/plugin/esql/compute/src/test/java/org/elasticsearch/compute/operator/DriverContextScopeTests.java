/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DelegatingCircuitBreaker;
import org.elasticsearch.compute.data.LocalCircuitBreaker;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.spill.QueryScope;
import org.elasticsearch.compute.test.ComputeTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;

/**
 * A worker's factory from a driver context sits where the driver's does: on the driver's view of the query breaker and
 * the driver's arrays when the query has a scope, on the node's breaker and arrays otherwise.
 */
public class DriverContextScopeTests extends ComputeTestCase {

    public void testWorkerFactoryTakesTheDriversViewAndArrays() {
        BlockFactory node = blockFactory();
        QueryScope scope = QueryScope.create("q", node, ByteSizeValue.ofMb(4).getBytes(), 1024, null, ignored -> {});
        QueryScope.Lease lease = scope.tryAcquire();
        LocalCircuitBreaker.SizeSettings settings = new LocalCircuitBreaker.SizeSettings(1024, 4096);
        QueryScope.DriverAllocation allocation = scope.newDriverAllocation(settings);
        DriverContext context = new DriverContext(allocation.bigArrays(), allocation.blockFactory(), settings, "driver", scope);
        assertThat(context.queryScope(), sameInstance(scope));

        BlockFactory worker = context.createChildBlockFactory();
        assertThat(worker.breaker(), instanceOf(LocalCircuitBreaker.class));
        DelegatingCircuitBreaker workerParent = (DelegatingCircuitBreaker) ((LocalCircuitBreaker) worker.breaker()).parentBreaker();
        assertThat("the worker's local breaker charges the driver's view", workerParent, sameInstance(allocation.view()));
        assertThat(worker.bigArrays(), sameInstance(allocation.bigArrays()));
        assertThat(worker.parent(), sameInstance(scope.blockFactory()));

        LongBlock block;
        try (LongBlock.Builder builder = worker.newLongBlockBuilder(1000)) {
            for (int i = 0; i < 1000; i++) {
                builder.appendLong(i);
            }
            block = builder.build();
        }
        LongArray array = worker.bigArrays().newLongArray(1000, false);
        assertThat(scope.breaker().used(), equalTo(node.breaker().getUsed()));
        block.allowPassingToDifferentDriver();
        context.releaseChildBlockFactory(worker);
        allocation.close();
        block.close();
        array.close();
        assertThat(node.breaker().getUsed(), equalTo(0L));
        assertThat(scope.breaker().used(), equalTo(0L));
        lease.close();
    }

    public void testWithoutAScopeTheWorkerFactoryIsAsBefore() {
        BlockFactory node = blockFactory();
        LocalCircuitBreaker.SizeSettings settings = new LocalCircuitBreaker.SizeSettings(1024, 4096);
        LocalCircuitBreaker local = new LocalCircuitBreaker(node.breaker(), 1024, 4096);
        DriverContext context = new DriverContext(node.bigArrays(), node.newChildFactory(local), settings, "driver");
        assertThat(context.queryScope(), equalTo(null));
        BlockFactory worker = context.createChildBlockFactory();
        assertThat(((LocalCircuitBreaker) worker.breaker()).parentBreaker(), sameInstance(node.breaker()));
        assertThat(worker.bigArrays(), sameInstance(node.bigArrays()));
        context.releaseChildBlockFactory(worker);
        local.close();
        assertThat(node.breaker().getUsed(), equalTo(0L));
    }
}
