/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data.arrow;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LimitedBreaker;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.test.MockBlockFactory;
import org.elasticsearch.test.ESTestCase;

public class CircuitBreakerAllocationListenerTests extends ESTestCase {

    private CircuitBreaker breaker(long limit) {
        return new LimitedBreaker("test", ByteSizeValue.ofBytes(limit));
    }

    private BufferAllocator allocator(CircuitBreaker breaker) {
        return new RootAllocator(new CircuitBreakerAllocationListener(breaker), Long.MAX_VALUE);
    }

    public void testAllocationWithinLimitSucceeds() {
        var breaker = breaker(1024);
        try (var allocator = allocator(breaker)) {
            ArrowBuf buf = allocator.buffer(512);
            assertEquals(512, breaker.getUsed());
            buf.close();
            assertEquals(0, breaker.getUsed());
        }
    }

    public void testLargeAllocationTripsBreaker() {
        var breaker = breaker(1024);
        try (var allocator = allocator(breaker)) {
            expectThrows(CircuitBreakingException.class, () -> allocator.buffer(2048));
        }
    }

    public void testMultipleAllocationsExceedingLimitTripsBreaker() {
        var breaker = breaker(2048);
        try (var allocator = allocator(breaker)) {
            ArrowBuf buf1 = allocator.buffer(1024);
            assertEquals(1024, breaker.getUsed());

            ArrowBuf buf2 = allocator.buffer(512);
            assertEquals(1536, breaker.getUsed());

            // This allocation should push us over the limit
            expectThrows(CircuitBreakingException.class, () -> allocator.buffer(1024));

            buf2.close();
            buf1.close();
            assertEquals(0, breaker.getUsed());
        }
    }

    public void testReleaseDecrementsBreaker() {
        var breaker = breaker(4096);
        try (var allocator = allocator(breaker)) {
            ArrowBuf buf1 = allocator.buffer(1024);
            assertEquals(1024, breaker.getUsed());

            // Release and re-allocate — breaker should track correctly
            buf1.close();
            assertEquals(0, breaker.getUsed());

            ArrowBuf buf2 = allocator.buffer(2048);
            assertEquals(2048, breaker.getUsed());
            buf2.close();
            assertEquals(0, breaker.getUsed());
        }
    }

    public void testAllocationAfterReleaseSucceeds() {
        var breaker = breaker(1024);

        try (var allocator = allocator(breaker)) {
            ArrowBuf buf1 = allocator.buffer(800);
            // Allocation can be larger than the requested size.
            // (the real value is 1024, i.e. a power of 2)
            assertTrue(breaker.getUsed() >= 800);

            // Would exceed limit if buf1 is still held
            expectThrows(Exception.class, () -> allocator.buffer(800));

            // Free buf1 and try again — should succeed now
            buf1.close();
            assertEquals(0, breaker.getUsed());

            ArrowBuf buf2 = allocator.buffer(800);
            assertTrue(breaker.getUsed() >= 800);
            buf2.close();
            assertEquals(0, breaker.getUsed());
        }
    }

    public void testVectorTransfer() {

        // This is what happens in FlightClient, that creates a child allocator. Closing the client also closes that allocator,
        // so vectors and their buffers that must have a longer lifetime (as blocks) must be transferred to the parent allocator.

        var breaker = breaker(1024);
        var blockFactory = new MockBlockFactory(BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(breaker));
        var rootAllocator = blockFactory.arrowAllocator();
        var childAllocator = rootAllocator.newChildAllocator("child", 0, Long.MAX_VALUE);

        var childVector = new IntVector("test", childAllocator);
        childVector.allocateNew(1);
        childVector.set(0, 42);
        childVector.setValueCount(1);
        assertEquals(1, childVector.getValueCount());

        // Transfer child vector to parent allocator
        var pair = childVector.getTransferPair(rootAllocator);
        pair.transfer();
        var rootVector = pair.getTo();

        // Child vector has been emptied
        assertEquals(0, childVector.getValueCount());
        childVector.close();
        childAllocator.close();

        // Data now lives in rootVector
        assertEquals(1, rootVector.getValueCount());

        // Need to retain buffers in the newly created block
        var block = IntArrowBufBlock.of(rootVector, blockFactory);
        rootVector.close();

        assertEquals(1, block.getTotalValueCount());
        assertEquals(42, block.getInt(0));
        block.close();

        assertEquals(0, breaker.getUsed());

        blockFactory.ensureAllBlocksAreReleased();
    }
}
