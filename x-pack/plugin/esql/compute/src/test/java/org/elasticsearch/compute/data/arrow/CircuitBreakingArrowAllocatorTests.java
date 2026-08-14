/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data.arrow;

import org.apache.arrow.memory.AllocationManager;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.IntVector;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LimitedBreaker;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.test.MockBlockFactory;
import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.atomic.AtomicInteger;

public class CircuitBreakingArrowAllocatorTests extends ESTestCase {

    private CircuitBreaker breaker(long limit) {
        return new LimitedBreaker("test", ByteSizeValue.ofBytes(limit));
    }

    private BufferAllocator allocator(CircuitBreaker breaker) {
        return CircuitBreakingArrowAllocator.create(breaker, requestSize -> requestSize);
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
            assertEquals(800, breaker.getUsed());

            // Would exceed limit if buf1 is still held
            expectThrows(Exception.class, () -> allocator.buffer(800));

            // Free buf1 and try again — should succeed now
            buf1.close();
            assertEquals(0, breaker.getUsed());

            ArrowBuf buf2 = allocator.buffer(800);
            assertEquals(800, breaker.getUsed());
            buf2.close();
            assertEquals(0, breaker.getUsed());
        }
    }

    public void testVectorTransfer() {

        // This is what happens in FlightClient, that creates a child allocator. Closing the client also closes that allocator,
        // so vectors and their buffers that must have a longer lifetime (as blocks) must be transferred to the parent allocator.

        var heapBreaker = breaker(1024);
        // use an explicit native breaker so Arrow pre/release accounting is visible and verifiable
        var nativeBreaker = new LimitedBreaker(CircuitBreaker.NATIVE_MEMORY, ByteSizeValue.ofBytes(1024 * 1024));
        var blockFactory = new MockBlockFactory(
            BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(heapBreaker).nativeMemoryBreaker(nativeBreaker)
        );
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

        assertEquals(0, nativeBreaker.getUsed());

        blockFactory.ensureAllBlocksAreReleased();
    }

    /**
     * Verifies that a native {@link OutOfMemoryError} thrown by the {@link AllocationManager.Factory#create} method does not
     * permanently strand the pre-allocation charge on the circuit breaker.
     *
     * <p>Arrow's {@code BaseAllocator.buffer()} calls {@code listener.onPreAllocation(size)} before delegating to
     * {@code AllocationManager.Factory.create()}. If {@code create()} throws {@link OutOfMemoryError}, Arrow corrects its own
     * internal counter in a {@code finally} block but never calls {@code listener.onRelease(size)}. Without the throwble-catching
     * wrapper in {@link CircuitBreakingArrowAllocator#oomCorrecting(CircuitBreaker, AllocationManager.Factory)}, the circuit breaker
     * would remain permanently overcharged by the size of the failed allocation.
     */
    public void testCircuitBreakerClearedOnAllocationManagerOom() {
        var breaker = new LimitedBreaker("test", ByteSizeValue.ofBytes(4096));
        var factory = oomingFactory(CircuitBreakingArrowAllocator.defaultFactory(), 3);

        try (var allocator = CircuitBreakingArrowAllocator.create(breaker, requestSize -> requestSize, factory)) {
            ArrowBuf buf1 = allocator.buffer(512);
            assertEquals(512, breaker.getUsed());

            ArrowBuf buf2 = allocator.buffer(512);
            assertEquals(1024, breaker.getUsed());

            // Third allocation fails at the memory allocation level; the circuit breaker charge must be refunded.
            expectThrows(OutOfMemoryError.class, () -> allocator.buffer(512));
            assertEquals("pre-allocation charge must be refunded after native OOM", 1024, breaker.getUsed());

            assertEquals(1024, breaker.getUsed());

            ArrowBuf buf3 = allocator.buffer(512);

            buf1.close();
            buf2.close();
            buf3.close();
            assertEquals(0, breaker.getUsed());
        }
    }

    private AllocationManager.Factory oomingFactory(AllocationManager.Factory baseFactory, int allowedCount) {
        final AtomicInteger counter = new AtomicInteger(allowedCount);
        return new AllocationManager.Factory() {
            @Override
            public AllocationManager create(BufferAllocator accountingAllocator, long size) {
                if (counter.decrementAndGet() == 0) {
                    throw new OutOfMemoryError("simulated OOM");
                }
                return baseFactory.create(accountingAllocator, size);
            }

            @Override
            public ArrowBuf empty() {
                return baseFactory.empty();
            }
        };
    }

    // private static final class CountingUnsafeAllocationManager extends AllocationManager {
    //
    // private static final ArrowBuf EMPTY;
    //
    // private final long allocatedSize;
    // private final long allocatedAddress;
    //
    // CountingUnsafeAllocationManager(BufferAllocator accountingAllocator, long requestedSize) {
    // super(accountingAllocator);
    // this.allocatedAddress = MemoryUtil.allocateMemory(requestedSize);
    // this.allocatedSize = requestedSize;
    // }
    //
    // public long getSize() {
    // return this.allocatedSize;
    // }
    //
    // protected long memoryAddress() {
    // return this.allocatedAddress;
    // }
    //
    // protected void release0() {
    // MemoryUtil.freeMemory(this.allocatedAddress);
    // }
    //
    // /**
    // * Returns a factory that allows {@code allowedCount - 1} successful allocations and then
    // * throws {@link OutOfMemoryError} from {@link AllocationManager.Factory#create} on the
    // * {@code allowedCount}-th call. The OOM is thrown before constructing the
    // * {@link AllocationManager} so that no dangling {@link org.apache.arrow.memory.BufferLedger}
    // * is left registered with the allocator.
    // */
    // static AllocationManager.Factory factory(int allowedCount) {
    // final AtomicInteger counter = new AtomicInteger(allowedCount);
    // return new AllocationManager.Factory() {
    // public AllocationManager create(BufferAllocator accountingAllocator, long size) {
    // if (counter.decrementAndGet() == 0) {
    // throw new OutOfMemoryError("simulated OOM");
    // }
    // return new CountingUnsafeAllocationManager(accountingAllocator, size);
    // }
    //
    // public ArrowBuf empty() {
    // return EMPTY;
    // }
    // };
    // }
    //
    // static {
    // EMPTY = new ArrowBuf(ReferenceManager.NO_OP, (BufferManager) null, 0L, MemoryUtil.allocateMemory(0L));
    // }
    // }
}
