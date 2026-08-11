/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data.arrow;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.RootAllocator;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LimitedBreaker;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.test.MockBlockFactory;
import org.elasticsearch.test.ESTestCase;

/**
 * Pins the allocation behaviour of {@link BlockFactory#EXACT_FIT_ROUNDING_POLICY}: the circuit
 * breaker must be charged exactly the number of bytes requested, with no power-of-two rounding,
 * on both sides of Arrow's 16 MiB chunk boundary.
 *
 * <p>Before the fix, {@code BlockFactory.arrowAllocator} used Arrow's default
 * {@code DefaultRoundingPolicy}, which rounds every sub-16 MiB request up to the next power of
 * two (worst case: 2x, e.g. 1,048,577 bytes → 2,097,152 reserved). The waste multiplied with
 * concurrency: up to 9 row-group range buffers live per reader, each in the 1–16 MiB band.
 */
public class ArrowAllocationAmplificationTests extends ESTestCase {

    private static final long LARGE_LIMIT = ByteSizeValue.ofGb(1).getBytes();

    private CircuitBreaker breaker() {
        return new LimitedBreaker("test", ByteSizeValue.ofBytes(LARGE_LIMIT));
    }

    /**
     * Verifies that the policy installed on the production allocator ({@link BlockFactory#EXACT_FIT_ROUNDING_POLICY})
     * charges exactly the requested size to the circuit breaker — no rounding tax.
     */
    private void assertExactCharge(long requestedBytes) {
        var breaker = breaker();
        try (
            var allocator = new RootAllocator(
                new CircuitBreakerAllocationListener(breaker),
                LARGE_LIMIT,
                BlockFactory.EXACT_FIT_ROUNDING_POLICY
            )
        ) {
            ArrowBuf buf = allocator.buffer(requestedBytes);
            assertEquals("breaker charge should equal requested bytes (no rounding)", requestedBytes, breaker.getUsed());
            buf.close();
            assertEquals(0, breaker.getUsed());
        }
    }

    // --- Sub-16 MiB: the power-of-two penalty zone ---

    public void testWorstCaseOneByteOverPowerOfTwo() {
        // 1,048,577 = 2^20 + 1 → old policy rounded to 2,097,152 (2x amplification)
        assertExactCharge(1_048_577L);
    }

    public void testTypicalColumnChunkSnappy() {
        // ~4.5 MiB: representative ClickBench SNAPPY column chunk → old policy: 8,388,608 (1.86x)
        assertExactCharge(4_500_000L);
    }

    public void testMidBandAllocation() {
        // 1.28 MiB → old policy: 2,097,152 (1.64x)
        assertExactCharge(1_280_000L);
    }

    public void testJustBelowChunkBoundary() {
        // 16 MiB - 1 byte: last byte before the 16 MiB threshold where old policy was exact
        assertExactCharge(16_777_215L);
    }

    // --- At and above 16 MiB: old policy was already exact here ---

    public void testAtChunkBoundary() {
        // Exactly 16 MiB: both old and new policy should charge exactly this
        assertExactCharge(16_777_216L);
    }

    public void testAboveChunkBoundary() {
        // 62.2 MiB: well above the threshold, both policies are exact
        assertExactCharge(62_200_000L);
    }

    /**
     * End-to-end check: verifies that {@link BlockFactory#arrowAllocator()} — the production
     * code path — wires in the exact-fit policy and not Arrow's default power-of-two policy.
     * Uses a worst-case size (one byte over a power of two) so any rounding would be unmistakable.
     */
    public void testProductionAllocatorUsesExactFitPolicy() {
        long requestedBytes = 1_048_577L; // 2^20 + 1 → old policy would reserve 2,097,152
        var breaker = breaker();
        var blockFactory = new MockBlockFactory(BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(breaker));
        ArrowBuf buf = blockFactory.arrowAllocator().buffer(requestedBytes);
        assertEquals("production arrowAllocator() must charge exactly the requested bytes", requestedBytes, breaker.getUsed());
        buf.close();
        blockFactory.ensureAllBlocksAreReleased();
        assertEquals(0, breaker.getUsed());
    }
}
