/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data.arrow;

import org.apache.arrow.memory.ArrowBuf;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LimitedBreaker;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.test.MockBlockFactory;
import org.elasticsearch.test.ESTestCase;

/**
 * Verifies that {@link BlockFactory#arrowAllocator()} charges the circuit breaker exactly the
 * number of bytes requested, with no power-of-two rounding, across sizes that cover both sides
 * of Arrow's 16 MiB chunk boundary.
 *
 * <p>Before the fix, {@code BlockFactory.arrowAllocator} used Arrow's default
 * {@code DefaultRoundingPolicy}, which rounds every sub-16 MiB request up to the next power of
 * two (worst case: 2x, e.g. 1,048,577 bytes → 2,097,152 reserved). The waste multiplied with
 * concurrency: up to 9 row-group range buffers live per reader, each in the 1–16 MiB band.
 */
public class ArrowAllocationAmplificationTests extends ESTestCase {

    private static final long LARGE_LIMIT = ByteSizeValue.ofGb(1).getBytes();

    /**
     * Exercises {@link BlockFactory#arrowAllocator()} for each representative allocation size and
     * asserts the breaker is charged exactly the requested bytes — no rounding amplification.
     *
     * <p>Sizes chosen to cover:
     * <ul>
     *   <li>1,048,577 — one byte over a power of two (worst case: old policy → 2x)</li>
     *   <li>4,500,000 — typical ClickBench SNAPPY column chunk (old policy → 1.86x)</li>
     *   <li>1,280,000 — mid-band (old policy → 1.64x)</li>
     *   <li>16,777,215 — one byte below the 16 MiB chunk boundary</li>
     *   <li>16,777,216 — exactly at the boundary (both policies exact)</li>
     *   <li>62,200,000 — well above the boundary (both policies exact)</li>
     * </ul>
     */
    public void testProductionAllocatorChargesExactSize() {
        long[] sizes = { 1_048_577L, 4_500_000L, 1_280_000L, 16_777_215L, 16_777_216L, 62_200_000L };
        for (long requestedBytes : sizes) {
            var breaker = new LimitedBreaker("test", ByteSizeValue.ofBytes(LARGE_LIMIT));
            var blockFactory = new MockBlockFactory(BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(breaker));
            ArrowBuf buf = blockFactory.arrowAllocator().buffer(requestedBytes);
            assertEquals(
                "arrowAllocator() must charge exactly the requested bytes for size " + requestedBytes,
                requestedBytes,
                breaker.getUsed()
            );
            buf.close();
            blockFactory.ensureAllBlocksAreReleased();
            assertEquals(0, breaker.getUsed());
        }
    }
}
