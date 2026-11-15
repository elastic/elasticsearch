/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.MockBlockFactory;
import org.elasticsearch.indices.breaker.CircuitBreakerMetrics;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

/** Logical Source holders share a Block identity; distinct backing-storage views remain a documented overestimate. */
public class SequenceHeldBlocksTests extends ESTestCase {
    /** Repeated page slots and separate Source owners share one reported Block identity. */
    public void testRepeatedBlockReferencesAndOwnersCountOnce() {
        MockBlockFactory blocks = blocks();
        SequenceHeldMemory memory = new SequenceHeldMemory();
        SequenceHeldBlocks held = new SequenceHeldBlocks(memory);
        Block block = blocks.newConstantIntBlockWith(7, 1);
        block.incRef();
        try (Page first = new Page(1, block, block); Page second = first.shallowCopy()) {
            long bytes = block.ramBytesUsed();
            long admitted = blocks.breaker().getUsed();
            var firstOwner = held.hold(first);
            var secondOwner = held.hold(second);
            assertEquals(bytes, memory.currentBytes());
            assertEquals(1, held.distinctBlocks());
            firstOwner.close();
            assertEquals(bytes, memory.currentBytes());
            secondOwner.close();
            secondOwner.close();
            assertEquals(0, memory.currentBytes());
            assertEquals(bytes, memory.peakBytes());
            assertFalse(block.isReleased());
            assertEquals(admitted, blocks.breaker().getUsed());
        }
        assertEquals(0, blocks.breaker().getUsed());
        blocks.ensureAllBlocksAreReleased();
    }

    /** Raw, marker-bearing bound and collected pages may overlap without multiplying their shared blocks. */
    public void testRawBoundAndCollectedPageOwnershipEndsAtHandoff() {
        MockBlockFactory blocks = blocks();
        SequenceHeldMemory memory = new SequenceHeldMemory();
        SequenceHeldBlocks held = new SequenceHeldBlocks(memory);
        Block rawBlock = blocks.newConstantIntBlockWith(7, 1);
        rawBlock.incRef();
        try (
            Page raw = new Page(rawBlock);
            Page bound = new Page(rawBlock, blocks.newConstantIntBlockWith(0, 1));
            Page collected = bound.shallowCopy()
        ) {
            var rawOwner = held.hold(raw);
            var boundOwner = held.hold(bound);
            var collectedOwner = held.hold(collected);
            long bytes = bound.ramBytesUsedByBlocks();
            assertEquals(bytes, memory.currentBytes());
            rawOwner.close();
            boundOwner.close();
            assertEquals(bytes, memory.currentBytes());
            collectedOwner.close();
            assertEquals(0, memory.currentBytes());
            assertEquals(bytes, memory.peakBytes());
            assertFalse(rawBlock.isReleased());
        }
        blocks.ensureAllBlocksAreReleased();
    }

    /** Distinct views intentionally retain a visible conservative overcount instead of guessing physical sharing. */
    public void testDistinctWrappersOverTheSameVectorAreConservative() {
        MockBlockFactory blocks = blocks();
        SequenceHeldMemory memory = new SequenceHeldMemory();
        SequenceHeldBlocks held = new SequenceHeldBlocks(memory);
        var vector = blocks.newConstantIntVector(7, 1);
        vector.incRef();
        try (Block first = vector.asBlock(); Block second = vector.asBlock()) {
            assertNotSame(first, second);
            var firstOwner = held.hold(first);
            var secondOwner = held.hold(second);
            assertEquals(first.ramBytesUsed() + second.ramBytesUsed(), memory.currentBytes());
            assertEquals(2, held.distinctBlocks());
            firstOwner.close();
            secondOwner.close();
            assertEquals(0, memory.currentBytes());
        }
        blocks.ensureAllBlocksAreReleased();
    }

    /** Observation failure unwinds logical holdings while the real block owner remains responsible for cleanup. */
    public void testPartialOwnerReleaseDoesNotReleaseItsBlocks() {
        MockBlockFactory blocks = blocks();
        SequenceHeldMemory memory = new SequenceHeldMemory();
        SequenceHeldBlocks held = new SequenceHeldBlocks(memory);
        try (Block block = blocks.newConstantIntBlockWith(7, 1)) {
            var owner = held.newOwner(new Block[] { block, null });
            owner.claimNext();
            long bytes = block.ramBytesUsed();
            expectThrows(NullPointerException.class, owner::claimNext);
            assertEquals(bytes, memory.currentBytes());
            owner.close();
            owner.close();
            assertEquals(0, memory.currentBytes());
            assertEquals(bytes, memory.peakBytes());
            assertEquals(0, held.distinctBlocks());
            assertFalse(block.isReleased());
        }
        blocks.ensureAllBlocksAreReleased();
    }

    private static MockBlockFactory blocks() {
        Settings settings = Settings.builder()
            .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "1mb")
            .put(HierarchyCircuitBreakerService.USE_REAL_MEMORY_USAGE_SETTING.getKey(), false)
            .build();
        CircuitBreaker breaker = new HierarchyCircuitBreakerService(
            CircuitBreakerMetrics.NOOP,
            settings,
            List.of(),
            new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        ).getBreaker(CircuitBreaker.REQUEST);
        return new MockBlockFactory(BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(breaker));
    }
}
