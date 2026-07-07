/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.cache.StatsCapturingIterator;
import org.elasticsearch.xpack.esql.datasources.spi.NullSpliceRowPositionStrategy;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Verifies that every wrapper on the external producer-loop drain path forwards the non-blocking drain contract
 * ({@link CloseableIterator#waitForReady()} / {@link CloseableIterator#pollNext()} / {@link CloseableIterator#isExhausted()})
 * to its delegate and applies its transform on the polled page — WITHOUT ever touching the delegate's blocking
 * {@code hasNext()}/{@code next()}. The probe delegate throws from {@code hasNext()}/{@code next()}, so a wrapper that
 * left {@code pollNext()}/{@code isExhausted()} at the {@code hasNext()}-based default would fail loudly.
 *
 * <p>Covered wrappers: {@link StatsCapturingIterator}, {@link SchemaAdaptingIterator}, {@link VirtualColumnIterator},
 * and {@link NullSpliceRowPositionStrategy}'s {@code NullSplicingIterator}.
 */
public class DrainContractForwardingTests extends ESTestCase {

    private final BlockFactory blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(new NoopCircuitBreaker("test"))
        .build();

    public void testStatsCapturingIteratorForwardsContract() {
        ProbeIterator probe = new ProbeIterator();
        CloseableIterator<Page> wrapper = StatsCapturingIterator.wrap(probe, new ConcurrentHashMap<>());
        assertForwardsContract(wrapper, probe, 1 /* pass-through block count */);
    }

    public void testSchemaAdaptingIteratorForwardsContract() {
        ProbeIterator probe = new ProbeIterator();
        List<Attribute> schema = List.of(attr("a", DataType.INTEGER));
        ColumnMapping identity = new ColumnMapping(new int[] { 0 }, null);
        CloseableIterator<Page> wrapper = new SchemaAdaptingIterator(probe, schema, identity, blockFactory);
        assertForwardsContract(wrapper, probe, 1 /* identity mapping keeps one block */);
    }

    public void testVirtualColumnIteratorForwardsContract() {
        ProbeIterator probe = new ProbeIterator();
        // One data column ("a") plus one partition column ("p") → inject() adds a constant block, output width 2.
        List<Attribute> fullOutput = List.of(attr("a", DataType.INTEGER), attr("p", DataType.INTEGER));
        CloseableIterator<Page> wrapper = new VirtualColumnIterator(probe, fullOutput, Set.of("p"), Map.of("p", 7), blockFactory);
        assertForwardsContract(wrapper, probe, 2 /* data + injected partition constant */);
    }

    public void testNullSplicingIteratorForwardsContract() {
        ProbeIterator probe = new ProbeIterator();
        // rowPositionSlot == inner block count (1) → splice appends a trailing null block, output width 2.
        CloseableIterator<Page> wrapper = new NullSpliceRowPositionStrategy(blockFactory, "test").apply(probe, 1);
        assertForwardsContract(wrapper, probe, 2 /* original block + null splice */);
    }

    /**
     * Drives the three contract methods through {@code wrapper}, asserting each forwards to {@code probe} and applies
     * the wrapper's transform. Never invokes {@code hasNext()}/{@code next()} (the probe throws from both).
     */
    private void assertForwardsContract(CloseableIterator<Page> wrapper, ProbeIterator probe, int expectedOutputBlocks) {
        // waitForReady forwards the delegate's exact listener instance.
        SubscribableListener<Void> ready = new SubscribableListener<>();
        probe.ready = ready;
        assertSame("waitForReady must forward to the delegate", ready, wrapper.waitForReady());

        // isExhausted forwards the delegate's flag.
        probe.exhausted = false;
        assertFalse("isExhausted must forward (false)", wrapper.isExhausted());
        probe.exhausted = true;
        assertTrue("isExhausted must forward (true)", wrapper.isExhausted());

        // pollNext == null forwards a delegate null without any transform or blocking call.
        probe.nextPage = null;
        assertNull("pollNext must forward a null delegate poll", wrapper.pollNext());

        // pollNext on a real page applies the wrapper's transform and delivers it.
        probe.nextPage = oneIntBlockPage();
        Page out = wrapper.pollNext();
        assertNotNull("pollNext must deliver the transformed page", out);
        assertEquals("transformed page block count", expectedOutputBlocks, out.getBlockCount());
        assertEquals("positions preserved by the transform", 3, out.getPositionCount());
        out.releaseBlocks();

        assertFalse("probe hasNext must never be called on the drain path", probe.hasNextCalled);
        assertFalse("probe next must never be called on the drain path", probe.nextCalled);
    }

    private Page oneIntBlockPage() {
        IntBlock block = blockFactory.newConstantIntBlockWith(42, 3);
        return new Page(3, new Block[] { block });
    }

    private static Attribute attr(String name, DataType type) {
        return new ReferenceAttribute(Source.EMPTY, name, type);
    }

    /** Delegate whose blocking {@code hasNext()}/{@code next()} throw; only the non-blocking contract is usable. */
    private static final class ProbeIterator implements CloseableIterator<Page> {
        private Page nextPage;
        private boolean exhausted;
        private SubscribableListener<Void> ready = SubscribableListener.newSucceeded(null);
        private boolean hasNextCalled;
        private boolean nextCalled;

        @Override
        public boolean hasNext() {
            hasNextCalled = true;
            throw new AssertionError("hasNext must not be called on the non-blocking drain path");
        }

        @Override
        public Page next() {
            nextCalled = true;
            throw new AssertionError("next must not be called on the non-blocking drain path");
        }

        @Override
        public Page pollNext() {
            Page p = nextPage;
            nextPage = null;
            return p;
        }

        @Override
        public boolean isExhausted() {
            return exhausted;
        }

        @Override
        public SubscribableListener<Void> waitForReady() {
            return ready;
        }

        @Override
        public void close() {}
    }
}
