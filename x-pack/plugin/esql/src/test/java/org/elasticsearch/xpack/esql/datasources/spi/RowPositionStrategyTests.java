/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.NoSuchElementException;

/** Unit coverage for the {@link RowPositionStrategy} family. */
public class RowPositionStrategyTests extends ESTestCase {

    private BlockFactory blockFactory;

    @Before
    public void initBlockFactory() {
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofGb(1));
        CircuitBreaker breaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
        blockFactory = BlockFactory.builder(bigArrays).breaker(breaker).build();
    }

    public void testPassThroughReturnsInnerUnchanged() throws IOException {
        Page page1 = pageOf(intBlock(1, 2, 3));
        Page page2 = pageOf(intBlock(4));
        try (CloseableIterator<Page> inner = iteratorOf(page1, page2)) {
            CloseableIterator<Page> out = PassThroughRowPositionStrategy.INSTANCE.apply(inner, -1);
            assertSame(inner, out);
        }
    }

    public void testPassThroughIgnoresSlotArgument() throws IOException {
        // PassThrough must be slot-agnostic: even when _rowPosition is in the projection, the reader
        // has already populated the slot in its native iterator. Strategy returns inner unchanged.
        Page page = pageOf(intBlock(1));
        try (CloseableIterator<Page> inner = iteratorOf(page)) {
            CloseableIterator<Page> out = PassThroughRowPositionStrategy.INSTANCE.apply(inner, 0);
            assertSame(inner, out);
        }
    }

    public void testNullSpliceWithoutRowPositionInProjectionReturnsInner() throws IOException {
        Page page = pageOf(intBlock(1, 2));
        try (CloseableIterator<Page> inner = iteratorOf(page)) {
            NullSpliceRowPositionStrategy strategy = new NullSpliceRowPositionStrategy(blockFactory, "test-reason");
            assertEquals("test-reason", strategy.reason());
            CloseableIterator<Page> out = strategy.apply(inner, -1);
            assertSame(inner, out);
        }
    }

    public void testNullSpliceInjectsNullBlockAtSlot() throws IOException {
        Page page = pageOf(intBlock(10, 20, 30));
        // _rowPosition at slot 1 between two real data columns; the dispatcher pre-computed slot=1
        // and passes it directly to the strategy.
        try (CloseableIterator<Page> inner = iteratorOf(page)) {
            NullSpliceRowPositionStrategy strategy = new NullSpliceRowPositionStrategy(blockFactory, "unit-test");
            try (CloseableIterator<Page> out = strategy.apply(inner, 1)) {
                assertTrue(out.hasNext());
                try (Page wrapped = out.next()) {
                    assertEquals(3, wrapped.getPositionCount());
                    assertEquals(2, wrapped.getBlockCount());
                    Block nullBlock = wrapped.getBlock(1);
                    assertTrue(nullBlock.areAllValuesNull());
                }
                assertFalse(out.hasNext());
            }
        }
    }

    public void testNullSpliceClosesInnerOnClose() throws IOException {
        Page page = pageOf(intBlock(1));
        boolean[] innerClosed = { false };
        CloseableIterator<Page> inner = new CloseableIterator<>() {
            private boolean served = false;

            @Override
            public boolean hasNext() {
                return served == false;
            }

            @Override
            public Page next() {
                if (served) {
                    throw new NoSuchElementException();
                }
                served = true;
                return page;
            }

            @Override
            public void close() {
                innerClosed[0] = true;
                if (served == false) {
                    page.releaseBlocks();
                }
            }
        };
        NullSpliceRowPositionStrategy strategy = new NullSpliceRowPositionStrategy(blockFactory, "close-test");
        try (CloseableIterator<Page> out = strategy.apply(inner, 0)) {
            assertTrue(out.hasNext());
            Page wrapped = out.next();
            wrapped.releaseBlocks();
        }
        assertTrue("inner close must propagate", innerClosed[0]);
    }

    private IntBlock intBlock(int... values) {
        try (IntBlock.Builder b = blockFactory.newIntBlockBuilder(values.length)) {
            for (int v : values) {
                b.appendInt(v);
            }
            return b.build();
        }
    }

    private static Page pageOf(Block... blocks) {
        int positions = blocks.length == 0 ? 0 : blocks[0].getPositionCount();
        return new Page(positions, blocks);
    }

    private static CloseableIterator<Page> iteratorOf(Page... pages) {
        Deque<Page> queue = new ArrayDeque<>();
        for (Page p : pages) {
            queue.add(p);
        }
        return new CloseableIterator<>() {
            @Override
            public boolean hasNext() {
                return queue.isEmpty() == false;
            }

            @Override
            public Page next() {
                Page p = queue.poll();
                if (p == null) {
                    throw new NoSuchElementException();
                }
                return p;
            }

            @Override
            public void close() {
                Releasables.closeExpectNoException(() -> {
                    while (queue.isEmpty() == false) {
                        Page p = queue.poll();
                        p.releaseBlocks();
                    }
                });
            }
        };
    }
}
