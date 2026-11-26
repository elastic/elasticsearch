/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LimitedBreaker;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/** Narrowing and forward reading of a parent page's element pages, with every block returned to the breaker. */
public class ChildWindowsTests extends ESTestCase {

    private final LimitedBreaker breaker = new LimitedBreaker("test", ByteSizeValue.ofMb(16));
    private final BlockFactory blockFactory = new BlockFactory(breaker, BigArrays.NON_RECYCLING_INSTANCE);

    /** Elements of dropped parents go; the rest point at their parent's index among the survivors. */
    public void testRetainParentsDropsAndRenumbers() {
        // parents 0..3; elements: p0 x2, p1 x1 | p2 x1, p3 x2
        try (ChildWindows windows = new ChildWindows(4, List.of(page(new int[] { 0, 0, 1 }), page(new int[] { 2, 3, 3 })))) {
            windows.retainParents(new int[] { 1, 3 });
            assertThat(windows.parentCount(), equalTo(2));
            assertThat(windows.elementCount(), equalTo(3L));
            assertThat(drain(windows), equalTo(List.of(List.of(0L, 102L), List.of(1L, 304L), List.of(1L, 305L))));
        }
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    /** A page left with no element is dropped; narrowing after a page was taken is refused. */
    public void testRetainParentsEmptiesPagesAndIsRefusedOnceRead() {
        try (ChildWindows windows = new ChildWindows(4, List.of(page(new int[] { 0, 1 }), page(new int[] { 3 })))) {
            windows.retainParents(new int[] { 3 });
            assertTrue(windows.hasNext());
            Page first = windows.next();
            first.releaseBlocks();
            assertFalse(windows.hasNext());
            expectThrows(IllegalStateException.class, () -> windows.retainParents(new int[] { 0 }));
        }
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testRetainParentsRejectsUnorderedPositions() {
        try (ChildWindows windows = new ChildWindows(3, List.of(page(new int[] { 0, 1, 2 })))) {
            expectThrows(IllegalArgumentException.class, () -> windows.retainParents(new int[] { 2, 1 }));
        }
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    /** Close releases the pages a consumer did not take, and the windows answer nothing afterwards. */
    public void testCloseReleasesUntakenPages() {
        ChildWindows windows = new ChildWindows(2, List.of(page(new int[] { 0 }), page(new int[] { 1 })));
        windows.next().releaseBlocks();
        windows.close();
        windows.close();
        assertTrue(windows.isReleased());
        expectThrows(IllegalStateException.class, windows::hasNext);
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    /** A wrapper over an iterator that reads no child relations fails rather than answering with no elements. */
    public void testForwardFailsOverAnIteratorWithoutChildRelations() {
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> ChildRelationPages.forward(new Object(), "items"));
        assertThat(e.getMessage(), containsString("child relation [items] requested"));
    }

    /** An element page: parent positions, and a leaf whose value is {@code parent * 100 + element index}. */
    private Page page(int[] parents) {
        try (
            IntBlock.Builder positions = blockFactory.newIntBlockBuilder(parents.length);
            LongBlock.Builder values = blockFactory.newLongBlockBuilder(parents.length)
        ) {
            for (int e = 0; e < parents.length; e++) {
                positions.appendInt(parents[e]);
                values.appendLong(parents[e] * 100L + nextValue++);
            }
            return new Page(positions.build(), values.build());
        }
    }

    private int nextValue;

    private static List<List<Long>> drain(ChildWindows windows) {
        List<List<Long>> out = new ArrayList<>();
        while (windows.hasNext()) {
            Page page = windows.next();
            try {
                IntBlock parents = page.getBlock(0);
                LongBlock values = page.getBlock(1);
                for (int e = 0; e < page.getPositionCount(); e++) {
                    out.add(List.of((long) parents.getInt(e), values.getLong(e)));
                }
            } finally {
                page.releaseBlocks();
            }
        }
        return out;
    }
}
