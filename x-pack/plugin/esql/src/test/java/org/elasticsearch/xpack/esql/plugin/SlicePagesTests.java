/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

public class SlicePagesTests extends ESTestCase {

    public void testSliceFromSinglePage() {
        Page page = buildPage(0, 10);
        List<Page> result = TransportEsqlQueryAction.slicePages(List.of(page), 2, 3);
        assertEquals(1, result.size());
        assertPageValues(result.get(0), 2, 3, 4);
        releaseAll(result);
        page.releaseBlocks();
    }

    public void testSliceAcrossPages() {
        Page p1 = buildPage(0, 3);
        Page p2 = buildPage(3, 3);
        Page p3 = buildPage(6, 4);
        List<Page> result = TransportEsqlQueryAction.slicePages(List.of(p1, p2, p3), 2, 5);
        int totalRows = result.stream().mapToInt(Page::getPositionCount).sum();
        assertEquals(5, totalRows);
        releaseAll(result);
        p1.releaseBlocks();
        p2.releaseBlocks();
        p3.releaseBlocks();
    }

    public void testSliceEntirePage() {
        Page page = buildPage(0, 5);
        List<Page> result = TransportEsqlQueryAction.slicePages(List.of(page), 0, 5);
        assertEquals(1, result.size());
        assertEquals(5, result.get(0).getPositionCount());
        releaseAll(result);
        page.releaseBlocks();
    }

    public void testSliceBeyondEnd() {
        Page page = buildPage(0, 3);
        List<Page> result = TransportEsqlQueryAction.slicePages(List.of(page), 1, 100);
        int totalRows = result.stream().mapToInt(Page::getPositionCount).sum();
        assertEquals(2, totalRows);
        releaseAll(result);
        page.releaseBlocks();
    }

    public void testSliceEmptyInput() {
        List<Page> result = TransportEsqlQueryAction.slicePages(List.of(), 0, 10);
        assertTrue(result.isEmpty());
    }

    private static Page buildPage(int startValue, int count) {
        try (IntBlock.Builder builder = TestBlockFactory.getNonBreakingInstance().newIntBlockBuilder(count)) {
            for (int i = 0; i < count; i++) {
                builder.appendInt(startValue + i);
            }
            return new Page(builder.build());
        }
    }

    private static void assertPageValues(Page page, int... expected) {
        IntBlock block = page.getBlock(0);
        assertEquals(expected.length, page.getPositionCount());
        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], block.getInt(i));
        }
    }

    private static void releaseAll(List<Page> pages) {
        for (Page p : pages) {
            p.releaseBlocks();
        }
    }
}
