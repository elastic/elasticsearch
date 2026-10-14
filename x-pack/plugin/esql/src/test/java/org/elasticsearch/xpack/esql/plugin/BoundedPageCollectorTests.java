/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.junit.After;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.startsWith;

public class BoundedPageCollectorTests extends ESTestCase {
    private final CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(100));
    private final BlockFactory blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(breaker).build();

    @After
    public void assertAllReleased() {
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    private Page page(int positions) {
        int[] values = new int[positions];
        for (int i = 0; i < positions; i++) {
            values[i] = randomInt();
        }
        return new Page(blockFactory.newIntArrayVector(values, positions).asBlock());
    }

    private static void release(BoundedPageCollector collector) {
        collector.pages().forEach(Page::releaseBlocks);
    }

    public void testNoLimitCollectsEverything() {
        BoundedPageCollector collector = new BoundedPageCollector(null);
        int pages = between(1, 20);
        for (int i = 0; i < pages; i++) {
            collector.accept(page(between(1, 1000)));
        }
        assertThat(collector.pages(), hasSize(pages));
        release(collector);
    }

    public void testUnderLimitCollectsEverything() {
        Page first = page(between(1, 100));
        Page second = page(between(1, 100));
        long total = first.ramBytesUsedByBlocks() + second.ramBytesUsedByBlocks();
        BoundedPageCollector collector = new BoundedPageCollector(ByteSizeValue.ofBytes(total));
        collector.accept(first);
        collector.accept(second);
        assertThat(collector.pages(), hasSize(2));
        release(collector);
    }

    /**
     * The page that crosses the limit fails the accept but is still owned by the collector, so the
     * failure path that releases {@link BoundedPageCollector#pages()} does not leak it.
     */
    public void testCrossingLimitFailsAndKeepsPageForRelease() {
        Page first = page(between(1, 100));
        BoundedPageCollector collector = new BoundedPageCollector(ByteSizeValue.ofBytes(first.ramBytesUsedByBlocks()));
        collector.accept(first);
        Page second = page(between(1, 100));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> collector.accept(second));
        assertThat(e.getMessage(), startsWith(BoundedPageCollector.TOO_LARGE_MESSAGE_PREFIX));
        assertThat(e.getMessage(), containsString("[" + PlannerSettings.INTERMEDIATE_LOCAL_RELATION_MAX_SIZE.getKey() + "]"));
        assertThat(collector.pages(), hasSize(2));
        release(collector);
    }
}
