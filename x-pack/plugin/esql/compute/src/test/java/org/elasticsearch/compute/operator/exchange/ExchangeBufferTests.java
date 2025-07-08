/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.exchange;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.MockBlockFactory;
import org.elasticsearch.compute.test.RandomBlock;
import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;

public class ExchangeBufferTests extends ESTestCase {

    public void testDrainPages() throws Exception {
        ExchangeBuffer buffer = new ExchangeBuffer(randomIntBetween(10, 1000));
        var blockFactory = blockFactory();
        CountDownLatch latch = new CountDownLatch(1);
        Thread[] producers = new Thread[between(1, 4)];
        AtomicBoolean stopped = new AtomicBoolean();
        AtomicInteger addedPages = new AtomicInteger();
        for (int t = 0; t < producers.length; t++) {
            producers[t] = new Thread(() -> {
                safeAwait(latch);
                while (stopped.get() == false && addedPages.incrementAndGet() < 10_000) {
                    buffer.addPage(randomPage(blockFactory));
                }
            });
            producers[t].start();
        }
        latch.countDown();
        try {
            int minPage = between(10, 100);
            int receivedPage = 0;
            while (receivedPage < minPage) {
                Page p = buffer.pollPage();
                if (p != null) {
                    p.releaseBlocks();
                    ++receivedPage;
                }
            }
        } finally {
            buffer.finish(true);
            stopped.set(true);
        }
        for (Thread t : producers) {
            t.join();
        }
        assertThat(buffer.size(), equalTo(0));
        blockFactory.ensureAllBlocksAreReleased();
    }

    public void testOutstandingPages() throws Exception {
        ExchangeBuffer buffer = new ExchangeBuffer(randomIntBetween(1000, 10000));
        var blockFactory = blockFactory();
        Page p1 = randomPage(blockFactory);
        Page p2 = randomPage(blockFactory);
        buffer.addPage(p1);
        buffer.addPage(p2);
        buffer.finish(false);
        buffer.addPage(randomPage(blockFactory));
        assertThat(buffer.size(), equalTo(2));
        assertSame(buffer.pollPage(), p1);
        p1.releaseBlocks();
        assertSame(buffer.pollPage(), p2);
        p2.releaseBlocks();
        assertNull(buffer.pollPage());
        assertTrue(buffer.isFinished());
        blockFactory.ensureAllBlocksAreReleased();
    }

    private static MockBlockFactory blockFactory() {
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofGb(1)).withCircuitBreaking();
        CircuitBreaker breaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
        return new MockBlockFactory(breaker, bigArrays);
    }

    private static Page randomPage(BlockFactory blockFactory) {
        Block block = RandomBlock.randomBlock(
            blockFactory,
            randomFrom(ElementType.LONG, ElementType.BYTES_REF, ElementType.BOOLEAN),
            randomIntBetween(1, 100),
            randomBoolean(),
            0,
            between(1, 2),
            0,
            between(1, 2)
        ).block();
        return new Page(block);
    }
}
