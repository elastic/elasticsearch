/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.fetch;

import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.test.ComputeTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;

import static org.hamcrest.Matchers.equalTo;

public class PageCollectorSinkOperatorTests extends ComputeTestCase {
    public void testKeepsEachShardsPagesInOrder() {
        BlockFactory blockFactory = blockFactory();
        try (PageCollectorSinkOperator.PageCollector collector = new PageCollectorSinkOperator.PageCollector(2)) {
            try (
                PageCollectorSinkOperator first = new PageCollectorSinkOperator(collector, 0);
                PageCollectorSinkOperator second = new PageCollectorSinkOperator(collector, 1)
            ) {
                first.addInput(page(blockFactory, 1, 2));
                second.addInput(page(blockFactory, 10));
                first.addInput(page(blockFactory, 3));
                first.finish();
                second.finish();
                assertTrue(first.isFinished());
                assertFalse(first.needsInput());
            }
            assertThat(collector.rows(0), equalTo(3));
            assertThat(collector.rows(1), equalTo(1));
            List<Page> pages = collector.take(0);
            try {
                assertThat(pages.size(), equalTo(2));
                assertThat(((IntBlock) pages.get(0).getBlock(0)).getInt(0), equalTo(1));
                assertThat(((IntBlock) pages.get(1).getBlock(0)).getInt(0), equalTo(3));
            } finally {
                pages.forEach(Page::releaseBlocks);
            }
            assertThat("taken pages leave the collector", collector.rows(0), equalTo(0));
            // shard 1 is still held and closing the collector releases it
        }
    }

    public void testFactoryFindsTheShardOfTheDriver() {
        BlockFactory blockFactory = blockFactory();
        try (PageCollectorSinkOperator.PageCollector collector = new PageCollectorSinkOperator.PageCollector(3)) {
            PageCollectorSinkOperator.Factory factory = new PageCollectorSinkOperator.Factory(collector, driverContext -> 2);
            assertThat(factory.describe(), equalTo("PageCollectorSinkOperator"));
            try (var sink = factory.get(new DriverContext(blockFactory.bigArrays(), blockFactory, null))) {
                sink.addInput(page(blockFactory, 5));
            }
            assertThat(collector.rows(2), equalTo(1));
        }
    }

    /**
     * A request can fail and close its collector while a fetch driver still runs. The pages that driver adds afterwards
     * are released, not kept where nobody reads them.
     */
    public void testReleasesPagesAddedAfterClose() {
        BlockFactory blockFactory = blockFactory();
        PageCollectorSinkOperator.PageCollector collector = new PageCollectorSinkOperator.PageCollector(1);
        try (PageCollectorSinkOperator sink = new PageCollectorSinkOperator(collector, 0)) {
            sink.addInput(page(blockFactory, 1));
            collector.close();
            sink.addInput(page(blockFactory, 2));
        }
        assertThat(collector.rows(0), equalTo(0));
        // the test case checks that every breaker is empty
    }

    /**
     * Each shard's driver adds from its own thread, while the request may close the collector at any time. Every page
     * ends up either taken in order or released.
     */
    public void testConcurrentSinksAndClose() throws Exception {
        BlockFactory blockFactory = blockFactory();
        int shards = between(2, 4);
        int pagesPerShard = between(10, 100);
        boolean closeEarly = randomBoolean();
        PageCollectorSinkOperator.PageCollector collector = new PageCollectorSinkOperator.PageCollector(shards);
        CyclicBarrier start = new CyclicBarrier(shards + 1);
        List<Thread> threads = new ArrayList<>();
        for (int s = 0; s < shards; s++) {
            int shard = s;
            Thread thread = new Thread(() -> {
                try (PageCollectorSinkOperator sink = new PageCollectorSinkOperator(collector, shard)) {
                    safeAwait(start);
                    for (int i = 0; i < pagesPerShard; i++) {
                        sink.addInput(page(blockFactory, i));
                    }
                }
            });
            thread.start();
            threads.add(thread);
        }
        safeAwait(start);
        if (closeEarly) {
            collector.close();
        }
        for (Thread thread : threads) {
            thread.join();
        }
        if (closeEarly == false) {
            for (int s = 0; s < shards; s++) {
                List<Page> pages = collector.take(s);
                try {
                    assertThat(pages.size(), equalTo(pagesPerShard));
                    for (int i = 0; i < pages.size(); i++) {
                        assertThat(((IntBlock) pages.get(i).getBlock(0)).getInt(0), equalTo(i));
                    }
                } finally {
                    pages.forEach(Page::releaseBlocks);
                }
            }
        }
        collector.close();
    }

    private static Page page(BlockFactory blockFactory, int... values) {
        return new Page(blockFactory.newIntArrayVector(values, values.length).asBlock());
    }
}
