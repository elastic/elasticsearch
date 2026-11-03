/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch;

import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.SinkOperator;
import org.elasticsearch.compute.test.ComputeTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;

import static org.hamcrest.Matchers.equalTo;

public class ShardPagesTests extends ComputeTestCase {
    /**
     * The sink of each driver fills the slot of the shard its source claimed, and each slot keeps its pages in order.
     */
    public void testKeepsThePagesOfEachShardInOrder() {
        BlockFactory blockFactory = blockFactory();
        try (ShardPages shardPages = new ShardPages(3)) {
            DriverContext firstDriver = driverContext(blockFactory);
            DriverContext thirdDriver = driverContext(blockFactory);
            SinkOperator.SinkOperatorFactory sinks = shardPages.sinks(driverContext -> driverContext == firstDriver ? 0 : 2);
            try (SinkOperator first = sinks.get(firstDriver); SinkOperator third = sinks.get(thirdDriver)) {
                first.addInput(page(blockFactory, 1, 2));
                third.addInput(page(blockFactory, 10));
                first.addInput(page(blockFactory, 3));
                first.finish();
                third.finish();
                assertTrue(first.isFinished());
                assertFalse(first.needsInput());
            }
            List<Page> pages = shardPages.take(0);
            try {
                assertThat(pages.size(), equalTo(2));
                assertThat(((IntBlock) pages.get(0).getBlock(0)).getInt(0), equalTo(1));
                assertThat(((IntBlock) pages.get(1).getBlock(0)).getInt(0), equalTo(3));
            } finally {
                pages.forEach(Page::releaseBlocks);
            }
            assertThat("taken pages leave their slot", shardPages.take(0), equalTo(List.of()));
            assertThat(shardPages.take(1), equalTo(List.of()));
            // shard 2 still holds its page, and closing releases it
        }
    }

    /**
     * A request can fail and close its pages while a driver still runs. The pages that driver adds afterwards are released,
     * not kept where nobody reads them.
     */
    public void testReleasesPagesAddedAfterClose() {
        BlockFactory blockFactory = blockFactory();
        ShardPages shardPages = new ShardPages(1);
        try (SinkOperator sink = shardPages.sinks(driverContext -> 0).get(driverContext(blockFactory))) {
            sink.addInput(page(blockFactory, 1));
            shardPages.close();
            sink.addInput(page(blockFactory, 2));
        }
        assertThat(shardPages.take(0), equalTo(List.of()));
        // the test case checks that every breaker is empty
    }

    /**
     * The driver of each shard adds from its own thread, while the request may close the pages at any time. Every page ends
     * up either taken in order or released.
     */
    public void testConcurrentDriversAndClose() throws Exception {
        BlockFactory blockFactory = blockFactory();
        int shards = between(2, 4);
        int pagesPerShard = between(10, 100);
        boolean closeEarly = randomBoolean();
        ShardPages shardPages = new ShardPages(shards);
        CyclicBarrier start = new CyclicBarrier(shards + 1);
        List<Thread> threads = new ArrayList<>();
        for (int s = 0; s < shards; s++) {
            int shard = s;
            Thread thread = new Thread(() -> {
                try (SinkOperator sink = shardPages.sinks(driverContext -> shard).get(driverContext(blockFactory))) {
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
            shardPages.close();
        }
        for (Thread thread : threads) {
            thread.join();
        }
        if (closeEarly == false) {
            for (int s = 0; s < shards; s++) {
                List<Page> pages = shardPages.take(s);
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
        shardPages.close();
    }

    private static DriverContext driverContext(BlockFactory blockFactory) {
        return new DriverContext(blockFactory.bigArrays(), blockFactory, null);
    }

    private static Page page(BlockFactory blockFactory, int... values) {
        return new Page(blockFactory.newIntArrayVector(values, values.length).asBlock());
    }
}
