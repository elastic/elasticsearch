/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.exchange;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.PageConsumerOperator;
import org.elasticsearch.compute.test.CannedSourceOperator;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.compute.test.TestDriverFactory;
import org.elasticsearch.compute.test.TestDriverRunner;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.hamcrest.Matchers.equalTo;

public class BroadcastExchangeSinkTests extends ComputeTestCase {
    private static final String TEST_EXECUTOR = "broadcast_exchange_test";

    private TestThreadPool threadPool;

    @Before
    public void setThreadPool() {
        threadPool = new TestThreadPool(
            "test",
            new FixedExecutorBuilder(Settings.EMPTY, TEST_EXECUTOR, 4, 1024, "esql", EsExecutors.TaskTrackingConfig.DEFAULT)
        );
    }

    @After
    public void shutdownThreadPool() {
        terminate(threadPool);
    }

    public void testRowsAreBroadcastToEveryConsumer() throws Exception {
        verifyBroadcast(-1);
    }

    public void testFinishingAConsumerEarlyDoesNotHangOrLeak() throws Exception {
        verifyBroadcast(randomIntBetween(0, 3));
    }

    private void verifyBroadcast(int earlyConsumer) throws Exception {
        int consumerCount = 4;
        int rowCount = 2_000;
        BlockFactory exchangeBlockFactory = blockFactory();
        BlockFactory inputBlockFactory = blockFactory();
        long usedBefore = inputBlockFactory.breaker().getUsed();
        List<Partition> partitions = createPartitions(consumerCount, exchangeBlockFactory);
        List<Page> inputPages = inputPages(inputBlockFactory, rowCount);
        Map<Row, Integer> expectedRows = new HashMap<>();
        for (Page page : inputPages) {
            LongBlock keys = page.getBlock(0);
            LongBlock ids = page.getBlock(1);
            for (int position = 0; position < page.getPositionCount(); position++) {
                expectedRows.merge(new Row(keys.getLong(position), ids.getLong(position)), 1, Integer::sum);
            }
        }

        if (earlyConsumer >= 0) {
            partitions.get(earlyConsumer).source().finish();
        }

        List<Queue<Page>> outputPages = new ArrayList<>(consumerCount);
        List<Driver> drivers = new ArrayList<>(consumerCount + 1);
        drivers.add(
            TestDriverFactory.create(
                driverContext(),
                new CannedSourceOperator(inputPages.iterator()),
                List.of(),
                new ExchangeSinkOperator(new BroadcastExchangeSink(partitions.stream().map(Partition::sink).toList()))
            )
        );
        for (int consumer = 0; consumer < consumerCount; consumer++) {
            Queue<Page> pages = new ConcurrentLinkedQueue<>();
            outputPages.add(pages);
            if (consumer != earlyConsumer) {
                drivers.add(
                    TestDriverFactory.create(
                        driverContext(),
                        new ExchangeSourceOperator(partitions.get(consumer).source()),
                        List.of(),
                        new PageConsumerOperator(pages::add)
                    )
                );
            }
        }

        try {
            new TestDriverRunner().numThreads(consumerCount + 1).run(drivers);
            for (Partition partition : partitions) {
                partition.completion().actionGet(TimeValue.timeValueSeconds(10));
            }

            for (int consumer = 0; consumer < consumerCount; consumer++) {
                Map<Row, Integer> actualRows = new HashMap<>();
                for (Page page : outputPages.get(consumer)) {
                    LongBlock keys = page.getBlock(0);
                    LongBlock ids = page.getBlock(1);
                    for (int position = 0; position < page.getPositionCount(); position++) {
                        actualRows.merge(new Row(keys.getLong(position), ids.getLong(position)), 1, Integer::sum);
                    }
                }
                assertThat(actualRows, equalTo(consumer == earlyConsumer ? Map.of() : expectedRows));
            }
        } finally {
            for (Queue<Page> pages : outputPages) {
                for (Page page : pages) {
                    page.releaseBlocks();
                }
            }
        }
        assertThat(inputBlockFactory.breaker().getUsed(), equalTo(usedBefore));
    }

    private List<Partition> createPartitions(int count, BlockFactory exchangeBlockFactory) {
        List<Partition> partitions = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            ExchangeSinkHandler sinkHandler = new ExchangeSinkHandler(exchangeBlockFactory, 2, threadPool.relativeTimeInMillisSupplier());
            ExchangeSourceHandler sourceHandler = new ExchangeSourceHandler(2, threadPool.executor(TEST_EXECUTOR));
            ExchangeSource source = sourceHandler.createExchangeSource();
            PlainActionFuture<Void> completion = new PlainActionFuture<>();
            sourceHandler.addRemoteSink(sinkHandler::fetchPageAsync, true, () -> {}, 1, completion);
            partitions.add(new Partition(sinkHandler.createExchangeSink(() -> {}), source, completion));
        }
        return partitions;
    }

    private List<Page> inputPages(BlockFactory blockFactory, int rowCount) {
        List<Page> pages = new ArrayList<>();
        for (int start = 0; start < rowCount; start += 128) {
            int pageSize = Math.min(128, rowCount - start);
            try (
                LongBlock.Builder keys = blockFactory.newLongBlockBuilder(pageSize);
                LongBlock.Builder ids = blockFactory.newLongBlockBuilder(pageSize)
            ) {
                for (int row = start; row < start + pageSize; row++) {
                    keys.appendLong(row % 79);
                    ids.appendLong(row);
                }
                pages.add(new Page(keys.build(), ids.build()));
            }
        }
        return pages;
    }

    private DriverContext driverContext() {
        BlockFactory blockFactory = blockFactory();
        return new DriverContext(blockFactory.bigArrays(), blockFactory, null);
    }

    private record Row(long key, long id) {}

    private record Partition(ExchangeSink sink, ExchangeSource source, PlainActionFuture<Void> completion) {}
}
