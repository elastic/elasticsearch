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
import org.elasticsearch.compute.data.ElementType;
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

public class PartitionedExchangeSinkTests extends ComputeTestCase {

    private static final String TEST_EXECUTOR = "partitioned_exchange_test";

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

    public void testRowsAreRoutedToTheirHashPartition() throws Exception {
        verifyPartitioning(-1);
    }

    public void testFinishingAConsumerEarlyDoesNotHangOrLeak() throws Exception {
        verifyPartitioning(randomIntBetween(0, 3));
    }

    private void verifyPartitioning(int earlyConsumer) throws Exception {
        int partitionCount = 4;
        int rowCount = 2_000;
        BlockFactory exchangeBlockFactory = blockFactory();
        HashPartitioner partitioner = new HashPartitioner(new int[] { 0 }, new ElementType[] { ElementType.LONG }, partitionCount);
        List<Partition> partitions = createPartitions(partitionCount, exchangeBlockFactory);
        List<Page> inputPages = inputPages(blockFactory(), rowCount);
        Map<Row, Integer> expectedRows = new HashMap<>();
        Map<Row, Integer> expectedPartitions = new HashMap<>();
        for (Page page : inputPages) {
            int[] rowPartitions = new int[page.getPositionCount()];
            partitioner.partition(page, rowPartitions);
            LongBlock keys = page.getBlock(0);
            LongBlock ids = page.getBlock(1);
            for (int position = 0; position < page.getPositionCount(); position++) {
                Row row = new Row(keys.getLong(position), ids.getLong(position));
                expectedRows.merge(row, 1, Integer::sum);
                expectedPartitions.put(row, rowPartitions[position]);
            }
        }

        if (earlyConsumer >= 0) {
            partitions.get(earlyConsumer).source().finish();
        }

        List<Queue<Page>> outputPages = new ArrayList<>(partitionCount);
        List<Driver> drivers = new ArrayList<>(partitionCount + 1);
        drivers.add(
            TestDriverFactory.create(
                driverContext(),
                new CannedSourceOperator(inputPages.iterator()),
                List.of(),
                new ExchangeSinkOperator(new PartitionedExchangeSink(partitions.stream().map(Partition::sink).toList(), partitioner))
            )
        );
        for (int partition = 0; partition < partitionCount; partition++) {
            Queue<Page> pages = new ConcurrentLinkedQueue<>();
            outputPages.add(pages);
            if (partition != earlyConsumer) {
                drivers.add(
                    TestDriverFactory.create(
                        driverContext(),
                        new ExchangeSourceOperator(partitions.get(partition).source()),
                        List.of(),
                        new PageConsumerOperator(pages::add)
                    )
                );
            }
        }

        try {
            new TestDriverRunner().numThreads(partitionCount + 1).run(drivers);
            for (Partition partition : partitions) {
                partition.completion().actionGet(TimeValue.timeValueSeconds(10));
            }

            Map<Row, Integer> actualRows = new HashMap<>();
            for (int partition = 0; partition < partitionCount; partition++) {
                for (Page page : outputPages.get(partition)) {
                    int[] rowPartitions = new int[page.getPositionCount()];
                    partitioner.partition(page, rowPartitions);
                    LongBlock keys = page.getBlock(0);
                    LongBlock ids = page.getBlock(1);
                    for (int position = 0; position < page.getPositionCount(); position++) {
                        Row row = new Row(keys.getLong(position), ids.getLong(position));
                        assertThat(rowPartitions[position], equalTo(partition));
                        actualRows.merge(row, 1, Integer::sum);
                    }
                }
            }

            Map<Row, Integer> expectedRowsAfterEarlyFinish = new HashMap<>();
            for (Map.Entry<Row, Integer> entry : expectedRows.entrySet()) {
                if (expectedPartitions.get(entry.getKey()) != earlyConsumer) {
                    expectedRowsAfterEarlyFinish.put(entry.getKey(), entry.getValue());
                }
            }
            assertThat(actualRows, equalTo(expectedRowsAfterEarlyFinish));
        } finally {
            for (Queue<Page> pages : outputPages) {
                for (Page page : pages) {
                    page.releaseBlocks();
                }
            }
        }
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
