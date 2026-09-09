/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.aggregation.blockhash.LongIntBlockHash;
import org.elasticsearch.compute.aggregation.blockhash.PartitionedBlockHash;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.HashAggregationOperator;
import org.elasticsearch.compute.test.CannedSourceOperator;
import org.elasticsearch.compute.test.TestDriverFactory;
import org.elasticsearch.compute.test.TestDriverRunner;
import org.elasticsearch.compute.test.TestResultPageSinkOperator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

/**
 * Tests grouping aggregations with and without partitioning.
 */
public abstract class PartitionedGroupingAggregatorFunctionTestCase extends GroupingAggregatorFunctionTestCase {
    private static final String WORKER_EXECUTOR = "esql_partition_worker";

    private int longKeyChannel = -1;
    private int intKeyChannel = -1;

    /**
     * Checks that partitioning does not change aggregation results.
     */
    public final void testPartitioning() {
        assumeTrue("partitioned block hashes are not supported on this platform", PartitionedBlockHash.supportPartitioning());
        DriverContext driverContext = driverContext();
        try (var agg = aggregatorFunction().groupingAggregator(driverContext, channels(AggregatorMode.SINGLE))) {
            assumeTrue("aggregator [" + agg + "] doesn't support partitioning", agg.supportPartitioning());
        }
        var pages = pagesWithKeys(driverContext.blockFactory());
        try {
            List<AggResult> singlePassResult = runAggregation(driverContext(), pages, Integer.MAX_VALUE);
            assertThat(singlePassResult, not(empty()));
            List<AggResult> partitioningResult = runAggregation(driverContext(), pages, between(128, 1024));
            assertThat(partitioningResult, equalTo(singlePassResult));
        } finally {
            Releasables.close(pages);
        }
    }

    /**
     * Checks that partitioning releases all resources when a cranky breaker trips midway
     */
    public final void testPartitioningWithCrankyBreaker() {
        assumeTrue("partitioned block hashes are not supported on this platform", PartitionedBlockHash.supportPartitioning());
        DriverContext driverContext = driverContext();
        try (var agg = aggregatorFunction().groupingAggregator(driverContext, channels(AggregatorMode.SINGLE))) {
            assumeTrue("aggregator [" + agg + "] doesn't support partitioning", agg.supportPartitioning());
        }
        var pages = pagesWithKeys(driverContext.blockFactory());
        try {
            List<AggResult> partitioningResult = runAggregation(crankyDriverContext(), pages, between(128, 1024));
            assertThat(partitioningResult, not(empty()));
            // if succeeds, then the result should be the same as single pass
            List<AggResult> singlePassResult = runAggregation(driverContext(), pages, Integer.MAX_VALUE);
            assertThat(partitioningResult, equalTo(singlePassResult));
        } catch (CircuitBreakingException e) {
            assertThat(e.getMessage(), containsString("cranky breaker"));
        } finally {
            Releasables.close(pages);
        }
    }

    List<Page> pagesWithKeys(BlockFactory blockFactory) {
        List<Page> pages = CannedSourceOperator.collectPages(simpleInput(blockFactory, between(1, 20_000)));
        for (int i = 0; i < pages.size(); i++) {
            Page page = pages.get(i);
            if (longKeyChannel == -1) {
                longKeyChannel = page.getBlockCount();
                intKeyChannel = longKeyChannel + 1;
            } else if (longKeyChannel != page.getBlockCount()) {
                throw new AssertionError("expected [" + longKeyChannel + "] blocks; got [" + page.getBlockCount());
            }
            Block[] blocks = new Block[page.getBlockCount() + 2];
            for (int b = 0; b < page.getBlockCount(); b++) {
                blocks[b] = page.getBlock(b);
            }
            blocks[longKeyChannel] = longKeyBlock(blockFactory, page.getPositionCount());
            blocks[intKeyChannel] = intKeyBlock(blockFactory, page.getPositionCount());
            pages.set(i, new Page(blocks));
        }
        return pages;
    }

    record Key(long longKey, int intKey) implements Comparable<Key> {
        @Override
        public int compareTo(Key other) {
            int compareLong = Long.compare(longKey, other.longKey);
            return compareLong != 0 ? compareLong : Integer.compare(intKey, other.intKey);
        }
    }

    record AggResult(Key key, Object value) implements Comparable<AggResult> {
        @Override
        public int compareTo(AggResult other) {
            return key.compareTo(other.key);
        }
    }

    List<AggResult> runAggregation(DriverContext driverContext, List<Page> inputPages, int partitionKeysThreshold) {
        final TestThreadPool threadPool = new TestThreadPool(
            getTestClass().getSimpleName(),
            new FixedExecutorBuilder(
                Settings.EMPTY,
                WORKER_EXECUTOR,
                between(1, 8),
                between(1, 1024),
                "esql",
                EsExecutors.TaskTrackingConfig.DEFAULT
            )
        );
        final List<Page> outputPages = new ArrayList<>();
        HashAggregationOperator hashOperator = null;
        try {
            var parallelConfig = new HashAggregationOperator.ParallelConfig(
                threadPool.executor(WORKER_EXECUTOR),
                between(1, 8),
                between(1, 10),
                partitionKeysThreshold
            );
            hashOperator = new HashAggregationOperator(
                AggregatorMode.SINGLE,
                List.of(aggregatorFunction().groupingAggregatorFactory(AggregatorMode.SINGLE, channels(AggregatorMode.SINGLE))),
                dc -> new LongIntBlockHash(
                    List.of(
                        new BlockHash.GroupSpec(longKeyChannel, ElementType.LONG),
                        new BlockHash.GroupSpec(intKeyChannel, ElementType.INT)
                    ),
                    dc.blockFactory(),
                    1024,
                    false
                ),
                randomIntBetween(1, 1024),
                randomDouble(),
                randomIntBetween(128, 4096),
                null,
                driverContext,
                parallelConfig
            );
            try (
                var source = new CannedSourceOperator(CannedSourceOperator.deepCopyOf(driverContext.blockFactory(), inputPages).iterator());
                Driver driver = TestDriverFactory.create(
                    driverContext,
                    source,
                    List.of(hashOperator),
                    new TestResultPageSinkOperator(outputPages::add)
                )
            ) {
                hashOperator = null;
                new TestDriverRunner().run(driver);
            }
            List<AggResult> rows = new ArrayList<>();
            for (Page page : outputPages) {
                assertThat(page.getBlockCount(), equalTo(3));
                LongBlock longKeys = page.getBlock(0);
                IntBlock intKeys = page.getBlock(1);
                Block valueBlock = page.getBlock(2);
                for (int p = 0; p < page.getPositionCount(); p++) {
                    rows.add(new AggResult(new Key(longKeys.getLong(p), intKeys.getInt(p)), BlockUtils.toJavaObject(valueBlock, p)));
                }
            }
            return rows.stream().sorted().toList();
        } finally {
            Releasables.close(hashOperator);
            Releasables.close(outputPages);
            terminate(threadPool);
        }
    }

    static LongBlock longKeyBlock(BlockFactory blockFactory, int positionCount) {
        boolean singleValue = randomBoolean();
        try (var builder = blockFactory.newLongBlockBuilder(positionCount)) {
            for (int p = 0; p < positionCount; p++) {
                int valueCount = singleValue ? 1 : randomIntBetween(0, 3);
                if (valueCount == 0) {
                    builder.appendNull();
                } else if (valueCount == 1) {
                    builder.appendLong(between(1, 100));
                } else {
                    builder.beginPositionEntry();
                    for (int v = 0; v < valueCount; v++) {
                        builder.appendLong(between(1, 100));
                    }
                    builder.endPositionEntry();
                }
            }
            return builder.build();
        }
    }

    static IntBlock intKeyBlock(BlockFactory blockFactory, int positionCount) {
        boolean singleValue = randomBoolean();
        try (var builder = blockFactory.newIntBlockBuilder(positionCount)) {
            for (int p = 0; p < positionCount; p++) {
                int valueCount = singleValue ? 1 : randomIntBetween(0, 3);
                if (valueCount == 0) {
                    builder.appendNull();
                } else if (valueCount == 1) {
                    builder.appendInt(between(1, 100));
                } else {
                    builder.beginPositionEntry();
                    for (int v = 0; v < valueCount; v++) {
                        builder.appendInt(between(1, 100));
                    }
                    builder.endPositionEntry();
                }
            }
            return builder.build();
        }
    }
}
