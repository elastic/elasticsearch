/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.aggregation.SumIntAggregatorFunctionSupplier;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.HashAggregationOperator;
import org.elasticsearch.compute.test.CannedSourceOperator;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.compute.test.RandomBlock;
import org.elasticsearch.compute.test.TestDriverFactory;
import org.elasticsearch.compute.test.TestDriverRunner;
import org.elasticsearch.compute.test.TestResultPageSinkOperator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 * Verifies that a {@link PartitionedBlockHash} yields the same results with and without partitioning.
 * Partitioned grouping aggregations are covered by
 * {@link org.elasticsearch.compute.aggregation.PartitionedGroupingAggregatorFunctionTestCase}.
 */
public abstract class PartitionedBlockHashTestCase extends ComputeTestCase {
    private static final String WORKER_EXECUTOR = "esql_partition_worker";
    private static final AggregatorFunctionSupplier AGGREGATOR = new SumIntAggregatorFunctionSupplier();

    /**
     * Element types of the grouping keys
     */
    protected abstract List<ElementType> keyTypes();

    protected abstract PartitionedBlockHash newBlockHash(List<BlockHash.GroupSpec> groups, BlockFactory blockFactory, int emitBatchSize);

    public final void testPartitioning() {
        List<BlockHash.GroupSpec> groups = groups();
        BlockFactory blockFactory = blockFactory();
        List<Page> pages = randomPages(blockFactory, groups);
        try {
            Map<List<Object>, Object> singlePass = runAggregation(driverContext(blockFactory()), groups, pages, Integer.MAX_VALUE);
            Map<List<Object>, Object> partitioned = runAggregation(driverContext(blockFactory()), groups, pages, between(128, 1024));
            assertThat(partitioned, equalTo(singlePass));
        } finally {
            Releasables.close(pages);
        }
    }

    public final void testPartitioningWithCrankyBreaker() {
        List<BlockHash.GroupSpec> groups = groups();
        BlockFactory blockFactory = blockFactory();
        List<Page> pages = randomPages(blockFactory, groups);
        try {
            Map<List<Object>, Object> partitioned = runAggregation(driverContext(crankyBlockFactory()), groups, pages, between(128, 1024));
            Map<List<Object>, Object> singlePass = runAggregation(driverContext(blockFactory()), groups, pages, Integer.MAX_VALUE);
            assertThat(partitioned, equalTo(singlePass));
        } catch (CircuitBreakingException e) {
            assertThat(e.getMessage(), containsString("cranky breaker"));
        } finally {
            Releasables.close(pages);
        }
    }

    private List<BlockHash.GroupSpec> groups() {
        List<ElementType> keyTypes = keyTypes();
        List<BlockHash.GroupSpec> groups = new ArrayList<>(keyTypes.size());
        for (int c = 0; c < keyTypes.size(); c++) {
            groups.add(new BlockHash.GroupSpec(c, keyTypes.get(c)));
        }
        return groups;
    }

    private static DriverContext driverContext(BlockFactory blockFactory) {
        return new DriverContext(blockFactory.bigArrays(), blockFactory, null);
    }

    private List<Page> randomPages(BlockFactory blockFactory, List<BlockHash.GroupSpec> groups) {
        int pageCount = between(1, 20);
        List<Page> pages = new ArrayList<>(pageCount);
        for (int i = 0; i < pageCount; i++) {
            Block[] blocks = new Block[groups.size() + 1];
            int positionCount = between(1, 1_000);
            for (int g = 0; g < groups.size(); g++) {
                boolean vector = randomBoolean();
                blocks[g] = RandomBlock.randomBlock(
                    blockFactory,
                    groups.get(g).elementType(),
                    positionCount,
                    vector == false && randomBoolean(),
                    1,
                    vector ? 1 : between(1, 3),
                    0,
                    vector ? 0 : between(0, 3)
                ).block();
            }
            blocks[groups.size()] = RandomBlock.randomBlock(blockFactory, ElementType.INT, positionCount, false, 0, 2, 0, 0).block();
            pages.add(new Page(blocks));
        }
        return pages;
    }

    private Map<List<Object>, Object> runAggregation(
        DriverContext driverContext,
        List<BlockHash.GroupSpec> groups,
        List<Page> inputPages,
        int partitionKeysThreshold
    ) {
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
            int emitBatchSize = randomFrom(100, 1_000, 10_000);
            hashOperator = new HashAggregationOperator(
                AggregatorMode.SINGLE,
                List.of(AGGREGATOR.groupingAggregatorFactory(AggregatorMode.SINGLE, List.of(groups.size()))),
                dc -> newBlockHash(groups, dc.blockFactory(), emitBatchSize),
                randomIntBetween(1, 1024),
                randomDouble(),
                randomIntBetween(128, 4096),
                null,
                null,
                driverContext,
                parallelConfig,
                randomBoolean()
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
            Map<List<Object>, Object> rows = new HashMap<>();
            for (Page page : outputPages) {
                assertThat(page.getBlockCount(), equalTo(groups.size() + 1));
                for (int p = 0; p < page.getPositionCount(); p++) {
                    List<Object> key = new ArrayList<>(groups.size());
                    for (int g = 0; g < groups.size(); g++) {
                        key.add(BlockUtils.toJavaObject(page.getBlock(g), p));
                    }
                    Object previous = rows.put(key, BlockUtils.toJavaObject(page.getBlock(groups.size()), p));
                    assertThat("duplicate key " + key, previous, nullValue());
                }
            }
            return rows;
        } finally {
            Releasables.close(hashOperator);
            Releasables.close(outputPages);
            terminate(threadPool);
        }
    }
}
