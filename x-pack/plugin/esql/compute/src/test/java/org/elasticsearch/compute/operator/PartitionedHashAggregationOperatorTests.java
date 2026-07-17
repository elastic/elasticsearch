/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.aggregation.SumLongAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.aggregation.blockhash.HashImplFactory;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.TestWarningsSource;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

/**
 * Exercises {@link PartitionedHashAggregationOperator} end to end: conversion from the legacy
 * single table to N partitions, bucket-sort routing via {@code addGather}, per-partition early
 * emit, null-key handling, and the multi-valued-key fallback to permanent single-table behavior.
 * Every test compares the operator's output (which is always {@link AggregatorMode#INITIAL},
 * i.e. intermediate state - sum/seen/failed per group, possibly split across several emitted
 * pages for the same key) against a hand-computed reference by folding all emitted rows for a
 * key together, exactly as a downstream {@code INTERMEDIATE}-mode consumer would.
 */
public class PartitionedHashAggregationOperatorTests extends ESTestCase {

    private BlockFactory blockFactory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        blockFactory = new BlockFactory(new NoopCircuitBreaker("test-noop"), BigArrays.NON_RECYCLING_INSTANCE);
        assumeTrue("SwissHash not available on this JVM", HashImplFactory.SWISS_HASH_AVAILABLE);
    }

    public void testNeverConvertsStaysUntagged() {
        Map<Long, Long> oracle = new HashMap<>();
        List<Page> input = randomInput(50, 5, oracle, false);

        PartitionedHashAggregationOperator.Builder builder = new PartitionedHashAggregationOperator.Builder().groupSpecs(
            List.of(new BlockHash.GroupSpec(0, ElementType.LONG))
        )
            .aggregators(List.of(sumLongFactory()))
            .partitionCount(8)
            .partitionConversionThreshold(1_000) // never crossed by 5 distinct keys
            .maxPageSize(10_000)
            .aggregationBatchSize(10_000);

        List<TaggedPage> results = runOperator(builder, input);
        for (TaggedPage tagged : results) {
            assertThat(
                "never-converted output must be tagged NONE_PARTITION",
                tagged.partition,
                equalTo(PartitionedHashAggregationOperator.NONE_PARTITION)
            );
        }
        assertMatchesOracle(results, oracle, 0L);
    }

    public void testConvertsAndProducesRealPartitionTags() {
        Map<Long, Long> oracle = new HashMap<>();
        List<Page> input = randomInput(5_000, 200, oracle, false);

        PartitionedHashAggregationOperator.Builder builder = new PartitionedHashAggregationOperator.Builder().groupSpecs(
            List.of(new BlockHash.GroupSpec(0, ElementType.LONG))
        )
            .aggregators(List.of(sumLongFactory()))
            .partitionCount(8)
            .partitionConversionThreshold(50)
            .perPartitionEmit(Integer.MAX_VALUE, 1.0) // no periodic early emit; only finish() emits
            .maxPageSize(10_000)
            .aggregationBatchSize(10_000);

        List<TaggedPage> results = runOperator(builder, input);
        assertTrue(
            "expected at least one real partition tag once converted",
            results.stream().anyMatch(t -> t.partition != PartitionedHashAggregationOperator.NONE_PARTITION)
        );
        assertMatchesOracle(results, oracle, 0L);
    }

    public void testPerPartitionEarlyEmitStillReconciles() {
        Map<Long, Long> oracle = new HashMap<>();
        List<Page> input = randomInput(8_000, 300, oracle, false);

        PartitionedHashAggregationOperator.Builder builder = new PartitionedHashAggregationOperator.Builder().groupSpecs(
            List.of(new BlockHash.GroupSpec(0, ElementType.LONG))
        )
            .aggregators(List.of(sumLongFactory()))
            .partitionCount(4)
            .partitionConversionThreshold(30)
            .perPartitionEmit(20, 0.9) // aggressive: force frequent per-partition resets
            .maxPageSize(500)
            .aggregationBatchSize(500);

        List<TaggedPage> results = runOperator(builder, input);
        assertTrue("expected multiple emitted pages when early emit is aggressive", results.size() > 1);
        assertMatchesOracle(results, oracle, 0L);
    }

    public void testNullKeysAggregateCorrectly() {
        Map<Long, Long> oracle = new HashMap<>();
        List<Page> input = randomInput(4_000, 200, oracle, true);

        PartitionedHashAggregationOperator.Builder builder = new PartitionedHashAggregationOperator.Builder().groupSpecs(
            List.of(new BlockHash.GroupSpec(0, ElementType.LONG))
        )
            .aggregators(List.of(sumLongFactory()))
            .partitionCount(8)
            .partitionConversionThreshold(30)
            .perPartitionEmit(50, 0.5)
            .maxPageSize(2_000)
            .aggregationBatchSize(2_000);

        List<TaggedPage> results = runOperator(builder, input);
        assertMatchesOracle(results, oracle, 0L);
    }

    public void testMultiValuedKeyTriggersFallbackAndStillReconciles() {
        Map<Long, Long> oracle = new HashMap<>();
        List<Page> input = new ArrayList<>(randomInput(6_000, 200, oracle, false));

        // Append one page with a multi-valued grouping key, contributing further to the oracle.
        LongBlock keys;
        LongBlock values;
        try (
            LongBlock.Builder keyBuilder = blockFactory.newLongBlockBuilder(3);
            LongBlock.Builder valueBuilder = blockFactory.newLongBlockBuilder(3)
        ) {
            keyBuilder.beginPositionEntry();
            keyBuilder.appendLong(1L);
            keyBuilder.appendLong(2L);
            keyBuilder.endPositionEntry();
            valueBuilder.appendLong(1000L);
            oracle.merge(1L, 1000L, Long::sum);
            oracle.merge(2L, 1000L, Long::sum);

            keyBuilder.appendLong(3L);
            valueBuilder.appendLong(2000L);
            oracle.merge(3L, 2000L, Long::sum);

            keys = keyBuilder.build();
            values = valueBuilder.build();
        }
        input.add(new Page(keys, values));
        // More ordinary rows afterward, to confirm the operator keeps working post-fallback.
        input.addAll(randomInput(2_000, 200, oracle, false));

        PartitionedHashAggregationOperator.Builder builder = new PartitionedHashAggregationOperator.Builder().groupSpecs(
            List.of(new BlockHash.GroupSpec(0, ElementType.LONG))
        )
            .aggregators(List.of(sumLongFactory()))
            .partitionCount(8)
            .partitionConversionThreshold(30)
            .perPartitionEmit(Integer.MAX_VALUE, 1.0)
            .maxPageSize(10_000)
            .aggregationBatchSize(10_000);

        List<TaggedPage> results = runOperator(builder, input);
        // After the fallback, everything (including the tail of ordinary rows) is legacy/untagged.
        assertThat(results.get(results.size() - 1).partition, equalTo(PartitionedHashAggregationOperator.NONE_PARTITION));
        assertMatchesOracle(results, oracle, 0L);
    }

    private PartitionedHashAggregationOperator.AggregatorSpec sumLongFactory() {
        return new PartitionedHashAggregationOperator.AggregatorSpec(
            new SumLongAggregatorFunctionSupplier(TestWarningsSource.INSTANCE),
            List.of(1)
        );
    }

    /**
     * Builds {@code rows} random (key, value) pairs over {@code cardinality} distinct keys (plus,
     * if {@code withNulls}, some null keys folded into oracle key {@code 0L} by convention here),
     * split across a handful of pages, updating {@code oracle} (key -&gt; expected sum) as it goes.
     */
    private List<Page> randomInput(int rows, int cardinality, Map<Long, Long> oracle, boolean withNulls) {
        List<Page> pages = new ArrayList<>();
        int remaining = rows;
        while (remaining > 0) {
            int pageSize = Math.min(remaining, between(50, 500));
            remaining -= pageSize;
            try (
                LongBlock.Builder keyBuilder = blockFactory.newLongBlockBuilder(pageSize);
                LongBlock.Builder valueBuilder = blockFactory.newLongBlockBuilder(pageSize)
            ) {
                for (int i = 0; i < pageSize; i++) {
                    long value = randomLongBetween(-1000, 1000);
                    if (withNulls && rarely()) {
                        keyBuilder.appendNull();
                        oracle.merge(0L, value, Long::sum);
                    } else {
                        long key = randomLongBetween(0, cardinality - 1);
                        keyBuilder.appendLong(key);
                        oracle.merge(key, value, Long::sum);
                    }
                    valueBuilder.appendLong(value);
                }
                LongBlock keys = keyBuilder.build();
                LongBlock values = valueBuilder.build();
                pages.add(new Page(keys, values));
            }
        }
        return pages;
    }

    private record TaggedPage(int partition, Page page) {}

    private List<TaggedPage> runOperator(PartitionedHashAggregationOperator.Builder builder, List<Page> input) {
        DriverContext driverContext = driverContext();
        PartitionedHashAggregationOperator operator = builder.build().get(driverContext);
        try {
            List<TaggedPage> results = new ArrayList<>();
            for (Page page : input) {
                assertTrue(operator.needsInput());
                Page copy = copyPage(page);
                operator.addInput(copy);
                drain(operator, results);
            }
            operator.finish();
            drain(operator, results);
            assertTrue(operator.isFinished());
            return results;
        } finally {
            operator.close();
        }
    }

    private void drain(PartitionedHashAggregationOperator operator, List<TaggedPage> results) {
        Page out;
        while ((out = operator.getOutput()) != null) {
            results.add(new TaggedPage(operator.outputPartition(), out));
        }
    }

    /**
     * {@code addInput} releases the page's blocks; the test still owns {@code input} for building
     * the oracle and reusing across assertions, so feed the operator an independent copy.
     */
    private Page copyPage(Page page) {
        Block[] blocks = new Block[page.getBlockCount()];
        boolean success = false;
        try {
            for (int i = 0; i < blocks.length; i++) {
                Block b = page.getBlock(i);
                blocks[i] = b.elementType()
                    .newBlockBuilder(b.getPositionCount(), blockFactory)
                    .copyFrom(b, 0, b.getPositionCount())
                    .build();
            }
            Page copy = new Page(blocks);
            success = true;
            return copy;
        } finally {
            if (success == false) {
                Releasables.closeExpectNoException(blocks);
            }
        }
    }

    /**
     * Folds every emitted (key, sum, seen) row across all pages/partitions together and compares
     * the per-key totals to {@code oracle}. {@code nullOracleKey} is the oracle key a null
     * grouping key's contributions were folded into when building the oracle.
     */
    private void assertMatchesOracle(List<TaggedPage> results, Map<Long, Long> oracle, long nullOracleKey) {
        Map<Long, Long> actual = new HashMap<>();
        for (TaggedPage tagged : results) {
            Page page = tagged.page;
            assertThat(page.getBlockCount(), equalTo(4)); // key, sum, seen, failed
            LongBlock keys = page.getBlock(0);
            LongBlock sums = page.getBlock(1);
            BooleanBlock seenFlags = page.getBlock(2);
            for (int i = 0; i < page.getPositionCount(); i++) {
                if (seenFlags.getBoolean(i) == false) {
                    continue;
                }
                long key = keys.isNull(i) ? nullOracleKey : keys.getLong(i);
                actual.merge(key, sums.getLong(i), Long::sum);
            }
        }
        assertThat(actual, equalTo(oracle));
    }

    private DriverContext driverContext() {
        return new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, blockFactory, null);
    }
}
