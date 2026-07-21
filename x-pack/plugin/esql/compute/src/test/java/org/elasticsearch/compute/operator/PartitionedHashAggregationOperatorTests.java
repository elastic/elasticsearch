/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.aggregation.CountAggregatorFunction;
import org.elasticsearch.compute.aggregation.MaxLongAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.SumLongAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.aggregation.blockhash.HashImplFactory;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
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

    public void testIntGroupingKey() {
        Map<Integer, Long> oracle = new HashMap<>();
        List<Page> input = randomIntInput(4_000, 100, oracle);

        PartitionedHashAggregationOperator.Builder builder = new PartitionedHashAggregationOperator.Builder().groupSpecs(
            List.of(new BlockHash.GroupSpec(0, ElementType.INT))
        )
            .aggregators(List.of(sumLongFactory()))
            .partitionCount(8)
            .partitionConversionThreshold(30)
            .maxPageSize(10_000)
            .aggregationBatchSize(10_000);

        List<TaggedPage> results = runOperator(builder, input);
        assertTrue(
            "expected conversion to partitioned mode for 100 distinct int keys",
            results.stream().anyMatch(t -> t.partition != PartitionedHashAggregationOperator.NONE_PARTITION)
        );
        assertMatchesIntOracle(results, oracle);
    }

    public void testBytesRefGroupingKey() {
        Map<String, Long> oracle = new HashMap<>();
        List<Page> input = randomBytesRefInput(3_000, 50, oracle);

        PartitionedHashAggregationOperator.Builder builder = new PartitionedHashAggregationOperator.Builder().groupSpecs(
            List.of(new BlockHash.GroupSpec(0, ElementType.BYTES_REF))
        )
            .aggregators(List.of(sumLongFactory()))
            .partitionCount(8)
            .partitionConversionThreshold(30)
            .maxPageSize(10_000)
            .aggregationBatchSize(10_000);

        List<TaggedPage> results = runOperator(builder, input);
        assertTrue(
            "expected conversion to partitioned mode for 50 distinct bytesref keys",
            results.stream().anyMatch(t -> t.partition != PartitionedHashAggregationOperator.NONE_PARTITION)
        );
        assertMatchesBytesRefOracle(results, oracle);
    }

    public void testTwoLongGroupingKeys() {
        // Two LONG columns -> PackedValuesBlockHash (fixed-width) -> router works.
        Map<String, Long> oracle = new HashMap<>();
        List<Page> input = randomTwoLongInput(5_000, 20, oracle);

        PartitionedHashAggregationOperator.Builder builder = new PartitionedHashAggregationOperator.Builder().groupSpecs(
            List.of(new BlockHash.GroupSpec(0, ElementType.LONG), new BlockHash.GroupSpec(1, ElementType.LONG))
        )
            .aggregators(List.of(sumLongFactoryAt(2)))
            .partitionCount(8)
            .partitionConversionThreshold(50)
            .maxPageSize(10_000)
            .aggregationBatchSize(10_000);

        List<TaggedPage> results = runOperator(builder, input);
        assertTrue(
            "expected conversion with " + oracle.size() + " distinct key pairs",
            results.stream().anyMatch(t -> t.partition != PartitionedHashAggregationOperator.NONE_PARTITION)
        );
        assertMatchesTwoLongOracle(results, oracle);
    }

    public void testRouterNullFallsBackToSingleTable() {
        // LONG+BYTES_REF -> LongBytesRefAdaptiveBlockHash -> router() == null -> permanent single-table.
        Map<String, Long> oracle = new HashMap<>();
        List<Page> input = randomLongBytesRefInput(3_000, 50, oracle);

        PartitionedHashAggregationOperator.Builder builder = new PartitionedHashAggregationOperator.Builder().groupSpecs(
            List.of(new BlockHash.GroupSpec(0, ElementType.LONG), new BlockHash.GroupSpec(1, ElementType.BYTES_REF))
        )
            .aggregators(List.of(sumLongFactoryAt(2)))
            .partitionCount(8)
            .partitionConversionThreshold(30)
            .maxPageSize(10_000)
            .aggregationBatchSize(10_000);

        List<TaggedPage> results = runOperator(builder, input);
        for (TaggedPage tagged : results) {
            assertThat(
                "LONG+BYTES_REF has no router; all output must be NONE_PARTITION",
                tagged.partition,
                equalTo(PartitionedHashAggregationOperator.NONE_PARTITION)
            );
        }
        assertMatchesLongBytesRefOracle(results, oracle);
    }

    public void testMultipleAggregators() {
        Map<Long, Long> sumOracle = new HashMap<>();
        Map<Long, Long> maxOracle = new HashMap<>();
        List<Page> input = randomInputWithMax(4_000, 100, sumOracle, maxOracle);

        PartitionedHashAggregationOperator.Builder builder = new PartitionedHashAggregationOperator.Builder().groupSpecs(
            List.of(new BlockHash.GroupSpec(0, ElementType.LONG))
        )
            .aggregators(List.of(sumLongFactory(), maxLongFactory()))
            .partitionCount(8)
            .partitionConversionThreshold(30)
            .maxPageSize(10_000)
            .aggregationBatchSize(10_000);

        List<TaggedPage> results = runOperator(builder, input);
        assertMatchesSumAndMaxOracle(results, sumOracle, maxOracle);
    }

    /**
     * Regression test: COUNT aggregation in the promoted (partitioned) path calls
     * {@code addGather} on {@link org.elasticsearch.compute.aggregation.CountGroupingAggregatorFunction},
     * which previously threw {@link UnsupportedOperationException} because both anonymous
     * {@code AddInput} inner classes were missing the override.
     */
    public void testCountAggregationInPromotedPath() {
        Map<Long, Long> oracle = new HashMap<>();
        // Single-column pages (key only): COUNT(*) needs no value column.
        List<Page> input = randomCountInput(4_000, 200, oracle);

        PartitionedHashAggregationOperator.Builder builder = new PartitionedHashAggregationOperator.Builder().groupSpecs(
            List.of(new BlockHash.GroupSpec(0, ElementType.LONG))
        )
            // Empty channel list → countAll=true → exercises the countAll addGather path.
            .aggregators(List.of(countAllFactory()))
            .partitionCount(8)
            .partitionConversionThreshold(30)  // low threshold forces conversion → addGather calls
            .perPartitionEmit(Integer.MAX_VALUE, 1.0)
            .maxPageSize(10_000)
            .aggregationBatchSize(10_000);

        List<TaggedPage> results = runOperator(builder, input);
        assertTrue(
            "expected conversion to partitioned mode for 200 distinct keys",
            results.stream().anyMatch(t -> t.partition != PartitionedHashAggregationOperator.NONE_PARTITION)
        );
        assertMatchesCountOracle(results, oracle);
    }

    /**
     * Regression test: after finish() drains a converted (partitioned) operator, both {@code legacy}
     * and {@code partitions} are null. {@code toString()} must not throw in that state.
     */
    public void testToStringAfterFinishDoesNotThrow() {
        Map<Long, Long> oracle = new HashMap<>();
        List<Page> input = randomInput(5_000, 200, oracle, false);

        PartitionedHashAggregationOperator.Builder builder = new PartitionedHashAggregationOperator.Builder().groupSpecs(
            List.of(new BlockHash.GroupSpec(0, ElementType.LONG))
        )
            .aggregators(List.of(sumLongFactory()))
            .partitionCount(8)
            .partitionConversionThreshold(50)
            .maxPageSize(10_000)
            .aggregationBatchSize(10_000);

        DriverContext driverContext = driverContext();
        PartitionedHashAggregationOperator operator = builder.build().get(driverContext);
        try {
            for (Page page : input) {
                if (operator.needsInput()) {
                    operator.addInput(copyPage(page));
                }
                Page out;
                while ((out = operator.getOutput()) != null) {
                    out.releaseBlocks();
                }
            }
            operator.finish();
            Page out;
            while ((out = operator.getOutput()) != null) {
                out.releaseBlocks();
            }
            // Both legacy and partitions are null at this point — toString() must not throw.
            operator.toString();
        } finally {
            operator.close();
        }
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

    private PartitionedHashAggregationOperator.AggregatorSpec sumLongFactoryAt(int channel) {
        return new PartitionedHashAggregationOperator.AggregatorSpec(
            new SumLongAggregatorFunctionSupplier(TestWarningsSource.INSTANCE),
            List.of(channel)
        );
    }

    private PartitionedHashAggregationOperator.AggregatorSpec maxLongFactory() {
        return new PartitionedHashAggregationOperator.AggregatorSpec(new MaxLongAggregatorFunctionSupplier(), List.of(1));
    }

    private List<Page> randomIntInput(int rows, int cardinality, Map<Integer, Long> oracle) {
        List<Page> pages = new ArrayList<>();
        int remaining = rows;
        while (remaining > 0) {
            int pageSize = Math.min(remaining, between(50, 500));
            remaining -= pageSize;
            try (
                IntBlock.Builder keyBuilder = blockFactory.newIntBlockBuilder(pageSize);
                LongBlock.Builder valueBuilder = blockFactory.newLongBlockBuilder(pageSize)
            ) {
                for (int i = 0; i < pageSize; i++) {
                    int key = between(0, cardinality - 1);
                    long value = randomLongBetween(-1000, 1000);
                    keyBuilder.appendInt(key);
                    valueBuilder.appendLong(value);
                    oracle.merge(key, value, Long::sum);
                }
                pages.add(new Page(keyBuilder.build(), valueBuilder.build()));
            }
        }
        return pages;
    }

    private List<Page> randomBytesRefInput(int rows, int cardinality, Map<String, Long> oracle) {
        List<Page> pages = new ArrayList<>();
        int remaining = rows;
        while (remaining > 0) {
            int pageSize = Math.min(remaining, between(50, 500));
            remaining -= pageSize;
            try (
                BytesRefBlock.Builder keyBuilder = blockFactory.newBytesRefBlockBuilder(pageSize);
                LongBlock.Builder valueBuilder = blockFactory.newLongBlockBuilder(pageSize)
            ) {
                for (int i = 0; i < pageSize; i++) {
                    String keyStr = "key" + between(0, cardinality - 1);
                    long value = randomLongBetween(-1000, 1000);
                    keyBuilder.appendBytesRef(new BytesRef(keyStr));
                    valueBuilder.appendLong(value);
                    oracle.merge(keyStr, value, Long::sum);
                }
                pages.add(new Page(keyBuilder.build(), valueBuilder.build()));
            }
        }
        return pages;
    }

    private List<Page> randomTwoLongInput(int rows, int cardinality, Map<String, Long> oracle) {
        List<Page> pages = new ArrayList<>();
        int remaining = rows;
        while (remaining > 0) {
            int pageSize = Math.min(remaining, between(50, 500));
            remaining -= pageSize;
            try (
                LongBlock.Builder key1Builder = blockFactory.newLongBlockBuilder(pageSize);
                LongBlock.Builder key2Builder = blockFactory.newLongBlockBuilder(pageSize);
                LongBlock.Builder valueBuilder = blockFactory.newLongBlockBuilder(pageSize)
            ) {
                for (int i = 0; i < pageSize; i++) {
                    long key1 = between(0, cardinality - 1);
                    long key2 = between(0, cardinality - 1);
                    long value = randomLongBetween(-1000, 1000);
                    key1Builder.appendLong(key1);
                    key2Builder.appendLong(key2);
                    valueBuilder.appendLong(value);
                    oracle.merge(key1 + ":" + key2, value, Long::sum);
                }
                pages.add(new Page(key1Builder.build(), key2Builder.build(), valueBuilder.build()));
            }
        }
        return pages;
    }

    private List<Page> randomLongBytesRefInput(int rows, int cardinality, Map<String, Long> oracle) {
        List<Page> pages = new ArrayList<>();
        int remaining = rows;
        while (remaining > 0) {
            int pageSize = Math.min(remaining, between(50, 500));
            remaining -= pageSize;
            try (
                LongBlock.Builder key1Builder = blockFactory.newLongBlockBuilder(pageSize);
                BytesRefBlock.Builder key2Builder = blockFactory.newBytesRefBlockBuilder(pageSize);
                LongBlock.Builder valueBuilder = blockFactory.newLongBlockBuilder(pageSize)
            ) {
                for (int i = 0; i < pageSize; i++) {
                    long key1 = between(0, cardinality - 1);
                    String key2Str = "tag" + between(0, cardinality - 1);
                    long value = randomLongBetween(-1000, 1000);
                    key1Builder.appendLong(key1);
                    key2Builder.appendBytesRef(new BytesRef(key2Str));
                    valueBuilder.appendLong(value);
                    oracle.merge(key1 + ":" + key2Str, value, Long::sum);
                }
                pages.add(new Page(key1Builder.build(), key2Builder.build(), valueBuilder.build()));
            }
        }
        return pages;
    }

    private List<Page> randomInputWithMax(int rows, int cardinality, Map<Long, Long> sumOracle, Map<Long, Long> maxOracle) {
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
                    long key = between(0, cardinality - 1);
                    long value = randomLongBetween(-1000, 1000);
                    keyBuilder.appendLong(key);
                    valueBuilder.appendLong(value);
                    sumOracle.merge(key, value, Long::sum);
                    maxOracle.merge(key, value, Long::max);
                }
                pages.add(new Page(keyBuilder.build(), valueBuilder.build()));
            }
        }
        return pages;
    }

    private void assertMatchesIntOracle(List<TaggedPage> results, Map<Integer, Long> oracle) {
        Map<Integer, Long> actual = new HashMap<>();
        for (TaggedPage tagged : results) {
            Page page = tagged.page;
            assertThat(page.getBlockCount(), equalTo(4)); // key, sum, seen, failed
            IntBlock keys = page.getBlock(0);
            LongBlock sums = page.getBlock(1);
            BooleanBlock seenFlags = page.getBlock(2);
            for (int i = 0; i < page.getPositionCount(); i++) {
                if (seenFlags.getBoolean(i) == false) {
                    continue;
                }
                actual.merge(keys.getInt(i), sums.getLong(i), Long::sum);
            }
        }
        assertThat(actual, equalTo(oracle));
    }

    private void assertMatchesBytesRefOracle(List<TaggedPage> results, Map<String, Long> oracle) {
        Map<String, Long> actual = new HashMap<>();
        BytesRef scratch = new BytesRef();
        for (TaggedPage tagged : results) {
            Page page = tagged.page;
            assertThat(page.getBlockCount(), equalTo(4)); // key, sum, seen, failed
            BytesRefBlock keys = page.getBlock(0);
            LongBlock sums = page.getBlock(1);
            BooleanBlock seenFlags = page.getBlock(2);
            for (int i = 0; i < page.getPositionCount(); i++) {
                if (seenFlags.getBoolean(i) == false) {
                    continue;
                }
                String key = keys.getBytesRef(i, scratch).utf8ToString();
                actual.merge(key, sums.getLong(i), Long::sum);
            }
        }
        assertThat(actual, equalTo(oracle));
    }

    /**
     * Folds output from a two-LONG-key aggregation. Output layout per page:
     * [key1(LONG), key2(LONG), sum(LONG), seen(BOOLEAN), failed(BOOLEAN)].
     */
    private void assertMatchesTwoLongOracle(List<TaggedPage> results, Map<String, Long> oracle) {
        Map<String, Long> actual = new HashMap<>();
        for (TaggedPage tagged : results) {
            Page page = tagged.page;
            assertThat(page.getBlockCount(), equalTo(5)); // key1, key2, sum, seen, failed
            LongBlock keys1 = page.getBlock(0);
            LongBlock keys2 = page.getBlock(1);
            LongBlock sums = page.getBlock(2);
            BooleanBlock seenFlags = page.getBlock(3);
            for (int i = 0; i < page.getPositionCount(); i++) {
                if (seenFlags.getBoolean(i) == false) {
                    continue;
                }
                actual.merge(keys1.getLong(i) + ":" + keys2.getLong(i), sums.getLong(i), Long::sum);
            }
        }
        assertThat(actual, equalTo(oracle));
    }

    /**
     * Folds output from a LONG+BYTES_REF-key aggregation. Output layout per page:
     * [key1(LONG), key2(BYTES_REF), sum(LONG), seen(BOOLEAN), failed(BOOLEAN)].
     */
    private void assertMatchesLongBytesRefOracle(List<TaggedPage> results, Map<String, Long> oracle) {
        Map<String, Long> actual = new HashMap<>();
        BytesRef scratch = new BytesRef();
        for (TaggedPage tagged : results) {
            Page page = tagged.page;
            assertThat(page.getBlockCount(), equalTo(5)); // key1, key2, sum, seen, failed
            LongBlock keys1 = page.getBlock(0);
            BytesRefBlock keys2 = page.getBlock(1);
            LongBlock sums = page.getBlock(2);
            BooleanBlock seenFlags = page.getBlock(3);
            for (int i = 0; i < page.getPositionCount(); i++) {
                if (seenFlags.getBoolean(i) == false) {
                    continue;
                }
                String key = keys1.getLong(i) + ":" + keys2.getBytesRef(i, scratch).utf8ToString();
                actual.merge(key, sums.getLong(i), Long::sum);
            }
        }
        assertThat(actual, equalTo(oracle));
    }

    /**
     * Folds output from a single-LONG-key aggregation with both SumLong and MaxLong. Output layout:
     * [key(LONG), sum(LONG), seen_sum(BOOLEAN), failed_sum(BOOLEAN), max(LONG), seen_max(BOOLEAN)].
     */
    private void assertMatchesSumAndMaxOracle(List<TaggedPage> results, Map<Long, Long> sumOracle, Map<Long, Long> maxOracle) {
        Map<Long, Long> actualSum = new HashMap<>();
        Map<Long, Long> actualMax = new HashMap<>();
        for (TaggedPage tagged : results) {
            Page page = tagged.page;
            assertThat(page.getBlockCount(), equalTo(6)); // key, sum, seen, failed, max, seen
            LongBlock keys = page.getBlock(0);
            LongBlock sums = page.getBlock(1);
            BooleanBlock sumSeen = page.getBlock(2);
            LongBlock maxVals = page.getBlock(4);
            BooleanBlock maxSeen = page.getBlock(5);
            for (int i = 0; i < page.getPositionCount(); i++) {
                long key = keys.isNull(i) ? 0L : keys.getLong(i);
                if (sumSeen.getBoolean(i)) {
                    actualSum.merge(key, sums.getLong(i), Long::sum);
                }
                if (maxSeen.getBoolean(i)) {
                    actualMax.merge(key, maxVals.getLong(i), Long::max);
                }
            }
        }
        assertThat(actualSum, equalTo(sumOracle));
        assertThat(actualMax, equalTo(maxOracle));
    }

    /** COUNT(*) aggregator spec (empty channels → countAll=true). */
    private PartitionedHashAggregationOperator.AggregatorSpec countAllFactory() {
        return new PartitionedHashAggregationOperator.AggregatorSpec(CountAggregatorFunction.supplier(), List.of());
    }

    /**
     * Builds {@code rows} single-column (LONG key) pages over {@code cardinality} distinct keys,
     * updating {@code countOracle} (key → expected occurrence count). Used for COUNT(*) tests
     * where no value column is needed.
     */
    private List<Page> randomCountInput(int rows, int cardinality, Map<Long, Long> countOracle) {
        List<Page> pages = new ArrayList<>();
        int remaining = rows;
        while (remaining > 0) {
            int pageSize = Math.min(remaining, between(50, 500));
            remaining -= pageSize;
            try (LongBlock.Builder keyBuilder = blockFactory.newLongBlockBuilder(pageSize)) {
                for (int i = 0; i < pageSize; i++) {
                    long key = randomLongBetween(0, cardinality - 1);
                    keyBuilder.appendLong(key);
                    countOracle.merge(key, 1L, Long::sum);
                }
                pages.add(new Page(keyBuilder.build()));
            }
        }
        return pages;
    }

    /**
     * Folds COUNT(*) intermediate state rows across all tagged pages and compares against
     * {@code oracle}. COUNT intermediate state: [key (LONG), count (LONG), seen (BOOLEAN)] = 3 blocks.
     */
    private void assertMatchesCountOracle(List<TaggedPage> results, Map<Long, Long> oracle) {
        Map<Long, Long> actual = new HashMap<>();
        for (TaggedPage tagged : results) {
            Page page = tagged.page;
            assertThat(page.getBlockCount(), equalTo(3)); // key + count + seen
            LongBlock keys = page.getBlock(0);
            LongBlock counts = page.getBlock(1);
            BooleanBlock seenFlags = page.getBlock(2);
            for (int i = 0; i < page.getPositionCount(); i++) {
                if (seenFlags.getBoolean(i) == false) {
                    continue;
                }
                long key = keys.isNull(i) ? 0L : keys.getLong(i);
                actual.merge(key, counts.getLong(i), Long::sum);
            }
        }
        assertThat(actual, equalTo(oracle));
    }
}
