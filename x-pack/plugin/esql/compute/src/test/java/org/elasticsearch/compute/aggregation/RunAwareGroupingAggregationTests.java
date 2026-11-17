/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.HashAggregationOperatorTests;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.test.ComputeTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.hamcrest.Matchers.equalTo;

/**
 * {@code SUM(int)}, {@code COUNT(field)} and {@code COUNT(*)} over a key built as the index's primary sort field, whose group
 * ids reach the aggregators as runs. Combining a run before touching its group's state must give what adding every position
 * does, so each case is read three ways: through the run path, through the plain path, and from the rows themselves.
 */
public class RunAwareGroupingAggregationTests extends ComputeTestCase {

    /** The key and value of every row of every page; a null value is a row whose value is missing. */
    private record Input(List<long[]> keys, List<Integer[]> values) {}

    /** Runs far longer than a page, so they cross page boundaries, over ints large enough that a run's sum leaves the int range. */
    public void testLongRunsAcrossPages() {
        final List<long[]> keys = new ArrayList<>();
        final List<Integer[]> values = new ArrayList<>();
        long key = 0;
        int left = 0;
        for (int page = 0; page < between(2, 5); page++) {
            final long[] pageKeys = new long[between(3_000, 6_000)];
            final Integer[] pageValues = new Integer[pageKeys.length];
            for (int row = 0; row < pageKeys.length; row++) {
                if (left == 0) {
                    key += between(1, 5);
                    left = between(1, 4_000);
                }
                left--;
                pageKeys[row] = key;
                pageValues[row] = randomBoolean() ? randomIntBetween(Integer.MAX_VALUE - 1000, Integer.MAX_VALUE) : randomInt();
            }
            keys.add(pageKeys);
            values.add(pageValues);
        }
        assertAllWays(new Input(keys, values));
    }

    /** A group that appears in several runs of one page is added by each of them. */
    public void testGroupInSeveralRuns() {
        final long[] keys = { 1, 1, 2, 2, 1, 1, 3, 2, 2, 2, 1 };
        final Integer[] values = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 };
        assertAllWays(new Input(List.of(keys), List.<Integer[]>of(values)));
    }

    public void testOneGroupPage() {
        final long[] keys = new long[between(1, 300)];
        Arrays.fill(keys, randomLong());
        final Integer[] values = new Integer[keys.length];
        Arrays.setAll(values, i -> randomInt());
        assertAllWays(new Input(List.of(keys), List.<Integer[]>of(values)));
    }

    public void testPagesOfOneRow() {
        final List<long[]> keys = new ArrayList<>();
        final List<Integer[]> values = new ArrayList<>();
        for (int page = 0; page < between(5, 40); page++) {
            keys.add(new long[] { randomLongBetween(0, 4) });
            values.add(new Integer[] { randomInt() });
        }
        assertAllWays(new Input(keys, values));
    }

    /** Keys that are not sorted at all: a flag only says the ids tend to come in runs, never that they do. */
    public void testKeysThatAreNotSorted() {
        final List<long[]> keys = new ArrayList<>();
        final List<Integer[]> values = new ArrayList<>();
        for (int page = 0; page < between(2, 6); page++) {
            final long[] pageKeys = new long[between(1, 2_000)];
            final Integer[] pageValues = new Integer[pageKeys.length];
            for (int row = 0; row < pageKeys.length; row++) {
                pageKeys[row] = randomLongBetween(0, 30);
                pageValues[row] = randomInt();
            }
            keys.add(pageKeys);
            values.add(pageValues);
        }
        assertAllWays(new Input(keys, values));
    }

    /** Missing values take the aggregators' other paths, which have no run form and fall back. */
    public void testMissingValues() {
        final List<long[]> keys = new ArrayList<>();
        final List<Integer[]> values = new ArrayList<>();
        long key = 0;
        for (int page = 0; page < between(2, 5); page++) {
            final long[] pageKeys = new long[between(1, 3_000)];
            final Integer[] pageValues = new Integer[pageKeys.length];
            for (int row = 0; row < pageKeys.length; row++) {
                if (randomInt(200) == 0) {
                    key++;
                }
                pageKeys[row] = key;
                pageValues[row] = randomInt(5) == 0 ? null : randomInt();
            }
            keys.add(pageKeys);
            values.add(pageValues);
        }
        assertAllWays(new Input(keys, values));
    }

    private void assertAllWays(Input input) {
        final Map<Long, List<Object>> expected = expected(input);
        assertThat("run path", read(input, true), equalTo(expected));
        assertThat("plain path", read(input, false), equalTo(expected));
    }

    /** Per key: the sum of its values or null when it has none, the count of its values, and the count of its rows. */
    private static Map<Long, List<Object>> expected(Input input) {
        final Map<Long, long[]> acc = new TreeMap<>();
        final Map<Long, Boolean> sawValue = new TreeMap<>();
        for (int page = 0; page < input.keys().size(); page++) {
            final long[] keys = input.keys().get(page);
            final Integer[] values = input.values().get(page);
            for (int row = 0; row < keys.length; row++) {
                final long[] counts = acc.computeIfAbsent(keys[row], k -> new long[3]);
                counts[2]++;
                if (values[row] != null) {
                    counts[0] += values[row];
                    counts[1]++;
                    sawValue.put(keys[row], true);
                }
            }
        }
        final Map<Long, List<Object>> result = new TreeMap<>();
        acc.forEach((key, counts) -> result.put(key, Arrays.asList(sawValue.containsKey(key) ? counts[0] : null, counts[1], counts[2])));
        return result;
    }

    private Map<Long, List<Object>> read(Input input, boolean primarySorted) {
        final DriverContext driverContext = driverContext();
        final AggregatorMode mode = AggregatorMode.SINGLE;
        final Map<Long, List<Object>> result = new TreeMap<>();
        try (
            Operator operator = HashAggregationOperatorTests.randomBuilder()
                .groups(List.of(new BlockHash.GroupSpec(0, ElementType.LONG, null, null, primarySorted)))
                .mode(mode)
                .aggregators(
                    List.of(
                        new SumIntAggregatorFunctionSupplier().groupingAggregatorFactory(mode, List.of(1)),
                        CountAggregatorFunction.supplier().groupingAggregatorFactory(mode, List.of(1)),
                        CountAggregatorFunction.supplier().groupingAggregatorFactory(mode, List.of())
                    )
                )
                .build()
                .get(driverContext)
        ) {
            for (int page = 0; page < input.keys().size(); page++) {
                operator.addInput(page(driverContext, input.keys().get(page), input.values().get(page)));
            }
            operator.finish();
            Page out;
            while ((out = operator.getOutput()) != null) {
                try {
                    final LongBlock keys = out.getBlock(0);
                    final LongBlock sums = out.getBlock(1);
                    final LongBlock valueCounts = out.getBlock(2);
                    final LongBlock rowCounts = out.getBlock(3);
                    for (int p = 0; p < out.getPositionCount(); p++) {
                        final List<Object> row = Arrays.asList(
                            sums.isNull(p) ? null : sums.getLong(sums.getFirstValueIndex(p)),
                            valueCounts.getLong(valueCounts.getFirstValueIndex(p)),
                            rowCounts.getLong(rowCounts.getFirstValueIndex(p))
                        );
                        assertNull("a group read twice", result.put(keys.getLong(keys.getFirstValueIndex(p)), row));
                    }
                } finally {
                    out.releaseBlocks();
                }
            }
        }
        return result;
    }

    /** A page whose values are a vector when none is missing, as the loaders hand them over, and a block with nulls otherwise. */
    private static Page page(DriverContext driverContext, long[] keys, Integer[] values) {
        final Block keyBlock = driverContext.blockFactory().newLongArrayVector(keys.clone(), keys.length).asBlock();
        if (Arrays.stream(values).noneMatch(v -> v == null)) {
            final int[] plain = Arrays.stream(values).mapToInt(Integer::intValue).toArray();
            return new Page(keyBlock, driverContext.blockFactory().newIntArrayVector(plain, plain.length).asBlock());
        }
        try (IntBlock.Builder builder = driverContext.blockFactory().newIntBlockBuilder(values.length)) {
            for (Integer value : values) {
                if (value == null) {
                    builder.appendNull();
                } else {
                    builder.appendInt(value);
                }
            }
            return new Page(keyBlock, builder.build());
        }
    }

    private DriverContext driverContext() {
        return new DriverContext(blockFactory().bigArrays(), blockFactory(), null);
    }
}
