/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.BlockTestUtils;
import org.elasticsearch.indices.CrankyCircuitBreakerService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

/**
 * Checks {@link RowKeyHash} against a model where the key of a row is the list of values of each key column.
 */
public class RowKeyHashTests extends BlockHashTestCase {
    /**
     * The element types {@code LIMIT ... BY} accepts as keys.
     */
    private static final List<ElementType> TYPES = List.of(
        ElementType.BOOLEAN,
        ElementType.INT,
        ElementType.LONG,
        ElementType.FLOAT,
        ElementType.DOUBLE,
        ElementType.BYTES_REF,
        ElementType.DOUBLE_RANGE,
        ElementType.NULL
    );

    /**
     * How a page holds its keys. Each takes a different path through the hashes.
     */
    private enum Shape {
        /** One value at every position: the batch paths over vectors. */
        VECTORS,
        /** At most one value, with nulls: the batch paths that handle nulls, where a hash has one. */
        NULLS,
        /** Nulls and multivalues: the multivalue path. */
        MULTIVALUES
    }

    public void testOneColumn() {
        assertGroups(blockFactory, List.of(randomFrom(TYPES)), between(1, 10));
    }

    public void testSeveralColumns() {
        assertGroups(blockFactory, randomList(2, 5, () -> randomFrom(TYPES)), between(1, 10));
    }

    /**
     * Sixty four columns fill the null bits of eight bytes, so the multivalue bit goes in a ninth.
     */
    public void testSixtyFourColumns() {
        List<ElementType> types = new ArrayList<>(Collections.nCopies(63, ElementType.LONG));
        types.add(ElementType.BYTES_REF);
        Collections.shuffle(types, random());
        assertGroups(blockFactory, types, between(1, 5));
    }

    /**
     * A single double key groups like several keys: two NaNs with different bits are different keys.
     */
    public void testNaNsKeepTheirBits() {
        double nan = Double.NaN;
        double otherNan = Double.longBitsToDouble(Double.doubleToRawLongBits(Double.NaN) | 1L);
        try (RowKeyHash hash = new RowKeyHash(new int[] { 0 }, List.of(ElementType.DOUBLE), blockFactory)) {
            // A vector page, through the batch path.
            Page vectors = new Page(blockFactory.newDoubleArrayVector(new double[] { nan, otherNan, nan }, 3).asBlock());
            int[] first;
            try {
                first = hash.add(vectors);
            } finally {
                vectors.releaseBlocks();
            }
            assertThat(first[0], equalTo(first[2]));
            assertThat(first[0] == first[1], equalTo(false));
            // A page with a null and a multivalue, through the other paths: the same keys keep their groups.
            Page mixed = new Page(
                BlockTestUtils.asBlock(blockFactory, ElementType.DOUBLE, Arrays.asList(otherNan, null, nan, List.of(1.0, 2.0)))
            );
            try {
                int[] second = hash.add(mixed);
                assertThat(second[0], equalTo(first[1]));
                assertThat(second[2], equalTo(first[0]));
            } finally {
                mixed.releaseBlocks();
            }
        }
    }

    public void testCircuitBreaking() {
        CrankyCircuitBreakerService crankyService = new CrankyCircuitBreakerService();
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, crankyService);
        BlockFactory crankyFactory = BlockFactory.builder(bigArrays).build();
        try {
            assertGroups(crankyFactory, randomList(1, 3, () -> randomFrom(TYPES)), between(1, 5));
        } catch (CircuitBreakingException e) {
            assertThat(e.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
        }
        // The hash charges its own breaker, which the shared teardown doesn't check.
        assertThat(crankyService.getBreaker(CircuitBreaker.REQUEST).getUsed(), equalTo(0L));
    }

    /**
     * @param hashFactory builds the hash, while the pages always come from {@link #blockFactory}
     */
    private void assertGroups(BlockFactory hashFactory, List<ElementType> types, int pages) {
        int[] channels = new int[types.size()];
        for (int c = 0; c < channels.length; c++) {
            channels[c] = c;
        }
        // A few values per column, so keys repeat.
        List<List<Object>> pools = new ArrayList<>();
        for (ElementType type : types) {
            pools.add(randomList(1, 4, () -> BlockTestUtils.randomValue(type)));
        }
        Map<Integer, List<List<Object>>> keyOfGroup = new HashMap<>();
        Map<List<List<Object>>, Integer> groupOfKey = new HashMap<>();
        try (RowKeyHash hash = new RowKeyHash(channels, types, hashFactory)) {
            for (int p = 0; p < pages; p++) {
                Shape shape = randomFrom(Shape.values());
                int positions = between(1, 200);
                List<List<List<Object>>> rows = randomRows(types, pools, shape, positions);
                Page page = page(types, rows);
                try {
                    int[] groupIds = hash.add(page);
                    for (int r = 0; r < positions; r++) {
                        List<List<Object>> key = rows.get(r);
                        Integer expected = groupOfKey.putIfAbsent(key, groupIds[r]);
                        if (expected != null) {
                            assertThat("same key, same group: " + key, groupIds[r], equalTo(expected));
                        }
                        List<List<Object>> previous = keyOfGroup.putIfAbsent(groupIds[r], key);
                        if (previous != null) {
                            assertThat("one key per group", key, equalTo(previous));
                        }
                    }
                } finally {
                    page.releaseBlocks();
                }
            }
            assertThat(hash.numKeys(), equalTo(groupOfKey.size()));
            for (int group = 0; group < hash.numKeys(); group++) {
                assertTrue("group ids are dense, missing " + group, keyOfGroup.containsKey(group));
            }
        }
    }

    private List<List<List<Object>>> randomRows(List<ElementType> types, List<List<Object>> pools, Shape shape, int positions) {
        List<List<List<Object>>> rows = new ArrayList<>();
        for (int r = 0; r < positions; r++) {
            List<List<Object>> row = new ArrayList<>();
            for (int c = 0; c < types.size(); c++) {
                int count = switch (shape) {
                    case VECTORS -> 1;
                    case NULLS -> between(0, 1);
                    case MULTIVALUES -> between(0, 3);
                };
                List<Object> values = new ArrayList<>();
                if (types.get(c) != ElementType.NULL) {
                    for (int v = 0; v < count; v++) {
                        values.add(randomFrom(pools.get(c)));
                    }
                }
                row.add(values);
            }
            rows.add(row);
        }
        return rows;
    }

    private Page page(List<ElementType> types, List<List<List<Object>>> rows) {
        Block[] blocks = new Block[types.size()];
        for (int c = 0; c < types.size(); c++) {
            List<Object> column = new ArrayList<>();
            boolean allNull = true;
            for (List<List<Object>> row : rows) {
                List<Object> values = row.get(c);
                allNull &= values.isEmpty();
                column.add(values.isEmpty() ? null : values.size() == 1 ? values.get(0) : values);
            }
            // A column with no value is often a constant null block, whatever its type.
            blocks[c] = allNull && randomBoolean()
                ? blockFactory.newConstantNullBlock(rows.size())
                : BlockTestUtils.asBlock(blockFactory, types.get(c), column);
        }
        Page page = new Page(blocks);
        // Keywords read from doc values often come as ordinals over a dictionary.
        return randomBoolean() ? BlockTestUtils.convertBytesRefsToOrdinals(page) : page;
    }
}
