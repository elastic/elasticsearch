/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static com.carrotsearch.randomizedtesting.RandomizedTest.between;
import static org.elasticsearch.compute.data.BlockUtils.toJavaObject;
import static org.elasticsearch.compute.test.BlockTestUtils.valuesAtPositions;
import static org.elasticsearch.core.TimeValue.timeValueNanos;
import static org.elasticsearch.test.ListMatcher.matchesList;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test utilities for asserting {@link BlockHash#getKeys} behavior.
 */
class BlockHashKeysTestHelper implements Releasable {
    private static final Logger logger = LogManager.getLogger(BlockHashKeysTestHelper.class);

    private final BlockHash blockHash;
    final IntVector nonEmpty;

    /**
     * @param blockHash must not be modified after being passed here;
     *        this helper does not close the {@link BlockHash}
     */
    BlockHashKeysTestHelper(BlockHash blockHash) {
        this.blockHash = blockHash;
        this.nonEmpty = blockHash.nonEmpty();
    }

    /**
     * Pick random positions from {@code nonEmpty}.
     */
    static List<Integer> randomPositionsFrom(IntVector nonEmpty) {
        int maxSize = Math.max(nonEmpty.getPositionCount() / 2, 1);
        int size = between(1, maxSize);
        Set<Integer> set = new HashSet<>();
        while (set.size() < size) {
            set.add(between(0, nonEmpty.getPositionCount() - 1));
        }
        List<Integer> list = new ArrayList<>(set);
        Collections.sort(list);
        return ESTestCase.shuffledList(list);
    }

    /**
     * Build an {@link IntVector} by picking values from {@code nonEmpty} at the given positions.
     */
    static IntVector select(IntVector nonEmpty, List<Integer> positions) {
        try (IntVector.Builder builder = nonEmpty.blockFactory().newIntVectorFixedBuilder(positions.size())) {
            for (int p : positions) {
                builder.appendInt(nonEmpty.getInt(p));
            }
            return builder.build();
        }
    }

    /**
     * Map from the <strong>value</strong> of {@code nonEmpty} to its position
     * <strong>in</strong> non-empty.
     */
    static Map<Integer, Integer> reverseNonEmpty(IntVector nonEmpty) {
        Map<Integer, Integer> result = new HashMap<>();
        for (int p = 0; p < nonEmpty.getPositionCount(); p++) {
            result.put(nonEmpty.getInt(p), p);
        }
        return result;
    }

    Block[] getKeys(IntVector selected) {
        return blockHash.getKeys(selected);
    }

    /**
     * Fetch the keys for the given positions as java objects.
     */
    static Set<List<Object>> keysToJavaObjects(Block[] keyBlocks) {
        Set<List<Object>> keys = new TreeSet<>(new KeyComparator());
        for (int p = 0; p < keyBlocks[0].getPositionCount(); p++) {
            List<Object> key = new ArrayList<>(keyBlocks.length);
            for (Block keyBlock : keyBlocks) {
                if (keyBlock.isNull(p)) {
                    key.add(null);
                } else {
                    key.add(valuesAtPositions(keyBlock, p, p + 1).get(0).get(0));
                    assertThat(keyBlock.getValueCount(p), equalTo(1));
                }
            }
            boolean firstTime = keys.add(key);
            assertTrue(firstTime);
        }
        return keys;

    }

    /**
     * Assert that the keys in the block hash match the expected set.
     */
    void assertKeys(IntVector selected, Set<List<Object>> expectedKeys) {
        Block[] keyBlocks = blockHash.getKeys(selected);
        try {
            Set<List<Object>> keys = keysToJavaObjects(keyBlocks);
            if (false == keys.equals(expectedKeys)) {
                List<List<Object>> keyList = new ArrayList<>(keys);
                List<List<Object>> oracleList = new ArrayList<>(expectedKeys);
                Collections.sort(keyList, new KeyComparator());
                Collections.sort(oracleList, new KeyComparator());
                int remaining = 5;
                int sub = 0;
                while (remaining > 0 && sub < keyList.size()) {
                    if (keyList.get(sub).equals(oracleList.get(sub)) == false) {
                        remaining--;
                    }
                    sub++;
                }
                sub++;
                if (sub == keyList.size()) {
                    assertMap("small number of key mismatches", keyList, matchesList(oracleList));
                } else {
                    assertMap("showing subset of key mismatches", keyList.subList(0, sub), matchesList(oracleList.subList(0, sub)));
                }
            }
            assertOneByOne(nonEmpty, keyBlocks);
        } finally {
            Releasables.closeExpectNoException(keyBlocks);
        }
    }

    /**
     * Pick a random subset of keys and assert that
     * {@link BlockHash#getKeys} returns the right values.
     */
    void assertRandomKeySubset() {
        long start = System.nanoTime();
        List<Integer> positions = randomPositionsFrom(nonEmpty);
        Block[] allKeys = null;
        Block[] keys = null;
        try (IntVector selected = select(nonEmpty, positions)) {
            allKeys = blockHash.getKeys(nonEmpty);
            keys = blockHash.getKeys(selected);

            assertKeyPositionCounts(selected, keys);
            assertSubset(selected, allKeys, keys);
            assertOneByOne(selected, keys);
            assertAddIsNoop(selected, keys);

        } finally {
            if (allKeys != null) {
                Releasables.close(allKeys);
            }
            if (keys != null) {
                Releasables.close(keys);
            }
            logger.info("finished testing subset of length {} in {}", positions.size(), timeValueNanos(System.nanoTime() - start));
        }
    }

    /**
     * Re-add the keys and assert they map to the same group ids.
     */
    private void assertAddIsNoop(IntVector selected, Block[] keys) {
        blockHash.add(new Page(keys), new GroupingAggregatorFunction.AddInput() {
            @Override
            public void add(int positionOffset, IntArrayBlock groupIds) {
                fail("shouldn't be called");
            }

            @Override
            public void add(int positionOffset, IntBigArrayBlock groupIds) {
                fail("shouldn't be called");
            }

            @Override
            public void add(int positionOffset, IntVector groupIds) {
                for (int p = 0; p < groupIds.getPositionCount(); p++) {
                    assertThat(groupIds.getInt(p), equalTo(selected.getInt(positionOffset + p)));
                }
            }

            @Override
            public void close() {}
        });
    }

    private static void assertKeyPositionCounts(IntVector selected, Block[] keys) {
        for (Block key : keys) {
            assertThat(key.getPositionCount(), equalTo(selected.getPositionCount()));
        }
    }

    /**
     * Assert that the subset keys match the corresponding entries in all keys.
     */
    private void assertSubset(IntVector selected, Block[] allKeys, Block[] keys) {
        Map<Integer, Integer> reverse = reverseNonEmpty(nonEmpty);
        for (int p = 0; p < keys[0].getPositionCount(); p++) {
            for (int k = 0; k < keys.length; k++) {
                assertThat(toJavaObject(keys[k], p), equalTo(toJavaObject(allKeys[k], reverse.get(selected.getInt(p)))));
            }
        }
    }

    /**
     * Fetch keys one row at a time and assert they match the expected keys.
     */
    private void assertOneByOne(IntVector selected, Block[] keys) {
        for (int p = 0; p < keys[0].getPositionCount(); p++) {
            try (IntVector constVector = nonEmpty.blockFactory().newConstantIntVector(selected.getInt(p), 1)) {
                Block[] oneRow = getKeys(constVector);
                try {
                    assertThat(oneRow.length, equalTo(keys.length));
                    for (int k = 0; k < oneRow.length; k++) {
                        assertThat(toJavaObject(oneRow[k], 0), equalTo(toJavaObject(keys[k], p)));
                    }
                } finally {
                    Releasables.close(oneRow);
                }
            }
        }
    }

    @Override
    public void close() {
        Releasables.close(nonEmpty);
    }

    static class KeyComparator implements Comparator<List<?>> {
        @Override
        public int compare(List<?> lhs, List<?> rhs) {
            for (int i = 0; i < lhs.size(); i++) {
                @SuppressWarnings("unchecked")
                Comparable<Object> l = (Comparable<Object>) lhs.get(i);
                Object r = rhs.get(i);
                if (l == null) {
                    if (r == null) {
                        continue;
                    } else {
                        return 1;
                    }
                }
                if (r == null) {
                    return -1;
                }
                int cmp = l.compareTo(r);
                if (cmp != 0) {
                    return cmp;
                }
            }
            return 0;
        }
    }
}
