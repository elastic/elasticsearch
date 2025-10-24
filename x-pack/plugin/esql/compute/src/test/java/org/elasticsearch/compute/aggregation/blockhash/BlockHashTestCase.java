/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.MockBlockFactory;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class BlockHashTestCase extends ESTestCase {

    final CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofGb(1));
    final BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, mockBreakerService(breaker));
    final MockBlockFactory blockFactory = new MockBlockFactory(breaker, bigArrays);

    @After
    public void checkBreaker() {
        blockFactory.ensureAllBlocksAreReleased();
        assertThat(breaker.getUsed(), is(0L));
    }

    // A breaker service that always returns the given breaker for getBreaker(CircuitBreaker.REQUEST)
    private static CircuitBreakerService mockBreakerService(CircuitBreaker breaker) {
        CircuitBreakerService breakerService = mock(CircuitBreakerService.class);
        when(breakerService.getBreaker(CircuitBreaker.REQUEST)).thenReturn(breaker);
        return breakerService;
    }

    protected record OrdsAndKeys(String description, int positionOffset, IntBlock ords, Block[] keys, IntVector nonEmpty) {}

    protected static void hash(boolean collectKeys, BlockHash blockHash, Consumer<OrdsAndKeys> callback, Block... values) {
        blockHash.add(new Page(values), new GroupingAggregatorFunction.AddInput() {
            private void addBlock(int positionOffset, IntBlock groupIds) {
                OrdsAndKeys result = new OrdsAndKeys(
                    blockHash.toString(),
                    positionOffset,
                    groupIds,
                    collectKeys ? blockHash.getKeys() : null,
                    blockHash.nonEmpty()
                );

                try {
                    Set<Integer> allowedOrds = new HashSet<>();
                    for (int p = 0; p < result.nonEmpty.getPositionCount(); p++) {
                        allowedOrds.add(result.nonEmpty.getInt(p));
                    }
                    for (int p = 0; p < result.ords.getPositionCount(); p++) {
                        if (result.ords.isNull(p)) {
                            continue;
                        }
                        int start = result.ords.getFirstValueIndex(p);
                        int end = start + result.ords.getValueCount(p);
                        for (int i = start; i < end; i++) {
                            int ord = result.ords.getInt(i);
                            if (false == allowedOrds.contains(ord)) {
                                fail("ord is not allowed " + ord);
                            }
                        }
                    }
                    callback.accept(result);
                } finally {
                    Releasables.close(result.keys == null ? null : Releasables.wrap(result.keys), result.nonEmpty);
                }
            }

            @Override
            public void add(int positionOffset, IntArrayBlock groupIds) {
                addBlock(positionOffset, groupIds);
            }

            @Override
            public void add(int positionOffset, IntBigArrayBlock groupIds) {
                addBlock(positionOffset, groupIds);
            }

            @Override
            public void add(int positionOffset, IntVector groupIds) {
                addBlock(positionOffset, groupIds.asBlock());
            }

            @Override
            public void close() {
                fail("hashes should not close AddInput");
            }
        });
        if (blockHash instanceof LongLongBlockHash == false
            && blockHash instanceof BytesRefLongBlockHash == false
            && blockHash instanceof BytesRef2BlockHash == false
            && blockHash instanceof BytesRef3BlockHash == false) {
            Block[] keys = blockHash.getKeys();
            try (ReleasableIterator<IntBlock> lookup = blockHash.lookup(new Page(keys), ByteSizeValue.ofKb(between(1, 100)))) {
                while (lookup.hasNext()) {
                    try (IntBlock ords = lookup.next()) {
                        for (int p = 0; p < ords.getPositionCount(); p++) {
                            assertFalse(ords.isNull(p));
                        }
                    }
                }
            } finally {
                Releasables.closeExpectNoException(keys);
            }
        }
    }

    private static void assertSeenGroupIdsAndNonEmpty(BlockHash blockHash) {
        try (BitArray seenGroupIds = blockHash.seenGroupIds(BigArrays.NON_RECYCLING_INSTANCE); IntVector nonEmpty = blockHash.nonEmpty()) {
            assertThat(
                "seenGroupIds cardinality doesn't match with nonEmpty size",
                seenGroupIds.cardinality(),
                equalTo((long) nonEmpty.getPositionCount())
            );

            for (int position = 0; position < nonEmpty.getPositionCount(); position++) {
                int groupId = nonEmpty.getInt(position);
                assertThat("group " + groupId + " from nonEmpty isn't set in seenGroupIds", seenGroupIds.get(groupId), is(true));
            }
        }
    }

    protected void assertOrds(IntBlock ordsBlock, Integer... expectedOrds) {
        assertOrds(ordsBlock, Arrays.stream(expectedOrds).map(l -> l == null ? null : new int[] { l }).toArray(int[][]::new));
    }

    protected void assertOrds(IntBlock ordsBlock, int[]... expectedOrds) {
        assertEquals(expectedOrds.length, ordsBlock.getPositionCount());
        for (int p = 0; p < expectedOrds.length; p++) {
            int start = ordsBlock.getFirstValueIndex(p);
            int count = ordsBlock.getValueCount(p);
            if (expectedOrds[p] == null) {
                if (false == ordsBlock.isNull(p)) {
                    StringBuilder error = new StringBuilder();
                    error.append(p);
                    error.append(": expected null but was [");
                    for (int i = 0; i < count; i++) {
                        if (i != 0) {
                            error.append(", ");
                        }
                        error.append(ordsBlock.getInt(start + i));
                    }
                    fail(error.append("]").toString());
                }
                continue;
            }
            assertFalse(p + ": expected not null", ordsBlock.isNull(p));
            int[] actual = new int[count];
            for (int i = 0; i < count; i++) {
                actual[i] = ordsBlock.getInt(start + i);
            }
            assertThat("position " + p, actual, equalTo(expectedOrds[p]));
        }
    }

    protected void assertKeys(Block[] actualKeys, Object... expectedKeys) {
        Object[][] flipped = new Object[expectedKeys.length][];
        for (int r = 0; r < flipped.length; r++) {
            flipped[r] = new Object[] { expectedKeys[r] };
        }
        assertKeys(actualKeys, flipped);
    }

    protected void assertKeys(Block[] actualKeys, Object[][] expectedKeys) {
        for (int r = 0; r < expectedKeys.length; r++) {
            assertThat(actualKeys, arrayWithSize(expectedKeys[r].length));
        }
        for (int c = 0; c < actualKeys.length; c++) {
            assertThat("block " + c, actualKeys[c].getPositionCount(), equalTo(expectedKeys.length));
        }
        for (int r = 0; r < expectedKeys.length; r++) {
            for (int c = 0; c < actualKeys.length; c++) {
                if (expectedKeys[r][c] == null) {
                    assertThat("expected null key", actualKeys[c].isNull(r), equalTo(true));
                    continue;
                }
                assertThat("expected non-null key", actualKeys[c].isNull(r), equalTo(false));
                if (expectedKeys[r][c] instanceof Integer v) {
                    assertThat(((IntBlock) actualKeys[c]).getInt(r), equalTo(v));
                } else if (expectedKeys[r][c] instanceof Long v) {
                    assertThat(((LongBlock) actualKeys[c]).getLong(r), equalTo(v));
                } else if (expectedKeys[r][c] instanceof Double v) {
                    assertThat(((DoubleBlock) actualKeys[c]).getDouble(r), equalTo(v));
                } else if (expectedKeys[r][c] instanceof String v) {
                    assertThat(((BytesRefBlock) actualKeys[c]).getBytesRef(r, new BytesRef()), equalTo(new BytesRef(v)));
                } else if (expectedKeys[r][c] instanceof Boolean v) {
                    assertThat(((BooleanBlock) actualKeys[c]).getBoolean(r), equalTo(v));
                } else {
                    throw new IllegalArgumentException("unsupported type " + expectedKeys[r][c].getClass());
                }
            }
        }
    }

    protected IntVector intRange(int startInclusive, int endExclusive) {
        return IntVector.range(startInclusive, endExclusive, TestBlockFactory.getNonBreakingInstance());
    }

    protected IntVector intVector(int... values) {
        return TestBlockFactory.getNonBreakingInstance().newIntArrayVector(values, values.length);
    }
}
