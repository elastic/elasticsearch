/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.table;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.common.util.LimitedBreaker;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.compute.test.MockBlockFactory;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.compute.test.TestWarningsSource;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class RowInTableLookupTests extends ESTestCase {
    final CircuitBreakerService breakerService = newLimitedBreakerService(ByteSizeValue.ofGb(1));
    final CircuitBreaker breaker = breakerService.getBreaker(CircuitBreaker.REQUEST);
    final MockBlockFactory blockFactory = new MockBlockFactory(
        BlockFactory.builder(new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, breakerService))
    );

    public void testDuplicateInts() {
        try (IntBlock.Builder builder = blockFactory.newIntBlockBuilder(2)) {
            builder.appendInt(1);
            builder.appendInt(1);
            try (Block b = builder.build()) {
                Exception e = expectThrows(IllegalArgumentException.class, () -> RowInTableLookup.build(blockFactory, new Block[] { b }));
                assertThat(e.getMessage(), equalTo("found a duplicate row"));
            }
        }
    }

    public void testMultivaluedInts() {
        try (IntBlock.Builder builder = blockFactory.newIntBlockBuilder(2)) {
            builder.beginPositionEntry();
            builder.appendInt(1);
            builder.appendInt(2);
            builder.endPositionEntry();
            try (Block b = builder.build()) {
                Exception e = expectThrows(IllegalArgumentException.class, () -> RowInTableLookup.build(blockFactory, new Block[] { b }));
                assertThat(e.getMessage(), equalTo("only single valued keys are supported"));
            }
        }
    }

    public void testDuplicateBytes() {
        try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(2)) {
            builder.appendBytesRef(new BytesRef("foo"));
            builder.appendBytesRef(new BytesRef("foo"));
            try (Block b = builder.build()) {
                Exception e = expectThrows(IllegalArgumentException.class, () -> RowInTableLookup.build(blockFactory, new Block[] { b }));
                assertThat(e.getMessage(), equalTo("found a duplicate row"));
            }
        }
    }

    public void testMultivaluedBytes() {
        try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(2)) {
            builder.beginPositionEntry();
            builder.appendBytesRef(new BytesRef("foo"));
            builder.appendBytesRef(new BytesRef("bar"));
            builder.endPositionEntry();
            try (Block b = builder.build()) {
                Exception e = expectThrows(IllegalArgumentException.class, () -> RowInTableLookup.build(blockFactory, new Block[] { b }));
                assertThat(e.getMessage(), equalTo("only single valued keys are supported"));
            }
        }
    }

    public void testRepeatedKeysReturnEveryMatchingBuildRow() {
        Block buildKeys;
        try (IntBlock.Builder builder = blockFactory.newIntBlockBuilder(3)) {
            builder.appendInt(1);
            builder.appendInt(2);
            builder.appendInt(1);
            buildKeys = builder.build();
        }
        RowInTableLookup lookup = RowInTableLookup.build(blockFactory, new Block[] { buildKeys }, true);
        buildKeys.close();

        Block probeKeys;
        try (IntBlock.Builder builder = blockFactory.newIntBlockBuilder(4)) {
            builder.appendInt(1);
            builder.appendInt(3);
            builder.appendInt(2);
            builder.appendInt(1);
            probeKeys = builder.build();
        }
        Page probe = new Page(probeKeys);
        try (lookup; ReleasableIterator<IntBlock> matches = lookup.lookup(probe, ByteSizeValue.ofKb(1))) {
            assertTrue(matches.hasNext());
            try (IntBlock result = matches.next()) {
                assertThat(result.getValueCount(0), equalTo(2));
                assertThat(result.getInt(result.getFirstValueIndex(0)), equalTo(0));
                assertThat(result.getInt(result.getFirstValueIndex(0) + 1), equalTo(2));
                assertThat(result.getValueCount(1), equalTo(0));
                assertThat(result.getValueCount(2), equalTo(1));
                assertThat(result.getInt(result.getFirstValueIndex(2)), equalTo(1));
                assertThat(result.getValueCount(3), equalTo(2));
                assertThat(result.getInt(result.getFirstValueIndex(3)), equalTo(0));
                assertThat(result.getInt(result.getFirstValueIndex(3) + 1), equalTo(2));
            }
            assertFalse(matches.hasNext());
        } finally {
            probe.releaseBlocks();
        }
    }

    public void testRepeatedBooleanKeysSkipUnseenValues() {
        Block buildKeys;
        try (BooleanBlock.Builder builder = blockFactory.newBooleanBlockBuilder(2)) {
            builder.appendBoolean(true);
            builder.appendBoolean(true);
            buildKeys = builder.build();
        }
        RowInTableLookup lookup = RowInTableLookup.build(blockFactory, new Block[] { buildKeys }, true);
        buildKeys.close();

        Block probeKeys;
        try (BooleanBlock.Builder builder = blockFactory.newBooleanBlockBuilder(3)) {
            builder.appendBoolean(false);
            builder.appendNull();
            builder.appendBoolean(true);
            probeKeys = builder.build();
        }
        assertThat(lookupRows(lookup, new Page(probeKeys)), equalTo(List.of(List.of(), List.of(), List.of(0, 1))));
    }

    public void testRepeatedDoubleKeysReturnEveryMatchingBuildRow() {
        Block buildKeys;
        try (DoubleBlock.Builder builder = blockFactory.newDoubleBlockBuilder(3)) {
            builder.appendDouble(1.5);
            builder.appendDouble(-0.5);
            builder.appendDouble(1.5);
            buildKeys = builder.build();
        }
        RowInTableLookup lookup = RowInTableLookup.build(blockFactory, new Block[] { buildKeys }, true);
        buildKeys.close();

        Block probeKeys;
        try (DoubleBlock.Builder builder = blockFactory.newDoubleBlockBuilder(3)) {
            builder.appendDouble(-0.5);
            builder.appendDouble(2.0);
            builder.appendDouble(1.5);
            probeKeys = builder.build();
        }
        assertThat(lookupRows(lookup, new Page(probeKeys)), equalTo(List.of(List.of(1), List.of(), List.of(0, 2))));
    }

    private static List<List<Integer>> lookupRows(RowInTableLookup lookup, Page probe) {
        List<List<Integer>> rows = new ArrayList<>();
        try (lookup; ReleasableIterator<IntBlock> matches = lookup.lookup(probe, ByteSizeValue.ofKb(1))) {
            while (matches.hasNext()) {
                try (IntBlock result = matches.next()) {
                    for (int p = 0; p < result.getPositionCount(); p++) {
                        List<Integer> positionRows = new ArrayList<>();
                        int start = result.getFirstValueIndex(p);
                        for (int i = start; i < start + result.getValueCount(p); i++) {
                            positionRows.add(result.getInt(i));
                        }
                        rows.add(positionRows);
                    }
                }
            }
        } finally {
            probe.releaseBlocks();
        }
        return rows;
    }

    public void testRepeatedKeyHashIsReleasedWhenRowIndexAllocationFails() {
        int rowCount = 12345;
        CircuitBreakerService failingBreakerService = LimitedBreaker.service("lookup-test", ByteSizeValue.ofGb(1));
        CircuitBreaker failingBreaker = failingBreakerService.getBreaker(CircuitBreaker.REQUEST);
        BigArrays failingBigArrays = new BigArrays(null, failingBreakerService, CircuitBreaker.REQUEST) {
            @Override
            public IntArray newIntArray(long size, boolean clearOnResize) {
                if (size == rowCount) {
                    throw new CircuitBreakingException("injected allocation failure", CircuitBreaker.Durability.TRANSIENT);
                }
                return super.newIntArray(size, clearOnResize);
            }
        };
        BlockFactory failingBlockFactory = BlockFactory.builder(failingBigArrays).breaker(failingBreaker).build();

        try (IntBlock.Builder builder = TestBlockFactory.getNonBreakingInstance().newIntBlockBuilder(rowCount)) {
            for (int row = 0; row < rowCount; row++) {
                builder.appendInt(row % 10);
            }
            try (Block keys = builder.build()) {
                expectThrows(
                    CircuitBreakingException.class,
                    () -> new RepeatedKeyBlockHashRowInTableLookup(failingBlockFactory, new Block[] { keys })
                );
                assertThat(failingBreaker.getUsed(), is(0L));
            }
        }
    }

    public void testUniqueRepeatedKeyLookupReturnsSingleValuedPositions() {
        Block buildKeys;
        try (IntBlock.Builder builder = blockFactory.newIntBlockBuilder(2)) {
            builder.appendInt(1);
            builder.appendInt(2);
            buildKeys = builder.build();
        }
        RowInTableLookup lookup = RowInTableLookup.build(blockFactory, new Block[] { buildKeys }, true);
        buildKeys.close();

        Block probeKeys;
        try (IntBlock.Builder builder = blockFactory.newIntBlockBuilder(1)) {
            builder.appendInt(2);
            probeKeys = builder.build();
        }
        Page probe = new Page(probeKeys);
        try (lookup; ReleasableIterator<IntBlock> matches = lookup.lookup(probe, ByteSizeValue.ofKb(1))) {
            try (IntBlock result = matches.next()) {
                assertNotNull(result.asVector());
                assertThat(result.getInt(0), equalTo(1));
            }
            assertFalse(matches.hasNext());
        } finally {
            probe.releaseBlocks();
        }
    }

    public void testRepeatedLookupNullAndMultivaluedProbeKeysDoNotMatch() {
        Block buildKeys;
        try (IntBlock.Builder builder = blockFactory.newIntBlockBuilder(1)) {
            builder.appendInt(1);
            buildKeys = builder.build();
        }
        DriverContext driverContext = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, TestBlockFactory.getNonBreakingInstance(), null);
        Warnings warnings = driverContext.createWarnings(new TestWarningsSource("lookup"));
        RowInTableLookup lookup = RowInTableLookup.build(blockFactory, new Block[] { buildKeys }, true);
        buildKeys.close();

        Block probeKeys;
        try (IntBlock.Builder builder = blockFactory.newIntBlockBuilder(3)) {
            builder.appendNull();
            builder.beginPositionEntry();
            builder.appendInt(1);
            builder.appendInt(2);
            builder.endPositionEntry();
            builder.appendInt(1);
            probeKeys = builder.build();
        }
        Page probe = new Page(probeKeys);
        try (lookup; ReleasableIterator<IntBlock> matches = lookup.lookup(probe, ByteSizeValue.ofKb(1), warnings)) {
            try (IntBlock result = matches.next()) {
                assertThat(result.getValueCount(0), equalTo(0));
                assertThat(result.getValueCount(1), equalTo(0));
                assertThat(result.getValueCount(2), equalTo(1));
                assertThat(result.getInt(result.getFirstValueIndex(2)), equalTo(0));
            }
            assertFalse(matches.hasNext());
        } finally {
            probe.releaseBlocks();
        }
        driverContext.finish();
        assertThat(
            driverContext.warnings(),
            equalTo(
                List.of(
                    "Line 1:1: evaluation of [lookup] failed, treating result as null. Only first 20 failures recorded.",
                    "Line 1:1: java.lang.IllegalArgumentException: LOOKUP JOIN encountered multi-value"
                )
            )
        );
    }

    @After
    public void checkBreaker() {
        blockFactory.ensureAllBlocksAreReleased();
        assertThat(breaker.getUsed(), is(0L));
    }
}
