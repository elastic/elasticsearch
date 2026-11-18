/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.table;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.MockBlockFactory;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;

import java.util.List;
import java.util.stream.IntStream;

import static org.elasticsearch.compute.data.BlockUtils.toJavaObject;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

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

    public void testRepeatedCompositeKeysReturnEveryMatchingRow() {
        try (
            IntBlock firstKey = blockFactory.newIntArrayVector(new int[] { 1, 1, 2, 1, 2 }, 5).asBlock();
            BytesRefBlock secondKey = bytes("a", "b", "a", "a", "a");
            RowInTableLookup lookup = RowInTableLookup.build(blockFactory, new Block[] { firstKey, secondKey }, true);
            Page probe = new Page(blockFactory.newIntArrayVector(new int[] { 1, 1, 2, 2, 1 }, 5).asBlock(), bytes("a", "b", "a", "b", "a"));
            ReleasableIterator<IntBlock> blocks = lookup.lookup(probe, blockFactory, ByteSizeValue.ofKb(1));
            IntBlock matches = blocks.next()
        ) {
            assertThat(matches.getPositionCount(), equalTo(5));
            assertThat(toJavaObject(matches, 0), equalTo(List.of(0, 3)));
            assertThat(toJavaObject(matches, 1), equalTo(1));
            assertThat(toJavaObject(matches, 2), equalTo(List.of(2, 4)));
            assertNull(toJavaObject(matches, 3));
            assertThat(toJavaObject(matches, 4), equalTo(List.of(0, 3)));
            assertFalse(blocks.hasNext());
        }
    }

    public void testRepeatedLookupSplitsResultsBetweenProbeRows() {
        int buildRows = 300;
        int probeRows = 4;
        List<Integer> expectedMatches = IntStream.range(0, buildRows).boxed().toList();
        try (
            IntBlock key = blockFactory.newConstantIntBlockWith(7, buildRows);
            RowInTableLookup lookup = RowInTableLookup.build(blockFactory, new Block[] { key }, true);
            Page probe = new Page(blockFactory.newConstantIntBlockWith(7, probeRows));
            ReleasableIterator<IntBlock> blocks = lookup.lookup(probe, blockFactory, ByteSizeValue.ofKb(1))
        ) {
            for (int p = 0; p < probeRows; p++) {
                assertTrue(blocks.hasNext());
                try (IntBlock matches = blocks.next()) {
                    assertThat(matches.getPositionCount(), equalTo(1));
                    assertThat(toJavaObject(matches, 0), equalTo(expectedMatches));
                }
            }
            assertFalse(blocks.hasNext());
        }
    }

    public void testRepeatedLookupRejectsNullBuildKeys() {
        try (IntBlock.Builder builder = blockFactory.newIntBlockBuilder(2)) {
            builder.appendInt(1).appendNull();
            try (IntBlock key = builder.build()) {
                var e = expectThrows(IllegalArgumentException.class, () -> RowInTableLookup.build(blockFactory, new Block[] { key }, true));
                assertThat(e.getMessage(), equalTo("repeated-key lookup requires non-null build keys"));
            }
        }
    }

    private BytesRefBlock bytes(String... keys) {
        try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(keys.length)) {
            for (String key : keys) {
                builder.appendBytesRef(new BytesRef(key));
            }
            return builder.build();
        }
    }

    public void testEmptyTable() {
        MockBlockFactory probeFactory = new MockBlockFactory(
            BlockFactory.builder(
                new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, newLimitedBreakerService(ByteSizeValue.ofGb(1)))
            )
        );
        try (IntBlock.Builder builder = blockFactory.newIntBlockBuilder(0)) {
            try (Block keys = builder.build(); RowInTableLookup lookup = RowInTableLookup.build(blockFactory, new Block[] { keys })) {
                assertThat(lookup, instanceOf(EmptyRowInTableLookup.class));
                int positions = between(1, 100);
                Page page = new Page(probeFactory.newConstantIntBlockWith(randomInt(), positions));
                try (ReleasableIterator<IntBlock> rows = lookup.lookup(page, probeFactory, ByteSizeValue.ofKb(1))) {
                    try (IntBlock r = rows.next()) {
                        assertThat(r.getPositionCount(), equalTo(positions));
                        assertThat(r.areAllValuesNull(), is(true));
                        assertThat(r.blockFactory(), sameInstance(probeFactory));
                    }
                    assertThat(rows.hasNext(), is(false));
                } finally {
                    page.releaseBlocks();
                }
            }
        }
        probeFactory.ensureAllBlocksAreReleased();
    }

    @After
    public void checkBreaker() {
        blockFactory.ensureAllBlocksAreReleased();
        assertThat(breaker.getUsed(), is(0L));
    }
}
