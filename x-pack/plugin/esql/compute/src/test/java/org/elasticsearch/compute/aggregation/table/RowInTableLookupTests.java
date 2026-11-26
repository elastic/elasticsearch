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
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.MockBlockFactory;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.stream.IntStream;

import static org.elasticsearch.compute.data.BlockUtils.toJavaObject;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
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

    /**
     * A key repeated in the build table returns every build row that has it, and a key repeated in the probe gets the same
     * matches each time.
     */
    public void testRepeatedCompositeKeysReturnEveryMatchingRow() {
        assertRepeatedKeyLookup(
            List.of(
                row(1, "a"), // 0
                row(1, "b"), // 1
                row(2, "a"), // 2
                row(1, "a"), // 3
                row(2, "a")  // 4
            ),
            probe(1, "a").matches(0, 3),
            probe(1, "b").matches(1),
            probe(2, "a").matches(2, 4),
            probe(2, "b").matchesNothing(),
            probe(1, "a").matches(0, 3)
        );
    }

    public void testConcurrentRepeatedKeyLookups() {
        int threads = between(2, 4);
        int positions = 64;
        int[] keys = IntStream.range(0, threads * 2).map(p -> p % threads).toArray();
        List<MockBlockFactory> probeFactories = IntStream.range(0, threads).mapToObj(t -> newProbeFactory()).toList();
        Page[] probes = new Page[threads];
        try (
            IntBlock buildKeys = blockFactory.newIntArrayVector(keys, keys.length).asBlock();
            RowInTableLookup lookup = RowInTableLookup.build(blockFactory, new Block[] { buildKeys }, true)
        ) {
            long tableBytes = breaker.getUsed();
            for (int t = 0; t < threads; t++) {
                int hit = t;
                int[] probeKeys = IntStream.range(0, positions).map(p -> p % 2 == 0 ? hit : -1).toArray();
                probes[t] = new Page(probeFactories.get(t).newIntArrayVector(probeKeys, positions).asBlock());
            }

            var probesReady = new CyclicBarrier(threads);
            startInParallel(threads, t -> {
                MockBlockFactory probeFactory = probeFactories.get(t);
                List<Integer> expectedMatches = List.of(t, t + threads);
                try (ReleasableIterator<IntBlock> blocks = lookup.lookup(probes[t], probeFactory, ByteSizeValue.ofKb(8))) {
                    safeAwait(probesReady);
                    try (IntBlock matches = blocks.next()) {
                        assertThat(matches.blockFactory(), sameInstance(probeFactory));
                        assertThat(matches.getPositionCount(), equalTo(positions));
                        for (int p = 0; p < positions; p += 2) {
                            assertThat(toJavaObject(matches, p), equalTo(expectedMatches));
                            assertTrue(matches.isNull(p + 1));
                        }
                        assertThat(breaker.getUsed(), equalTo(tableBytes));
                    }
                    assertFalse(blocks.hasNext());
                }
            });
        } finally {
            Releasables.close(probes);
        }
        for (MockBlockFactory probeFactory : probeFactories) {
            probeFactory.ensureAllBlocksAreReleased();
            assertThat(probeFactory.breaker().getUsed(), is(0L));
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

    /**
     * Fills each result block up to the target, rather than reserving room for every remaining probe row and splitting early.
     */
    public void testRepeatedLookupFillsResultsToTarget() {
        int probeRows = 4096;
        ByteSizeValue target = ByteSizeValue.ofKb(8);
        // Every probe row matches both build rows.
        int rowsPerBlock = Math.toIntExact(target.getBytes() / (2 * Integer.BYTES));
        List<Integer> blockSizes = new ArrayList<>();
        try (
            IntBlock key = blockFactory.newConstantIntBlockWith(7, 2);
            RowInTableLookup lookup = RowInTableLookup.build(blockFactory, new Block[] { key }, true);
            Page probe = new Page(blockFactory.newConstantIntBlockWith(7, probeRows));
            ReleasableIterator<IntBlock> blocks = lookup.lookup(probe, blockFactory, target)
        ) {
            while (blocks.hasNext()) {
                try (IntBlock matches = blocks.next()) {
                    for (int p = 0; p < matches.getPositionCount(); p++) {
                        assertThat(toJavaObject(matches, p), equalTo(List.of(0, 1)));
                    }
                    blockSizes.add(matches.getPositionCount());
                }
            }
        }
        assertThat(blockSizes.getFirst(), equalTo(rowsPerBlock));
        assertThat(blockSizes, everyItem(lessThanOrEqualTo(rowsPerBlock)));
        assertThat(blockSizes.stream().mapToInt(Integer::intValue).sum(), equalTo(probeRows));
    }

    /**
     * A build row with a null or multivalued key matches nothing, as in {@code LOOKUP JOIN}: a probe row with the same nulls
     * misses, and a multivalued key matches none of its values.
     */
    public void testRepeatedLookupSkipsNullAndMultivaluedBuildKeys() {
        assertRepeatedKeyLookup(
            List.of(
                row(1, "a"),                  // 0
                row(1, null),                 // 1
                row(null, "a"),               // 2
                row(1, "a"),                  // 3
                row(null, null),              // 4
                row(List.of(1, 2), "a"),      // 5
                row(2, "a"),                  // 6
                row(3, List.of("a", "a")),    // 7
                row(4, "a")                   // 8
            ),
            probe(1, "a").matches(0, 3),
            probe(2, "a").matches(6),
            probe(3, "a").matchesNothing(),
            probe(4, "a").matches(8),
            probe(1, null).matchesNothing(),
            probe(null, "a").matchesNothing(),
            probe(null, null).matchesNothing()
        );
    }

    /**
     * A table whose every build row has a null or multivalued key matches nothing.
     */
    public void testRepeatedLookupWithNoMatchableBuildRows() {
        assertRepeatedKeyLookup(
            List.of(row((Object) null), row(List.of(1, 2))),
            probe(1).matchesNothing(),
            probe(2).matchesNothing(),
            probe((Object) null).matchesNothing()
        );
    }

    /**
     * Builds a repeated-key lookup from {@code buildRows}, probes it with every row in {@code probes} as one page, and
     * checks that each probe row gets exactly the build rows it expects.
     */
    private void assertRepeatedKeyLookup(List<List<Object>> buildRows, ProbeRow... probes) {
        Block[] buildKeys = BlockUtils.fromList(blockFactory, buildRows);
        try (
            RowInTableLookup lookup = RowInTableLookup.build(blockFactory, buildKeys, true);
            Page probe = new Page(BlockUtils.fromList(blockFactory, Arrays.stream(probes).map(ProbeRow::key).toList()));
            ReleasableIterator<IntBlock> blocks = lookup.lookup(probe, blockFactory, ByteSizeValue.ofKb(1));
            IntBlock matches = blocks.next()
        ) {
            List<List<Integer>> actual = new ArrayList<>();
            for (int p = 0; p < matches.getPositionCount(); p++) {
                int first = matches.getFirstValueIndex(p);
                List<Integer> buildRowsForProbe = new ArrayList<>();
                for (int i = first; i < first + matches.getValueCount(p); i++) {
                    buildRowsForProbe.add(matches.getInt(i));
                }
                actual.add(buildRowsForProbe);
            }
            assertThat(actual, equalTo(Arrays.stream(probes).map(ProbeRow::expectedMatches).toList()));
            assertFalse(blocks.hasNext());
        } finally {
            Releasables.close(buildKeys);
        }
    }

    /** One build or probe row's key values, in key column order. */
    private static List<Object> row(Object... keys) {
        return Arrays.asList(keys);
    }

    private static ProbeRow probe(Object... keys) {
        return new ProbeRow(row(keys), List.of());
    }

    /** A probe row's key and the build rows it should match, in ascending order. */
    private record ProbeRow(List<Object> key, List<Integer> expectedMatches) {
        ProbeRow matches(int... buildRows) {
            return new ProbeRow(key, Arrays.stream(buildRows).boxed().toList());
        }

        ProbeRow matchesNothing() {
            return new ProbeRow(key, List.of());
        }
    }

    public void testEmptyTable() {
        MockBlockFactory probeFactory = newProbeFactory();
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

    private static MockBlockFactory newProbeFactory() {
        return new MockBlockFactory(
            BlockFactory.builder(
                new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, newLimitedBreakerService(ByteSizeValue.ofGb(1)))
            )
        );
    }

    @After
    public void checkBreaker() {
        blockFactory.ensureAllBlocksAreReleased();
        assertThat(breaker.getUsed(), is(0L));
    }
}
