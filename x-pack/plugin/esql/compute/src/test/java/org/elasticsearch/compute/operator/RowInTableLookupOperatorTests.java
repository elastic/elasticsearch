/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.TrackingCircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.aggregation.table.RowInTableLookup;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LocalCircuitBreaker;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.OperatorTestCase;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.compute.test.TestDriverRunner;
import org.elasticsearch.compute.test.TestWarningsSource;
import org.elasticsearch.compute.test.operator.blocksource.SequenceLongBlockSourceOperator;
import org.elasticsearch.compute.test.operator.blocksource.TupleLongLongBlockSourceOperator;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.indices.breaker.AllCircuitBreakerStats;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.CircuitBreakerStats;
import org.hamcrest.Matcher;

import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.LongStream;

import static org.elasticsearch.compute.data.BlockUtils.toJavaObject;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.matchesRegex;
import static org.hamcrest.Matchers.sameInstance;

public class RowInTableLookupOperatorTests extends OperatorTestCase {
    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        return new SequenceLongBlockSourceOperator(blockFactory, LongStream.range(0, size).map(l -> randomFrom(1, 7, 14, 20)));
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> results) {
        assertSimpleOutput(input, results, 0, 1);
    }

    private void assertSimpleOutput(List<Page> input, List<Page> results, int keyChannel, int outputChannel) {
        int count = input.stream().mapToInt(Page::getPositionCount).sum();
        assertThat(results.stream().mapToInt(Page::getPositionCount).sum(), equalTo(count));
        int keysIdx = 0;
        int ordsIdx = 0;
        LongBlock keys = null;
        int keysOffset = 0;
        IntBlock ords = null;
        int ordsOffset = 0;
        int p = 0;
        while (p < count) {
            if (keys == null) {
                keys = input.get(keysIdx++).getBlock(keyChannel);
            }
            if (ords == null) {
                ords = results.get(ordsIdx++).getBlock(outputChannel);
            }
            int valueCount = keys.getValueCount(p - keysOffset);
            assertThat(ords.getValueCount(p - ordsOffset), equalTo(valueCount));
            int keysStart = keys.getFirstValueIndex(p - keysOffset);
            int ordsStart = ords.getFirstValueIndex(p - ordsOffset);
            for (int k = keysStart, l = ordsStart; k < keysStart + valueCount; k++, l++) {
                assertThat(ords.getInt(l), equalTo(switch ((int) keys.getLong(k)) {
                    case 1 -> 0;
                    case 7 -> 1;
                    case 14 -> 2;
                    case 20 -> 3;
                    default -> null;
                }));
            }
            p++;
            if (p - keysOffset == keys.getPositionCount()) {
                keysOffset += keys.getPositionCount();
                keys = null;
            }
            if (p - ordsOffset == ords.getPositionCount()) {
                ordsOffset += ords.getPositionCount();
                ords = null;
            }
        }
    }

    @Override
    protected Operator.OperatorFactory simple(SimpleOptions options) {
        return new RowInTableLookupOperator.Factory(
            new RowInTableLookupOperator.Key[] {
                new RowInTableLookupOperator.Key(
                    "foo",
                    TestBlockFactory.getNonBreakingInstance().newLongArrayVector(new long[] { 1, 7, 14, 20 }, 4).asBlock()
                ) },
            new int[] { 0 }
        );
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return matchesRegex("RowInTableLookup\\[keys=\\[\\{name=foo, type=LONG, positions=4, size=\\d+b}], mapping=\\[0]]");
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return matchesRegex(
            "RowInTableLookup\\[PackedValuesBlockHash\\{groups=\\[0:LONG], entries=4, size=\\d+(\\.\\d*k)?b}, keys=\\[foo], mapping=\\[0]]"
        );
    }

    public void testSelectBlocks() {
        var runner = new TestDriverRunner().builder(driverContext()).collectDeepCopy();
        runner.input(
            new TupleLongLongBlockSourceOperator(
                runner.blockFactory(),
                LongStream.range(0, 1000).mapToObj(l -> Tuple.tuple(randomLong(), randomFrom(1L, 7L, 14L, 20L)))
            )
        );
        List<Page> results = runner.run(
            new RowInTableLookupOperator.Factory(
                new RowInTableLookupOperator.Key[] {
                    new RowInTableLookupOperator.Key(
                        "foo",
                        TestBlockFactory.getNonBreakingInstance().newLongArrayVector(new long[] { 1, 7, 14, 20 }, 4).asBlock()
                    ) },
                new int[] { 1 }
            )
        );
        assertSimpleOutput(runner.deepCopy(), results, 1, 2);
    }

    /**
     * The table is built with the parent factory because it outlives any one driver, but the results of each lookup
     * belong to the driver that probed and are charged to its local breaker.
     */
    public void testLookupUsesDriverFactory() {
        BlockFactory parent = blockFactory();
        LocalCircuitBreaker local = new LocalCircuitBreaker(parent.breaker(), 0, 0);
        BlockFactory driverFactory = parent.newChildFactory(local);
        // Pages from ROW and other local sources are built on a non-breaking factory.
        BlockFactory localSourceFactory = TestBlockFactory.getNonBreakingInstance();
        try (Operator op = simple().get(new DriverContext(driverFactory.bigArrays(), driverFactory, null))) {
            op.addInput(new Page(localSourceFactory.newLongArrayVector(new long[] { 7, 3 }, 2).asBlock()));
            Page output = op.getOutput();
            try {
                assertThat(output.getBlock(1).blockFactory(), sameInstance(driverFactory));
            } finally {
                output.releaseBlocks();
            }
        } finally {
            local.close();
        }
    }

    public void testFactorySharesOneTable() {
        BlockFactory blockFactory = blockFactory();
        CircuitBreaker breaker = blockFactory.breaker();
        Operator.OperatorFactory factory = simple();
        long before = breaker.getUsed();

        Operator first = factory.get(new DriverContext(blockFactory.bigArrays(), blockFactory, null));
        long withTable = breaker.getUsed();
        assertThat("the first operator builds the table", withTable, greaterThan(before));

        Operator second = factory.get(new DriverContext(blockFactory.bigArrays(), blockFactory, null));
        assertThat("the second operator reuses the table", breaker.getUsed(), equalTo(withTable));
        lookupOneRow(first);
        lookupOneRow(second);
        assertThat(breaker.getUsed(), equalTo(withTable));

        first.close();
        assertThat("the table stays while the second operator uses it", breaker.getUsed(), equalTo(withTable));
        second.close();
        assertThat("the last close releases the table", breaker.getUsed(), equalTo(before));

        // Once released, the next operator builds a fresh table rather than using the closed one.
        Operator third = factory.get(new DriverContext(blockFactory.bigArrays(), blockFactory, null));
        assertThat(breaker.getUsed(), equalTo(withTable));
        lookupOneRow(third);
        third.close();
        assertThat(breaker.getUsed(), equalTo(before));
    }

    public void testRepeatedKeysShareTableAndKeepWarningsLocal() {
        BlockFactory parent = blockFactory();
        CircuitBreaker breaker = parent.breaker();
        try (
            LongBlock key = parent.newLongArrayVector(new long[] { 7, 9, 7 }, 3).asBlock();
            LocalCircuitBreaker firstBreaker = new LocalCircuitBreaker(breaker, 0, 0);
            LocalCircuitBreaker secondBreaker = new LocalCircuitBreaker(breaker, 0, 0)
        ) {
            BlockFactory firstFactory = parent.newChildFactory(firstBreaker);
            BlockFactory secondFactory = parent.newChildFactory(secondBreaker);
            DriverContext firstContext = new DriverContext(firstFactory.bigArrays(), firstFactory, null);
            DriverContext secondContext = new DriverContext(secondFactory.bigArrays(), secondFactory, null);
            var factory = new RowInTableLookupOperator.Factory(
                new RowInTableLookupOperator.Key[] { new RowInTableLookupOperator.Key("foo", key) },
                new int[] { 0 },
                true,
                new TestWarningsSource("join")
            );
            long before = breaker.getUsed();
            try (Operator first = factory.get(firstContext)) {
                long withTable = breaker.getUsed();
                assertThat(withTable, greaterThan(before));
                try (Operator second = factory.get(secondContext)) {
                    assertThat(breaker.getUsed(), equalTo(withTable));

                    try (LongBlock.Builder builder = firstFactory.newLongBlockBuilder(4)) {
                        builder.appendLong(7).appendNull();
                        builder.beginPositionEntry().appendLong(7).appendLong(9).endPositionEntry();
                        builder.appendLong(9);
                        first.addInput(new Page(builder.build()));
                    }
                    try (Page output = first.getOutput()) {
                        IntBlock matches = output.getBlock(1);
                        assertThat(matches.blockFactory(), sameInstance(firstFactory));
                        assertThat(toJavaObject(matches, 0), equalTo(List.of(0, 2)));
                        assertNull(toJavaObject(matches, 1));
                        assertNull(toJavaObject(matches, 2));
                        assertThat(toJavaObject(matches, 3), equalTo(1));
                    }

                    second.addInput(new Page(secondFactory.newConstantLongBlockWith(7, 1)));
                    try (Page output = second.getOutput()) {
                        IntBlock matches = output.getBlock(1);
                        assertThat(matches.blockFactory(), sameInstance(secondFactory));
                        assertThat(toJavaObject(matches, 0), equalTo(List.of(0, 2)));
                    }
                }
            }
            assertThat(breaker.getUsed(), equalTo(before));
            firstContext.finish();
            secondContext.finish();
            assertThat(
                firstContext.warnings(),
                contains(
                    "Line 1:1: evaluation of [join] failed, treating result as null. Only first 20 failures recorded.",
                    "Line 1:1: java.lang.IllegalArgumentException: JOIN encountered multi-value"
                )
            );
            assertTrue(secondContext.warnings().isEmpty());
        }
    }

    /**
     * A multivalued probe key misses, even when its values repeat, and the lookup doesn't copy the rest of the key
     * column to null it out. Lives here rather than in {@code RowInTableLookupTests} for {@link #trackingBlockFactory},
     * which also charges the {@link BigArrays} that hold a copied column's bytes.
     */
    public void testRepeatedLookupMissesMultivaluedProbeKeysWithoutCopyingThem() {
        int positions = 128;
        BytesRef large = new BytesRef("x".repeat(4096));
        BlockFactory blockFactory = blockFactory();
        TrackingCircuitBreaker probeBreaker = new TrackingCircuitBreaker();
        BlockFactory probeFactory = trackingBlockFactory(probeBreaker);
        try (
            BytesRefBlock key = bytes(blockFactory, "a", "b", "a");
            RowInTableLookup lookup = RowInTableLookup.build(blockFactory, new Block[] { key }, true);
            BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(positions)
        ) {
            builder.beginPositionEntry().appendBytesRef(new BytesRef("a")).appendBytesRef(new BytesRef("a")).endPositionEntry();
            builder.appendBytesRef(new BytesRef("a"));
            for (int p = 2; p < positions; p++) {
                builder.appendBytesRef(large);
            }
            try (
                Page probe = new Page(builder.build());
                ReleasableIterator<IntBlock> blocks = lookup.lookup(probe, probeFactory, ByteSizeValue.ofKb(8));
                IntBlock matches = blocks.next()
            ) {
                assertFalse(blocks.hasNext());
                assertThat(matches.getPositionCount(), equalTo(positions));
                assertNull(toJavaObject(matches, 0));
                assertThat(toJavaObject(matches, 1), equalTo(List.of(0, 2)));
                for (int p = 2; p < positions; p++) {
                    assertNull(toJavaObject(matches, p));
                }
            }
        }
        assertThat(probeBreaker.getUsed(), equalTo(0L));
        // Copying the key column would reserve more than 500kb.
        assertThat(probeBreaker.peak(), lessThan(ByteSizeValue.ofKb(64).getBytes()));
    }

    /**
     * A small repeated-key table doesn't hold a hash batch buffer sized from a byte budget.
     */
    public void testSmallRepeatedTableRetainsLittle() {
        BlockFactory blockFactory = blockFactory();
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker();
        try (
            IntBlock key = blockFactory.newConstantIntBlockWith(7, 2);
            RowInTableLookup lookup = RowInTableLookup.build(trackingBlockFactory(breaker), new Block[] { key }, true)
        ) {
            // A buffer of DEFAULT_MAX_BLOCK_PRIMITIVE_ARRAY_SIZE ints would retain over 2mb.
            assertThat(breaker.getUsed(), lessThan(ByteSizeValue.ofKb(64).getBytes()));
        }
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    /**
     * An empty build still warns on a multivalued probe key, like a non-empty repeated-key table does.
     */
    public void testEmptyRepeatedTableWarnsOnMultivaluedKeys() {
        BlockFactory blockFactory = blockFactory();
        DriverContext context = new DriverContext(blockFactory.bigArrays(), blockFactory, null);
        try (LongBlock key = blockFactory.newLongArrayVector(new long[0], 0).asBlock()) {
            var factory = new RowInTableLookupOperator.Factory(
                new RowInTableLookupOperator.Key[] { new RowInTableLookupOperator.Key("foo", key) },
                new int[] { 0 },
                true,
                new TestWarningsSource("join")
            );
            try (Operator operator = factory.get(context)) {
                try (LongBlock.Builder builder = blockFactory.newLongBlockBuilder(2)) {
                    builder.appendLong(7);
                    builder.beginPositionEntry().appendLong(7).appendLong(9).endPositionEntry();
                    operator.addInput(new Page(builder.build()));
                }
                try (Page output = operator.getOutput()) {
                    IntBlock matches = output.getBlock(1);
                    assertTrue(matches.isNull(0));
                    assertTrue(matches.isNull(1));
                }
            }
        }
        context.finish();
        assertThat(
            context.warnings(),
            contains(
                "Line 1:1: evaluation of [join] failed, treating result as null. Only first 20 failures recorded.",
                "Line 1:1: java.lang.IllegalArgumentException: JOIN encountered multi-value"
            )
        );
    }

    /**
     * A build row with a multivalued key matches nothing. A probe key equal to one of its values gets a warning, as a probe
     * key that matches a multivalued document in a {@code LOOKUP JOIN} lookup index does, and other probe keys don't. A
     * build row with a null key matches nothing without a warning.
     */
    public void testRepeatedTableWarnsOnlyWhenProbeKeyHitsMultivaluedBuildKey() {
        BlockFactory blockFactory = blockFactory();
        DriverContext quiet = new DriverContext(blockFactory.bigArrays(), blockFactory, null);
        DriverContext warned = new DriverContext(blockFactory.bigArrays(), blockFactory, null);
        LongBlock key;
        try (LongBlock.Builder builder = blockFactory.newLongBlockBuilder(4)) {
            builder.appendLong(7);
            builder.beginPositionEntry().appendLong(20).appendLong(30).endPositionEntry();
            builder.appendNull();
            builder.appendLong(20);
            key = builder.build();
        }
        try (key) {
            var factory = new RowInTableLookupOperator.Factory(
                new RowInTableLookupOperator.Key[] { new RowInTableLookupOperator.Key("foo", key) },
                new int[] { 0 },
                true,
                new TestWarningsSource("join")
            );
            try (Operator operator = factory.get(quiet)) {
                operator.addInput(new Page(blockFactory.newLongArrayVector(new long[] { 7, 9 }, 2).asBlock()));
                try (Page output = operator.getOutput()) {
                    IntBlock matches = output.getBlock(1);
                    assertThat(toJavaObject(matches, 0), equalTo(0));
                    assertNull(toJavaObject(matches, 1));
                }
            }
            try (Operator operator = factory.get(warned)) {
                operator.addInput(new Page(blockFactory.newLongArrayVector(new long[] { 20, 30 }, 2).asBlock()));
                try (Page output = operator.getOutput()) {
                    IntBlock matches = output.getBlock(1);
                    assertThat(toJavaObject(matches, 0), equalTo(3));
                    assertNull(toJavaObject(matches, 1));
                }
            }
        }
        quiet.finish();
        warned.finish();
        assertTrue(quiet.warnings().isEmpty());
        assertThat(
            warned.warnings(),
            contains(
                "Line 1:1: evaluation of [join] failed, treating result as null. Only first 20 failures recorded.",
                "Line 1:1: java.lang.IllegalArgumentException: JOIN encountered multi-value"
            )
        );
    }

    /**
     * The driver that built the table can finish, closing its local breaker, while another driver still uses the
     * table. A closed local breaker keeps up to its maximum over-reservation of the bytes released to it, so the
     * table must be charged to the parent. These local breakers never pass released bytes on, which makes a wrong
     * charge visible.
     */
    public void testTableOutlivesBuilderLocalBreaker() {
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker();
        BlockFactory blockFactory = trackingBlockFactory(breaker);
        Operator.OperatorFactory factory = simple();
        LocalCircuitBreaker builderBreaker = new LocalCircuitBreaker(breaker, 0, Long.MAX_VALUE);
        LocalCircuitBreaker otherBreaker = new LocalCircuitBreaker(breaker, 0, Long.MAX_VALUE);
        BlockFactory builderFactory = blockFactory.newChildFactory(builderBreaker);
        BlockFactory otherFactory = blockFactory.newChildFactory(otherBreaker);
        Operator builder = factory.get(new DriverContext(builderFactory.bigArrays(), builderFactory, null));
        Operator other = factory.get(new DriverContext(otherFactory.bigArrays(), otherFactory, null));
        lookupOneRow(builder);
        builder.close();
        builderBreaker.close();
        lookupOneRow(other);
        other.close();
        otherBreaker.close();
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    /**
     * Keys are hashed in place. Copying them first would hold the copy for the whole build, so building through the
     * operator would peak higher than building straight from the key.
     */
    public void testBuildDoesNotCopyKeys() {
        int size = between(1_000, 10_000);
        LongBlock key = TestBlockFactory.getNonBreakingInstance()
            .newLongArrayVector(LongStream.range(0, size).map(i -> i * 3).toArray(), size)
            .asBlock();

        TrackingCircuitBreaker direct = new TrackingCircuitBreaker();
        RowInTableLookup.build(trackingBlockFactory(direct), new Block[] { key }).close();
        assertThat(direct.getUsed(), equalTo(0L));

        TrackingCircuitBreaker viaOperator = new TrackingCircuitBreaker();
        RowInTableLookupOperator.buildLookup(
            trackingBlockFactory(viaOperator),
            new RowInTableLookupOperator.Key[] { new RowInTableLookupOperator.Key("foo", key) },
            false
        ).close();
        assertThat(viaOperator.peak(), equalTo(direct.peak()));
        assertThat(viaOperator.getUsed(), equalTo(0L));
    }

    /**
     * Plans planned at the same time on one node can share a key block, and building may incRef and decRef it. Once
     * the block is promoted with {@link Block#allowPassingToDifferentDriver}, concurrent builds keep its ref count
     * balanced. A key with a null is not a vector, so it takes the direct encoder path that changes the ref count.
     */
    public void testConcurrentBuildsFromSharedKey() throws Exception {
        LongBlock key;
        try (LongBlock.Builder builder = blockFactory().newLongBlockBuilder(4)) {
            key = builder.appendLong(1).appendLong(7).appendNull().appendLong(20).build();
        }
        assertNull(key.asVector());
        key.allowPassingToDifferentDriver();
        RowInTableLookupOperator.Key[] keys = new RowInTableLookupOperator.Key[] { new RowInTableLookupOperator.Key("foo", key) };
        BlockFactory parent = blockFactory().parent();
        int threads = between(2, 4);
        CyclicBarrier barrier = new CyclicBarrier(threads);
        Thread[] builders = new Thread[threads];
        AtomicReference<Exception> failure = new AtomicReference<>();
        for (int t = 0; t < threads; t++) {
            builders[t] = new Thread(() -> {
                try {
                    barrier.await();
                    for (int i = 0; i < 100; i++) {
                        RowInTableLookupOperator.buildLookup(parent, keys, false).close();
                    }
                } catch (Exception e) {
                    failure.compareAndSet(null, e);
                }
            });
            builders[t].start();
        }
        for (Thread builder : builders) {
            builder.join();
        }
        assertNull(failure.get());
        assertTrue(key.hasReferences());
        key.close();
        assertTrue(key.isReleased());
    }

    private Page keysPage(int positions) {
        return new Page(
            blockFactory().newLongArrayVector(LongStream.range(0, positions).map(l -> randomFrom(1, 7, 14, 20)).toArray(), positions)
                .asBlock()
        );
    }

    private void lookupOneRow(Operator operator) {
        operator.addInput(keysPage(1));
        operator.getOutput().releaseBlocks();
    }

    private static BytesRefBlock bytes(BlockFactory blockFactory, String... values) {
        try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(values.length)) {
            for (String value : values) {
                builder.appendBytesRef(new BytesRef(value));
            }
            return builder.build();
        }
    }

    /**
     * A block factory whose blocks and {@link BigArrays} both charge {@code breaker}, so the tests can check every
     * reservation of a table build.
     */
    private static BlockFactory trackingBlockFactory(TrackingCircuitBreaker breaker) {
        CircuitBreakerService service = new CircuitBreakerService() {
            @Override
            public CircuitBreaker getBreaker(String name) {
                return breaker;
            }

            @Override
            public AllCircuitBreakerStats stats() {
                return null;
            }

            @Override
            public CircuitBreakerStats stats(String name) {
                return null;
            }
        };
        return new BlockFactory(breaker, new BigArrays(null, service, CircuitBreaker.REQUEST).withCircuitBreaking());
    }
}
