/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.TrackingCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.aggregation.table.RowInTableLookup;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LocalCircuitBreaker;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.OperatorTestCase;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.compute.test.TestDriverRunner;
import org.elasticsearch.compute.test.operator.blocksource.SequenceLongBlockSourceOperator;
import org.elasticsearch.compute.test.operator.blocksource.TupleLongLongBlockSourceOperator;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.indices.breaker.AllCircuitBreakerStats;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.CircuitBreakerStats;
import org.hamcrest.Matcher;

import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
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
            new RowInTableLookupOperator.Key[] { new RowInTableLookupOperator.Key("foo", key) }
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
                        RowInTableLookupOperator.buildLookup(parent, keys).close();
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
