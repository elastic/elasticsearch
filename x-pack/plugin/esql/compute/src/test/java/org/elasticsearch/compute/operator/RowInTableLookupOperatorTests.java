/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.TrackingCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.aggregation.table.RowInTableLookup;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.BlockTestUtils;
import org.elasticsearch.compute.test.OperatorTestCase;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.compute.test.TestDriverRunner;
import org.elasticsearch.compute.test.operator.blocksource.SequenceLongBlockSourceOperator;
import org.elasticsearch.compute.test.operator.blocksource.TupleLongLongBlockSourceOperator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.indices.breaker.AllCircuitBreakerStats;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.CircuitBreakerStats;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.matchesRegex;
import static org.hamcrest.Matchers.not;
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
        return new RowInTableLookupOperator.Factory(simpleKeys(), new int[] { 0 });
    }

    private static RowInTableLookupOperator.Key[] simpleKeys() {
        return new RowInTableLookupOperator.Key[] {
            new RowInTableLookupOperator.Key(
                "foo",
                TestBlockFactory.getNonBreakingInstance().newLongArrayVector(new long[] { 1, 7, 14, 20 }, 4).asBlock()
            ) };
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return matchesRegex("RowInTableLookup\\[keys=\\[\\{name=foo, type=LONG, positions=4, size=\\d+b}], mapping=\\[0]]");
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return matchesRegex("RowInTableLookup\\[unbuilt, keys=\\[foo], mapping=\\[0]]");
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
     * Operators created by one {@link RowInTableLookupOperator.Factory} share a single table, charged to the
     * drivers' common parent breaker once, and released when the last of them closes.
     */
    public void testFactorySharesOneTable() {
        BlockFactory blockFactory = blockFactory();
        CircuitBreaker breaker = blockFactory.breaker();
        Operator.OperatorFactory factory = simple();
        long before = breaker.getUsed();

        Operator first = factory.get(new DriverContext(blockFactory.bigArrays(), blockFactory, null));
        assertThat("the table is built on the first input", breaker.getUsed(), equalTo(before));
        lookupOneRow(first);
        long withTable = breaker.getUsed();
        assertThat(withTable, greaterThan(before));

        Operator second = factory.get(new DriverContext(blockFactory.bigArrays(), blockFactory, null));
        lookupOneRow(second);
        assertThat("the second operator reuses the table", breaker.getUsed(), equalTo(withTable));

        first.close();
        assertThat("the table stays while the second operator uses it", breaker.getUsed(), equalTo(withTable));
        second.close();
        assertThat("the last close releases the table", breaker.getUsed(), equalTo(before));

        // Once released, the next operator builds a fresh table rather than using the closed one.
        Operator third = factory.get(new DriverContext(blockFactory.bigArrays(), blockFactory, null));
        lookupOneRow(third);
        assertThat(breaker.getUsed(), equalTo(withTable));
        third.close();
        assertThat(breaker.getUsed(), equalTo(before));
    }

    /**
     * With {@link HashJoinTables} the table outlives the operators of the factory that built it, so the next factory
     * looking up the same key blocks starts with it built, and it is released only when the tables close.
     */
    public void testTablesKeepTableForNextFactory() {
        BlockFactory blockFactory = blockFactory();
        CircuitBreaker breaker = blockFactory.breaker();
        DriverContext driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);
        RowInTableLookupOperator.Key[] keys = simpleKeys();
        long before = breaker.getUsed();
        try (HashJoinTables tables = new HashJoinTables()) {
            try (Operator first = new RowInTableLookupOperator.Factory(keys, new int[] { 0 }, tables).get(driverContext)) {
                lookupOneRow(first);
            }
            long withTable = breaker.getUsed();
            assertThat("the table outlives the operators", withTable, greaterThan(before));

            try (Operator next = new RowInTableLookupOperator.Factory(keys, new int[] { 0 }, tables).get(driverContext)) {
                assertThat(next.toString(), not(containsString("unbuilt")));
                lookupOneRow(next);
                assertThat("the next factory reuses the table", breaker.getUsed(), equalTo(withTable));
            }
            assertThat(breaker.getUsed(), equalTo(withTable));
        }
        assertThat("closing the tables releases the table", breaker.getUsed(), equalTo(before));
    }

    /**
     * A build that failed for one factory is retried by the next rather than failing it the same way, as it would be
     * without {@link HashJoinTables}.
     */
    public void testTablesRetryFailedBuild() {
        TripOnceBreaker breaker = new TripOnceBreaker();
        BlockFactory blockFactory = trackingBlockFactory(breaker);
        DriverContext driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);
        RowInTableLookupOperator.Key[] keys = simpleKeys();
        try (HashJoinTables tables = new HashJoinTables()) {
            try (Operator first = new RowInTableLookupOperator.Factory(keys, new int[] { 0 }, tables).get(driverContext)) {
                expectThrows(CircuitBreakingException.class, () -> first.addInput(keysPage(1)));
            }
            try (Operator next = new RowInTableLookupOperator.Factory(keys, new int[] { 0 }, tables).get(driverContext)) {
                assertThat(next.toString(), containsString("unbuilt"));
                lookupOneRow(next);
            }
        }
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    /**
     * A driver whose input is empty never needs the table, so it finishes without building one.
     */
    public void testNoInputSkipsBuild() {
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker();
        BlockFactory blockFactory = trackingBlockFactory(breaker);
        try (Operator operator = simple().get(new DriverContext(blockFactory.bigArrays(), blockFactory, null))) {
            operator.finish();
            assertTrue(operator.isFinished());
        }
        assertThat(breaker.peak(), equalTo(0L));
    }

    /**
     * While one driver builds the shared table the others hold their input rather than block, so their upstream
     * operators keep running. They block once they hold {@link RowInTableLookupOperator#MAX_PENDING_BYTES}, and
     * emit what they held, in order, once the table is built.
     */
    public void testHoldsInputWhileAnotherDriverBuilds() {
        PausingBreaker breaker = new PausingBreaker(-1);
        BlockFactory blockFactory = trackingBlockFactory(breaker);
        Operator.OperatorFactory factory = simple();
        DriverContext driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);
        try (Operator builder = factory.get(driverContext); Operator waiter = factory.get(driverContext)) {
            Thread build = new Thread(() -> builder.addInput(keysPage(1)));
            build.start();
            safeAwait(breaker.paused);

            List<Page> input = new ArrayList<>();
            while (waiter.needsInput()) {
                Page page = keysPage(10_000);
                input.add(BlockTestUtils.deepCopyOf(page, TestBlockFactory.getNonBreakingInstance()));
                waiter.addInput(page);
                assertNull(waiter.getOutput());
                if (waiter.needsInput()) {
                    assertTrue("below the limit the driver keeps moving pages", waiter.isBlocked().listener().isDone());
                }
            }
            assertThat(input.size(), greaterThan(1));
            assertFalse("holding the limit, it waits for the build", waiter.isBlocked().listener().isDone());

            breaker.resume.countDown();
            safeJoin(build);
            assertTrue(waiter.isBlocked().listener().isDone());
            waiter.finish();
            List<Page> results = new ArrayList<>();
            while (waiter.isFinished() == false) {
                Page out = waiter.getOutput();
                if (out != null) {
                    results.add(out);
                }
            }
            try {
                assertSimpleOutput(input, results);
            } finally {
                Releasables.close(() -> results.forEach(Page::releaseBlocks), () -> {
                    Page out;
                    while ((out = builder.getOutput()) != null) {
                        out.releaseBlocks();
                    }
                });
            }
        }
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    /**
     * A failed build fails the drivers waiting on it too, rather than leaving them blocked, and they release the
     * input they held.
     */
    public void testBuildFailureFailsWaitingDrivers() {
        PausingBreaker breaker = new PausingBreaker(0);
        BlockFactory blockFactory = trackingBlockFactory(breaker);
        Operator.OperatorFactory factory = simple();
        DriverContext driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);
        try (Operator builder = factory.get(driverContext); Operator waiter = factory.get(driverContext)) {
            AtomicReference<Exception> builderFailure = new AtomicReference<>();
            Thread build = new Thread(
                () -> builderFailure.set(expectThrows(CircuitBreakingException.class, () -> { builder.addInput(keysPage(1)); }))
            );
            build.start();
            safeAwait(breaker.paused);

            waiter.addInput(keysPage(10));
            waiter.finish();
            assertFalse(waiter.isBlocked().listener().isDone());

            breaker.resume.countDown();
            safeJoin(build);
            assertTrue(waiter.isBlocked().listener().isDone());
            assertThat(expectThrows(CircuitBreakingException.class, waiter::getOutput), sameInstance(builderFailure.get()));
        }
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    /**
     * A vector-backed key is hashed in place. Copying it first would hold the copy for the whole build, so building
     * through the operator would peak higher than building straight from the key.
     */
    public void testBuildDoesNotCopyVectorKeys() {
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
     * Holds the first reservation, which is the start of a table build, until {@link #resume} is released, so a
     * test can act while the build is in progress.
     */
    private static class PausingBreaker extends TrackingCircuitBreaker {
        private final CountDownLatch paused = new CountDownLatch(1);
        private final CountDownLatch resume = new CountDownLatch(1);
        private final AtomicBoolean first = new AtomicBoolean(true);

        PausingBreaker(long limit) {
            super(limit);
        }

        @Override
        public void addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {
            if (first.compareAndSet(true, false)) {
                paused.countDown();
                safeAwait(resume);
            }
            super.addEstimateBytesAndMaybeBreak(bytes, label);
        }
    }

    /**
     * Trips the first reservation, which is the start of a table build, and allows the rest.
     */
    private static class TripOnceBreaker extends TrackingCircuitBreaker {
        private final AtomicBoolean first = new AtomicBoolean(true);

        @Override
        public void addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {
            if (first.compareAndSet(true, false)) {
                throw new CircuitBreakingException("tripped once", CircuitBreaker.Durability.TRANSIENT);
            }
            super.addEstimateBytesAndMaybeBreak(bytes, label);
        }
    }

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
