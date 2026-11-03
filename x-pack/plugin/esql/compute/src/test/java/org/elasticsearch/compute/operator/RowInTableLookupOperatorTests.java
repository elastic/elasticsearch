/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.TrackingCircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.aggregation.table.RowInTableLookup;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LocalCircuitBreaker;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.BlockTestUtils;
import org.elasticsearch.compute.test.CannedSourceOperator;
import org.elasticsearch.compute.test.OperatorTestCase;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.compute.test.TestDriverFactory;
import org.elasticsearch.compute.test.TestDriverRunner;
import org.elasticsearch.compute.test.operator.blocksource.SequenceLongBlockSourceOperator;
import org.elasticsearch.compute.test.operator.blocksource.TupleLongLongBlockSourceOperator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.indices.breaker.AllCircuitBreakerStats;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.CircuitBreakerStats;
import org.elasticsearch.tasks.TaskCancelledException;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
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
     * The source stays unfinished for the driver's first iteration, so the driver asks the lookup for output before
     * any row could have reached it.
     */
    public void testNoInputSkipsBuild() {
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker();
        BlockFactory blockFactory = trackingBlockFactory(breaker);
        DriverContext driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);
        SourceOperator noRows = new SourceOperator() {
            private int calls;

            @Override
            public void finish() {}

            @Override
            public boolean isFinished() {
                return calls > 1;
            }

            @Override
            public Page getOutput() {
                calls++;
                return null;
            }

            @Override
            public void close() {}
        };
        List<Page> results = new ArrayList<>();
        try (
            Driver driver = TestDriverFactory.create(
                driverContext,
                noRows,
                List.of(simple().get(driverContext)),
                new PageConsumerOperator(results::add)
            )
        ) {
            new TestDriverRunner().run(driver);
        }
        assertThat(results, empty());
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

    public void testCancelWhileWaitingForBuild() {
        assertOthersWaitAfterStop((driver, sink) -> driver.cancel("test"), future -> {
            Exception e = expectThrows(Exception.class, () -> future.actionGet(TimeValue.timeValueSeconds(10)));
            assertThat(ExceptionsHelper.unwrapCause(e), instanceOf(TaskCancelledException.class));
        });
    }

    public void testFinishEarlyWhileWaitingForBuild() {
        assertOthersWaitAfterStop((driver, sink) -> {
            // An exchange sink is finished by the time it asks its driver to finish early.
            sink.finish();
            driver.finishEarly();
        }, future -> future.actionGet(TimeValue.timeValueSeconds(10)));
    }

    /**
     * A driver completes the listener it is blocked on when it is cancelled or finishes early. That must stop only that
     * driver: the operators of the other drivers keep waiting for the table, then use it.
     */
    private void assertOthersWaitAfterStop(BiConsumer<Driver, SinkOperator> stop, Consumer<PlainActionFuture<Void>> assertStopped) {
        PausingBreaker breaker = new PausingBreaker(-1);
        BlockFactory blockFactory = trackingBlockFactory(breaker);
        Operator.OperatorFactory factory = simple();
        DriverContext driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);
        try (Operator builder = factory.get(driverContext); Operator other = factory.get(driverContext)) {
            Thread build = new Thread(() -> builder.addInput(keysPage(1)));
            build.start();
            safeAwait(breaker.paused);

            List<Page> input = new ArrayList<>();
            List<Page> results = new ArrayList<>();
            try {
                DriverContext stoppedContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);
                List<Page> stoppedOutput = new ArrayList<>();
                SinkOperator sink = new PageConsumerOperator(stoppedOutput::add);
                Driver stopped = TestDriverFactory.create(
                    stoppedContext,
                    new CannedSourceOperator(List.of(keysPage(10)).iterator()),
                    List.of(factory.get(stoppedContext)),
                    sink
                );
                PlainActionFuture<Void> future = new PlainActionFuture<>();
                Driver.start(new ThreadContext(Settings.EMPTY), EsExecutors.DIRECT_EXECUTOR_SERVICE, stopped, 10_000, future);
                assertThat(stopped.status().status(), equalTo(DriverStatus.Status.ASYNC));

                Page page = keysPage(10);
                input.add(BlockTestUtils.deepCopyOf(page, TestBlockFactory.getNonBreakingInstance()));
                other.addInput(page);
                other.finish();

                stop.accept(stopped, sink);
                assertStopped.accept(future);
                assertThat(stoppedOutput, empty());
                assertFalse(other.isBlocked().listener().isDone());
                assertThat(other.toString(), containsString("unbuilt"));
            } finally {
                breaker.resume.countDown();
                safeJoin(build);
            }

            assertTrue(other.isBlocked().listener().isDone());
            while (other.isFinished() == false) {
                Page out = other.getOutput();
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

    public void testBlockHashResultsUseProbeFactory() {
        assertResultsUseProbeFactory(
            TestBlockFactory.getNonBreakingInstance().newLongArrayVector(new long[] { 1, 7, 14, 20 }, 4).asBlock(),
            f -> f.newLongArrayVector(new long[] { 7, 3 }, 2).asBlock()
        );
    }

    public void testAscendingSequenceResultsUseProbeFactory() {
        int min = randomFrom(0, 5);
        int probed = min + randomFrom(2, 9);
        assertResultsUseProbeFactory(
            TestBlockFactory.getNonBreakingInstance().newIntArrayVector(new int[] { min, min + 1, min + 2 }, 3).asBlock(),
            f -> f.newIntArrayVector(new int[] { min + 1, probed }, 2).asBlock()
        );
    }

    public void testEmptyTableResultsUseProbeFactory() {
        assertResultsUseProbeFactory(
            TestBlockFactory.getNonBreakingInstance().newLongArrayVector(new long[0], 0).asBlock(),
            f -> f.newLongArrayVector(new long[] { 7, 3 }, 2).asBlock()
        );
    }

    /**
     * The table is built with the parent factory because it outlives any one driver, but the results of each lookup
     * belong to the driver that probed and are charged to its local breaker.
     */
    private void assertResultsUseProbeFactory(Block keys, Function<BlockFactory, Block> probe) {
        BlockFactory parent = blockFactory();
        LocalCircuitBreaker local = new LocalCircuitBreaker(parent.breaker(), 0, 0);
        BlockFactory driverFactory = parent.newChildFactory(local);
        Operator.OperatorFactory factory = new RowInTableLookupOperator.Factory(
            new RowInTableLookupOperator.Key[] { new RowInTableLookupOperator.Key("foo", keys) },
            new int[] { 0 }
        );
        try (Operator operator = factory.get(new DriverContext(driverFactory.bigArrays(), driverFactory, null))) {
            operator.addInput(new Page(probe.apply(driverFactory)));
            Page out = operator.getOutput();
            try {
                assertThat(out.getBlock(1).blockFactory(), sameInstance(driverFactory));
            } finally {
                out.releaseBlocks();
            }
        } finally {
            local.close();
        }
    }

    /**
     * The driver that built the table can finish, closing its local breaker, while another driver still uses the
     * table. Bytes released to a closed local breaker never reach its parent, so the table must be charged to the
     * parent.
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
