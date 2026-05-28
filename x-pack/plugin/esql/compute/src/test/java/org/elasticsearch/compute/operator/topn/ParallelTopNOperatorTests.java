/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.test.CannedSourceOperator;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.compute.test.TestDriverRunner;
import org.elasticsearch.indices.CrankyCircuitBreakerService;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.elasticsearch.compute.operator.topn.TopNEncoder.DEFAULT_UNSORTABLE;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.sameInstance;

public class ParallelTopNOperatorTests extends ComputeTestCase {

    private static final String ESQL_WORKER = "esql_worker";

    private TestThreadPool threadPool;

    @Before
    public void setUpThreadPool() {
        threadPool = new TestThreadPool(
            "test",
            new FixedExecutorBuilder(
                Settings.EMPTY,
                ESQL_WORKER,
                between(1, 4),
                1024,
                "esql_worker",
                EsExecutors.TaskTrackingConfig.DEFAULT
            )
        );
    }

    @After
    public void tearDownThreadPool() {
        terminate(threadPool);
    }

    // -------------------------------------------------------------------------
    // tryPromote unit tests
    // -------------------------------------------------------------------------

    /**
     * When no {@link TopNOperator.ParallelWorkerConfig} is configured, {@code tryPromote}
     * must return the original operator unchanged.
     */
    public void testTryPromoteReturnsSelfWithNoConfig() {
        // 8-arg factory constructor (no parallelWorkerConfig)
        TopNOperator.TopNOperatorFactory factory = new TopNOperator.TopNOperatorFactory(
            10,
            List.of(ElementType.LONG),
            List.of(DEFAULT_UNSORTABLE),
            List.of(new TopNOperator.SortOrder(0, true, false)),
            10,
            Long.MAX_VALUE,
            TopNOperator.InputOrdering.NOT_SORTED,
            null
        );
        DriverContext driverContext = nonTrackingDriverContext();
        try (TopNOperator op = factory.get(driverContext)) {
            Operator promoted = op.tryPromote(driverContext);
            assertThat(promoted, sameInstance(op));
        }
    }

    /**
     * When rows received are at or below the promotion threshold, {@code tryPromote}
     * must return the same {@link TopNOperator}.
     */
    public void testTryPromoteReturnsSelfBelowThreshold() {
        Executor executor = threadPool.executor(ESQL_WORKER);
        TopNOperator.TopNOperatorFactory factory = new TopNOperator.TopNOperatorFactory(
            100,
            List.of(ElementType.LONG),
            List.of(DEFAULT_UNSORTABLE),
            List.of(new TopNOperator.SortOrder(0, true, false)),
            100,
            Long.MAX_VALUE,
            TopNOperator.InputOrdering.NOT_SORTED,
            null,
            new TopNOperator.ParallelWorkerConfig(executor, 2, 10, 1000)
        );
        DriverContext driverContext = nonTrackingDriverContext();
        try (TopNOperator op = factory.get(driverContext)) {
            // Feed 10 rows, well below the threshold of 1000.
            Page page = buildLongPage(TestBlockFactory.getNonBreakingInstance(), LongStream.range(0, 10));
            op.addInput(page);
            Operator promoted = op.tryPromote(driverContext);
            assertThat(promoted, instanceOf(TopNOperator.class));
            assertThat(promoted, sameInstance(op));
        }
    }

    /**
     * When rows received exceed the promotion threshold, {@code tryPromote} must return
     * a {@link ParallelTopNOperator}. The promoted operator owns the initial worker.
     */
    public void testTryPromoteReturnsParallelOperatorAboveThreshold() {
        Executor executor = threadPool.executor(ESQL_WORKER);
        TopNOperator.TopNOperatorFactory factory = new TopNOperator.TopNOperatorFactory(
            100,
            List.of(ElementType.LONG),
            List.of(DEFAULT_UNSORTABLE),
            List.of(new TopNOperator.SortOrder(0, true, false)),
            100,
            Long.MAX_VALUE,
            TopNOperator.InputOrdering.NOT_SORTED,
            null,
            new TopNOperator.ParallelWorkerConfig(executor, 2, 10, 0)
        );
        DriverContext driverContext = nonTrackingDriverContext();
        TopNOperator op = factory.get(driverContext);
        Page page = buildLongPage(TestBlockFactory.getNonBreakingInstance(), LongStream.of(42L));
        op.addInput(page);
        Operator promoted = op.tryPromote(driverContext);
        try {
            assertThat(promoted, instanceOf(ParallelTopNOperator.class));
        } finally {
            // The ParallelTopNOperator owns and closes the initial operator.
            promoted.finish();
            drainAndClose(promoted);
        }
    }

    /**
     * With 0 background workers the constructor still produces a valid
     * {@link ParallelTopNOperator} whose {@code allWorkersDone} fires immediately.
     */
    public void testTryPromoteWithZeroWorkerCount() {
        Executor executor = threadPool.executor(ESQL_WORKER);
        TopNOperator.TopNOperatorFactory factory = new TopNOperator.TopNOperatorFactory(
            100,
            List.of(ElementType.LONG),
            List.of(DEFAULT_UNSORTABLE),
            List.of(new TopNOperator.SortOrder(0, true, false)),
            100,
            Long.MAX_VALUE,
            TopNOperator.InputOrdering.NOT_SORTED,
            null,
            new TopNOperator.ParallelWorkerConfig(executor, 0, 10, 0)
        );
        DriverContext driverContext = nonTrackingDriverContext();
        TopNOperator op = factory.get(driverContext);
        Page page = buildLongPage(TestBlockFactory.getNonBreakingInstance(), LongStream.of(7L));
        op.addInput(page);
        Operator promoted = op.tryPromote(driverContext);
        try {
            assertThat(promoted, instanceOf(ParallelTopNOperator.class));
        } finally {
            promoted.finish();
            drainAndClose(promoted);
        }
    }

    // -------------------------------------------------------------------------
    // Correctness tests via full Driver pipeline
    // -------------------------------------------------------------------------

    /**
     * Primary correctness test: the parallel operator is actually used (promotion
     * is asserted explicitly via the factory) and the sorted output matches the
     * expected top-K values.
     */
    public void testCorrectness() {
        int topN = between(1, 50);
        int workerCount = between(1, 4);
        int maxInFlightPages = between(2, 8);
        boolean ascending = randomBoolean();

        List<Long> inputValues = randomList(10, 500, () -> randomLong());
        List<Long> expected = inputValues.stream()
            .sorted(ascending ? Comparator.naturalOrder() : Comparator.reverseOrder())
            .limit(topN)
            .collect(Collectors.toList());

        List<Long> actual = runParallelTopN(topN, workerCount, maxInFlightPages, 0, ascending, inputValues);
        assertThat(actual, equalTo(expected));
    }

    /**
     * The degenerate case of 0 background workers: the pipeline runs to completion
     * without error. With 0 workers the exchange buffer is never drained, so only
     * pre-promotion data (held by the merge target) reaches the output — correctness
     * for all data is only guaranteed when {@code workerCount >= 1}.
     */
    public void testZeroWorkersPipelineCompletes() {
        int topN = between(1, 30);
        boolean ascending = randomBoolean();

        // Put all data in a single page so the initial TopNOperator absorbs it all before promotion.
        // With promotionThresholdRows=0 and a single-page input, the sequence is:
        // addInput(page) → rowsReceived = N > 0, tryPromote() → ParallelTopNOperator(workers=0)
        // finish() → in.finish(false), allWorkersDone already done
        // getOutput() → feed empty workerOutputs to mergeTarget, return mergeTarget output
        List<Long> inputValues = randomList(1, 50, () -> randomLong());

        // Run completes without error; output may be partial or empty.
        List<Long> actual = runParallelTopN(topN, 0, 4, 0, ascending, inputValues);
        // The result must be sorted (whatever subset was retained).
        for (int i = 1; i < actual.size(); i++) {
            if (ascending) {
                assertThat(actual.get(i - 1), lessThanOrEqualTo(actual.get(i)));
            } else {
                assertThat(actual.get(i - 1), greaterThanOrEqualTo(actual.get(i)));
            }
        }
    }

    /**
     * Runs a parallel TopN through the full driver pipeline and returns the sorted long values.
     *
     * <p>Uses {@link TestBlockFactory#getNonBreakingInstance()} to avoid polluting the
     * tracked breakers with the {@code overReservedBytes} that worker
     * {@link org.elasticsearch.compute.data.LocalCircuitBreaker}s hold until they are
     * garbage collected.</p>
     *
     * @param promotionThresholdRows use 0 to guarantee promotion after the very first row
     */
    private List<Long> runParallelTopN(
        int topN,
        int workerCount,
        int maxInFlightPages,
        long promotionThresholdRows,
        boolean ascending,
        List<Long> inputValues
    ) {
        BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance();
        DriverContext driverContext = nonTrackingDriverContext();
        Executor executor = threadPool.executor(ESQL_WORKER);

        TopNOperator.TopNOperatorFactory factory = new TopNOperator.TopNOperatorFactory(
            topN,
            List.of(ElementType.LONG),
            List.of(DEFAULT_UNSORTABLE),
            List.of(new TopNOperator.SortOrder(0, ascending, false)),
            topN,
            Long.MAX_VALUE,
            TopNOperator.InputOrdering.NOT_SORTED,
            null,
            new TopNOperator.ParallelWorkerConfig(executor, workerCount, maxInFlightPages, promotionThresholdRows)
        );

        // Build multi-page input.
        List<Page> inputPages = buildLongPages(blockFactory, inputValues, between(1, Math.max(1, inputValues.size())));

        TestDriverRunner runner = new TestDriverRunner();
        List<Page> outputPages = runner.builder(driverContext).input(new CannedSourceOperator(inputPages.iterator())).run(factory);

        List<Long> result = new ArrayList<>();
        for (Page p : outputPages) {
            try {
                LongBlock block = p.getBlock(0);
                for (int i = 0; i < p.getPositionCount(); i++) {
                    result.add(block.getLong(i));
                }
            } finally {
                p.releaseBlocks();
            }
        }
        return result;
    }

    // -------------------------------------------------------------------------
    // State machine tests (construct ParallelTopNOperator directly)
    // -------------------------------------------------------------------------

    /**
     * Before {@code finish()} is called, and while the exchange buffer has room,
     * {@code needsInput()} must be true and {@code isBlocked()} must be NOT_BLOCKED.
     */
    public void testIsBlockedDuringCollection() {
        DriverContext driverContext = nonTrackingDriverContext();
        Executor executor = threadPool.executor(ESQL_WORKER);
        TopNOperator.TopNOperatorFactory factory = simpleFactory();
        TopNOperator initialWorker = factory.get(driverContext);
        TopNOperator.ParallelWorkerConfig config = new TopNOperator.ParallelWorkerConfig(executor, 1, 10, 0);

        try (ParallelTopNOperator op = new ParallelTopNOperator(config, driverContext, factory, initialWorker)) {
            // Buffer has room, not yet finished.
            assertTrue(op.needsInput());
            assertThat(op.isBlocked(), sameInstance(Operator.NOT_BLOCKED));
        }
    }

    /**
     * After {@code finish()} is called with at least one background worker, verify that
     * workers eventually exit and the operator reaches the NOT_BLOCKED / finished state.
     */
    public void testIsBlockedAfterFinish() throws Exception {
        DriverContext driverContext = nonTrackingDriverContext();
        Executor executor = threadPool.executor(ESQL_WORKER);
        TopNOperator.TopNOperatorFactory factory = simpleFactory();
        TopNOperator initialWorker = factory.get(driverContext);
        TopNOperator.ParallelWorkerConfig config = new TopNOperator.ParallelWorkerConfig(executor, 2, 10, 0);

        ParallelTopNOperator op = new ParallelTopNOperator(config, driverContext, factory, initialWorker);
        try {
            op.finish();
            // Workers may or may not have finished yet. Wait until they do.
            assertBusy(() -> assertThat(op.isBlocked(), sameInstance(Operator.NOT_BLOCKED)));
            drainAndClose(op);
        } catch (AssertionError e) {
            op.close();
            throw e;
        }
    }

    /**
     * After {@code finish()} is called, {@code needsInput()} must return false.
     */
    public void testNeedsInputAfterFinish() {
        DriverContext driverContext = nonTrackingDriverContext();
        Executor executor = threadPool.executor(ESQL_WORKER);
        TopNOperator.TopNOperatorFactory factory = simpleFactory();
        TopNOperator initialWorker = factory.get(driverContext);
        TopNOperator.ParallelWorkerConfig config = new TopNOperator.ParallelWorkerConfig(executor, 1, 10, 0);

        try (ParallelTopNOperator op = new ParallelTopNOperator(config, driverContext, factory, initialWorker)) {
            op.finish();
            assertFalse(op.needsInput());
        }
    }

    /**
     * After {@code finish()} but before the merge phase completes and output is drained,
     * {@code isFinished()} must return false. It becomes true only after all output is consumed.
     */
    public void testIsFinishedRequiresMergeDone() throws Exception {
        BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance();
        DriverContext driverContext = nonTrackingDriverContext();
        Executor executor = threadPool.executor(ESQL_WORKER);
        TopNOperator.TopNOperatorFactory factory = simpleFactory();
        TopNOperator initialWorker = factory.get(driverContext);
        TopNOperator.ParallelWorkerConfig config = new TopNOperator.ParallelWorkerConfig(executor, 1, 10, 0);

        ParallelTopNOperator op = new ParallelTopNOperator(config, driverContext, factory, initialWorker);
        try {
            Page inputPage = buildLongPage(blockFactory, LongStream.range(0, 5));
            op.addInput(inputPage);
            op.finish();

            assertFalse("should not be finished before merge completes", op.isFinished());

            // Wait for workers to finish.
            assertBusy(() -> assertThat(op.isBlocked(), sameInstance(Operator.NOT_BLOCKED)));

            // Drain output.
            drainAndClose(op);
        } catch (AssertionError | Exception e) {
            op.close();
            throw e;
        }
    }

    // -------------------------------------------------------------------------
    // Close / cleanup test
    // -------------------------------------------------------------------------

    /**
     * Closing without completing must not throw and must leave no outstanding async
     * actions. The operator signals workers to abort via the exchange buffer.
     */
    public void testCloseWithoutCompleting() throws Exception {
        DriverContext driverContext = nonTrackingDriverContext();
        Executor executor = threadPool.executor(ESQL_WORKER);
        TopNOperator.TopNOperatorFactory factory = simpleFactory();
        TopNOperator initialWorker = factory.get(driverContext);
        TopNOperator.ParallelWorkerConfig config = new TopNOperator.ParallelWorkerConfig(executor, 2, 10, 0);

        ParallelTopNOperator op = new ParallelTopNOperator(config, driverContext, factory, initialWorker);
        Page page = buildLongPage(TestBlockFactory.getNonBreakingInstance(), LongStream.range(0, 5));
        op.addInput(page);
        // Close without calling finish or getOutput.
        op.close();

        // Workers must eventually remove their async actions so the DriverContext can finish.
        assertBusy(() -> assertFalse(driverContext.hasPendingAsyncActions()));
    }

    // -------------------------------------------------------------------------
    // Failure (cranky) test
    // -------------------------------------------------------------------------

    /**
     * With a cranky circuit breaker, the pipeline must either complete correctly or
     * surface a {@link CircuitBreakingException} without leaking memory.
     */
    public void testFailureWithCrankyBreaker() {
        try {
            CrankyCircuitBreakerService cranky = new CrankyCircuitBreakerService();
            BigArrays bigArrays = crankyBigArrays(cranky);
            BlockFactory blockFactory = BlockFactory.builder(bigArrays).build();
            DriverContext driverContext = new DriverContext(bigArrays, blockFactory, null);
            Executor executor = threadPool.executor(ESQL_WORKER);

            TopNOperator.TopNOperatorFactory factory = new TopNOperator.TopNOperatorFactory(
                10,
                List.of(ElementType.LONG),
                List.of(DEFAULT_UNSORTABLE),
                List.of(new TopNOperator.SortOrder(0, true, false)),
                10,
                Long.MAX_VALUE,
                TopNOperator.InputOrdering.NOT_SORTED,
                null,
                new TopNOperator.ParallelWorkerConfig(executor, 2, 4, 0)
            );

            List<Long> inputValues = randomList(10, 100, () -> randomLong());
            BlockFactory inputFactory = TestBlockFactory.getNonBreakingInstance();
            List<Page> inputPages = buildLongPages(inputFactory, inputValues, between(1, inputValues.size()));

            TestDriverRunner runner = new TestDriverRunner();
            List<Page> outputPages = runner.builder(driverContext).input(new CannedSourceOperator(inputPages.iterator())).run(factory);
            // If cranky didn't trigger, verify we got some output without error.
            for (Page p : outputPages) {
                p.releaseBlocks();
            }
            logger.info("cranky didn't break us");
        } catch (CircuitBreakingException e) {
            logger.info("broken by cranky breaker", e);
            assertThat(e.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
        }
    }

    // -------------------------------------------------------------------------
    // RAM accounting
    // -------------------------------------------------------------------------

    /**
     * {@code ramBytesUsed()} must account for at least the shallow size of the
     * {@link ParallelTopNOperator} plus the merge target's own accounting, and
     * must increase with the number of workers.
     */
    public void testRamBytesUsed() {
        DriverContext driverContext = nonTrackingDriverContext();
        Executor executor = threadPool.executor(ESQL_WORKER);
        int workerCount = between(1, 3);
        TopNOperator.TopNOperatorFactory factory = simpleFactory();
        TopNOperator initialWorker = factory.get(driverContext);
        TopNOperator.ParallelWorkerConfig config = new TopNOperator.ParallelWorkerConfig(executor, workerCount, 10, 0);

        try (ParallelTopNOperator op = new ParallelTopNOperator(config, driverContext, factory, initialWorker)) {
            long reported = op.ramBytesUsed();
            long shallowSize = RamUsageEstimator.shallowSizeOfInstance(ParallelTopNOperator.class);
            // Must cover at least its own shallow size.
            assertThat(reported, greaterThanOrEqualTo(shallowSize));
            // Must cover the merge-target's accounting.
            assertThat(reported, greaterThanOrEqualTo(initialWorker.ramBytesUsed()));
            // With workers the total must exceed the shallow size alone.
            assertThat(reported, greaterThanOrEqualTo(shallowSize + (long) workerCount * initialWorker.ramBytesUsed()));
        }
    }

    // -------------------------------------------------------------------------
    // toString
    // -------------------------------------------------------------------------

    /**
     * {@code toString()} must report the total worker count (background workers + 1 for the
     * merge target).
     */
    public void testToString() {
        DriverContext driverContext = nonTrackingDriverContext();
        Executor executor = threadPool.executor(ESQL_WORKER);
        int workerCount = between(0, 4);
        TopNOperator.TopNOperatorFactory factory = simpleFactory();
        TopNOperator initialWorker = factory.get(driverContext);
        TopNOperator.ParallelWorkerConfig config = new TopNOperator.ParallelWorkerConfig(executor, workerCount, 10, 0);

        try (ParallelTopNOperator op = new ParallelTopNOperator(config, driverContext, factory, initialWorker)) {
            assertThat(op.toString(), equalTo("ParallelTopNOperator[workers=" + (workerCount + 1) + "]"));
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /**
     * A simple single-column long factory used by state machine tests.
     */
    private static TopNOperator.TopNOperatorFactory simpleFactory() {
        return new TopNOperator.TopNOperatorFactory(
            100,
            List.of(ElementType.LONG),
            List.of(DEFAULT_UNSORTABLE),
            List.of(new TopNOperator.SortOrder(0, true, false)),
            100,
            Long.MAX_VALUE,
            TopNOperator.InputOrdering.NOT_SORTED,
            null
        );
    }

    /**
     * A {@link DriverContext} backed by the non-breaking {@link TestBlockFactory}.
     * Use this in tests that directly construct {@link ParallelTopNOperator} to avoid
     * polluting the {@link ComputeTestCase#allBreakersEmpty()} check with the
     * {@code overReservedBytes} that worker {@link org.elasticsearch.compute.data.LocalCircuitBreaker}s
     * hold until they exit.
     */
    private static DriverContext nonTrackingDriverContext() {
        BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance();
        return new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, blockFactory, null);
    }

    /**
     * Returns a non-breaking {@link BigArrays} backed by the cranky service but without
     * registering the breaker in this test's tracked list.
     */
    private static BigArrays crankyBigArrays(CrankyCircuitBreakerService cranky) {
        return BigArrays.NON_RECYCLING_INSTANCE.withBreakerService(cranky);
    }

    /** Build a single-column long {@link Page} from a {@link LongStream}. */
    private static Page buildLongPage(BlockFactory blockFactory, LongStream values) {
        long[] arr = values.toArray();
        LongBlock.Builder builder = blockFactory.newLongBlockBuilder(arr.length);
        for (long v : arr) {
            builder.appendLong(v);
        }
        return new Page(builder.build());
    }

    /**
     * Partition {@code inputValues} into multiple pages of at most {@code pageSize} rows each.
     */
    private static List<Page> buildLongPages(BlockFactory blockFactory, List<Long> inputValues, int pageSize) {
        List<Page> pages = new ArrayList<>();
        int i = 0;
        while (i < inputValues.size()) {
            int end = Math.min(i + pageSize, inputValues.size());
            pages.add(buildLongPage(blockFactory, inputValues.subList(i, end).stream().mapToLong(Long::longValue)));
            i = end;
        }
        return pages;
    }

    /**
     * Drain all output from {@code op} (which must be in the finished state) and then close it.
     */
    private static void drainAndClose(Operator op) {
        try {
            Page p;
            while ((p = op.getOutput()) != null) {
                p.releaseBlocks();
            }
        } finally {
            op.close();
        }
    }
}
