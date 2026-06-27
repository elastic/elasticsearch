/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.tests.util.RamUsageTester;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.MapMatcher;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.stream.LongStream;

import static org.elasticsearch.compute.operator.topn.TopNEncoder.DEFAULT_UNSORTABLE;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.sameInstance;

public class ParallelTopNOperatorTests extends TopNOperatorTests {

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

    /**
     * Returns the executor for the esql_worker thread pool.
     */
    private Executor workerExecutor() {
        return threadPool.executor(ESQL_WORKER);
    }

    // -------------------------------------------------------------------------
    // Override simple() to exercise the parallel path
    // -------------------------------------------------------------------------

    @Override
    protected Operator.OperatorFactory simple(SimpleOptions options) {
        List<ElementType> elementTypes = List.of(ElementType.LONG);
        List<TopNEncoder> encoders = List.of(DEFAULT_UNSORTABLE);
        List<TopNOperator.SortOrder> sortOrders = List.of(new TopNOperator.SortOrder(0, true, false));
        return new TopNOperator.TopNOperatorFactory(
            4,
            elementTypes,
            encoders,
            sortOrders,
            pageSize,
            Long.MAX_VALUE,
            TopNOperator.InputOrdering.NOT_SORTED,
            null,
            new TopNOperator.ParallelWorkerConfig(workerExecutor(), 2, 8, 0)
        );
    }

    // -------------------------------------------------------------------------
    // Override createTopNOperatorFactory() so all inherited correctness tests
    // run against the parallel operator path
    // -------------------------------------------------------------------------

    @Override
    protected Operator.OperatorFactory createTopNOperatorFactory(
        int topCount,
        List<ElementType> elementTypes,
        List<TopNEncoder> encoders,
        List<TopNOperator.SortOrder> sortOrders,
        int[] groupKeys,
        int maxPageSize,
        long jumboPageBytes,
        TopNOperator.InputOrdering inputOrdering,
        @Nullable SharedMinCompetitive.Supplier minCompetitive
    ) {
        if (groupKeys.length > 0) {
            // GroupedTopN has no parallel support; delegate to parent
            return super.createTopNOperatorFactory(
                topCount,
                elementTypes,
                encoders,
                sortOrders,
                groupKeys,
                maxPageSize,
                jumboPageBytes,
                inputOrdering,
                minCompetitive
            );
        }
        return new TopNOperator.TopNOperatorFactory(
            topCount,
            elementTypes,
            encoders,
            sortOrders,
            maxPageSize,
            jumboPageBytes,
            inputOrdering,
            minCompetitive,
            new TopNOperator.ParallelWorkerConfig(workerExecutor(), between(1, 4), between(4, 16), 0)
        );
    }

    // -------------------------------------------------------------------------
    // Parent test overrides for parallel-operator compatibility
    // -------------------------------------------------------------------------

    /**
     * Override status assertions to accept partial counts from the pre-promotion
     * {@link TopNOperator}. When promotion occurs, pages and rows received after
     * promotion are counted in the {@link ParallelTopNOperator}, not in the
     * original operator whose status is checked by the parent harness. So we
     * accept any count {@code <= totalInputPages} / {@code <= totalInputRows}.
     */
    @Override
    protected void assertStatus(@Nullable Map<String, Object> map, List<Page> input, List<Page> output) {
        if (map == null) {
            return; // ParallelTopNOperator has no status()
        }
        // Pages and rows received by the initial TopNOperator may be a subset of
        // the total input because the rest was processed by background workers.
        int totalInputRows = input.stream().mapToInt(Page::getPositionCount).sum();
        int totalOutputRows = output.stream().mapToInt(Page::getPositionCount).sum();
        MapMatcher matcher = MapMatcher.matchesMap().extraOk();
        if (map.containsKey("pages_processed")) {
            matcher = matcher.entry("pages_processed", greaterThanOrEqualTo(0));
        } else {
            // pages_received on the merge-target counts only pre-promotion input pages; not
            // useful to assert here since it reflects a subset of total input.
            matcher = matcher.entry("pages_emitted", output.size());
        }
        matcher = matcher.entry("rows_received", lessThanOrEqualTo(totalInputRows)).entry("rows_emitted", totalOutputRows);
        MapMatcher.assertMap(map, matcher);
    }

    /**
     * {@code testSimpleCircuitBreaking} is not compatible with the parallel operator:
     * background workers run on an external thread pool that may not have released all
     * of their {@link org.elasticsearch.compute.data.LocalCircuitBreaker} bytes by the
     * time the test checks the breaker. Skip this test variant for the parallel path.
     */
    @Override
    public void testSimpleCircuitBreaking() {
        assumeTrue("testSimpleCircuitBreaking is not compatible with the async parallel operator path", false);
    }

    /**
     * Override {@code testRamBytesUsed} to use the sequential factory for the RAM
     * estimation. The parent test uses {@link RamUsageTester} which traverses the full
     * object graph; when {@link TopNOperator} holds a reference to a
     * {@link TopNOperator.ParallelWorkerConfig} that contains a thread-pool executor,
     * the traversal fails because executor fields are inaccessible. The parallel-specific
     * RAM test is {@link #testParallelRamBytesUsed}.
     */
    @Override
    public void testRamBytesUsed() {
        int topCount = 10_000;
        long underCount = 250;
        DriverContext context = driverContext();
        List<ElementType> elementTypes = Collections.nCopies(1, ElementType.LONG);
        List<TopNEncoder> encoders = Collections.nCopies(1, DEFAULT_UNSORTABLE);
        // Use the parent's sequential factory so RamUsageTester can traverse the graph
        try (
            Operator op = super.createTopNOperatorFactory(
                topCount,
                elementTypes,
                encoders,
                List.of(new TopNOperator.SortOrder(0, true, false)),
                groupKeys(),
                pageSize,
                Long.MAX_VALUE,
                TopNOperator.InputOrdering.NOT_SORTED,
                null
            ).get(context)
        ) {
            Accountable accountable = (Accountable) op;
            long actualEmpty = RamUsageTester.ramUsed(op, RAM_USAGE_ACCUMULATOR);
            assertThat(accountable.ramBytesUsed(), both(greaterThan(actualEmpty - underCount)).and(lessThan(actualEmpty)));
            for (Page p : org.elasticsearch.compute.test.CannedSourceOperator.collectPages(simpleInput(context.blockFactory(), topCount))) {
                op.addInput(p);
            }
            long actualFull = RamUsageTester.ramUsed(op, RAM_USAGE_ACCUMULATOR);
            assertThat(accountable.ramBytesUsed(), both(greaterThan(actualFull - underCount)).and(lessThan(actualFull)));
        }
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
        DriverContext driverContext = driverContext();
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
        TopNOperator.TopNOperatorFactory factory = new TopNOperator.TopNOperatorFactory(
            100,
            List.of(ElementType.LONG),
            List.of(DEFAULT_UNSORTABLE),
            List.of(new TopNOperator.SortOrder(0, true, false)),
            100,
            Long.MAX_VALUE,
            TopNOperator.InputOrdering.NOT_SORTED,
            null,
            new TopNOperator.ParallelWorkerConfig(workerExecutor(), 2, 10, 1000)
        );
        DriverContext driverContext = driverContext();
        try (TopNOperator op = factory.get(driverContext)) {
            // Feed 10 rows, well below the threshold of 1000.
            Page page = buildLongPage(driverContext().blockFactory(), LongStream.range(0, 10));
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
        TopNOperator.TopNOperatorFactory factory = new TopNOperator.TopNOperatorFactory(
            100,
            List.of(ElementType.LONG),
            List.of(DEFAULT_UNSORTABLE),
            List.of(new TopNOperator.SortOrder(0, true, false)),
            100,
            Long.MAX_VALUE,
            TopNOperator.InputOrdering.NOT_SORTED,
            null,
            new TopNOperator.ParallelWorkerConfig(workerExecutor(), 2, 10, 0)
        );
        DriverContext driverContext = driverContext();
        TopNOperator op = factory.get(driverContext);
        Page page = buildLongPage(driverContext().blockFactory(), LongStream.of(42L));
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

    // -------------------------------------------------------------------------
    // State machine tests (construct ParallelTopNOperator directly)
    // -------------------------------------------------------------------------

    /**
     * Before {@code finish()} is called, and while the exchange buffer has room,
     * {@code needsInput()} must be true and {@code isBlocked()} must be NOT_BLOCKED.
     */
    public void testIsBlockedDuringCollection() {
        DriverContext driverContext = driverContext();
        TopNOperator.TopNOperatorFactory workerFactory = workerOnlyFactory();
        TopNOperator initialWorker = workerFactory.get(driverContext);
        TopNOperator.ParallelWorkerConfig config = new TopNOperator.ParallelWorkerConfig(workerExecutor(), 1, 10, 0);

        try (ParallelTopNOperator op = new ParallelTopNOperator(config, driverContext, initialWorker)) {
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
        DriverContext driverContext = driverContext();
        TopNOperator.TopNOperatorFactory workerFactory = workerOnlyFactory();
        TopNOperator initialWorker = workerFactory.get(driverContext);
        TopNOperator.ParallelWorkerConfig config = new TopNOperator.ParallelWorkerConfig(workerExecutor(), 2, 10, 0);

        ParallelTopNOperator op = new ParallelTopNOperator(config, driverContext, initialWorker);
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
     * After {@code finish()} but before the merge phase completes and output is drained,
     * {@code isFinished()} must return false. It becomes true only after all output is consumed.
     */
    public void testIsFinishedRequiresMergeDone() throws Exception {
        DriverContext driverContext = driverContext();
        TopNOperator.TopNOperatorFactory workerFactory = workerOnlyFactory();
        TopNOperator initialWorker = workerFactory.get(driverContext);
        TopNOperator.ParallelWorkerConfig config = new TopNOperator.ParallelWorkerConfig(workerExecutor(), 1, 10, 0);

        ParallelTopNOperator op = new ParallelTopNOperator(config, driverContext, initialWorker);
        try {
            Page inputPage = buildLongPage(driverContext().blockFactory(), LongStream.range(0, 5));
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
    // Close / cleanup test (parallel-specific variant)
    // -------------------------------------------------------------------------

    /**
     * Closing without completing must not throw and must leave no outstanding async
     * actions. The operator signals workers to abort via the exchange buffer.
     * <p>
     * This tests the {@link ParallelTopNOperator} close path directly (not the
     * sequential {@link TopNOperator} path tested by the inherited
     * {@code testCloseWithoutCompleting}).
     */
    public void testParallelCloseWithoutCompleting() throws Exception {
        DriverContext driverContext = driverContext();
        TopNOperator.TopNOperatorFactory workerFactory = workerOnlyFactory();
        TopNOperator initialWorker = workerFactory.get(driverContext);
        TopNOperator.ParallelWorkerConfig config = new TopNOperator.ParallelWorkerConfig(workerExecutor(), 2, 10, 0);

        ParallelTopNOperator op = new ParallelTopNOperator(config, driverContext, initialWorker);
        Page page = buildLongPage(driverContext().blockFactory(), LongStream.range(0, 5));
        op.addInput(page);
        // Close without calling finish or getOutput.
        op.close();

        // Workers must eventually remove their async actions so the DriverContext can finish.
        assertBusy(() -> assertFalse(driverContext.hasPendingAsyncActions()));
    }

    // -------------------------------------------------------------------------
    // RAM accounting (parallel-specific variant)
    // -------------------------------------------------------------------------

    /**
     * {@code ramBytesUsed()} must account for at least the shallow size of the
     * {@link ParallelTopNOperator} plus the merge target's own accounting, and
     * must increase with the number of workers.
     * <p>
     * This tests the {@link ParallelTopNOperator} RAM accounting directly (not the
     * sequential accumulation phase tested by the inherited {@code testRamBytesUsed}).
     */
    public void testParallelRamBytesUsed() throws Exception {
        DriverContext driverContext = driverContext();
        int workerCount = between(1, 3);
        TopNOperator.TopNOperatorFactory workerFactory = workerOnlyFactory();
        TopNOperator initialWorker = workerFactory.get(driverContext);
        TopNOperator.ParallelWorkerConfig config = new TopNOperator.ParallelWorkerConfig(workerExecutor(), workerCount, 10, 0);

        try (ParallelTopNOperator op = new ParallelTopNOperator(config, driverContext, initialWorker)) {
            long shallowSize = RamUsageEstimator.shallowSizeOfInstance(ParallelTopNOperator.class);

            // While workers are still running their state is not safe to read; ramBytesUsed
            // must still cover at least the shallow size and the merge-target's own accounting.
            long reportedBeforeDone = op.ramBytesUsed();
            assertThat(reportedBeforeDone, greaterThanOrEqualTo(shallowSize));
            assertThat(reportedBeforeDone, greaterThanOrEqualTo(initialWorker.ramBytesUsed()));

            // After workers finish it is safe to include their accounting too.
            op.finish();
            assertBusy(() -> assertThat(op.isBlocked(), sameInstance(Operator.NOT_BLOCKED)));
            long reportedAfterDone = op.ramBytesUsed();
            assertThat(reportedAfterDone, greaterThanOrEqualTo(shallowSize));
            assertThat(reportedAfterDone, greaterThanOrEqualTo(initialWorker.ramBytesUsed()));

            drainAndClose(op);
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
        DriverContext driverContext = driverContext();
        int workerCount = between(1, 4);
        TopNOperator.TopNOperatorFactory workerFactory = workerOnlyFactory();
        TopNOperator initialWorker = workerFactory.get(driverContext);
        TopNOperator.ParallelWorkerConfig config = new TopNOperator.ParallelWorkerConfig(workerExecutor(), workerCount, 10, 0);

        try (ParallelTopNOperator op = new ParallelTopNOperator(config, driverContext, initialWorker)) {
            assertThat(op.toString(), equalTo("ParallelTopNOperator[workers=" + (workerCount + 1) + "]"));
        }
    }

    public void testPartialWorkerRejectionProducesCorrectOutput() throws Exception {
        Executor randomlyRejectingExecutor = task -> {
            if (randomBoolean() && task instanceof AbstractRunnable runnable) {
                runnable.onRejection(new EsRejectedExecutionException("Rejected!", false));
            } else {
                workerExecutor().execute(task);
            }
        };

        DriverContext driverContext = driverContext();
        TopNOperator.TopNOperatorFactory workerFactory = workerOnlyFactory();
        TopNOperator initialWorker = workerFactory.get(driverContext);
        TopNOperator.ParallelWorkerConfig config = new TopNOperator.ParallelWorkerConfig(randomlyRejectingExecutor, 2, 10, 0);

        ParallelTopNOperator op = new ParallelTopNOperator(config, driverContext, initialWorker);

        List<Long> input = new ArrayList<>(LongStream.range(1, 1001).boxed().toList());
        Collections.shuffle(input, random());

        try {
            // Split data into 100 pages
            for (int i = 0; i < 100; i++) {
                List<Long> slice = input.subList(i * 10, (i + 1) * 10);
                op.addInput(buildLongPage(driverContext().blockFactory(), slice.stream().mapToLong(Long::longValue)));
            }
            op.finish();

            assertBusy(() -> assertThat(op.isBlocked(), sameInstance(Operator.NOT_BLOCKED)));
            assertThat(getLongResults(op), equalTo(LongStream.range(1, 101).boxed().toList()));
        } finally {
            op.close();
        }
    }

    /**
     * Test which catches a race condition bug. Previously, noMoreInputs() was used rather isFinished()
     * for checking if the input buffer was empty before merging the worker operators into the main
     * merge operator. It was possible for a page to be added to the queue, then for finish() to be
     * called, and then for notMoreInputs to return true while the page was still in the queue.
     * This test can catch the race-condition, though the code now has the correct call to isFinished().
     */
    public void testNoPageLossUnderConcurrentFinish() throws Exception {
        for (int i = 0; i < 1000; i++) {
            DriverContext ctx = driverContext();
            TopNOperator.TopNOperatorFactory factory = workerOnlyFactory();
            TopNOperator initial = factory.get(ctx);
            TopNOperator.ParallelWorkerConfig config = new TopNOperator.ParallelWorkerConfig(workerExecutor(), 1, 10, 0);
            ParallelTopNOperator op = new ParallelTopNOperator(config, ctx, initial);
            try {
                op.addInput(buildLongPage(ctx.blockFactory(), LongStream.of(123L)));
                op.finish();
                assertBusy(() -> assertThat(op.isBlocked(), sameInstance(Operator.NOT_BLOCKED)));
                assertThat("iteration " + i + ": page lost", getLongResults(op), equalTo(List.of(123L)));
            } finally {
                op.close();
            }
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /**
     * A simple single-column long factory used by state machine tests that construct
     * {@link ParallelTopNOperator} directly. This factory does NOT have a
     * {@link TopNOperator.ParallelWorkerConfig}, so operators it creates are plain
     * background workers (not themselves promotable).
     */
    private static TopNOperator.TopNOperatorFactory workerOnlyFactory() {
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

    private static List<Long> getLongResults(ParallelTopNOperator op) {
        List<Long> results = new ArrayList<>();
        Page out;
        while ((out = op.getOutput()) != null) {
            LongBlock block = out.getBlock(0);
            for (int i = 0; i < block.getPositionCount(); i++) {
                results.add(block.getLong(i));
            }
            out.releaseBlocks();
        }
        return results;
    }

}
