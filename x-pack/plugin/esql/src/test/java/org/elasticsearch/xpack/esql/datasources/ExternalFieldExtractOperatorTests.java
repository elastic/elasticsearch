/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.DriverEarlyTerminationException;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.AsyncOperatorTestCase;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.compute.test.operator.blocksource.AbstractBlockSourceOperator;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.CrankyCircuitBreakerService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.test.MapMatcher;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.ColumnExtractor;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalClientException;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalException.Condition;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalServerException;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalUnavailableException;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link ExternalFieldExtractOperator}: the channel-reshaping logic that drops
 * {@code _rowPosition} from the page, materialises deferred columns via the driver-shared
 * {@link SourceExtractors} registry, and assembles the output page in declared output order.
 */
public class ExternalFieldExtractOperatorTests extends AsyncOperatorTestCase {

    private static final String TEST_EXECUTOR_NAME = "external_field_operator_tests";

    // Leak-tracking factory: ComputeTestCase's teardown asserts every block allocated by any
    // test is released, so each test doubles as a leak test. Initialized in a @Before method
    // rather than a field initializer to avoid a this-escape during construction.
    private BlockFactory blockFactory;

    private TestThreadPool threadPool;

    @Before
    public void initBlockFactory() {
        blockFactory = blockFactory();
    }

    @Before
    public void setThreadPool() {
        int numThreads = randomBoolean() ? 1 : between(2, 16);
        threadPool = new TestThreadPool(
            "test",
            new FixedExecutorBuilder(Settings.EMPTY, TEST_EXECUTOR_NAME, numThreads, 1024, "esql", EsExecutors.TaskTrackingConfig.DEFAULT)
        );
    }

    @After
    public void shutdownThreadPool() {
        terminate(threadPool);
    }

    /**
     * Materializes on a separate thread after a random delay, instead of inline on the calling
     * thread, so {@link #simple} exercises the operator's real isBlocked/checkpoint handling
     * rather than always completing synchronously inside {@code addInput}.
     */
    private Executor randomDelayExecutor() {
        return command -> threadPool.schedule(
            command,
            TimeValue.timeValueMillis(randomIntBetween(0, 10)),
            threadPool.executor(TEST_EXECUTOR_NAME)
        );
    }

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        return new AbstractBlockSourceOperator(blockFactory, 5 * randomPageSize()) {
            @Override
            protected int remaining() {
                return size - currentPosition;
            }

            @Override
            protected Page createPage(int positionOffset, int length) {
                long[] sortKeys = new long[length];
                long[] rowPositions = new long[length];
                int[] passThrough = new int[length];
                for (int i = 0; i < length; i++) {
                    int globalPosition = positionOffset + i;
                    sortKeys[i] = globalPosition;
                    rowPositions[i] = SourceExtractors.encode(0, globalPosition);
                    passThrough[i] = globalPosition;
                }
                currentPosition += length;
                return new Page(
                    length,
                    blockFactory.newLongArrayVector(sortKeys, length).asBlock(),
                    blockFactory.newLongArrayVector(rowPositions, length).asBlock(),
                    blockFactory.newIntArrayVector(passThrough, length).asBlock()
                );
            }
        };
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> results) {
        assertEquals(input.size(), results.size());
        for (int pageIndex = 0; pageIndex < input.size(); pageIndex++) {
            Page inputPage = input.get(pageIndex);
            Page resultPage = results.get(pageIndex);
            assertEquals(inputPage.getPositionCount(), resultPage.getPositionCount());
            assertEquals(3, resultPage.getBlockCount());
            LongVector inputSortKeys = ((LongBlock) inputPage.getBlock(0)).asVector();
            IntVector inputPassThrough = ((IntBlock) inputPage.getBlock(2)).asVector();
            LongVector resultSortKeys = ((LongBlock) resultPage.getBlock(0)).asVector();
            IntVector resultPassThrough = ((IntBlock) resultPage.getBlock(1)).asVector();
            IntVector resultExtracted = ((IntBlock) resultPage.getBlock(2)).asVector();
            assertNotNull(inputSortKeys);
            assertNotNull(inputPassThrough);
            assertNotNull(resultSortKeys);
            assertNotNull(resultPassThrough);
            assertNotNull(resultExtracted);
            for (int p = 0; p < inputPage.getPositionCount(); p++) {
                assertEquals(inputSortKeys.getLong(p), resultSortKeys.getLong(p));
                assertEquals(inputPassThrough.getInt(p), resultPassThrough.getInt(p));
                int globalPosition = inputPassThrough.getInt(p);
                assertEquals(Math.multiplyExact(globalPosition, globalPosition), resultExtracted.getInt(p));
            }
        }
    }

    @Override
    protected Operator.OperatorFactory simple(SimpleOptions options) {
        // testSimpleCircuitBreaking drives this factory through dozens of binary-search iterations;
        // keep it synchronous there so it stays fast and so each iteration behaves identically.
        Executor executor = options.requiresDeterministicFactory() ? Runnable::run : randomDelayExecutor();
        return new ExternalFieldExtractOperator.Factory(1, List.of(0, 2), List.of("col"), List.of(DataType.INTEGER), driverContext -> {
            SourceExtractors registry = new SourceExtractors();
            registry.register(new SquaredPositionExtractor());
            return registry;
        }, null, executor);
    }

    @Override
    protected int largeInputSize() {
        // ExternalFieldExtractOperator serializes materializations (maxOutstandingRequests = 1), and
        // randomDelayExecutor() adds a real per-page delay, so the default (up to 10,000 rows split
        // into pages as small as 1) can blow past TestDriverRunner's 30s budget. Bound it like
        // InferenceOperatorTestCase does for the same reason.
        return between(500, 5_000);
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return equalTo("ExternalFieldExtractOperator[rowPositionChannel=1, passThrough=2, deferred=[col]]");
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return expectedDescriptionOfSimple();
    }

    @Override
    protected MapMatcher extendStatusMatcher(MapMatcher mapMatcher, List<Page> input, List<Page> output) {
        return mapMatcher.entry("pages_processed", input.size())
            .entry("rows_extracted", input.stream().mapToInt(Page::getPositionCount).sum())
            .entry("extract_nanos", greaterThanOrEqualTo(0))
            .entry("extract_cpu_nanos", greaterThanOrEqualTo(0));
    }

    public void testReshapeAndExtract() {
        try (SourceExtractors registry = new SourceExtractors()) {
            int idA = registry.register(new IntListExtractor(new int[] { 100, 101, 102, 103 }));
            int idB = registry.register(new IntListExtractor(new int[] { 200, 201, 202 }));

            // Build an input page that simulates output of the source: channels are
            // ch0 = sortKey (long), ch1 = _rowPosition (encoded long), ch2 = passThru (int)
            // Five rows surviving TopN, drawn from both extractors:
            // row 0: A[3]
            // row 1: B[1]
            // row 2: A[0]
            // row 3: B[2]
            // row 4: A[2]
            long[] sortKey = { 7L, 8L, 9L, 10L, 11L };
            long[] rowPosition = {
                SourceExtractors.encode(idA, 3),
                SourceExtractors.encode(idB, 1),
                SourceExtractors.encode(idA, 0),
                SourceExtractors.encode(idB, 2),
                SourceExtractors.encode(idA, 2) };
            int[] passThru = { 1, 2, 3, 4, 5 };
            Page input = newPage(sortKey, rowPosition, passThru);

            DriverContext driverContext = mock(DriverContext.class);
            when(driverContext.blockFactory()).thenReturn(blockFactory);

            ExternalFieldExtractOperator op = new ExternalFieldExtractOperator(/* rowPositionChannel = */ 1,
                /* passThroughChannels = */ List.of(0, 2),
                /* deferredColumnNames = */ List.of("col"),
                /* deferredColumnTypes = */ List.of(DataType.INTEGER),
                registry,
                blockFactory,
                null
            );
            op.addInput(input);
            op.finish();
            Page output = op.getOutput();
            assertNull("operator must drain in one shot", op.getOutput());
            assertTrue(op.isFinished());
            try {
                // Output layout: sortKey, passThru, deferred col — _rowPosition stripped.
                assertEquals(3, output.getBlockCount());
                assertEquals(5, output.getPositionCount());

                LongVector outSort = ((LongBlock) output.getBlock(0)).asVector();
                IntVector outPass = ((IntBlock) output.getBlock(1)).asVector();
                IntBlock outDeferred = (IntBlock) output.getBlock(2);

                assertNotNull("sortKey must remain a dense vector", outSort);
                assertNotNull("passThru must remain a dense vector", outPass);
                for (int i = 0; i < 5; i++) {
                    assertEquals(sortKey[i], outSort.getLong(i));
                    assertEquals(passThru[i], outPass.getInt(i));
                }
                // Deferred values must align row-for-row with the surviving (id, pos) refs.
                assertEquals(103, outDeferred.getInt(0));
                assertEquals(201, outDeferred.getInt(1));
                assertEquals(100, outDeferred.getInt(2));
                assertEquals(202, outDeferred.getInt(3));
                assertEquals(102, outDeferred.getInt(4));
            } finally {
                output.releaseBlocks();
                op.close();
            }
        }
    }

    public void testEmptyPageReshape() {
        try (SourceExtractors registry = new SourceExtractors()) {
            registry.register(new IntListExtractor(new int[] { 1, 2 }));

            Page empty = newPage(new long[0], new long[0], new int[0]);

            ExternalFieldExtractOperator op = new ExternalFieldExtractOperator(
                1,
                List.of(0, 2),
                List.of("col"),
                List.of(DataType.INTEGER),
                registry,
                blockFactory,
                null
            );
            op.addInput(empty);
            op.finish();
            Page output = op.getOutput();
            try {
                assertEquals(3, output.getBlockCount());
                assertEquals(0, output.getPositionCount());
                // Deferred slot must be a constant-null placeholder so downstream operators see
                // the right shape even on a zero-row page.
                Block d = output.getBlock(2);
                assertEquals(0, d.getPositionCount());
            } finally {
                output.releaseBlocks();
                op.close();
            }
        }
    }

    public void testEmptyPageDoesNotUseExecutor() {
        try (SourceExtractors registry = new SourceExtractors()) {
            registry.register(new IntListExtractor(new int[] { 1 }));
            DriverContext driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);
            Executor executor = command -> fail("empty pages must not be scheduled");
            ExternalFieldExtractOperator op = new ExternalFieldExtractOperator(
                1,
                List.of(0, 2),
                List.of("col"),
                List.of(DataType.INTEGER),
                registry,
                driverContext,
                null,
                executor
            );
            op.addInput(newPage(new long[0], new long[0], new int[0]));
            op.finish();
            assertTrue(op.isBlocked().listener().isDone());
            Page output = op.getOutput();
            try {
                assertEquals(0, output.getPositionCount());
                assertTrue(op.isFinished());
                assertEquals(1, ((ExternalFieldExtractOperator.Status) op.status()).pagesProcessed());
            } finally {
                output.releaseBlocks();
                op.close();
                driverContext.finish();
            }
        }
    }

    public void testMaterializationBlocksUntilExecutorCompletes() {
        SourceExtractors registry = new SourceExtractors();
        DriverContext driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);
        AtomicReference<Runnable> scheduled = new AtomicReference<>();
        Executor executor = command -> assertTrue("only one materialization may be outstanding", scheduled.compareAndSet(null, command));
        ExternalFieldExtractOperator op = new ExternalFieldExtractOperator(
            1,
            List.of(0, 2),
            List.of("col"),
            List.of(DataType.INTEGER),
            registry,
            driverContext,
            null,
            executor
        );
        try {
            int id = registry.register(new IntListExtractor(new int[] { 10 }));
            op.addInput(newPage(new long[] { 1 }, new long[] { SourceExtractors.encode(id, 0) }, new int[] { 2 }));
            assertFalse(op.needsInput());
            assertFalse(op.isBlocked().listener().isDone());
            assertNull(op.getOutput());

            op.finish();
            assertFalse("finish must not hide an outstanding materialization", op.isBlocked().listener().isDone());
            assertFalse(op.isFinished());

            Runnable task = scheduled.getAndSet(null);
            assertNotNull(task);
            task.run();

            assertTrue(op.isBlocked().listener().isDone());
            Page output = op.getOutput();
            try {
                assertEquals(10, ((IntBlock) output.getBlock(2)).getInt(0));
                assertTrue(op.isFinished());
                assertEquals(1, ((ExternalFieldExtractOperator.Status) op.status()).pagesProcessed());
            } finally {
                output.releaseBlocks();
            }
        } finally {
            op.close();
            driverContext.finish();
            registry.close();
        }
    }

    public void testCloseClosesRegistryWithoutFinishingDriverContext() {
        SourceExtractors registry = new SourceExtractors();
        DriverContext driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);
        ExternalFieldExtractOperator op = new ExternalFieldExtractOperator(
            1,
            List.of(0, 2),
            List.of("col"),
            List.of(DataType.INTEGER),
            registry,
            driverContext,
            null,
            Runnable::run
        );
        registry.register(new IntListExtractor(new int[] { 10 }));

        op.close();

        assertFalse(driverContext.isFinished());
        assertEquals(0, registry.size());
        driverContext.finish();
    }

    public void testCloseDefersRegistryClosureUntilMaterializationCompletes() {
        SourceExtractors registry = new SourceExtractors();
        DriverContext driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);
        AtomicReference<Runnable> scheduled = new AtomicReference<>();
        ExternalFieldExtractOperator op = new ExternalFieldExtractOperator(
            1,
            List.of(0, 2),
            List.of("col"),
            List.of(DataType.INTEGER),
            registry,
            driverContext,
            null,
            command -> assertTrue(scheduled.compareAndSet(null, command))
        );
        try {
            int id = registry.register(new IntListExtractor(new int[] { 10 }));
            op.addInput(newPage(new long[] { 1 }, new long[] { SourceExtractors.encode(id, 0) }, new int[] { 2 }));

            op.close();
            assertEquals("the pending worker still owns the registry", 1, registry.size());

            Runnable task = scheduled.getAndSet(null);
            assertNotNull(task);
            task.run();
            assertFalse(driverContext.isFinished());
            assertEquals("the registry closes after the worker releases its materialization ref", 0, registry.size());
        } finally {
            driverContext.finish();
            registry.close();
        }
    }

    public void testExecutorRejectionReleasesRegistryRef() {
        SourceExtractors registry = new SourceExtractors();
        DriverContext driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);
        ExternalFieldExtractOperator op = new ExternalFieldExtractOperator(
            1,
            List.of(0, 2),
            List.of("col"),
            List.of(DataType.INTEGER),
            registry,
            driverContext,
            null,
            command -> {
                throw new RejectedExecutionException("simulated rejection");
            }
        );
        try {
            int id = registry.register(new IntListExtractor(new int[] { 10 }));
            op.addInput(newPage(new long[] { 1 }, new long[] { SourceExtractors.encode(id, 0) }, new int[] { 2 }));

            op.close();

            assertFalse(driverContext.isFinished());
            assertEquals("executor rejection must not retain the registry", 0, registry.size());
        } finally {
            driverContext.finish();
            registry.close();
        }
    }

    /**
     * Empty pages go through {@code reshapeEmpty()}, whose only breaker-checked allocation is
     * {@code newConstantNullBlock} per deferred column. A cranky breaker will eventually trip
     * there — including after the first placeholder has already been allocated — and must not
     * leak the input page or the pass-through refs already {@code incRef}'d. Input blocks are
     * built on the leak-tracking factory so construction itself cannot trip; the operator uses
     * the cranky factory. Leak detection is {@link ComputeTestCase}'s teardown plus a per-attempt
     * breaker check.
     */
    public void testReshapeEmptyWithCrankyBreakerDoesNotLeak() {
        BlockFactory cranky = crankyBlockFactory();
        for (int attempt = 0; attempt < 100; attempt++) {
            try (SourceExtractors registry = new SourceExtractors()) {
                registry.register(new IntListExtractor(new int[] { 1 }));
                // Two deferred columns so the breaker can trip after the first placeholder is live,
                // exercising reshapeEmpty's cleanup of a partially filled outBlocks array.
                Page empty = newPage(new long[0], new long[0], new int[0]);
                ExternalFieldExtractOperator op = new ExternalFieldExtractOperator(
                    1,
                    List.of(0, 2),
                    List.of("colA", "colB"),
                    List.of(DataType.INTEGER, DataType.INTEGER),
                    registry,
                    cranky,
                    null
                );
                op.addInput(empty);
                op.finish();
                try {
                    Page output = op.getOutput();
                    try {
                        assertEquals(4, output.getBlockCount());
                        assertEquals(0, output.getPositionCount());
                    } finally {
                        output.releaseBlocks();
                    }
                } catch (CircuitBreakingException e) {
                    assertEquals(CrankyCircuitBreakerService.ERROR_MESSAGE, e.getMessage());
                } finally {
                    op.close();
                }
            }
            assertEquals("breaker leaked on attempt " + attempt, 0L, cranky.breaker().getUsed());
        }
    }

    public void testFactoryRejectsNullsAndNegatives() {
        SourceExtractors registry = new SourceExtractors();
        Executor executor = Runnable::run;
        try {
            expectThrows(
                IllegalArgumentException.class,
                () -> new ExternalFieldExtractOperator.Factory(-1, List.of(), List.of(), List.of(), ctx -> registry, null, executor)
            );
            expectThrows(
                IllegalArgumentException.class,
                () -> new ExternalFieldExtractOperator.Factory(0, null, List.of(), List.of(), ctx -> registry, null, executor)
            );
            expectThrows(
                IllegalArgumentException.class,
                () -> new ExternalFieldExtractOperator.Factory(0, List.of(), null, List.of(), ctx -> registry, null, executor)
            );
            expectThrows(
                IllegalArgumentException.class,
                () -> new ExternalFieldExtractOperator.Factory(0, List.of(), List.of(), null, ctx -> registry, null, executor)
            );
            expectThrows(
                IllegalArgumentException.class,
                () -> new ExternalFieldExtractOperator.Factory(0, List.of(), List.of("col"), List.of(), ctx -> registry, null, executor)
            );
            expectThrows(
                IllegalArgumentException.class,
                () -> new ExternalFieldExtractOperator.Factory(0, List.of(), List.of(), List.of(), null, null, executor)
            );
            expectThrows(
                IllegalArgumentException.class,
                () -> new ExternalFieldExtractOperator.Factory(0, List.of(), List.of(), List.of(), ctx -> registry, null, null)
            );
        } finally {
            registry.close();
        }
    }

    public void testFactoryRejectsNullRegistryLookup() {
        ExternalFieldExtractOperator.Factory factory = new ExternalFieldExtractOperator.Factory(
            0,
            List.of(),
            List.of(),
            List.of(),
            ctx -> null,
            null,
            Runnable::run
        );
        DriverContext driverContext = mock(DriverContext.class);
        when(driverContext.blockFactory()).thenReturn(blockFactory);
        expectThrows(IllegalStateException.class, () -> factory.get(driverContext));
    }

    public void testCloseReleasesPendingPage() {
        try (SourceExtractors registry = new SourceExtractors()) {
            registry.register(new IntListExtractor(new int[] { 1 }));
            Page page = newPage(new long[] { 1L }, new long[] { SourceExtractors.encode(0, 0) }, new int[] { 9 });

            ExternalFieldExtractOperator op = new ExternalFieldExtractOperator(
                1,
                List.of(0, 2),
                List.of("col"),
                List.of(DataType.INTEGER),
                registry,
                blockFactory,
                null
            );
            op.addInput(page);
            // Don't drain; close must release the pending page so we don't leak blocks.
            op.close();
        }
    }

    /**
     * A failure inside {@code registry.materialize(...)} — the realistic trigger is a breaker
     * trip while allocating the deferred columns — must not leak the detached input page:
     * {@link ExternalFieldExtractOperator#getOutput()} owns the page and releases it on every
     * path, including {@link Error}s. Leak detection is {@link ComputeTestCase}'s teardown.
     */
    public void testMaterializeFailureReleasesPage() {
        CircuitBreakingException breaker = new CircuitBreakingException(
            "simulated breaker trip during extraction",
            CircuitBreaker.Durability.TRANSIENT
        );
        AssertionError error = new AssertionError("simulated error during extraction");
        for (Throwable failure : List.of(breaker, error)) {
            try (SourceExtractors registry = new SourceExtractors()) {
                int id = registry.register(new ThrowingExtractor(failure));
                Page page = newPage(new long[] { 1L }, new long[] { SourceExtractors.encode(id, 0) }, new int[] { 9 });

                ExternalFieldExtractOperator op = new ExternalFieldExtractOperator(
                    1,
                    List.of(0, 2),
                    List.of("col"),
                    List.of(DataType.INTEGER),
                    registry,
                    blockFactory,
                    null
                );
                op.addInput(page);
                try {
                    if (failure instanceof CircuitBreakingException) {
                        CircuitBreakingException thrown = expectThrows(CircuitBreakingException.class, op::getOutput);
                        assertSame(failure, thrown);
                        assertEquals(RestStatus.TOO_MANY_REQUESTS, ExceptionsHelper.status(thrown));
                    } else {
                        assertSame(failure, expectThrows(AssertionError.class, op::getOutput));
                    }
                } finally {
                    op.close();
                }
            }
        }
    }

    /**
     * A checked I/O failure during deferred extraction is an external-read failure, even though it
     * occurs after TopN on the driver thread. The first extractor successfully allocates its block
     * before the second fails, so leak tracking also verifies cleanup of partial registry output.
     */
    public void testIoFailureDuringMaterializationIsClassified() {
        IOException failure = new IOException("Access denied reading object hits.parquet");
        try (SourceExtractors registry = new SourceExtractors()) {
            int successfulId = registry.register(new IntListExtractor(new int[] { 10 }));
            int failingId = registry.register(new ThrowingExtractor(failure));
            Page page = newPage(
                new long[] { 1L, 2L },
                new long[] { SourceExtractors.encode(successfulId, 0), SourceExtractors.encode(failingId, 0) },
                new int[] { 9, 10 }
            );

            ExternalFieldExtractOperator op = new ExternalFieldExtractOperator(
                1,
                List.of(0, 2),
                List.of("col"),
                List.of(DataType.INTEGER),
                registry,
                blockFactory,
                null
            );
            op.addInput(page);
            op.finish();
            try {
                ExternalClientException thrown = expectThrows(ExternalClientException.class, op::getOutput);
                assertEquals(RestStatus.BAD_REQUEST, ExceptionsHelper.status(thrown));
                assertEquals("external_client_exception", ElasticsearchException.getExceptionName(thrown));
                assertTrue(thrown.getMessage().startsWith("Failed to read external source: "));
                assertTrue(thrown.getMessage().contains(failure.getMessage()));
                assertNull("the read failure must not be chained to prevent caused_by leaks", thrown.getCause());
            } finally {
                op.close();
            }
        }
    }

    /**
     * A storage-layer 503 already carries the retryable status. Classification at this operator
     * must leave it unchanged rather than re-wrapping it as a client or server exception.
     */
    public void testUnavailableExceptionDuringMaterializationStays503() {
        ExternalUnavailableException failure = new ExternalUnavailableException(
            Condition.STORE_UNAVAILABLE,
            StoragePath.NONE,
            "",
            "",
            false,
            0L,
            new IOException("connection reset")
        );
        try (SourceExtractors registry = new SourceExtractors()) {
            int id = registry.register(new ThrowingExtractor(failure));
            Page page = newPage(new long[] { 1L }, new long[] { SourceExtractors.encode(id, 0) }, new int[] { 9 });

            ExternalFieldExtractOperator op = new ExternalFieldExtractOperator(
                1,
                List.of(0, 2),
                List.of("col"),
                List.of(DataType.INTEGER),
                registry,
                blockFactory,
                null
            );
            op.addInput(page);
            op.finish();
            try {
                ExternalUnavailableException thrown = expectThrows(ExternalUnavailableException.class, op::getOutput);
                assertEquals(failure.getMessage(), thrown.getMessage());
                assertNull("the transport cause must not reach caused_by", thrown.getCause());
                assertEquals(RestStatus.SERVICE_UNAVAILABLE, ExceptionsHelper.status(thrown));
                assertEquals("external_unavailable_exception", ElasticsearchException.getExceptionName(thrown));
            } finally {
                op.close();
            }
        }
    }

    /**
     * Materializing against a closed registry is a broken invariant, not bad input. Classification
     * at this operator must turn that {@link IllegalStateException} into a 500.
     */
    public void testClosedRegistryDuringMaterializationIsServerException() {
        SourceExtractors registry = new SourceExtractors();
        DriverContext driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);
        AtomicReference<Runnable> scheduled = new AtomicReference<>();
        ExternalFieldExtractOperator op = new ExternalFieldExtractOperator(
            1,
            List.of(0, 2),
            List.of("col"),
            List.of(DataType.INTEGER),
            registry,
            driverContext,
            null,
            command -> assertTrue(scheduled.compareAndSet(null, command))
        );
        try {
            int id = registry.register(new IntListExtractor(new int[] { 10 }));
            Page page = newPage(new long[] { 1L }, new long[] { SourceExtractors.encode(id, 0) }, new int[] { 9 });
            op.addInput(page);
            op.finish();
            registry.close();
            scheduled.getAndSet(null).run();
            ExternalServerException thrown = expectThrows(ExternalServerException.class, op::getOutput);
            assertEquals(RestStatus.INTERNAL_SERVER_ERROR, ExceptionsHelper.status(thrown));
            assertEquals("external_server_exception", ElasticsearchException.getExceptionName(thrown));
            assertNull(thrown.getCause());
            assertTrue(thrown.getMessage().contains("SourceExtractors is closed"));
        } finally {
            op.close();
            driverContext.finish();
            registry.close();
        }
    }

    /**
     * The {@code _rowPosition} type check throws before materialization even starts; the
     * detached input page must still be released by {@code getOutput()}.
     */
    public void testBadRowPositionChannelReleasesPage() {
        try (SourceExtractors registry = new SourceExtractors()) {
            registry.register(new IntListExtractor(new int[] { 1 }));
            // The _rowPosition channel (1) holds ints instead of the encoded longs the operator requires.
            Block sortBlock = blockFactory.newLongArrayVector(new long[] { 1L }, 1).asBlock();
            Block badRpBlock = blockFactory.newIntArrayVector(new int[] { 0 }, 1).asBlock();
            Block passBlock = blockFactory.newIntArrayVector(new int[] { 9 }, 1).asBlock();
            Page page = new Page(1, sortBlock, badRpBlock, passBlock);

            ExternalFieldExtractOperator op = new ExternalFieldExtractOperator(
                1,
                List.of(0, 2),
                List.of("col"),
                List.of(DataType.INTEGER),
                registry,
                blockFactory,
                null
            );
            op.addInput(page);
            expectThrows(IllegalStateException.class, op::getOutput);
            op.close();
        }
    }

    private Page newPage(long[] sortKey, long[] rowPosition, int[] passThru) {
        assert sortKey.length == rowPosition.length && rowPosition.length == passThru.length;
        int n = sortKey.length;
        Block sortBlock = blockFactory.newLongArrayVector(sortKey, n).asBlock();
        Block rpBlock = blockFactory.newLongArrayVector(rowPosition, n).asBlock();
        Block passBlock = blockFactory.newIntArrayVector(passThru, n).asBlock();
        return new Page(n, sortBlock, rpBlock, passBlock);
    }

    /** Same minimal in-memory column extractor used in {@link SourceExtractorsTests}. */
    private static final class IntListExtractor implements ColumnExtractor {
        private final int[] values;

        IntListExtractor(int[] values) {
            this.values = values;
        }

        @Override
        public long rowCount() {
            return values.length;
        }

        @Override
        public Block[] extract(String[] columnNames, DataType[] targetTypes, long[] localPositions, BlockFactory factory)
            throws IOException {
            Block[] result = new Block[columnNames.length];
            boolean built = false;
            try {
                for (int c = 0; c < columnNames.length; c++) {
                    try (IntBlock.Builder builder = factory.newIntBlockBuilder(localPositions.length)) {
                        for (long pos : localPositions) {
                            builder.appendInt(values[Math.toIntExact(pos)]);
                        }
                        result[c] = builder.build();
                    }
                }
                built = true;
                return result;
            } finally {
                if (built == false) org.elasticsearch.core.Releasables.closeExpectNoException(result);
            }
        }

        @Override
        public void close() {}
    }

    private static final class SquaredPositionExtractor implements ColumnExtractor {
        @Override
        public long rowCount() {
            return Integer.MAX_VALUE;
        }

        @Override
        public Block[] extract(String[] columnNames, DataType[] targetTypes, long[] localPositions, BlockFactory factory) {
            Block[] result = new Block[columnNames.length];
            boolean built = false;
            try {
                for (int c = 0; c < columnNames.length; c++) {
                    try (IntBlock.Builder builder = factory.newIntBlockBuilder(localPositions.length)) {
                        for (long position : localPositions) {
                            int value = Math.toIntExact(position);
                            builder.appendInt(value * value);
                        }
                        result[c] = builder.build();
                    }
                }
                built = true;
                return result;
            } finally {
                if (built == false) {
                    org.elasticsearch.core.Releasables.closeExpectNoException(result);
                }
            }
        }

        @Override
        public void close() {}
    }

    /**
     * Extractor that allocates nothing and rethrows a supplied checked exception, runtime
     * exception, or error unchanged.
     */
    private static final class ThrowingExtractor implements ColumnExtractor {
        private final Throwable failure;

        ThrowingExtractor(Throwable failure) {
            this.failure = failure;
        }

        @Override
        public long rowCount() {
            return 1;
        }

        @Override
        public Block[] extract(String[] columnNames, DataType[] targetTypes, long[] localPositions, BlockFactory factory)
            throws IOException {
            if (failure instanceof IOException ioException) {
                throw ioException;
            }
            if (failure instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            if (failure instanceof Error error) {
                throw error;
            }
            throw new AssertionError("unsupported test failure", failure);
        }

        @Override
        public void close() {}
    }

    /**
     * The early-termination check runs on the read executor, so its exception travels as a failed
     * {@link ExternalFieldExtractOperator.Result} and is rethrown from {@code getOutput()} on the driver thread.
     * The Driver only winds down cleanly if it sees the exact {@link DriverEarlyTerminationException} (LIMIT
     * reached, exchange sink closed) or {@link TaskCancelledException}; wrapping either would fail the query.
     * The extractor throws if called, so this also proves the check runs before materialization.
     */
    public void testEarlyTerminationOnExecutorReachesDriverUnchanged() {
        RuntimeException termination = randomBoolean()
            ? new DriverEarlyTerminationException("exchange sink is closed")
            : new TaskCancelledException("cancelled");
        DriverContext driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);
        driverContext.initializeEarlyTerminationChecker(() -> { throw termination; });
        SourceExtractors registry = new SourceExtractors();
        int id = registry.register(new ThrowingExtractor(new AssertionError("materialize must not run after early termination")));
        try (
            ExternalFieldExtractOperator op = new ExternalFieldExtractOperator(
                1,
                List.of(0, 2),
                List.of("col"),
                List.of(DataType.INTEGER),
                registry,
                driverContext,
                null,
                Runnable::run
            )
        ) {
            op.addInput(newPage(new long[] { 1 }, new long[] { SourceExtractors.encode(id, 0) }, new int[] { 2 }));
            RuntimeException thrown = expectThrows(RuntimeException.class, op::getOutput);
            assertSame(termination, thrown);
        } finally {
            driverContext.finish();
        }
        assertEquals(0, registry.size());
    }

    /**
     * An executor rejection must fail the operator, not just release the registry ref: the rejection has to
     * surface from {@code getOutput()} so the driver fails instead of silently producing no rows.
     */
    public void testExecutorRejectionSurfacesFromGetOutput() {
        SourceExtractors registry = new SourceExtractors();
        DriverContext driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);
        EsRejectedExecutionException rejection = new EsRejectedExecutionException("simulated rejection");
        try (
            ExternalFieldExtractOperator op = new ExternalFieldExtractOperator(
                1,
                List.of(0, 2),
                List.of("col"),
                List.of(DataType.INTEGER),
                registry,
                driverContext,
                null,
                command -> {
                    throw rejection;
                }
            )
        ) {
            int id = registry.register(new IntListExtractor(new int[] { 10 }));
            op.addInput(newPage(new long[] { 1 }, new long[] { SourceExtractors.encode(id, 0) }, new int[] { 2 }));
            assertSame(rejection, expectThrows(EsRejectedExecutionException.class, op::getOutput));
        } finally {
            driverContext.finish();
        }
        assertEquals(0, registry.size());
    }
}
