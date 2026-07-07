/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.DriverRunner;
import org.elasticsearch.compute.operator.PageConsumerOperator;
import org.elasticsearch.compute.test.TestDriverFactory;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.NoConfigFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.PassThroughRowPositionStrategy;
import org.elasticsearch.xpack.esql.datasources.spi.RowPositionStrategy;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;

import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Regression tests for the multi-file text-read producer/consumer deadlock (elastic/elasticsearch#152771 introduced,
 * the streaming-parallel-parse drain wedge). Both cover {@link AsyncExternalSourceOperatorFactory}'s producer-loop
 * drain, driven through a real {@link Driver} on a bounded producer pool.
 *
 * <ul>
 *   <li>{@link #testDrainDoesNotPinPoolThreadInBlockingHasNext} — the DEADLOCK test. The drain must pull pages via
 *       the non-blocking {@link CloseableIterator#pollNext()} contract and never enter a blocking
 *       {@link CloseableIterator#hasNext()} on a pool thread. With the pre-fix drain (which called {@code hasNext()}
 *       after {@code waitForReady().isDone()}), the bounded producer pool's threads pin inside {@code hasNext()} and
 *       the run never completes — the test times out. With the fix it completes.</li>
 *   <li>{@link #testDrainDeliversEveryPageAcrossAMidStreamGap} — the EOF-DROP test. A {@code pollNext()==null} paired
 *       with a done {@code waitForReady()} in the MIDDLE of a stream (a POISON/end-of-chunk marker consumed, next
 *       chunk still parsing) must NOT be read as EOF. The drain must keep going until {@link CloseableIterator#isExhausted()}
 *       is true, so every page is delivered. A pre-fix drain that concluded EOF on the done ready-signal drops the
 *       trailing page(s) — partial results, no error.</li>
 * </ul>
 */
public class AsyncExternalSourceDrainDeadlockTests extends ESTestCase {

    private static final BlockFactory TEST_BLOCK_FACTORY = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(new NoopCircuitBreaker("none"))
        .build();

    private static final String DRIVER_POOL_NAME = "test-driver";

    private ThreadPool driverThreadPool;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        driverThreadPool = new TestThreadPool(
            "drain-deadlock-tests",
            new FixedExecutorBuilder(
                Settings.EMPTY,
                DRIVER_POOL_NAME,
                4,
                1024,
                "drain-deadlock-tests." + DRIVER_POOL_NAME,
                EsExecutors.TaskTrackingConfig.DEFAULT
            )
        );
    }

    @Override
    public void tearDown() throws Exception {
        terminate(driverThreadPool);
        super.tearDown();
    }

    /**
     * DEADLOCK test. {@code k} producer-pool threads run the drain; {@code >= k+1} concurrent single-file units each
     * present an iterator whose {@code hasNext()} blocks forever (the wedged latch) but whose non-blocking
     * {@code pollNext()} / {@code waitForReady()} / {@code isExhausted()} contract is correct. A drain that ever calls
     * {@code hasNext()} pins its pool thread; {@code k} such pins exhaust the pool and the run cannot complete. The
     * fix drains via {@code pollNext()} and never touches {@code hasNext()}, so it completes.
     */
    public void testDrainDoesNotPinPoolThreadInBlockingHasNext() throws Exception {
        int poolThreads = 2;   // "2k" with k=1 — the single production pool backing both executor + producerExecutor
        int units = poolThreads + 1;
        // The latch every unit's hasNext() blocks on. Never released while the test runs — a blocking hasNext on the
        // producer pool would hang the whole run. Interrupted only at teardown to unwedge a pre-fix run.
        CountDownLatch wedgedHasNext = new CountDownLatch(1);
        ExecutorService producerExec = Executors.newFixedThreadPool(
            poolThreads,
            EsExecutors.daemonThreadFactory("test", "drain-deadlock-producer")
        );
        try {
            AtomicInteger totalPages = new AtomicInteger();
            List<Driver> drivers = new ArrayList<>(units);
            for (int u = 0; u < units; u++) {
                DriverContext ctx = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, TEST_BLOCK_FACTORY, null);
                // producerExecutor defaults to the builder's executor when not set — i.e. the SINGLE production
                // topology where drain + read/parse share one pool. That is exactly the wiring the deadlock needs.
                ExternalSliceQueue sliceQueue = new ExternalSliceQueue(
                    List.of(new FileSplit("test", StoragePath.of("s3://bucket/u" + u + ".txt"), 0, 100, "text", Map.of(), Map.of()))
                );
                AsyncExternalSourceOperatorFactory factory = AsyncExternalSourceOperatorFactory.builder(
                    new StubStorageProvider(),
                    new SinglecornerFormatReader(() -> new WedgedHasNextIterator(wedgedHasNext)),
                    StoragePath.of("s3://bucket/u" + u + ".txt"),
                    singleIntAttribute(),
                    100,
                    1,
                    producerExec
                ).sliceQueue(sliceQueue).build();
                drivers.add(TestDriverFactory.create(ctx, factory.get(ctx), List.of(), new PageConsumerOperator(page -> {
                    totalPages.incrementAndGet();
                    page.releaseBlocks();
                })));
            }

            boolean completed = runDrivers(drivers, TimeValue.timeValueSeconds(15));
            assertTrue("producer-loop drain wedged: a bounded pool thread pinned in a blocking hasNext()", completed);
            assertEquals("every unit must deliver its single page", units, totalPages.get());
        } finally {
            wedgedHasNext.countDown();
            producerExec.shutdownNow();
            assertTrue(producerExec.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    /**
     * EOF-DROP test. One unit whose iterator yields page1, then a single {@code pollNext()==null} with an
     * immediately-done {@code waitForReady()} and {@code isExhausted()==false} (a consumed POISON / mid-stream gap),
     * then page2, then genuine exhaustion. The drain must deliver BOTH pages — concluding EOF on the mid-stream done
     * ready-signal would silently drop page2.
     */
    public void testDrainDeliversEveryPageAcrossAMidStreamGap() throws Exception {
        ExecutorService producerExec = Executors.newFixedThreadPool(2, EsExecutors.daemonThreadFactory("test", "drain-eof-producer"));
        try {
            ExternalSliceQueue sliceQueue = new ExternalSliceQueue(
                List.of(new FileSplit("test", StoragePath.of("s3://bucket/gap.txt"), 0, 100, "text", Map.of(), Map.of()))
            );
            DriverContext ctx = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, TEST_BLOCK_FACTORY, null);
            AsyncExternalSourceOperatorFactory factory = AsyncExternalSourceOperatorFactory.builder(
                new StubStorageProvider(),
                new SinglecornerFormatReader(GapAtMiddleIterator::new),
                StoragePath.of("s3://bucket/gap.txt"),
                singleIntAttribute(),
                100,
                4,
                producerExec
            ).sliceQueue(sliceQueue).build();

            AtomicInteger pageCount = new AtomicInteger();
            Driver driver = TestDriverFactory.create(ctx, factory.get(ctx), List.of(), new PageConsumerOperator(page -> {
                pageCount.incrementAndGet();
                page.releaseBlocks();
            }));

            boolean completed = runDrivers(List.of(driver), TimeValue.timeValueSeconds(15));
            assertTrue("drain did not complete", completed);
            assertEquals("both pages must be delivered — no silent drop at a mid-stream ready-but-empty gap", 2, pageCount.get());
        } finally {
            producerExec.shutdownNow();
            assertTrue(producerExec.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    /** Runs the drivers to completion with a bounded wait. Returns {@code true} on completion, {@code false} on timeout. */
    private boolean runDrivers(List<Driver> drivers, TimeValue timeout) {
        DriverRunner runner = new DriverRunner(driverThreadPool.getThreadContext()) {
            @Override
            protected void start(Driver driver, ActionListener<Void> driverListener) {
                Driver.start(
                    driverThreadPool.getThreadContext(),
                    driverThreadPool.executor(DRIVER_POOL_NAME),
                    driver,
                    between(1, 10_000),
                    driverListener
                );
            }
        };
        PlainActionFuture<Void> future = new PlainActionFuture<>();
        runner.runToCompletion(drivers, future);
        try {
            future.actionGet(timeout);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private static List<Attribute> singleIntAttribute() {
        return List.of(
            new FieldAttribute(
                Source.EMPTY,
                "value",
                new EsField("value", DataType.INTEGER, Map.of(), false, EsField.TimeSeriesFieldType.NONE)
            )
        );
    }

    private static Page smallIntPage() {
        var builder = TEST_BLOCK_FACTORY.newIntBlockBuilder(1);
        builder.appendInt(1);
        IntBlock block = builder.build();
        return new Page(block);
    }

    /** {@link FormatReader} that returns a caller-supplied fake iterator for every read. */
    private static final class SinglecornerFormatReader implements NoConfigFormatReader {
        private final java.util.function.Supplier<CloseableIterator<Page>> iteratorSupplier;

        SinglecornerFormatReader(java.util.function.Supplier<CloseableIterator<Page>> iteratorSupplier) {
            this.iteratorSupplier = iteratorSupplier;
        }

        @Override
        public RowPositionStrategy rowPositionStrategy() {
            return PassThroughRowPositionStrategy.INSTANCE;
        }

        @Override
        public SourceMetadata metadata(StorageObject object) {
            return null;
        }

        @Override
        public CloseableIterator<Page> read(StorageObject object, FormatReadContext context) {
            return iteratorSupplier.get();
        }

        @Override
        public String formatName() {
            return "test-single-corner";
        }

        @Override
        public List<String> fileExtensions() {
            return List.of(".txt");
        }

        @Override
        public void close() {}
    }

    /**
     * Emits one page. {@link #hasNext()} BLOCKS forever on the shared latch (the wedged-latch signature from the
     * ticket's jstack), while {@link #pollNext()} / {@link #waitForReady()} / {@link #isExhausted()} are correct and
     * non-blocking. A drain that ever calls {@code hasNext()} pins its pool thread; a poll-based drain sails through.
     */
    private static final class WedgedHasNextIterator implements CloseableIterator<Page> {
        private final CountDownLatch wedged;
        private boolean delivered = false;

        WedgedHasNextIterator(CountDownLatch wedged) {
            this.wedged = wedged;
        }

        @Override
        public boolean hasNext() {
            // The bug: a drain that reaches hasNext() blocks here on a pool thread and never returns.
            try {
                wedged.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
            return delivered == false;
        }

        @Override
        public Page next() {
            if (delivered) {
                throw new NoSuchElementException();
            }
            delivered = true;
            return smallIntPage();
        }

        @Override
        public Page pollNext() {
            if (delivered) {
                return null;
            }
            delivered = true;
            return smallIntPage();
        }

        @Override
        public boolean isExhausted() {
            return delivered;
        }

        @Override
        public SubscribableListener<Void> waitForReady() {
            return SubscribableListener.newSucceeded(null);
        }

        @Override
        public void close() {}
    }

    /**
     * Emits page1, then a single {@code pollNext()==null} gap with a done {@code waitForReady()} and
     * {@code isExhausted()==false}, then page2, then exhaustion. Models a consumed POISON / mid-stream chunk boundary.
     * {@code hasNext()}/{@code next()} exist so a pre-fix (hasNext-based) drain drops page2 by concluding EOF at the gap.
     */
    private static final class GapAtMiddleIterator implements CloseableIterator<Page> {
        // 0=page1 available, 1=gap (one null), 2=page2 available, 3=exhausted
        private int state = 0;

        @Override
        public boolean hasNext() {
            return state == 0 || state == 2;
        }

        @Override
        public Page next() {
            if (state == 0) {
                state = 1;
                return smallIntPage();
            }
            if (state == 2) {
                state = 3;
                return smallIntPage();
            }
            throw new NoSuchElementException();
        }

        @Override
        public Page pollNext() {
            switch (state) {
                case 0 -> {
                    state = 1;
                    return smallIntPage();
                }
                case 1 -> {
                    // The trap: no page right now, but NOT exhausted; waitForReady is done (POISON consumed).
                    state = 2;
                    return null;
                }
                case 2 -> {
                    state = 3;
                    return smallIntPage();
                }
                default -> {
                    return null;
                }
            }
        }

        @Override
        public boolean isExhausted() {
            return state == 3;
        }

        @Override
        public SubscribableListener<Void> waitForReady() {
            // Always "ready" — the exact condition that must NOT be read as EOF at the mid-stream gap.
            return SubscribableListener.newSucceeded(null);
        }

        @Override
        public void close() {}
    }

    private static final class StubStorageProvider implements StorageProvider {
        @Override
        public StorageObject newObject(StoragePath path) {
            return new StubStorageObject(path);
        }

        @Override
        public StorageObject newObject(StoragePath path, long length) {
            return new StubStorageObject(path);
        }

        @Override
        public StorageObject newObject(StoragePath path, long length, Instant lastModified) {
            return new StubStorageObject(path);
        }

        @Override
        public StorageIterator listObjects(StoragePath prefix, boolean recursive) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean exists(StoragePath path) {
            return true;
        }

        @Override
        public List<String> supportedSchemes() {
            return List.of("s3");
        }

        @Override
        public void close() {}
    }

    private record StubStorageObject(StoragePath path) implements StorageObject {
        @Override
        public InputStream newStream() {
            return InputStream.nullInputStream();
        }

        @Override
        public InputStream newStream(long position, long length) {
            return InputStream.nullInputStream();
        }

        @Override
        public long length() {
            return 0;
        }

        @Override
        public Instant lastModified() {
            return Instant.EPOCH;
        }

        @Override
        public boolean exists() {
            return true;
        }
    }
}
