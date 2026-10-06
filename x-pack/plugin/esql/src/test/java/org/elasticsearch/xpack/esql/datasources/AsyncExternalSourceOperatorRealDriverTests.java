/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
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
import org.elasticsearch.compute.operator.DriverStatus;
import org.elasticsearch.compute.operator.LimitOperator;
import org.elasticsearch.compute.operator.OperatorStatus;
import org.elasticsearch.compute.operator.PageConsumerOperator;
import org.elasticsearch.compute.operator.SourceOperator;
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
import org.elasticsearch.xpack.esql.datasources.spi.AbstractTestStorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.NoConfigFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.PassThroughRowPositionStrategy;
import org.elasticsearch.xpack.esql.datasources.spi.RowPositionStrategy;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.StorageChildren;
import org.elasticsearch.xpack.esql.datasources.spi.StorageIdentity;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObjectMetrics;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObjectMetricsCounters;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;
import org.junit.After;
import org.junit.Before;

import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

/**
 * End-to-end tests that drive {@link AsyncExternalSourceOperator} through a real
 * {@link Driver} + {@link DriverRunner}, so the consumer actually parks on the
 * buffer's {@code waitForReading()} listener rather than busy-polling
 * {@link SourceOperator#getOutput()} from a test thread.
 *
 * <p>This closes the operator-layer gap left by
 * {@code AsyncExternalSourceOperatorFactoryTests}: those tests poll {@code getOutput} from
 * a plain thread with {@code Thread.sleep(1)} retries and therefore never exercise the
 * driver-parking code path that the lost-wakeup fix in
 * {@code AsyncExternalSourceBuffer} guards. The buffer-layer regression test
 * {@code AsyncExternalSourceBufferTests#testNoLostWakeupUnderConcurrentAddAndPoll}
 * covers the buffer in isolation; this test class covers the integration with a
 * real Driver where the consumer's blocked-status comes from
 * {@code AsyncExternalSourceOperator#isBlocked()} and the driver yields its
 * executor slot (recorded as {@code DriverStatus.Status.ASYNC} sleep) until the listener
 * fires.
 */
public class AsyncExternalSourceOperatorRealDriverTests extends ESTestCase {

    private static final BlockFactory TEST_BLOCK_FACTORY = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(new NoopCircuitBreaker("none"))
        .build();

    private static final String DRIVER_POOL_NAME = "test-driver";

    private ThreadPool driverThreadPool;
    private ExecutorService producerExec;

    @Before
    public void startExecutors() {
        driverThreadPool = new TestThreadPool(
            "real-driver-tests",
            new FixedExecutorBuilder(
                Settings.EMPTY,
                DRIVER_POOL_NAME,
                between(1, 4),
                1024,
                "real-driver-tests." + DRIVER_POOL_NAME,
                EsExecutors.TaskTrackingConfig.DEFAULT
            )
        );
        producerExec = Executors.newFixedThreadPool(2, EsExecutors.daemonThreadFactory("test", "real-driver-producer"));
    }

    @After
    public void stopExecutors() throws Exception {
        try {
            producerExec.shutdownNow();
            assertTrue(producerExec.awaitTermination(10, TimeUnit.SECONDS));
        } finally {
            terminate(driverThreadPool);
        }
    }

    /**
     * Single Driver, slow producer (sleep on each {@code next()}), small buffer ({@code bufferSize=1}),
     * pages large enough that two of them exceed buffer capacity. The Driver's consumer thread
     * must park on the buffer's empty-listener between pages — proven by a non-zero
     * {@code ASYNC} sleep count (driver records {@code ASYNC} for {@code isBlocked()} waits) with
     * the buffer's reason on the driver status after completion. A pre-fix buffer that lost the
     * empty-wakeup would hang here.
     */
    public void testRealDriverParksOnEmptyBuffer() throws Exception {
        int totalPages = 50;
        int bufferSize = 1;
        long producerSleepMillis = 5L;
        int pagesPerSplit = totalPages;

        AtomicInteger readCount = new AtomicInteger();
        FormatReader formatReader = new SlowMultiPageFormatReader(readCount, pagesPerSplit, producerSleepMillis);

        ExternalSliceQueue sliceQueue = new ExternalSliceQueue(
            List.of(new FileSplit("test", StoragePath.of("s3://bucket/slow.parquet"), 0, 100, "parquet", Map.of(), Map.of()))
        );

        DriverContext driverContext = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, TEST_BLOCK_FACTORY, null);
        AsyncExternalSourceOperatorFactory factory = AsyncExternalSourceOperatorFactory.builder(
            new StubStorageProvider(),
            formatReader,
            StoragePath.of("s3://bucket/slow.parquet"),
            singleIntAttribute(),
            100,
            bufferSize,
            producerExec
        ).sliceQueue(sliceQueue).build();

        AtomicInteger pageCount = new AtomicInteger();
        Driver driver = TestDriverFactory.create(driverContext, factory.get(driverContext), List.of(), new PageConsumerOperator(page -> {
            pageCount.incrementAndGet();
            page.releaseBlocks();
        }));

        runDriver(driver);

        assertEquals("expected one slice read", 1, readCount.get());
        assertEquals("driver must drain every page", totalPages, pageCount.get());
        assertDriverParkedOnEmptyBuffer(driver);
    }

    /**
     * Four real Drivers sharing one {@link ExternalSliceQueue}, each with its own
     * {@link AsyncExternalSourceOperator}, distributing 20 splits with {@code pagesPerSplit=10}.
     * The consumer threads are real driver executor slots that park on
     * {@link AsyncExternalSourceBuffer#waitForReading()} when their per-driver buffer is empty.
     * The total page count must equal {@code splits * pagesPerSplit}; no split may be lost or
     * double-read across drivers.
     */
    public void testMultiDriverInterleavedRealDriver() throws Exception {
        int driverCount = 4;
        int splits = 20;
        int pagesPerSplit = 10;
        int bufferSize = 1;

        List<FileSplit> queueEntries = new ArrayList<>();
        for (int i = 0; i < splits; i++) {
            queueEntries.add(
                new FileSplit("test", StoragePath.of("s3://bucket/rg" + i + ".parquet"), 0, 100, "parquet", Map.of(), Map.of())
            );
        }
        ExternalSliceQueue sliceQueue = new ExternalSliceQueue(List.copyOf(queueEntries));

        AtomicInteger readCount = new AtomicInteger();
        FormatReader formatReader = new SlowMultiPageFormatReader(readCount, pagesPerSplit, 1L);
        StubStorageProvider storageProvider = new StubStorageProvider();

        AtomicInteger pageCount = new AtomicInteger();
        List<Driver> drivers = new ArrayList<>(driverCount);
        for (int d = 0; d < driverCount; d++) {
            DriverContext ctx = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, TEST_BLOCK_FACTORY, null);
            AsyncExternalSourceOperatorFactory factory = AsyncExternalSourceOperatorFactory.builder(
                storageProvider,
                formatReader,
                StoragePath.of("s3://bucket/rg0.parquet"),
                singleIntAttribute(),
                100,
                bufferSize,
                producerExec
            ).sliceQueue(sliceQueue).build();
            drivers.add(TestDriverFactory.create(ctx, factory.get(ctx), List.of(), new PageConsumerOperator(page -> {
                pageCount.incrementAndGet();
                page.releaseBlocks();
            })));
        }

        runDrivers(drivers);

        assertEquals("every split must be read exactly once", splits, readCount.get());
        assertEquals("total pages must be splits * pagesPerSplit", splits * pagesPerSplit, pageCount.get());
    }

    /**
     * Two real drivers share one source factory and a {@link LimitOperator.Factory} of 1. Each
     * producer opens a split, emits a one-row page, then parks in the second {@code next()} so
     * {@code Driver.closeEarlyFinishedOperators} copies {@code status()} while the live bytes
     * view still includes the in-flight object. A snapshot taken only after producer completion
     * would report {@code bytes_read=0}.
     */
    public void testMultiDriverBytesReadVisibleWhenLimitClosesBeforeSnapshot() throws Exception {
        final long openBytes = 1000L;
        final long pageBytes = 4000L;
        final long expectedPerDriver = openBytes + pageBytes;

        CountDownLatch parkedOnSecondNext = new CountDownLatch(2);
        AtomicBoolean abandoned = new AtomicBoolean();
        CountingStorageProvider storageProvider = new CountingStorageProvider();
        FormatReader formatReader = new LimitCloseParkFormatReader(storageProvider, parkedOnSecondNext, abandoned, openBytes, pageBytes);

        ExternalSliceQueue sliceQueue = new ExternalSliceQueue(
            List.of(
                new FileSplit("test", StoragePath.of("s3://bucket/a.parquet"), 0, 100, "parquet", Map.of(), Map.of()),
                new FileSplit("test", StoragePath.of("s3://bucket/b.parquet"), 0, 100, "parquet", Map.of(), Map.of())
            )
        );

        AsyncExternalSourceOperatorFactory factory = AsyncExternalSourceOperatorFactory.builder(
            storageProvider,
            formatReader,
            StoragePath.of("s3://bucket/a.parquet"),
            singleIntAttribute(),
            100,
            1,
            producerExec
        ).sliceQueue(sliceQueue).build();

        LimitOperator.Factory limitFactory = new LimitOperator.Factory(1);
        DriverContext ctx1 = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, TEST_BLOCK_FACTORY, null);
        DriverContext ctx2 = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, TEST_BLOCK_FACTORY, null);
        SourceOperator source1 = factory.get(ctx1);
        SourceOperator source2 = factory.get(ctx2);

        boolean completed = false;
        try {
            assertTrue("both producers must park in the second next() before drivers run", parkedOnSecondNext.await(30, TimeUnit.SECONDS));

            List<Driver> drivers = List.of(
                TestDriverFactory.create(ctx1, source1, List.of(limitFactory.get(ctx1)), new PageConsumerOperator(Page::releaseBlocks)),
                TestDriverFactory.create(ctx2, source2, List.of(limitFactory.get(ctx2)), new PageConsumerOperator(Page::releaseBlocks))
            );
            runDrivers(drivers);

            long sum = 0L;
            for (Driver driver : drivers) {
                boolean found = false;
                for (OperatorStatus op : driver.profile().operators()) {
                    if (op.operator().equals("ExternalDataSourceOperator")) {
                        assertEquals(
                            "LIMIT close must copy live bytesRead before the producer snapshot",
                            expectedPerDriver,
                            op.bytesRead()
                        );
                        sum += op.bytesRead();
                        found = true;
                    }
                }
                assertTrue("driver profile must include ExternalDataSourceOperator: " + driver.profile().operators(), found);
            }
            assertEquals(2L * expectedPerDriver, sum);
            completed = true;
        } finally {
            if (completed == false) {
                abandoned.set(true);
            }
        }
    }

    /**
     * Asserts the driver parked at least once on the external-source buffer's empty-listener.
     * The driver records {@link DriverStatus.Status#ASYNC} (not {@code WAITING}) for
     * {@code isBlocked()} returns, and the sleep reason is the string registered by
     * {@link AsyncExternalSourceBuffer#waitForReading()} on {@code IsBlockedResult}; any
     * change to that reason string must update this assertion.
     */
    private static void assertDriverParkedOnEmptyBuffer(Driver driver) {
        DriverStatus status = driver.status();
        Long waits = status.sleeps().counts().get("async external source buffer empty");
        assertNotNull("driver must have recorded at least one async-source park; sleeps were " + status.sleeps().counts(), waits);
    }

    private void runDriver(Driver driver) {
        runDrivers(List.of(driver));
    }

    private void runDrivers(List<Driver> drivers) {
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
        future.actionGet(TimeValue.timeValueSeconds(30));
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

    /**
     * Builds a page with enough rows that {@link Page#ramBytesUsedByBlocks()} exceeds
     * {@link org.elasticsearch.compute.operator.Operator#TARGET_PAGE_SIZE} (256 KiB). This
     * matters because the buffer's effective capacity is {@code bufferSize * TARGET_PAGE_SIZE}
     * bytes — with tiny pages, {@code bufferSize=1} would still hold thousands of pages and
     * the consumer would never observe an empty buffer.
     */
    private static Page createTestPage() {
        // 96 KiB worth of int positions guarantees each page exceeds TARGET_PAGE_SIZE once block
        // overhead is included (and even without overhead, three such pages overflow a
        // bufferSize=1 buffer).
        int positions = 96 * 1024;
        var builder = TEST_BLOCK_FACTORY.newIntBlockBuilder(positions);
        for (int i = 0; i < positions; i++) {
            builder.appendInt(i);
        }
        IntBlock block = builder.build();
        return new Page(block);
    }

    /** One-row page so {@code bufferSize=1} still has room after LIMIT 1 accepts a single row. */
    private static Page createOneRowPage() {
        var builder = TEST_BLOCK_FACTORY.newIntBlockBuilder(1);
        builder.appendInt(1);
        return new Page(builder.build());
    }

    /**
     * Format reader that returns a fixed number of pages per split, sleeping between each
     * {@code next()} call so the consumer Driver is forced to park on
     * {@link AsyncExternalSourceBuffer#waitForReading()} between pages.
     */
    private static class SlowMultiPageFormatReader implements NoConfigFormatReader {
        @Override
        public RowPositionStrategy rowPositionStrategy() {
            return PassThroughRowPositionStrategy.INSTANCE;
        }

        private final AtomicInteger readCount;
        private final int pagesPerRead;
        private final long sleepMillis;

        SlowMultiPageFormatReader(AtomicInteger readCount, int pagesPerRead, long sleepMillis) {
            this.readCount = readCount;
            this.pagesPerRead = pagesPerRead;
            this.sleepMillis = sleepMillis;
        }

        @Override
        public SourceMetadata metadata(StorageObject object) {
            return null;
        }

        @Override
        public CloseableIterator<Page> read(StorageObject object, FormatReadContext context) {
            readCount.incrementAndGet();
            return new CloseableIterator<>() {
                private int remaining = pagesPerRead;

                @Override
                public boolean hasNext() {
                    return remaining > 0;
                }

                @Override
                public Page next() {
                    if (remaining <= 0) {
                        throw new NoSuchElementException();
                    }
                    remaining--;
                    if (sleepMillis > 0) {
                        try {
                            Thread.sleep(sleepMillis);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new RuntimeException(e);
                        }
                    }
                    return createTestPage();
                }

                @Override
                public void close() {}
            };
        }

        @Override
        public String formatName() {
            return "test-slow-multi-page";
        }

        @Override
        public List<String> fileExtensions() {
            return List.of(".parquet");
        }

        @Override
        public void close() {}
    }

    /**
     * Minimal {@link StorageProvider} that returns trivial empty-stream objects. The reader
     * stubs above ignore the storage object entirely, so we only need a stable instance to
     * satisfy the operator's storage lookups.
     */
    private static class StubStorageProvider implements StorageProvider {
        @Override
        public StorageChildren listChildren(StoragePath prefix, int limit) {
            return null; // directory-aware listing is irrelevant to this test double
        }

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

        @Override
        public StorageIdentity storageIdentity() {
            return AbstractTestStorageObject.NOOP;
        }
    }

    /**
     * Reuses one counting object per path so {@link RangeStorageObject} wrapping still reports
     * the same {@link StorageObject#metrics()} the reader increments via {@code path()}.
     */
    private static class CountingStorageProvider extends StubStorageProvider {
        private final ConcurrentHashMap<StoragePath, CountingStorageObject> objects = new ConcurrentHashMap<>();

        CountingStorageObject objectFor(StoragePath path) {
            return objects.computeIfAbsent(path, CountingStorageObject::new);
        }

        @Override
        public StorageObject newObject(StoragePath path) {
            return objectFor(path);
        }

        @Override
        public StorageObject newObject(StoragePath path, long length) {
            return objectFor(path);
        }

        @Override
        public StorageObject newObject(StoragePath path, long length, Instant lastModified) {
            return objectFor(path);
        }
    }

    private static final class CountingStorageObject extends AbstractTestStorageObject {
        private final StoragePath path;
        private final StorageObjectMetricsCounters counters = new StorageObjectMetricsCounters();

        CountingStorageObject(StoragePath path) {
            this.path = path;
        }

        @Override
        public StoragePath path() {
            return path;
        }

        @Override
        public StorageObjectMetrics metrics() {
            return counters.snapshot();
        }

        void addRequest(long bytes) {
            counters.addRequest(1L, bytes);
        }

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

    /**
     * Counts open and first-page bytes on the path-keyed storage object, then parks in the second
     * {@code next()} until {@link StorageRetryCancellation} observes {@code finish(true)}.
     */
    private static class LimitCloseParkFormatReader implements NoConfigFormatReader {
        private final CountingStorageProvider storageProvider;
        private final CountDownLatch parkedOnSecondNext;
        private final AtomicBoolean abandoned;
        private final long openBytes;
        private final long pageBytes;

        LimitCloseParkFormatReader(
            CountingStorageProvider storageProvider,
            CountDownLatch parkedOnSecondNext,
            AtomicBoolean abandoned,
            long openBytes,
            long pageBytes
        ) {
            this.storageProvider = storageProvider;
            this.parkedOnSecondNext = parkedOnSecondNext;
            this.abandoned = abandoned;
            this.openBytes = openBytes;
            this.pageBytes = pageBytes;
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
            CountingStorageObject counting = storageProvider.objectFor(object.path());
            counting.addRequest(openBytes);
            return new CloseableIterator<>() {
                private int remaining = 2;

                @Override
                public boolean hasNext() {
                    return remaining > 0;
                }

                @Override
                public Page next() {
                    if (remaining <= 0) {
                        throw new NoSuchElementException();
                    }
                    remaining--;
                    if (remaining == 1) {
                        counting.addRequest(pageBytes);
                        return createOneRowPage();
                    }
                    parkedOnSecondNext.countDown();
                    while (abandoned.get() == false && StorageRetryCancellation.isCancelled() == false) {
                        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                    }
                    return createOneRowPage();
                }

                @Override
                public void close() {}
            };
        }

        @Override
        public String formatName() {
            return "test-limit-close-park";
        }

        @Override
        public List<String> fileExtensions() {
            return List.of(".parquet");
        }

        @Override
        public void close() {}
    }
}
