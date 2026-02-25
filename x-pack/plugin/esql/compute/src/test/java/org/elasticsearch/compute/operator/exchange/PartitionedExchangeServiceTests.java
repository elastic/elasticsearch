/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.exchange;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.DriverRunner;
import org.elasticsearch.compute.operator.SinkOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.MockBlockFactory;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;

/**
 * Tests for the partitioned exchange infrastructure ({@link PartitionedExchangeSinkHandler}
 * and {@link PartitionedExchangeSourceHandler}) that routes pages by partition ID to
 * per-driver buffers for parallel final aggregation on the coordinator.
 */
public class PartitionedExchangeServiceTests extends ESTestCase {

    private TestThreadPool threadPool;
    private static final String ESQL_TEST_EXECUTOR = "esql_test_executor";

    @Before
    public void setThreadPool() {
        int numThreads = randomBoolean() ? 1 : between(2, 16);
        threadPool = new TestThreadPool(
            "test",
            new FixedExecutorBuilder(Settings.EMPTY, ESQL_TEST_EXECUTOR, numThreads, 1024, "esql", EsExecutors.TaskTrackingConfig.DEFAULT)
        );
    }

    @After
    public void shutdownThreadPool() {
        terminate(threadPool);
    }

    /**
     * A fetched page and the finished flag from an exchange response.
     * We need this because {@link ActionListener#respondAndRelease} closes the response
     * after the listener fires, so we must extract the page inside the listener callback.
     */
    private record FetchResult(Page page, boolean finished) {}

    /**
     * Fetches a page from the partitioned sink handler for a specific driver.
     * Takes the page inside the listener callback before the response is closed.
     */
    private FetchResult fetchPage(PartitionedExchangeSinkHandler sinkHandler, int driverIndex) {
        PlainActionFuture<FetchResult> future = new PlainActionFuture<>();
        sinkHandler.fetchPageAsync(driverIndex, false, ActionListener.wrap(resp -> {
            future.onResponse(new FetchResult(resp.takePage(), resp.finished()));
        }, future::onFailure));
        return safeGet(future);
    }

    /**
     * Tests that pages are routed to the correct driver based on partitionId.
     */
    public void testRoutingByPartitionId() throws Exception {
        int numPartitions = 256;
        int numDrivers = 8;
        int partitionsPerDriver = numPartitions / numDrivers;
        BlockFactory blockFactory = blockFactory();

        PartitionedExchangeSinkHandler sinkHandler = new PartitionedExchangeSinkHandler(
            blockFactory,
            numPartitions,
            numDrivers,
            128,
            threadPool.relativeTimeInMillisSupplier()
        );

        ExchangeSink sink = sinkHandler.createExchangeSink(() -> {});

        // Send pages with specific partition IDs and verify they arrive at the correct driver buffer
        int totalPages = numPartitions;
        for (int p = 0; p < totalPages; p++) {
            Page page = Page.withPartitionId(p, blockFactory.newConstantIntBlockWith(p, 1));
            sink.addPage(page);
        }
        sink.finish();

        // For each driver, fetch all pages and verify their partition IDs are in the expected range
        for (int d = 0; d < numDrivers; d++) {
            int expectedMinPartition = d * partitionsPerDriver;
            int expectedMaxPartition = (d + 1) * partitionsPerDriver;
            int pagesForDriver = 0;

            while (true) {
                FetchResult result = fetchPage(sinkHandler, d);
                if (result.page() == null && result.finished()) {
                    break;
                }
                if (result.page() != null) {
                    int partitionId = result.page().getPartitionId();
                    assertThat(
                        "partitionId " + partitionId + " should be routed to driver " + d,
                        partitionId,
                        greaterThanOrEqualTo(expectedMinPartition)
                    );
                    assertThat(
                        "partitionId " + partitionId + " should be routed to driver " + d,
                        partitionId,
                        lessThan(expectedMaxPartition)
                    );
                    pagesForDriver++;
                    result.page().releaseBlocks();
                }
            }
            assertThat("Driver " + d + " should receive " + partitionsPerDriver + " pages", pagesForDriver, equalTo(partitionsPerDriver));
        }
    }

    /**
     * Tests that no pages are lost or duplicated when routing through the partitioned exchange.
     */
    public void testNoPagesLostOrDuplicated() throws Exception {
        int numPartitions = 256;
        int numDrivers = randomFrom(1, 2, 4, 8, 16, 32);
        BlockFactory blockFactory = blockFactory();

        PartitionedExchangeSinkHandler sinkHandler = new PartitionedExchangeSinkHandler(
            blockFactory,
            numPartitions,
            numDrivers,
            128,
            threadPool.relativeTimeInMillisSupplier()
        );

        // Create multiple sinks and send pages with various partition IDs
        int numSinks = randomIntBetween(1, 4);
        List<ExchangeSink> sinks = new ArrayList<>();
        for (int i = 0; i < numSinks; i++) {
            sinks.add(sinkHandler.createExchangeSink(() -> {}));
        }

        Set<Integer> sentValues = ConcurrentCollections.newConcurrentSet();
        int totalPages = randomIntBetween(50, 500);
        for (int i = 0; i < totalPages; i++) {
            int partitionId = randomIntBetween(0, numPartitions - 1);
            int value = i;
            sentValues.add(value);
            Page page = Page.withPartitionId(partitionId, blockFactory.newConstantIntBlockWith(value, 1));
            randomFrom(sinks).addPage(page);
        }
        for (ExchangeSink sink : sinks) {
            sink.finish();
        }

        // Collect all pages from all drivers
        Set<Integer> receivedValues = ConcurrentCollections.newConcurrentSet();
        for (int d = 0; d < numDrivers; d++) {
            while (true) {
                FetchResult result = fetchPage(sinkHandler, d);
                if (result.page() == null && result.finished()) {
                    break;
                }
                if (result.page() != null) {
                    IntBlock block = result.page().getBlock(0);
                    for (int i = 0; i < block.getPositionCount(); i++) {
                        assertTrue("duplicate value: " + block.getInt(i), receivedValues.add(block.getInt(i)));
                    }
                    result.page().releaseBlocks();
                }
            }
        }

        assertThat(receivedValues, equalTo(sentValues));
    }

    /**
     * Tests that pages with NO_PARTITION (-1) are rejected.
     */
    public void testRejectsNoPartitionPages() {
        BlockFactory blockFactory = blockFactory();
        PartitionedExchangeSinkHandler sinkHandler = new PartitionedExchangeSinkHandler(
            blockFactory,
            256,
            8,
            128,
            threadPool.relativeTimeInMillisSupplier()
        );

        ExchangeSink sink = sinkHandler.createExchangeSink(() -> {});
        Page page = new Page(blockFactory.newConstantIntBlockWith(42, 1));
        assertThat(page.getPartitionId(), equalTo(Page.NO_PARTITION));
        expectThrows(IllegalArgumentException.class, () -> sink.addPage(page));
        page.releaseBlocks();
        sink.finish();
    }

    /**
     * Tests the partition-to-driver mapping.
     */
    public void testPartitionToDriverMapping() {
        BlockFactory blockFactory = blockFactory();
        PartitionedExchangeSinkHandler handler = new PartitionedExchangeSinkHandler(
            blockFactory,
            256,
            32,
            128,
            threadPool.relativeTimeInMillisSupplier()
        );

        // 256 partitions / 32 drivers = 8 partitions per driver
        assertThat(handler.driverForPartition(0), equalTo(0));
        assertThat(handler.driverForPartition(7), equalTo(0));
        assertThat(handler.driverForPartition(8), equalTo(1));
        assertThat(handler.driverForPartition(15), equalTo(1));
        assertThat(handler.driverForPartition(248), equalTo(31));
        assertThat(handler.driverForPartition(255), equalTo(31));
    }

    /**
     * Tests the constructor validation.
     */
    public void testConstructorValidation() {
        BlockFactory blockFactory = blockFactory();
        expectThrows(
            IllegalArgumentException.class,
            () -> new PartitionedExchangeSinkHandler(blockFactory, 0, 1, 128, threadPool.relativeTimeInMillisSupplier())
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> new PartitionedExchangeSinkHandler(blockFactory, 256, 0, 128, threadPool.relativeTimeInMillisSupplier())
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> new PartitionedExchangeSinkHandler(blockFactory, 256, 3, 128, threadPool.relativeTimeInMillisSupplier())
        );
    }

    /**
     * End-to-end test with real {@link Driver}s: multiple producer drivers create pages
     * tagged with partition IDs and write them to a partitioned sink. Multiple consumer
     * drivers read from their respective partition buffers via
     * {@link PartitionedExchangeSourceHandler}. Verifies that all values are delivered
     * exactly once and none are lost or duplicated.
     */
    public void testEndToEndWithDrivers() throws Exception {
        int numPartitions = 256;
        int numConsumerDrivers = randomFrom(1, 2, 4, 8);
        int numProducerDrivers = randomIntBetween(1, 4);
        int maxSeqNo = randomIntBetween(100, 2000);
        BlockFactory blockFactory = blockFactory();

        PartitionedExchangeSinkHandler sinkHandler = new PartitionedExchangeSinkHandler(
            blockFactory,
            numPartitions,
            numConsumerDrivers,
            64,
            threadPool.relativeTimeInMillisSupplier()
        );

        Set<Integer> receivedValues = ConcurrentCollections.newConcurrentSet();
        AtomicInteger producedSeqNo = new AtomicInteger(-1);

        // Build producer drivers
        List<Driver> drivers = new ArrayList<>();
        for (int i = 0; i < numProducerDrivers; i++) {
            DriverContext dc = driverContext();
            SourceOperator source = new SourceOperator() {
                @Override
                public void finish() {}

                @Override
                public boolean isFinished() {
                    return producedSeqNo.get() >= maxSeqNo;
                }

                @Override
                public Page getOutput() {
                    if (randomInt(100) < 5) {
                        return null;
                    }
                    int seqNo = producedSeqNo.incrementAndGet();
                    if (seqNo < maxSeqNo) {
                        // Assign a partition based on seqNo hash
                        int partitionId = seqNo % numPartitions;
                        return Page.withPartitionId(partitionId, dc.blockFactory().newConstantIntBlockWith(seqNo, 1));
                    }
                    return null;
                }

                @Override
                public void close() {}
            };
            drivers.add(
                createDriver(
                    "test-session:1",
                    "producer-" + i,
                    dc,
                    source,
                    new ExchangeSinkOperator(sinkHandler.createExchangeSink(() -> {}))
                )
            );
        }

        // Build consumer drivers with PartitionedExchangeSourceHandler
        for (int d = 0; d < numConsumerDrivers; d++) {
            DriverContext dc = driverContext();
            int driverIdx = d;
            int partitionsPerDriver = numPartitions / numConsumerDrivers;

            PartitionedExchangeSourceHandler sourceHandler = new PartitionedExchangeSourceHandler(
                sinkHandler,
                driverIdx,
                64,
                threadPool.executor(ESQL_TEST_EXECUTOR)
            );

            PlainActionFuture<Void> fetchFuture = new PlainActionFuture<>();
            sourceHandler.startFetching(1, fetchFuture);

            ExchangeSource exchangeSource = sourceHandler.createExchangeSource();

            SinkOperator sink = new SinkOperator() {
                private boolean finished = false;

                @Override
                public boolean needsInput() {
                    return isFinished() == false;
                }

                @Override
                protected void doAddInput(Page page) {
                    try {
                        IntBlock block = page.getBlock(0);
                        for (int i = 0; i < block.getPositionCount(); i++) {
                            int v = block.getInt(i);
                            assertTrue("duplicate value: " + v, receivedValues.add(v));
                        }
                    } finally {
                        page.releaseBlocks();
                    }
                }

                @Override
                public void finish() {
                    finished = true;
                }

                @Override
                public boolean isFinished() {
                    return finished;
                }

                @Override
                public void close() {}
            };

            drivers.add(createDriver("test-session:2", "consumer-" + d, dc, new ExchangeSourceOperator(exchangeSource), sink));
        }

        PlainActionFuture<Void> future = new PlainActionFuture<>();
        new DriverRunner(threadPool.getThreadContext()) {
            @Override
            protected void start(Driver driver, ActionListener<Void> listener) {
                Driver.start(threadPool.getThreadContext(), threadPool.executor(ESQL_TEST_EXECUTOR), driver, between(1, 10000), listener);
            }
        }.runToCompletion(drivers, future);
        future.actionGet(TimeValue.timeValueMinutes(1));

        // Verify all seqNos were received
        var expectedSeqNos = IntStream.range(0, maxSeqNo).boxed().collect(java.util.stream.Collectors.toSet());
        assertThat(receivedValues, equalTo(expectedSeqNos));
    }

    /**
     * Concurrency stress test: multiple sinks write to the partitioned exchange from
     * different threads simultaneously. Verifies thread safety of
     * {@link PartitionedExchangeSinkHandler#addPage} and that all values are received
     * exactly once across all driver buffers.
     */
    public void testMultipleSinksConcurrent() throws Exception {
        int numPartitions = 16;
        int numDrivers = 4;
        BlockFactory blockFactory = blockFactory();

        PartitionedExchangeSinkHandler sinkHandler = new PartitionedExchangeSinkHandler(
            blockFactory,
            numPartitions,
            numDrivers,
            128,
            threadPool.relativeTimeInMillisSupplier()
        );

        Set<Integer> sentValues = ConcurrentCollections.newConcurrentSet();
        AtomicInteger nextValue = new AtomicInteger();
        int numSinks = 4;
        int pagesPerSink = 100;

        // Run sinks concurrently on the thread pool
        PlainActionFuture<Void> sinksFuture = new PlainActionFuture<>();
        AtomicInteger completedSinks = new AtomicInteger();
        for (int s = 0; s < numSinks; s++) {
            ExchangeSink sink = sinkHandler.createExchangeSink(() -> {});
            threadPool.executor(ESQL_TEST_EXECUTOR).execute(() -> {
                try {
                    for (int i = 0; i < pagesPerSink; i++) {
                        int value = nextValue.getAndIncrement();
                        sentValues.add(value);
                        int partitionId = value % numPartitions;
                        Page page = Page.withPartitionId(partitionId, blockFactory.newConstantIntBlockWith(value, 1));
                        sink.addPage(page);
                    }
                    sink.finish();
                    if (completedSinks.incrementAndGet() == numSinks) {
                        sinksFuture.onResponse(null);
                    }
                } catch (Exception e) {
                    sinksFuture.onFailure(e);
                }
            });
        }
        safeGet(sinksFuture);

        // Collect all values from all drivers
        Set<Integer> receivedValues = ConcurrentCollections.newConcurrentSet();
        for (int d = 0; d < numDrivers; d++) {
            while (true) {
                FetchResult result = fetchPage(sinkHandler, d);
                if (result.page() == null && result.finished()) {
                    break;
                }
                if (result.page() != null) {
                    IntBlock block = result.page().getBlock(0);
                    for (int i = 0; i < block.getPositionCount(); i++) {
                        assertTrue("duplicate value", receivedValues.add(block.getInt(i)));
                    }
                    result.page().releaseBlocks();
                }
            }
        }

        assertThat(receivedValues, equalTo(sentValues));
    }

    private static Driver createDriver(
        String sessionId,
        String description,
        DriverContext dc,
        SourceOperator sourceOperator,
        SinkOperator sinkOperator
    ) {
        return new Driver(
            sessionId,
            "test",
            "unset",
            "unset",
            0,
            0,
            dc,
            () -> description,
            sourceOperator,
            List.of(),
            sinkOperator,
            Driver.DEFAULT_STATUS_INTERVAL,
            () -> {}
        );
    }

    private final List<CircuitBreaker> breakers = Collections.synchronizedList(new ArrayList<>());
    private final List<MockBlockFactory> blockFactories = new ArrayList<>();

    private DriverContext driverContext() {
        BlockFactory blockFactory = blockFactory();
        return new DriverContext(blockFactory.bigArrays(), blockFactory, null);
    }

    private BlockFactory blockFactory() {
        MockBigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofGb(1));
        CircuitBreaker breaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
        breakers.add(breaker);
        MockBlockFactory factory = new MockBlockFactory(breaker, bigArrays);
        blockFactories.add(factory);
        return factory;
    }

    @After
    public void allMemoryReleased() {
        for (MockBlockFactory blockFactory : blockFactories) {
            blockFactory.ensureAllBlocksAreReleased();
        }
        for (CircuitBreaker breaker : breakers) {
            assertThat(breaker.getUsed(), equalTo(0L));
        }
    }
}
