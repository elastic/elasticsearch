/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.exchange;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.node.VersionInformation;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.EsqlRefCountingListener;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.DriverRunner;
import org.elasticsearch.compute.operator.SinkOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.MockBlockFactory;
import org.elasticsearch.core.ReleasableRef;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancellationService;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.test.transport.StubbableTransport;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.transport.AbstractSimpleTransportTestCase;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportResponse;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

public class ExchangeServiceTests extends ESTestCase {

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

    public void testBasic() throws Exception {
        BlockFactory blockFactory = blockFactory();
        Page[] pages = new Page[7];
        for (int i = 0; i < pages.length; i++) {
            pages[i] = new Page(blockFactory.newConstantIntBlockWith(i, 2));
        }
        ExchangeSinkHandler sinkExchanger = new ExchangeSinkHandler(blockFactory, 2, threadPool.relativeTimeInMillisSupplier());
        AtomicInteger pagesAddedToSink = new AtomicInteger();
        ExchangeSink sink1 = sinkExchanger.createExchangeSink(pagesAddedToSink::incrementAndGet);
        ExchangeSink sink2 = sinkExchanger.createExchangeSink(pagesAddedToSink::incrementAndGet);
        ExchangeSourceHandler sourceExchanger = new ExchangeSourceHandler(3, threadPool.executor(ESQL_TEST_EXECUTOR));
        ExchangeSource source = sourceExchanger.createExchangeSource();
        AtomicInteger pagesAddedToSource = new AtomicInteger();
        PlainActionFuture<Void> remoteSinkFuture = new PlainActionFuture<>();
        sourceExchanger.addRemoteSink(
            sinkExchanger::fetchPageAsync,
            randomBoolean(),
            pagesAddedToSource::incrementAndGet,
            1,
            remoteSinkFuture
        );
        SubscribableListener<Void> waitForReading = source.waitForReading().listener();
        assertFalse(waitForReading.isDone());
        assertNull(source.pollPage());
        assertTrue(sink1.waitForWriting().listener().isDone());
        randomFrom(sink1, sink2).addPage(pages[0]);
        assertThat(pagesAddedToSink.get(), equalTo(1));
        randomFrom(sink1, sink2).addPage(pages[1]);
        assertThat(pagesAddedToSink.get(), equalTo(2));
        assertBusy(() -> assertThat(pagesAddedToSource.get(), equalTo(2)));
        // source and sink buffers can store 5 pages
        for (Page p : List.of(pages[2], pages[3], pages[4])) {
            ExchangeSink sink = randomFrom(sink1, sink2);
            assertBusy(() -> assertTrue(sink.waitForWriting().listener().isDone()));
            sink.addPage(p);
        }
        assertThat(pagesAddedToSink.get(), equalTo(5));
        assertBusy(() -> assertThat(pagesAddedToSource.get(), equalTo(3)));
        // sink buffer is full
        assertFalse(randomFrom(sink1, sink2).waitForWriting().listener().isDone());
        assertBusy(() -> assertTrue(source.waitForReading().listener().isDone()));
        assertEquals(pages[0], source.pollPage());
        assertBusy(() -> assertTrue(source.waitForReading().listener().isDone()));
        assertEquals(pages[1], source.pollPage());
        assertBusy(() -> assertThat(pagesAddedToSource.get(), equalTo(5)));
        // sink can write again
        assertBusy(() -> assertTrue(randomFrom(sink1, sink2).waitForWriting().listener().isDone()));
        randomFrom(sink1, sink2).addPage(pages[5]);
        assertBusy(() -> assertTrue(randomFrom(sink1, sink2).waitForWriting().listener().isDone()));
        randomFrom(sink1, sink2).addPage(pages[6]);
        assertThat(pagesAddedToSink.get(), equalTo(7));
        // sink buffer is full
        assertFalse(randomFrom(sink1, sink2).waitForWriting().listener().isDone());
        sink1.finish();
        assertTrue(sink1.isFinished());
        for (int i = 0; i < 5; i++) {
            assertBusy(() -> assertTrue(source.waitForReading().listener().isDone()));
            assertEquals(pages[2 + i], source.pollPage());
        }
        assertBusy(() -> assertThat(pagesAddedToSource.get(), equalTo(7)));
        // source buffer is empty
        assertFalse(source.waitForReading().listener().isDone());
        assertBusy(() -> assertTrue(sink2.waitForWriting().listener().isDone()));
        sink2.finish();
        assertTrue(sink2.isFinished());
        assertTrue(source.isFinished());
        source.finish();
        ESTestCase.terminate(threadPool);
        for (Page page : pages) {
            page.releaseBlocks();
        }
        safeGet(remoteSinkFuture);
    }

    /**
     * Generates sequence numbers up to the {@code maxInputSeqNo} (exclusive)
     */
    static class SeqNoGenerator implements SourceOperator.SourceOperatorFactory {
        final AtomicInteger nextSeqNo = new AtomicInteger(-1);
        final int maxInputSeqNo;

        SeqNoGenerator(int maxInputSeqNo) {
            this.maxInputSeqNo = maxInputSeqNo;
        }

        @Override
        public String describe() {
            return "SeqNoGenerator(maxInputSeqNo=" + maxInputSeqNo + ")";
        }

        @Override
        public SourceOperator get(DriverContext driverContext) {
            return new SourceOperator() {
                @Override
                public void finish() {

                }

                @Override
                public boolean isFinished() {
                    return nextSeqNo.get() >= maxInputSeqNo;
                }

                @Override
                public Page getOutput() {
                    if (randomInt(100) < 5) {
                        return null;
                    }
                    int size = randomIntBetween(1, 10);
                    try (IntBlock.Builder builder = driverContext.blockFactory().newIntBlockBuilder(size)) {
                        for (int i = 0; i < size; i++) {
                            int seqNo = nextSeqNo.incrementAndGet();
                            if (seqNo < maxInputSeqNo) {
                                builder.appendInt(seqNo);
                            }
                        }
                        return new Page(builder.build());
                    }
                }

                @Override
                public void close() {

                }
            };
        }
    }

    /**
     * Collects the received sequence numbers that are less than {@code maxOutputSeqNo}.
     */
    static final class SeqNoCollector implements SinkOperator.SinkOperatorFactory {
        final long maxOutputSeqNo;
        final Set<Integer> receivedSeqNos = ConcurrentCollections.newConcurrentSet();

        SeqNoCollector(long maxOutputSeqNo) {
            this.maxOutputSeqNo = maxOutputSeqNo;
        }

        @Override
        public String describe() {
            return "SeqNoCollector(maxOutputSeqNo=" + maxOutputSeqNo + ")";
        }

        @Override
        public SinkOperator get(DriverContext driverContext) {
            return new SinkOperator() {
                private boolean finished = false;

                @Override
                public boolean needsInput() {
                    return isFinished() == false;
                }

                @Override
                protected void doAddInput(Page page) {
                    try {
                        assertFalse("already finished", finished);
                        IntBlock block = page.getBlock(0);
                        for (int i = 0; i < block.getPositionCount(); i++) {
                            int v = block.getInt(i);
                            if (v < maxOutputSeqNo) {
                                assertTrue(receivedSeqNos.add(v));
                                // Early termination
                                if (receivedSeqNos.size() >= maxOutputSeqNo && randomBoolean()) {
                                    finished = true;
                                }
                            }
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
                public void close() {

                }
            };
        }
    }

    Set<Integer> runConcurrentTest(
        int maxInputSeqNo,
        int maxOutputSeqNo,
        Supplier<ExchangeSource> exchangeSource,
        Supplier<ExchangeSink> exchangeSink
    ) {
        final SeqNoCollector seqNoCollector = new SeqNoCollector(maxOutputSeqNo);
        final SeqNoGenerator seqNoGenerator = new SeqNoGenerator(maxInputSeqNo);
        int numSinks = randomIntBetween(1, 8);
        int numSources = randomIntBetween(1, 8);
        List<Driver> drivers = new ArrayList<>(numSinks + numSources);
        for (int i = 0; i < numSinks; i++) {
            DriverContext dc = driverContext();
            Driver d = createDriver(
                "test-session:1",
                "sink-" + i,
                dc,
                seqNoGenerator.get(dc),
                new ExchangeSinkOperator(exchangeSink.get())
            );
            drivers.add(d);
        }
        for (int i = 0; i < numSources; i++) {
            DriverContext dc = driverContext();
            Driver d = createDriver(
                "test-session:2",
                "source-" + i,
                dc,
                new ExchangeSourceOperator(exchangeSource.get()),
                seqNoCollector.get(dc)
            );
            drivers.add(d);
        }
        PlainActionFuture<Void> future = new PlainActionFuture<>();
        new DriverRunner(threadPool.getThreadContext()) {
            @Override
            protected void start(Driver driver, ActionListener<Void> listener) {
                Driver.start(threadPool.getThreadContext(), threadPool.executor(ESQL_TEST_EXECUTOR), driver, between(1, 10000), listener);
            }
        }.runToCompletion(drivers, future);
        future.actionGet(TimeValue.timeValueMinutes(1));
        return seqNoCollector.receivedSeqNos;
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

    public void testConcurrentWithHandlers() {
        BlockFactory blockFactory = blockFactory();
        var sourceExchanger = new ExchangeSourceHandler(randomExchangeBuffer(), threadPool.executor(ESQL_TEST_EXECUTOR));
        PlainActionFuture<Void> remoteSinksFuture = new PlainActionFuture<>();
        try (RefCountingListener refs = new RefCountingListener(remoteSinksFuture)) {
            List<ExchangeSinkHandler> sinkHandlers = new ArrayList<>();
            Supplier<ExchangeSink> exchangeSink = () -> {
                final ExchangeSinkHandler sinkHandler;
                if (sinkHandlers.isEmpty() == false && randomBoolean()) {
                    sinkHandler = randomFrom(sinkHandlers);
                } else {
                    sinkHandler = new ExchangeSinkHandler(blockFactory, randomExchangeBuffer(), threadPool.relativeTimeInMillisSupplier());
                    sourceExchanger.addRemoteSink(
                        sinkHandler::fetchPageAsync,
                        randomBoolean(),
                        () -> {},
                        randomIntBetween(1, 3),
                        refs.acquire()
                    );
                    sinkHandlers.add(sinkHandler);
                }
                return sinkHandler.createExchangeSink(() -> {});
            };
            final int maxInputSeqNo = rarely() ? -1 : randomIntBetween(0, 50_000);
            final int maxOutputSeqNo = rarely() ? -1 : randomIntBetween(0, 50_000);
            Set<Integer> actualSeqNos = runConcurrentTest(
                maxInputSeqNo,
                maxOutputSeqNo,
                sourceExchanger::createExchangeSource,
                exchangeSink
            );
            var expectedSeqNos = IntStream.range(0, Math.min(maxInputSeqNo, maxOutputSeqNo)).boxed().collect(Collectors.toSet());
            assertThat(actualSeqNos, hasSize(expectedSeqNos.size()));
            assertThat(actualSeqNos, equalTo(expectedSeqNos));
        }
        safeGet(remoteSinksFuture);
    }

    public void testExchangeSourceContinueOnFailure() {
        BlockFactory blockFactory = blockFactory();
        var exchangeSourceHandler = new ExchangeSourceHandler(randomExchangeBuffer(), threadPool.executor(ESQL_TEST_EXECUTOR));
        final int maxInputSeqNo = rarely() ? -1 : randomIntBetween(0, 50_000);
        final int maxOutputSeqNo = rarely() ? -1 : randomIntBetween(0, 50_000);
        Set<Integer> expectedSeqNos = ConcurrentCollections.newConcurrentSet();
        AtomicInteger failedRequests = new AtomicInteger();
        AtomicInteger totalSinks = new AtomicInteger();
        AtomicInteger failedSinks = new AtomicInteger();
        AtomicInteger completedSinks = new AtomicInteger();
        PlainActionFuture<Void> remoteSinksFuture = new PlainActionFuture<>();
        try (RefCountingListener refs = new RefCountingListener(remoteSinksFuture)) {
            Supplier<ExchangeSink> exchangeSink = () -> {
                var sinkHandler = new ExchangeSinkHandler(blockFactory, randomExchangeBuffer(), threadPool.relativeTimeInMillisSupplier());
                int failAfter = randomBoolean() ? Integer.MAX_VALUE : randomIntBetween(0, 100);
                AtomicInteger fetched = new AtomicInteger();
                int instance = randomIntBetween(1, 3);
                totalSinks.incrementAndGet();
                AtomicBoolean sinkFailed = new AtomicBoolean();
                ActionListener<Void> oneSinkListener = refs.acquire();
                exchangeSourceHandler.addRemoteSink((allSourcesFinished, listener) -> {
                    if (fetched.incrementAndGet() > failAfter) {
                        sinkHandler.fetchPageAsync(true, listener.delegateFailure((l, r) -> {
                            failedRequests.incrementAndGet();
                            sinkFailed.set(true);
                            listener.onFailure(new CircuitBreakingException("simulated", CircuitBreaker.Durability.PERMANENT));
                        }));
                    } else {
                        sinkHandler.fetchPageAsync(allSourcesFinished, listener.delegateFailure((l, r) -> {
                            Page page = r.takePage();
                            if (page != null) {
                                IntBlock block = page.getBlock(0);
                                for (int i = 0; i < block.getPositionCount(); i++) {
                                    int v = block.getInt(i);
                                    if (v < maxOutputSeqNo) {
                                        expectedSeqNos.add(v);
                                    }
                                }
                            }
                            l.onResponse(new ExchangeResponse(blockFactory, page, r.finished()));
                        }));
                    }
                }, false, () -> {}, instance, ActionListener.wrap(r -> {
                    assertFalse(sinkFailed.get());
                    completedSinks.incrementAndGet();
                    oneSinkListener.onResponse(null);
                }, e -> {
                    assertTrue(sinkFailed.get());
                    failedSinks.incrementAndGet();
                    oneSinkListener.onFailure(e);
                }));
                return sinkHandler.createExchangeSink(() -> {});
            };
            Set<Integer> actualSeqNos = runConcurrentTest(
                maxInputSeqNo,
                maxOutputSeqNo,
                exchangeSourceHandler::createExchangeSource,
                exchangeSink
            );
            assertThat(actualSeqNos, equalTo(expectedSeqNos));
        }
        if (failedRequests.get() > 0) {
            expectThrows(CircuitBreakingException.class, () -> remoteSinksFuture.actionGet(1, TimeUnit.MINUTES));
            assertThat(failedSinks.get(), greaterThan(0));
            assertThat(completedSinks.get() + failedSinks.get(), equalTo(totalSinks.get()));
        } else {
            safeGet(remoteSinksFuture);
            assertThat(failedSinks.get(), equalTo(0));
            assertThat(completedSinks.get(), equalTo(totalSinks.get()));
        }
    }

    public void testClosingSinks() {
        BlockFactory blockFactory = blockFactory();
        IntBlock block1 = blockFactory.newConstantIntBlockWith(1, 2);
        IntBlock block2 = blockFactory.newConstantIntBlockWith(1, 2);
        Page p1 = new Page(block1);
        Page p2 = new Page(block2);
        ExchangeSinkHandler sinkExchanger = new ExchangeSinkHandler(blockFactory, 2, threadPool.relativeTimeInMillisSupplier());
        ExchangeSink sink = sinkExchanger.createExchangeSink(() -> {});
        sink.addPage(p1);
        sink.addPage(p2);
        assertFalse(sink.waitForWriting().listener().isDone());
        PlainActionFuture<ExchangeResponse> future = new PlainActionFuture<>();
        sinkExchanger.fetchPageAsync(true, future);
        ExchangeResponse resp = safeGet(future);
        assertTrue(resp.finished());
        assertNull(resp.takePage());
        assertTrue(sink.waitForWriting().listener().isDone());
        assertTrue(sink.isFinished());
    }

    public void testFinishEarly() throws Exception {
        ExchangeSourceHandler sourceHandler = new ExchangeSourceHandler(20, threadPool.generic());
        Semaphore permits = new Semaphore(between(1, 5));
        BlockFactory blockFactory = blockFactory();
        Queue<Page> pages = ConcurrentCollections.newQueue();
        ExchangeSource exchangeSource = sourceHandler.createExchangeSource();
        AtomicBoolean sinkClosed = new AtomicBoolean();
        PlainActionFuture<Void> sinkCompleted = new PlainActionFuture<>();
        sourceHandler.addRemoteSink((allSourcesFinished, listener) -> {
            if (allSourcesFinished) {
                sinkClosed.set(true);
                permits.release(10);
                listener.onResponse(new ExchangeResponse(blockFactory, null, sinkClosed.get()));
            } else {
                try {
                    if (permits.tryAcquire(between(0, 100), TimeUnit.MICROSECONDS)) {
                        boolean closed = sinkClosed.get();
                        final Page page;
                        if (closed) {
                            page = new Page(blockFactory.newConstantIntBlockWith(1, 1));
                            pages.add(page);
                        } else {
                            page = null;
                        }
                        listener.onResponse(new ExchangeResponse(blockFactory, page, closed));
                    } else {
                        listener.onResponse(new ExchangeResponse(blockFactory, null, sinkClosed.get()));
                    }
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
            }
        }, false, () -> {}, between(1, 3), sinkCompleted);
        threadPool.schedule(
            () -> sourceHandler.finishEarly(randomBoolean(), ActionListener.noop()),
            TimeValue.timeValueMillis(between(0, 10)),
            threadPool.generic()
        );
        sinkCompleted.actionGet();
        Page p;
        while ((p = exchangeSource.pollPage()) != null) {
            assertSame(p, pages.poll());
            p.releaseBlocks();
        }
        while ((p = pages.poll()) != null) {
            p.releaseBlocks();
        }
        assertTrue(exchangeSource.isFinished());
        exchangeSource.finish();
    }

    public void testConcurrentWithTransportActions() {
        MockTransportService node0 = newTransportService();
        ExchangeService exchange0 = new ExchangeService(Settings.EMPTY, threadPool, ESQL_TEST_EXECUTOR, blockFactory());
        exchange0.registerTransportHandler(node0);
        MockTransportService node1 = newTransportService();
        ExchangeService exchange1 = new ExchangeService(Settings.EMPTY, threadPool, ESQL_TEST_EXECUTOR, blockFactory());
        exchange1.registerTransportHandler(node1);
        AbstractSimpleTransportTestCase.connectToNode(node0, node1.getLocalNode());
        Set<String> finishingRequests = ConcurrentCollections.newConcurrentSet();
        node1.addRequestHandlingBehavior(ExchangeService.EXCHANGE_ACTION_NAME, (handler, request, channel, task) -> {
            final ExchangeRequest exchangeRequest = (ExchangeRequest) request;
            if (exchangeRequest.sourcesFinished()) {
                String exchangeId = exchangeRequest.exchangeId();
                assertTrue("tried to finish [" + exchangeId + "] twice", finishingRequests.add(exchangeId));
            }
            handler.messageReceived(request, channel, task);
        });

        try (exchange0; exchange1; node0; node1) {
            String exchangeId = "exchange";
            Task task = new Task(1, "", "", "", null, Collections.emptyMap());
            var sourceHandler = new ExchangeSourceHandler(randomExchangeBuffer(), threadPool.executor(ESQL_TEST_EXECUTOR));
            ExchangeSinkHandler sinkHandler = exchange1.createSinkHandler(exchangeId, randomExchangeBuffer());
            Transport.Connection connection = node0.getConnection(node1.getLocalNode());
            sourceHandler.addRemoteSink(
                exchange0.newRemoteSink(task, exchangeId, node0, connection),
                randomBoolean(),
                () -> {},
                randomIntBetween(1, 5),
                ActionListener.noop()
            );
            final int maxInputSeqNo = rarely() ? -1 : randomIntBetween(0, 50_000);
            final int maxOutputSeqNo = rarely() ? -1 : randomIntBetween(0, 50_000);
            Set<Integer> actualSeqNos = runConcurrentTest(
                maxInputSeqNo,
                maxOutputSeqNo,
                sourceHandler::createExchangeSource,
                () -> sinkHandler.createExchangeSink(() -> {})
            );
            var expectedSeqNos = IntStream.range(0, Math.min(maxInputSeqNo, maxOutputSeqNo)).boxed().collect(Collectors.toSet());
            assertThat(actualSeqNos, hasSize(expectedSeqNos.size()));
            assertThat(actualSeqNos, equalTo(expectedSeqNos));
        }
    }

    public void testFailToRespondPage() throws Exception {
        Settings settings = Settings.builder().build();
        MockTransportService node0 = newTransportService();
        ExchangeService exchange0 = new ExchangeService(settings, threadPool, ESQL_TEST_EXECUTOR, blockFactory());
        exchange0.registerTransportHandler(node0);
        MockTransportService node1 = newTransportService();
        ExchangeService exchange1 = new ExchangeService(settings, threadPool, ESQL_TEST_EXECUTOR, blockFactory());
        exchange1.registerTransportHandler(node1);
        AbstractSimpleTransportTestCase.connectToNode(node0, node1.getLocalNode());
        final int maxSeqNo = randomIntBetween(1000, 5000);
        final int disconnectOnSeqNo = randomIntBetween(100, 500);
        BlockFactory blockFactory = blockFactory();
        node1.addRequestHandlingBehavior(ExchangeService.EXCHANGE_ACTION_NAME, new StubbableTransport.RequestHandlingBehavior<>() {
            @Override
            public void messageReceived(
                TransportRequestHandler<TransportRequest> handler,
                TransportRequest request,
                TransportChannel channel,
                Task task
            ) throws Exception {
                FilterTransportChannel filterChannel = new FilterTransportChannel(channel) {
                    @Override
                    public void sendResponse(TransportResponse transportResponse) {
                        ExchangeResponse origResp = (ExchangeResponse) transportResponse;
                        Page page = origResp.takePage();
                        if (page != null) {
                            IntBlock block = page.getBlock(0);
                            for (int i = 0; i < block.getPositionCount(); i++) {
                                if (block.getInt(i) == disconnectOnSeqNo) {
                                    page.releaseBlocks();
                                    sendResponse(new IOException("page is too large"));
                                    return;
                                }
                            }
                        }
                        try (var newRespRef = ReleasableRef.of(new ExchangeResponse(blockFactory, page, origResp.finished()))) {
                            super.sendResponse(newRespRef.get());
                        }
                    }
                };
                handler.messageReceived(request, filterChannel, task);
            }
        });
        try (exchange0; exchange1; node0; node1) {
            String exchangeId = "exchange";
            Task task = new Task(1, "", "", "", null, Collections.emptyMap());
            var sourceHandler = new ExchangeSourceHandler(randomIntBetween(1, 128), threadPool.executor(ESQL_TEST_EXECUTOR));
            ExchangeSinkHandler sinkHandler = exchange1.createSinkHandler(exchangeId, randomIntBetween(1, 128));
            Transport.Connection connection = node0.getConnection(node1.getLocalNode());
            PlainActionFuture<Void> remoteSinkFuture = new PlainActionFuture<>();
            sourceHandler.addRemoteSink(
                exchange0.newRemoteSink(task, exchangeId, node0, connection),
                true,
                () -> {},
                randomIntBetween(1, 5),
                remoteSinkFuture
            );
            Exception driverException = expectThrows(
                Exception.class,
                () -> runConcurrentTest(
                    maxSeqNo,
                    maxSeqNo,
                    sourceHandler::createExchangeSource,
                    () -> sinkHandler.createExchangeSink(() -> {})
                )
            );
            assertThat(driverException, instanceOf(TaskCancelledException.class));
            var sinkException = expectThrows(Exception.class, remoteSinkFuture::actionGet);
            Throwable cause = ExceptionsHelper.unwrap(sinkException, IOException.class);
            assertNotNull(cause);
            assertThat(cause.getMessage(), equalTo("page is too large"));
            PlainActionFuture<Void> sinkCompletionFuture = new PlainActionFuture<>();
            sinkHandler.addCompletionListener(sinkCompletionFuture);
            safeGet(sinkCompletionFuture);
        }
    }

    public void testNoCyclicException() throws Exception {
        PlainActionFuture<Void> future = new PlainActionFuture<>();
        try (EsqlRefCountingListener refs = new EsqlRefCountingListener(future)) {
            var exchangeSourceHandler = new ExchangeSourceHandler(between(10, 100), threadPool.generic());
            int numSinks = between(5, 10);
            for (int i = 0; i < numSinks; i++) {
                RemoteSink remoteSink = (allSourcesFinished, listener) -> threadPool.schedule(
                    () -> listener.onFailure(new IOException("simulated")),
                    TimeValue.timeValueMillis(1),
                    threadPool.generic()
                );
                exchangeSourceHandler.addRemoteSink(remoteSink, randomBoolean(), () -> {}, between(1, 3), refs.acquire());
            }
        }
        Exception err = expectThrows(Exception.class, () -> future.actionGet(10, TimeUnit.SECONDS));
        assertThat(ExceptionsHelper.unwrap(err, IOException.class).getMessage(), equalTo("simulated"));
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            // ensure no cyclic exception
            ElasticsearchException.writeException(err, output);
        }
    }

    private MockTransportService newTransportService() {
        List<NamedWriteableRegistry.Entry> namedWriteables = new ArrayList<>(ClusterModule.getNamedWriteables());
        NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(namedWriteables);
        MockTransportService service = MockTransportService.createNewService(
            Settings.EMPTY,
            MockTransportService.newMockTransport(Settings.EMPTY, TransportVersion.current(), threadPool, namedWriteableRegistry),
            VersionInformation.CURRENT,
            threadPool,
            null,
            Collections.emptySet()
        );
        service.getTaskManager().setTaskCancellationService(new TaskCancellationService(service));
        service.start();
        service.acceptIncomingRequests();
        return service;
    }

    private int randomExchangeBuffer() {
        return randomBoolean() ? randomIntBetween(1, 3) : randomIntBetween(1, 128);
    }

    private static class FilterTransportChannel implements TransportChannel {
        private final TransportChannel in;

        FilterTransportChannel(TransportChannel in) {
            this.in = in;
        }

        @Override
        public String getProfileName() {
            return in.getProfileName();
        }

        @Override
        public void sendResponse(TransportResponse response) {
            in.sendResponse(response);
        }

        @Override
        public void sendResponse(Exception exception) {
            in.sendResponse(exception);
        }
    }

    private final List<CircuitBreaker> breakers = Collections.synchronizedList(new ArrayList<>());

    private DriverContext driverContext() {
        BlockFactory blockFactory = blockFactory();
        return new DriverContext(blockFactory.bigArrays(), blockFactory);
    }

    private BlockFactory blockFactory() {
        MockBigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofGb(1));
        CircuitBreaker breaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
        breakers.add(breaker);
        MockBlockFactory factory = new MockBlockFactory(breaker, bigArrays);
        blockFactories.add(factory);
        return factory;
    }

    private final List<MockBlockFactory> blockFactories = new ArrayList<>();

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
