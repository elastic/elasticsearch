/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.exchange;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.node.VersionInformation;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.TrackingCircuitBreaker;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
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
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancellationService;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.test.transport.StubbableTransport;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.transport.AbstractSimpleTransportTestCase;
import org.elasticsearch.transport.AbstractTransportRequest;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.sameInstance;

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
        ExchangeSinkHandler sinkExchanger = new ExchangeSinkHandler(2, threadPool.relativeTimeInMillisSupplier());
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
        assertBusy(() -> assertTrue(source.isFinished()));
        source.finish();
        ESTestCase.terminate(threadPool);
        for (Page page : pages) {
            page.releaseBlocks();
        }
        safeGet(remoteSinkFuture);
    }

    public void testLocalExchangeBasic() {
        BlockFactory blockFactory = blockFactory();
        Page[] pages = new Page[] {
            new Page(blockFactory.newConstantIntBlockWith(0, 1)),
            new Page(blockFactory.newConstantIntBlockWith(1, 1)),
            new Page(blockFactory.newConstantIntBlockWith(2, 1)) };
        LocalExchange exchange = new LocalExchange(2);
        ExchangeSource source = exchange.exchangeSource();
        AtomicInteger pagesAdded = new AtomicInteger();
        ExchangeSink sink1 = exchange.exchangeSink(pagesAdded::incrementAndGet);
        ExchangeSink sink2 = exchange.exchangeSink(pagesAdded::incrementAndGet);

        assertFalse(source.waitForReading().listener().isDone());
        sink1.addPage(pages[0]);
        sink2.addPage(pages[1]);
        assertThat(pagesAdded.get(), equalTo(2));
        assertFalse(sink1.waitForWriting().listener().isDone());

        assertSame(pages[0], source.pollPage());
        assertTrue(sink1.waitForWriting().listener().isDone());
        sink1.finish();
        assertTrue(sink1.isFinished());
        assertFalse(source.isFinished());

        sink2.addPage(pages[2]);
        sink2.finish();
        assertSame(pages[1], source.pollPage());
        assertSame(pages[2], source.pollPage());
        assertNull(source.pollPage());
        assertTrue(source.isFinished());
        source.finish();

        for (Page page : pages) {
            page.releaseBlocks();
        }
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
        var sourceExchanger = new ExchangeSourceHandler(randomExchangeBuffer(), threadPool.executor(ESQL_TEST_EXECUTOR));
        PlainActionFuture<Void> remoteSinksFuture = new PlainActionFuture<>();
        try (RefCountingListener refs = new RefCountingListener(remoteSinksFuture)) {
            List<ExchangeSinkHandler> sinkHandlers = new ArrayList<>();
            Supplier<ExchangeSink> exchangeSink = () -> {
                final ExchangeSinkHandler sinkHandler;
                if (sinkHandlers.isEmpty() == false && randomBoolean()) {
                    sinkHandler = randomFrom(sinkHandlers);
                } else {
                    sinkHandler = new ExchangeSinkHandler(randomExchangeBuffer(), threadPool.relativeTimeInMillisSupplier());
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
                var sinkHandler = new ExchangeSinkHandler(randomExchangeBuffer(), threadPool.relativeTimeInMillisSupplier());
                int failAfter = randomBoolean() ? Integer.MAX_VALUE : randomIntBetween(0, 100);
                AtomicInteger fetched = new AtomicInteger();
                int instance = randomIntBetween(1, 3);
                totalSinks.incrementAndGet();
                AtomicBoolean sinkFailed = new AtomicBoolean();
                ActionListener<Void> oneSinkListener = refs.acquire();
                exchangeSourceHandler.addRemoteSink((allSourcesFinished, listener) -> {
                    // Don't simulate failure when allSourcesFinished - the exchange source might have
                    // already completed, and the failure can be ignored in the completion listener
                    if (fetched.incrementAndGet() > failAfter && allSourcesFinished == false) {
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
                            l.onResponse(new ExchangeResponse(page, r.finished()));
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
        ExchangeSinkHandler sinkExchanger = new ExchangeSinkHandler(2, threadPool.relativeTimeInMillisSupplier());
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

    public void testFinishEarly() {
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
                listener.onResponse(new ExchangeResponse(null, sinkClosed.get()));
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
                        listener.onResponse(new ExchangeResponse(page, closed));
                    } else {
                        listener.onResponse(new ExchangeResponse(null, sinkClosed.get()));
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
                        try (var newRespRef = ReleasableRef.of(new ExchangeResponse(page, origResp.finished()))) {
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

    /**
     * A remote sink reserves no estimate before its first fetch, so the coordinator charges the first page while it arrives, on top of
     * the blocks it reads from it.
     */
    public void testCoordinatorChargesFirstPageWhileItArrives() throws Exception {
        TrackingCircuitBreaker coordinatorBreaker = new TrackingCircuitBreaker();
        BlockFactory dataNodeFactory = blockFactory();
        Page page = intPage(dataNodeFactory, 10_000);
        long blockBytes = page.ramBytesUsedByBlocks();
        long serializedBytes = serializedSize(page);
        try (Page ignored = fetchFirstPage(trackingBlockFactory(coordinatorBreaker), dataNodeFactory, page).actionGet()) {
            assertThat(coordinatorBreaker.peak(), greaterThanOrEqualTo(blockBytes + serializedBytes));
            assertThat(coordinatorBreaker.getUsed(), equalTo(blockBytes));
        }
        assertThat(coordinatorBreaker.getUsed(), equalTo(0L));
    }

    /**
     * After the first page, each fetch reserves an estimate of the page before it is sent, so only the bytes beyond it are charged as
     * they arrive. The second page is much larger than the first, whose size is the estimate, and arrives in several pieces.
     */
    public void testCoordinatorChargesOnlyBytesBeyondTheEstimate() throws Exception {
        TrackingCircuitBreaker coordinatorBreaker = new TrackingCircuitBreaker();
        BlockFactory dataNodeFactory = blockFactory();
        // Both pages stay below the size at which blocks move to big arrays, so their blocks are charged to the breaker too.
        Page first = intPage(dataNodeFactory, 30_000);
        Page second = intPage(dataNodeFactory, 120_000);
        long estimate = first.ramBytesUsedByBlocks();
        long secondBlockBytes = second.ramBytesUsedByBlocks();
        long secondSerializedBytes = serializedSize(second);
        List<PlainActionFuture<Page>> fetched = fetchPages(trackingBlockFactory(coordinatorBreaker), dataNodeFactory, true, first, second);
        // Rethrows a failed fetch. The pages were released as they arrived.
        for (PlainActionFuture<Page> future : fetched) {
            future.actionGet();
        }
        assertThat(fetched, hasSize(2));
        // While the second page is read, its blocks are charged, and its wire bytes, partly as the estimate reserved before the fetch.
        assertThat(coordinatorBreaker.peak(), greaterThanOrEqualTo(secondBlockBytes + secondSerializedBytes));
        // Charging every byte that arrives on top of the estimate would count the estimate twice.
        assertThat(coordinatorBreaker.peak(), lessThan(secondBlockBytes + secondSerializedBytes + estimate / 2));
        // The estimate is refunded after the response listener returns, which can be after the future completes.
        assertBusy(() -> assertThat(coordinatorBreaker.getUsed(), equalTo(0L)));
    }

    public void testCoordinatorRejectsFirstPageWhileItArrives() throws Exception {
        TrackingCircuitBreaker coordinatorBreaker = new TrackingCircuitBreaker(ByteSizeValue.ofKb(1).getBytes());
        BlockFactory dataNodeFactory = blockFactory();
        Page page = intPage(dataNodeFactory, 10_000);
        PlainActionFuture<Page> fetched = fetchFirstPage(trackingBlockFactory(coordinatorBreaker), dataNodeFactory, page);
        Exception e = expectThrows(Exception.class, fetched::actionGet);
        CircuitBreakingException trip = (CircuitBreakingException) ExceptionsHelper.unwrap(e, CircuitBreakingException.class);
        assertNotNull(e.toString(), trip);
        assertThat(trip.getMessage(), containsString("fetch page"));
        assertThat(coordinatorBreaker.getUsed(), equalTo(0L));
    }

    /**
     * The data node charges the page it sends as it serializes it, through the channel, to the breaker of its blocks, which is the
     * breaker of its query. A page that does not fit is rejected there.
     */
    public void testDataNodeChargesPageWhileSendingIt() throws Exception {
        Page sized = intPage(blockFactory(), 10_000);
        long blockBytes = sized.ramBytesUsedByBlocks();
        long serializedBytes = serializedSize(sized);
        sized.releaseBlocks();
        // Room for the blocks, but not for the serialized page on top of them.
        long queryLimit = blockBytes + serializedBytes / 2;
        TrackingCircuitBreaker queryBreaker = new TrackingCircuitBreaker(queryLimit);
        Page page = intPage(trackingBlockFactory(queryBreaker), 10_000);
        TrackingCircuitBreaker dataNodeBreaker = new TrackingCircuitBreaker();
        PlainActionFuture<Page> fetched = fetchFirstPage(blockFactory(), trackingBlockFactory(dataNodeBreaker), page);
        Exception e = expectThrows(Exception.class, fetched::actionGet);
        CircuitBreakingException trip = (CircuitBreakingException) ExceptionsHelper.unwrap(e, CircuitBreakingException.class);
        assertNotNull(e.toString(), trip);
        assertThat(trip.getMessage(), containsString("RecyclerBytesStreamOutput"));
        assertThat(trip.getByteLimit(), equalTo(queryLimit));
        assertBusy(() -> assertThat(queryBreaker.getUsed(), equalTo(0L)));
        assertBusy(() -> assertThat(dataNodeBreaker.getUsed(), equalTo(0L)));
    }

    /**
     * A remote sink created with the block factory of a query charges the page to it while it arrives and reads it, and nothing to the
     * factory of the exchange service.
     */
    public void testRemoteSinkChargesItsOwnFactory() throws Exception {
        TrackingCircuitBreaker coordinatorBreaker = new TrackingCircuitBreaker();
        TrackingCircuitBreaker queryBreaker = new TrackingCircuitBreaker();
        BlockFactory dataNodeFactory = blockFactory();
        Page page = intPage(dataNodeFactory, 10_000);
        long blockBytes = page.ramBytesUsedByBlocks();
        long serializedBytes = serializedSize(page);
        BlockFactory coordinatorFactory = trackingBlockFactory(coordinatorBreaker);
        BlockFactory queryFactory = trackingBlockFactory(queryBreaker);
        try (Page ignored = fetchPages(coordinatorFactory, queryFactory, dataNodeFactory, false, page).getFirst().actionGet()) {
            assertThat(queryBreaker.peak(), greaterThanOrEqualTo(blockBytes + serializedBytes));
            assertThat(queryBreaker.getUsed(), equalTo(blockBytes));
        }
        assertThat(queryBreaker.getUsed(), equalTo(0L));
        assertThat(coordinatorBreaker.peak(), equalTo(0L));
        assertThat(coordinatorBreaker.getUsed(), equalTo(0L));
    }

    /**
     * Starts a coordinator and a data node, adds {@code page} to a sink on the data node and fetches it from the coordinator.
     */
    private PlainActionFuture<Page> fetchFirstPage(BlockFactory coordinatorFactory, BlockFactory dataNodeFactory, Page page)
        throws Exception {
        return fetchPages(coordinatorFactory, dataNodeFactory, false, page).getFirst();
    }

    private List<PlainActionFuture<Page>> fetchPages(
        BlockFactory coordinatorFactory,
        BlockFactory dataNodeFactory,
        boolean releaseEach,
        Page... pages
    ) throws Exception {
        return fetchPages(coordinatorFactory, coordinatorFactory, dataNodeFactory, releaseEach, pages);
    }

    /**
     * Starts a coordinator and a data node, adds {@code pages} to a sink on the data node and fetches them one after another from the
     * coordinator through a remote sink that reads them with {@code remoteSinkFactory}. If {@code releaseEach}, each page is released as
     * soon as it arrives.
     */
    private List<PlainActionFuture<Page>> fetchPages(
        BlockFactory coordinatorFactory,
        BlockFactory remoteSinkFactory,
        BlockFactory dataNodeFactory,
        boolean releaseEach,
        Page... pages
    ) throws Exception {
        MockTransportService node0 = newTransportService();
        ExchangeService exchange0 = new ExchangeService(Settings.EMPTY, threadPool, ESQL_TEST_EXECUTOR, coordinatorFactory);
        exchange0.registerTransportHandler(node0);
        MockTransportService node1 = newTransportService();
        ExchangeService exchange1 = new ExchangeService(Settings.EMPTY, threadPool, ESQL_TEST_EXECUTOR, dataNodeFactory);
        exchange1.registerTransportHandler(node1);
        AbstractSimpleTransportTestCase.connectToNode(node0, node1.getLocalNode());
        List<PlainActionFuture<Page>> fetched = new ArrayList<>();
        try (exchange0; exchange1; node0; node1) {
            String exchangeId = "exchange";
            ExchangeSinkHandler sinkHandler = exchange1.createSinkHandler(exchangeId, pages.length);
            ExchangeSink sink = sinkHandler.createExchangeSink(() -> {});
            for (Page page : pages) {
                sink.addPage(page);
            }
            Task task = new Task(1, "", "", "", null, Collections.emptyMap());
            RemoteSink remoteSink = exchange0.newRemoteSink(
                task,
                exchangeId,
                node0,
                node0.getConnection(node1.getLocalNode()),
                remoteSinkFactory
            );
            for (Page ignored : pages) {
                PlainActionFuture<Page> future = new PlainActionFuture<>();
                fetched.add(future);
                // The response is released once the listener returns, so take its page as the exchange source does.
                remoteSink.fetchPageAsync(false, future.map(response -> {
                    Page page = response.takePage();
                    if (releaseEach && page != null) {
                        page.releaseBlocks();
                    }
                    return page;
                }));
                try {
                    future.get(30, TimeUnit.SECONDS);
                } catch (ExecutionException e) {
                    // The fetch failed, which the caller checks on the future. Stop fetching further pages.
                    break;
                }
            }
            sink.finish();
            exchange1.finishSinkHandler(exchangeId, null);
        }
        return fetched;
    }

    private static Page intPage(BlockFactory blockFactory, int positions) {
        try (IntBlock.Builder builder = blockFactory.newIntBlockBuilder(positions)) {
            for (int i = 0; i < positions; i++) {
                builder.appendInt(i);
            }
            return new Page(builder.build());
        }
    }

    private static long serializedSize(Page page) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            page.writeTo(out);
            return out.size();
        }
    }

    private static BlockFactory trackingBlockFactory(CircuitBreaker breaker) {
        return BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(breaker).build();
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

    public void testLocalExchangeConcurrentRuns() {
        LocalExchange localExchange = new LocalExchange(between(1, 100));
        final int maxInputSeqNo = rarely() ? -1 : randomIntBetween(0, 50_000);
        final int maxOutputSeqNo = rarely() ? -1 : randomIntBetween(0, 50_000);
        Set<Integer> actualSeqNos = runConcurrentTest(
            maxInputSeqNo,
            maxOutputSeqNo,
            localExchange::exchangeSource,
            () -> localExchange.exchangeSink(() -> {})
        );
        var expectedSeqNos = IntStream.range(0, Math.min(maxInputSeqNo, maxOutputSeqNo)).boxed().collect(Collectors.toSet());
        assertThat(actualSeqNos, hasSize(expectedSeqNos.size()));
        assertThat(actualSeqNos, equalTo(expectedSeqNos));
    }

    /**
     * A {@code sourcesFinished} fetch is the coordinator closing the exchange. The sink must leave
     * {@link ExchangeService#isEmpty()} immediately, a completion listener that also finishes the sink must still
     * run, and the next compute lookup must observe the close instead of a missing exchange. A second close is a
     * finished response.
     */
    public void testSourcesFinishedUnregistersSink() {
        MockTransportService node0 = newTransportService();
        ExchangeService exchange0 = new ExchangeService(Settings.EMPTY, threadPool, ESQL_TEST_EXECUTOR, blockFactory());
        exchange0.registerTransportHandler(node0);
        MockTransportService node1 = newTransportService();
        ExchangeService exchange1 = new ExchangeService(Settings.EMPTY, threadPool, ESQL_TEST_EXECUTOR, blockFactory());
        exchange1.registerTransportHandler(node1);
        AbstractSimpleTransportTestCase.connectToNode(node0, node1.getLocalNode());
        try (exchange0; exchange1; node0; node1) {
            String exchangeId = "exchange-" + randomAlphaOfLength(8);
            Task task = new Task(1, "", "", "", null, Collections.emptyMap());
            ExchangeSinkHandler created = exchange1.createSinkHandler(exchangeId, randomExchangeBuffer());
            AtomicBoolean completionRan = new AtomicBoolean();
            created.addCompletionListener(ActionListener.running(() -> {
                completionRan.set(true);
                exchange1.finishSinkHandler(exchangeId, null);
            }));
            Transport.Connection connection = node0.getConnection(node1.getLocalNode());
            PlainActionFuture<Void> closed = new PlainActionFuture<>();
            exchange0.newRemoteSink(task, exchangeId, node0, connection).close(closed);
            closed.actionGet(10, TimeUnit.SECONDS);
            assertTrue(completionRan.get());
            assertTrue(exchange1.isEmpty());
            // The marker stays until it expires, so a retried compute request also sees the close.
            assertNull(exchange1.getSinkHandlerIfOpen(exchangeId));
            assertNull(exchange1.getSinkHandlerIfOpen(exchangeId));

            PlainActionFuture<Void> closedAgain = new PlainActionFuture<>();
            exchange0.newRemoteSink(task, exchangeId, node0, connection).close(closedAgain);
            closedAgain.actionGet(10, TimeUnit.SECONDS);
            assertTrue(exchange1.isEmpty());

            // No completion listener: the transport handler itself must drop the sink after sourcesFinished.
            String bareId = "exchange-bare-" + randomAlphaOfLength(8);
            exchange1.createSinkHandler(bareId, randomExchangeBuffer());
            PlainActionFuture<Void> bareClosed = new PlainActionFuture<>();
            exchange0.newRemoteSink(task, bareId, node0, connection).close(bareClosed);
            bareClosed.actionGet(10, TimeUnit.SECONDS);
            assertTrue("sink with no completion listener must be unregistered", exchange1.isEmpty());
            assertNull(exchange1.getSinkHandlerIfOpen(bareId));
        }
    }

    /**
     * A refusable open that arrives after admission closed is refused with a {@link NodeClosedException} from the target and leaves no
     * sink. Opens that cannot be retried elsewhere, through either action name, are still admitted after the cutoff and are not waited
     * for by the drain.
     */
    public void testAdmissionClosedRefusesOnlyRefusableOpens() {
        MockTransportService node0 = newTransportService();
        ExchangeService exchange0 = new ExchangeService(Settings.EMPTY, threadPool, ESQL_TEST_EXECUTOR, blockFactory());
        exchange0.registerTransportHandler(node0);
        MockTransportService node1 = newTransportService();
        ExchangeService exchange1 = new ExchangeService(Settings.EMPTY, threadPool, ESQL_TEST_EXECUTOR, blockFactory());
        exchange1.registerTransportHandler(node1);
        AbstractSimpleTransportTestCase.connectToNode(node0, node1.getLocalNode());
        try (exchange0; exchange1; node0; node1) {
            Transport.Connection connection = node0.getConnection(node1.getLocalNode());
            SubscribableListener<Void> drained = exchange1.closeAdmission();
            assertTrue("nothing was admitted, so the drain completes right away", drained.isDone());
            assertTrue(exchange1.isAdmissionClosed());

            PlainActionFuture<Void> refused = new PlainActionFuture<>();
            ExchangeService.openExchange(node0, connection, "refused", randomExchangeBuffer(), true, threadPool.generic(), refused);
            // get() rather than actionGet(), which would unwrap the remote exception the coordinator classifies
            Throwable e = expectThrows(ExecutionException.class, () -> refused.get(10, TimeUnit.SECONDS)).getCause();
            assertThat(e, instanceOf(RemoteTransportException.class));
            assertThat(ExceptionsHelper.unwrapCause(e), instanceOf(NodeClosedException.class));
            assertTrue("a refused open must not create a sink", exchange1.isEmpty());

            PlainActionFuture<Void> admitted = new PlainActionFuture<>();
            ExchangeService.openExchange(node0, connection, "admitted", randomExchangeBuffer(), false, threadPool.generic(), admitted);
            admitted.actionGet(10, TimeUnit.SECONDS);

            PlainActionFuture<ActionResponse.Empty> admittedForCcs = new PlainActionFuture<>();
            node0.sendRequest(
                connection,
                ExchangeService.OPEN_EXCHANGE_ACTION_NAME_FOR_CCS,
                new ExchangeService.OpenExchangeRequest("admitted-ccs", randomExchangeBuffer(), false),
                TransportRequestOptions.EMPTY,
                new ActionListenerResponseHandler<>(admittedForCcs, in -> ActionResponse.Empty.INSTANCE, threadPool.generic())
            );
            admittedForCcs.actionGet(10, TimeUnit.SECONDS);
            assertThat(exchange1.sinkKeys(), equalTo(Set.of("admitted", "admitted-ccs")));
            assertThat(exchange1.closeAdmission(), sameInstance(drained));

            finishSink(exchange1, "admitted");
            finishSink(exchange1, "admitted-ccs");
        }
    }

    /**
     * The drain waits for every refusable exchange admitted before the cutoff, and only for those. Repeated closes return the same
     * listener.
     */
    public void testDrainWaitsForAdmittedRefusableExchanges() {
        try (ExchangeService exchange = new ExchangeService(Settings.EMPTY, threadPool, ESQL_TEST_EXECUTOR, blockFactory())) {
            assertTrue(exchange.admitOpen("refusable-1", randomExchangeBuffer(), true));
            assertTrue(exchange.admitOpen("refusable-2", randomExchangeBuffer(), true));
            assertTrue(exchange.admitOpen("non-refusable", randomExchangeBuffer(), false));
            assertFalse(exchange.isAdmissionClosed());

            SubscribableListener<Void> drained = exchange.closeAdmission();
            assertFalse(drained.isDone());
            assertThat(exchange.closeAdmission(), sameInstance(drained));
            assertFalse("refused after the cutoff", exchange.admitOpen("late", randomExchangeBuffer(), true));
            assertFalse(exchange.sinkKeys().contains("late"));
            assertTrue("still admitted after the cutoff", exchange.admitOpen("late-non-refusable", randomExchangeBuffer(), false));

            finishSink(exchange, "refusable-1");
            assertFalse(drained.isDone());
            finishSink(exchange, "non-refusable");
            finishSink(exchange, "refusable-1"); // a second finish of the same exchange is a no-op
            assertFalse(drained.isDone());
            finishSink(exchange, "refusable-2");
            assertTrue(drained.isDone());
            assertThat(exchange.sinkKeys(), equalTo(Set.of("late-non-refusable")));
            finishSink(exchange, "late-non-refusable");
        }
    }

    /**
     * Opens, finishes and the cutoff race. Every open is either admitted, with a sink, or refused, without one, and the drain completes
     * only once every admitted refusable exchange is gone.
     */
    public void testCloseAdmissionRacesOpensAndFinishes() throws InterruptedException {
        try (ExchangeService exchange = new ExchangeService(Settings.EMPTY, threadPool, ESQL_TEST_EXECUTOR, blockFactory())) {
            int numOpeners = between(2, 8);
            int opensPerThread = between(10, 200);
            int closeAfter = between(0, numOpeners * opensPerThread / 2);
            AtomicInteger opens = new AtomicInteger();
            Set<String> admittedRefusable = ConcurrentCollections.newConcurrentSet();
            AtomicInteger refused = new AtomicInteger();
            AtomicBoolean closed = new AtomicBoolean();
            CyclicBarrier barrier = new CyclicBarrier(numOpeners + 1);
            List<Thread> threads = new ArrayList<>();
            for (int t = 0; t < numOpeners; t++) {
                int thread = t;
                threads.add(new Thread(() -> {
                    safeAwait(barrier);
                    for (int i = 0; i < opensPerThread; i++) {
                        String id = "exchange-" + thread + "-" + i;
                        boolean refusable = randomBoolean();
                        boolean closedBefore = closed.get();
                        opens.incrementAndGet();
                        if (exchange.admitOpen(id, randomExchangeBuffer(), refusable)) {
                            assertFalse("admitted a refusable open after the cutoff", refusable && closedBefore);
                            if (refusable) {
                                admittedRefusable.add(id);
                            }
                            if (randomBoolean()) {
                                Thread.yield();
                            }
                            finishSink(exchange, id);
                        } else {
                            assertTrue("refused an open that cannot be retried", refusable);
                            assertFalse("a refused open must not create a sink", exchange.sinkKeys().contains(id));
                            refused.incrementAndGet();
                        }
                    }
                }));
            }
            threads.forEach(Thread::start);
            safeAwait(barrier);
            // an opener that died on a failed assertion must not leave this waiting for opens that will not come
            while (opens.get() < closeAfter && threads.stream().anyMatch(Thread::isAlive)) {
                Thread.onSpinWait();
            }
            SubscribableListener<Void> drained = exchange.closeAdmission();
            // only set once the cutoff is in place, so an opener that sees it must be refused
            closed.set(true);
            drained.addListener(ActionListener.running(() -> {
                for (String id : admittedRefusable) {
                    assertFalse("drain completed while [" + id + "] was still open", exchange.sinkKeys().contains(id));
                }
            }));
            for (Thread thread : threads) {
                thread.join();
            }
            assertTrue(drained.isDone());
            safeAwait(drained);
            assertTrue(exchange.isEmpty());
            logger.info("admitted [{}] refusable exchanges, refused [{}]", admittedRefusable.size(), refused.get());
        }
    }

    /**
     * An admitted exchange whose compute request never arrives is removed by the inactive-sink reaper, which completes the drain.
     */
    public void testReaperRemovesUnclaimedAdmittedExchange() throws Exception {
        Settings settings = Settings.builder().put(ExchangeService.INACTIVE_SINKS_INTERVAL_SETTING, TimeValue.timeValueMillis(100)).build();
        try (ExchangeService exchange = new ExchangeService(settings, threadPool, ESQL_TEST_EXECUTOR, blockFactory())) {
            assertTrue(exchange.admitOpen("unclaimed", randomExchangeBuffer(), true));
            SubscribableListener<Void> drained = exchange.closeAdmission();
            safeAwait(drained);
            assertTrue(exchange.isEmpty());
        }
    }

    /**
     * A coordinator that gives up on an exchange it opened removes the sink on the target without a parent task, so the target's drain
     * does not wait for the inactive-sink reaper.
     */
    public void testFinishUnusedRemoteSinkCompletesDrain() {
        MockTransportService node0 = newTransportService();
        ExchangeService exchange0 = new ExchangeService(Settings.EMPTY, threadPool, ESQL_TEST_EXECUTOR, blockFactory());
        exchange0.registerTransportHandler(node0);
        MockTransportService node1 = newTransportService();
        ExchangeService exchange1 = new ExchangeService(Settings.EMPTY, threadPool, ESQL_TEST_EXECUTOR, blockFactory());
        exchange1.registerTransportHandler(node1);
        AbstractSimpleTransportTestCase.connectToNode(node0, node1.getLocalNode());
        try (exchange0; exchange1; node0; node1) {
            Transport.Connection connection = node0.getConnection(node1.getLocalNode());
            PlainActionFuture<Void> opened = new PlainActionFuture<>();
            ExchangeService.openExchange(node0, connection, "unused", randomExchangeBuffer(), true, threadPool.generic(), opened);
            opened.actionGet(10, TimeUnit.SECONDS);
            SubscribableListener<Void> drained = exchange1.closeAdmission();
            assertFalse(drained.isDone());

            exchange0.finishUnusedRemoteSink(node0, connection, "unused");
            safeAwait(drained);
            assertTrue(exchange1.isEmpty());
        }
    }

    /**
     * Closing a remote sink whose task is cancelled still reaches the target and removes the sink there, even though the target
     * holds a ban on that task. The close ({@code sourcesFinished=true}) masks its parent task, so neither the coordinator's
     * refusal of new child connections nor the target's ban applies to it. The close is reached either directly or through a fetch
     * that fails because the task is cancelled.
     */
    public void testCloseUnderCancelledParentReachesTarget() throws Exception {
        MockTransportService node0 = newTransportService();
        ExchangeService exchange0 = new ExchangeService(Settings.EMPTY, threadPool, ESQL_TEST_EXECUTOR, blockFactory());
        exchange0.registerTransportHandler(node0);
        MockTransportService node1 = newTransportService();
        ExchangeService exchange1 = new ExchangeService(Settings.EMPTY, threadPool, ESQL_TEST_EXECUTOR, blockFactory());
        exchange1.registerTransportHandler(node1);
        AbstractSimpleTransportTestCase.connectToNode(node0, node1.getLocalNode());
        // A fetch for "holder" is held on the target and never answered until the end, so the target keeps the ban on the parent task
        // for the rest of the test: the ban is lifted once no child request of the task is outstanding.
        PlainActionFuture<TransportChannel> heldFetch = new PlainActionFuture<>();
        node1.addRequestHandlingBehavior(ExchangeService.EXCHANGE_ACTION_NAME, (handler, request, channel, task) -> {
            if (request instanceof ExchangeRequest r && r.exchangeId().equals("holder") && r.sourcesFinished() == false) {
                heldFetch.onResponse(channel);
            } else {
                handler.messageReceived(request, channel, task);
            }
        });
        try (exchange0; exchange1; node0; node1) {
            Transport.Connection connection = node0.getConnection(node1.getLocalNode());
            for (String exchangeId : List.of("holder", "closed")) {
                PlainActionFuture<Void> opened = new PlainActionFuture<>();
                ExchangeService.openExchange(node0, connection, exchangeId, randomExchangeBuffer(), true, threadPool.generic(), opened);
                opened.actionGet(10, TimeUnit.SECONDS);
            }
            CancellableTask parent = (CancellableTask) node0.getTaskManager().register("transport", "test", new CancellableTestRequest());
            try {
                RemoteSink holder = exchange0.newRemoteSink(parent, "holder", node0, connection);
                PlainActionFuture<ExchangeResponse> holderFetch = new PlainActionFuture<>();
                holder.fetchPageAsync(false, holderFetch);
                TransportChannel holderChannel = heldFetch.get(10, TimeUnit.SECONDS);

                node0.getTaskManager().cancelTaskAndDescendants(parent, "test", false, ActionListener.noop());
                TaskId parentId = new TaskId(node0.getLocalNode().getId(), parent.getId());
                assertBusy(() -> assertTrue(node1.getTaskManager().getBannedTaskIds().contains(parentId)));
                assertTrue(parent.isCancelled());

                SubscribableListener<Void> drained = exchange1.closeAdmission();
                assertFalse(drained.isDone());
                RemoteSink closed = exchange0.newRemoteSink(parent, "closed", node0, connection);
                if (randomBoolean()) {
                    PlainActionFuture<Void> closeDone = new PlainActionFuture<>();
                    closed.close(closeDone);
                    closeDone.actionGet(10, TimeUnit.SECONDS);
                } else {
                    // The fetch is a child request of the cancelled task, so the coordinator refuses to send it and closes the sink.
                    PlainActionFuture<ExchangeResponse> fetch = new PlainActionFuture<>();
                    closed.fetchPageAsync(false, fetch);
                    Exception e = expectThrows(ExecutionException.class, () -> fetch.get(10, TimeUnit.SECONDS));
                    assertNotNull(ExceptionsHelper.unwrap(e, TaskCancelledException.class));
                }
                assertTrue(
                    "the ban must still be in place when the close arrives",
                    node1.getTaskManager().getBannedTaskIds().contains(parentId)
                );
                assertThat(exchange1.sinkKeys(), equalTo(Set.of("holder")));

                holderChannel.sendResponse(new TaskCancelledException("test"));
                expectThrows(ExecutionException.class, () -> holderFetch.get(10, TimeUnit.SECONDS));
                finishSink(exchange1, "holder");
                safeAwait(drained);
                assertTrue(exchange1.isEmpty());
            } finally {
                node0.getTaskManager().unregister(parent);
            }
        }
    }

    /**
     * Registers a {@link CancellableTask} on the coordinator to stand in for the query's task.
     */
    private static class CancellableTestRequest extends AbstractTransportRequest {
        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, "test", parentTaskId, headers);
        }
    }

    /**
     * A sender from before the refusable flag never sends it, so its opens are admitted after the cutoff and the drain does not wait
     * for them.
     */
    public void testOpenFromOlderSenderIsAdmittedAfterCutoff() throws Exception {
        assertOpenOverOlderTransportVersionIsAdmittedAfterCutoff(true);
    }

    /**
     * A target from before the refusable flag never reads it: the sender leaves it out, and the open is admitted as before.
     */
    public void testOpenToOlderTargetIsAdmittedAfterCutoff() throws Exception {
        assertOpenOverOlderTransportVersionIsAdmittedAfterCutoff(false);
    }

    /**
     * Opens a refusable exchange over a real connection that negotiated the transport version before the flag. The older side runs
     * the current code at that version, which is how the wire format of an older node is checked here.
     */
    private void assertOpenOverOlderTransportVersionIsAdmittedAfterCutoff(boolean olderSender) throws Exception {
        TransportVersion before = TransportVersionUtils.getPreviousVersion(ExchangeService.ESQL_REFUSABLE_OPEN_EXCHANGE);
        MockTransportService node0 = newTransportService(olderSender ? before : TransportVersion.current());
        ExchangeService exchange0 = new ExchangeService(Settings.EMPTY, threadPool, ESQL_TEST_EXECUTOR, blockFactory());
        exchange0.registerTransportHandler(node0);
        MockTransportService node1 = newTransportService(olderSender ? TransportVersion.current() : before);
        ExchangeService exchange1 = new ExchangeService(Settings.EMPTY, threadPool, ESQL_TEST_EXECUTOR, blockFactory());
        exchange1.registerTransportHandler(node1);
        AbstractSimpleTransportTestCase.connectToNode(node0, node1.getLocalNode());
        try (exchange0; exchange1; node0; node1) {
            Transport.Connection connection = node0.getConnection(node1.getLocalNode());
            assertThat(connection.getTransportVersion(), equalTo(before));
            SubscribableListener<Void> drained = exchange1.closeAdmission();
            assertTrue(drained.isDone());

            PlainActionFuture<Void> opened = new PlainActionFuture<>();
            ExchangeService.openExchange(node0, connection, "older", randomExchangeBuffer(), true, threadPool.generic(), opened);
            opened.actionGet(10, TimeUnit.SECONDS);
            assertThat(exchange1.sinkKeys(), equalTo(Set.of("older")));

            exchange0.finishUnusedRemoteSink(node0, connection, "older");
            assertBusy(() -> assertTrue(exchange1.isEmpty()));
        }
    }

    public void testOpenExchangeRequestSerialization() throws IOException {
        NamedWriteableRegistry registry = new NamedWriteableRegistry(List.of());
        String sessionId = randomAlphaOfLength(10);
        int exchangeBuffer = randomExchangeBuffer();
        boolean refusable = randomBoolean();
        var request = new ExchangeService.OpenExchangeRequest(sessionId, exchangeBuffer, refusable);

        var current = copyWriteable(request, registry, ExchangeService.OpenExchangeRequest::new, TransportVersion.current());
        assertThat(current.sessionId(), equalTo(sessionId));
        assertThat(current.exchangeBuffer(), equalTo(exchangeBuffer));
        assertThat(current.refusable(), equalTo(refusable));

        // A node that predates the flag neither sends nor reads it: its opens are always admitted.
        TransportVersion before = TransportVersionUtils.getPreviousVersion(ExchangeService.ESQL_REFUSABLE_OPEN_EXCHANGE);
        var old = copyWriteable(request, registry, ExchangeService.OpenExchangeRequest::new, before);
        assertThat(old.sessionId(), equalTo(sessionId));
        assertThat(old.exchangeBuffer(), equalTo(exchangeBuffer));
        assertFalse(old.refusable());
    }

    /**
     * Removes the sink as its compute would; the failure finishes a sink that no driver used.
     */
    private static void finishSink(ExchangeService exchange, String exchangeId) {
        exchange.finishSinkHandler(exchangeId, new TaskCancelledException("test"));
    }

    private MockTransportService newTransportService() {
        return newTransportService(TransportVersion.current());
    }

    private MockTransportService newTransportService(TransportVersion transportVersion) {
        List<NamedWriteableRegistry.Entry> namedWriteables = new ArrayList<>(ClusterModule.getNamedWriteables());
        NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(namedWriteables);
        MockTransportService service = MockTransportService.createNewService(
            Settings.EMPTY,
            MockTransportService.newMockTransport(Settings.EMPTY, transportVersion, threadPool, namedWriteableRegistry),
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
        return new DriverContext(blockFactory.bigArrays(), blockFactory, null);
    }

    private BlockFactory blockFactory() {
        MockBigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofGb(1));
        breakers.add(bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST));
        MockBlockFactory factory = new MockBlockFactory(BlockFactory.builder(bigArrays));
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
