/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.exchange;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.node.VersionInformation;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.MockBlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.DriverRunner;
import org.elasticsearch.compute.operator.SinkOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.core.ReleasableRef;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancellationService;
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
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

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
        ExchangeSink sink1 = sinkExchanger.createExchangeSink();
        ExchangeSink sink2 = sinkExchanger.createExchangeSink();
        ExchangeSourceHandler sourceExchanger = new ExchangeSourceHandler(3, threadPool.executor(ESQL_TEST_EXECUTOR));
        PlainActionFuture<Void> sourceCompletion = new PlainActionFuture<>();
        sourceExchanger.addCompletionListener(sourceCompletion);
        ExchangeSource source = sourceExchanger.createExchangeSource();
        sourceExchanger.addRemoteSink(sinkExchanger::fetchPageAsync, 1);
        SubscribableListener<Void> waitForReading = source.waitForReading();
        assertFalse(waitForReading.isDone());
        assertNull(source.pollPage());
        assertTrue(sink1.waitForWriting().isDone());
        randomFrom(sink1, sink2).addPage(pages[0]);
        randomFrom(sink1, sink2).addPage(pages[1]);
        // source and sink buffers can store 5 pages
        for (Page p : List.of(pages[2], pages[3], pages[4])) {
            ExchangeSink sink = randomFrom(sink1, sink2);
            assertBusy(() -> assertTrue(sink.waitForWriting().isDone()));
            sink.addPage(p);
        }
        // sink buffer is full
        assertFalse(randomFrom(sink1, sink2).waitForWriting().isDone());
        assertBusy(() -> assertTrue(source.waitForReading().isDone()));
        assertEquals(pages[0], source.pollPage());
        assertBusy(() -> assertTrue(source.waitForReading().isDone()));
        assertEquals(pages[1], source.pollPage());
        // sink can write again
        assertBusy(() -> assertTrue(randomFrom(sink1, sink2).waitForWriting().isDone()));
        randomFrom(sink1, sink2).addPage(pages[5]);
        assertBusy(() -> assertTrue(randomFrom(sink1, sink2).waitForWriting().isDone()));
        randomFrom(sink1, sink2).addPage(pages[6]);
        // sink buffer is full
        assertFalse(randomFrom(sink1, sink2).waitForWriting().isDone());
        sink1.finish();
        assertTrue(sink1.isFinished());
        for (int i = 0; i < 5; i++) {
            assertBusy(() -> assertTrue(source.waitForReading().isDone()));
            assertEquals(pages[2 + i], source.pollPage());
        }
        // source buffer is empty
        assertFalse(source.waitForReading().isDone());
        assertBusy(() -> assertTrue(sink2.waitForWriting().isDone()));
        sink2.finish();
        assertTrue(sink2.isFinished());
        assertTrue(source.isFinished());
        assertFalse(sourceCompletion.isDone());
        source.finish();
        sourceCompletion.actionGet(10, TimeUnit.SECONDS);
        ESTestCase.terminate(threadPool);
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

    void runConcurrentTest(
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
            String description = "sink-" + i;
            ExchangeSinkOperator sinkOperator = new ExchangeSinkOperator(exchangeSink.get(), Function.identity());
            DriverContext dc = driverContext();
            Driver d = new Driver(
                "test-session:1",
                0,
                0,
                dc,
                () -> description,
                seqNoGenerator.get(dc),
                List.of(),
                sinkOperator,
                Driver.DEFAULT_STATUS_INTERVAL,
                () -> {}
            );
            drivers.add(d);
        }
        for (int i = 0; i < numSources; i++) {
            String description = "source-" + i;
            ExchangeSourceOperator sourceOperator = new ExchangeSourceOperator(exchangeSource.get());
            DriverContext dc = driverContext();
            Driver d = new Driver(
                "test-session:2",
                0,
                0,
                dc,
                () -> description,
                sourceOperator,
                List.of(),
                seqNoCollector.get(dc),
                Driver.DEFAULT_STATUS_INTERVAL,
                () -> {}
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
        var expectedSeqNos = IntStream.range(0, Math.min(maxInputSeqNo, maxOutputSeqNo)).boxed().collect(Collectors.toSet());
        assertThat(seqNoCollector.receivedSeqNos, hasSize(expectedSeqNos.size()));
        assertThat(seqNoCollector.receivedSeqNos, equalTo(expectedSeqNos));
    }

    public void testConcurrentWithHandlers() {
        BlockFactory blockFactory = blockFactory();
        PlainActionFuture<Void> sourceCompletionFuture = new PlainActionFuture<>();
        var sourceExchanger = new ExchangeSourceHandler(randomExchangeBuffer(), threadPool.executor(ESQL_TEST_EXECUTOR));
        sourceExchanger.addCompletionListener(sourceCompletionFuture);
        List<ExchangeSinkHandler> sinkHandlers = new ArrayList<>();
        Supplier<ExchangeSink> exchangeSink = () -> {
            final ExchangeSinkHandler sinkHandler;
            if (sinkHandlers.isEmpty() == false && randomBoolean()) {
                sinkHandler = randomFrom(sinkHandlers);
            } else {
                sinkHandler = new ExchangeSinkHandler(blockFactory, randomExchangeBuffer(), threadPool.relativeTimeInMillisSupplier());
                sourceExchanger.addRemoteSink(sinkHandler::fetchPageAsync, randomIntBetween(1, 3));
                sinkHandlers.add(sinkHandler);
            }
            return sinkHandler.createExchangeSink();
        };
        final int maxInputSeqNo = rarely() ? -1 : randomIntBetween(0, 50_000);
        final int maxOutputSeqNo = rarely() ? -1 : randomIntBetween(0, 50_000);
        runConcurrentTest(maxInputSeqNo, maxOutputSeqNo, sourceExchanger::createExchangeSource, exchangeSink);
        sourceCompletionFuture.actionGet(10, TimeUnit.SECONDS);
    }

    public void testEarlyTerminate() {
        BlockFactory blockFactory = blockFactory();
        IntBlock block1 = blockFactory.newConstantIntBlockWith(1, 2);
        IntBlock block2 = blockFactory.newConstantIntBlockWith(1, 2);
        Page p1 = new Page(block1);
        Page p2 = new Page(block2);
        ExchangeSinkHandler sinkExchanger = new ExchangeSinkHandler(blockFactory, 2, threadPool.relativeTimeInMillisSupplier());
        ExchangeSink sink = sinkExchanger.createExchangeSink();
        sink.addPage(p1);
        sink.addPage(p2);
        assertFalse(sink.waitForWriting().isDone());
        PlainActionFuture<ExchangeResponse> future = new PlainActionFuture<>();
        sinkExchanger.fetchPageAsync(true, future);
        ExchangeResponse resp = future.actionGet();
        assertTrue(resp.finished());
        assertNull(resp.takePage());
        assertTrue(sink.waitForWriting().isDone());
        assertTrue(sink.isFinished());
    }

    public void testConcurrentWithTransportActions() {
        MockTransportService node0 = newTransportService();
        ExchangeService exchange0 = new ExchangeService(Settings.EMPTY, threadPool, ESQL_TEST_EXECUTOR, blockFactory());
        exchange0.registerTransportHandler(node0);
        MockTransportService node1 = newTransportService();
        ExchangeService exchange1 = new ExchangeService(Settings.EMPTY, threadPool, ESQL_TEST_EXECUTOR, blockFactory());
        exchange1.registerTransportHandler(node1);
        AbstractSimpleTransportTestCase.connectToNode(node0, node1.getLocalNode());

        try (exchange0; exchange1; node0; node1) {
            String exchangeId = "exchange";
            Task task = new Task(1, "", "", "", null, Collections.emptyMap());
            var sourceHandler = new ExchangeSourceHandler(randomExchangeBuffer(), threadPool.executor(ESQL_TEST_EXECUTOR));
            PlainActionFuture<Void> sourceCompletionFuture = new PlainActionFuture<>();
            sourceHandler.addCompletionListener(sourceCompletionFuture);
            ExchangeSinkHandler sinkHandler = exchange1.createSinkHandler(exchangeId, randomExchangeBuffer());
            Transport.Connection connection = node0.getConnection(node1.getLocalNode());
            sourceHandler.addRemoteSink(exchange0.newRemoteSink(task, exchangeId, node0, connection), randomIntBetween(1, 5));
            final int maxInputSeqNo = rarely() ? -1 : randomIntBetween(0, 50_000);
            final int maxOutputSeqNo = rarely() ? -1 : randomIntBetween(0, 50_000);
            runConcurrentTest(maxInputSeqNo, maxOutputSeqNo, sourceHandler::createExchangeSource, sinkHandler::createExchangeSink);
            sourceCompletionFuture.actionGet(10, TimeUnit.SECONDS);
        }
    }

    public void testFailToRespondPage() {
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
            PlainActionFuture<Void> sourceCompletionFuture = new PlainActionFuture<>();
            sourceHandler.addCompletionListener(sourceCompletionFuture);
            ExchangeSinkHandler sinkHandler = exchange1.createSinkHandler(exchangeId, randomIntBetween(1, 128));
            Transport.Connection connection = node0.getConnection(node1.getLocalDiscoNode());
            sourceHandler.addRemoteSink(exchange0.newRemoteSink(task, exchangeId, node0, connection), randomIntBetween(1, 5));
            Exception err = expectThrows(
                Exception.class,
                () -> runConcurrentTest(maxSeqNo, maxSeqNo, sourceHandler::createExchangeSource, sinkHandler::createExchangeSink)
            );
            Throwable cause = ExceptionsHelper.unwrap(err, IOException.class);
            assertNotNull(cause);
            assertThat(cause.getMessage(), equalTo("page is too large"));
            sinkHandler.onFailure(new RuntimeException(cause));
            sourceCompletionFuture.actionGet(10, TimeUnit.SECONDS);
        }
    }

    private MockTransportService newTransportService() {
        List<NamedWriteableRegistry.Entry> namedWriteables = new ArrayList<>(ClusterModule.getNamedWriteables());
        namedWriteables.addAll(Block.getNamedWriteables());
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
