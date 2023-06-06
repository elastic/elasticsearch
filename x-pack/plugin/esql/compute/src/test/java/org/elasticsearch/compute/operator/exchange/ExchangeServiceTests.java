/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.exchange;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ListenableActionFuture;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.ConstantIntVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.DriverRunner;
import org.elasticsearch.compute.operator.SinkOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.test.transport.StubbableTransport;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.transport.AbstractSimpleTransportTestCase;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class ExchangeServiceTests extends ESTestCase {

    private TestThreadPool threadPool;

    @Before
    public void setThreadPool() {
        int numThreads = randomBoolean() ? 1 : between(2, 16);
        threadPool = new TestThreadPool(
            "test",
            new FixedExecutorBuilder(Settings.EMPTY, "esql_test_executor", numThreads, 1024, "esql", false)
        );
    }

    @After
    public void shutdownThreadPool() {
        terminate(threadPool);
    }

    public void testBasic() throws Exception {
        Page[] pages = new Page[7];
        for (int i = 0; i < pages.length; i++) {
            pages[i] = new Page(new ConstantIntVector(i, 2).asBlock());
        }
        ExchangeSinkHandler sinkExchanger = new ExchangeSinkHandler(2);
        ExchangeSink sink1 = sinkExchanger.createExchangeSink();
        ExchangeSink sink2 = sinkExchanger.createExchangeSink();
        ExchangeSourceHandler sourceExchanger = new ExchangeSourceHandler(3, threadPool.executor("esql_test_executor"));
        ExchangeSource source = sourceExchanger.createExchangeSource();
        sourceExchanger.addRemoteSink(sinkExchanger::fetchPageAsync, 1);
        ListenableActionFuture<Void> waitForReading = source.waitForReading();
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
        ESTestCase.terminate(threadPool);
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
                    IntBlock.Builder builder = IntBlock.newBlockBuilder(size);
                    for (int i = 0; i < size; i++) {
                        int seqNo = nextSeqNo.incrementAndGet();
                        if (seqNo < maxInputSeqNo) {
                            builder.appendInt(seqNo);
                        }
                    }
                    return new Page(builder.build());
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
                public void addInput(Page page) {
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
            ExchangeSinkOperator sinkOperator = new ExchangeSinkOperator(exchangeSink.get());
            DriverContext dc = new DriverContext();
            Driver d = new Driver("test-session:1", dc, () -> description, seqNoGenerator.get(dc), List.of(), sinkOperator, () -> {});
            drivers.add(d);
        }
        for (int i = 0; i < numSources; i++) {
            String description = "source-" + i;
            ExchangeSourceOperator sourceOperator = new ExchangeSourceOperator(exchangeSource.get());
            DriverContext dc = new DriverContext();
            Driver d = new Driver("test-session:2", dc, () -> description, sourceOperator, List.of(), seqNoCollector.get(dc), () -> {});
            drivers.add(d);
        }
        PlainActionFuture<Void> future = new PlainActionFuture<>();
        new DriverRunner() {
            @Override
            protected void start(Driver driver, ActionListener<Void> listener) {
                Driver.start(threadPool.executor("esql_test_executor"), driver, listener);
            }
        }.runToCompletion(drivers, future);
        future.actionGet(TimeValue.timeValueMinutes(1));
        var expectedSeqNos = IntStream.range(0, Math.min(maxInputSeqNo, maxOutputSeqNo)).boxed().collect(Collectors.toSet());
        assertThat(seqNoCollector.receivedSeqNos, hasSize(expectedSeqNos.size()));
        assertThat(seqNoCollector.receivedSeqNos, equalTo(expectedSeqNos));
    }

    public void testConcurrentWithHandlers() {
        var sourceExchanger = new ExchangeSourceHandler(randomExchangeBuffer(), threadPool.executor("esql_test_executor"));
        List<ExchangeSinkHandler> sinkHandlers = new ArrayList<>();
        Supplier<ExchangeSink> exchangeSink = () -> {
            final ExchangeSinkHandler sinkHandler;
            if (sinkHandlers.isEmpty() == false && randomBoolean()) {
                sinkHandler = randomFrom(sinkHandlers);
            } else {
                sinkHandler = new ExchangeSinkHandler(randomExchangeBuffer());
                sourceExchanger.addRemoteSink(sinkHandler::fetchPageAsync, randomIntBetween(1, 3));
                sinkHandlers.add(sinkHandler);
            }
            return sinkHandler.createExchangeSink();
        };
        final int maxInputSeqNo = rarely() ? -1 : randomIntBetween(0, 50_000);
        final int maxOutputSeqNo = rarely() ? -1 : randomIntBetween(0, 50_000);
        runConcurrentTest(maxInputSeqNo, maxOutputSeqNo, sourceExchanger::createExchangeSource, exchangeSink);
    }

    public void testEarlyTerminate() {
        IntBlock block = new ConstantIntVector(1, 2).asBlock();
        Page p1 = new Page(block);
        Page p2 = new Page(block);
        ExchangeSinkHandler sinkExchanger = new ExchangeSinkHandler(2);
        ExchangeSink sink = sinkExchanger.createExchangeSink();
        sink.addPage(p1);
        sink.addPage(p2);
        assertFalse(sink.waitForWriting().isDone());
        PlainActionFuture<ExchangeResponse> future = new PlainActionFuture<>();
        sinkExchanger.fetchPageAsync(true, future);
        ExchangeResponse resp = future.actionGet();
        assertTrue(resp.finished());
        assertNull(resp.page());
        assertTrue(sink.waitForWriting().isDone());
        assertTrue(sink.isFinished());
    }

    public void testConcurrentWithTransportActions() throws Exception {
        MockTransportService node0 = newTransportService();
        ExchangeService exchange0 = new ExchangeService(Settings.EMPTY, threadPool);
        exchange0.registerTransportHandler(node0);
        MockTransportService node1 = newTransportService();
        ExchangeService exchange1 = new ExchangeService(Settings.EMPTY, threadPool);
        exchange1.registerTransportHandler(node1);
        AbstractSimpleTransportTestCase.connectToNode(node0, node1.getLocalNode());

        try (exchange0; exchange1; node0; node1) {
            String exchangeId = "exchange";
            Task task = new Task(1, "", "", "", null, Collections.emptyMap());
            ExchangeSourceHandler sourceHandler = exchange0.createSourceHandler(exchangeId, randomExchangeBuffer(), "esql_test_executor");
            ExchangeSinkHandler sinkHandler = exchange1.createSinkHandler(exchangeId, randomExchangeBuffer());
            sourceHandler.addRemoteSink(exchange0.newRemoteSink(task, exchangeId, node0, node1.getLocalNode()), randomIntBetween(1, 5));
            final int maxInputSeqNo = rarely() ? -1 : randomIntBetween(0, 50_000);
            final int maxOutputSeqNo = rarely() ? -1 : randomIntBetween(0, 50_000);
            runConcurrentTest(maxInputSeqNo, maxOutputSeqNo, sourceHandler::createExchangeSource, sinkHandler::createExchangeSink);
        }
    }

    public void testFailToRespondPage() throws Exception {
        Settings settings = Settings.builder().build();
        MockTransportService node0 = newTransportService();
        ExchangeService exchange0 = new ExchangeService(settings, threadPool);
        exchange0.registerTransportHandler(node0);
        MockTransportService node1 = newTransportService();
        ExchangeService exchange1 = new ExchangeService(settings, threadPool);
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
                    public void sendResponse(TransportResponse response) throws IOException {
                        ExchangeResponse exchangeResponse = (ExchangeResponse) response;
                        if (exchangeResponse.page() != null) {
                            IntBlock block = exchangeResponse.page().getBlock(0);
                            for (int i = 0; i < block.getPositionCount(); i++) {
                                if (block.getInt(i) == disconnectOnSeqNo) {
                                    throw new IOException("page is too large");
                                }
                            }
                        }
                        super.sendResponse(response);
                    }
                };
                handler.messageReceived(request, filterChannel, task);
            }
        });
        try (exchange0; exchange1; node0; node1) {
            String exchangeId = "exchange";
            Task task = new Task(1, "", "", "", null, Collections.emptyMap());
            ExchangeSourceHandler sourceHandler = exchange0.createSourceHandler(exchangeId, randomIntBetween(1, 128), "esql_test_executor");
            ExchangeSinkHandler sinkHandler = exchange1.createSinkHandler(exchangeId, randomIntBetween(1, 128));
            sourceHandler.addRemoteSink(exchange0.newRemoteSink(task, exchangeId, node0, node1.getLocalNode()), randomIntBetween(1, 5));
            Exception err = expectThrows(
                Exception.class,
                () -> runConcurrentTest(maxSeqNo, maxSeqNo, sourceHandler::createExchangeSource, sinkHandler::createExchangeSink)
            );
            Throwable cause = ExceptionsHelper.unwrap(err, IOException.class);
            assertNotNull(cause);
            assertThat(cause.getMessage(), equalTo("page is too large"));
        }
    }

    public void testTimeoutExchangeRequest() throws Exception {
        int inactiveTimeoutInMillis = between(1, 100);
        Settings settings = Settings.builder()
            .put(ExchangeService.INACTIVE_TIMEOUT_SETTING.getKey(), TimeValue.timeValueMillis(inactiveTimeoutInMillis))
            .build();
        MockTransportService node0 = newTransportService();
        ExchangeService exchange0 = new ExchangeService(settings, threadPool);
        exchange0.registerTransportHandler(node0);
        MockTransportService node1 = newTransportService();
        ExchangeService exchange1 = new ExchangeService(settings, threadPool);
        exchange1.registerTransportHandler(node1);
        AbstractSimpleTransportTestCase.connectToNode(node0, node1.getLocalNode());
        // exchange source will retry the timed out response
        CountDownLatch latch = new CountDownLatch(between(1, 5));
        node1.addRequestHandlingBehavior(ExchangeService.EXCHANGE_ACTION_NAME, new StubbableTransport.RequestHandlingBehavior<>() {
            @Override
            public void messageReceived(
                TransportRequestHandler<TransportRequest> handler,
                TransportRequest request,
                TransportChannel channel,
                Task task
            ) throws Exception {
                handler.messageReceived(request, new FilterTransportChannel(channel) {
                    @Override
                    public void sendResponse(TransportResponse response) throws IOException {
                        latch.countDown();
                        super.sendResponse(response);
                    }

                    @Override
                    public void sendResponse(Exception exception) throws IOException {
                        latch.countDown();
                        super.sendResponse(exception);
                    }
                }, task);
            }
        });
        try (exchange0; exchange1; node0; node1) {
            String exchangeId = "exchange";
            Task task = new Task(1, "", "", "", null, Collections.emptyMap());
            final int maxInputSeqNo = rarely() ? -1 : randomIntBetween(0, 50_000);
            PlainActionFuture<Void> collectorFuture = new PlainActionFuture<>();
            {
                final int maxOutputSeqNo = randomIntBetween(1, 50_000);
                SeqNoCollector seqNoCollector = new SeqNoCollector(maxOutputSeqNo);
                ExchangeSourceHandler sourceHandler = exchange0.createSourceHandler(
                    exchangeId,
                    randomIntBetween(1, 128),
                    "esql_test_executor"
                );
                sourceHandler.addRemoteSink(exchange0.newRemoteSink(task, exchangeId, node0, node1.getLocalNode()), randomIntBetween(1, 5));
                int numSources = randomIntBetween(1, 10);
                List<Driver> sourceDrivers = new ArrayList<>(numSources);
                for (int i = 0; i < numSources; i++) {
                    String description = "source-" + i;
                    ExchangeSourceOperator sourceOperator = new ExchangeSourceOperator(sourceHandler.createExchangeSource());
                    DriverContext dc = new DriverContext();
                    Driver d = new Driver(description, dc, () -> description, sourceOperator, List.of(), seqNoCollector.get(dc), () -> {});
                    sourceDrivers.add(d);
                }
                new DriverRunner() {
                    @Override
                    protected void start(Driver driver, ActionListener<Void> listener) {
                        Driver.start(threadPool.executor("esql_test_executor"), driver, listener);
                    }
                }.runToCompletion(sourceDrivers, collectorFuture);
            }
            // Verify that some exchange requests are timed out because we don't have the exchange sink handler yet
            assertTrue(latch.await(10, TimeUnit.SECONDS));
            PlainActionFuture<Void> generatorFuture = new PlainActionFuture<>();
            {
                SeqNoGenerator seqNoGenerator = new SeqNoGenerator(maxInputSeqNo);
                int numSinks = randomIntBetween(1, 10);
                ExchangeSinkHandler sinkHandler = exchange1.createSinkHandler(exchangeId, randomIntBetween(1, 128));
                List<Driver> sinkDrivers = new ArrayList<>(numSinks);
                for (int i = 0; i < numSinks; i++) {
                    String description = "sink-" + i;
                    ExchangeSinkOperator sinkOperator = new ExchangeSinkOperator(sinkHandler.createExchangeSink());
                    DriverContext dc = new DriverContext();
                    Driver d = new Driver(description, dc, () -> description, seqNoGenerator.get(dc), List.of(), sinkOperator, () -> {});
                    sinkDrivers.add(d);
                }
                new DriverRunner() {
                    @Override
                    protected void start(Driver driver, ActionListener<Void> listener) {
                        Driver.start(threadPool.executor("esql_test_executor"), driver, listener);
                    }
                }.runToCompletion(sinkDrivers, generatorFuture);
            }
            generatorFuture.actionGet(1, TimeUnit.MINUTES);
            collectorFuture.actionGet(1, TimeUnit.MINUTES);
        }
    }

    private MockTransportService newTransportService() {
        List<NamedWriteableRegistry.Entry> namedWriteables = new ArrayList<>(ClusterModule.getNamedWriteables());
        namedWriteables.addAll(Block.getNamedWriteables());
        NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(namedWriteables);
        MockTransportService service = MockTransportService.createNewService(
            Settings.EMPTY,
            MockTransportService.newMockTransport(Settings.EMPTY, TransportVersion.CURRENT, threadPool, namedWriteableRegistry),
            Version.CURRENT,
            threadPool,
            null,
            Collections.emptySet()
        );
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
        public String getChannelType() {
            return in.getChannelType();
        }

        @Override
        public void sendResponse(TransportResponse response) throws IOException {
            in.sendResponse(response);
        }

        @Override
        public void sendResponse(Exception exception) throws IOException {
            in.sendResponse(exception);
        }
    }
}
