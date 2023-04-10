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
import org.elasticsearch.compute.operator.DriverRunner;
import org.elasticsearch.compute.operator.SinkOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.core.IOUtils;
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
        assertTrue(randomFrom(sink1, sink2).waitForWriting().isDone());
        randomFrom(sink1, sink2).addPage(pages[5]);
        assertTrue(randomFrom(sink1, sink2).waitForWriting().isDone());
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
        assertTrue(sink2.waitForWriting().isDone());
        sink2.finish();
        assertTrue(sink2.isFinished());
        assertTrue(source.isFinished());
        ESTestCase.terminate(threadPool);
    }

    void runConcurrentTest(
        int maxInputSeqNo,
        int maxOutputSeqNo,
        Supplier<ExchangeSource> exchangeSource,
        Supplier<ExchangeSink> exchangeSink
    ) {
        final AtomicInteger nextSeqNo = new AtomicInteger(-1);
        class SeqNoGenerator extends SourceOperator {
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
        }

        final Set<Integer> receivedSeqNos = ConcurrentCollections.newConcurrentSet();
        class SeqNoCollector extends SinkOperator {
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
                        if (receivedSeqNos.size() >= maxOutputSeqNo) {
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
        }
        int numSinks = randomIntBetween(1, 8);
        int numSources = randomIntBetween(1, 8);
        List<Driver> drivers = new ArrayList<>(numSinks + numSources);
        for (int i = 0; i < numSinks; i++) {
            String description = "sink-" + i;
            ExchangeSinkOperator sinkOperator = new ExchangeSinkOperator(exchangeSink.get());
            Driver d = new Driver("test-session:1", () -> description, new SeqNoGenerator(), List.of(), sinkOperator, () -> {});
            drivers.add(d);
        }
        for (int i = 0; i < numSources; i++) {
            String description = "source-" + i;
            ExchangeSourceOperator sourceOperator = new ExchangeSourceOperator(exchangeSource.get());
            Driver d = new Driver("test-session:2", () -> description, sourceOperator, List.of(), new SeqNoCollector(), () -> {});
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
        assertThat(receivedSeqNos, hasSize(expectedSeqNos.size()));
        assertThat(receivedSeqNos, equalTo(expectedSeqNos));
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
        ExchangeService exchange0 = new ExchangeService(node0, threadPool);
        MockTransportService node1 = newTransportService();
        ExchangeService exchange1 = new ExchangeService(node1, threadPool);
        AbstractSimpleTransportTestCase.connectToNode(node0, node1.getLocalNode());

        try {
            String exchangeId = "exchange";
            Task task = new Task(1, "", "", "", null, Collections.emptyMap());
            ExchangeSourceHandler sourceHandler = exchange0.createSourceHandler(exchangeId, randomExchangeBuffer());
            ExchangeSinkHandler sinkHandler = exchange1.createSinkHandler(exchangeId, randomExchangeBuffer());
            sourceHandler.addRemoteSink(exchange0.newRemoteSink(task, exchangeId, node1.getLocalNode()), randomIntBetween(1, 5));
            final int maxInputSeqNo = rarely() ? -1 : randomIntBetween(0, 50_000);
            final int maxOutputSeqNo = rarely() ? -1 : randomIntBetween(0, 50_000);
            runConcurrentTest(maxInputSeqNo, maxOutputSeqNo, sourceHandler::createExchangeSource, sinkHandler::createExchangeSink);
        } finally {
            IOUtils.close(node0, node1);
        }
    }

    public void testFailToRespondPage() throws Exception {
        MockTransportService node0 = newTransportService();
        ExchangeService exchange0 = new ExchangeService(node0, threadPool);
        MockTransportService node1 = newTransportService();
        ExchangeService exchange1 = new ExchangeService(node1, threadPool);
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
        try {
            String exchangeId = "exchange";
            Task task = new Task(1, "", "", "", null, Collections.emptyMap());
            ExchangeSourceHandler sourceHandler = exchange0.createSourceHandler(exchangeId, randomIntBetween(1, 128));
            ExchangeSinkHandler sinkHandler = exchange1.createSinkHandler(exchangeId, randomIntBetween(1, 128));
            sourceHandler.addRemoteSink(exchange0.newRemoteSink(task, exchangeId, node1.getLocalNode()), randomIntBetween(1, 5));
            Exception err = expectThrows(
                Exception.class,
                () -> runConcurrentTest(maxSeqNo, maxSeqNo, sourceHandler::createExchangeSource, sinkHandler::createExchangeSink)
            );
            Throwable cause = ExceptionsHelper.unwrap(err, IOException.class);
            assertNotNull(cause);
            assertThat(cause.getMessage(), equalTo("page is too large"));
        } finally {
            IOUtils.close(node0, node1);
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
