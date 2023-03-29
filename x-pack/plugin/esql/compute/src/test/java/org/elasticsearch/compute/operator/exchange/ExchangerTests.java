/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.exchange;

import org.elasticsearch.action.support.ListenableActionFuture;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.compute.data.ConstantIntVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.SinkOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class ExchangerTests extends ESTestCase {

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

    public void testConcurrent() {
        final int maxSeqNo = rarely() ? -1 : randomIntBetween(0, 50_000);
        final AtomicInteger nextSeqNo = new AtomicInteger(-1);
        class SeqNoGenerator extends SourceOperator {
            @Override
            public void finish() {

            }

            @Override
            public boolean isFinished() {
                return nextSeqNo.get() >= maxSeqNo;
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
                    if (seqNo < maxSeqNo) {
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
                IntBlock block = page.getBlock(0);
                for (int i = 0; i < block.getPositionCount(); i++) {
                    assertTrue(receivedSeqNos.add(block.getInt(i)));
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
        List<ExchangeSinkHandler> sinkExchangers = new ArrayList<>();
        int numSinks = randomIntBetween(1, 8);
        int numSources = randomIntBetween(1, 8);
        List<Driver> drivers = new ArrayList<>(numSinks + numSources);
        for (int i = 0; i < numSinks; i++) {
            final ExchangeSinkHandler sinkExchanger;
            if (sinkExchangers.isEmpty() == false && randomBoolean()) {
                sinkExchanger = randomFrom(sinkExchangers);
            } else {
                sinkExchanger = new ExchangeSinkHandler(randomIntBetween(1, 64));
                sinkExchangers.add(sinkExchanger);
            }
            String description = "sink-" + i;
            ExchangeSinkOperator sinkOperator = new ExchangeSinkOperator(sinkExchanger.createExchangeSink());
            Driver d = new Driver("test-session:1", () -> description, new SeqNoGenerator(), List.of(), sinkOperator, () -> {});
            drivers.add(d);
        }

        var sourceExchanger = new ExchangeSourceHandler(randomIntBetween(1, 64), threadPool.executor("esql_test_executor"));
        for (int i = 0; i < numSources; i++) {
            String description = "source-" + i;
            ExchangeSourceOperator sourceOperator = new ExchangeSourceOperator(sourceExchanger.createExchangeSource());
            Driver d = new Driver("test-session:2", () -> description, sourceOperator, List.of(), new SeqNoCollector(), () -> {});
            drivers.add(d);
        }
        for (ExchangeSinkHandler sinkExchanger : sinkExchangers) {
            sourceExchanger.addRemoteSink(sinkExchanger::fetchPageAsync, randomIntBetween(1, 10));
        }
        PlainActionFuture<Void> future = new PlainActionFuture<>();
        try (RefCountingListener ref = new RefCountingListener(future)) {
            for (Driver driver : drivers) {
                Driver.start(threadPool.executor("esql_test_executor"), driver, ref.acquire());
            }
        }
        future.actionGet(TimeValue.timeValueMinutes(2));
        for (ExchangeSinkHandler sinkExchanger : sinkExchangers) {
            assertThat(sinkExchanger.bufferSize(), equalTo(0));
        }
        var expectedSeqNos = IntStream.range(0, maxSeqNo).boxed().collect(Collectors.toSet());
        assertThat(receivedSeqNos, hasSize(expectedSeqNos.size()));
        assertThat(receivedSeqNos, equalTo(expectedSeqNos));
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
        sinkExchanger.fetchPageAsync(new ExchangeRequest(true), future);
        ExchangeResponse resp = future.actionGet();
        assertTrue(resp.finished());
        assertNull(resp.page());
        assertTrue(sink.waitForWriting().isDone());
        assertTrue(sink.isFinished());
    }
}
