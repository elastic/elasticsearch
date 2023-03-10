/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.exchange;

import org.elasticsearch.action.support.ListenableActionFuture;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.compute.data.ConstantIntVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.SinkOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;

public class LocalExchangerTests extends ESTestCase {

    public void testBasic() {
        IntBlock block = new ConstantIntVector(1, 2).asBlock();
        Page p1 = new Page(block);
        Page p2 = new Page(block);
        Page p3 = new Page(block);
        LocalExchanger localExchanger = new LocalExchanger(2);
        ExchangeSink sink1 = localExchanger.createExchangeSink();
        ExchangeSink sink2 = localExchanger.createExchangeSink();
        ExchangeSource source = localExchanger.createExchangeSource();
        ListenableActionFuture<Void> waitForReading = source.waitForReading();
        assertNotNull(waitForReading);
        assertFalse(waitForReading.isDone());
        assertNull(source.pollPage());
        assertSame(Operator.NOT_BLOCKED, sink1.waitForWriting());
        sink1.addPage(p1);
        sink1.addPage(p2);
        sink1.finish();
        assertTrue(sink1.isFinished());

        ListenableActionFuture<Void> waitForWriting = sink1.waitForWriting();
        assertSame(waitForWriting, sink2.waitForWriting());
        assertNotNull(waitForWriting);
        assertFalse(waitForWriting.isDone());
        assertTrue(waitForReading.isDone());
        assertSame(p1, source.pollPage());
        assertTrue(waitForWriting.isDone());
        assertSame(p2, source.pollPage());
        waitForReading = source.waitForReading();
        assertNotNull(waitForReading);
        assertFalse(waitForReading.isDone());

        sink2.addPage(p3);
        sink2.finish();
        assertTrue(sink2.isFinished());

        assertFalse(source.isFinished());
        assertTrue(waitForReading.isDone());
        assertSame(p3, source.pollPage());
        assertTrue(source.isFinished());
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

        int numSinks = randomIntBetween(1, 8);
        int numSources = randomIntBetween(1, 8);
        int maxBufferSize = randomIntBetween(1, 64);
        var exchanger = new LocalExchanger(maxBufferSize);
        List<Driver> drivers = new ArrayList<>(numSinks + numSources);
        for (int i = 0; i < numSinks; i++) {
            String description = "sink-" + i;
            ExchangeSinkOperator sinkOperator = new ExchangeSinkOperator(exchanger.createExchangeSink());
            Driver d = new Driver("test-session:1", () -> description, new SeqNoGenerator(), List.of(), sinkOperator, () -> {});
            drivers.add(d);
        }
        for (int i = 0; i < numSources; i++) {
            String description = "source-" + i;
            ExchangeSourceOperator sourceOperator = new ExchangeSourceOperator(exchanger.createExchangeSource());
            Driver d = new Driver("test-session:2", () -> description, sourceOperator, List.of(), new SeqNoCollector(), () -> {});
            drivers.add(d);
        }
        // Sometimes use a single thread to make sure no deadlock when sinks/sources are blocked
        int numThreads = randomBoolean() ? 1 : between(2, 16);
        TestThreadPool threadPool = new TestThreadPool(
            "test",
            new FixedExecutorBuilder(Settings.EMPTY, "esql_test_executor", numThreads, 1024, "esql", false)
        );
        try {
            ListenableActionFuture<Void> future = new ListenableActionFuture<>();
            try (RefCountingListener ref = new RefCountingListener(future)) {
                for (Driver driver : drivers) {
                    Driver.start(threadPool.executor("esql_test_executor"), driver, ref.acquire());
                }
            }
            future.actionGet(TimeValue.timeValueMinutes(2));
            var expectedSeqNos = IntStream.range(0, maxSeqNo).boxed().collect(Collectors.toSet());
            assertThat(receivedSeqNos, equalTo(expectedSeqNos));
        } finally {
            ESTestCase.terminate(threadPool);
        }
    }
}
