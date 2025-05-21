/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.OperatorTestCase;
import org.elasticsearch.compute.test.RandomBlock;
import org.elasticsearch.compute.test.SequenceLongBlockSourceOperator;
import org.elasticsearch.core.TimeValue;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.LongStream;

import static org.elasticsearch.compute.test.RandomBlock.randomElementType;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public class LimitOperatorTests extends OperatorTestCase {
    @Override
    protected LimitOperator.Factory simple(SimpleOptions options) {
        return new LimitOperator.Factory(100);
    }

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        return new SequenceLongBlockSourceOperator(blockFactory, LongStream.range(0, size));
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return equalTo("LimitOperator[limit = 100]");
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return equalTo("LimitOperator[limit = 100/100]");
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> results) {
        int inputPositionCount = input.stream().mapToInt(p -> p.getPositionCount()).sum();
        int outputPositionCount = results.stream().mapToInt(p -> p.getPositionCount()).sum();
        assertThat(outputPositionCount, equalTo(Math.min(100, inputPositionCount)));
    }

    public void testStatus() {
        BlockFactory blockFactory = driverContext().blockFactory();
        LimitOperator op = simple(SimpleOptions.DEFAULT).get(driverContext());

        LimitOperator.Status status = op.status();
        assertThat(status.limit(), equalTo(100));
        assertThat(status.limitRemaining(), equalTo(100));
        assertThat(status.pagesProcessed(), equalTo(0));

        Page p = new Page(blockFactory.newConstantNullBlock(10));
        try {
            op.addInput(p);
            assertSame(p, op.getOutput());
        } finally {
            p.releaseBlocks();
        }
        status = op.status();
        assertThat(status.limit(), equalTo(100));
        assertThat(status.limitRemaining(), equalTo(90));
        assertThat(status.pagesProcessed(), equalTo(1));
    }

    public void testNeedInput() {
        BlockFactory blockFactory = driverContext().blockFactory();
        try (LimitOperator op = simple(SimpleOptions.DEFAULT).get(driverContext())) {
            assertTrue(op.needsInput());
            Page p = new Page(blockFactory.newConstantNullBlock(10));
            op.addInput(p);
            assertFalse(op.needsInput());
            op.getOutput().releaseBlocks();
            assertTrue(op.needsInput());
            op.finish();
            assertFalse(op.needsInput());
        }
    }

    public void testBlockBiggerThanRemaining() {
        BlockFactory blockFactory = driverContext().blockFactory();
        for (int i = 0; i < 100; i++) {
            try (var op = simple().get(driverContext())) {
                assertTrue(op.needsInput());
                Page p = new Page(randomBlock(blockFactory, 200));  // test doesn't close because operator returns a view
                op.addInput(p);
                assertFalse(op.needsInput());
                Page result = op.getOutput();
                try {
                    assertThat(result.getPositionCount(), equalTo(100));
                } finally {
                    result.releaseBlocks();
                }
                assertFalse(op.needsInput());
                assertTrue(op.isFinished());
            }
        }
    }

    public void testBlockPreciselyRemaining() {
        BlockFactory blockFactory = driverContext().blockFactory();
        for (int i = 0; i < 100; i++) {
            try (var op = simple().get(driverContext())) {
                assertTrue(op.needsInput());
                Page p = new Page(randomBlock(blockFactory, 100));  // test doesn't close because operator returns same page
                op.addInput(p);
                assertFalse(op.needsInput());
                Page result = op.getOutput();
                try {
                    assertThat(result, sameInstance(p));
                } finally {
                    result.releaseBlocks();
                }
                assertFalse(op.needsInput());
                assertTrue(op.isFinished());
            }
        }
    }

    public void testEarlyTermination() {
        int numDrivers = between(1, 4);
        final List<Driver> drivers = new ArrayList<>();
        final int limit = between(1, 10_000);
        final LimitOperator.Factory limitFactory = new LimitOperator.Factory(limit);
        final AtomicInteger receivedRows = new AtomicInteger();
        for (int i = 0; i < numDrivers; i++) {
            DriverContext driverContext = driverContext();
            SourceOperator sourceOperator = new SourceOperator() {
                boolean finished = false;

                @Override
                public void finish() {
                    finished = true;
                }

                @Override
                public boolean isFinished() {
                    return finished;
                }

                @Override
                public Page getOutput() {
                    return new Page(randomBlock(driverContext.blockFactory(), between(1, 100)));
                }

                @Override
                public void close() {

                }
            };
            SinkOperator sinkOperator = new PageConsumerOperator(p -> {
                receivedRows.addAndGet(p.getPositionCount());
                p.releaseBlocks();
            });
            drivers.add(
                new Driver(
                    "unset",
                    "test",
                    "cluster",
                    "node",
                    0,
                    0,
                    driverContext,
                    () -> "test",
                    sourceOperator,
                    List.of(limitFactory.get(driverContext)),
                    sinkOperator,
                    TimeValue.timeValueMillis(1),
                    () -> {}
                )
            );
        }
        runDriver(drivers);
        assertThat(receivedRows.get(), equalTo(limit));
    }

    Block randomBlock(BlockFactory blockFactory, int size) {
        if (randomBoolean()) {
            return blockFactory.newConstantNullBlock(size);
        }
        return RandomBlock.randomBlock(blockFactory, randomElementType(), size, false, 1, 1, 0, 0).block();
    }
}
