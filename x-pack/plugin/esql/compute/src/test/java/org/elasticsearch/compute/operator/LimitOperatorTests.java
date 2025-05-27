/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.OperatorTestCase;
import org.elasticsearch.compute.test.RandomBlock;
import org.elasticsearch.compute.test.SequenceLongBlockSourceOperator;
import org.elasticsearch.core.TimeValue;
import org.hamcrest.Matcher;
import org.junit.Before;

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
        return new LimitOperator.Factory(100, 500);
    }

    private LimitOperator.Factory limitOperator(int limit, int pageSize) {
        return new LimitOperator.Factory(limit, pageSize);
    }

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        return new SequenceLongBlockSourceOperator(blockFactory, LongStream.range(0, size));
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return equalTo("LimitOperator[limit = 100, pageSize = 500]");
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return equalTo("LimitOperator[limit = 100/100]");
    }

    private ElementType elementType;

    @Before
    public void setUpElementTypes() throws Exception {
        elementType = randomFrom(ElementType.INT, ElementType.NULL, ElementType.BYTES_REF);
    }

    private Page randomPage(BlockFactory blockFactory, int size) {
        if (randomBoolean()) {
            return new Page(blockFactory.newConstantNullBlock(size));
        }
        Block block = RandomBlock.randomBlock(
            blockFactory,
            elementType,
            size,
            elementType == ElementType.NULL || randomBoolean(),
            1,
            1,
            0,
            0
        ).block();
        return new Page(block);
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
            op.finish();
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
        // small page size
        try (LimitOperator op = new LimitOperator(new Limiter(100), blockFactory, 5)) {
            assertTrue(op.needsInput());
            Page p = randomPage(blockFactory, 10);
            op.addInput(p);
            assertFalse(op.needsInput());
            op.getOutput().releaseBlocks();
            assertTrue(op.needsInput());
            op.finish();
            assertFalse(op.needsInput());
        }
        // small page size
        try (LimitOperator op = new LimitOperator(new Limiter(100), blockFactory, 50)) {
            for (int i = 0; i < 5; i++) {
                assertTrue(op.needsInput());
                Page p = randomPage(blockFactory, 10);
                op.addInput(p);
            }
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
                Page p = randomPage(blockFactory, 200);  // test doesn't close because operator returns a view
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
                Page p = randomPage(blockFactory, 100);  // test doesn't close because operator returns same page
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
        final LimitOperator.Factory limitFactory = new LimitOperator.Factory(limit, between(1024, 2048));
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
                    return randomPage(blockFactory(), between(1, 100));

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
