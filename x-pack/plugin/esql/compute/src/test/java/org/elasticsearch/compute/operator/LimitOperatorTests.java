/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BasicBlockTests;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public class LimitOperatorTests extends OperatorTestCase {
    @Override
    protected LimitOperator.Factory simple(BigArrays bigArrays) {
        return new LimitOperator.Factory(100);
    }

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        return new SequenceLongBlockSourceOperator(blockFactory, LongStream.range(0, size));
    }

    @Override
    protected String expectedDescriptionOfSimple() {
        return "LimitOperator[limit = 100]";
    }

    @Override
    protected String expectedToStringOfSimple() {
        return "LimitOperator[limit = 100/100]";
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> results) {
        int inputPositionCount = input.stream().mapToInt(p -> p.getPositionCount()).sum();
        int outputPositionCount = results.stream().mapToInt(p -> p.getPositionCount()).sum();
        assertThat(outputPositionCount, equalTo(Math.min(100, inputPositionCount)));
    }

    @Override
    protected ByteSizeValue smallEnoughToCircuitBreak() {
        assumeFalse("doesn't use big arrays", true);
        return null;
    }

    public void testStatus() {
        BlockFactory blockFactory = driverContext().blockFactory();
        LimitOperator op = simple(BigArrays.NON_RECYCLING_INSTANCE).get(driverContext());

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
        try (LimitOperator op = simple(BigArrays.NON_RECYCLING_INSTANCE).get(driverContext())) {
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
            try (var op = simple(BigArrays.NON_RECYCLING_INSTANCE).get(driverContext())) {
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
            try (var op = simple(BigArrays.NON_RECYCLING_INSTANCE).get(driverContext())) {
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

    Block randomBlock(BlockFactory blockFactory, int size) {
        if (randomBoolean()) {
            return blockFactory.newConstantNullBlock(size);
        }
        return BasicBlockTests.randomBlock(blockFactory, randomElement(), size, false, 1, 1, 0, 0).block();
    }

    static ElementType randomElement() {
        List<ElementType> l = new ArrayList<>();
        for (ElementType elementType : ElementType.values()) {
            if (elementType == ElementType.UNKNOWN || elementType == ElementType.NULL || elementType == ElementType.DOC) {
                continue;
            }
            l.add(elementType);
        }
        return randomFrom(l);
    }
}
