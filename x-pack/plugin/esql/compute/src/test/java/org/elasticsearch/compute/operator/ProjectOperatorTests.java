/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.ConstantIntVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Tuple;

import java.util.BitSet;
import java.util.List;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.equalTo;

public class ProjectOperatorTests extends OperatorTestCase {
    public void testProjectionOnEmptyPage() {
        var page = new Page(0);
        var projection = new ProjectOperator(randomMask(randomIntBetween(2, 10)));
        projection.addInput(page);
        assertEquals(page, projection.getOutput());
    }

    public void testProjection() {
        var size = randomIntBetween(2, 5);
        var blocks = new Block[size];
        for (int i = 0; i < blocks.length; i++) {
            blocks[i] = new ConstantIntVector(i, size).asBlock();
        }

        var page = new Page(size, blocks);
        var mask = randomMask(size);

        var projection = new ProjectOperator(mask);
        projection.addInput(page);
        var out = projection.getOutput();
        assertEquals(mask.cardinality(), out.getBlockCount());

        int lastSetIndex = -1;
        for (int i = 0; i < out.getBlockCount(); i++) {
            var block = out.<IntBlock>getBlock(i);
            var shouldBeSetInMask = block.getInt(0);
            assertTrue(mask.get(shouldBeSetInMask));
            lastSetIndex = mask.nextSetBit(lastSetIndex + 1);
            assertEquals(shouldBeSetInMask, lastSetIndex);
        }
    }

    private BitSet randomMask(int size) {
        var mask = new BitSet(size);
        for (int i = 0; i < size; i++) {
            mask.set(i, randomBoolean());
        }
        return mask;
    }

    @Override
    protected SourceOperator simpleInput(int end) {
        return new TupleBlockSourceOperator(LongStream.range(0, end).mapToObj(l -> Tuple.tuple(l, end - l)));
    }

    @Override
    protected Operator.OperatorFactory simple(BigArrays bigArrays) {
        BitSet mask = new BitSet();
        mask.set(1, true);
        return new ProjectOperator.ProjectOperatorFactory(mask);
    }

    @Override
    protected String expectedDescriptionOfSimple() {
        return "ProjectOperator[mask = {1}]";
    }

    @Override
    protected String expectedToStringOfSimple() {
        return expectedDescriptionOfSimple();
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> results) {
        long expected = input.stream().mapToInt(Page::getPositionCount).sum();
        int total = 0;
        for (Page page : results) {
            assertThat(page.getBlockCount(), equalTo(1));
            LongBlock remaining = page.getBlock(0);
            total += page.getPositionCount();
            for (int i = 0; i < page.getPositionCount(); i++) {
                assertThat(remaining.getLong(i), equalTo(expected));
                expected--;
            }
        }
        assertThat(total, equalTo(input.stream().mapToInt(Page::getPositionCount).sum()));
    }

    @Override
    protected ByteSizeValue smallEnoughToCircuitBreak() {
        assumeTrue("doesn't use big arrays so can't break", false);
        return null;
    }
}
