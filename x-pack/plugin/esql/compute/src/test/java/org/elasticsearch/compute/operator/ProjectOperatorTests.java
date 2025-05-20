/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.OperatorTestCase;
import org.elasticsearch.core.Tuple;
import org.hamcrest.Matcher;

import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class ProjectOperatorTests extends OperatorTestCase {
    public void testProjectionOnEmptyPage() {
        var page = new Page(0);
        var projection = new ProjectOperator(randomProjection(10));
        projection.addInput(page);
        assertEquals(page, projection.getOutput());
    }

    public void testProjection() {
        DriverContext context = driverContext();
        var size = randomIntBetween(2, 5);
        var blocks = new Block[size];
        for (int i = 0; i < blocks.length; i++) {
            blocks[i] = context.blockFactory().newConstantIntBlockWith(i, size);
        }

        var page = new Page(size, blocks);
        var randomProjection = randomProjection(size);

        var projection = new ProjectOperator(randomProjection);
        projection.addInput(page);
        var out = projection.getOutput();
        assertThat(randomProjection.size(), lessThanOrEqualTo(out.getBlockCount()));

        for (int i = 0; i < out.getBlockCount(); i++) {
            var block = out.<IntBlock>getBlock(i);
            assertEquals(blocks[randomProjection.get(i)], block);
        }

        out.releaseBlocks();
    }

    private List<Integer> randomProjection(int size) {
        return randomList(size, () -> randomIntBetween(0, size - 1));
    }

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int end) {
        return new TupleBlockSourceOperator(blockFactory, LongStream.range(0, end).mapToObj(l -> Tuple.tuple(l, end - l)));
    }

    @Override
    protected Operator.OperatorFactory simple(SimpleOptions options) {
        return new ProjectOperator.ProjectOperatorFactory(Arrays.asList(1));
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return equalTo("ProjectOperator[projection = [1]]");
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
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

    public void testDescriptionOfMany() {
        ProjectOperator.ProjectOperatorFactory factory = new ProjectOperator.ProjectOperatorFactory(
            IntStream.range(0, 100).boxed().toList()
        );
        assertThat(factory.describe(), equalTo("ProjectOperator[projection = [100 fields]]"));
        try (Operator op = factory.get(driverContext())) {
            assertThat(op.toString(), equalTo("ProjectOperator[projection = [100 fields]]"));
        }
    }
}
