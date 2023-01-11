/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockBuilder;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import static org.elasticsearch.compute.operator.TopNOperator.compareFirstPositionsOfBlocks;
import static org.elasticsearch.core.Tuple.tuple;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThan;

public class TopNOperatorTests extends OperatorTestCase {

    @Override
    protected Operator.OperatorFactory simple(BigArrays bigArrays) {
        return new TopNOperator.TopNOperatorFactory(4, List.of(new TopNOperator.SortOrder(0, true, false)));
    }

    @Override
    protected String expectedDescriptionOfSimple() {
        return "TopNOperator(count = 4, sortOrders = [SortOrder[channel=0, asc=true, nullsFirst=false]])";
    }

    @Override
    protected void assertSimpleOutput(int end, List<Page> results) {
        // we have basic and random tests
    }

    @Override
    protected ByteSizeValue smallEnoughToCircuitBreak() {
        assumeTrue("TopN doesn't break the circuit breaker for now", false);
        return ByteSizeValue.ZERO;
    }

    public void testRandomTopN() {
        for (boolean asc : List.of(true, false)) {
            int limit = randomIntBetween(1, 20);
            List<Long> inputValues = randomList(0, 5000, ESTestCase::randomLong);
            Comparator<Long> comparator = asc ? Comparator.naturalOrder() : Comparator.reverseOrder();
            List<Long> expectedValues = inputValues.stream().sorted(comparator).limit(limit).toList();
            List<Long> outputValues = topN(inputValues, limit, asc, false);
            assertThat(outputValues, equalTo(expectedValues));
        }
    }

    public void testBasicTopN() {
        List<Long> values = Arrays.asList(2L, 1L, 4L, null, 5L, 10L, null, 20L, 4L, 100L);
        assertThat(topN(values, 1, true, false), equalTo(Arrays.asList(1L)));
        assertThat(topN(values, 1, false, false), equalTo(Arrays.asList(100L)));
        assertThat(topN(values, 2, true, false), equalTo(Arrays.asList(1L, 2L)));
        assertThat(topN(values, 2, false, false), equalTo(Arrays.asList(100L, 20L)));
        assertThat(topN(values, 3, true, false), equalTo(Arrays.asList(1L, 2L, 4L)));
        assertThat(topN(values, 3, false, false), equalTo(Arrays.asList(100L, 20L, 10L)));
        assertThat(topN(values, 4, true, false), equalTo(Arrays.asList(1L, 2L, 4L, 4L)));
        assertThat(topN(values, 4, false, false), equalTo(Arrays.asList(100L, 20L, 10L, 5L)));
        assertThat(topN(values, 100, true, false), equalTo(Arrays.asList(1L, 2L, 4L, 4L, 5L, 10L, 20L, 100L, null, null)));
        assertThat(topN(values, 100, false, false), equalTo(Arrays.asList(100L, 20L, 10L, 5L, 4L, 4L, 2L, 1L, null, null)));
        assertThat(topN(values, 1, true, true), equalTo(Arrays.asList(new Long[] { null })));
        assertThat(topN(values, 1, false, true), equalTo(Arrays.asList(new Long[] { null })));
        assertThat(topN(values, 2, true, true), equalTo(Arrays.asList(null, null)));
        assertThat(topN(values, 2, false, true), equalTo(Arrays.asList(null, null)));
        assertThat(topN(values, 3, true, true), equalTo(Arrays.asList(null, null, 1L)));
        assertThat(topN(values, 3, false, true), equalTo(Arrays.asList(null, null, 100L)));
        assertThat(topN(values, 4, true, true), equalTo(Arrays.asList(null, null, 1L, 2L)));
        assertThat(topN(values, 4, false, true), equalTo(Arrays.asList(null, null, 100L, 20L)));
        assertThat(topN(values, 100, true, true), equalTo(Arrays.asList(null, null, 1L, 2L, 4L, 4L, 5L, 10L, 20L, 100L)));
        assertThat(topN(values, 100, false, true), equalTo(Arrays.asList(null, null, 100L, 20L, 10L, 5L, 4L, 4L, 2L, 1L)));
    }

    public void testCompareInts() {
        Block[] bs = new Block[] {
            BlockBuilder.newIntBlockBuilder(1).appendInt(Integer.MIN_VALUE).build(),
            BlockBuilder.newIntBlockBuilder(1).appendInt(randomIntBetween(-1000, -1)).build(),
            BlockBuilder.newIntBlockBuilder(1).appendInt(0).build(),
            BlockBuilder.newIntBlockBuilder(1).appendInt(randomIntBetween(1, 1000)).build(),
            BlockBuilder.newIntBlockBuilder(1).appendInt(Integer.MAX_VALUE).build() };
        for (Block b : bs) {
            assertEquals(0, compareFirstPositionsOfBlocks(randomBoolean(), randomBoolean(), b, b));
            Block nullBlock = BlockBuilder.newConstantNullBlockWith(1);
            assertEquals(-1, compareFirstPositionsOfBlocks(randomBoolean(), true, b, nullBlock));
            assertEquals(1, compareFirstPositionsOfBlocks(randomBoolean(), false, b, nullBlock));
            assertEquals(1, compareFirstPositionsOfBlocks(randomBoolean(), true, nullBlock, b));
            assertEquals(-1, compareFirstPositionsOfBlocks(randomBoolean(), false, nullBlock, b));
        }
        for (int i = 0; i < bs.length - 1; i++) {
            for (int j = i + 1; j < bs.length; j++) {
                assertEquals(1, compareFirstPositionsOfBlocks(true, randomBoolean(), bs[i], bs[j]));
                assertEquals(-1, compareFirstPositionsOfBlocks(true, randomBoolean(), bs[j], bs[i]));
                assertEquals(-1, compareFirstPositionsOfBlocks(false, randomBoolean(), bs[i], bs[j]));
                assertEquals(1, compareFirstPositionsOfBlocks(false, randomBoolean(), bs[j], bs[i]));
            }
        }
    }

    public void testCompareBytesRef() {
        Block b1 = BlockBuilder.newBytesRefBlockBuilder(1).appendBytesRef(new BytesRef("bye")).build();
        Block b2 = BlockBuilder.newBytesRefBlockBuilder(1).appendBytesRef(new BytesRef("hello")).build();
        assertEquals(0, compareFirstPositionsOfBlocks(randomBoolean(), randomBoolean(), b1, b1));
        assertEquals(0, compareFirstPositionsOfBlocks(randomBoolean(), randomBoolean(), b2, b2));

        assertThat(compareFirstPositionsOfBlocks(true, randomBoolean(), b1, b2), greaterThan(0));
        assertThat(compareFirstPositionsOfBlocks(true, rarely(), b2, b1), lessThan(0));
        assertThat(compareFirstPositionsOfBlocks(false, randomBoolean(), b1, b2), lessThan(0));
        assertThat(compareFirstPositionsOfBlocks(false, rarely(), b2, b1), greaterThan(0));
    }

    public void testCompareWithIncompatibleTypes() {
        Block i1 = BlockBuilder.newIntBlockBuilder(1).appendInt(randomInt()).build();
        Block l1 = BlockBuilder.newLongBlockBuilder(1).appendLong(randomLong()).build();
        Block b1 = BlockBuilder.newBytesRefBlockBuilder(1).appendBytesRef(new BytesRef("hello")).build();
        IllegalStateException error = expectThrows(
            IllegalStateException.class,
            () -> TopNOperator.compareFirstPositionsOfBlocks(randomBoolean(), randomBoolean(), randomFrom(i1, l1), b1)
        );
        assertThat(error.getMessage(), containsString("Blocks have incompatible element types"));
    }

    public void testCompareWithNulls() {
        Block i1 = BlockBuilder.newIntBlockBuilder(1).appendInt(100).build();
        Block i2 = BlockBuilder.newIntBlockBuilder(1).appendNull().build();
        assertEquals(-1, compareFirstPositionsOfBlocks(randomBoolean(), true, i1, i2));
        assertEquals(1, compareFirstPositionsOfBlocks(randomBoolean(), true, i2, i1));
        assertEquals(1, compareFirstPositionsOfBlocks(randomBoolean(), false, i1, i2));
        assertEquals(-1, compareFirstPositionsOfBlocks(randomBoolean(), false, i2, i1));
    }

    private List<Long> topN(List<Long> inputValues, int limit, boolean ascendingOrder, boolean nullsFirst) {
        return topNTwoColumns(
            inputValues.stream().map(v -> tuple(v, 0L)).toList(),
            limit,
            List.of(new TopNOperator.SortOrder(0, ascendingOrder, nullsFirst))
        ).stream().map(Tuple::v1).toList();
    }

    public void testTopNTwoColumns() {
        List<Tuple<Long, Long>> values = Arrays.asList(tuple(1L, 1L), tuple(1L, 2L), tuple(null, null), tuple(null, 1L), tuple(1L, null));
        assertThat(
            topNTwoColumns(values, 5, List.of(new TopNOperator.SortOrder(0, true, false), new TopNOperator.SortOrder(1, true, false))),
            equalTo(List.of(tuple(1L, 1L), tuple(1L, 2L), tuple(1L, null), tuple(null, 1L), tuple(null, null)))
        );
        assertThat(
            topNTwoColumns(values, 5, List.of(new TopNOperator.SortOrder(0, true, true), new TopNOperator.SortOrder(1, true, false))),
            equalTo(List.of(tuple(null, 1L), tuple(null, null), tuple(1L, 1L), tuple(1L, 2L), tuple(1L, null)))
        );
        assertThat(
            topNTwoColumns(values, 5, List.of(new TopNOperator.SortOrder(0, true, false), new TopNOperator.SortOrder(1, true, true))),
            equalTo(List.of(tuple(1L, null), tuple(1L, 1L), tuple(1L, 2L), tuple(null, null), tuple(null, 1L)))
        );
    }

    private List<Tuple<Long, Long>> topNTwoColumns(
        List<Tuple<Long, Long>> inputValues,
        int limit,
        List<TopNOperator.SortOrder> sortOrders
    ) {
        List<Tuple<Long, Long>> outputValues = new ArrayList<>();
        try (
            Driver driver = new Driver(
                new TupleBlockSourceOperator(inputValues, randomIntBetween(1, 1000)),
                List.of(new TopNOperator(limit, sortOrders)),
                new PageConsumerOperator(page -> {
                    Block block1 = page.getBlock(0);
                    Block block2 = page.getBlock(1);
                    for (int i = 0; i < block1.getPositionCount(); i++) {
                        outputValues.add(tuple(block1.isNull(i) ? null : block1.getLong(i), block2.isNull(i) ? null : block2.getLong(i)));
                    }
                }),
                () -> {}
            )
        ) {
            driver.run();
        }
        assertThat(outputValues, hasSize(Math.min(limit, inputValues.size())));
        return outputValues;
    }
}
