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
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntArrayVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static org.elasticsearch.compute.data.BlockTestUtils.append;
import static org.elasticsearch.compute.data.BlockTestUtils.randomValue;
import static org.elasticsearch.compute.data.BlockTestUtils.readInto;
import static org.elasticsearch.core.Tuple.tuple;
import static org.elasticsearch.test.ListMatcher.matchesList;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class TopNOperatorTests extends OperatorTestCase {

    private final int pageSize = randomPageSize();

    @Override
    protected Operator.OperatorFactory simple(BigArrays bigArrays) {
        return new TopNOperator.TopNOperatorFactory(4, List.of(new TopNOperator.SortOrder(0, true, false)), pageSize);
    }

    @Override
    protected String expectedDescriptionOfSimple() {
        return "TopNOperator[count = 4, sortOrders = [SortOrder[channel=0, asc=true, nullsFirst=false]]]";
    }

    @Override
    protected String expectedToStringOfSimple() {
        return "TopNOperator[count = 0/4, sortOrder = SortOrder[channel=0, asc=true, nullsFirst=false]]";
    }

    @Override
    protected SourceOperator simpleInput(int size) {
        return new SequenceLongBlockSourceOperator(LongStream.range(0, size).map(l -> ESTestCase.randomLong()));
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> results) {
        for (int i = 0; i < results.size() - 1; i++) {
            assertThat(results.get(i).getPositionCount(), equalTo(pageSize));
        }
        assertThat(results.get(results.size() - 1).getPositionCount(), lessThanOrEqualTo(pageSize));
        long[] topN = input.stream()
            .flatMapToLong(
                page -> IntStream.range(0, page.getPositionCount())
                    .filter(p -> false == page.getBlock(0).isNull(p))
                    .mapToLong(p -> ((LongBlock) page.getBlock(0)).getLong(p))
            )
            .sorted()
            .limit(4)
            .toArray();

        results.stream().forEach(page -> assertThat(page.getPositionCount(), equalTo(4)));
        results.stream().forEach(page -> assertThat(page.getBlockCount(), equalTo(1)));
        assertThat(
            results.stream()
                .flatMapToLong(page -> IntStream.range(0, page.getPositionCount()).mapToLong(i -> page.<LongBlock>getBlock(0).getLong(i)))
                .toArray(),
            equalTo(topN)
        );
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
            IntBlock.newBlockBuilder(2).appendInt(Integer.MIN_VALUE).appendInt(randomIntBetween(-1000, -1)).build(),
            IntBlock.newBlockBuilder(2).appendInt(randomIntBetween(-1000, -1)).appendInt(0).build(),
            IntBlock.newBlockBuilder(2).appendInt(0).appendInt(randomIntBetween(1, 1000)).build(),
            IntBlock.newBlockBuilder(2).appendInt(randomIntBetween(1, 1000)).appendInt(Integer.MAX_VALUE).build(),
            IntBlock.newBlockBuilder(2).appendInt(Integer.MAX_VALUE).appendInt(0).build() };

        Page page = new Page(bs);
        TopNOperator.RowFactory rowFactory = new TopNOperator.RowFactory(page);
        TopNOperator.Row bRow0 = rowFactory.row(page, 0, null);
        TopNOperator.Row bRow1 = rowFactory.row(page, 1, null);

        Block nullBlock = Block.constantNullBlock(1);
        Block[] nullBs = new Block[] { nullBlock, nullBlock, nullBlock, nullBlock, nullBlock };
        Page nullPage = new Page(nullBs);
        TopNOperator.RowFactory nullRowFactory = new TopNOperator.RowFactory(page);
        TopNOperator.Row nullRow = nullRowFactory.row(nullPage, 0, null);

        for (int i = 0; i < bs.length; i++) {
            assertEquals(0, TopNOperator.comparePositions(randomBoolean(), randomBoolean(), bRow0, bRow0, i));
            assertEquals(-1, TopNOperator.comparePositions(randomBoolean(), true, bRow0, nullRow, i));
            assertEquals(1, TopNOperator.comparePositions(randomBoolean(), false, bRow0, nullRow, i));
            assertEquals(1, TopNOperator.comparePositions(randomBoolean(), true, nullRow, bRow0, i));
            assertEquals(-1, TopNOperator.comparePositions(randomBoolean(), false, nullRow, bRow0, i));
        }
        for (int i = 0; i < bs.length - 1; i++) {
            assertEquals(1, TopNOperator.comparePositions(true, randomBoolean(), bRow0, bRow1, i));
            assertEquals(-1, TopNOperator.comparePositions(true, randomBoolean(), bRow1, bRow0, i));
            assertEquals(-1, TopNOperator.comparePositions(false, randomBoolean(), bRow0, bRow1, i));
            assertEquals(1, TopNOperator.comparePositions(false, randomBoolean(), bRow1, bRow0, i));
        }
    }

    public void testCompareBytesRef() {
        Block[] bs = new Block[] {
            BytesRefBlock.newBlockBuilder(2).appendBytesRef(new BytesRef("bye")).appendBytesRef(new BytesRef("hello")).build() };
        Page page = new Page(bs);
        TopNOperator.RowFactory rowFactory = new TopNOperator.RowFactory(page);
        TopNOperator.Row bRow0 = rowFactory.row(page, 0, null);
        TopNOperator.Row bRow1 = rowFactory.row(page, 1, null);

        assertEquals(0, TopNOperator.comparePositions(false, randomBoolean(), bRow0, bRow0, 0));
        assertEquals(0, TopNOperator.comparePositions(false, randomBoolean(), bRow1, bRow1, 0));
        assertThat(TopNOperator.comparePositions(true, randomBoolean(), bRow0, bRow1, 0), greaterThan(0));
        assertThat(TopNOperator.comparePositions(true, randomBoolean(), bRow1, bRow0, 0), lessThan(0));
        assertThat(TopNOperator.comparePositions(false, randomBoolean(), bRow0, bRow1, 0), lessThan(0));
        assertThat(TopNOperator.comparePositions(false, rarely(), bRow1, bRow0, 0), greaterThan(0));
    }

    public void testCompareBooleans() {
        Block[] bs = new Block[] {
            BooleanBlock.newBlockBuilder(2).appendBoolean(false).appendBoolean(true).build(),
            BooleanBlock.newBlockBuilder(2).appendBoolean(true).appendBoolean(false).build() };

        Page page = new Page(bs);
        TopNOperator.RowFactory rowFactory = new TopNOperator.RowFactory(page);
        TopNOperator.Row bRow0 = rowFactory.row(page, 0, null);
        TopNOperator.Row bRow1 = rowFactory.row(page, 1, null);

        Block nullBlock = Block.constantNullBlock(2);
        Block[] nullBs = new Block[] { nullBlock, nullBlock };
        Page nullPage = new Page(nullBs);
        TopNOperator.RowFactory nullRowFactory = new TopNOperator.RowFactory(page);
        TopNOperator.Row nullRow = nullRowFactory.row(nullPage, 0, null);

        assertEquals(0, TopNOperator.comparePositions(randomBoolean(), randomBoolean(), bRow0, bRow0, 0));
        assertEquals(0, TopNOperator.comparePositions(randomBoolean(), randomBoolean(), bRow1, bRow1, 0));

        assertEquals(-1, TopNOperator.comparePositions(randomBoolean(), true, bRow0, nullRow, 0));
        assertEquals(1, TopNOperator.comparePositions(randomBoolean(), false, bRow0, nullRow, 0));
        assertEquals(1, TopNOperator.comparePositions(randomBoolean(), true, nullRow, bRow0, 0));
        assertEquals(-1, TopNOperator.comparePositions(randomBoolean(), false, nullRow, bRow0, 0));

        for (int i = 0; i < bs.length - 1; i++) {
            assertEquals(1, TopNOperator.comparePositions(true, randomBoolean(), bRow0, bRow1, 0));
            assertEquals(-1, TopNOperator.comparePositions(true, randomBoolean(), bRow1, bRow0, 0));
            assertEquals(-1, TopNOperator.comparePositions(false, randomBoolean(), bRow0, bRow1, 0));
            assertEquals(1, TopNOperator.comparePositions(false, randomBoolean(), bRow1, bRow0, 0));
        }
    }

    public void testCompareWithNulls() {
        Block i1 = IntBlock.newBlockBuilder(2).appendInt(100).appendNull().build();

        Page page = new Page(i1);
        TopNOperator.RowFactory rowFactory = new TopNOperator.RowFactory(page);
        TopNOperator.Row bRow0 = rowFactory.row(page, 0, null);
        TopNOperator.Row bRow1 = rowFactory.row(page, 1, null);

        assertEquals(-1, TopNOperator.comparePositions(randomBoolean(), true, bRow0, bRow1, 0));
        assertEquals(1, TopNOperator.comparePositions(randomBoolean(), true, bRow1, bRow0, 0));
        assertEquals(1, TopNOperator.comparePositions(randomBoolean(), false, bRow0, bRow1, 0));
        assertEquals(-1, TopNOperator.comparePositions(randomBoolean(), false, bRow1, bRow0, 0));
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

    public void testCollectAllValues() {
        int size = 10;
        int topCount = 3;
        List<Block> blocks = new ArrayList<>();
        List<List<? extends Object>> expectedTop = new ArrayList<>();

        IntBlock keys = new IntArrayVector(IntStream.range(0, size).toArray(), size).asBlock();
        List<Integer> topKeys = new ArrayList<>(IntStream.range(size - topCount, size).boxed().toList());
        Collections.reverse(topKeys);
        expectedTop.add(topKeys);
        blocks.add(keys);

        for (ElementType e : ElementType.values()) {
            if (e == ElementType.UNKNOWN) {
                continue;
            }
            List<Object> eTop = new ArrayList<>();
            Block.Builder builder = e.newBlockBuilder(size);
            for (int i = 0; i < size; i++) {
                Object value = randomValue(e);
                append(builder, value);
                if (i >= size - topCount) {
                    eTop.add(value);
                }
            }
            Collections.reverse(eTop);
            blocks.add(builder.build());
            expectedTop.add(eTop);
        }

        List<List<Object>> actualTop = new ArrayList<>();
        DriverContext driverContext = new DriverContext();
        try (
            Driver driver = new Driver(
                driverContext,
                new CannedSourceOperator(List.of(new Page(blocks.toArray(Block[]::new))).iterator()),
                List.of(new TopNOperator(topCount, List.of(new TopNOperator.SortOrder(0, false, false)), randomPageSize())),
                new PageConsumerOperator(page -> readInto(actualTop, page)),
                () -> {}
            )
        ) {
            driver.run();
        }

        assertMap(actualTop, matchesList(expectedTop));
        assertDriverContext(driverContext);
    }

    public void testCollectAllValues_RandomMultiValues() {
        int rows = 10;
        int topCount = 3;
        int blocksCount = 20;
        List<Block> blocks = new ArrayList<>();
        List<List<?>> expectedTop = new ArrayList<>();

        IntBlock keys = new IntArrayVector(IntStream.range(0, rows).toArray(), rows).asBlock();
        List<Integer> topKeys = new ArrayList<>(IntStream.range(rows - topCount, rows).boxed().toList());
        Collections.reverse(topKeys);
        expectedTop.add(topKeys);
        blocks.add(keys);

        for (int type = 0; type < blocksCount; type++) {
            ElementType e = randomFrom(ElementType.values());
            if (e == ElementType.UNKNOWN) {
                continue;
            }
            List<Object> eTop = new ArrayList<>();
            Block.Builder builder = e.newBlockBuilder(rows);
            for (int i = 0; i < rows; i++) {
                if (e != ElementType.DOC && e != ElementType.NULL && randomBoolean()) {
                    // generate a multi-value block
                    int mvCount = randomIntBetween(5, 10);
                    List<Object> eTopList = new ArrayList<>(mvCount);
                    builder.beginPositionEntry();
                    for (int j = 0; j < mvCount; j++) {
                        Object value = randomValue(e);
                        append(builder, value);
                        if (i >= rows - topCount) {
                            eTopList.add(value);
                        }
                    }
                    builder.endPositionEntry();
                    if (i >= rows - topCount) {
                        eTop.add(eTopList);
                    }
                } else {
                    Object value = randomValue(e);
                    append(builder, value);
                    if (i >= rows - topCount) {
                        eTop.add(value);
                    }
                }
            }
            Collections.reverse(eTop);
            blocks.add(builder.build());
            expectedTop.add(eTop);
        }

        DriverContext driverContext = new DriverContext();
        List<List<Object>> actualTop = new ArrayList<>();
        try (
            Driver driver = new Driver(
                driverContext,
                new CannedSourceOperator(List.of(new Page(blocks.toArray(Block[]::new))).iterator()),
                List.of(new TopNOperator(topCount, List.of(new TopNOperator.SortOrder(0, false, false)), randomPageSize())),
                new PageConsumerOperator(page -> readInto(actualTop, page)),
                () -> {}
            )
        ) {
            driver.run();
        }

        assertMap(actualTop, matchesList(expectedTop));
        assertDriverContext(driverContext);
    }

    private List<Tuple<Long, Long>> topNTwoColumns(
        List<Tuple<Long, Long>> inputValues,
        int limit,
        List<TopNOperator.SortOrder> sortOrders
    ) {
        DriverContext driverContext = new DriverContext();
        List<Tuple<Long, Long>> outputValues = new ArrayList<>();
        try (
            Driver driver = new Driver(
                driverContext,
                new TupleBlockSourceOperator(inputValues, randomIntBetween(1, 1000)),
                List.of(new TopNOperator(limit, sortOrders, randomPageSize())),
                new PageConsumerOperator(page -> {
                    LongBlock block1 = page.getBlock(0);
                    LongBlock block2 = page.getBlock(1);
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
        assertDriverContext(driverContext);
        return outputValues;
    }

    public void testTopNManyDescriptionAndToString() {
        TopNOperator.TopNOperatorFactory factory = new TopNOperator.TopNOperatorFactory(
            10,
            List.of(new TopNOperator.SortOrder(1, false, false), new TopNOperator.SortOrder(3, false, true)),
            randomPageSize()
        );
        String sorts = List.of("SortOrder[channel=1, asc=false, nullsFirst=false]", "SortOrder[channel=3, asc=false, nullsFirst=true]")
            .stream()
            .collect(Collectors.joining(", "));
        assertThat(factory.describe(), equalTo("TopNOperator[count = 10, sortOrders = [" + sorts + "]]"));
        try (Operator operator = factory.get(new DriverContext())) {
            assertThat(operator.toString(), equalTo("TopNOperator[count = 0/10, sortOrders = [" + sorts + "]]"));
        }
    }
}
