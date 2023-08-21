/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.network.NetworkAddress;
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
import org.elasticsearch.compute.data.TestBlockBuilder;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.versionfield.Version;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static java.util.Comparator.naturalOrder;
import static java.util.Comparator.reverseOrder;
import static org.elasticsearch.compute.data.BlockTestUtils.append;
import static org.elasticsearch.compute.data.BlockTestUtils.randomValue;
import static org.elasticsearch.compute.data.BlockTestUtils.readInto;
import static org.elasticsearch.compute.data.BlockUtils.toJavaObject;
import static org.elasticsearch.compute.data.ElementType.BOOLEAN;
import static org.elasticsearch.compute.data.ElementType.BYTES_REF;
import static org.elasticsearch.compute.data.ElementType.DOUBLE;
import static org.elasticsearch.compute.data.ElementType.INT;
import static org.elasticsearch.compute.data.ElementType.LONG;
import static org.elasticsearch.compute.operator.TopNOperator.BYTESREF_FIXED_LENGTH_ENCODER;
import static org.elasticsearch.compute.operator.TopNOperator.BYTESREF_UTF8_ENCODER;
import static org.elasticsearch.compute.operator.TopNOperator.DEFAULT_ENCODER;
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
    // versions taken from org.elasticsearch.xpack.versionfield.VersionTests
    private static final List<String> VERSIONS = List.of(
        "1",
        "1.0",
        "1.0.0.0.0.0.0.0.0.1",
        "1.0.0",
        "2.0.0",
        "11.0.0",
        "2.1.0",
        "2.1.1",
        "2.1.1.0",
        "2.0.0",
        "11.0.0",
        "2.0",
        "1.0.0-a",
        "1.0.0-b",
        "1.0.0-1.0.0",
        "1.0.0-2.0",
        "1.0.0-alpha",
        "1.0.0-alpha.1",
        "1.0.0-alpha.beta",
        "1.0.0-beta",
        "1.0.0-beta.2",
        "1.0.0-beta.11",
        "1.0.0-beta11",
        "1.0.0-beta2",
        "1.0.0-rc.1",
        "2.0.0-pre127",
        "2.0.0-pre128",
        "2.0.0-pre128-somethingelse",
        "2.0.0-pre20201231z110026",
        "2.0.0-pre227",
        "99999.99999.99999",
        "1.invalid",
        "",
        "a",
        "lkjlaskdjf",
        "1.2.3-rc1"
    );

    @Override
    protected Operator.OperatorFactory simple(BigArrays bigArrays) {
        return new TopNOperator.TopNOperatorFactory(4, List.of(new TopNOperator.SortOrder(0, true, false)), pageSize);
    }

    @Override
    protected String expectedDescriptionOfSimple() {
        return "TopNOperator[count = 4, sortOrders = [SortOrder[channel=0, asc=true, nullsFirst=false, " + "encoder=DefaultEncoder]]]";
    }

    @Override
    protected String expectedToStringOfSimple() {
        return "TopNOperator[count = 0/4, sortOrder = SortOrder[channel=0, asc=true, nullsFirst=false, " + "encoder=DefaultEncoder]]";
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
            Comparator<Long> comparator = asc ? naturalOrder() : reverseOrder();
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

        Block nullBlock = Block.constantNullBlock(1);
        Block[] nullBs = new Block[] { nullBlock, nullBlock, nullBlock, nullBlock, nullBlock };
        Page nullPage = new Page(nullBs);
        TopNOperator.RowFactory nullRowFactory = new TopNOperator.RowFactory(page);

        for (int i = 0; i < bs.length; i++) {
            Tuple<TopNOperator.Row, TopNOperator.Row> rows = nonBytesRefRows(
                randomBoolean(),
                randomBoolean(),
                so -> rowFactory.row(page, 0, null, so),
                null,
                i
            );
            assertEquals(0, TopNOperator.compareRows(rows.v1(), rows.v1()));

            rows = nonBytesRefRows(
                randomBoolean(),
                true,
                so -> rowFactory.row(page, 0, null, so),
                so -> nullRowFactory.row(nullPage, 0, null, so),
                i
            );
            assertEquals(-1, TopNOperator.compareRows(rows.v1(), rows.v2()));

            rows = nonBytesRefRows(
                randomBoolean(),
                false,
                so -> rowFactory.row(page, 0, null, so),
                so -> nullRowFactory.row(nullPage, 0, null, so),
                i
            );
            assertEquals(1, TopNOperator.compareRows(rows.v1(), rows.v2()));

            rows = nonBytesRefRows(
                randomBoolean(),
                true,
                so -> rowFactory.row(page, 0, null, so),
                so -> nullRowFactory.row(nullPage, 0, null, so),
                i
            );
            assertEquals(1, TopNOperator.compareRows(rows.v2(), rows.v1()));

            rows = nonBytesRefRows(
                randomBoolean(),
                false,
                so -> rowFactory.row(page, 0, null, so),
                so -> nullRowFactory.row(nullPage, 0, null, so),
                i
            );
            assertEquals(-1, TopNOperator.compareRows(rows.v2(), rows.v1()));
        }
        for (int i = 0; i < bs.length - 1; i++) {
            Tuple<TopNOperator.Row, TopNOperator.Row> rows = nonBytesRefRows(
                true,
                randomBoolean(),
                so -> rowFactory.row(page, 0, null, so),
                so -> rowFactory.row(page, 1, null, so),
                i
            );
            assertThat(TopNOperator.compareRows(rows.v1(), rows.v2()), greaterThan(0));
            rows = nonBytesRefRows(
                true,
                randomBoolean(),
                so -> rowFactory.row(page, 0, null, so),
                so -> rowFactory.row(page, 1, null, so),
                i
            );
            assertThat(TopNOperator.compareRows(rows.v2(), rows.v1()), lessThan(0));
            rows = nonBytesRefRows(
                false,
                randomBoolean(),
                so -> rowFactory.row(page, 0, null, so),
                so -> rowFactory.row(page, 1, null, so),
                i
            );
            assertThat(TopNOperator.compareRows(rows.v1(), rows.v2()), lessThan(0));
            rows = nonBytesRefRows(
                false,
                randomBoolean(),
                so -> rowFactory.row(page, 0, null, so),
                so -> rowFactory.row(page, 1, null, so),
                i
            );
            assertThat(TopNOperator.compareRows(rows.v2(), rows.v1()), greaterThan(0));
        }
    }

    private Tuple<TopNOperator.Row, TopNOperator.Row> nonBytesRefRows(
        boolean asc,
        boolean nullsFirst,
        Function<List<TopNOperator.SortOrder>, TopNOperator.Row> row1,
        Function<List<TopNOperator.SortOrder>, TopNOperator.Row> row2,
        int position
    ) {
        return rows(asc, nullsFirst, row1, row2, position, DEFAULT_ENCODER);
    }

    private Tuple<TopNOperator.Row, TopNOperator.Row> bytesRefRows(
        boolean asc,
        boolean nullsFirst,
        Function<List<TopNOperator.SortOrder>, TopNOperator.Row> row1,
        Function<List<TopNOperator.SortOrder>, TopNOperator.Row> row2,
        int position
    ) {
        return rows(asc, nullsFirst, row1, row2, position, BYTESREF_UTF8_ENCODER);
    }

    private Tuple<TopNOperator.Row, TopNOperator.Row> rows(
        boolean asc,
        boolean nullsFirst,
        Function<List<TopNOperator.SortOrder>, TopNOperator.Row> row1,
        Function<List<TopNOperator.SortOrder>, TopNOperator.Row> row2,
        int position,
        TopNEncoder encoder
    ) {
        List<TopNOperator.SortOrder> so = List.of(new TopNOperator.SortOrder(position, asc, nullsFirst, encoder));
        return new Tuple<>(row1 == null ? null : row1.apply(so), row2 == null ? null : row2.apply(so));
    }

    public void testCompareBytesRef() {
        Block[] bs = new Block[] {
            BytesRefBlock.newBlockBuilder(2).appendBytesRef(new BytesRef("bye")).appendBytesRef(new BytesRef("hello")).build() };
        Page page = new Page(bs);
        TopNOperator.RowFactory rowFactory = new TopNOperator.RowFactory(page);

        Tuple<TopNOperator.Row, TopNOperator.Row> rows = bytesRefRows(
            false,
            randomBoolean(),
            so -> rowFactory.row(page, 0, null, so),
            null,
            0
        );
        assertEquals(0, TopNOperator.compareRows(rows.v1(), rows.v1()));
        rows = bytesRefRows(false, randomBoolean(), so -> rowFactory.row(page, 1, null, so), null, 0);
        assertEquals(0, TopNOperator.compareRows(rows.v1(), rows.v1()));

        rows = bytesRefRows(true, randomBoolean(), so -> rowFactory.row(page, 0, null, so), so -> rowFactory.row(page, 1, null, so), 0);
        assertThat(TopNOperator.compareRows(rows.v1(), rows.v2()), greaterThan(0));
        rows = bytesRefRows(true, randomBoolean(), so -> rowFactory.row(page, 0, null, so), so -> rowFactory.row(page, 1, null, so), 0);
        assertThat(TopNOperator.compareRows(rows.v2(), rows.v1()), lessThan(0));
        rows = bytesRefRows(false, randomBoolean(), so -> rowFactory.row(page, 0, null, so), so -> rowFactory.row(page, 1, null, so), 0);
        assertThat(TopNOperator.compareRows(rows.v1(), rows.v2()), lessThan(0));
        rows = bytesRefRows(false, rarely(), so -> rowFactory.row(page, 0, null, so), so -> rowFactory.row(page, 1, null, so), 0);
        assertThat(TopNOperator.compareRows(rows.v2(), rows.v1()), greaterThan(0));
    }

    public void testCompareBooleans() {
        Block[] bs = new Block[] {
            BooleanBlock.newBlockBuilder(2).appendBoolean(false).appendBoolean(true).build(),
            BooleanBlock.newBlockBuilder(2).appendBoolean(true).appendBoolean(false).build() };

        Page page = new Page(bs);
        TopNOperator.RowFactory rowFactory = new TopNOperator.RowFactory(page);

        Block nullBlock = Block.constantNullBlock(2);
        Block[] nullBs = new Block[] { nullBlock, nullBlock };
        Page nullPage = new Page(nullBs);
        TopNOperator.RowFactory nullRowFactory = new TopNOperator.RowFactory(page);

        Tuple<TopNOperator.Row, TopNOperator.Row> rows = nonBytesRefRows(
            randomBoolean(),
            randomBoolean(),
            so -> rowFactory.row(page, 0, null, so),
            so -> rowFactory.row(page, 1, null, so),
            0
        );
        assertEquals(0, TopNOperator.compareRows(rows.v1(), rows.v1()));
        assertEquals(0, TopNOperator.compareRows(rows.v2(), rows.v2()));

        rows = nonBytesRefRows(
            randomBoolean(),
            true,
            so -> rowFactory.row(page, 0, null, so),
            so -> nullRowFactory.row(nullPage, 0, null, so),
            0
        );
        assertEquals(-1, TopNOperator.compareRows(rows.v1(), rows.v2()));
        rows = nonBytesRefRows(
            randomBoolean(),
            false,
            so -> rowFactory.row(page, 0, null, so),
            so -> nullRowFactory.row(nullPage, 0, null, so),
            0
        );
        assertEquals(1, TopNOperator.compareRows(rows.v1(), rows.v2()));
        rows = nonBytesRefRows(
            randomBoolean(),
            true,
            so -> rowFactory.row(page, 0, null, so),
            so -> nullRowFactory.row(nullPage, 0, null, so),
            0
        );
        assertEquals(1, TopNOperator.compareRows(rows.v2(), rows.v1()));
        rows = nonBytesRefRows(
            randomBoolean(),
            false,
            so -> rowFactory.row(page, 0, null, so),
            so -> nullRowFactory.row(nullPage, 0, null, so),
            0
        );
        assertEquals(-1, TopNOperator.compareRows(rows.v2(), rows.v1()));

        for (int i = 0; i < bs.length - 1; i++) {
            rows = nonBytesRefRows(
                true,
                randomBoolean(),
                so -> rowFactory.row(page, 0, null, so),
                so -> rowFactory.row(page, 1, null, so),
                0
            );
            assertEquals(1, TopNOperator.compareRows(rows.v1(), rows.v2()));
            rows = nonBytesRefRows(
                true,
                randomBoolean(),
                so -> rowFactory.row(page, 0, null, so),
                so -> rowFactory.row(page, 1, null, so),
                0
            );
            assertEquals(-1, TopNOperator.compareRows(rows.v2(), rows.v1()));
            rows = nonBytesRefRows(
                false,
                randomBoolean(),
                so -> rowFactory.row(page, 0, null, so),
                so -> rowFactory.row(page, 1, null, so),
                0
            );
            assertEquals(-1, TopNOperator.compareRows(rows.v1(), rows.v2()));
            rows = nonBytesRefRows(
                false,
                randomBoolean(),
                so -> rowFactory.row(page, 0, null, so),
                so -> rowFactory.row(page, 1, null, so),
                0
            );
            assertEquals(1, TopNOperator.compareRows(rows.v2(), rows.v1()));
        }
    }

    public void testCompareWithNulls() {
        Block i1 = IntBlock.newBlockBuilder(2).appendInt(100).appendNull().build();

        Page page = new Page(i1);
        TopNOperator.RowFactory rowFactory = new TopNOperator.RowFactory(page);

        Tuple<TopNOperator.Row, TopNOperator.Row> rows = nonBytesRefRows(
            randomBoolean(),
            true,
            so -> rowFactory.row(page, 0, null, so),
            so -> rowFactory.row(page, 1, null, so),
            0
        );
        assertEquals(-1, TopNOperator.compareRows(rows.v1(), rows.v2()));
        rows = nonBytesRefRows(randomBoolean(), true, so -> rowFactory.row(page, 0, null, so), so -> rowFactory.row(page, 1, null, so), 0);
        assertEquals(1, TopNOperator.compareRows(rows.v2(), rows.v1()));
        rows = nonBytesRefRows(randomBoolean(), false, so -> rowFactory.row(page, 0, null, so), so -> rowFactory.row(page, 1, null, so), 0);
        assertEquals(1, TopNOperator.compareRows(rows.v1(), rows.v2()));
        rows = nonBytesRefRows(randomBoolean(), false, so -> rowFactory.row(page, 0, null, so), so -> rowFactory.row(page, 1, null, so), 0);
        assertEquals(-1, TopNOperator.compareRows(rows.v2(), rows.v1()));
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
                List.of(
                    new TopNOperator(
                        topCount,
                        List.of(new TopNOperator.SortOrder(0, false, false, BYTESREF_UTF8_ENCODER)),
                        randomPageSize()
                    )
                ),
                new PageConsumerOperator(page -> readInto(actualTop, page)),
                () -> {}
            )
        ) {
            runDriver(driver);
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
                List.of(
                    new TopNOperator(
                        topCount,
                        List.of(new TopNOperator.SortOrder(0, false, false, BYTESREF_UTF8_ENCODER)),
                        randomPageSize()
                    )
                ),
                new PageConsumerOperator(page -> readInto(actualTop, page)),
                () -> {}
            )
        ) {
            runDriver(driver);
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
            runDriver(driver);
        }
        assertThat(outputValues, hasSize(Math.min(limit, inputValues.size())));
        assertDriverContext(driverContext);
        return outputValues;
    }

    public void testTopNManyDescriptionAndToString() {
        TopNOperator.TopNOperatorFactory factory = new TopNOperator.TopNOperatorFactory(
            10,
            List.of(
                new TopNOperator.SortOrder(1, false, false, BYTESREF_UTF8_ENCODER),
                new TopNOperator.SortOrder(3, false, true, BYTESREF_FIXED_LENGTH_ENCODER)
            ),
            randomPageSize()
        );
        String sorts = List.of(
            "SortOrder[channel=1, asc=false, nullsFirst=false, encoder=UTF8TopNEncoder]",
            "SortOrder[channel=3, asc=false, nullsFirst=true, encoder=FixedLengthTopNEncoder]"
        ).stream().collect(Collectors.joining(", "));
        assertThat(factory.describe(), equalTo("TopNOperator[count = 10, sortOrders = [" + sorts + "]]"));
        try (Operator operator = factory.get(new DriverContext())) {
            assertThat(operator.toString(), equalTo("TopNOperator[count = 0/10, sortOrders = [" + sorts + "]]"));
        }
    }

    private static final List<List<Object>> INT_MV = List.of(
        List.of(100),
        List.of(63, 61, 62),
        List.of(22, 21, 22),
        List.of(50),
        List.of(-1, 63, 2)
    );

    private static final List<List<Object>> LONG_MV = List.of(
        List.of(17, -1, -5, 0, 4),
        List.of(1, 2, 3, 4, 5),
        List.of(5, 4, 3, 2, 2),
        List.of(),
        List.of(5000, 1000, 7000),
        List.of(),
        List.of(-10000)
    );

    private static final List<List<Object>> DOUBLE_MV = List.of(
        List.of(0.01, 0.01, 1, 1, 1),
        List.of(-1, -0.01, 1, 0.01),
        List.of(0, 0, 17),
        List.of(),
        List.of(1, 5, -1, -0.01, -5),
        List.of(100),
        List.of(63, -61, -62.123)
    );

    private static final List<List<Object>> BOOL_MV = List.of(
        List.of(true, true, true),
        List.of(true, false),
        List.of(true),
        List.of(false),
        List.of(false, false, false),
        List.of(true, false)
    );

    public void testTopNWithSortingOnSameField_DESC_then_ASC_int() {
        assertSortingOnMV(
            INT_MV,
            List.of(100, List.of(-1, 63, 2), List.of(63, 61, 62), 50, List.of(22, 21, 22)),
            INT,
            new TopNOperator.SortOrder(0, false, false),
            new TopNOperator.SortOrder(0, true, false)
        );
    }

    public void testTopNWithSortingOnSameField_DESC_then_ASC_long() {
        List<Object> expectedValues = new ArrayList<>();
        expectedValues.addAll(
            List.of(
                List.of(5000L, 1000L, 7000L),
                List.of(17L, -1L, -5L, 0L, 4L),
                List.of(1L, 2L, 3L, 4L, 5L),
                List.of(5L, 4L, 3L, 2L, 2L),
                -10000L
            )
        );
        expectedValues.add(null);
        expectedValues.add(null);

        assertSortingOnMV(
            LONG_MV,
            expectedValues,
            LONG,
            new TopNOperator.SortOrder(0, false, false),
            new TopNOperator.SortOrder(0, true, false)
        );
    }

    public void testTopNWithSortingOnSameField_DESC_then_ASC_double() {
        List<Object> expectedValues = new ArrayList<>();
        expectedValues.addAll(
            List.of(
                100d,
                List.of(63d, -61d, -62.123d),
                List.of(0d, 0d, 17d),
                List.of(1d, 5d, -1d, -0.01d, -5d),
                List.of(-1d, -0.01d, 1d, 0.01d),
                List.of(0.01d, 0.01d, 1d, 1d, 1d)
            )
        );
        expectedValues.add(null);

        assertSortingOnMV(
            DOUBLE_MV,
            expectedValues,
            DOUBLE,
            new TopNOperator.SortOrder(0, false, false),
            new TopNOperator.SortOrder(0, true, false)
        );
    }

    public void testTopNWithSortingOnSameField_DESC_then_ASC_boolean() {
        assertSortingOnMV(
            BOOL_MV,
            List.of(List.of(true, false), List.of(true, false), true, List.of(true, true, true), List.of(false, false, false), false),
            BOOLEAN,
            new TopNOperator.SortOrder(0, false, false),
            new TopNOperator.SortOrder(0, true, false)
        );
    }

    public void testTopNWithSortingOnSameField_DESC_then_ASC_BytesRef() {
        assertSortingOnMV(
            INT_MV,
            List.of(
                List.of(new BytesRef("-1"), new BytesRef("63"), new BytesRef("2")),
                List.of(new BytesRef("63"), new BytesRef("61"), new BytesRef("62")),
                new BytesRef("50"),
                List.of(new BytesRef("22"), new BytesRef("21"), new BytesRef("22")),
                new BytesRef("100")
            ),
            BYTES_REF,
            new TopNOperator.SortOrder(0, false, false, BYTESREF_UTF8_ENCODER),
            new TopNOperator.SortOrder(0, true, false, BYTESREF_UTF8_ENCODER)
        );
    }

    public void testTopNWithSortingOnSameField_ASC_then_DESC_int() {
        assertSortingOnMV(
            INT_MV,
            List.of(List.of(-1, 63, 2), List.of(22, 21, 22), 50, List.of(63, 61, 62), 100),
            INT,
            new TopNOperator.SortOrder(0, true, false),
            new TopNOperator.SortOrder(0, false, false)
        );
    }

    public void testTopNWithSortingOnSameField_ASC_then_DESC_long() {
        List<Object> expectedValues = new ArrayList<>();
        expectedValues.addAll(
            List.of(
                -10000L,
                List.of(17L, -1L, -5L, 0L, 4L),
                List.of(1L, 2L, 3L, 4L, 5L),
                List.of(5L, 4L, 3L, 2L, 2L),
                List.of(5000L, 1000L, 7000L)
            )
        );
        expectedValues.add(null);
        expectedValues.add(null);

        assertSortingOnMV(
            LONG_MV,
            expectedValues,
            LONG,
            new TopNOperator.SortOrder(0, true, false),
            new TopNOperator.SortOrder(0, false, false)
        );
    }

    public void testTopNWithSortingOnSameField_ASC_then_DESC_double() {
        List<Object> expectedValues = new ArrayList<>();
        expectedValues.addAll(
            List.of(
                List.of(63d, -61d, -62.123d),
                List.of(1d, 5d, -1d, -0.01d, -5d),
                List.of(-1d, -0.01d, 1d, 0.01d),
                List.of(0d, 0d, 17d),
                List.of(0.01d, 0.01d, 1d, 1d, 1d),
                100d
            )
        );
        expectedValues.add(null);

        assertSortingOnMV(
            DOUBLE_MV,
            expectedValues,
            DOUBLE,
            new TopNOperator.SortOrder(0, true, false),
            new TopNOperator.SortOrder(0, false, false)
        );
    }

    public void testTopNWithSortingOnSameField_ASC_then_DESC_BytesRef() {
        assertSortingOnMV(
            INT_MV,
            List.of(
                List.of(new BytesRef("-1"), new BytesRef("63"), new BytesRef("2")),
                new BytesRef("100"),
                List.of(new BytesRef("22"), new BytesRef("21"), new BytesRef("22")),
                new BytesRef("50"),
                List.of(new BytesRef("63"), new BytesRef("61"), new BytesRef("62"))
            ),
            BYTES_REF,
            new TopNOperator.SortOrder(0, true, false, BYTESREF_UTF8_ENCODER),
            new TopNOperator.SortOrder(0, false, false, BYTESREF_UTF8_ENCODER)
        );
    }

    private void assertSortingOnMV(
        List<List<Object>> values,
        List<Object> expectedValues,
        ElementType blockType,
        TopNOperator.SortOrder... sortOrders
    ) {
        Block block = TestBlockBuilder.blockFromValues(values, blockType);
        assert block.mvOrdering() == Block.MvOrdering.UNORDERED : "Blocks created for this test must have unordered multi-values";
        Page page = new Page(block);

        List<List<Object>> actualValues = new ArrayList<>();
        int topCount = randomIntBetween(1, values.size());
        try (
            Driver driver = new Driver(
                new DriverContext(),
                new CannedSourceOperator(List.of(page).iterator()),
                List.of(new TopNOperator(topCount, List.of(sortOrders), randomPageSize())),
                new PageConsumerOperator(p -> readInto(actualValues, p)),
                () -> {}
            )
        ) {
            runDriver(driver);
        }
        assertMap(actualValues, matchesList(List.of(expectedValues.subList(0, topCount))));
    }

    public void testRandomMultiValuesTopN() {
        int rows = randomIntBetween(50, 100);
        int topCount = randomIntBetween(1, rows);
        int blocksCount = randomIntBetween(20, 30);
        int sortingByColumns = randomIntBetween(1, 10);

        Set<TopNOperator.SortOrder> uniqueOrders = new LinkedHashSet<>(sortingByColumns);
        List<List<List<Object>>> expectedValues = new ArrayList<>(rows);
        List<Block> blocks = new ArrayList<>(blocksCount);
        Map<Integer, TopNEncoder> columnBytesRefEncoder = new HashMap<>(blocksCount);

        for (int i = 0; i < rows; i++) {
            expectedValues.add(new ArrayList<>(blocksCount));
        }

        for (int type = 0; type < blocksCount; type++) {
            ElementType e = randomValueOtherThanMany(
                t -> t == ElementType.UNKNOWN || t == ElementType.DOC,
                () -> randomFrom(ElementType.values())
            );
            Block.Builder builder = e.newBlockBuilder(rows);
            List<Object> previousValue = null;
            Function<ElementType, Object> randomValueSupplier = (blockType) -> randomValue(blockType);
            if (e == BYTES_REF) {
                if (rarely()) {
                    if (randomBoolean()) {
                        // deal with IP fields (BytesRef block) like ES does and properly encode the ip addresses
                        randomValueSupplier = (blockType) -> new BytesRef(InetAddressPoint.encode(randomIp(randomBoolean())));
                    } else {
                        // create a valid Version
                        randomValueSupplier = (blockType) -> randomVersion().toBytesRef();
                    }
                    // use the right BytesRef encoder (don't touch the bytes)
                    columnBytesRefEncoder.put(type, BYTESREF_FIXED_LENGTH_ENCODER);
                } else {
                    columnBytesRefEncoder.put(type, BYTESREF_UTF8_ENCODER);
                }
            } else {
                columnBytesRefEncoder.put(type, DEFAULT_ENCODER);
            }

            for (int i = 0; i < rows; i++) {
                List<Object> values = new ArrayList<>();
                // let's make things a bit more real for this TopN sorting: have some "equal" values in different rows for the same block
                if (rarely() && previousValue != null) {
                    values = previousValue;
                } else {
                    if (e != ElementType.NULL && randomBoolean()) {
                        // generate a multi-value block
                        int mvCount = randomIntBetween(5, 10);
                        for (int j = 0; j < mvCount; j++) {
                            Object value = randomValueSupplier.apply(e);
                            values.add(value);
                        }
                    } else {// null or single-valued value
                        values.add(randomValueSupplier.apply(e));
                    }

                    if (usually() && randomBoolean()) {
                        // let's remember the "previous" value, maybe we'll use it again in a different row
                        previousValue = values;
                    }
                }

                if (values.size() == 1) {
                    append(builder, values.get(0));
                } else {
                    builder.beginPositionEntry();
                    for (Object o : values) {
                        append(builder, o);
                    }
                    builder.endPositionEntry();
                }

                expectedValues.get(i).add(values);
            }
            blocks.add(builder.build());
        }

        // simulate the LogicalPlanOptimizer.PruneRedundantSortClauses by eliminating duplicate sorting columns (same column, same asc/desc,
        // same "nulls" handling)
        while (uniqueOrders.size() < sortingByColumns) {
            int column = randomIntBetween(0, blocksCount - 1);
            uniqueOrders.add(new TopNOperator.SortOrder(column, randomBoolean(), randomBoolean(), columnBytesRefEncoder.get(column)));
        }

        List<List<List<Object>>> actualValues = new ArrayList<>();
        List<Page> results = this.drive(
            new TopNOperator(topCount, uniqueOrders.stream().toList(), rows),
            List.of(new Page(blocks.toArray(Block[]::new))).iterator()
        );
        for (Page p : results) {
            readAsRows(actualValues, p);
        }

        List<List<List<Object>>> topNExpectedValues = expectedValues.stream()
            .sorted(new NaiveTopNComparator(uniqueOrders))
            .limit(topCount)
            .toList();
        List<List<Object>> actualReducedValues = extractAndReduceSortedValues(actualValues, uniqueOrders);
        List<List<Object>> expectedReducedValues = extractAndReduceSortedValues(topNExpectedValues, uniqueOrders);

        assertThat(actualReducedValues.size(), equalTo(topNExpectedValues.size()));
        assertThat(expectedReducedValues.size(), equalTo(topNExpectedValues.size()));
        for (int i = 0; i < topNExpectedValues.size(); i++) {
            assertThat(topNExpectedValues.get(i).size(), equalTo(actualValues.get(i).size()));
        }
        assertMap(actualReducedValues, matchesList(expectedReducedValues));
    }

    public void testIPSortingSingleValue() throws UnknownHostException {
        List<String> ips = List.of("123.4.245.23", "104.244.253.29", "1.198.3.93", "32.183.93.40", "104.30.244.2", "104.244.4.1");
        Block.Builder builder = BYTES_REF.newBlockBuilder(ips.size());
        boolean asc = randomBoolean();

        for (String ip : ips) {
            append(builder, new BytesRef(InetAddressPoint.encode(InetAddress.getByName(ip))));
        }

        Set<TopNOperator.SortOrder> orders = new HashSet<>(1);
        orders.add(new TopNOperator.SortOrder(0, asc, randomBoolean(), BYTESREF_FIXED_LENGTH_ENCODER));

        List<List<Object>> actual = new ArrayList<>();
        try (
            Driver driver = new Driver(
                new DriverContext(),
                new CannedSourceOperator(List.of(new Page(builder.build())).iterator()),
                List.of(new TopNOperator(ips.size(), orders.stream().toList(), randomPageSize())),
                new PageConsumerOperator(p -> readInto(actual, p)),
                () -> {}
            )
        ) {
            runDriver(driver);
        }

        assertThat(actual.size(), equalTo(1));
        List<String> actualDecodedIps = actual.get(0)
            .stream()
            .map(row -> InetAddressPoint.decode(BytesRef.deepCopyOf((BytesRef) row).bytes))
            .map(NetworkAddress::format)
            .collect(Collectors.toCollection(ArrayList::new));
        assertThat(
            actualDecodedIps,
            equalTo(
                asc
                    ? List.of("1.198.3.93", "32.183.93.40", "104.30.244.2", "104.244.4.1", "104.244.253.29", "123.4.245.23")
                    : List.of("123.4.245.23", "104.244.253.29", "104.244.4.1", "104.30.244.2", "32.183.93.40", "1.198.3.93")
            )
        );
    }

    public void testIPSortingUnorderedMultiValues() throws UnknownHostException {
        List<List<String>> ips = new ArrayList<>();
        ips.add(List.of("123.4.245.23", "123.4.245.23"));
        ips.add(null);
        ips.add(List.of("104.30.244.2", "127.0.0.1"));
        ips.add(null);
        ips.add(List.of("1.198.3.93", "255.123.123.0", "2.3.4.5"));
        ips.add(List.of("1.1.1.0", "32.183.93.40"));
        ips.add(List.of("124.255.255.255", "104.30.244.2"));
        ips.add(List.of("104.244.4.1"));

        boolean asc = randomBoolean();
        List<List<String>> expectedDecodedIps = new ArrayList<>();
        if (asc) {
            expectedDecodedIps.add(List.of("1.1.1.0", "32.183.93.40"));
            expectedDecodedIps.add(List.of("1.198.3.93", "255.123.123.0", "2.3.4.5"));
            expectedDecodedIps.add(List.of("104.30.244.2", "127.0.0.1"));
            expectedDecodedIps.add(List.of("124.255.255.255", "104.30.244.2"));
            expectedDecodedIps.add(List.of("104.244.4.1"));
            expectedDecodedIps.add(List.of("123.4.245.23", "123.4.245.23"));
        } else {
            expectedDecodedIps.add(List.of("1.198.3.93", "255.123.123.0", "2.3.4.5"));
            expectedDecodedIps.add(List.of("104.30.244.2", "127.0.0.1"));
            expectedDecodedIps.add(List.of("124.255.255.255", "104.30.244.2"));
            expectedDecodedIps.add(List.of("123.4.245.23", "123.4.245.23"));
            expectedDecodedIps.add(List.of("104.244.4.1"));
            expectedDecodedIps.add(List.of("1.1.1.0", "32.183.93.40"));
        }

        assertIPSortingOnMultiValues(ips, asc, Block.MvOrdering.UNORDERED, expectedDecodedIps);
    }

    public void testIPSortingOrderedMultiValues() throws UnknownHostException {
        List<List<String>> ips = new ArrayList<>();
        ips.add(List.of("123.4.245.23", "123.4.245.23"));
        ips.add(null);
        ips.add(List.of("104.30.244.2", "127.0.0.1"));
        ips.add(null);
        ips.add(List.of("1.198.3.93", "2.3.4.5", "255.123.123.0"));
        ips.add(List.of("1.1.1.0", "32.183.93.40"));
        ips.add(List.of("104.30.244.2", "124.255.255.255"));
        ips.add(List.of("104.244.4.1"));

        boolean asc = randomBoolean();
        List<List<String>> expectedDecodedIps = new ArrayList<>();
        if (asc) {
            expectedDecodedIps.add(List.of("1.1.1.0", "32.183.93.40"));
            expectedDecodedIps.add(List.of("1.198.3.93", "2.3.4.5", "255.123.123.0"));
            expectedDecodedIps.add(List.of("104.30.244.2", "127.0.0.1"));
            expectedDecodedIps.add(List.of("104.30.244.2", "124.255.255.255"));
            expectedDecodedIps.add(List.of("104.244.4.1"));
            expectedDecodedIps.add(List.of("123.4.245.23", "123.4.245.23"));
        } else {
            expectedDecodedIps.add(List.of("1.198.3.93", "2.3.4.5", "255.123.123.0"));
            expectedDecodedIps.add(List.of("104.30.244.2", "127.0.0.1"));
            expectedDecodedIps.add(List.of("104.30.244.2", "124.255.255.255"));
            expectedDecodedIps.add(List.of("123.4.245.23", "123.4.245.23"));
            expectedDecodedIps.add(List.of("104.244.4.1"));
            expectedDecodedIps.add(List.of("1.1.1.0", "32.183.93.40"));
        }

        assertIPSortingOnMultiValues(ips, asc, Block.MvOrdering.ASCENDING, expectedDecodedIps);
    }

    private void assertIPSortingOnMultiValues(
        List<List<String>> ips,
        boolean asc,
        Block.MvOrdering blockOrdering,
        List<List<String>> expectedDecodedIps
    ) throws UnknownHostException {
        Block.Builder builder = BYTES_REF.newBlockBuilder(ips.size());
        builder.mvOrdering(blockOrdering);
        boolean nullsFirst = randomBoolean();

        for (List<String> mvIp : ips) {
            if (mvIp == null) {
                builder.appendNull();
            } else {
                builder.beginPositionEntry();
                for (String ip : mvIp) {
                    append(builder, new BytesRef(InetAddressPoint.encode(InetAddress.getByName(ip))));
                }
                builder.endPositionEntry();
            }
        }

        Set<TopNOperator.SortOrder> orders = new HashSet<>(1);
        orders.add(new TopNOperator.SortOrder(0, asc, nullsFirst, BYTESREF_FIXED_LENGTH_ENCODER));

        List<List<Object>> actual = new ArrayList<>();
        try (
            Driver driver = new Driver(
                new DriverContext(),
                new CannedSourceOperator(List.of(new Page(builder.build())).iterator()),
                List.of(new TopNOperator(ips.size(), orders.stream().toList(), randomPageSize())),
                new PageConsumerOperator(p -> readInto(actual, p)),
                () -> {}
            )
        ) {
            runDriver(driver);
        }

        assertThat(actual.size(), equalTo(1));
        List<List<String>> actualDecodedIps = actual.get(0).stream().map(row -> {
            if (row == null) {
                return null;
            } else if (row instanceof List<?> list) {
                return list.stream()
                    .map(v -> InetAddressPoint.decode(BytesRef.deepCopyOf((BytesRef) v).bytes))
                    .map(NetworkAddress::format)
                    .collect(Collectors.toCollection(ArrayList::new));
            } else {
                return List.of(NetworkAddress.format(InetAddressPoint.decode(BytesRef.deepCopyOf((BytesRef) row).bytes)));
            }
        }).collect(Collectors.toCollection(ArrayList::new));

        if (nullsFirst) {
            expectedDecodedIps.add(0, null);
            expectedDecodedIps.add(0, null);
        } else {
            expectedDecodedIps.add(null);
            expectedDecodedIps.add(null);
        }

        assertThat(actualDecodedIps, equalTo(expectedDecodedIps));
    }

    /**
     * This test checks that the separator character \0 is handled properly when the compared BytesRefs have such a character in them.
     * This compares one row made of a text [abNULc] and an int 100 with a second row made of text [ab] and an int 100.
     *
     * This is the equivalent query:
     * from test
     * | sort text asc [random nulls first/last], integer [random asc/desc] [random nulls first/last]
     * | limit 2
     *
     * with test data as:
     * text   | integer
     * ----------------
     * abNULc | 100
     * ab     | 100
     *
     * [abNULc] should always be greater than [ab] (because of their length - when one string is the prefix of the other, the longer string
     * is greater).
     * If this NUL byte in [abNULc] would be somehow considered the separator, then the comparison would be [ab] vs [ab] and then
     * [NULc100] vs [100] which results in [NULc100] less than [100] (the opposite of [abNULc] greater than [ab])
     */
    public void testZeroByte() {
        String text1 = new String(new char[] { 'a', 'b', '\0', 'c' });
        String text2 = new String(new char[] { 'a', 'b' });

        Block.Builder builderText = BYTES_REF.newBlockBuilder(2);
        Block.Builder builderInt = INT.newBlockBuilder(2);
        append(builderText, new BytesRef(text1));
        append(builderText, new BytesRef(text2));
        append(builderInt, 100);
        append(builderInt, 100);

        List<Block> blocks = new ArrayList<>(2);
        blocks.add(builderText.build());
        blocks.add(builderInt.build());
        Set<TopNOperator.SortOrder> orders = new HashSet<>(2);
        orders.add(new TopNOperator.SortOrder(0, true, randomBoolean(), BYTESREF_UTF8_ENCODER));
        orders.add(new TopNOperator.SortOrder(1, randomBoolean(), randomBoolean(), DEFAULT_ENCODER));

        List<List<Object>> actual = new ArrayList<>();
        try (
            Driver driver = new Driver(
                new DriverContext(),
                new CannedSourceOperator(List.of(new Page(blocks.toArray(Block[]::new))).iterator()),
                List.of(new TopNOperator(2, orders.stream().toList(), randomPageSize())),
                new PageConsumerOperator(p -> readInto(actual, p)),
                () -> {}
            )
        ) {
            runDriver(driver);
        }

        assertThat(actual.size(), equalTo(2));
        assertThat(actual.get(0).size(), equalTo(2));
        assertThat(((BytesRef) actual.get(0).get(0)).utf8ToString(), equalTo(text2));
        assertThat(((BytesRef) actual.get(0).get(1)).utf8ToString(), equalTo(text1));
        assertThat(actual.get(1).size(), equalTo(2));
        assertThat((Integer) actual.get(1).get(0), equalTo(100));
        assertThat((Integer) actual.get(1).get(1), equalTo(100));
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static void readAsRows(List<List<List<Object>>> values, Page page) {
        if (page.getBlockCount() == 0) {
            fail("No blocks returned!");
        }
        if (values.isEmpty()) {
            while (values.size() < page.getPositionCount()) {
                values.add(new ArrayList<>());
            }
        } else {
            if (values.size() != page.getPositionCount()) {
                throw new IllegalArgumentException("Can't load values from blocks with different numbers of positions");
            }
        }

        for (int i = 0; i < page.getBlockCount(); i++) {
            for (int p = 0; p < page.getBlock(i).getPositionCount(); p++) {
                Object value = toJavaObject(page.getBlock(i), p);
                if (value instanceof List l) {
                    values.get(p).add(l);
                } else {
                    List<Object> valueAsList = new ArrayList<>(1); // list of null is also possible
                    valueAsList.add(value);
                    values.get(p).add(valueAsList);
                }
            }
        }
    }

    /**
     * Because the ordering algorithm used here (mainly vanilla Java sorting with streams and Comparators) uses
     * compareTo which also considers equality, multiple rows can be considered equal between each others
     * and the order in which these rows end up in the final result can differ from the algorithm used by TopNOperator (which uses
     * PriorityQueue.lessThan method that has no concept of "equality").
     *
     * As a consequence, this method does on-the-spot min/max "reduction" of MV values, so that rows are compared based on these
     * values and not the actual content of a MV value. It is not the ideal scenario (comparing all values of all rows in the result),
     * but it's as close as possible to a very general and fully randomized unit test for TopNOperator with multi-values support.
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private List<List<Object>> extractAndReduceSortedValues(List<List<List<Object>>> rows, Set<TopNOperator.SortOrder> orders) {
        List<List<Object>> result = new ArrayList<>(rows.size());

        for (List<List<Object>> row : rows) {
            List<Object> resultRow = new ArrayList<>(orders.size());
            for (TopNOperator.SortOrder order : orders) {
                List<Object> valueAt = row.get(order.channel());
                if (valueAt.size() == 1) {
                    resultRow.add(valueAt);
                } else {
                    Object minMax = order.asc()
                        ? valueAt.stream().map(element -> (Comparable) element).min(Comparator.<Comparable>naturalOrder()).get()
                        : valueAt.stream().map(element -> (Comparable) element).max(Comparator.<Comparable>naturalOrder()).get();
                    resultRow.add(List.of(minMax));
                }
            }
            result.add(resultRow);
        }
        return result;
    }

    private class NaiveTopNComparator implements Comparator<List<List<Object>>> {
        private final Set<TopNOperator.SortOrder> orders;

        NaiveTopNComparator(Set<TopNOperator.SortOrder> orders) {
            this.orders = orders;
        }

        @Override
        public int compare(List<List<Object>> row1, List<List<Object>> row2) {
            for (TopNOperator.SortOrder order : orders) {
                int cmp = comparePositions(order.asc(), order.nullsFirst(), row1.get(order.channel()), row2.get(order.channel()));
                if (cmp != 0) {
                    return cmp;
                }
            }
            return 0;
        }

        @SuppressWarnings({ "unchecked", "rawtypes" })
        private int comparePositions(boolean asc, boolean nullsFirst, List<Object> value1, List<Object> value2) {
            boolean firstIsNull = value1.size() == 1 && value1.get(0) == null;
            boolean secondIsNull = value2.size() == 1 && value2.get(0) == null;

            if (firstIsNull || secondIsNull) {
                return Boolean.compare(firstIsNull, secondIsNull) * (nullsFirst ? -1 : 1);
            }
            List<Comparable> v1 = value1.stream().map(element -> (Comparable) element).toList();
            List<Comparable> v2 = value2.stream().map(element -> (Comparable) element).toList();
            Comparable minMax1 = (Comparable) (asc
                ? v1.stream().min(Comparator.<Comparable>naturalOrder()).get()
                : v1.stream().max(Comparator.<Comparable>naturalOrder()).get());
            Comparable minMax2 = (Comparable) (asc
                ? v2.stream().min(Comparator.<Comparable>naturalOrder()).get()
                : v2.stream().max(Comparator.<Comparable>naturalOrder()).get());

            return (asc ? 1 : -1) * minMax1.compareTo(minMax2);
        }
    }

    private Version randomVersion() {
        return new Version(randomFrom(VERSIONS));
    }
}
