/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.AlwaysReferencedIndexedByShardId;
import org.elasticsearch.compute.operator.CountingCircuitBreaker;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.PageConsumerOperator;
import org.elasticsearch.compute.test.CannedSourceOperator;
import org.elasticsearch.compute.test.OperatorTestCase;
import org.elasticsearch.compute.test.SequenceLongBlockSourceOperator;
import org.elasticsearch.compute.test.TestBlockBuilder;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.compute.test.TestDriverFactory;
import org.elasticsearch.compute.test.TupleLongLongBlockSourceOperator;
import org.elasticsearch.compute.test.TypedAbstractBlockSourceBuilder;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.indices.CrankyCircuitBreakerService;
import org.elasticsearch.test.ListMatcher;
import org.elasticsearch.xpack.versionfield.Version;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static org.elasticsearch.compute.data.BlockUtils.toJavaObject;
import static org.elasticsearch.compute.data.ElementType.AGGREGATE_METRIC_DOUBLE;
import static org.elasticsearch.compute.data.ElementType.BOOLEAN;
import static org.elasticsearch.compute.data.ElementType.BYTES_REF;
import static org.elasticsearch.compute.data.ElementType.COMPOSITE;
import static org.elasticsearch.compute.data.ElementType.DOUBLE;
import static org.elasticsearch.compute.data.ElementType.EXPONENTIAL_HISTOGRAM;
import static org.elasticsearch.compute.data.ElementType.FLOAT;
import static org.elasticsearch.compute.data.ElementType.INT;
import static org.elasticsearch.compute.data.ElementType.LONG;
import static org.elasticsearch.compute.data.ElementType.LONG_RANGE;
import static org.elasticsearch.compute.data.ElementType.TDIGEST;
import static org.elasticsearch.compute.operator.topn.TopNEncoder.DEFAULT_SORTABLE;
import static org.elasticsearch.compute.operator.topn.TopNEncoder.DEFAULT_UNSORTABLE;
import static org.elasticsearch.compute.operator.topn.TopNEncoder.UTF8;
import static org.elasticsearch.compute.operator.topn.TopNEncoderTests.randomPointAsWKB;
import static org.elasticsearch.compute.test.BlockTestUtils.append;
import static org.elasticsearch.compute.test.BlockTestUtils.randomValue;
import static org.elasticsearch.compute.test.BlockTestUtils.readInto;
import static org.elasticsearch.core.Tuple.tuple;
import static org.elasticsearch.test.ListMatcher.matchesList;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

public abstract class TopNOperatorTests extends OperatorTestCase {
    private static final List<Integer> NO_GROUP_KEYS = List.of();
    protected final int pageSize = randomPageSize();
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

    protected abstract List<Integer> groupKeys();

    public void testRandomTopN() {
        for (boolean asc : List.of(true, false)) {
            testRandomTopN(asc, driverContext());
        }
    }

    public void testRandomTopNCranky() {
        try {
            testRandomTopN(randomBoolean(), crankyDriverContext());
            logger.info("cranky didn't break us");
        } catch (CircuitBreakingException e) {
            logger.info("broken", e);
            assertThat(e.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
        }
    }

    protected abstract void testRandomTopN(boolean asc, DriverContext context);

    protected List<Long> topNLong(
        DriverContext driverContext,
        List<Long> inputValues,
        int limit,
        boolean ascendingOrder,
        boolean nullsFirst
    ) {
        return topNTwoLongColumns(
            driverContext,
            inputValues.stream().map(v -> tuple(v, 0L)).toList(),
            limit,
            List.of(DEFAULT_UNSORTABLE, DEFAULT_UNSORTABLE),
            List.of(new TopNOperator.SortOrder(0, ascendingOrder, nullsFirst)),
            groupKeys()
        ).stream().map(Tuple::v1).toList();
    }

    private static TupleLongLongBlockSourceOperator longLongSourceOperator(DriverContext driverContext, List<Tuple<Long, Long>> values) {
        return new TupleLongLongBlockSourceOperator(driverContext.blockFactory(), values, randomIntBetween(1, 1000));
    }

    public void testCompareInts() {
        BlockFactory blockFactory = blockFactory();
        testCompare(
            new Page(
                blockFactory.newIntBlockBuilder(2).appendInt(Integer.MIN_VALUE).appendInt(randomIntBetween(-1000, -1)).build(),
                blockFactory.newIntBlockBuilder(2).appendInt(randomIntBetween(-1000, -1)).appendInt(0).build(),
                blockFactory.newIntBlockBuilder(2).appendInt(0).appendInt(randomIntBetween(1, 1000)).build(),
                blockFactory.newIntBlockBuilder(2).appendInt(randomIntBetween(1, 1000)).appendInt(Integer.MAX_VALUE).build(),
                blockFactory.newIntBlockBuilder(2).appendInt(0).appendInt(Integer.MAX_VALUE).build()
            ),
            INT,
            DEFAULT_SORTABLE
        );
    }

    public void testCompareLongs() {
        BlockFactory blockFactory = blockFactory();
        testCompare(
            new Page(
                blockFactory.newLongBlockBuilder(2).appendLong(Long.MIN_VALUE).appendLong(randomLongBetween(-1000, -1)).build(),
                blockFactory.newLongBlockBuilder(2).appendLong(randomLongBetween(-1000, -1)).appendLong(0).build(),
                blockFactory.newLongBlockBuilder(2).appendLong(0).appendLong(randomLongBetween(1, 1000)).build(),
                blockFactory.newLongBlockBuilder(2).appendLong(randomLongBetween(1, 1000)).appendLong(Long.MAX_VALUE).build(),
                blockFactory.newLongBlockBuilder(2).appendLong(0).appendLong(Long.MAX_VALUE).build()
            ),
            LONG,
            DEFAULT_SORTABLE
        );
    }

    public void testCompareFloats() {
        BlockFactory blockFactory = blockFactory();
        testCompare(
            new Page(
                blockFactory.newFloatBlockBuilder(2).appendFloat(-Float.MAX_VALUE).appendFloat(randomFloatBetween(-1000, -1, true)).build(),
                blockFactory.newFloatBlockBuilder(2).appendFloat(randomFloatBetween(-1000, -1, true)).appendFloat(0.0f).build(),
                blockFactory.newFloatBlockBuilder(2).appendFloat(0).appendFloat(randomFloatBetween(1, 1000, true)).build(),
                blockFactory.newFloatBlockBuilder(2).appendFloat(randomLongBetween(1, 1000)).appendFloat(Float.MAX_VALUE).build(),
                blockFactory.newFloatBlockBuilder(2).appendFloat(0.0f).appendFloat(Float.MAX_VALUE).build()
            ),
            FLOAT,
            DEFAULT_SORTABLE
        );
    }

    public void testCompareDoubles() {
        BlockFactory blockFactory = blockFactory();
        testCompare(
            new Page(
                blockFactory.newDoubleBlockBuilder(2)
                    .appendDouble(-Double.MAX_VALUE)
                    .appendDouble(randomDoubleBetween(-1000, -1, true))
                    .build(),
                blockFactory.newDoubleBlockBuilder(2).appendDouble(randomDoubleBetween(-1000, -1, true)).appendDouble(0.0).build(),
                blockFactory.newDoubleBlockBuilder(2).appendDouble(0).appendDouble(randomDoubleBetween(1, 1000, true)).build(),
                blockFactory.newDoubleBlockBuilder(2).appendDouble(randomLongBetween(1, 1000)).appendDouble(Double.MAX_VALUE).build(),
                blockFactory.newDoubleBlockBuilder(2).appendDouble(0.0).appendDouble(Double.MAX_VALUE).build()
            ),
            DOUBLE,
            DEFAULT_SORTABLE
        );
    }

    public void testCompareUtf8() {
        BlockFactory blockFactory = blockFactory();
        testCompare(
            new Page(
                blockFactory.newBytesRefBlockBuilder(2).appendBytesRef(new BytesRef("bye")).appendBytesRef(new BytesRef("hello")).build()
            ),
            BYTES_REF,
            UTF8
        );
    }

    public void testCompareBooleans() {
        BlockFactory blockFactory = blockFactory();
        testCompare(
            new Page(blockFactory.newBooleanBlockBuilder(2).appendBoolean(false).appendBoolean(true).build()),
            BOOLEAN,
            DEFAULT_SORTABLE
        );
    }

    private void testCompare(Page page, ElementType elementType, TopNEncoder encoder) {
        Block nullBlock = TestBlockFactory.getNonBreakingInstance().newConstantNullBlock(1);
        Page nullPage = new Page(new Block[] { nullBlock, nullBlock, nullBlock, nullBlock, nullBlock });

        for (int b = 0; b < page.getBlockCount(); b++) {
            // Non-null identity
            for (int p = 0; p < page.getPositionCount(); p++) {
                Row row = row(elementType, encoder, b, randomBoolean(), randomBoolean(), page, p);
                assertEquals(0, TopNOperator.compareRows(row, row));
            }

            // Null identity
            for (int p = 0; p < page.getPositionCount(); p++) {
                Row row = row(elementType, encoder, b, randomBoolean(), randomBoolean(), nullPage, p);
                assertEquals(0, TopNOperator.compareRows(row, row));
            }

            // nulls first
            for (int p = 0; p < page.getPositionCount(); p++) {
                boolean asc = randomBoolean();
                Row nonNullRow = row(elementType, encoder, b, asc, true, page, p);
                Row nullRow = row(elementType, encoder, b, asc, true, nullPage, p);
                assertEquals(-1, TopNOperator.compareRows(nonNullRow, nullRow));
                assertEquals(1, TopNOperator.compareRows(nullRow, nonNullRow));
            }

            // nulls last
            for (int p = 0; p < page.getPositionCount(); p++) {
                boolean asc = randomBoolean();
                Row nonNullRow = row(elementType, encoder, b, asc, false, page, p);
                Row nullRow = row(elementType, encoder, b, asc, false, nullPage, p);
                assertEquals(1, TopNOperator.compareRows(nonNullRow, nullRow));
                assertEquals(-1, TopNOperator.compareRows(nullRow, nonNullRow));
            }

            // ascending
            {
                boolean nullsFirst = randomBoolean();
                Row r1 = row(elementType, encoder, b, true, nullsFirst, page, 0);
                Row r2 = row(elementType, encoder, b, true, nullsFirst, page, 1);
                assertThat(TopNOperator.compareRows(r1, r2), greaterThan(0));
                assertThat(TopNOperator.compareRows(r2, r1), lessThan(0));
            }
            // descending
            {
                boolean nullsFirst = randomBoolean();
                Row r1 = row(elementType, encoder, b, false, nullsFirst, page, 0);
                Row r2 = row(elementType, encoder, b, false, nullsFirst, page, 1);
                assertThat(TopNOperator.compareRows(r1, r2), lessThan(0));
                assertThat(TopNOperator.compareRows(r2, r1), greaterThan(0));
            }
        }
        page.releaseBlocks();
    }

    private Row row(ElementType elementType, TopNEncoder encoder, int channel, boolean asc, boolean nullsFirst, Page page, int position) {
        final var sortOrders = List.of(new TopNOperator.SortOrder(channel, asc, nullsFirst));
        RowFiller rf = new UngroupedRowFiller(
            IntStream.range(0, page.getBlockCount()).mapToObj(i -> elementType).toList(),
            IntStream.range(0, page.getBlockCount()).mapToObj(i -> encoder).toList(),
            sortOrders,
            page
        );
        Row row = new UngroupedRow(nonBreakingBigArrays().breakerService().getBreaker("request"), sortOrders, 0, 0);
        rf.writeKey(position, row);
        rf.writeValues(position, row);
        return row;
    }

    public void testTopNTwoColumns() {
        List<Tuple<Long, Long>> values = Arrays.asList(tuple(1L, 1L), tuple(1L, 2L), tuple(null, null), tuple(null, 1L), tuple(1L, null));
        assertThat(
            topNTwoLongColumns(
                driverContext(),
                values,
                5,
                List.of(TopNEncoder.DEFAULT_SORTABLE, TopNEncoder.DEFAULT_SORTABLE),
                List.of(new TopNOperator.SortOrder(0, true, false), new TopNOperator.SortOrder(1, true, false)),
                groupKeys()
            ),
            equalTo(List.of(tuple(1L, 1L), tuple(1L, 2L), tuple(1L, null), tuple(null, 1L), tuple(null, null)))
        );
        assertThat(
            topNTwoLongColumns(
                driverContext(),
                values,
                5,
                List.of(TopNEncoder.DEFAULT_SORTABLE, TopNEncoder.DEFAULT_SORTABLE),
                List.of(new TopNOperator.SortOrder(0, true, true), new TopNOperator.SortOrder(1, true, false)),
                groupKeys()
            ),
            equalTo(List.of(tuple(null, 1L), tuple(null, null), tuple(1L, 1L), tuple(1L, 2L), tuple(1L, null)))
        );
        assertThat(
            topNTwoLongColumns(
                driverContext(),
                values,
                5,
                List.of(TopNEncoder.DEFAULT_SORTABLE, TopNEncoder.DEFAULT_SORTABLE),
                List.of(new TopNOperator.SortOrder(0, true, false), new TopNOperator.SortOrder(1, true, true)),
                groupKeys()
            ),
            equalTo(List.of(tuple(1L, null), tuple(1L, 1L), tuple(1L, 2L), tuple(null, null), tuple(null, 1L)))
        );
    }

    public void testCollectAllValues() {
        DriverContext driverContext = driverContext();
        BlockFactory blockFactory = driverContext.blockFactory();

        int size = 10;
        int topCount = 3;
        List<Block> blocks = new ArrayList<>();
        List<List<Object>> rawValues = new ArrayList<>();

        IntBlock keys = blockFactory.newIntArrayVector(IntStream.range(0, size).toArray(), size).asBlock();
        blocks.add(keys);
        rawValues.add(IntStream.range(0, size).<Object>mapToObj(Integer::valueOf).toList());

        List<ElementType> elementTypes = new ArrayList<>();
        List<TopNEncoder> encoders = new ArrayList<>();

        // Add the keys
        elementTypes.add(INT);
        encoders.add(DEFAULT_SORTABLE);

        for (ElementType e : ElementType.values()) {
            if (e == ElementType.UNKNOWN || e == COMPOSITE || e == EXPONENTIAL_HISTOGRAM || e == TDIGEST) {
                continue;
            }
            elementTypes.add(e);
            encoders.add(nonKeyEncoder(e));
            try (Block.Builder builder = e.newBlockBuilder(size, driverContext().blockFactory())) {
                var rawValuesForElement = new ArrayList<>(size);
                for (int i = 0; i < size; i++) {
                    Object value = randomValue(e);
                    append(builder, value);
                    rawValuesForElement.add(value);
                }
                blocks.add(builder.build());
                rawValues.add(rawValuesForElement);
            }
        }

        List<List<Object>> actualTop = new ArrayList<>();
        List<TopNOperator.SortOrder> sortOrders = List.of(new TopNOperator.SortOrder(0, false, false));
        try (
            Driver driver = TestDriverFactory.create(
                driverContext,
                new CannedSourceOperator(List.of(new Page(blocks.toArray(Block[]::new))).iterator()),
                List.of(
                    new TopNOperator(
                        blockFactory,
                        nonBreakingBigArrays().breakerService().getBreaker("request"),
                        topCount,
                        elementTypes,
                        encoders,
                        sortOrders,
                        groupKeys(),
                        randomPageSize()
                    )
                ),
                new PageConsumerOperator(page -> readInto(actualTop, page))
            )
        ) {
            runDriver(driver);
        }

        assertMap(actualTop, matchesList(expectedTop(rawValues, sortOrders, topCount)));
        assertDriverContext(driverContext);
    }

    protected List<List<Object>> expectedTop(List<List<Object>> input, List<TopNOperator.SortOrder> sortOrders, int topCount) {
        // input is channel-oriented, transpose to row-oriented for processing and then back format.
        return transpose(expectedTopRowOriented(transpose(input), sortOrders, topCount));
    }

    protected abstract List<List<Object>> expectedTopRowOriented(
        List<List<Object>> rowOriented,
        List<TopNOperator.SortOrder> sortOrders,
        int topCount
    );

    private static List<List<Object>> transpose(List<List<Object>> input) {
        if (input.isEmpty()) {
            return new ArrayList<>();
        }
        int numRows = input.getFirst().size();
        return IntStream.range(0, numRows).mapToObj(row -> input.stream().map(channel -> channel.get(row)).toList()).toList();
    }

    public void testCollectAllValues_RandomMultiValues() {
        DriverContext driverContext = driverContext();
        BlockFactory blockFactory = driverContext.blockFactory();

        int rows = 10;
        int topCount = 3;
        int blocksCount = 20;
        List<Block> blocks = new ArrayList<>();
        List<List<Object>> rawValues = new ArrayList<>();

        IntBlock keys = blockFactory.newIntArrayVector(IntStream.range(0, rows).toArray(), rows).asBlock();
        blocks.add(keys);
        rawValues.add(IntStream.range(0, rows).<Object>mapToObj(Integer::valueOf).toList());

        List<ElementType> elementTypes = new ArrayList<>(blocksCount);
        List<TopNEncoder> encoders = new ArrayList<>(blocksCount);

        // Add the keys
        elementTypes.add(INT);
        encoders.add(DEFAULT_UNSORTABLE);

        for (int type = 0; type < blocksCount; type++) {
            ElementType e = randomFrom(ElementType.values());
            if (e == ElementType.UNKNOWN
                || e == COMPOSITE
                || e == AGGREGATE_METRIC_DOUBLE
                || e == EXPONENTIAL_HISTOGRAM
                || e == TDIGEST
                || e == LONG_RANGE) {
                continue;
            }
            elementTypes.add(e);
            encoders.add(nonKeyEncoder(e));
            List<Object> channelValues = new ArrayList<>();
            try (Block.Builder builder = e.newBlockBuilder(rows, driverContext().blockFactory())) {
                for (int i = 0; i < rows; i++) {
                    if (e != ElementType.DOC && e != ElementType.NULL && randomBoolean()) {
                        // generate a multi-value block
                        int mvCount = randomIntBetween(5, 10);
                        List<Object> mvValues = new ArrayList<>(mvCount);
                        builder.beginPositionEntry();
                        for (int j = 0; j < mvCount; j++) {
                            Object value = randomValue(e);
                            append(builder, value);
                            mvValues.add(value);
                        }
                        builder.endPositionEntry();
                        channelValues.add(mvValues);
                    } else {
                        Object value = randomValue(e);
                        append(builder, value);
                        channelValues.add(value);
                    }
                }
                blocks.add(builder.build());
                rawValues.add(channelValues);
            }
        }

        List<List<Object>> actualTop = new ArrayList<>();
        List<TopNOperator.SortOrder> sortOrders = List.of(new TopNOperator.SortOrder(0, false, false));
        try (
            Driver driver = TestDriverFactory.create(
                driverContext,
                new CannedSourceOperator(List.of(new Page(blocks.toArray(Block[]::new))).iterator()),
                List.of(
                    new TopNOperator(
                        blockFactory,
                        nonBreakingBigArrays().breakerService().getBreaker("request"),
                        topCount,
                        elementTypes,
                        encoders,
                        sortOrders,
                        groupKeys(),
                        randomPageSize()
                    )
                ),
                new PageConsumerOperator(page -> readInto(actualTop, page))
            )
        ) {
            runDriver(driver);
        }

        assertMap(actualTop, matchesList(expectedTop(rawValues, sortOrders, topCount)));
        assertDriverContext(driverContext);
    }

    private static TopNEncoder nonKeyEncoder(ElementType elementType) {
        return switch (elementType) {
            case BYTES_REF -> UTF8;
            case DOC -> new DocVectorEncoder(AlwaysReferencedIndexedByShardId.INSTANCE);
            default -> DEFAULT_UNSORTABLE;
        };
    }

    protected List<Tuple<Long, Long>> topNTwoLongColumns(
        DriverContext driverContext,
        List<Tuple<Long, Long>> values,
        int limit,
        List<TopNEncoder> encoder,
        List<TopNOperator.SortOrder> sortOrders,
        List<Integer> groupKeys
    ) {
        var pages = topNMultipleColumns(
            driverContext,
            new TupleLongLongBlockSourceOperator(driverContext.blockFactory(), values, randomIntBetween(1, 1000)),
            limit,
            encoder,
            sortOrders,
            groupKeys
        );
        return pageToTuples(
            (block, i) -> block.isNull(i) ? null : ((LongBlock) block).getLong(i),
            (block, i) -> block.isNull(i) ? null : ((LongBlock) block).getLong(i),
            pages
        );
    }

    protected <T, S> List<Page> topNMultipleColumns(
        DriverContext driverContext,
        TypedAbstractBlockSourceBuilder sourceOperator,
        int limit,
        List<TopNEncoder> encoder,
        List<TopNOperator.SortOrder> sortOrders,
        List<Integer> groupKeys
    ) {
        var pages = new ArrayList<Page>();
        boolean success = false;
        try {
            try (
                Driver driver = TestDriverFactory.create(
                    driverContext,
                    sourceOperator,
                    List.of(
                        new TopNOperator(
                            driverContext.blockFactory(),
                            nonBreakingBigArrays().breakerService().getBreaker("request"),
                            limit,
                            sourceOperator.elementTypes(),
                            encoder,
                            sortOrders,
                            groupKeys,
                            randomPageSize()
                        )
                    ),
                    new PageConsumerOperator(pages::add)
                )
            ) {
                runDriver(driver);
            }
            assertDriverContext(driverContext);
            success = true;
        } finally {
            if (success == false) {
                Releasables.close(pages);
            }
        }
        return pages;
    }

    protected static <T, S> List<Tuple<T, S>> pageToTuples(
        BiFunction<Block, Integer, T> getFirstBlockValue,
        BiFunction<Block, Integer, S> getSecondBlockValue,
        List<Page> pages
    ) {
        var result = new ArrayList<Tuple<T, S>>();
        for (Page page : pages) {
            var block1 = page.getBlock(0);
            var block2 = page.getBlock(1);
            for (int i = 0; i < block1.getPositionCount(); i++) {
                result.add(
                    tuple(
                        block1.isNull(i) ? null : getFirstBlockValue.apply(block1, i),
                        block2.isNull(i) ? null : getSecondBlockValue.apply(block2, i)
                    )
                );
            }
            page.releaseBlocks();
        }

        return result;
    }

    public void testTopNManyDescriptionAndToString() {
        int fixedLength = between(1, 100);
        TopNOperator.TopNOperatorFactory factory = new TopNOperator.TopNOperatorFactory(
            10,
            List.of(BYTES_REF, BYTES_REF),
            List.of(UTF8, new FixedLengthTopNEncoder(fixedLength)),
            List.of(new TopNOperator.SortOrder(1, false, false), new TopNOperator.SortOrder(3, false, true)),
            randomPageSize()
        );
        String sorts = List.of("SortOrder[channel=1, asc=false, nullsFirst=false]", "SortOrder[channel=3, asc=false, nullsFirst=true]")
            .stream()
            .collect(Collectors.joining(", "));
        String tail = ", elementTypes=[BYTES_REF, BYTES_REF], encoders=[UTF8TopNEncoder, FixedLengthTopNEncoder["
            + fixedLength
            + "]], sortOrders=["
            + sorts
            + "]]";
        assertThat(factory.describe(), equalTo("TopNOperator[count=10" + tail));
        try (Operator operator = factory.get(driverContext())) {
            assertThat(operator.toString(), equalTo("TopNOperator[count=0/10" + tail));
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
            DEFAULT_SORTABLE,
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
            DEFAULT_SORTABLE,
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
            DEFAULT_SORTABLE,
            new TopNOperator.SortOrder(0, false, false),
            new TopNOperator.SortOrder(0, true, false)
        );
    }

    public void testTopNWithSortingOnSameField_DESC_then_ASC_boolean() {
        assertSortingOnMV(
            BOOL_MV,
            List.of(List.of(true, false), List.of(true, false), true, List.of(true, true, true), List.of(false, false, false), false),
            BOOLEAN,
            DEFAULT_SORTABLE,
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
            UTF8,
            new TopNOperator.SortOrder(0, false, false),
            new TopNOperator.SortOrder(0, true, false)
        );
    }

    public void testTopNWithSortingOnSameField_ASC_then_DESC_int() {
        assertSortingOnMV(
            INT_MV,
            List.of(List.of(-1, 63, 2), List.of(22, 21, 22), 50, List.of(63, 61, 62), 100),
            INT,
            DEFAULT_SORTABLE,
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
            DEFAULT_SORTABLE,
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
            DEFAULT_SORTABLE,
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
            UTF8,
            new TopNOperator.SortOrder(0, true, false),
            new TopNOperator.SortOrder(0, false, false)
        );
    }

    private void assertSortingOnMV(
        List<List<Object>> values,
        List<Object> expectedValues,
        ElementType blockType,
        TopNEncoder encoder,
        TopNOperator.SortOrder... sortOrders
    ) {
        DriverContext driverContext = driverContext();
        Block block = TestBlockBuilder.blockFromValues(values, blockType);
        assert block.mvOrdering() == Block.MvOrdering.UNORDERED : "Blocks created for this test must have unordered multi-values";
        Page page = new Page(block);

        List<List<Object>> actualValues = new ArrayList<>();
        int topCount = randomIntBetween(1, values.size());
        try (
            Driver driver = TestDriverFactory.create(
                driverContext,
                new CannedSourceOperator(List.of(page).iterator()),
                List.of(
                    new TopNOperator(
                        driverContext.blockFactory(),
                        nonBreakingBigArrays().breakerService().getBreaker("request"),
                        topCount,
                        List.of(blockType),
                        List.of(encoder),
                        List.of(sortOrders),
                        NO_GROUP_KEYS,
                        randomPageSize()
                    )
                ),
                new PageConsumerOperator(p -> readInto(actualValues, p))
            )
        ) {
            runDriver(driver);
        }
        assertMap(actualValues, matchesList(List.of(expectedValues.subList(0, topCount))));
    }

    public void testIPSortingSingleValue() throws UnknownHostException {
        List<String> ips = List.of("123.4.245.23", "104.244.253.29", "1.198.3.93", "32.183.93.40", "104.30.244.2", "104.244.4.1");
        try (Block.Builder builder = BYTES_REF.newBlockBuilder(ips.size(), driverContext().blockFactory())) {
            boolean asc = randomBoolean();

            for (String ip : ips) {
                append(builder, new BytesRef(InetAddressPoint.encode(InetAddress.getByName(ip))));
            }

            DriverContext driverContext = driverContext();
            List<List<Object>> actual = new ArrayList<>();
            try (
                Driver driver = TestDriverFactory.create(
                    driverContext,
                    new CannedSourceOperator(List.of(new Page(builder.build())).iterator()),
                    List.of(
                        new TopNOperator(
                            driverContext.blockFactory(),
                            nonBreakingBigArrays().breakerService().getBreaker("request"),
                            ips.size(),
                            List.of(BYTES_REF),
                            List.of(TopNEncoder.IP),
                            List.of(new TopNOperator.SortOrder(0, asc, randomBoolean())),
                            NO_GROUP_KEYS,
                            randomPageSize()
                        )
                    ),
                    new PageConsumerOperator(p -> readInto(actual, p))
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
        ips.add(List.of("123.4.245.23", "123.4.245.24"));
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
            expectedDecodedIps.add(List.of("123.4.245.23", "123.4.245.24"));
        } else {
            expectedDecodedIps.add(List.of("1.198.3.93", "2.3.4.5", "255.123.123.0"));
            expectedDecodedIps.add(List.of("104.30.244.2", "127.0.0.1"));
            expectedDecodedIps.add(List.of("104.30.244.2", "124.255.255.255"));
            expectedDecodedIps.add(List.of("123.4.245.23", "123.4.245.24"));
            expectedDecodedIps.add(List.of("104.244.4.1"));
            expectedDecodedIps.add(List.of("1.1.1.0", "32.183.93.40"));
        }

        assertIPSortingOnMultiValues(ips, asc, Block.MvOrdering.DEDUPLICATED_AND_SORTED_ASCENDING, expectedDecodedIps);
    }

    private void assertIPSortingOnMultiValues(
        List<List<String>> ips,
        boolean asc,
        Block.MvOrdering blockOrdering,
        List<List<String>> expectedDecodedIps
    ) throws UnknownHostException {
        try (Block.Builder builder = BYTES_REF.newBlockBuilder(ips.size(), driverContext().blockFactory())) {
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

            List<List<Object>> actual = new ArrayList<>();
            DriverContext driverContext = driverContext();
            try (
                Driver driver = TestDriverFactory.create(
                    driverContext,
                    new CannedSourceOperator(List.of(new Page(builder.build())).iterator()),
                    List.of(
                        new TopNOperator(
                            driverContext.blockFactory(),
                            nonBreakingBigArrays().breakerService().getBreaker("request"),
                            ips.size(),
                            List.of(BYTES_REF),
                            List.of(TopNEncoder.IP),
                            List.of(new TopNOperator.SortOrder(0, asc, nullsFirst)),
                            NO_GROUP_KEYS,
                            randomPageSize()
                        )
                    ),
                    new PageConsumerOperator(p -> readInto(actual, p))
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

        List<Block> blocks = new ArrayList<>(2);
        try (
            Block.Builder builderText = BYTES_REF.newBlockBuilder(2, driverContext().blockFactory());
            Block.Builder builderInt = INT.newBlockBuilder(2, driverContext().blockFactory())
        ) {
            append(builderText, new BytesRef(text1));
            append(builderText, new BytesRef(text2));
            append(builderInt, 100);
            append(builderInt, 100);

            blocks.add(builderText.build());
            blocks.add(builderInt.build());
        }

        List<List<Object>> actual = new ArrayList<>();
        DriverContext driverContext = driverContext();
        try (
            Driver driver = TestDriverFactory.create(
                driverContext,
                new CannedSourceOperator(List.of(new Page(blocks.toArray(Block[]::new))).iterator()),
                List.of(
                    new TopNOperator(
                        driverContext.blockFactory(),
                        nonBreakingBigArrays().breakerService().getBreaker("request"),
                        2,
                        List.of(BYTES_REF, INT),
                        List.of(TopNEncoder.UTF8, DEFAULT_UNSORTABLE),
                        List.of(
                            new TopNOperator.SortOrder(0, true, randomBoolean()),
                            new TopNOperator.SortOrder(1, randomBoolean(), randomBoolean())
                        ),
                        NO_GROUP_KEYS,
                        randomPageSize()
                    )
                ),
                new PageConsumerOperator(p -> readInto(actual, p))
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

    public void testErrorBeforeFullyDraining() {
        int maxPageSize = between(1, 100);
        int topCount = maxPageSize * 4;
        int docCount = topCount * 10;
        List<List<Object>> actual = new ArrayList<>();
        DriverContext driverContext = driverContext();
        try (
            Driver driver = TestDriverFactory.create(
                driverContext,
                new SequenceLongBlockSourceOperator(driverContext.blockFactory(), LongStream.range(0, docCount)),
                List.of(
                    new TopNOperator(
                        driverContext.blockFactory(),
                        nonBreakingBigArrays().breakerService().getBreaker("request"),
                        topCount,
                        List.of(LONG),
                        List.of(DEFAULT_UNSORTABLE),
                        List.of(new TopNOperator.SortOrder(0, true, randomBoolean())),
                        NO_GROUP_KEYS,
                        maxPageSize
                    )
                ),
                new PageConsumerOperator(p -> {
                    assertThat(p.getPositionCount(), equalTo(maxPageSize));
                    if (actual.isEmpty()) {
                        readInto(actual, p);
                    } else {
                        p.releaseBlocks();
                        throw new RuntimeException("boo");
                    }
                })
            )
        ) {
            Exception e = expectThrows(RuntimeException.class, () -> runDriver(driver));
            assertThat(e.getMessage(), equalTo("boo"));
        }

        ListMatcher values = matchesList();
        for (int i = 0; i < maxPageSize; i++) {
            values = values.item((long) i);
        }
        assertMap(actual, matchesList().item(values));
    }

    public void testCloseWithoutCompleting() {
        CircuitBreaker breaker = new MockBigArrays.LimitedBreaker(CircuitBreaker.REQUEST, ByteSizeValue.ofGb(1));
        try (
            TopNOperator op = new TopNOperator(
                driverContext().blockFactory(),
                breaker,
                2,
                List.of(INT),
                List.of(DEFAULT_UNSORTABLE),
                List.of(new TopNOperator.SortOrder(0, randomBoolean(), randomBoolean())),
                NO_GROUP_KEYS,
                randomPageSize()
            )
        ) {
            op.addInput(new Page(blockFactory().newIntArrayVector(new int[] { 1 }, 1).asBlock()));
        }
    }

    public void testRowResizes() {
        int columns = 1000;
        int rows = 1000;
        CountingCircuitBreaker breaker = new CountingCircuitBreaker(
            new MockBigArrays.LimitedBreaker(CircuitBreaker.REQUEST, ByteSizeValue.ofGb(1))
        );
        List<ElementType> types = Collections.nCopies(columns, INT);
        List<TopNEncoder> encoders = Collections.nCopies(columns, DEFAULT_UNSORTABLE);
        boolean asc = randomBoolean();
        try (
            TopNOperator op = new TopNOperator(
                driverContext().blockFactory(),
                breaker,
                10,
                types,
                encoders,
                List.of(new TopNOperator.SortOrder(0, asc, randomBoolean())),
                NO_GROUP_KEYS,
                randomPageSize()
            )
        ) {
            int[] blockValues = IntStream.range(0, rows).toArray();
            Block block = blockFactory().newIntArrayVector(blockValues, rows).asBlock();
            Block[] blocks = new Block[1000];
            for (int i = 0; i < 1000; i++) {
                blocks[i] = block;
                block.incRef();
            }
            block.decRef();
            op.addInput(new Page(blocks));

            // 105 are from the objects
            // 1 is for the min-heap itself
            // -1 IF we're sorting ascending. We encode one less value.
            assertThat(breaker.getMemoryRequestCount(), equalTo(asc ? 105L : 106L));
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    protected static void readAsRows(List<List<List<Object>>> values, Page page) {
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

    protected static List<List<Object>> readAsRowsSingleValue(Page page) {
        assertThat(page.getBlockCount(), greaterThan(0));
        return IntStream.range(0, page.getPositionCount())
            .mapToObj(position -> IntStream.range(0, page.getBlockCount()).mapToObj(i -> toJavaObject(page.getBlock(i), position)).toList())
            .toList();
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
    protected List<List<Object>> extractAndReduceSortedValues(List<List<List<Object>>> rows, Set<TopNOperator.SortOrder> orders) {
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

    protected class NaiveTopNComparator implements Comparator<List<List<Object>>> {
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

    static Version randomVersion() {
        return new Version(randomFrom(VERSIONS));
    }

    protected static class RandomMultiValueBlocksResult {
        final List<List<List<Object>>> expectedValues;
        final List<Block> blocks;
        final boolean[] validSortKeys;
        final List<ElementType> elementTypes;
        final List<TopNEncoder> encoders;

        RandomMultiValueBlocksResult(
            List<List<List<Object>>> expectedValues,
            List<Block> blocks,
            boolean[] validSortKeys,
            List<ElementType> elementTypes,
            List<TopNEncoder> encoders
        ) {
            this.expectedValues = expectedValues;
            this.blocks = blocks;
            this.validSortKeys = validSortKeys;
            this.elementTypes = elementTypes;
            this.encoders = encoders;
        }
    }

    protected RandomMultiValueBlocksResult generateRandomMultiValueBlocks(int rows, int blocksCount, DriverContext driverContext) {
        List<List<List<Object>>> expectedValues = new ArrayList<>(rows);
        List<Block> blocks = new ArrayList<>(blocksCount);
        boolean[] validSortKeys = new boolean[blocksCount];
        List<ElementType> elementTypes = new ArrayList<>(blocksCount);
        List<TopNEncoder> encoders = new ArrayList<>(blocksCount);

        for (int i = 0; i < rows; i++) {
            expectedValues.add(new ArrayList<>(blocksCount));
        }

        for (int type = 0; type < blocksCount; type++) {
            ElementType e = randomValueOtherThanMany(
                t -> t == ElementType.UNKNOWN
                    || t == ElementType.DOC
                    || t == COMPOSITE
                    || t == AGGREGATE_METRIC_DOUBLE
                    || t == EXPONENTIAL_HISTOGRAM
                    || t == TDIGEST
                    || t == LONG_RANGE,
                () -> randomFrom(ElementType.values())
            );
            elementTypes.add(e);
            validSortKeys[type] = true;
            try (Block.Builder builder = e.newBlockBuilder(rows, driverContext.blockFactory())) {
                List<Object> previousValue = null;
                Function<ElementType, Object> randomValueSupplier = (blockType) -> randomValue(blockType);
                if (e == BYTES_REF) {
                    if (rarely()) {
                        randomValueSupplier = switch (randomInt(2)) {
                            case 0 -> {
                                encoders.add(TopNEncoder.IP);
                                yield (blockType) -> new BytesRef(InetAddressPoint.encode(randomIp(randomBoolean())));
                            }
                            case 1 -> {
                                encoders.add(TopNEncoder.VERSION);
                                yield (blockType) -> randomVersion().toBytesRef();
                            }
                            case 2 -> {
                                encoders.add(DEFAULT_UNSORTABLE);
                                validSortKeys[type] = false;
                                yield (blockType) -> randomPointAsWKB();
                            }
                            default -> throw new UnsupportedOperationException();
                        };
                    } else {
                        encoders.add(UTF8);
                    }
                } else {
                    encoders.add(DEFAULT_SORTABLE);
                }

                for (int i = 0; i < rows; i++) {
                    List<Object> values = new ArrayList<>();
                    if (rarely() && previousValue != null) {
                        values = previousValue;
                    } else {
                        if (e != ElementType.NULL && randomBoolean()) {
                            int mvCount = randomIntBetween(5, 10);
                            for (int j = 0; j < mvCount; j++) {
                                Object value = randomValueSupplier.apply(e);
                                values.add(value);
                            }
                        } else {
                            Object value = randomValueSupplier.apply(e);
                            values.add(value);
                        }

                        if (usually() && randomBoolean()) {
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
        }

        return new RandomMultiValueBlocksResult(expectedValues, blocks, validSortKeys, elementTypes, encoders);
    }

    protected Set<TopNOperator.SortOrder> generateSortOrders(
        int sortingByColumns,
        int blocksCount,
        boolean[] validSortKeys,
        java.util.function.Predicate<Integer> excludeColumn
    ) {
        Set<TopNOperator.SortOrder> uniqueOrders = new LinkedHashSet<>(sortingByColumns);
        for (int i = 0; i < sortingByColumns; i++) {
            int column = randomValueOtherThanMany(
                c -> false == validSortKeys[c] || excludeColumn.test(c),
                () -> randomIntBetween(0, blocksCount - 1)
            );
            uniqueOrders.add(new TopNOperator.SortOrder(column, randomBoolean(), randomBoolean()));
        }
        return uniqueOrders;
    }

    protected static class RandomBlocksResult {
        final List<List<Object>> expectedValues;
        final List<Block> blocks;
        final boolean[] validSortKeys;
        final List<ElementType> elementTypes;
        final List<TopNEncoder> encoders;

        RandomBlocksResult(
            List<List<Object>> expectedValues,
            List<Block> blocks,
            boolean[] validSortKeys,
            List<ElementType> elementTypes,
            List<TopNEncoder> encoders
        ) {
            this.expectedValues = expectedValues;
            this.blocks = blocks;
            this.validSortKeys = validSortKeys;
            this.elementTypes = elementTypes;
            this.encoders = encoders;
        }
    }

    protected RandomBlocksResult generateRandomSingleValueBlocks(int rows, int blocksCount, DriverContext driverContext) {
        List<List<Object>> expectedValues = new ArrayList<>(rows);
        List<Block> blocks = new ArrayList<>(blocksCount);
        boolean[] validSortKeys = new boolean[blocksCount];
        List<ElementType> elementTypes = new ArrayList<>(blocksCount);
        List<TopNEncoder> encoders = new ArrayList<>(blocksCount);

        for (int i = 0; i < rows; i++) {
            expectedValues.add(new ArrayList<>(blocksCount));
        }

        for (int type = 0; type < blocksCount; type++) {
            ElementType e = randomValueOtherThanMany(
                t -> t == ElementType.UNKNOWN
                    || t == ElementType.DOC
                    || t == COMPOSITE
                    || t == AGGREGATE_METRIC_DOUBLE
                    || t == EXPONENTIAL_HISTOGRAM
                    || t == TDIGEST
                    || t == LONG_RANGE,
                () -> randomFrom(ElementType.values())
            );
            elementTypes.add(e);
            validSortKeys[type] = true;
            try (Block.Builder builder = e.newBlockBuilder(rows, driverContext.blockFactory())) {
                Function<ElementType, Object> randomValueSupplier = (blockType) -> randomValue(blockType);
                if (e == BYTES_REF) {
                    if (rarely()) {
                        randomValueSupplier = switch (randomInt(2)) {
                            case 0 -> {
                                encoders.add(TopNEncoder.IP);
                                yield (blockType) -> new BytesRef(InetAddressPoint.encode(randomIp(randomBoolean())));
                            }
                            case 1 -> {
                                encoders.add(TopNEncoder.VERSION);
                                yield (blockType) -> randomVersion().toBytesRef();
                            }
                            case 2 -> {
                                encoders.add(DEFAULT_UNSORTABLE);
                                validSortKeys[type] = false;
                                yield (blockType) -> randomPointAsWKB();
                            }
                            default -> throw new UnsupportedOperationException();
                        };
                    } else {
                        encoders.add(UTF8);
                    }
                } else {
                    encoders.add(DEFAULT_SORTABLE);
                }

                for (int i = 0; i < rows; i++) {
                    Object value = randomValueSupplier.apply(e);
                    append(builder, value);
                    expectedValues.get(i).add(value);
                }
                blocks.add(builder.build());
            }
        }

        return new RandomBlocksResult(expectedValues, blocks, validSortKeys, elementTypes, encoders);
    }
}
