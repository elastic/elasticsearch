/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.tests.util.RamUsageTester;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.IndexedByShardIdFromList;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.ListRowsBlockSourceOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.operator.topn.TopNOperator.SortOrder;
import org.elasticsearch.compute.test.CannedSourceOperator;
import org.elasticsearch.compute.test.TestBlockBuilder;
import org.elasticsearch.compute.test.TupleLongLongBlockSourceOperator;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.SimpleRefCounted;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matcher;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.elasticsearch.compute.data.ElementType.DOC;
import static org.elasticsearch.compute.data.ElementType.LONG;
import static org.elasticsearch.compute.operator.topn.TopNEncoder.DEFAULT_UNSORTABLE;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class GroupedTopNOperatorTests extends TopNOperatorTests {
    private static final int TOP_COUNT = 4;

    @Override
    protected List<Integer> groupKeys() {
        return List.of(0);
    }

    @Override
    protected void testRandomTopN(boolean asc, DriverContext context) {
        int limit = randomIntBetween(1, 20);
        List<Tuple<Long, Long>> inputValues = randomList(0, 1000, () -> Tuple.tuple(ESTestCase.randomLong(), ESTestCase.randomLong(9)));
        List<Tuple<Long, Long>> expectedValues = computeTopN(inputValues, limit, asc);
        List<Tuple<Long, Long>> outputValues = topNTwoLongColumns(
            context,
            inputValues,
            limit,
            List.of(DEFAULT_UNSORTABLE, DEFAULT_UNSORTABLE),
            List.of(new SortOrder(0, asc, false)),
            List.of(1)
        );

        assertThat(outputValues, equalTo(expectedValues));
    }

    @Override
    protected List<List<Object>> expectedTop(List<List<Object>> input, List<SortOrder> sortOrders, int topCount) {
        // input is channel-oriented, whereas computeTopN expects row-oriented, thus, transpose before and after.
        return transpose(computeTopN(transpose(input), groupKeys(), sortOrders, topCount));
    }

    private static List<List<Object>> transpose(List<List<Object>> input) {
        return input.isEmpty()
            ? List.of()
            : IntStream.range(0, input.getFirst().size())
                .mapToObj(row -> input.stream().map(channel -> channel.get(row)).toList())
                .toList();
    }

    public void testBasicTopN() {
        List<Long> values = Arrays.asList(2L, 1L, 4L, null, 4L, null);
        assertThat(topNLong(values, 1, true, false), equalTo(Arrays.asList(1L, 2L, 4L, null)));
        assertThat(topNLong(values, 1, false, false), equalTo(Arrays.asList(4L, 2L, 1L, null)));
        assertThat(topNLong(values, 1, true, true), equalTo(Arrays.asList(null, 1L, 2L, 4L)));
        assertThat(topNLong(values, 1, false, true), equalTo(Arrays.asList(null, 4L, 2L, 1L)));
        assertThat(topNLong(values, 2, true, false), equalTo(Arrays.asList(1L, 2L, 4L, 4L, null, null)));
        assertThat(topNLong(values, 2, false, false), equalTo(Arrays.asList(4L, 4L, 2L, 1L, null, null)));
        assertThat(topNLong(values, 2, true, true), equalTo(Arrays.asList(null, null, 1L, 2L, 4L, 4L)));
        assertThat(topNLong(values, 2, false, true), equalTo(Arrays.asList(null, null, 4L, 4L, 2L, 1L)));
    }

    private List<Long> topNLong(List<Long> inputValues, int limit, boolean ascendingOrder, boolean nullsFirst) {
        return topNLong(driverContext(), inputValues, limit, ascendingOrder, nullsFirst);
    }

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        return new TupleLongLongBlockSourceOperator(
            blockFactory,
            LongStream.range(0, size).mapToObj(l -> Tuple.tuple(ESTestCase.randomLong(), ESTestCase.randomLong(9))),
            between(1, size * 2)
        );
    }

    @Override
    protected TopNOperator.TopNOperatorFactory simple(SimpleOptions options) {
        return new TopNOperator.TopNOperatorFactory(
            TOP_COUNT,
            List.of(LONG, LONG),
            List.of(DEFAULT_UNSORTABLE, DEFAULT_UNSORTABLE),
            List.of(new SortOrder(0, true, false)),
            List.of(1),
            pageSize
        );
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return equalTo(
            "TopNOperator[count=4, elementTypes=[LONG, LONG], encoders=[DefaultUnsortable, DefaultUnsortable], "
                + "sortOrders=[SortOrder[channel=0, asc=true, nullsFirst=false]], groupKeys=[1]]"
        );
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return equalTo(
            "TopNOperator[count=0/4, elementTypes=[LONG, LONG], encoders=[DefaultUnsortable, DefaultUnsortable], "
                + "sortOrders=[SortOrder[channel=0, asc=true, nullsFirst=false]], groupKeys=[1]]"
        );
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> results) {
        for (int i = 0; i < results.size() - 1; i++) {
            assertThat(results.get(i).getPositionCount(), equalTo(pageSize));
        }
        assertThat(results.get(results.size() - 1).getPositionCount(), lessThanOrEqualTo(pageSize));
        List<Tuple<Long, Long>> values = input.stream()
            .flatMap(
                page -> IntStream.range(0, page.getPositionCount())
                    .filter(p -> false == page.getBlock(0).isNull(p))
                    .mapToObj(p -> Tuple.tuple(((LongBlock) page.getBlock(0)).getLong(p), ((LongBlock) page.getBlock(1)).getLong(p)))

            )
            .toList();
        var expected = computeTopN(values, TOP_COUNT, true);
        assertThat(
            results.stream()
                .flatMap(
                    page -> IntStream.range(0, page.getPositionCount())
                        .mapToObj(i -> Tuple.tuple(page.<LongBlock>getBlock(0).getLong(i), page.<LongBlock>getBlock(1).getLong(i)))
                )
                .toList(),
            equalTo(expected)
        );
    }

    // FIXME(gal, NOCOMMIT) Ignored because RamUsageTester is not working properly with hashmap and we'll replace these maps anyway.
    public void ignored() {
        RamUsageTester.Accumulator acc = new RamUsageTester.Accumulator() {
            @Override
            public long accumulateObject(Object o, long shallowSize, Map<Field, Object> fieldValues, Collection<Object> queue) {
                if (o instanceof ElementType) {
                    return 0; // shared
                }
                if (o instanceof TopNEncoder) {
                    return 0; // shared
                }
                if (o instanceof CircuitBreaker) {
                    return 0; // shared
                }
                if (o instanceof BlockFactory) {
                    return 0; // shard
                }
                return super.accumulateObject(o, shallowSize, fieldValues, queue);
            }
        };
        int topCount = 10_000;
        // We under-count by a few bytes because of the lists. In that end that's fine, but we need to account for it here.
        long underCount = 200;
        DriverContext context = driverContext();
        try (
            TopNOperator op = new TopNOperator.TopNOperatorFactory(
                topCount,
                List.of(LONG, LONG),
                List.of(DEFAULT_UNSORTABLE, DEFAULT_UNSORTABLE),
                List.of(new SortOrder(0, true, false)),
                List.of(1),
                pageSize
            ).get(context)
        ) {
            long actualEmpty = RamUsageTester.ramUsed(op, acc);
            assertThat(op.ramBytesUsed(), both(greaterThan(actualEmpty - underCount)).and(lessThan(actualEmpty)));
            // But when we fill it then we're quite close
            for (Page p : CannedSourceOperator.collectPages(simpleInput(context.blockFactory(), topCount))) {
                op.addInput(p);
            }
            long actualFull = RamUsageTester.ramUsed(op, acc);
            assertThat(op.ramBytesUsed(), both(greaterThan(actualFull - underCount)).and(lessThan(actualFull)));

            // TODO empty it again and check.
        }
    }

    public void testShardContextManagement_limitEqualToCount_noShardContextIsReleased() {
        topNShardContextManagementAux(2, Stream.generate(() -> true).limit(4).toList());
    }

    public void testShardContextManagement_notAllShardsPassTopN_shardsAreReleased() {
        topNShardContextManagementAux(1, List.of(true, false, false, true));
    }

    private void topNShardContextManagementAux(int limit, List<Boolean> expectedOpenAfterTopN) {
        List<List<?>> values = Arrays.asList(
            Arrays.asList(new BlockUtils.Doc(0, 10, 100), 1L, 1L),
            Arrays.asList(new BlockUtils.Doc(1, 20, 200), 2L, 2L),
            Arrays.asList(new BlockUtils.Doc(2, 30, 300), null, 1L),
            Arrays.asList(new BlockUtils.Doc(3, 40, 400), -3L, 2L)
        );

        List<RefCounted> refCountedList = Stream.<RefCounted>generate(() -> new SimpleRefCounted()).limit(4).toList();
        var shardRefCounters = new IndexedByShardIdFromList<>(refCountedList);
        var pages = topNMultipleColumns(
            driverContext(),
            new ListRowsBlockSourceOperator(driverContext().blockFactory(), List.of(DOC, LONG, LONG), values) {
                @Override
                protected TestBlockBuilder getTestBlockBuilder(int b) {
                    return b == 0 ? new TestBlockBuilder.DocBlockBuilder(blockFactory, shardRefCounters) : super.getTestBlockBuilder(b);
                }
            },
            limit,
            List.of(new DocVectorEncoder(shardRefCounters), DEFAULT_UNSORTABLE, DEFAULT_UNSORTABLE),
            List.of(new SortOrder(1, true, false)),
            List.of(2)
        );
        try {
            refCountedList.forEach(RefCounted::decRef);

            assertThat(refCountedList.stream().map(RefCounted::hasReferences).toList(), equalTo(expectedOpenAfterTopN));
            assertThat(pageToValues(pages), equalTo(computeTopN(values, 2, 1, limit, true)));

            for (var rc : refCountedList) {
                assertFalse(rc.hasReferences());
            }
        } finally {
            Releasables.close(pages);
        }
    }

    public void testRandomMultipleColumns() {
        DriverContext driverContext = driverContext();
        int rows = randomIntBetween(50, 100);
        int topCount = randomIntBetween(1, 10);
        int blocksCount = randomIntBetween(10, 20);
        int sortingByColumns = randomIntBetween(2, 3);
        int groupKeysCount = randomIntBetween(2, 3);

        RandomBlocksResult randomBlocksResult = generateRandomSingleValueBlocks(rows, blocksCount, driverContext);

        List<Integer> sortColumns = new ArrayList<>();
        for (int i = 0; i < sortingByColumns; i++) {
            sortColumns.add(
                randomValueOtherThanMany(
                    c -> randomBlocksResult.validSortKeys[c] == false || sortColumns.contains(c),
                    () -> randomIntBetween(0, blocksCount - 1)
                )
            );
        }

        List<Integer> groupKeys = new ArrayList<>();
        for (int i = 0; i < groupKeysCount; i++) {
            groupKeys.add(
                randomValueOtherThanMany(
                    c -> sortColumns.contains(c) || groupKeys.contains(c) || false == randomBlocksResult.validSortKeys[c],
                    () -> randomIntBetween(0, blocksCount - 1)
                )
            );
        }

        List<SortOrder> uniqueOrders = sortColumns.stream().map(column -> new SortOrder(column, randomBoolean(), randomBoolean())).toList();

        List<Page> results = drive(
            new TopNOperator(
                driverContext.blockFactory(),
                nonBreakingBigArrays().breakerService().getBreaker("request"),
                topCount,
                randomBlocksResult.elementTypes,
                randomBlocksResult.encoders,
                uniqueOrders.stream().toList(),
                groupKeys,
                rows
            ),
            List.of(new Page(randomBlocksResult.blocks.toArray(Block[]::new))).iterator(),
            driverContext
        );
        List<List<Object>> actualValues = new ArrayList<>();
        for (Page p : results) {
            actualValues.addAll(readAsRowsSingleValue(p));
            p.releaseBlocks();
        }

        List<List<Object>> topNExpectedValues = computeTopN(randomBlocksResult.expectedValues, groupKeys, uniqueOrders, topCount);

        assertThat(actualValues, equalTo(topNExpectedValues));
    }

    private static List<Tuple<Long, Long>> computeTopN(List<Tuple<Long, Long>> inputValues, int limit, boolean ascendingOrder) {
        return computeTopN(inputValues.stream().map(e -> Arrays.asList(e.v1(), e.v2())).toList(), 1, 0, limit, ascendingOrder).stream()
            .map(l -> Tuple.tuple((Long) l.get(0), (Long) l.get(1)))
            .toList();
    }

    private static List<? extends List<?>> computeTopN(
        List<? extends List<?>> inputValues,
        int groupChannel,
        int sortChannel,
        int limit,
        boolean ascendingOrder
    ) {
        List<List<Object>> singleValueInput = new ArrayList<>();
        for (List<?> row : inputValues) {
            List<Object> rowAsObject = row.stream().map(v -> (Object) v).toList();
            singleValueInput.add(rowAsObject);
        }
        List<SortOrder> sortOrders = List.of(new SortOrder(sortChannel, ascendingOrder, false));
        return new GroupedTopNOperatorTests().computeTopN(singleValueInput, List.of(groupChannel), sortOrders, limit);
    }

    private List<List<Object>> computeTopN(
        List<List<Object>> inputValues,
        List<Integer> groupChannels,
        List<SortOrder> sortOrders,
        int limit
    ) {
        Comparator<List<Object>> comparator = (row1, row2) -> {
            for (SortOrder order : sortOrders) {
                Object v1 = row1.get(order.channel());
                Object v2 = row2.get(order.channel());
                boolean firstIsNull = v1 == null;
                boolean secondIsNull = v2 == null;

                if (firstIsNull || secondIsNull) {
                    int nullCompare = Boolean.compare(firstIsNull, secondIsNull) * (order.nullsFirst() ? -1 : 1);
                    if (nullCompare != 0) {
                        return nullCompare;
                    }
                    continue;
                }

                @SuppressWarnings("unchecked")
                int cmp = ((Comparable<Object>) v1).compareTo(v2);
                if (cmp != 0) {
                    return order.asc() ? cmp : -cmp;
                }
            }
            return 0;
        };

        Map<List<Object>, List<List<Object>>> grouped = inputValues.stream()
            .collect(Collectors.groupingBy(row -> groupChannels.stream().map(row::get).toList()));

        List<List<Object>> topNExpectedValues = new ArrayList<>();
        for (List<List<Object>> groupRows : grouped.values()) {
            List<List<Object>> sortedGroup = groupRows.stream().sorted(comparator).limit(limit).toList();
            topNExpectedValues.addAll(sortedGroup);
        }
        topNExpectedValues.sort(comparator);
        return topNExpectedValues;
    }

    private static List<List<?>> pageToValues(List<Page> pages) {
        var result = new ArrayList<List<?>>();
        for (Page page : pages) {
            var blocks = IntStream.range(0, page.getBlockCount()).mapToObj(page::<Block>getBlock).toList();
            result.addAll(
                IntStream.range(0, page.getPositionCount())
                    .mapToObj(position -> blocks.stream().map(block -> getBlockValue(block, position)).toList())
                    .toList()
            );
            page.releaseBlocks();
        }

        return result;
    }

    private static Object getBlockValue(Block block, int position) {
        return block.isNull(position) ? null : switch (block) {
            case LongBlock longBlock -> longBlock.getLong(position);
            case DocBlock docBlock -> {
                var vector = docBlock.asVector();
                yield new BlockUtils.Doc(
                    vector.shards().getInt(position),
                    vector.segments().getInt(position),
                    vector.docs().getInt(position)
                );
            }
            default -> throw new IllegalArgumentException("Unsupported block type: " + block.getClass());
        };
    }
}
