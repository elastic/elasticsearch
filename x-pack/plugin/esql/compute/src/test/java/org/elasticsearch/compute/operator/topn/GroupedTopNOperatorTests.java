/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.IndexedByShardIdFromList;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.PageConsumerOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.operator.topn.TopNOperator.SortOrder;
import org.elasticsearch.compute.test.CannedSourceOperator;
import org.elasticsearch.compute.test.TestBlockBuilder;
import org.elasticsearch.compute.test.TestDriverFactory;
import org.elasticsearch.compute.test.TestDriverRunner;
import org.elasticsearch.compute.test.operator.blocksource.ListRowsBlockSourceOperator;
import org.elasticsearch.compute.test.operator.blocksource.TupleLongLongBlockSourceOperator;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.SimpleRefCounted;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.elasticsearch.compute.data.ElementType.DOC;
import static org.elasticsearch.compute.data.ElementType.LONG;
import static org.elasticsearch.compute.operator.topn.TopNEncoder.DEFAULT_UNSORTABLE;
import static org.elasticsearch.compute.test.BlockTestUtils.append;
import static org.elasticsearch.compute.test.BlockTestUtils.readInto;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class GroupedTopNOperatorTests extends TopNOperatorTests {
    private static final int TOP_COUNT = 4;

    @Override
    protected int[] groupKeys() {
        return new int[] { 0 };
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
            new int[] { 1 }
        );

        assertThat(outputValues, equalTo(expectedValues));
    }

    @Override
    protected List<List<Object>> expectedTopRowOriented(List<List<Object>> rowOriented, List<SortOrder> sortOrders, int topCount) {
        return computeTopN(rowOriented, IntStream.of(groupKeys()).boxed().toList(), sortOrders, topCount);
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
    protected Operator.OperatorFactory simple(SimpleOptions options) {
        return new GroupedTopNOperator.GroupedTopNOperatorFactory(
            TOP_COUNT,
            List.of(LONG, LONG),
            List.of(DEFAULT_UNSORTABLE, DEFAULT_UNSORTABLE),
            List.of(new SortOrder(0, true, false)),
            List.of(1),
            pageSize,
            Long.MAX_VALUE
        );
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return equalTo(
            "GroupedTopNOperator[count=4, elementTypes=[LONG, LONG], encoders=[DefaultUnsortable, DefaultUnsortable], "
                + "sortOrders=[SortOrder[channel=0, asc=true, nullsFirst=false]], groupKeys=[1]]"
        );
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return equalTo(
            "GroupedTopNOperator[count=0/0/4, elementTypes=[LONG, LONG], encoders=[DefaultUnsortable, DefaultUnsortable], "
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

    /**
     * Tests that the SORTED input ordering optimization short-circuiting addInput() doesn't incorrectly skip rows
     * belonging to groups not yet populated when another group's row is rejected.
     *
     * <pre>{@code
     * Scenario (SORT ASC, LIMIT 1 BY group):
     * - Page 1: (group=0), (group=1) → both groups populated
     * - Page 2: (group=0), (group=2) → (group=0) rejected from full group 0 → break skips (group=2), which was empty
     * Expected groups: {0, 1, 2}
     * Bug result: {0, 1} (group 2 missing)
     * }</pre>
     */
    public void testSortedInputWithMultipleGroups() {
        int topCount = 1;
        int[] groupKeys = new int[] { 1 };
        List<ElementType> elementTypes = List.of(ElementType.INT, ElementType.INT);
        List<TopNEncoder> encoders = List.of(TopNEncoder.DEFAULT_SORTABLE, DEFAULT_UNSORTABLE);
        List<SortOrder> sortOrders = List.of(new SortOrder(0, true, true));

        BlockFactory bf = driverContext().blockFactory();

        // Page 1: sorted ASC by sort key
        Page page1;
        try (
            Block.Builder sortCol = ElementType.INT.newBlockBuilder(2, bf);
            Block.Builder groupCol = ElementType.INT.newBlockBuilder(2, bf)
        ) {
            append(sortCol, 1);
            append(sortCol, 3);
            append(groupCol, 0);
            append(groupCol, 1);
            page1 = new Page(sortCol.build(), groupCol.build());
        }

        // Page 2: sorted ASC by sort key
        Page page2;
        try (
            Block.Builder sortCol = ElementType.INT.newBlockBuilder(2, bf);
            Block.Builder groupCol = ElementType.INT.newBlockBuilder(2, bf)
        ) {
            append(sortCol, 2);
            append(sortCol, 4);
            append(groupCol, 0);
            append(groupCol, 2);
            page2 = new Page(sortCol.build(), groupCol.build());
        }

        List<List<Object>> actual = new ArrayList<>();
        DriverContext driverContext = driverContext();

        try (
            Driver driver = TestDriverFactory.create(
                driverContext,
                new CannedSourceOperator(List.of(page1, page2).iterator()),
                List.of(
                    new GroupedTopNOperator(
                        driverContext.blockFactory(),
                        nonBreakingBigArrays().breakerService().getBreaker("request"),
                        topCount,
                        elementTypes,
                        encoders,
                        sortOrders,
                        groupKeys,
                        randomPageSize(),
                        Long.MAX_VALUE
                    )
                ),
                new PageConsumerOperator(p -> readInto(actual, p))
            )
        ) {
            new TestDriverRunner().run(driver);
        }

        // 3 groups, each with 1 value, ordered ASC: [1, 3, 4]
        assertThat(actual.get(0), equalTo(List.of(1, 3, 4)));
        assertThat(actual.get(1), equalTo(List.of(0, 1, 2)));
    }

    public void testMultivalueGroupKey() {
        DriverContext driverContext = driverContext();
        BlockFactory blockFactory = driverContext.blockFactory();

        int topCount = 1;
        int[] groupKeys = new int[] { 2 }; // group key at channel 2
        List<ElementType> elementTypes = List.of(LONG, LONG, LONG);
        List<TopNEncoder> encoders = List.of(TopNEncoder.DEFAULT_SORTABLE, DEFAULT_UNSORTABLE, DEFAULT_UNSORTABLE);
        List<SortOrder> sortOrders = List.of(new SortOrder(0, true, false));

        Page page = new Page(
            BlockUtils.fromList(
                blockFactory,
                List.of(
                    // (To keep indentation)
                    List.of(10L, 100L, List.of(1L, 2L)),
                    List.of(20L, 200L, 1L),
                    List.of(30L, 300L, 3L)
                )
            )
        );

        List<List<Object>> actual = new ArrayList<>();
        try (
            Driver driver = TestDriverFactory.create(
                driverContext,
                new CannedSourceOperator(List.of(page).iterator()),
                List.of(
                    new GroupedTopNOperator(
                        blockFactory,
                        nonBreakingBigArrays().breakerService().getBreaker("request"),
                        topCount,
                        elementTypes,
                        encoders,
                        sortOrders,
                        groupKeys,
                        randomPageSize(),
                        Long.MAX_VALUE
                    )
                ),
                new PageConsumerOperator(p -> readInto(actual, p))
            )
        ) {
            new TestDriverRunner().run(driver);
        }

        assertThat(actual.get(0), equalTo(List.of(10L, 10L, 30L))); // Sort key
        assertThat(actual.get(1), equalTo(List.of(100L, 100L, 300L))); // Value
        assertThat(actual.get(2), equalTo(List.of(1L, 2L, 3L))); // Group key
    }

    public void testMultivalueGroupKeyDuplicateWinner() {
        DriverContext driverContext = driverContext();
        BlockFactory bf = driverContext.blockFactory();

        int topCount = 1;
        int[] groupKeys = new int[] { 2 };
        List<ElementType> elementTypes = List.of(LONG, LONG, LONG);
        List<TopNEncoder> encoders = List.of(TopNEncoder.DEFAULT_SORTABLE, DEFAULT_UNSORTABLE, DEFAULT_UNSORTABLE);
        List<SortOrder> sortOrders = List.of(new SortOrder(0, true, false));

        Page page = new Page(BlockUtils.fromList(bf, List.of(List.of(5L, 50L, List.of(1L, 2L)), List.of(10L, 100L, 1L))));

        List<List<Object>> actual = new ArrayList<>();
        try (
            Driver driver = TestDriverFactory.create(
                driverContext,
                new CannedSourceOperator(List.of(page).iterator()),
                List.of(
                    new GroupedTopNOperator(
                        bf,
                        nonBreakingBigArrays().breakerService().getBreaker("request"),
                        topCount,
                        elementTypes,
                        encoders,
                        sortOrders,
                        groupKeys,
                        randomPageSize(),
                        Long.MAX_VALUE
                    )
                ),
                new PageConsumerOperator(p -> readInto(actual, p))
            )
        ) {
            new TestDriverRunner().run(driver);
        }

        assertThat(actual.get(0), equalTo(List.of(5L, 5L))); // Sort key
        assertThat(actual.get(1), equalTo(List.of(50L, 50L))); // Value
        assertThat(actual.get(2), equalTo(List.of(1L, 2L))); // Group key
    }

    /**
     * Tests cartesian product expansion with two multivalue group keys:
     * a row with group_key1=[1, 2] and group_key2=[10, 20] belongs to groups
     * (1,10), (1,20), (2,10), (2,20), the same way as STATS BY handles
     * multiple multivalue group keys.
     *
     * <p>Without MV fanning, row 0 would only be in one group (the first combination),
     * so groups (1,20), (2,10), and (2,20) would be empty → output would have fewer rows.
     */
    public void testMultipleMultivalueGroupKeys() {
        DriverContext driverContext = driverContext();
        BlockFactory bf = driverContext.blockFactory();

        int topCount = 1;
        int[] groupKeys = new int[] { 2, 3 }; // two group keys at channels 2 and 3
        List<ElementType> elementTypes = List.of(LONG, LONG, LONG, LONG);
        List<TopNEncoder> encoders = List.of(TopNEncoder.DEFAULT_SORTABLE, DEFAULT_UNSORTABLE, DEFAULT_UNSORTABLE, DEFAULT_UNSORTABLE);
        List<SortOrder> sortOrders = List.of(new SortOrder(0, true, false));

        Page page = new Page(
            BlockUtils.fromList(
                bf,
                List.of(List.of(10L, 100L, List.of(1L, 2L), List.of(10L, 20L)), List.of(5L, 50L, 1L, 10L), List.of(15L, 150L, 2L, 20L))
            )
        );

        List<List<Object>> actual = new ArrayList<>();
        try (
            Driver driver = TestDriverFactory.create(
                driverContext,
                new CannedSourceOperator(List.of(page).iterator()),
                List.of(
                    new GroupedTopNOperator(
                        bf,
                        nonBreakingBigArrays().breakerService().getBreaker("request"),
                        topCount,
                        elementTypes,
                        encoders,
                        sortOrders,
                        groupKeys,
                        randomPageSize(),
                        Long.MAX_VALUE
                    )
                ),
                new PageConsumerOperator(p -> readInto(actual, p))
            )
        ) {
            new TestDriverRunner().run(driver);
        }

        assertThat(actual.get(0), equalTo(List.of(5L, 10L, 10L, 10L))); // Sort key
        assertThat(actual.get(1), equalTo(List.of(50L, 100L, 100L, 100L))); // Value
        assertThat(actual.get(2), equalTo(List.of(1L, 1L, 2L, 2L))); // Group key 1
        assertThat(actual.get(3), equalTo(List.of(10L, 20L, 10L, 20L))); // Group key 2
    }

    /**
     * Verifies that a single page position whose group-ID count (from multivalued group key expansion)
     * exceeds {@link GroupedTopNOperator}'s internal emit batch size (10K) is handled correctly.
     * <p>
     *     BlockHash's AddPage invokes the callback multiple times for the same position in this case.
     *     Each {@code (position, groupId)} pair is processed independently in the callback, so
     *     every combination is counted exactly once.
     *     This test uses one row with two multivalued group keys of 150 values each (150×150 = 22_500 > 10_240).
     * </p>
     */
    public void testSinglePositionExceedsEmitBatchSize() {
        int elementsPerKey = 150;
        assertThat(
            "This test only makes sense if the total groups is bigger than the emit batch size",
            elementsPerKey * elementsPerKey,
            greaterThan(GroupedTopNOperator.EMIT_BATCH_SIZE)
        );

        DriverContext driverContext = driverContext();
        BlockFactory bf = driverContext.blockFactory();

        int topCount = 1;
        int[] groupKeys = new int[] { 2, 3 };
        List<ElementType> elementTypes = List.of(LONG, LONG, LONG, LONG);
        List<TopNEncoder> encoders = List.of(TopNEncoder.DEFAULT_SORTABLE, DEFAULT_UNSORTABLE, DEFAULT_UNSORTABLE, DEFAULT_UNSORTABLE);
        List<SortOrder> sortOrders = List.of(new SortOrder(0, true, false));

        List<Long> keys1 = LongStream.range(0, 150).boxed().toList();
        List<Long> keys2 = LongStream.range(0, 150).boxed().toList();
        Page page = new Page(BlockUtils.fromList(bf, List.of(List.of(1L, 1L, keys1, keys2))));

        List<List<Object>> actual = new ArrayList<>();
        try (
            Driver driver = TestDriverFactory.create(
                driverContext,
                new CannedSourceOperator(List.of(page).iterator()),
                List.of(
                    new GroupedTopNOperator(
                        bf,
                        nonBreakingBigArrays().breakerService().getBreaker("request"),
                        topCount,
                        elementTypes,
                        encoders,
                        sortOrders,
                        groupKeys,
                        randomPageSize(),
                        Long.MAX_VALUE
                    )
                ),
                new PageConsumerOperator(p -> readInto(actual, p))
            )
        ) {
            new TestDriverRunner().run(driver);
        }

        int expectedRows = keys1.size() * keys2.size();
        assertThat(actual.getFirst().size(), equalTo(expectedRows));
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
            new int[] { 2 }
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
                    c -> sortColumns.contains(c) || groupKeys.contains(c) || randomBlocksResult.validSortKeys[c] == false
                    // BlockHash does not support FLOAT as a group key type.
                        || randomBlocksResult.elementTypes.get(c) == ElementType.FLOAT,
                    () -> randomIntBetween(0, blocksCount - 1)
                )
            );
        }

        List<SortOrder> uniqueOrders = sortColumns.stream().map(column -> new SortOrder(column, randomBoolean(), randomBoolean())).toList();

        List<Page> results = new TestDriverRunner().builder(driverContext)
            .input(List.of(new Page(randomBlocksResult.blocks.toArray(Block[]::new))).iterator())
            .run(
                new GroupedTopNOperator(
                    driverContext.blockFactory(),
                    nonBreakingBigArrays().breakerService().getBreaker("request"),
                    topCount,
                    randomBlocksResult.elementTypes,
                    randomBlocksResult.encoders,
                    uniqueOrders.stream().toList(),
                    groupKeys.stream().mapToInt(Integer::intValue).toArray(),
                    rows,
                    Long.MAX_VALUE
                )
            );
        List<List<Object>> actualValues = new ArrayList<>();
        for (Page p : results) {
            actualValues.addAll(readAsRowsSingleValue(p));
            p.releaseBlocks();
        }

        List<List<Object>> topNExpectedValues = computeTopN(randomBlocksResult.expectedValues, groupKeys, uniqueOrders, topCount);

        // We verify the output we got from the operator is sorted, but since we're asserting the results, we also need to handle ties which
        // the operator doesn't actually guarantee any order for.
        Comparator<List<Object>> sortOrderComparator = comparatorFromSortOrders(uniqueOrders);
        assertThat(isSorted(actualValues, sortOrderComparator), equalTo(true));

        // Given that sorting on repeated sort keys (Specially booleans, nulls...) may include arbitrary rows,
        // we'll only assert the expected groups and keys, by including them in this signature
        Function<List<Object>, List<Object>> signature = row -> {
            List<Object> sig = new ArrayList<>();
            for (int gk : groupKeys.stream().mapToInt(Integer::intValue).toArray()) {
                sig.add(row.get(gk));
            }
            for (SortOrder so : uniqueOrders) {
                sig.add(row.get(so.channel()));
            }
            return sig;
        };

        Map<List<Object>, Long> actualCounts = actualValues.stream()
            .map(signature)
            .collect(Collectors.groupingBy(s -> s, Collectors.counting()));
        Map<List<Object>, Long> expectedCounts = topNExpectedValues.stream()
            .map(signature)
            .collect(Collectors.groupingBy(s -> s, Collectors.counting()));

        assertThat(actualCounts, equalTo(expectedCounts));
    }

    private static Comparator<List<Object>> comparatorFromSortOrders(List<SortOrder> sortOrders) {
        return (row1, row2) -> {
            assertEquals(row1.size(), row2.size());
            for (SortOrder order : sortOrders) {
                int cmp = compareValues(order).compare(row1.get(order.channel()), row2.get(order.channel()));
                if (cmp != 0) {
                    return cmp;
                }
            }
            return 0;
        };
    }

    private static final Comparator<List<Object>> TIE_BREAKING_COMPARATOR = (row1, row2) -> {
        for (int i = 0; i < row1.size(); i++) {
            int cmp = compareValues(new SortOrder(i, true, true)).compare(row1.get(i), row2.get(i));
            if (cmp != 0) {
                return cmp;
            }
        }
        return 0;
    };

    private static boolean isSorted(List<List<Object>> values, Comparator<List<Object>> comparator) {
        return IntStream.range(1, values.size()).allMatch(i -> comparator.compare(values.get(i - 1), values.get(i)) <= 0);
    }

    private static Comparator<Object> compareValues(SortOrder order) {
        Comparator<Object> baseComparator = order.asc() ? CASTING_COMPARATOR : CASTING_COMPARATOR.reversed();
        return order.nullsFirst() ? Comparator.nullsFirst(baseComparator) : Comparator.nullsLast(baseComparator);
    }

    @SuppressWarnings("unchecked")
    private static final Comparator<Object> CASTING_COMPARATOR = (o1, o2) -> ((Comparable<Object>) o1).compareTo(o2);

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

                int cmp = CASTING_COMPARATOR.compare(v1, v2);
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
