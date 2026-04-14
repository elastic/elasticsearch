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
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.IndexedByShardIdFromList;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.GroupKeyEncoder;
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

import static org.elasticsearch.compute.data.ElementType.DOC;
import static org.elasticsearch.compute.data.ElementType.LONG;
import static org.elasticsearch.compute.operator.topn.TopNEncoder.DEFAULT_UNSORTABLE;
import static org.elasticsearch.compute.test.BlockTestUtils.readInto;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
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

    @Override
    public void testStatus() {
        BlockFactory blockFactory = driverContext().blockFactory();
        try (Operator op = simple(SimpleOptions.DEFAULT).get(driverContext())) {
            Operator.Status status = op.status();
            assertThat(status, instanceOf(GroupedTopNOperatorStatus.class));
            GroupedTopNOperatorStatus groupedStatus = (GroupedTopNOperatorStatus) status;
            assertThat(groupedStatus.occupiedRows(), equalTo(0));
            assertThat(groupedStatus.groupCount(), equalTo(0));
            assertThat(groupedStatus.ramBytesUsed(), greaterThan(0L));
            assertThat(groupedStatus.pagesReceived(), equalTo(0));
            assertThat(groupedStatus.pagesEmitted(), equalTo(0));
            assertThat(groupedStatus.rowsReceived(), equalTo(0L));
            assertThat(groupedStatus.rowsEmitted(), equalTo(0L));

            Page p = new Page(
                blockFactory.newConstantLongBlockWith(1, 10),
                blockFactory.newLongArrayVector(new long[] { 1L, 1L, 1L, 1L, 1L, 2L, 2L, 2L, 2L, 2L }, 10).asBlock()
            );
            op.addInput(p);
            status = op.status();
            groupedStatus = (GroupedTopNOperatorStatus) status;
            assertThat(groupedStatus.receiveNanos(), greaterThan(0L));
            assertThat(groupedStatus.emitNanos(), equalTo(0L));
            assertThat(groupedStatus.occupiedRows(), equalTo(8));
            assertThat(groupedStatus.groupCount(), equalTo(2));
            assertThat(groupedStatus.ramBytesUsed(), greaterThan(0L));
            assertThat(groupedStatus.pagesReceived(), equalTo(1));
            assertThat(groupedStatus.pagesEmitted(), equalTo(0));
            assertThat(groupedStatus.rowsReceived(), equalTo(10L));
            assertThat(groupedStatus.rowsEmitted(), equalTo(0L));
        }
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
     * {@snippet lang="text" :
     * Scenario (SORT ASC, LIMIT 1 BY group):
     * - Page 1: (group=0), (group=1) → both groups populated
     * - Page 2: (group=0), (group=2) → (group=0) rejected from full group 0 → break skips (group=2), which was empty
     * Expected groups: {0, 1, 2}
     * Bug result: {0, 1} (group 2 missing)
     * }
     */
    public void testSortedInputWithMultipleGroups() {
        List<ElementType> elementTypes = List.of(ElementType.INT, ElementType.INT);
        List<TopNEncoder> encoders = List.of(TopNEncoder.DEFAULT_SORTABLE, DEFAULT_UNSORTABLE);
        List<SortOrder> sortOrders = List.of(new SortOrder(0, true, true));

        BlockFactory bf = driverContext().blockFactory();
        Page page1 = new Page(BlockUtils.fromList(bf, List.of(List.of(1, 0), List.of(3, 1))));
        Page page2 = new Page(BlockUtils.fromList(bf, List.of(List.of(2, 0), List.of(4, 2))));

        List<List<Object>> actual = runGroupedTopN(List.of(page1, page2), 1, elementTypes, encoders, sortOrders, new int[] { 1 });

        // 3 groups, each with 1 value, ordered ASC: [1, 3, 4]
        assertThat(actual.get(0), equalTo(List.of(1, 3, 4)));
        assertThat(actual.get(1), equalTo(List.of(0, 1, 2)));
    }

    public void testMultivalueGroupKey() {
        BlockFactory blockFactory = driverContext().blockFactory();
        List<ElementType> elementTypes = List.of(LONG, LONG, LONG);
        List<TopNEncoder> encoders = List.of(TopNEncoder.DEFAULT_SORTABLE, DEFAULT_UNSORTABLE, DEFAULT_UNSORTABLE);
        List<SortOrder> sortOrders = List.of(new SortOrder(0, true, false));

        Page page = new Page(
            BlockUtils.fromList(blockFactory, List.of(List.of(10L, 100L, List.of(1L, 2L)), List.of(20L, 200L, 1L), List.of(30L, 300L, 3L)))
        );

        List<List<Object>> actual = runGroupedTopN(List.of(page), 1, elementTypes, encoders, sortOrders, new int[] { 2 });

        // List semantics: [1,2] is one group, 1 is another, 3 is another. Sorted ASC by sort key.
        assertThat(actual.get(0), equalTo(List.of(10L, 20L, 30L))); // Sort key
        assertThat(actual.get(1), equalTo(List.of(100L, 200L, 300L))); // Value
        assertThat(actual.get(2), equalTo(List.of(List.of(1L, 2L), 1L, 3L))); // Group key (MV preserved)
    }

    public void testMultivalueGroupKeyDuplicateWinner() {
        BlockFactory bf = driverContext().blockFactory();
        List<ElementType> elementTypes = List.of(LONG, LONG, LONG);
        List<TopNEncoder> encoders = List.of(TopNEncoder.DEFAULT_SORTABLE, DEFAULT_UNSORTABLE, DEFAULT_UNSORTABLE);
        List<SortOrder> sortOrders = List.of(new SortOrder(0, true, false));

        Page page = new Page(BlockUtils.fromList(bf, List.of(List.of(5L, 50L, List.of(1L, 2L)), List.of(10L, 100L, 1L))));

        List<List<Object>> actual = runGroupedTopN(List.of(page), 1, elementTypes, encoders, sortOrders, new int[] { 2 });

        // List semantics: [1,2] is one group, 1 is another. Sorted ASC by sort key.
        assertThat(actual.get(0), equalTo(List.of(5L, 10L))); // Sort key
        assertThat(actual.get(1), equalTo(List.of(50L, 100L))); // Value
        assertThat(actual.get(2), equalTo(List.of(List.of(1L, 2L), 1L))); // Group key (MV preserved)
    }

    /**
     * Tests list semantics with two multivalue group keys: a row with group_key1=[1, 2]
     * and group_key2=[10, 20] belongs to exactly one group keyed by ([1,2], [10,20]).
     * No cartesian product expansion occurs.
     */
    public void testMultipleMultivalueGroupKeys() {
        BlockFactory bf = driverContext().blockFactory();
        List<ElementType> elementTypes = List.of(LONG, LONG, LONG, LONG);
        List<TopNEncoder> encoders = List.of(TopNEncoder.DEFAULT_SORTABLE, DEFAULT_UNSORTABLE, DEFAULT_UNSORTABLE, DEFAULT_UNSORTABLE);
        List<SortOrder> sortOrders = List.of(new SortOrder(0, true, false));

        Page page = new Page(
            BlockUtils.fromList(
                bf,
                List.of(List.of(10L, 100L, List.of(1L, 2L), List.of(10L, 20L)), List.of(5L, 50L, 1L, 10L), List.of(15L, 150L, 2L, 20L))
            )
        );

        List<List<Object>> actual = runGroupedTopN(List.of(page), 1, elementTypes, encoders, sortOrders, new int[] { 2, 3 });

        // List semantics: 3 distinct groups, each with 1 row, sorted ASC by sort key
        assertThat(actual.get(0), equalTo(List.of(5L, 10L, 15L))); // Sort key
        assertThat(actual.get(1), equalTo(List.of(50L, 100L, 150L))); // Value
        assertThat(actual.get(2), equalTo(List.of(1L, List.of(1L, 2L), 2L))); // Group key 1 (MV preserved)
        assertThat(actual.get(3), equalTo(List.of(10L, List.of(10L, 20L), 20L))); // Group key 2 (MV preserved)
    }

    public void testShardContextManagement_limitEqualToCount_noShardContextIsReleased() {
        topNShardContextManagementAux(2, List.of(true, true, true, true));
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

        List<RefCounted> refCountedList = List.of(
            new SimpleRefCounted(),
            new SimpleRefCounted(),
            new SimpleRefCounted(),
            new SimpleRefCounted()
        );
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
            List<List<Object>> valuesAsObjects = values.stream().map(row -> row.stream().map(v -> (Object) v).toList()).toList();
            List<List<Object>> actual = new ArrayList<>();
            for (Page p : pages) {
                actual.addAll(readAsRowsSingleValue(p));
            }
            assertThat(actual, equalTo(computeTopN(valuesAsObjects, List.of(2), List.of(new SortOrder(1, true, false)), limit)));
        } finally {
            Releasables.close(pages);
        }

        for (var rc : refCountedList) {
            assertFalse(rc.hasReferences());
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
                    c -> sortColumns.contains(c) || groupKeys.contains(c) || randomBlocksResult.validSortKeys[c] == false,
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
                    uniqueOrders,
                    new GroupKeyEncoder(
                        groupKeys.stream().mapToInt(Integer::intValue).toArray(),
                        randomBlocksResult.elementTypes,
                        new BreakingBytesRefBuilder(nonBreakingBigArrays().breakerService().getBreaker("request"), "group-key-encoder")
                    ),
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

    private List<List<Object>> runGroupedTopN(
        List<Page> pages,
        int topCount,
        List<ElementType> elementTypes,
        List<TopNEncoder> encoders,
        List<SortOrder> sortOrders,
        int[] groupKeys
    ) {
        DriverContext driverContext = driverContext();
        List<List<Object>> actual = new ArrayList<>();
        try (
            Driver driver = TestDriverFactory.create(
                driverContext,
                new CannedSourceOperator(pages.iterator()),
                List.of(
                    new GroupedTopNOperator(
                        driverContext.blockFactory(),
                        nonBreakingBigArrays().breakerService().getBreaker("request"),
                        topCount,
                        elementTypes,
                        encoders,
                        sortOrders,
                        new GroupKeyEncoder(
                            groupKeys,
                            elementTypes,
                            new BreakingBytesRefBuilder(nonBreakingBigArrays().breakerService().getBreaker("request"), "group-key-encoder")
                        ),
                        randomPageSize(),
                        Long.MAX_VALUE
                    )
                ),
                new PageConsumerOperator(p -> readInto(actual, p))
            )
        ) {
            new TestDriverRunner().run(driver);
        }
        return actual;
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

    private static boolean isSorted(List<List<Object>> values, Comparator<List<Object>> comparator) {
        return IntStream.range(1, values.size()).allMatch(i -> comparator.compare(values.get(i - 1), values.get(i)) <= 0);
    }

    private static Comparator<Object> compareValues(SortOrder order) {
        Comparator<Object> baseComparator = order.asc() ? CASTING_COMPARATOR : CASTING_COMPARATOR.reversed();
        return order.nullsFirst() ? Comparator.nullsFirst(baseComparator) : Comparator.nullsLast(baseComparator);
    }

    @SuppressWarnings("unchecked")
    private static final Comparator<Object> CASTING_COMPARATOR = (o1, o2) -> ((Comparable<Object>) o1).compareTo(o2);

    private List<Tuple<Long, Long>> computeTopN(List<Tuple<Long, Long>> inputValues, int limit, boolean ascendingOrder) {
        List<List<Object>> rows = inputValues.stream().map(e -> Arrays.<Object>asList(e.v1(), e.v2())).toList();
        return computeTopN(rows, List.of(1), List.of(new SortOrder(0, ascendingOrder, false)), limit).stream()
            .map(l -> Tuple.tuple((Long) l.get(0), (Long) l.get(1)))
            .toList();
    }

    private List<List<Object>> computeTopN(
        List<List<Object>> inputValues,
        List<Integer> groupChannels,
        List<SortOrder> sortOrders,
        int limit
    ) {
        Comparator<List<Object>> comparator = comparatorFromSortOrders(sortOrders);

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

}
