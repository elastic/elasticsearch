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
import java.util.function.Function;
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
            List.of(new TopNOperator.SortOrder(0, asc, false)),
            List.of(1)
        );

        assertThat(outputValues, equalTo(expectedValues));
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
            List.of(new TopNOperator.SortOrder(0, true, false)),
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
                List.of(new TopNOperator.SortOrder(0, true, false)),
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
            List.of(new TopNOperator.SortOrder(1, true, false)),
            List.of(2)
        );
        refCountedList.forEach(RefCounted::decRef);

        assertThat(refCountedList.stream().map(RefCounted::hasReferences).toList(), equalTo(expectedOpenAfterTopN));

        var expectedValues = computeTopN(values, 2, 1, limit, true);
        assertThat(pageToValues(pages), equalTo(expectedValues));
        Releasables.close(pages);

        for (var rc : refCountedList) {
            assertFalse(rc.hasReferences());
        }
    }

    private static List<Tuple<Long, Long>> computeTopN(List<Tuple<Long, Long>> inputValues, int limit, boolean ascendingOrder) {
        return computeTopN(inputValues.stream().map(e -> Arrays.asList(e.v1(), e.v2())).toList(), 1, 0, limit, ascendingOrder).stream()
            .map(l -> Tuple.tuple((Long) l.get(0), (Long) l.get(1)))
            .toList();
    }

    @SuppressWarnings("unchecked")
    private static List<List<?>> computeTopN(
        List<? extends List<?>> inputValues,
        int groupChannel,
        int sortChannel,
        int limit,
        boolean ascendingOrder
    ) {
        Comparator<Long> longComparator = ascendingOrder ? Comparator.nullsLast(Comparator.naturalOrder()) : Comparator.reverseOrder();
        Comparator<List<?>> listComparator = Comparator.comparing(e -> (Long) e.get(sortChannel), longComparator);
        Map<Long, List<? extends List<?>>> map = inputValues.stream()
            .collect(
                Collectors.groupingBy(
                    l -> (Long) l.get(groupChannel),
                    Collectors.mapping(
                        Function.identity(),
                        Collectors.collectingAndThen(
                            Collectors.toList(),
                            list -> list.stream().sorted(listComparator).limit(limit).toList()
                        )
                    )
                )
            );
        return (List<List<?>>) map.values().stream().flatMap(Collection::stream).sorted(listComparator).toList();
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
