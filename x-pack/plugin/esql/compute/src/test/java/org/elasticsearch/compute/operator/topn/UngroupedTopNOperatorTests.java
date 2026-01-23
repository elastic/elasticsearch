/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.tests.util.RamUsageTester;
import org.apache.lucene.util.BytesRef;
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
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.operator.TupleDocLongBlockSourceOperator;
import org.elasticsearch.compute.test.CannedSourceOperator;
import org.elasticsearch.compute.test.SequenceLongBlockSourceOperator;
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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static java.util.Comparator.naturalOrder;
import static java.util.Comparator.reverseOrder;
import static org.elasticsearch.compute.data.ElementType.AGGREGATE_METRIC_DOUBLE;
import static org.elasticsearch.compute.data.ElementType.BYTES_REF;
import static org.elasticsearch.compute.data.ElementType.COMPOSITE;
import static org.elasticsearch.compute.data.ElementType.EXPONENTIAL_HISTOGRAM;
import static org.elasticsearch.compute.data.ElementType.LONG;
import static org.elasticsearch.compute.data.ElementType.LONG_RANGE;
import static org.elasticsearch.compute.data.ElementType.TDIGEST;
import static org.elasticsearch.compute.operator.topn.TopNEncoder.DEFAULT_SORTABLE;
import static org.elasticsearch.compute.operator.topn.TopNEncoder.DEFAULT_UNSORTABLE;
import static org.elasticsearch.compute.operator.topn.TopNEncoder.UTF8;
import static org.elasticsearch.compute.operator.topn.TopNEncoderTests.randomPointAsWKB;
import static org.elasticsearch.compute.test.BlockTestUtils.append;
import static org.elasticsearch.compute.test.BlockTestUtils.randomValue;
import static org.elasticsearch.core.Tuple.tuple;
import static org.elasticsearch.test.ListMatcher.matchesList;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.number.OrderingComparison.lessThanOrEqualTo;

public class UngroupedTopNOperatorTests extends TopNOperatorTests {
    @Override
    protected List<Integer> groupKeys() {
        return List.of();
    }

    @Override
    protected void testRandomTopN(boolean asc, DriverContext context) {
        int limit = randomIntBetween(1, 20);
        List<Long> inputValues = randomList(0, 5000, ESTestCase::randomLong);
        Comparator<Long> comparator = asc ? naturalOrder() : reverseOrder();
        List<Long> expectedValues = inputValues.stream().sorted(comparator).limit(limit).toList();
        List<Long> outputValues = topNLong(context, inputValues, limit, asc, false);
        assertThat(outputValues, equalTo(expectedValues));
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
    protected TopNOperator.TopNOperatorFactory simple(SimpleOptions options) {
        return new TopNOperator.TopNOperatorFactory(
            4,
            List.of(LONG),
            List.of(DEFAULT_UNSORTABLE),
            List.of(new TopNOperator.SortOrder(0, true, false)),
            groupKeys(),
            pageSize
        );
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return equalTo(
            "TopNOperator[count=0/4, elementTypes=[LONG], encoders=[DefaultUnsortable], "
                + "sortOrders=[SortOrder[channel=0, asc=true, nullsFirst=false]]]"
        );
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return equalTo(
            "TopNOperator[count=4, elementTypes=[LONG], encoders=[DefaultUnsortable], "
                + "sortOrders=[SortOrder[channel=0, asc=true, nullsFirst=false]]]"
        );
    }

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        return new SequenceLongBlockSourceOperator(
            blockFactory,
            LongStream.range(0, size).map(l -> ESTestCase.randomLong()),
            between(1, size * 2)
        );
    }

    public void testRamBytesUsed() {
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
                List.of(LONG),
                List.of(DEFAULT_UNSORTABLE),
                List.of(new TopNOperator.SortOrder(0, true, false)),
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
        topNShardContextManagementAux(4, Stream.generate(() -> true).limit(4).toList());
    }

    public void testShardContextManagement_notAllShardsPassTopN_shardsAreReleased() {
        topNShardContextManagementAux(2, List.of(true, false, false, true));
    }

    private void topNShardContextManagementAux(int limit, List<Boolean> expectedOpenAfterTopN) {
        List<Tuple<BlockUtils.Doc, Long>> values = Arrays.asList(
            tuple(new BlockUtils.Doc(0, 10, 100), 1L),
            tuple(new BlockUtils.Doc(1, 20, 200), 2L),
            tuple(new BlockUtils.Doc(2, 30, 300), null),
            tuple(new BlockUtils.Doc(3, 40, 400), -3L)
        );

        List<RefCounted> refCountedList = Stream.<RefCounted>generate(() -> new SimpleRefCounted()).limit(4).toList();
        var shardRefCounters = new IndexedByShardIdFromList<>(refCountedList);
        var pages = topNMultipleColumns(driverContext(), new TupleDocLongBlockSourceOperator(driverContext().blockFactory(), values) {
            @Override
            protected Block.Builder firstElementBlockBuilder(int length) {
                return DocBlock.newBlockBuilder(blockFactory, length).shardRefCounters(shardRefCounters);
            }
        },
            limit,
            List.of(new DocVectorEncoder(shardRefCounters), DEFAULT_UNSORTABLE),
            List.of(new TopNOperator.SortOrder(1, true, false)),
            groupKeys()
        );
        refCountedList.forEach(RefCounted::decRef);

        assertThat(refCountedList.stream().map(RefCounted::hasReferences).toList(), equalTo(expectedOpenAfterTopN));

        var expectedValues = values.stream()
            .sorted(Comparator.comparingLong(t -> t.v2() == null ? Long.MAX_VALUE : t.v2()))
            .limit(limit)
            .toList();
        assertThat(
            pageToTuples((b, i) -> (BlockUtils.Doc) BlockUtils.toJavaObject(b, i), (b, i) -> ((LongBlock) b).getLong(i), pages),
            equalTo(expectedValues)
        );
        Releasables.close(pages);

        for (var rc : refCountedList) {
            assertFalse(rc.hasReferences());
        }
    }

    public void testRandomMultiValuesTopN() {
        DriverContext driverContext = driverContext();
        int rows = randomIntBetween(50, 100);
        int topCount = randomIntBetween(1, rows);
        int blocksCount = randomIntBetween(20, 30);
        int sortingByColumns = randomIntBetween(1, 10);

        RandomMultiValueBlocksResult blocksResult = generateRandomMultiValueBlocks(rows, blocksCount, driverContext);
        Set<TopNOperator.SortOrder> uniqueOrders = generateSortOrders(
            sortingByColumns,
            blocksCount,
            blocksResult.validSortKeys,
            c -> false
        );

        List<List<List<Object>>> actualValues = new ArrayList<>();
        List<Page> results = drive(
            new TopNOperator(
                driverContext.blockFactory(),
                nonBreakingBigArrays().breakerService().getBreaker("request"),
                topCount,
                blocksResult.elementTypes,
                blocksResult.encoders,
                uniqueOrders.stream().toList(),
                groupKeys(),
                rows
            ),
            List.of(new Page(blocksResult.blocks.toArray(Block[]::new))).iterator(),
            driverContext
        );
        for (Page p : results) {
            readAsRows(actualValues, p);
            p.releaseBlocks();
        }

        List<List<List<Object>>> topNExpectedValues = blocksResult.expectedValues.stream()
            .sorted(new NaiveTopNComparator(uniqueOrders))
            .limit(topCount)
            .toList();
        List<List<Object>> actualReducedValues = extractAndReduceSortedValues(actualValues, uniqueOrders);
        List<List<Object>> expectedReducedValues = extractAndReduceSortedValues(topNExpectedValues, uniqueOrders);

        assertMap(actualReducedValues, matchesList(expectedReducedValues));
    }
}
