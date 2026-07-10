/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.test.TestWarningsSource;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 * Exercises {@code GroupingAggregatorFunction.AddInput#addGather}, the indirect-addressing entry
 * point generated for partitioned hash aggregation's bucket-sort routing (see
 * scratch/partitioned-hash-aggregation-design.md, Phase 2 build order step 1). Feeds rows through
 * addGather in shuffled order and checks the per-group result against a hand-computed reference,
 * which only a genuinely correct indirect read (as opposed to, say, silently degrading to a
 * contiguous read) can reproduce.
 * <p>
 *     Evaluation is always restricted to group ids that actually received a real value (i.e. keys
 *     of the hand-computed {@code expected} map), never to the full {@code [0, groupCount)} range:
 *     a group id can be "seen" (tracked in {@link AbstractArrayState}'s bitset) without its backing
 *     array slot ever having been grown by a real {@code set}/{@code increment} call, e.g. when
 *     every row assigned to it happened to be null. That's an existing array-backed-state
 *     precondition, not something addGather changes, so tests here simply avoid it.
 * </p>
 */
public class AddGatherTests extends ESTestCase {

    private BlockFactory blockFactory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        blockFactory = new BlockFactory(new NoopCircuitBreaker("test-noop"), BigArrays.NON_RECYCLING_INSTANCE);
    }

    public void testSumLongVectorValues() {
        int rows = between(1, 2000);
        int groupCount = between(1, 20);
        long[] values = new long[rows];
        int[] rowGroup = new int[rows];
        Map<Integer, Long> expected = new HashMap<>();
        for (int i = 0; i < rows; i++) {
            values[i] = randomLongBetween(-1000, 1000);
            rowGroup[i] = between(0, groupCount - 1);
            expected.merge(rowGroup[i], values[i], Long::sum);
        }
        int[] positions = shuffledPositions(rows);
        int[] groupIds = gatherGroupIds(rowGroup, positions);

        DriverContext ctx = driverContext();
        try (
            IntVector groupIdVector = blockFactory.newIntArrayVector(groupIds, rows);
            IntVector positionVector = blockFactory.newIntArrayVector(positions, rows);
            GroupingAggregatorFunction aggFn = new SumLongAggregatorFunctionSupplier(TestWarningsSource.INSTANCE).groupingAggregator(
                ctx,
                List.of(0)
            )
        ) {
            addLongsViaGather(aggFn, groupCount, blockFactory.newLongArrayVector(values, rows).asBlock(), groupIdVector, positionVector);
            assertLongSums(aggFn, ctx, expected);
        }
    }

    public void testSumLongBlockValuesWithNullsAndMultivalues() {
        int rows = between(1, 500);
        int groupCount = between(1, 20);
        int[] rowGroup = new int[rows];
        Map<Integer, Long> expected = new HashMap<>();

        LongBlock valueBlock;
        try (LongBlock.Builder builder = blockFactory.newLongBlockBuilder(rows)) {
            for (int i = 0; i < rows; i++) {
                rowGroup[i] = between(0, groupCount - 1);
                // Row 0 is always null, guaranteeing the block can never trivially reduce to a
                // vector (which cannot represent nulls), regardless of how the rest are randomized.
                switch (i == 0 ? 0 : between(0, 2)) {
                    case 0 -> builder.appendNull();
                    case 1 -> {
                        long v = randomLongBetween(-1000, 1000);
                        builder.appendLong(v);
                        expected.merge(rowGroup[i], v, Long::sum);
                    }
                    case 2 -> {
                        int valueCount = between(2, 4);
                        builder.beginPositionEntry();
                        for (int j = 0; j < valueCount; j++) {
                            long v = randomLongBetween(-1000, 1000);
                            builder.appendLong(v);
                            expected.merge(rowGroup[i], v, Long::sum);
                        }
                        builder.endPositionEntry();
                    }
                    default -> throw new IllegalStateException("unreachable");
                }
            }
            valueBlock = builder.build();
        }
        // Force a genuine block (not vector-backed) so the block-values addRawInputGatherLoop
        // variant is exercised here, rather than the vector fast path (covered above).
        assertThat(valueBlock.asVector(), nullValue());

        int[] positions = shuffledPositions(rows);
        int[] groupIds = gatherGroupIds(rowGroup, positions);

        DriverContext ctx = driverContext();
        try (
            IntVector groupIdVector = blockFactory.newIntArrayVector(groupIds, rows);
            IntVector positionVector = blockFactory.newIntArrayVector(positions, rows);
            GroupingAggregatorFunction aggFn = new SumLongAggregatorFunctionSupplier(TestWarningsSource.INSTANCE).groupingAggregator(
                ctx,
                List.of(0)
            )
        ) {
            addLongsViaGather(aggFn, groupCount, valueBlock, groupIdVector, positionVector);
            assertLongSums(aggFn, ctx, expected);
        }
    }

    public void testSumLongGatherOverflowMarksOnlyFailedGroupNull() {
        // Group 0 overflows (Long.MAX_VALUE - 1 + 2); groups 1 and 2 don't.
        long[] values = { 1L, 2L, Long.MAX_VALUE - 1, 2L, 4L, 5L };
        int[] rowGroup = { 1, 1, 0, 0, 2, 2 };
        int rows = values.length;

        int[] positions = shuffledPositions(rows);
        int[] groupIds = gatherGroupIds(rowGroup, positions);

        DriverContext ctx = driverContext();
        try (
            IntVector groupIdVector = blockFactory.newIntArrayVector(groupIds, rows);
            IntVector positionVector = blockFactory.newIntArrayVector(positions, rows);
            GroupingAggregatorFunction aggFn = new SumLongAggregatorFunctionSupplier(TestWarningsSource.INSTANCE).groupingAggregator(
                ctx,
                List.of(0)
            )
        ) {
            addLongsViaGather(aggFn, 3, blockFactory.newLongArrayVector(values, rows).asBlock(), groupIdVector, positionVector);

            try (
                IntVector selected = blockFactory.newIntArrayVector(rangeArray(3), 3);
                GroupingAggregatorEvaluationContext evalCtx = new GroupingAggregatorEvaluationContext(ctx)
            ) {
                GroupingAggregatorFunction.PreparedForEvaluation prepared = aggFn.prepareEvaluateFinal(selected, evalCtx);
                Block[] blocks = new Block[1];
                try {
                    prepared.evaluate(blocks, 0, selected);
                    LongBlock result = (LongBlock) blocks[0];
                    assertThat("overflowed group", result.isNull(0), equalTo(true));
                    assertThat("group 1", result.getLong(1), equalTo(3L));
                    assertThat("group 2", result.getLong(2), equalTo(9L));
                } finally {
                    Releasables.closeExpectNoException(blocks);
                    prepared.close();
                }
            }
        }

        // Message text (e.g. Math.addExact's "long overflow") isn't asserted verbatim: it's not
        // what this test is exercising, and has been observed to vary by JDK/build.
        assertWarnings(
            true,
            List.of(containsString("evaluation of [source] failed, treating result as null"), containsString("ArithmeticException"))
        );
    }

    public void testMaxBytesRefVectorValues() {
        int rows = between(1, 500);
        int groupCount = between(1, 20);
        BytesRef[] values = new BytesRef[rows];
        int[] rowGroup = new int[rows];
        Map<Integer, BytesRef> expected = new HashMap<>();
        for (int i = 0; i < rows; i++) {
            values[i] = new BytesRef(randomAlphaOfLengthBetween(0, 20));
            rowGroup[i] = between(0, groupCount - 1);
            expected.merge(rowGroup[i], values[i], (a, b) -> a.compareTo(b) >= 0 ? a : b);
        }
        int[] positions = shuffledPositions(rows);
        int[] groupIds = gatherGroupIds(rowGroup, positions);

        DriverContext ctx = driverContext();
        BytesRefVector.Builder valueBuilder = blockFactory.newBytesRefVectorBuilder(rows);
        for (BytesRef v : values) {
            valueBuilder.appendBytesRef(v);
        }
        // valueBuilder.build()'s vector isn't closed separately here: asBlock() below transfers its
        // single reference to the Page, which owns and releases it.
        BytesRefVector valueVector = valueBuilder.build();
        try (
            IntVector groupIdVector = blockFactory.newIntArrayVector(groupIds, rows);
            IntVector positionVector = blockFactory.newIntArrayVector(positions, rows);
            GroupingAggregatorFunction aggFn = new MaxBytesRefAggregatorFunctionSupplier().groupingAggregator(ctx, List.of(0))
        ) {
            try (Page page = new Page(valueVector.asBlock())) {
                GroupingAggregatorFunction.AddInput addInput = aggFn.prepareProcessRawInputPage(
                    new SeenGroupIds.Range(0, groupCount),
                    page
                );
                try {
                    addInput.addGather(groupIdVector, positionVector);
                } finally {
                    addInput.close();
                }
            }

            int[] selectedGroups = expected.keySet().stream().sorted().mapToInt(Integer::intValue).toArray();
            try (
                IntVector selected = blockFactory.newIntArrayVector(selectedGroups, selectedGroups.length);
                GroupingAggregatorEvaluationContext evalCtx = new GroupingAggregatorEvaluationContext(ctx)
            ) {
                GroupingAggregatorFunction.PreparedForEvaluation prepared = aggFn.prepareEvaluateFinal(selected, evalCtx);
                Block[] blocks = new Block[1];
                try {
                    prepared.evaluate(blocks, 0, selected);
                    BytesRefBlock result = (BytesRefBlock) blocks[0];
                    BytesRef scratch = new BytesRef();
                    for (int i = 0; i < selectedGroups.length; i++) {
                        assertThat("group " + selectedGroups[i], result.getBytesRef(i, scratch), equalTo(expected.get(selectedGroups[i])));
                    }
                } finally {
                    Releasables.closeExpectNoException(blocks);
                    prepared.close();
                }
            }
        }
    }

    /**
     * Feeds {@code valueBlock} through {@code aggFn} via {@code addGather}. Takes ownership of
     * {@code valueBlock} (closes it), but not of {@code groupIdVector}/{@code positionVector}.
     * Handles {@code prepareProcessRawInputPage} returning {@code null} (its documented opt-out
     * when every value in the page is null) by simply not calling addGather.
     */
    private void addLongsViaGather(
        GroupingAggregatorFunction aggFn,
        int groupCount,
        LongBlock valueBlock,
        IntVector groupIdVector,
        IntVector positionVector
    ) {
        try (Page page = new Page(valueBlock)) {
            GroupingAggregatorFunction.AddInput addInput = aggFn.prepareProcessRawInputPage(new SeenGroupIds.Range(0, groupCount), page);
            if (addInput == null) {
                return;
            }
            try {
                addInput.addGather(groupIdVector, positionVector);
            } finally {
                addInput.close();
            }
        }
    }

    private void assertLongSums(GroupingAggregatorFunction aggFn, DriverContext ctx, Map<Integer, Long> expected) {
        // Sorted so `selected`, per its contract, is ascending; a TreeMap gives both the sorted
        // keys and the per-group expectation in one structure.
        Map<Integer, Long> sortedExpected = new TreeMap<>(expected);
        int[] selectedGroups = sortedExpected.keySet().stream().mapToInt(Integer::intValue).toArray();
        try (
            IntVector selected = blockFactory.newIntArrayVector(selectedGroups, selectedGroups.length);
            GroupingAggregatorEvaluationContext evalCtx = new GroupingAggregatorEvaluationContext(ctx)
        ) {
            GroupingAggregatorFunction.PreparedForEvaluation prepared = aggFn.prepareEvaluateFinal(selected, evalCtx);
            Block[] blocks = new Block[1];
            try {
                prepared.evaluate(blocks, 0, selected);
                LongBlock result = (LongBlock) blocks[0];
                int i = 0;
                for (Map.Entry<Integer, Long> entry : sortedExpected.entrySet()) {
                    assertThat("group " + entry.getKey(), result.getLong(i), equalTo(entry.getValue()));
                    i++;
                }
            } finally {
                Releasables.closeExpectNoException(blocks);
                prepared.close();
            }
        }
    }

    /**
     * Builds the {@code groupIds} array a bucket-sort-style caller would pass alongside
     * {@code positions}: {@code groupIds[k]} is the group for the row originally at
     * {@code positions[k]}.
     */
    private int[] gatherGroupIds(int[] rowGroup, int[] positions) {
        int[] groupIds = new int[positions.length];
        for (int k = 0; k < positions.length; k++) {
            groupIds[k] = rowGroup[positions[k]];
        }
        return groupIds;
    }

    private int[] rangeArray(int n) {
        int[] r = new int[n];
        for (int i = 0; i < n; i++) {
            r[i] = i;
        }
        return r;
    }

    private int[] shuffledPositions(int rows) {
        int[] positions = rangeArray(rows);
        for (int i = rows - 1; i > 0; i--) {
            int j = between(0, i);
            int tmp = positions[i];
            positions[i] = positions[j];
            positions[j] = tmp;
        }
        return positions;
    }

    private DriverContext driverContext() {
        return new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, blockFactory, null);
    }
}
