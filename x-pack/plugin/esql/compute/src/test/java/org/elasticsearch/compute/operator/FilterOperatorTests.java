/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBigArrayVector;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.test.OperatorTestCase;
import org.elasticsearch.compute.test.TestDriverRunner;
import org.elasticsearch.compute.test.operator.blocksource.SequenceBooleanBlockSourceOperator;
import org.elasticsearch.compute.test.operator.blocksource.TupleLongLongBlockSourceOperator;
import org.elasticsearch.core.Tuple;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class FilterOperatorTests extends OperatorTestCase {
    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int end) {
        return new TupleLongLongBlockSourceOperator(blockFactory, LongStream.range(0, end).mapToObj(l -> Tuple.tuple(l, end - l)));
    }

    record SameLastDigit(DriverContext context, int lhs, int rhs) implements ExpressionEvaluator {
        @Override
        public Block eval(Page page) {
            LongVector lhsVector = page.<LongBlock>getBlock(0).asVector();
            LongVector rhsVector = page.<LongBlock>getBlock(1).asVector();
            BooleanVector.FixedBuilder result = context.blockFactory().newBooleanVectorFixedBuilder(page.getPositionCount());
            for (int p = 0; p < page.getPositionCount(); p++) {
                result.appendBoolean(lhsVector.getLong(p) % 10 == rhsVector.getLong(p) % 10);
            }
            return result.build().asBlock();
        }

        @Override
        public long baseRamBytesUsed() {
            return 0;
        }

        @Override
        public String toString() {
            return "SameLastDigit[lhs=" + lhs + ", rhs=" + rhs + ']';
        }

        @Override
        public void close() {}
    }

    @Override
    protected Operator.OperatorFactory simple(SimpleOptions options) {
        return new FilterOperator.FilterOperatorFactory(new ExpressionEvaluator.Factory() {

            @Override
            public ExpressionEvaluator get(DriverContext context) {
                return new SameLastDigit(context, 0, 1);
            }

            @Override
            public String toString() {
                return "SameLastDigit[lhs=0, rhs=1]";
            }
        });
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return equalTo("FilterOperator[evaluator=SameLastDigit[lhs=0, rhs=1]]");
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return expectedDescriptionOfSimple();
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> results) {
        int expectedCount = 0;
        for (var page : input) {
            LongVector lhs = page.<LongBlock>getBlock(0).asVector();
            LongVector rhs = page.<LongBlock>getBlock(1).asVector();
            for (int p = 0; p < page.getPositionCount(); p++) {
                if (lhs.getLong(p) % 10 == rhs.getLong(p) % 10) {
                    expectedCount++;
                }
            }
        }
        int actualCount = 0;
        for (var page : results) {
            LongVector lhs = page.<LongBlock>getBlock(0).asVector();
            LongVector rhs = page.<LongBlock>getBlock(1).asVector();
            for (int p = 0; p < page.getPositionCount(); p++) {
                assertThat(lhs.getLong(p) % 10, equalTo(rhs.getLong(p) % 10));
                actualCount++;
            }
        }
        assertThat(actualCount, equalTo(expectedCount));
    }

    public void testNoResults() {
        assertSimple(driverContext(), 3);
    }

    /**
     * {@link FilterOperator#toString()} is read by the {@link Driver} on every status update.
     * Recomputing the (often deep) evaluator-tree string each time can dominate CPU. Verify that
     * the same {@code String} instance is returned across calls so the description is computed
     * at most once per operator.
     */
    public void testToStringIsCached() {
        DriverContext ctx = driverContext();
        try (FilterOperator op = new FilterOperator(new SameLastDigit(ctx, 3, 7))) {
            String first = op.toString();
            String second = op.toString();
            assertThat(first, equalTo("FilterOperator[evaluator=SameLastDigit[lhs=3, rhs=7]]"));
            assertThat(second, sameInstance(first));
        }
    }

    public void testReadFromBlock() {
        var runner = new TestDriverRunner().builder(driverContext());
        runner.input(new SequenceBooleanBlockSourceOperator(runner.blockFactory(), List.of(true, false, true, false)));
        List<Page> results = runner.run(new FilterOperator.FilterOperatorFactory(dvrCtx -> new EvalOperatorTests.LoadFromPage(0)));
        List<Boolean> found = new ArrayList<>();
        for (var page : results) {
            BooleanVector lb = page.<BooleanBlock>getBlock(0).asVector();
            IntStream.range(0, lb.getPositionCount()).forEach(pos -> found.add(lb.getBoolean(pos)));
        }
        assertThat(found, equalTo(List.of(true, true)));
        results.forEach(Page::releaseBlocks);
        assertThat(runner.context().breaker().getUsed(), equalTo(0L));
    }

    public void testAllTrueArrayVector() {
        assertAllTrueVector(false);
    }

    public void testAllTrueBigArrayVector() {
        assertAllTrueVector(true);
    }

    public void testAllFalseArrayVector() {
        assertAllFalseVector(false);
    }

    public void testAllFalseBigArrayVector() {
        assertAllFalseVector(true);
    }

    public void testConstantTrueVector() {
        DriverContext ctx = driverContext();
        int positions = between(1, 1000);
        List<Long> found = filter(ctx, positions, ctx.blockFactory().newConstantBooleanBlockWith(true, positions), true);
        assertThat(found, equalTo(LongStream.range(0, positions).boxed().toList()));
        assertThat(ctx.breaker().getUsed(), equalTo(0L));
    }

    public void testConstantFalseVector() {
        DriverContext ctx = driverContext();
        int positions = between(1, 1000);
        assertThat(filter(ctx, positions, ctx.blockFactory().newConstantBooleanBlockWith(false, positions), false), nullValue());
        assertThat(ctx.breaker().getUsed(), equalTo(0L));
    }

    public void testAllNull() {
        DriverContext ctx = driverContext();
        int positions = between(1, 1000);
        assertThat(filter(ctx, positions, ctx.blockFactory().newConstantNullBlock(positions), false), nullValue());
        assertThat(ctx.breaker().getUsed(), equalTo(0L));
    }

    public void testMixedVector() {
        DriverContext ctx = driverContext();
        int positions = between(2, 1000);
        boolean[] values = new boolean[positions];
        for (int p = 0; p < positions; p++) {
            values[p] = randomBoolean();
        }
        // Make sure the vector is really mixed so neither fast path applies.
        int forcedTrue = between(0, positions - 1);
        values[forcedTrue] = true;
        values[randomValueOtherThan(forcedTrue, () -> between(0, positions - 1))] = false;
        List<Long> expected = new ArrayList<>();
        for (int p = 0; p < positions; p++) {
            if (values[p]) {
                expected.add((long) p);
            }
        }
        List<Long> found = filter(ctx, positions, booleanVector(ctx.blockFactory(), values, randomBoolean()).asBlock(), false);
        assertThat(found, equalTo(expected));
        assertThat(ctx.breaker().getUsed(), equalTo(0L));
    }

    public void testMixedBlockWithNulls() {
        DriverContext ctx = driverContext();
        int positions = between(1, 1000);
        List<Long> expected = new ArrayList<>();
        BooleanBlock predicate;
        try (BooleanBlock.Builder builder = ctx.blockFactory().newBooleanBlockBuilder(positions)) {
            for (int p = 0; p < positions; p++) {
                // Only non-null true values are selected; null is like false.
                if (randomBoolean()) {
                    builder.appendNull();
                } else {
                    boolean v = randomBoolean();
                    builder.appendBoolean(v);
                    if (v) {
                        expected.add((long) p);
                    }
                }
            }
            predicate = builder.build();
        }
        List<Long> found = filter(ctx, positions, predicate, false);
        assertThat(found, expected.isEmpty() ? nullValue() : equalTo(expected));
        assertThat(ctx.breaker().getUsed(), equalTo(0L));
    }

    /**
     * Empty pages never reach {@link FilterOperator#process}: {@link AbstractPageMappingOperator} passes them through.
     */
    public void testEmptyPage() {
        DriverContext ctx = driverContext();
        BooleanBlock predicate = randomBoolean()
            ? ctx.blockFactory().newConstantBooleanBlockWith(randomBoolean(), 0)
            : booleanVector(ctx.blockFactory(), new boolean[0], randomBoolean()).asBlock();
        Page page = new Page(ctx.blockFactory().newLongArrayVector(new long[0], 0).asBlock(), predicate);
        try (FilterOperator op = new FilterOperator(new EvalOperatorTests.LoadFromPage(1))) {
            op.addInput(page);
            assertThat(op.getOutput(), sameInstance(page));
        } finally {
            page.releaseBlocks();
        }
        assertThat(ctx.breaker().getUsed(), equalTo(0L));
    }

    private void assertAllTrueVector(boolean bigArray) {
        DriverContext ctx = driverContext();
        int positions = between(1, 1000);
        boolean[] values = new boolean[positions];
        Arrays.fill(values, true);
        List<Long> found = filter(ctx, positions, booleanVector(ctx.blockFactory(), values, bigArray).asBlock(), true);
        assertThat(found, equalTo(LongStream.range(0, positions).boxed().toList()));
        assertThat(ctx.breaker().getUsed(), equalTo(0L));
    }

    private void assertAllFalseVector(boolean bigArray) {
        DriverContext ctx = driverContext();
        int positions = between(1, 1000);
        assertThat(
            filter(ctx, positions, booleanVector(ctx.blockFactory(), new boolean[positions], bigArray).asBlock(), false),
            nullValue()
        );
        assertThat(ctx.breaker().getUsed(), equalTo(0L));
    }

    private static BooleanVector booleanVector(BlockFactory blockFactory, boolean[] values, boolean bigArray) {
        if (bigArray == false) {
            return blockFactory.newBooleanArrayVector(values, values.length);
        }
        BitArray bits = new BitArray(values.length, blockFactory.bigArrays());
        for (int p = 0; p < values.length; p++) {
            if (values[p]) {
                bits.set(p);
            }
        }
        return new BooleanBigArrayVector(bits, values.length, blockFactory);
    }

    /**
     * Runs a {@link FilterOperator} over a page of {@code [payload, predicate]} where the payload holds
     * the row numbers. Returns the selected payload values, or {@code null} if the page was dropped.
     * When {@code expectSameBlocks} the output must share the input blocks (a shallow copy).
     */
    private static List<Long> filter(DriverContext ctx, int positions, Block predicate, boolean expectSameBlocks) {
        LongBlock payload = ctx.blockFactory().newLongArrayVector(LongStream.range(0, positions).toArray(), positions).asBlock();
        Page page = new Page(payload, predicate);
        try (FilterOperator op = new FilterOperator(new EvalOperatorTests.LoadFromPage(1))) {
            op.addInput(page);
            Page output = op.getOutput();
            if (output == null) {
                return null;
            }
            try {
                if (expectSameBlocks) {
                    assertThat(output.getBlock(0), sameInstance(payload));
                }
                LongBlock selected = output.getBlock(0);
                List<Long> found = new ArrayList<>();
                for (int p = 0; p < selected.getPositionCount(); p++) {
                    found.add(selected.getLong(p));
                }
                assertThat(found, not(empty()));
                return found;
            } finally {
                output.releaseBlocks();
            }
        }
    }
}
