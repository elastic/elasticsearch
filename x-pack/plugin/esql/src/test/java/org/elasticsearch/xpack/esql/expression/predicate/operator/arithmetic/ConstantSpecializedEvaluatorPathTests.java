/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic;

import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.ConstantMethodResultSpecializer;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.junit.After;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * End-to-end exercise of the generated evaluator's two execution paths under
 * {@code @Fixed(jitConstant=true)}: the constant-specialized subclass produced by
 * {@link ConstantMethodResultSpecializer} (fast path, constant folded at JIT time) and the
 * {@code Standard} nested subclass (admission-rejected fallback that stores the
 * constant in a regular instance field).
 *
 * <p>Asserts both that the right subclass is reached and that the
 * {@code pathLabel()} marker actually surfaces in {@code toString()} — the
 * latter is what ESQL PROFILE reads, and without this test a silent rename
 * (e.g. dropping the marker emission in codegen) would slip through.
 *
 * <p>Uses {@link ModLongsByConstantEvaluator} as a representative; the
 * machinery is shared across all five JIT-constant ops, so one canary suffices.
 * The {@link ConstantMethodResultSpecializer#SHARED} singleton is reset in {@code @After}
 * since these tests mutate its admission threshold to force each path.
 */
public class ConstantSpecializedEvaluatorPathTests extends ESTestCase {

    @After
    public void resetSharedSpinner() {
        ConstantMethodResultSpecializer.SHARED.resetForTest();
    }

    public void testStandardPathLabeledAndComputes() {
        // Admission threshold above any plausible access count → spinner returns empty,
        // Factory.get() routes to the Standard nested subclass.
        ConstantMethodResultSpecializer.SHARED.setAdmissionThreshold(Integer.MAX_VALUE);

        long rhs = randomValueOtherThan(0L, ESTestCase::randomLong);
        long[] lhsValues = new long[] { 0L, 1L, randomLong(), randomLong(), randomLong() };

        DriverContext ctx = driverContext();
        try (ExpressionEvaluator eval = factory(rhs).get(ctx)) {
            assertThat(
                "Standard subclass must surface its pathLabel in toString so PROFILE distinguishes it",
                eval.toString(),
                containsString("(standard)")
            );
            assertResults(eval, ctx, lhsValues, rhs);
        }
    }

    public void testSpunPathLabeledAndComputes() {
        // Admission threshold = 1 → first access is admitted, spinner produces the
        // hidden subclass, Factory.get() returns the spun instance.
        ConstantMethodResultSpecializer.SHARED.setAdmissionThreshold(1);

        long rhs = randomValueOtherThan(0L, ESTestCase::randomLong);
        long[] lhsValues = new long[] { 0L, 1L, randomLong(), randomLong(), randomLong() };

        DriverContext ctx = driverContext();
        try (ExpressionEvaluator eval = factory(rhs).get(ctx)) {
            assertThat(
                "Spun subclass inherits the base pathLabel default so PROFILE marks it jit-folded",
                eval.toString(),
                containsString("(jit-folded)")
            );
            assertResults(eval, ctx, lhsValues, rhs);
        }
    }

    private ModLongsByConstantEvaluator.Factory factory(long rhs) {
        return new ModLongsByConstantEvaluator.Factory(Source.EMPTY, new FixedLongFactory(), rhs);
    }

    private void assertResults(ExpressionEvaluator eval, DriverContext ctx, long[] lhsValues, long rhs) {
        try (
            LongVector vec = ctx.blockFactory().newLongArrayVector(lhsValues, lhsValues.length);
            Block result = eval.eval(new Page(vec.asBlock()))
        ) {
            LongBlock out = (LongBlock) result;
            for (int i = 0; i < lhsValues.length; i++) {
                assertThat("position " + i, out.getLong(i), equalTo(lhsValues[i] % rhs));
            }
        }
    }

    private static DriverContext driverContext() {
        return new DriverContext(
            new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, new NoneCircuitBreakerService()).withCircuitBreaking(),
            TestBlockFactory.getNonBreakingInstance(),
            null
        );
    }

    /**
     * Pass-through factory that reads channel 0 of the input page as the lhs Block.
     * Used so the test owns the lhs construction directly rather than going through
     * the planner/Attribute machinery.
     */
    private static class FixedLongFactory implements ExpressionEvaluator.Factory {
        @Override
        public ExpressionEvaluator get(DriverContext context) {
            return new ExpressionEvaluator() {
                @Override
                public Block eval(Page page) {
                    Block in = page.getBlock(0);
                    in.incRef();
                    return in;
                }

                @Override
                public long baseRamBytesUsed() {
                    return 0;
                }

                @Override
                public void close() {}
            };
        }

        @Override
        public String toString() {
            return "Attribute[channel=0]";
        }
    }
}
