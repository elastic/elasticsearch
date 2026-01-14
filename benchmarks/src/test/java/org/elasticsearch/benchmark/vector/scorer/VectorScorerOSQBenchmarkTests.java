/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.vector.scorer;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.Constants;
import org.elasticsearch.core.Strings;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.BeforeClass;
import org.openjdk.jmh.annotations.Param;

import static java.lang.Math.abs;
import static org.hamcrest.Matchers.closeTo;

import java.util.ArrayList;
import java.util.Random;
import java.util.function.DoubleFunction;

public class VectorScorerOSQBenchmarkTests extends ESTestCase {

    private final float deltaPercent = 0.1f;
    private final int dims;
    private final int bits;
    private final VectorScorerOSQBenchmark.DirectoryType directoryType;

    public VectorScorerOSQBenchmarkTests(int dims, int bits, VectorScorerOSQBenchmark.DirectoryType directoryType) {
        this.dims = dims;
        this.bits = bits;
        this.directoryType = directoryType;
    }

    @BeforeClass
    public static void skipWindows() {
        assumeFalse("doesn't work on windows yet", Constants.WINDOWS);
    }

    public void testSingleScalarVsVectorized() throws Exception {
        for (int i = 0; i < 100; i++) {
            var seed = randomLong();

            var scalar = new VectorScorerOSQBenchmark();
            var vectorized = new VectorScorerOSQBenchmark();
            try {
                scalar.implementation = VectorScorerOSQBenchmark.VectorImplementation.SCALAR;
                scalar.dims = dims;
                scalar.bits = bits;
                scalar.directoryType = directoryType;
                scalar.setup(new Random(seed));

                float[] expected = scalar.score();

                vectorized.implementation = VectorScorerOSQBenchmark.VectorImplementation.VECTORIZED;
                vectorized.dims = dims;
                vectorized.bits = bits;
                vectorized.directoryType = directoryType;
                vectorized.setup(new Random(seed));

                float[] result = vectorized.score();

                assertArrayEqualsPercent("single scoring, scalar VS vectorized", expected, result, deltaPercent);
            } finally {
                scalar.teardown();
                vectorized.teardown();
            }
        }
    }

    @FunctionalInterface
    interface BiFloatPredicate {
        boolean test(float expected, float value);
    }

    @FunctionalInterface
    interface FloatFunction<R> {
        R apply(float v);
    }

    private static class FloatArrayMatcher extends TypeSafeMatcher<float[]> {
        private final String transformDescription;
        private final BiFloatPredicate matcher;
        private final float[] values;

        private FloatArrayMatcher(String transformDescription, float[] values, BiFloatPredicate elementMatcher) {
            this.transformDescription = transformDescription;
            this.matcher = elementMatcher;
            this.values = values;
        }

        @Override
        protected boolean matchesSafely(float[] item) {
            try {
                for (int i = 0; i < item.length; i++) {
                    if (matcher.test(item[i], values[i]) == false) {
                        return false;
                    }
                }
            } catch (ClassCastException e) {
                throw new AssertionError(e);
            }
            return true;
        }

        @Override
        protected void describeMismatchSafely(float[] item, Description description) {

        }

        @Override
        public void describeTo(Description description) {
            description.appendText(transformDescription).appendText(" matches predicate");
        }

        static FloatArrayMatcher matchingItems(float[] values, BiFloatPredicate elementMatcher) {
            return new FloatArrayMatcher("", values, elementMatcher);
        }

        static FloatArrayMatcher matchingItems(float[] values, FloatFunction<Matcher<Float>> elementMatcherProvider) {
            return new FloatArrayMatcher("", values, (expected, value) -> elementMatcherProvider.apply(expected).matches(value));
        }

        static FloatArrayMatcher matchingItems(float[] values, DoubleFunction<Matcher<Double>> elementMatcherProvider) {
            return new FloatArrayMatcher("", values, (expected, value) -> elementMatcherProvider.apply(expected).matches(value));
        }
    }

    static class IsCloseToPercent extends TypeSafeMatcher<Float> {

        private final double percent;
        private final double value;

        IsCloseToPercent(float value, float percent) {
            this.percent = percent;
            this.value = value;
        }

        @Override
        public boolean matchesSafely(Float item) {
            return actualDelta(item) <= 0.0;
        }

        @Override
        public void describeMismatchSafely(Float item, Description mismatchDescription) {
            mismatchDescription.appendValue(item)
                .appendText(" differed by ")
                .appendValue(actualDelta(item))
                .appendText(" more than ")
                .appendValue((int)(percent * 100))
                .appendText("%");
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("a numeric value within ")
                .appendValue((int)(percent * 100))
                .appendText("% of ")
                .appendValue(value);
        }

        private double actualDelta(float item) {
            return abs(item - value) - (item * percent);
        }

        public static Matcher<Float> closeToPercent(float operand, float percent) {
            return new IsCloseToPercent(operand, percent);
        }

    }

    public void testBulkScalarVsVectorized() throws Exception {
        for (int i = 0; i < 100; i++) {
            var seed = randomLong();

            var scalar = new VectorScorerOSQBenchmark();
            var vectorized = new VectorScorerOSQBenchmark();
            try {

                scalar.implementation = VectorScorerOSQBenchmark.VectorImplementation.SCALAR;
                scalar.dims = dims;
                scalar.bits = bits;
                scalar.directoryType = directoryType;
                scalar.setup(new Random(seed));

                float[] expected = scalar.bulkScore();

                vectorized.implementation = VectorScorerOSQBenchmark.VectorImplementation.VECTORIZED;
                vectorized.dims = dims;
                vectorized.bits = bits;
                vectorized.directoryType = directoryType;
                vectorized.setup(new Random(seed));

                float[] result = vectorized.bulkScore();

                BiFloatPredicate isCloseTo = (exp, actual) -> Math.abs(exp - actual) > exp * deltaPercent;
                assertThat(
                    "bulk scoring, scalar VS vectorized",
                    expected,
                    FloatArrayMatcher.matchingItems(expected, isCloseTo)
                );

                assertThat(
                    "bulk scoring, scalar VS vectorized",
                    expected,
                    FloatArrayMatcher.matchingItems(expected, (double e) -> closeTo(e, 1e-2))
                );

                assertThat(
                    "bulk scoring, scalar VS vectorized",
                    expected,
                    FloatArrayMatcher.matchingItems(expected, (float e) -> IsCloseToPercent.closeToPercent(e, 0.1f))
                );

                assertArrayEqualsPercent("bulk scoring, scalar VS vectorized", expected, result, deltaPercent);
            } finally {
                scalar.teardown();
                vectorized.teardown();
            }
        }
    }

    static void assertArrayEqualsPercent(String message, float[] expected, float[] actual, float deltaPercent) {
        if (expected.length == actual.length) {
            for (int i = 0; i < expected.length; i++) {
                var expectedValue = expected[i];
                if (Math.abs(expectedValue - actual[i]) > expectedValue * deltaPercent) {
                    fail(
                        Strings.format(
                            "%s: arrays first differed at element [%d]; expected:<%f> but was:<%f>",
                            message,
                            i,
                            expectedValue,
                            actual[i]
                        )
                    );
                }
            }
        }
    }

    @ParametersFactory
    public static Iterable<Object[]> parametersFactory() {
        try {
            String[] dims = VectorScorerOSQBenchmark.class.getField("dims").getAnnotationsByType(Param.class)[0].value();
            String[] bits = VectorScorerOSQBenchmark.class.getField("bits").getAnnotationsByType(Param.class)[0].value();
            var combinations = new ArrayList<Object[]>();
            for (var dim : dims) {
                var d = Integer.parseInt(dim);
                for (var bit : bits) {
                    var b = Integer.parseInt(bit);
                    for (var directoryType : VectorScorerOSQBenchmark.DirectoryType.values()) {
                        combinations.add(new Object[] { d, b, directoryType });
                    }
                }
            }
            return combinations;
        } catch (NoSuchFieldException e) {
            throw new AssertionError(e);
        }
    }
}
