/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.kstest;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.aggs.MlAggsHelper;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class BucketCountKSTestAggregatorTests extends ESTestCase {

    private static final double[] UNIFORM_FRACTIONS = new double[] { 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1 };
    private static final MlAggsHelper.DoubleBucketValues LOWER_TAILED_VALUES = new MlAggsHelper.DoubleBucketValues(
        new long[] { 40, 60, 20, 30, 30, 10, 10, 10, 10, 10 },
        new double[] { 40, 60, 20, 30, 30, 10, 10, 10, 10, 10 }
    );
    private static final MlAggsHelper.DoubleBucketValues LOWER_TAILED_VALUES_SPARSE = new MlAggsHelper.DoubleBucketValues(
        new long[] { 4, 8, 2, 3, 3, 2, 1, 1, 1, 0 },
        new double[] { 4, 8, 2, 3, 3, 2, 1, 1, 1, 0 }
    );

    private static final MlAggsHelper.DoubleBucketValues UPPER_TAILED_VALUES = new MlAggsHelper.DoubleBucketValues(
        new long[] { 10, 10, 10, 40, 40, 40, 40, 40, 40, 40 },
        new double[] { 10, 10, 10, 40, 40, 40, 40, 40, 40, 40 }
    );
    private static final MlAggsHelper.DoubleBucketValues UPPER_TAILED_VALUES_SPARSE = new MlAggsHelper.DoubleBucketValues(
        new long[] { 1, 2, 2, 6, 7, 7, 7, 6, 6, 7 },
        new double[] { 1, 2, 2, 6, 7, 7, 7, 6, 6, 7 }
    );

    private static Map<String, Double> runKsTestAndValidate(MlAggsHelper.DoubleBucketValues bucketValues, SamplingMethod samplingMethod) {
        Map<String, Double> ksTestValues = BucketCountKSTestAggregator.ksTest(
            UNIFORM_FRACTIONS,
            bucketValues,
            EnumSet.of(Alternative.GREATER, Alternative.LESS, Alternative.TWO_SIDED),
            samplingMethod
        );
        assertValidValues(ksTestValues, Alternative.GREATER, Alternative.LESS, Alternative.TWO_SIDED);
        return ksTestValues;
    }

    private static void assertValidValues(Map<String, Double> ksValues, Alternative... alternatives) {
        for (Alternative alternative : alternatives) {
            assertThat(ksValues, hasKey(alternative.toString()));
            assertThat(ksValues.get(alternative.toString()), allOf(greaterThanOrEqualTo(0.0), lessThanOrEqualTo(1.0)));
        }
    }

    public void testKsTestSameDistrib() {
        int size = randomIntBetween(10, 100);
        double[] fracs = Stream.generate(() -> 1.0 / size).limit(size).mapToDouble(Double::valueOf).toArray();
        long randomValue = randomLongBetween(10, 10000);

        long[] counts = Stream.generate(() -> randomValue).limit(size).mapToLong(Long::longValue).toArray();
        double[] vals = Stream.generate(() -> randomValue).limit(size).mapToDouble(Double::valueOf).toArray();
        SamplingMethod samplingMethod = randomFrom(
            new SamplingMethod.UpperTail(),
            new SamplingMethod.Uniform(),
            new SamplingMethod.LowerTail()
        );
        Map<String, Double> ksValues = BucketCountKSTestAggregator.ksTest(
            fracs,
            new MlAggsHelper.DoubleBucketValues(counts, vals),
            EnumSet.of(Alternative.GREATER, Alternative.LESS, Alternative.TWO_SIDED),
            samplingMethod
        );
        assertThat(
            ksValues,
            allOf(hasKey(Alternative.GREATER.toString()), hasKey(Alternative.LESS.toString()), hasKey(Alternative.TWO_SIDED.toString()))
        );
        // Since these two distributions are the "same" (both uniform)
        // assume that the p-value is greater than 0.9
        assertThat(ksValues.get("less"), greaterThan(0.9));
        assertThat(ksValues.get("greater"), greaterThan(0.9));
        assertThat(ksValues.get("two_sided"), greaterThan(0.9));
    }

    public void testKsTest_LowerTailedValues() {
        Map<String, Double> lessValsUpperSampled = runKsTestAndValidate(LOWER_TAILED_VALUES, new SamplingMethod.UpperTail());
        Map<String, Double> lessValsUpperSampledSparsed = runKsTestAndValidate(LOWER_TAILED_VALUES_SPARSE, new SamplingMethod.UpperTail());
        Map<String, Double> lessValsLowerSampled = runKsTestAndValidate(LOWER_TAILED_VALUES, new SamplingMethod.LowerTail());
        Map<String, Double> lessValsLowerSampledSparsed = runKsTestAndValidate(LOWER_TAILED_VALUES_SPARSE, new SamplingMethod.LowerTail());
        Map<String, Double> lessValsUniformSampled = runKsTestAndValidate(LOWER_TAILED_VALUES, new SamplingMethod.Uniform());
        Map<String, Double> lessValsUniformSampledSparsed = runKsTestAndValidate(LOWER_TAILED_VALUES_SPARSE, new SamplingMethod.Uniform());

        assertThat(
            lessValsUpperSampled.get(Alternative.LESS.toString()),
            greaterThanOrEqualTo(lessValsLowerSampled.get(Alternative.LESS.toString()))
        );
        assertThat(
            lessValsUniformSampled.get(Alternative.LESS.toString()),
            greaterThan(lessValsLowerSampled.get(Alternative.LESS.toString()))
        );

        // its difficult to make sure things are super close in the sparser case as the sparser data is more "uniform"
        // Having error of 0.25 allows for this. But, the two values should be similar as the distributions are "close"
        for (String alternative : Arrays.stream(Alternative.values()).map(Alternative::toString).collect(Collectors.toList())) {
            assertThat(alternative, lessValsLowerSampled.get(alternative), closeTo(lessValsLowerSampledSparsed.get(alternative), 0.25));
            assertThat(alternative, lessValsUpperSampled.get(alternative), closeTo(lessValsUpperSampledSparsed.get(alternative), 0.25));
            assertThat(alternative, lessValsUniformSampled.get(alternative), closeTo(lessValsUniformSampledSparsed.get(alternative), 0.25));
        }
    }

    public void testKsTest_UpperTailedValues() {
        Map<String, Double> greaterValsUpperSampled = runKsTestAndValidate(UPPER_TAILED_VALUES, new SamplingMethod.UpperTail());
        Map<String, Double> greaterValsUpperSampledSparsed = runKsTestAndValidate(
            UPPER_TAILED_VALUES_SPARSE,
            new SamplingMethod.UpperTail()
        );
        Map<String, Double> greaterValsLowerSampled = runKsTestAndValidate(UPPER_TAILED_VALUES, new SamplingMethod.LowerTail());
        Map<String, Double> greaterValsLowerSampledSparsed = runKsTestAndValidate(
            UPPER_TAILED_VALUES_SPARSE,
            new SamplingMethod.LowerTail()
        );
        Map<String, Double> greaterValsUniformSampled = runKsTestAndValidate(UPPER_TAILED_VALUES, new SamplingMethod.Uniform());
        Map<String, Double> greaterValsUniformSampledSparsed = runKsTestAndValidate(
            UPPER_TAILED_VALUES_SPARSE,
            new SamplingMethod.Uniform()
        );

        assertThat(
            greaterValsUpperSampled.get(Alternative.LESS.toString()),
            greaterThanOrEqualTo(greaterValsLowerSampled.get(Alternative.LESS.toString()))
        );
        assertThat(
            greaterValsUpperSampled.get(Alternative.GREATER.toString()),
            greaterThanOrEqualTo(greaterValsLowerSampled.get(Alternative.GREATER.toString()))
        );
        assertThat(
            greaterValsUniformSampled.get(Alternative.LESS.toString()),
            greaterThan(greaterValsLowerSampled.get(Alternative.LESS.toString()))
        );

        // its difficult to make sure things are super close in the sparser case as the sparser data is more "uniform"
        // Having error of 0.25 allows for this. But, the two values should be similar as the distributions are "close"
        for (String alternative : Arrays.stream(Alternative.values()).map(Alternative::toString).collect(Collectors.toList())) {
            assertThat(
                alternative,
                greaterValsLowerSampled.get(alternative),
                closeTo(greaterValsLowerSampledSparsed.get(alternative), 0.25)
            );
            assertThat(
                alternative,
                greaterValsUpperSampled.get(alternative),
                closeTo(greaterValsUpperSampledSparsed.get(alternative), 0.25)
            );
            assertThat(
                alternative,
                greaterValsUniformSampled.get(alternative),
                closeTo(greaterValsUniformSampledSparsed.get(alternative), 0.25)
            );
        }
    }

    public void testKsTestWithZeros() {
        double[] values = new double[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
        long[] counts = new long[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };

        Map<String, Double> nanVals = BucketCountKSTestAggregator.ksTest(
            UNIFORM_FRACTIONS,
            new MlAggsHelper.DoubleBucketValues(counts, values),
            EnumSet.of(Alternative.GREATER, Alternative.LESS, Alternative.TWO_SIDED),
            randomFrom(new SamplingMethod.UpperTail(), new SamplingMethod.LowerTail(), new SamplingMethod.Uniform())
        );
        assertThat(
            nanVals,
            allOf(hasKey(Alternative.GREATER.toString()), hasKey(Alternative.LESS.toString()), hasKey(Alternative.TWO_SIDED.toString()))
        );
        for (Alternative alternative : Alternative.values()) {
            assertThat(nanVals.get(alternative.toString()), equalTo(Double.NaN));
        }

        double[] percentiles = new double[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
        values = new double[] { 4, 4, 2, 1, 3, 3, 4, 4, 1, 1 };
        counts = new long[] { 4, 4, 2, 1, 3, 3, 4, 4, 1, 1 };

        nanVals = BucketCountKSTestAggregator.ksTest(
            percentiles,
            new MlAggsHelper.DoubleBucketValues(counts, values),
            EnumSet.of(Alternative.GREATER, Alternative.LESS, Alternative.TWO_SIDED),
            randomFrom(new SamplingMethod.UpperTail(), new SamplingMethod.LowerTail(), new SamplingMethod.Uniform())
        );
        assertThat(
            nanVals,
            allOf(hasKey(Alternative.GREATER.toString()), hasKey(Alternative.LESS.toString()), hasKey(Alternative.TWO_SIDED.toString()))
        );
        for (Alternative alternative : Alternative.values()) {
            assertThat(nanVals.get(alternative.toString()), equalTo(Double.NaN));
        }
    }

}
