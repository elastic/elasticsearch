/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.approximate;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.List;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static java.lang.Double.NaN;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.nullValue;

public class ConfidenceIntervalTests extends AbstractScalarFunctionTestCase {

    private static final String EVALUATOR_STRING =
        "ConfidenceIntervalEvaluator[bestEstimateBlock=Attribute[channel=0], estimatesBlock=Attribute[channel=1], "
            + "trialCountBlock=Attribute[channel=2], bucketCountBlock=Attribute[channel=3], confidenceLevelBlock=Attribute[channel=4]]";

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = List.of(
            randomBuckets(),
            allBucketsFilled(),
            nanBuckets_ignoreNan(),
            nanBuckets_zeroNan(),
            inconsistentData(),
            manyNans()
        );
        return parameterSuppliersFromTypedDataWithDefaultChecks(false, suppliers);
    }

    public ConfidenceIntervalTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new ConfidenceInterval(source, args.get(0), args.get(1), args.get(2), args.get(3), args.get(4));
    }

    private static TestCaseSupplier randomBuckets() {
        return new TestCaseSupplier(
            "randomBuckets",
            List.of(DataType.DOUBLE, DataType.DOUBLE, DataType.INTEGER, DataType.INTEGER, DataType.DOUBLE),
            () -> {
                int trialCount = randomIntBetween(1, 10);
                int bucketCount = randomIntBetween(4, 10);
                double confidenceLevel = randomDoubleBetween(0.8, 0.95, true);
                double bestEstimate = bucketCount / 2.0;
                List<Double> estimates = IntStream.range(0, trialCount * bucketCount)
                    .mapToDouble(i -> randomDoubleBetween((i % bucketCount) + 0.4, (i % bucketCount) + 0.6, true))
                    .boxed()
                    .toList();
                return new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(bestEstimate, DataType.DOUBLE, "bestEstimate"),
                        new TestCaseSupplier.TypedData(estimates, DataType.DOUBLE, "estimates"),
                        new TestCaseSupplier.TypedData(trialCount, DataType.INTEGER, "trialCount"),
                        new TestCaseSupplier.TypedData(bucketCount, DataType.INTEGER, "bucketCount"),
                        new TestCaseSupplier.TypedData(confidenceLevel, DataType.DOUBLE, "confidenceLevel")
                    ),
                    EVALUATOR_STRING,
                    DataType.DOUBLE,
                    contains(
                        both(greaterThan(0.0)).and(lessThan(bestEstimate)),
                        both(greaterThan(bestEstimate)).and(lessThan((double) bucketCount)),
                        equalTo(1.0)
                    )
                );
            }
        );
    }

    private static TestCaseSupplier allBucketsFilled() {
        return new TestCaseSupplier(
            "allBucketsFilled",
            List.of(DataType.DOUBLE, DataType.DOUBLE, DataType.INTEGER, DataType.INTEGER, DataType.DOUBLE),
            () -> new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(2.0, DataType.DOUBLE, "bestEstimate"),
                    new TestCaseSupplier.TypedData(
                        List.of(2.15, 1.73, 2.1, 2.49, 2.41, 2.06, 2.29, 1.97, 1.54, 1.97, 2.41, 1.75, 1.55, 2.33, 1.64),
                        DataType.DOUBLE,
                        "estimates"
                    ),
                    new TestCaseSupplier.TypedData(3, DataType.INTEGER, "trialCount"),
                    new TestCaseSupplier.TypedData(5, DataType.INTEGER, "bucketCount"),
                    new TestCaseSupplier.TypedData(0.8, DataType.DOUBLE, "confidence_level")
                ),
                EVALUATOR_STRING,
                DataType.DOUBLE,
                contains(closeTo(1.8293144967855208, 1e-9), closeTo(2.164428203663303, 1e-9), closeTo(1.0, 1e-9))
            )
        );
    }

    private static TestCaseSupplier nanBuckets_ignoreNan() {
        return new TestCaseSupplier(
            "nanBuckets_ignoreNan",
            List.of(DataType.DOUBLE, DataType.DOUBLE, DataType.INTEGER, DataType.INTEGER, DataType.DOUBLE),
            () -> new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(2.0, DataType.DOUBLE, "bestEstimate"),
                    new TestCaseSupplier.TypedData(
                        List.of(2.15, NaN, NaN, 2.49, 2.41, NaN, 2.29, NaN, 1.54, 1.97, 2.41, NaN, 1.55, NaN, 1.64),
                        DataType.DOUBLE,
                        "estimates"
                    ),
                    new TestCaseSupplier.TypedData(3, DataType.INTEGER, "trialCount"),
                    new TestCaseSupplier.TypedData(5, DataType.INTEGER, "bucketCount"),
                    new TestCaseSupplier.TypedData(0.8, DataType.DOUBLE, "confidence_level")
                ),
                EVALUATOR_STRING,
                DataType.DOUBLE,
                contains(closeTo(1.8443260740876288, 1e-9), closeTo(2.164997868635109, 1e-9), closeTo(0.0, 1e-9))
            )
        );
    }

    private static TestCaseSupplier nanBuckets_zeroNan() {
        return new TestCaseSupplier(
            "nanBuckets_zeroNan",
            List.of(DataType.DOUBLE, DataType.DOUBLE, DataType.INTEGER, DataType.INTEGER, DataType.DOUBLE),
            () -> new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(1.0, DataType.DOUBLE, "bestEstimate"),
                    new TestCaseSupplier.TypedData(
                        List.of(2.15, NaN, NaN, 2.49, 2.41, NaN, 2.29, NaN, 1.54, 1.97, 2.41, NaN, 1.55, NaN, 1.64),
                        DataType.DOUBLE,
                        "estimates"
                    ),
                    new TestCaseSupplier.TypedData(3, DataType.INTEGER, "trialCount"),
                    new TestCaseSupplier.TypedData(5, DataType.INTEGER, "bucketCount"),
                    new TestCaseSupplier.TypedData(0.8, DataType.DOUBLE, "confidence_level")
                ),
                EVALUATOR_STRING,
                DataType.DOUBLE,
                contains(closeTo(0.4041519539094244, 1e-9), closeTo(1.6023321533418913, 1e-9), closeTo(0.0, 1e-9))
            )
        );
    }

    private static TestCaseSupplier inconsistentData() {
        return new TestCaseSupplier(
            "inconsistentData",
            List.of(DataType.DOUBLE, DataType.DOUBLE, DataType.INTEGER, DataType.INTEGER, DataType.DOUBLE),
            () -> new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(123.456, DataType.DOUBLE, "bestEstimate"),
                    new TestCaseSupplier.TypedData(
                        List.of(2.15, NaN, NaN, 2.49, 2.41, NaN, 2.29, NaN, 1.54, 1.97, 2.41, NaN, 1.55, NaN, 1.64),
                        DataType.DOUBLE,
                        "estimates"
                    ),
                    new TestCaseSupplier.TypedData(3, DataType.INTEGER, "trialCount"),
                    new TestCaseSupplier.TypedData(5, DataType.INTEGER, "bucketCount"),
                    new TestCaseSupplier.TypedData(0.8, DataType.DOUBLE, "confidence_level")
                ),
                EVALUATOR_STRING,
                DataType.DOUBLE,
                nullValue()
            )
        );
    }

    private static TestCaseSupplier manyNans() {
        return new TestCaseSupplier(
            "manyNans",
            List.of(DataType.DOUBLE, DataType.DOUBLE, DataType.INTEGER, DataType.INTEGER, DataType.DOUBLE),
            () -> new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(2.0, DataType.DOUBLE, "bestEstimate"),
                    new TestCaseSupplier.TypedData(
                        List.of(2.15, NaN, NaN, NaN, NaN, NaN, 2.29, NaN, NaN, NaN, 2.41, NaN, NaN, NaN, 1.64),
                        DataType.DOUBLE,
                        "estimates"
                    ),
                    new TestCaseSupplier.TypedData(3, DataType.INTEGER, "trialCount"),
                    new TestCaseSupplier.TypedData(5, DataType.INTEGER, "bucketCount"),
                    new TestCaseSupplier.TypedData(0.8, DataType.DOUBLE, "confidence_level")
                ),
                EVALUATOR_STRING,
                DataType.DOUBLE,
                nullValue()
            )
        );
    }
}
