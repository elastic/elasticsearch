/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.operator.comparison;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.NumericUtils;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.randomExponentialHistogram;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.randomHistogram;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.randomTDigest;
import static org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier.appliesTo;
import static org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier.randomDenseVector;

public class EqualsTests extends AbstractScalarFunctionTestCase {
    public EqualsTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        suppliers.addAll(
            TestCaseSupplier.forBinaryComparisonWithWidening(
                new TestCaseSupplier.NumericTypeTestConfigs<>(
                    new TestCaseSupplier.NumericTypeTestConfig<>(
                        (Integer.MIN_VALUE >> 1) - 1,
                        (Integer.MAX_VALUE >> 1) - 1,
                        (l, r) -> l.intValue() == r.intValue(),
                        "EqualsIntsEvaluator"
                    ),
                    new TestCaseSupplier.NumericTypeTestConfig<>(
                        (Long.MIN_VALUE >> 1) - 1,
                        (Long.MAX_VALUE >> 1) - 1,
                        (l, r) -> l.longValue() == r.longValue(),
                        "EqualsLongsEvaluator"
                    ),
                    new TestCaseSupplier.NumericTypeTestConfig<>(
                        Double.NEGATIVE_INFINITY,
                        Double.POSITIVE_INFINITY,
                        // NB: this has different behavior than Double::equals
                        (l, r) -> l.doubleValue() == r.doubleValue(),
                        "EqualsDoublesEvaluator"
                    )
                ),
                "lhs",
                "rhs",
                (lhs, rhs) -> List.of(),
                false
            )
        );

        // Unsigned Long cases
        // TODO: These should be integrated into the type cross product above, but are currently broken
        // see https://github.com/elastic/elasticsearch/issues/102935
        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                "EqualsLongsEvaluator",
                "lhs",
                "rhs",
                Object::equals,
                DataType.BOOLEAN,
                TestCaseSupplier.ulongCases(BigInteger.ZERO, NumericUtils.UNSIGNED_LONG_MAX, true),
                TestCaseSupplier.ulongCases(BigInteger.ZERO, NumericUtils.UNSIGNED_LONG_MAX, true),
                List.of(),
                false
            )
        );
        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                "EqualsBoolsEvaluator",
                "lhs",
                "rhs",
                Object::equals,
                DataType.BOOLEAN,
                TestCaseSupplier.booleanCases(),
                TestCaseSupplier.booleanCases(),
                List.of(),
                false
            )
        );
        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                "EqualsBytesRefEvaluator",
                "lhs",
                "rhs",
                Object::equals,
                DataType.BOOLEAN,
                TestCaseSupplier.ipCases(),
                TestCaseSupplier.ipCases(),
                List.of(),
                false
            )
        );
        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                "EqualsBytesRefEvaluator",
                "lhs",
                "rhs",
                Object::equals,
                DataType.BOOLEAN,
                TestCaseSupplier.versionCases(""),
                TestCaseSupplier.versionCases(""),
                List.of(),
                false
            )
        );
        // Datetime
        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                "EqualsLongsEvaluator",
                "lhs",
                "rhs",
                Object::equals,
                DataType.BOOLEAN,
                TestCaseSupplier.dateCases(),
                TestCaseSupplier.dateCases(),
                List.of(),
                false
            )
        );

        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                "EqualsLongsEvaluator",
                "lhs",
                "rhs",
                Object::equals,
                DataType.BOOLEAN,
                TestCaseSupplier.dateNanosCases(),
                TestCaseSupplier.dateNanosCases(),
                List.of(),
                false
            )
        );

        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                "EqualsNanosMillisEvaluator",
                "lhs",
                "rhs",
                Object::equals,
                DataType.BOOLEAN,
                TestCaseSupplier.dateNanosCases(),
                TestCaseSupplier.dateCases(),
                List.of(),
                false
            )
        );

        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                "EqualsMillisNanosEvaluator",
                "lhs",
                "rhs",
                Object::equals,
                DataType.BOOLEAN,
                TestCaseSupplier.dateCases(),
                TestCaseSupplier.dateNanosCases(),
                List.of(),
                false
            )
        );

        suppliers.addAll(
            TestCaseSupplier.stringCases(
                Object::equals,
                (lhsType, rhsType) -> "EqualsBytesRefEvaluator[lhs=Attribute[channel=0], rhs=Attribute[channel=1]]",
                List.of(),
                DataType.BOOLEAN
            )
        );

        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                "EqualsGeometriesEvaluator",
                "lhs",
                "rhs",
                Object::equals,
                DataType.BOOLEAN,
                TestCaseSupplier.geoPointCases(),
                TestCaseSupplier.geoPointCases(),
                List.of(),
                false
            )
        );

        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                "EqualsGeometriesEvaluator",
                "lhs",
                "rhs",
                Object::equals,
                DataType.BOOLEAN,
                TestCaseSupplier.geoShapeCases(),
                TestCaseSupplier.geoShapeCases(),
                List.of(),
                false
            )
        );
        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                "EqualsGeometriesEvaluator",
                "lhs",
                "rhs",
                Object::equals,
                DataType.BOOLEAN,
                TestCaseSupplier.cartesianPointCases(),
                TestCaseSupplier.cartesianPointCases(),
                List.of(),
                false
            )
        );

        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                "EqualsGeometriesEvaluator",
                "lhs",
                "rhs",
                Object::equals,
                DataType.BOOLEAN,
                TestCaseSupplier.cartesianShapeCases(),
                TestCaseSupplier.cartesianShapeCases(),
                List.of(),
                false
            )
        );

        for (DataType gridType : new DataType[] { DataType.GEOHASH, DataType.GEOTILE, DataType.GEOHEX }) {
            suppliers.addAll(
                TestCaseSupplier.forBinaryNotCasting(
                    "EqualsLongsEvaluator",
                    "lhs",
                    "rhs",
                    Object::equals,
                    DataType.BOOLEAN,
                    TestCaseSupplier.geoGridCases(gridType),
                    TestCaseSupplier.geoGridCases(gridType),
                    List.of(),
                    false
                )
            );
        }

        // Flattened cases
        if (DataType.FLATTENED.supportedVersion().supportedLocally()) {
            suppliers.addAll(
                TestCaseSupplier.forBinaryNotCasting(
                    "EqualsBytesRefEvaluator",
                    "lhs",
                    "rhs",
                    Object::equals,
                    DataType.BOOLEAN,
                    TestCaseSupplier.flattenedCases(),
                    TestCaseSupplier.flattenedCases(),
                    List.of(),
                    false
                )
            );
        }

        // Date range cases
        if (DataType.DATE_RANGE.supportedVersion().supportedLocally()) {
            suppliers.addAll(
                TestCaseSupplier.forBinaryNotCasting(
                    "EqualsLongRangeEvaluator",
                    "lhs",
                    "rhs",
                    Object::equals,
                    DataType.BOOLEAN,
                    TestCaseSupplier.dateRangeCases(),
                    TestCaseSupplier.dateRangeCases(),
                    List.of(),
                    false
                )
            );
        }

        // Dense vector cases
        suppliers.add(new TestCaseSupplier("<dense_vector>, <dense_vector>", List.of(DataType.DENSE_VECTOR, DataType.DENSE_VECTOR), () -> {
            int dimensions = between(64, 128);
            List<Float> vector = randomDenseVector(dimensions);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(vector, DataType.DENSE_VECTOR, "lhs"),
                    new TestCaseSupplier.TypedData(vector, DataType.DENSE_VECTOR, "rhs")
                ),
                "EqualsDenseVectorEvaluator[lhs=Attribute[channel=0], rhs=Attribute[channel=1]]",
                DataType.BOOLEAN,
                org.hamcrest.Matchers.equalTo(true)
            );
        }));
        suppliers.add(
            new TestCaseSupplier("<dense_vector>, <different dense_vector>", List.of(DataType.DENSE_VECTOR, DataType.DENSE_VECTOR), () -> {
                int dimensions = between(64, 128);
                List<Float> left = randomDenseVector(dimensions);
                List<Float> right = randomValueOtherThan(left, () -> randomDenseVector(dimensions));
                return new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(left, DataType.DENSE_VECTOR, "lhs"),
                        new TestCaseSupplier.TypedData(right, DataType.DENSE_VECTOR, "rhs")
                    ),
                    "EqualsDenseVectorEvaluator[lhs=Attribute[channel=0], rhs=Attribute[channel=1]]",
                    DataType.BOOLEAN,
                    org.hamcrest.Matchers.equalTo(false)
                );
            })
        );

        // TDigest cases
        FunctionAppliesTo histogramAppliesTo = appliesTo(FunctionAppliesToLifecycle.GA, "9.6.0", "", false);
        suppliers.add(new TestCaseSupplier("<tdigest>, <tdigest>", List.of(DataType.TDIGEST, DataType.TDIGEST), () -> {
            var tdigest = randomTDigest();
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(tdigest, DataType.TDIGEST, "lhs").withAppliesTo(histogramAppliesTo),
                    new TestCaseSupplier.TypedData(tdigest, DataType.TDIGEST, "rhs").withAppliesTo(histogramAppliesTo)
                ),
                "EqualsTDigestEvaluator[lhs=Attribute[channel=0], rhs=Attribute[channel=1]]",
                DataType.BOOLEAN,
                org.hamcrest.Matchers.equalTo(true)
            );
        }));
        suppliers.add(new TestCaseSupplier("<tdigest>, <different tdigest>", List.of(DataType.TDIGEST, DataType.TDIGEST), () -> {
            var left = randomTDigest();
            var right = randomValueOtherThan(left, () -> randomTDigest());
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(left, DataType.TDIGEST, "lhs").withAppliesTo(histogramAppliesTo),
                    new TestCaseSupplier.TypedData(right, DataType.TDIGEST, "rhs").withAppliesTo(histogramAppliesTo)
                ),
                "EqualsTDigestEvaluator[lhs=Attribute[channel=0], rhs=Attribute[channel=1]]",
                DataType.BOOLEAN,
                org.hamcrest.Matchers.equalTo(false)
            );
        }));

        // Exponential histogram cases
        suppliers.add(
            new TestCaseSupplier(
                "<exponential_histogram>, <exponential_histogram>",
                List.of(DataType.EXPONENTIAL_HISTOGRAM, DataType.EXPONENTIAL_HISTOGRAM),
                () -> {
                    var histo = randomExponentialHistogram();
                    return new TestCaseSupplier.TestCase(
                        List.of(
                            new TestCaseSupplier.TypedData(histo, DataType.EXPONENTIAL_HISTOGRAM, "lhs").withAppliesTo(histogramAppliesTo),
                            new TestCaseSupplier.TypedData(histo, DataType.EXPONENTIAL_HISTOGRAM, "rhs").withAppliesTo(histogramAppliesTo)
                        ),
                        "EqualsExponentialHistogramEvaluator[lhs=Attribute[channel=0], rhs=Attribute[channel=1]]",
                        DataType.BOOLEAN,
                        org.hamcrest.Matchers.equalTo(true)
                    );
                }
            )
        );
        suppliers.add(
            new TestCaseSupplier(
                "<exponential_histogram>, <different exponential_histogram>",
                List.of(DataType.EXPONENTIAL_HISTOGRAM, DataType.EXPONENTIAL_HISTOGRAM),
                () -> {
                    var left = randomExponentialHistogram();
                    var right = randomValueOtherThan(left, () -> randomExponentialHistogram());
                    return new TestCaseSupplier.TestCase(
                        List.of(
                            new TestCaseSupplier.TypedData(left, DataType.EXPONENTIAL_HISTOGRAM, "lhs").withAppliesTo(histogramAppliesTo),
                            new TestCaseSupplier.TypedData(right, DataType.EXPONENTIAL_HISTOGRAM, "rhs").withAppliesTo(histogramAppliesTo)
                        ),
                        "EqualsExponentialHistogramEvaluator[lhs=Attribute[channel=0], rhs=Attribute[channel=1]]",
                        DataType.BOOLEAN,
                        org.hamcrest.Matchers.equalTo(false)
                    );
                }
            )
        );

        // Histogram cases
        suppliers.add(new TestCaseSupplier("<histogram>, <histogram>", List.of(DataType.HISTOGRAM, DataType.HISTOGRAM), () -> {
            var histo = randomHistogram();
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(histo, DataType.HISTOGRAM, "lhs").withAppliesTo(histogramAppliesTo),
                    new TestCaseSupplier.TypedData(histo, DataType.HISTOGRAM, "rhs").withAppliesTo(histogramAppliesTo)
                ),
                "EqualsBytesRefEvaluator[lhs=Attribute[channel=0], rhs=Attribute[channel=1]]",
                DataType.BOOLEAN,
                org.hamcrest.Matchers.equalTo(true)
            );
        }));
        suppliers.add(new TestCaseSupplier("<histogram>, <different histogram>", List.of(DataType.HISTOGRAM, DataType.HISTOGRAM), () -> {
            var left = randomHistogram();
            var right = randomValueOtherThan(left, () -> randomHistogram());
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(left, DataType.HISTOGRAM, "lhs").withAppliesTo(histogramAppliesTo),
                    new TestCaseSupplier.TypedData(right, DataType.HISTOGRAM, "rhs").withAppliesTo(histogramAppliesTo)
                ),
                "EqualsBytesRefEvaluator[lhs=Attribute[channel=0], rhs=Attribute[channel=1]]",
                DataType.BOOLEAN,
                org.hamcrest.Matchers.equalTo(false)
            );
        }));

        return parameterSuppliersFromTypedDataWithDefaultChecks(true, suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Equals(source, args.get(0), args.get(1));
    }

}
