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
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.math.BigInteger;
import java.util.ArrayList;

import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.test.ESTestCase.randomFloat;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;

public class NotEqualsTests extends AbstractScalarFunctionTestCase {
    public NotEqualsTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
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
                        (l, r) -> l.intValue() != r.intValue(),
                        "NotEqualsIntsEvaluator"
                    ),
                    new TestCaseSupplier.NumericTypeTestConfig<>(
                        (Long.MIN_VALUE >> 1) - 1,
                        (Long.MAX_VALUE >> 1) - 1,
                        (l, r) -> l.longValue() != r.longValue(),
                        "NotEqualsLongsEvaluator"
                    ),
                    new TestCaseSupplier.NumericTypeTestConfig<>(
                        Double.NEGATIVE_INFINITY,
                        Double.POSITIVE_INFINITY,
                        // NB: this has different behavior than Double::equals
                        (l, r) -> l.doubleValue() != r.doubleValue(),
                        "NotEqualsDoublesEvaluator"
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
                "NotEqualsLongsEvaluator",
                "lhs",
                "rhs",
                (l, r) -> false == l.equals(r),
                DataType.BOOLEAN,
                TestCaseSupplier.ulongCases(BigInteger.ZERO, BigInteger.valueOf(Long.MAX_VALUE), true),
                TestCaseSupplier.ulongCases(BigInteger.ZERO, BigInteger.valueOf(Long.MAX_VALUE), true),
                List.of(),
                true
            )
        );
        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                "NotEqualsBoolsEvaluator",
                "lhs",
                "rhs",
                (l, r) -> false == l.equals(r),
                DataType.BOOLEAN,
                TestCaseSupplier.booleanCases(),
                TestCaseSupplier.booleanCases(),
                List.of(),
                false
            )
        );
        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                "NotEqualsKeywordsEvaluator",
                "lhs",
                "rhs",
                (l, r) -> false == l.equals(r),
                DataType.BOOLEAN,
                TestCaseSupplier.ipCases(),
                TestCaseSupplier.ipCases(),
                List.of(),
                false
            )
        );
        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                "NotEqualsKeywordsEvaluator",
                "lhs",
                "rhs",
                (l, r) -> false == l.equals(r),
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
                "NotEqualsLongsEvaluator",
                "lhs",
                "rhs",
                (l, r) -> false == l.equals(r),
                DataType.BOOLEAN,
                TestCaseSupplier.dateCases(),
                TestCaseSupplier.dateCases(),
                List.of(),
                false
            )
        );
        // Datenanos
        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                "NotEqualsLongsEvaluator",
                "lhs",
                "rhs",
                (l, r) -> false == l.equals(r),
                DataType.BOOLEAN,
                TestCaseSupplier.dateNanosCases(),
                TestCaseSupplier.dateNanosCases(),
                List.of(),
                false
            )
        );

        // nanoseconds to milliseconds. NB: these have different evaluator names depending on the direction
        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                "NotEqualsNanosMillisEvaluator",
                "lhs",
                "rhs",
                (l, r) -> false == l.equals(r),
                DataType.BOOLEAN,
                TestCaseSupplier.dateNanosCases(),
                TestCaseSupplier.dateCases(),
                List.of(),
                false
            )
        );

        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                "NotEqualsMillisNanosEvaluator",
                "lhs",
                "rhs",
                (l, r) -> false == l.equals(r),
                DataType.BOOLEAN,
                TestCaseSupplier.dateCases(),
                TestCaseSupplier.dateNanosCases(),
                List.of(),
                false
            )
        );

        suppliers.addAll(
            TestCaseSupplier.stringCases(
                (l, r) -> false == l.equals(r),
                (lhsType, rhsType) -> "NotEqualsKeywordsEvaluator[lhs=Attribute[channel=0], rhs=Attribute[channel=1]]",
                List.of(),
                DataType.BOOLEAN
            )
        );
        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                "NotEqualsGeometriesEvaluator",
                "lhs",
                "rhs",
                (l, r) -> false == l.equals(r),
                DataType.BOOLEAN,
                TestCaseSupplier.geoPointCases(),
                TestCaseSupplier.geoPointCases(),
                List.of(),
                false
            )
        );
        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                "NotEqualsGeometriesEvaluator",
                "lhs",
                "rhs",
                (l, r) -> false == l.equals(r),
                DataType.BOOLEAN,
                TestCaseSupplier.geoShapeCases(),
                TestCaseSupplier.geoShapeCases(),
                List.of(),
                false
            )
        );
        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                "NotEqualsGeometriesEvaluator",
                "lhs",
                "rhs",
                (l, r) -> false == l.equals(r),
                DataType.BOOLEAN,
                TestCaseSupplier.cartesianPointCases(),
                TestCaseSupplier.cartesianPointCases(),
                List.of(),
                false
            )
        );
        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                "NotEqualsGeometriesEvaluator",
                "lhs",
                "rhs",
                (l, r) -> false == l.equals(r),
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
                    "NotEqualsLongsEvaluator",
                    "lhs",
                    "rhs",
                    (l, r) -> false == l.equals(r),
                    DataType.BOOLEAN,
                    TestCaseSupplier.geoGridCases(gridType),
                    TestCaseSupplier.geoGridCases(gridType),
                    List.of(),
                    false
                )
            );
        }

        // Dense vector cases
        suppliers.add(
            new TestCaseSupplier(
                "<dense_vector>, <dense_vector>",
                List.of(DataType.DENSE_VECTOR, DataType.DENSE_VECTOR),
                () -> {
                    List<Float> vector = randomDenseVector();
                    return new TestCaseSupplier.TestCase(
                        List.of(
                            new TestCaseSupplier.TypedData(vector, DataType.DENSE_VECTOR, "lhs"),
                            new TestCaseSupplier.TypedData(vector, DataType.DENSE_VECTOR, "rhs")
                        ),
                        "NotEqualsDenseVectorEvaluator[lhs=Attribute[channel=0], rhs=Attribute[channel=1]]",
                        DataType.BOOLEAN,
                        org.hamcrest.Matchers.equalTo(false)
                    );
                }
            )
        );
        suppliers.add(
            new TestCaseSupplier(
                "<dense_vector>, <different dense_vector>",
                List.of(DataType.DENSE_VECTOR, DataType.DENSE_VECTOR),
                () -> {
                    List<Float> left = randomDenseVector();
                    List<Float> right = randomDenseVector();
                    // Make sure vectors are different
                    if (left.equals(right) && left.size() > 0) {
                        right.set(0, right.get(0) + 1.0f);
                    }
                    return new TestCaseSupplier.TestCase(
                        List.of(
                            new TestCaseSupplier.TypedData(left, DataType.DENSE_VECTOR, "lhs"),
                            new TestCaseSupplier.TypedData(right, DataType.DENSE_VECTOR, "rhs")
                        ),
                        "NotEqualsDenseVectorEvaluator[lhs=Attribute[channel=0], rhs=Attribute[channel=1]]",
                        DataType.BOOLEAN,
                        org.hamcrest.Matchers.equalTo(true)
                    );
                }
            )
        );

        return parameterSuppliersFromTypedDataWithDefaultChecks(true, suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new NotEquals(source, args.get(0), args.get(1));
    }

    private static List<Float> randomDenseVector() {
        int dimensions = randomIntBetween(64, 128);
        List<Float> vector = new ArrayList<>();
        for (int i = 0; i < dimensions; i++) {
            vector.add(randomFloat());
        }
        return vector;
    }
}
