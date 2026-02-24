/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geo.ShapeTestUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.NumericUtils;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.randomLiteral;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.CARTESIAN;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.GEO;
import static org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier.TypedData.MULTI_ROW_NULL;
import static org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier.TypedData.NULL;
import static org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvSliceTests.randomGrid;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class MvDifferenceTests extends AbstractScalarFunctionTestCase {
    public MvDifferenceTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        booleans(suppliers);
        ints(suppliers);
        longs(suppliers);
        doubles(suppliers);
        bytesRefs(suppliers);

        return parameterSuppliersFromTypedData(
            anyNullIsNull(
                suppliers,
                (nullPosition, nullValueDataType, original) -> nullPosition == 0 ? nullValueDataType : original.expectedType(),
                (nullPosition, nullData, original) -> original
            )
        );
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new MvDifference(source, args.get(0), args.get(1));
    }

    private static <T> Matcher<?> matchResult(List<T> result) {
        if (result == null || result.isEmpty()) {
            return equalTo(null);
        }

        if (result.size() > 1) {
            return equalTo(result);
        }
        // except both single value and multivalue with single entry.
        return anyOf(equalTo(result), equalTo(result.stream().findFirst().get()));
    }

    private static Matcher<?> matchResult(TestCaseSupplier.TypedData typedData) {
        var data = typedData.data();
        if (data instanceof List<?>) {
            return matchResult((List<?>) data);
        }
        return equalTo(typedData.asLiteral().value());
    }

    private static void booleans(List<TestCaseSupplier> suppliers) {
        suppliers.add(new TestCaseSupplier(List.of(DataType.BOOLEAN, DataType.BOOLEAN), () -> {
            List<Boolean> field1 = randomList(1, 10, () -> randomBoolean());
            List<Boolean> field2 = randomList(1, 10, () -> randomBoolean());
            var result = new ArrayList<>(field1);
            result.removeAll(field2);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field1, DataType.BOOLEAN, "field1"),
                    new TestCaseSupplier.TypedData(field2, DataType.BOOLEAN, "field2")
                ),
                "MvDifferenceBooleanEvaluator[field1=Attribute[channel=0], field2=Attribute[channel=1]]",
                DataType.BOOLEAN,
                matchResult(result)
            );
        }));
    }

    private static void ints(List<TestCaseSupplier> suppliers) {
        suppliers.add(new TestCaseSupplier(List.of(DataType.INTEGER, DataType.INTEGER), () -> {
            List<Integer> field1 = randomList(1, 10, () -> randomIntBetween(1, 10));
            List<Integer> field2 = randomList(1, 10, () -> randomIntBetween(1, 10));
            var result = new ArrayList<>(field1);
            result.removeAll(field2);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field1, DataType.INTEGER, "field1"),
                    new TestCaseSupplier.TypedData(field2, DataType.INTEGER, "field2")
                ),
                "MvDifferenceIntEvaluator[field1=Attribute[channel=0], field2=Attribute[channel=1]]",
                DataType.INTEGER,
                matchResult(result)
            );
        }));
    }

    private static void longs(List<TestCaseSupplier> suppliers) {
        addLongTestCase(suppliers, DataType.LONG, ESTestCase::randomLong);
        addLongTestCase(suppliers, DataType.DATETIME, ESTestCase::randomLong);
        addLongTestCase(suppliers, DataType.DATE_NANOS, ESTestCase::randomNonNegativeLong);
        for (DataType gridType : new DataType[] { DataType.GEOHASH, DataType.GEOTILE, DataType.GEOHEX }) {
            addLongTestCase(suppliers, gridType, () -> randomGrid(gridType));
        }

        suppliers.add(new TestCaseSupplier(List.of(DataType.UNSIGNED_LONG, DataType.UNSIGNED_LONG), () -> {
            List<Long> field1 = randomList(1, 10, ESTestCase::randomLong);
            List<Long> field2 = randomList(1, 10, ESTestCase::randomLong);
            var result = new ArrayList<>(field1);
            result.removeAll(field2);
            var unsignedResult = result.stream().map(NumericUtils::unsignedLongAsBigInteger).toList();
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field1, DataType.UNSIGNED_LONG, "field1"),
                    new TestCaseSupplier.TypedData(field2, DataType.UNSIGNED_LONG, "field2")
                ),
                "MvDifferenceLongEvaluator[field1=Attribute[channel=0], field2=Attribute[channel=1]]",
                DataType.UNSIGNED_LONG,
                matchResult(unsignedResult)
            );
        }));
    }

    private static void addLongTestCase(List<TestCaseSupplier> suppliers, DataType dataType, Supplier<Long> longSupplier) {
        suppliers.add(new TestCaseSupplier(List.of(dataType, dataType), () -> {
            List<Long> field1 = randomList(1, 10, longSupplier);
            List<Long> field2 = randomList(1, 10, longSupplier);
            var result = new ArrayList<>(field1);
            result.removeAll(field2);

            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field1, dataType, "field1"),
                    new TestCaseSupplier.TypedData(field2, dataType, "field2")
                ),
                "MvDifferenceLongEvaluator[field1=Attribute[channel=0], field2=Attribute[channel=1]]",
                dataType,
                matchResult(result)
            );
        }));
    }

    private static void doubles(List<TestCaseSupplier> suppliers) {
        suppliers.add(new TestCaseSupplier(List.of(DataType.DOUBLE, DataType.DOUBLE), () -> {
            List<Double> field1 = randomList(1, 10, () -> randomDouble());
            List<Double> field2 = randomList(1, 10, () -> randomDouble());
            var result = new ArrayList<>(field1);
            result.removeAll(field2);

            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field1, DataType.DOUBLE, "field1"),
                    new TestCaseSupplier.TypedData(field2, DataType.DOUBLE, "field2")
                ),
                "MvDifferenceDoubleEvaluator[field1=Attribute[channel=0], field2=Attribute[channel=1]]",
                DataType.DOUBLE,
                matchResult(result)
            );
        }));
    }

    private static void bytesRefs(List<TestCaseSupplier> suppliers) {
        for (DataType lhs : new DataType[] { DataType.KEYWORD, DataType.TEXT }) {
            for (DataType rhs : new DataType[] { DataType.KEYWORD, DataType.TEXT }) {
                suppliers.add(new TestCaseSupplier(List.of(lhs, rhs), () -> {
                    List<Object> field1 = randomList(1, 10, () -> randomLiteral(lhs).value());
                    List<Object> field2 = randomList(1, 10, () -> randomLiteral(rhs).value());
                    var result = new ArrayList<>(field1);
                    result.removeAll(field2);

                    return new TestCaseSupplier.TestCase(
                        List.of(
                            new TestCaseSupplier.TypedData(field1, lhs, "field1"),
                            new TestCaseSupplier.TypedData(field2, rhs, "field2")
                        ),
                        "MvDifferenceBytesRefEvaluator[field1=Attribute[channel=0], field2=Attribute[channel=1]]",
                        DataType.KEYWORD,
                        matchResult(result)
                    );
                }));
            }
        }
        suppliers.add(new TestCaseSupplier(List.of(DataType.IP, DataType.IP), () -> {
            List<Object> field1 = randomList(1, 10, () -> randomLiteral(DataType.IP).value());
            List<Object> field2 = randomList(1, 10, () -> randomLiteral(DataType.IP).value());
            var result = new ArrayList<>(field1);
            result.removeAll(field2);

            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field1, DataType.IP, "field"),
                    new TestCaseSupplier.TypedData(field2, DataType.IP, "field")
                ),
                "MvDifferenceBytesRefEvaluator[field1=Attribute[channel=0], field2=Attribute[channel=1]]",
                DataType.IP,
                matchResult(result)
            );
        }));

        suppliers.add(new TestCaseSupplier(List.of(DataType.VERSION, DataType.VERSION), () -> {
            List<Object> field1 = randomList(1, 10, () -> randomLiteral(DataType.VERSION).value());
            List<Object> field2 = randomList(1, 10, () -> randomLiteral(DataType.VERSION).value());
            var result = new ArrayList<>(field1);
            result.removeAll(field2);

            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field1, DataType.VERSION, "field"),
                    new TestCaseSupplier.TypedData(field2, DataType.VERSION, "field")
                ),
                "MvDifferenceBytesRefEvaluator[field1=Attribute[channel=0], field2=Attribute[channel=1]]",
                DataType.VERSION,
                matchResult(result)
            );
        }));

        suppliers.add(new TestCaseSupplier(List.of(DataType.GEO_POINT, DataType.GEO_POINT), () -> {
            List<Object> field1 = randomList(1, 10, () -> new BytesRef(GEO.asWkt(GeometryTestUtils.randomPoint())));
            List<Object> field2 = randomList(1, 10, () -> new BytesRef(GEO.asWkt(GeometryTestUtils.randomPoint())));
            var result = new ArrayList<>(field1);
            result.removeAll(field2);

            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field1, DataType.GEO_POINT, "field1"),
                    new TestCaseSupplier.TypedData(field2, DataType.GEO_POINT, "field2")
                ),
                "MvDifferenceBytesRefEvaluator[field1=Attribute[channel=0], field2=Attribute[channel=1]]",
                DataType.GEO_POINT,
                matchResult(result)
            );
        }));

        suppliers.add(new TestCaseSupplier(List.of(DataType.CARTESIAN_POINT, DataType.CARTESIAN_POINT), () -> {
            List<Object> field1 = randomList(1, 10, () -> new BytesRef(CARTESIAN.asWkt(ShapeTestUtils.randomPoint())));
            List<Object> field2 = randomList(1, 10, () -> new BytesRef(CARTESIAN.asWkt(ShapeTestUtils.randomPoint())));
            var result = new ArrayList<>(field1);
            result.removeAll(field2);

            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field1, DataType.CARTESIAN_POINT, "field1"),
                    new TestCaseSupplier.TypedData(field2, DataType.CARTESIAN_POINT, "field2")
                ),
                "MvDifferenceBytesRefEvaluator[field1=Attribute[channel=0], field2=Attribute[channel=1]]",
                DataType.CARTESIAN_POINT,
                matchResult(result)
            );
        }));

        suppliers.add(new TestCaseSupplier(List.of(DataType.GEO_SHAPE, DataType.GEO_SHAPE), () -> {
            var field1 = randomList(1, 3, () -> new BytesRef(GEO.asWkt(GeometryTestUtils.randomGeometry(randomBoolean(), 500))));
            var field2 = randomList(1, 3, () -> new BytesRef(GEO.asWkt(GeometryTestUtils.randomGeometry(randomBoolean(), 500))));
            var result = new ArrayList<>(field1);
            result.removeAll(field2);

            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field1, DataType.GEO_SHAPE, "field1"),
                    new TestCaseSupplier.TypedData(field2, DataType.GEO_SHAPE, "field2")
                ),
                "MvDifferenceBytesRefEvaluator[field1=Attribute[channel=0], field2=Attribute[channel=1]]",
                DataType.GEO_SHAPE,
                matchResult(result)
            );
        }));

        suppliers.add(new TestCaseSupplier(List.of(DataType.CARTESIAN_SHAPE, DataType.CARTESIAN_SHAPE), () -> {
            var field1 = randomList(1, 3, () -> new BytesRef(CARTESIAN.asWkt(ShapeTestUtils.randomGeometry(randomBoolean(), 500))));
            var field2 = randomList(1, 3, () -> new BytesRef(CARTESIAN.asWkt(ShapeTestUtils.randomGeometry(randomBoolean(), 500))));
            var result = new ArrayList<>(field1);
            result.removeAll(field2);

            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field1, DataType.CARTESIAN_SHAPE, "field1"),
                    new TestCaseSupplier.TypedData(field2, DataType.CARTESIAN_SHAPE, "field2")
                ),
                "MvDifferenceBytesRefEvaluator[field1=Attribute[channel=0], field2=Attribute[channel=1]]",
                DataType.CARTESIAN_SHAPE,
                matchResult(result)
            );
        }));
    }

    // Adjusted from static method anyNullIsNull in {@code AbstractFunctionTestCase#}
    // - changed logic to expect a Boolean as an outcome and alternative evaluators (IsNullEvaluator, ConstantTrue)
    // - constructor TestCase that's used has default access which we can't access, using public constructor variant as a replacement
    // - Added prefix to generated tests for my sanity.
    // - changed construction of new lists by copying them and updating the entries where necessary instead of regenerating
    protected static List<TestCaseSupplier> anyNullIsNull(
        List<TestCaseSupplier> testCaseSuppliers,
        ExpectedType expectedType,
        ExpectedEvaluatorToString evaluatorToString
    ) {
        List<TestCaseSupplier> suppliers = new ArrayList<>(testCaseSuppliers);

        /*
         * For each original test case, add as many copies as there were
         * arguments, replacing one of the arguments with null and keeping
         * the others.
         *
         * Also, if this was the first time we saw the signature we copy it
         * *again*, replacing the argument with null, but annotating the
         * argument’s type as `null` explicitly.
         */
        Set<List<DataType>> uniqueSignatures = new HashSet<>();
        for (TestCaseSupplier original : testCaseSuppliers) {
            boolean firstTimeSeenSignature = uniqueSignatures.add(original.types());
            for (int typeIndex = 0; typeIndex < original.types().size(); typeIndex++) {
                final int nullPosition = typeIndex;

                suppliers.add(new TestCaseSupplier("G1: " + original.name() + " null in " + nullPosition, original.types(), () -> {
                    TestCaseSupplier.TestCase originalTestCase = original.get();
                    List<TestCaseSupplier.TypedData> typeDataWithNull = new ArrayList<>(originalTestCase.getData());
                    var data = typeDataWithNull.get(nullPosition);
                    var nullData = data.withData(data.isMultiRow() ? Collections.singletonList(null) : null);
                    var nulledData = typeDataWithNull.set(nullPosition, nullData);
                    return new TestCaseSupplier.TestCase(
                        typeDataWithNull,
                        evaluatorToString.evaluatorToString(nullPosition, nulledData, originalTestCase.evaluatorToString()),
                        expectedType.expectedType(nullPosition, nulledData.type(), originalTestCase),
                        resultGivenANullParameter(nullPosition, originalTestCase)
                    );
                }));

                if (firstTimeSeenSignature) {
                    var typesWithNull = new ArrayList<>(original.types());
                    typesWithNull.set(nullPosition, DataType.NULL);
                    boolean newSignature = uniqueSignatures.add(typesWithNull);
                    if (newSignature) {
                        suppliers.add(
                            new TestCaseSupplier(
                                "G2: " + toSpaceSeparatedString(typesWithNull) + " null in " + nullPosition,
                                typesWithNull,
                                () -> {
                                    TestCaseSupplier.TestCase originalTestCase = original.get();
                                    var typeDataWithNull = new ArrayList<>(originalTestCase.getData());
                                    typeDataWithNull.set(
                                        nullPosition,
                                        typeDataWithNull.get(nullPosition).isMultiRow() ? MULTI_ROW_NULL : NULL
                                    );
                                    return new TestCaseSupplier.TestCase(
                                        typeDataWithNull,
                                        nullPosition == 0 ? equalTo("ConstantNull") : originalTestCase.evaluatorToString(),
                                        expectedType.expectedType(nullPosition, DataType.NULL, originalTestCase),
                                        resultGivenANullParameter(nullPosition, originalTestCase)
                                    );
                                }
                            )
                        );
                    }
                }
            }
        }

        return suppliers;
    }

    private static Matcher<?> resultGivenANullParameter(int nullPosition, TestCaseSupplier.TestCase originalTestCase) {
        switch (nullPosition) {
            case 0:
                return nullValue();
            default:
                if (originalTestCase.getData().get(0).type() == DataType.UNSIGNED_LONG) {
                    return matchResult(unsignedLongAsBigInteger(originalTestCase.getData().get(0)));
                }
                return matchResult(originalTestCase.getData().get(0));
        }
    }

    private static List<?> unsignedLongAsBigInteger(TestCaseSupplier.TypedData typedData) {
        var data = typedData.data();
        if (data instanceof List<?>) {
            return ((List<?>) data).stream().map(entry -> NumericUtils.unsignedLongAsBigInteger((long) entry)).toList();
        }
        throw new RuntimeException("unexpected type, class=" + data.getClass() + " value=" + data);
    }

    private static String toSpaceSeparatedString(ArrayList<DataType> typesWithNull) {
        return typesWithNull.stream().map(Objects::toString).collect(Collectors.joining(" "));
    }
}
