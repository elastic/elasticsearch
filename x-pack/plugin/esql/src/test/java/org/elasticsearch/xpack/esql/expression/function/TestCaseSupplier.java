/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geo.ShapeTestUtils;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.tree.Location;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.NumericUtils;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.versionfield.Version;
import org.hamcrest.Matcher;

import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;
import java.time.Period;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.DoubleFunction;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.LongFunction;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.CARTESIAN;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.GEO;
import static org.hamcrest.Matchers.equalTo;

/**
 * This class exists to give a human-readable string representation of the test case.
 */
public record TestCaseSupplier(String name, List<DataType> types, Supplier<TestCase> supplier)
    implements
        Supplier<TestCaseSupplier.TestCase> {

    public static final Source TEST_SOURCE = new Source(new Location(1, 0), "source");
    public static final Configuration TEST_CONFIGURATION = EsqlTestUtils.configuration(TEST_SOURCE.text());

    private static final Logger logger = LogManager.getLogger(TestCaseSupplier.class);

    /**
     * Build a test case named after the types it takes.
     */
    public TestCaseSupplier(List<DataType> types, Supplier<TestCase> supplier) {
        this(nameFromTypes(types), types, supplier);
    }

    public static String nameFromTypes(List<DataType> types) {
        return types.stream().map(t -> "<" + t.typeName() + ">").collect(Collectors.joining(", "));
    }

    /**
     * Build a name for the test case based on objects likely to describe it.
     */
    public static String nameFrom(List<Object> paramDescriptors) {
        return paramDescriptors.stream().map(p -> {
            if (p == null) {
                return "null";
            }
            if (p instanceof DataType t) {
                return "<" + t.typeName() + ">";
            }
            return p.toString();
        }).collect(Collectors.joining(", "));
    }

    public static List<TestCaseSupplier> stringCases(
        BinaryOperator<Object> expected,
        BiFunction<DataType, DataType, String> evaluatorToString,
        List<String> warnings,
        DataType expectedType
    ) {
        List<TypedDataSupplier> lhsSuppliers = new ArrayList<>();
        List<TypedDataSupplier> rhsSuppliers = new ArrayList<>();
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        for (DataType type : DataType.stringTypes()) {
            lhsSuppliers.addAll(stringCases(type));
            rhsSuppliers.addAll(stringCases(type));
            casesCrossProduct(
                expected,
                lhsSuppliers,
                rhsSuppliers,
                (lhs, rhs) -> equalTo(evaluatorToString.apply(lhs, rhs)),
                (lhs, rhs) -> warnings,
                suppliers,
                expectedType,
                true
            );
        }
        return suppliers;
    }

    @Override
    public TestCase get() {
        TestCase supplied = supplier.get();
        if (types.size() != supplied.getData().size()) {
            throw new IllegalStateException(name + ": type/data size mismatch " + types.size() + "/" + supplied.getData().size());
        }
        for (int i = 0; i < types.size(); i++) {
            if (supplied.getData().get(i).type() != types.get(i)) {
                throw new IllegalStateException(
                    name + ": supplier/data type mismatch " + supplied.getData().get(i).type() + "/" + types.get(i)
                );
            }
        }
        return supplied;
    }

    @Override
    public String toString() {
        return name;
    }

    /**
     * Generate positive test cases for unary functions that operate on an {@code numeric}
     * fields by casting them to {@link DataType#DOUBLE}s.
     */
    public static List<TestCaseSupplier> forUnaryCastingToDouble(
        String name,
        String argName,
        UnaryOperator<Double> expected,
        Double min,
        Double max,
        List<String> warnings
    ) {
        String read = "Attribute[channel=0]";
        String eval = name + "[" + argName + "=";
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        forUnaryInt(
            suppliers,
            eval + castToDoubleEvaluator(read, DataType.INTEGER) + "]",
            DataType.DOUBLE,
            i -> expected.apply(Double.valueOf(i)),
            min.intValue(),
            max.intValue(),
            warnings
        );
        forUnaryLong(
            suppliers,
            eval + castToDoubleEvaluator(read, DataType.LONG) + "]",
            DataType.DOUBLE,
            i -> expected.apply(Double.valueOf(i)),
            min.longValue(),
            max.longValue(),
            warnings
        );
        forUnaryUnsignedLong(
            suppliers,
            eval + castToDoubleEvaluator(read, DataType.UNSIGNED_LONG) + "]",
            DataType.DOUBLE,
            ul -> expected.apply(ul.doubleValue()),
            BigInteger.valueOf((int) Math.ceil(min)),
            BigInteger.valueOf((int) Math.floor(max)),
            warnings
        );
        forUnaryDouble(suppliers, eval + read + "]", DataType.DOUBLE, expected::apply, min, max, warnings);
        return suppliers;
    }

    /**
     * Generate positive test cases for binary functions that operate on an {@code numeric}
     * fields by casting them to {@link DataType#DOUBLE}s.
     */
    public static List<TestCaseSupplier> forBinaryCastingToDouble(
        String name,
        String lhsName,
        String rhsName,
        BinaryOperator<Double> expected,
        Double lhsMin,
        Double lhsMax,
        Double rhsMin,
        Double rhsMax,
        List<String> warnings
    ) {
        List<TypedDataSupplier> lhsSuppliers = castToDoubleSuppliersFromRange(lhsMin, lhsMax);
        List<TypedDataSupplier> rhsSuppliers = castToDoubleSuppliersFromRange(rhsMin, rhsMax);
        return forBinaryCastingToDouble(name, lhsName, rhsName, expected, lhsSuppliers, rhsSuppliers, warnings);
    }

    public static List<TestCaseSupplier> forBinaryCastingToDouble(
        String name,
        String lhsName,
        String rhsName,
        BinaryOperator<Double> expected,
        List<TypedDataSupplier> lhsSuppliers,
        List<TypedDataSupplier> rhsSuppliers,
        List<String> warnings
    ) {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        casesCrossProduct(
            (l, r) -> expected.apply(((Number) l).doubleValue(), ((Number) r).doubleValue()),
            lhsSuppliers,
            rhsSuppliers,
            (lhsType, rhsType) -> equalTo(
                name
                    + "["
                    + lhsName
                    + "="
                    + castToDoubleEvaluator("Attribute[channel=0]", lhsType)
                    + ", "
                    + rhsName
                    + "="
                    + castToDoubleEvaluator("Attribute[channel=1]", rhsType)
                    + "]"
            ),
            (lhs, rhs) -> warnings,
            suppliers,
            DataType.DOUBLE,
            false
        );
        return suppliers;
    }

    public static void casesCrossProduct(
        BinaryOperator<Object> expected,
        List<TypedDataSupplier> lhsSuppliers,
        List<TypedDataSupplier> rhsSuppliers,
        BiFunction<DataType, DataType, Matcher<String>> evaluatorToString,
        BiFunction<TypedData, TypedData, List<String>> warnings,
        List<TestCaseSupplier> suppliers,
        DataType expectedType,
        boolean symmetric
    ) {
        for (TypedDataSupplier lhsSupplier : lhsSuppliers) {
            for (TypedDataSupplier rhsSupplier : rhsSuppliers) {
                suppliers.add(testCaseSupplier(lhsSupplier, rhsSupplier, evaluatorToString, expectedType, expected, warnings));
                if (symmetric) {
                    suppliers.add(testCaseSupplier(rhsSupplier, lhsSupplier, evaluatorToString, expectedType, expected, warnings));
                }
            }
        }
    }

    public static TestCaseSupplier testCaseSupplier(
        TypedDataSupplier lhsSupplier,
        TypedDataSupplier rhsSupplier,
        BiFunction<DataType, DataType, Matcher<String>> evaluatorToString,
        DataType expectedType,
        BinaryOperator<Object> expectedValue
    ) {
        return testCaseSupplier(lhsSupplier, rhsSupplier, evaluatorToString, expectedType, expectedValue, (lhs, rhs) -> List.of());
    }

    private static TestCaseSupplier testCaseSupplier(
        TypedDataSupplier lhsSupplier,
        TypedDataSupplier rhsSupplier,
        BiFunction<DataType, DataType, Matcher<String>> evaluatorToString,
        DataType expectedType,
        BinaryOperator<Object> expectedValue,
        BiFunction<TypedData, TypedData, List<String>> warnings
    ) {
        String caseName = lhsSupplier.name() + ", " + rhsSupplier.name();
        return new TestCaseSupplier(caseName, List.of(lhsSupplier.type(), rhsSupplier.type()), () -> {
            TypedData lhsTyped = lhsSupplier.get();
            TypedData rhsTyped = rhsSupplier.get();
            TestCase testCase = new TestCase(
                List.of(lhsTyped, rhsTyped),
                evaluatorToString.apply(lhsSupplier.type(), rhsSupplier.type()),
                expectedType,
                equalTo(expectedValue.apply(lhsTyped.getValue(), rhsTyped.getValue()))
            );
            for (String warning : warnings.apply(lhsTyped, rhsTyped)) {
                testCase = testCase.withWarning(warning);
            }
            if (DataType.isRepresentable(expectedType) == false) {
                testCase = testCase.withoutEvaluator();
            }
            return testCase;
        });
    }

    public static List<TypedDataSupplier> castToDoubleSuppliersFromRange(Double Min, Double Max) {
        List<TypedDataSupplier> suppliers = new ArrayList<>();
        suppliers.addAll(intCases(Min.intValue(), Max.intValue(), true));
        suppliers.addAll(longCases(Min.longValue(), Max.longValue(), true));
        suppliers.addAll(ulongCases(BigInteger.valueOf((long) Math.ceil(Min)), BigInteger.valueOf((long) Math.floor(Max)), true));
        suppliers.addAll(doubleCases(Min, Max, true));
        return suppliers;
    }

    public record NumericTypeTestConfig<T>(Number min, Number max, BiFunction<Number, Number, T> expected, String evaluatorName) {}

    public record NumericTypeTestConfigs<T>(
        NumericTypeTestConfig<T> intStuff,
        NumericTypeTestConfig<T> longStuff,
        NumericTypeTestConfig<T> doubleStuff
    ) {
        public NumericTypeTestConfig<T> get(DataType type) {
            if (type == DataType.INTEGER) {
                return intStuff;
            }
            if (type == DataType.LONG) {
                return longStuff;
            }
            if (type == DataType.DOUBLE) {
                return doubleStuff;
            }
            throw new IllegalArgumentException("bogus numeric type [" + type + "]");
        }
    }

    public static DataType widen(DataType lhs, DataType rhs) {
        if (lhs == rhs) {
            return lhs;
        }
        if (lhs == DataType.DOUBLE || rhs == DataType.DOUBLE) {
            return DataType.DOUBLE;
        }
        if (lhs == DataType.LONG || rhs == DataType.LONG) {
            return DataType.LONG;
        }
        throw new IllegalArgumentException("Invalid numeric widening lhs: [" + lhs + "] rhs: [" + rhs + "]");
    }

    public static List<TypedDataSupplier> getSuppliersForNumericType(DataType type, Number min, Number max, boolean includeZero) {
        if (type == DataType.INTEGER) {
            return intCases(NumericUtils.saturatingIntValue(min), NumericUtils.saturatingIntValue(max), includeZero);
        }
        if (type == DataType.LONG) {
            return longCases(min.longValue(), max.longValue(), includeZero);
        }
        if (type == DataType.UNSIGNED_LONG) {
            return ulongCases(
                min instanceof BigInteger ? (BigInteger) min : BigInteger.valueOf(Math.max(min.longValue(), 0L)),
                max instanceof BigInteger ? (BigInteger) max : BigInteger.valueOf(Math.max(max.longValue(), 0L)),
                includeZero
            );
        }
        if (type == DataType.DOUBLE) {
            return doubleCases(min.doubleValue(), max.doubleValue(), includeZero);
        }
        throw new IllegalArgumentException("bogus numeric type [" + type + "]");
    }

    public static List<TestCaseSupplier> forBinaryComparisonWithWidening(
        NumericTypeTestConfigs<Boolean> typeStuff,
        String lhsName,
        String rhsName,
        BiFunction<TypedData, TypedData, List<String>> warnings,
        boolean allowRhsZero
    ) {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        List<DataType> numericTypes = List.of(DataType.INTEGER, DataType.LONG, DataType.DOUBLE);

        for (DataType lhsType : numericTypes) {
            for (DataType rhsType : numericTypes) {
                DataType expected = widen(lhsType, rhsType);
                NumericTypeTestConfig<Boolean> expectedTypeStuff = typeStuff.get(expected);
                BiFunction<DataType, DataType, String> evaluatorToString = (lhs, rhs) -> expectedTypeStuff.evaluatorName()
                    + "["
                    + lhsName
                    + "="
                    + getCastEvaluator("Attribute[channel=0]", lhs, expected)
                    + ", "
                    + rhsName
                    + "="
                    + getCastEvaluator("Attribute[channel=1]", rhs, expected)
                    + "]";
                casesCrossProduct(
                    (l, r) -> expectedTypeStuff.expected().apply((Number) l, (Number) r),
                    getSuppliersForNumericType(lhsType, expectedTypeStuff.min(), expectedTypeStuff.max(), allowRhsZero),
                    getSuppliersForNumericType(rhsType, expectedTypeStuff.min(), expectedTypeStuff.max(), allowRhsZero),
                    (lhs, rhs) -> equalTo(evaluatorToString.apply(lhs, rhs)),
                    warnings,
                    suppliers,
                    DataType.BOOLEAN,
                    true
                );
            }
        }
        return suppliers;
    }

    public static List<TestCaseSupplier> forBinaryWithWidening(
        NumericTypeTestConfigs<Number> typeStuff,
        String lhsName,
        String rhsName,
        BiFunction<TypedData, TypedData, List<String>> warnings,
        boolean allowRhsZero
    ) {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        List<DataType> numericTypes = List.of(DataType.INTEGER, DataType.LONG, DataType.DOUBLE);

        for (DataType lhsType : numericTypes) {
            for (DataType rhsType : numericTypes) {
                DataType expected = widen(lhsType, rhsType);
                NumericTypeTestConfig<Number> expectedTypeStuff = typeStuff.get(expected);
                BiFunction<DataType, DataType, Matcher<String>> evaluatorToString = (lhs, rhs) -> equalTo(
                    expectedTypeStuff.evaluatorName()
                        + "["
                        + lhsName
                        + "="
                        + getCastEvaluator("Attribute[channel=0]", lhs, expected)
                        + ", "
                        + rhsName
                        + "="
                        + getCastEvaluator("Attribute[channel=1]", rhs, expected)
                        + "]"
                );
                casesCrossProduct(
                    (l, r) -> expectedTypeStuff.expected().apply((Number) l, (Number) r),
                    getSuppliersForNumericType(lhsType, expectedTypeStuff.min(), expectedTypeStuff.max(), true),
                    getSuppliersForNumericType(rhsType, expectedTypeStuff.min(), expectedTypeStuff.max(), allowRhsZero),
                    evaluatorToString,
                    warnings,
                    suppliers,
                    expected,
                    false
                );
            }
        }

        return suppliers;
    }

    public static List<TestCaseSupplier> forBinaryNotCasting(
        String name,
        String lhsName,
        String rhsName,
        BinaryOperator<Object> expected,
        DataType expectedType,
        List<TypedDataSupplier> lhsSuppliers,
        List<TypedDataSupplier> rhsSuppliers,
        List<String> warnings,
        boolean symmetric
    ) {
        return forBinaryNotCasting(
            expected,
            expectedType,
            lhsSuppliers,
            rhsSuppliers,
            equalTo(name + "[" + lhsName + "=Attribute[channel=0], " + rhsName + "=Attribute[channel=1]]"),
            (lhs, rhs) -> warnings,
            symmetric
        );
    }

    public static List<TestCaseSupplier> forBinaryNotCasting(
        BinaryOperator<Object> expected,
        DataType expectedType,
        List<TypedDataSupplier> lhsSuppliers,
        List<TypedDataSupplier> rhsSuppliers,
        Matcher<String> evaluatorToString,
        BiFunction<TypedData, TypedData, List<String>> warnings,
        boolean symmetric
    ) {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        casesCrossProduct(
            expected,
            lhsSuppliers,
            rhsSuppliers,
            (lhsType, rhsType) -> evaluatorToString,
            warnings,
            suppliers,
            expectedType,
            symmetric
        );
        return suppliers;
    }

    /**
     * Generate positive test cases for a unary function operating on an {@link DataType#INTEGER}.
     */
    public static void forUnaryInt(
        List<TestCaseSupplier> suppliers,
        String expectedEvaluatorToString,
        DataType expectedType,
        IntFunction<Object> expectedValue,
        int lowerBound,
        int upperBound,
        Function<Number, List<String>> expectedWarnings
    ) {
        unaryNumeric(
            suppliers,
            expectedEvaluatorToString,
            intCases(lowerBound, upperBound, true),
            expectedType,
            n -> expectedValue.apply(n.intValue()),
            n -> expectedWarnings.apply(n.intValue())
        );
    }

    public static void forUnaryInt(
        List<TestCaseSupplier> suppliers,
        String expectedEvaluatorToString,
        DataType expectedType,
        IntFunction<Object> expectedValue,
        int lowerBound,
        int upperBound,
        List<String> warnings
    ) {
        forUnaryInt(suppliers, expectedEvaluatorToString, expectedType, expectedValue, lowerBound, upperBound, unused -> warnings);
    }

    /**
     * Generate positive test cases for a unary function operating on an {@link DataType#LONG}.
     */
    public static void forUnaryLong(
        List<TestCaseSupplier> suppliers,
        String expectedEvaluatorToString,
        DataType expectedType,
        LongFunction<Object> expectedValue,
        long lowerBound,
        long upperBound,
        Function<Number, List<String>> expectedWarnings
    ) {
        unaryNumeric(
            suppliers,
            expectedEvaluatorToString,
            longCases(lowerBound, upperBound, true),
            expectedType,
            n -> expectedValue.apply(n.longValue()),
            expectedWarnings
        );
    }

    public static void forUnaryLong(
        List<TestCaseSupplier> suppliers,
        String expectedEvaluatorToString,
        DataType expectedType,
        LongFunction<Object> expectedValue,
        long lowerBound,
        long upperBound,
        List<String> warnings
    ) {
        forUnaryLong(suppliers, expectedEvaluatorToString, expectedType, expectedValue, lowerBound, upperBound, unused -> warnings);
    }

    /**
     * Generate positive test cases for a unary function operating on an {@link DataType#UNSIGNED_LONG}.
     */
    public static void forUnaryUnsignedLong(
        List<TestCaseSupplier> suppliers,
        String expectedEvaluatorToString,
        DataType expectedType,
        Function<BigInteger, Object> expectedValue,
        BigInteger lowerBound,
        BigInteger upperBound,
        Function<BigInteger, List<String>> expectedWarnings
    ) {
        unaryNumeric(
            suppliers,
            expectedEvaluatorToString,
            ulongCases(lowerBound, upperBound, true),
            expectedType,
            n -> expectedValue.apply((BigInteger) n),
            n -> expectedWarnings.apply((BigInteger) n)
        );
    }

    public static void forUnaryUnsignedLong(
        List<TestCaseSupplier> suppliers,
        String expectedEvaluatorToString,
        DataType expectedType,
        Function<BigInteger, Object> expectedValue,
        BigInteger lowerBound,
        BigInteger upperBound,
        List<String> warnings
    ) {
        forUnaryUnsignedLong(suppliers, expectedEvaluatorToString, expectedType, expectedValue, lowerBound, upperBound, unused -> warnings);
    }

    /**
     * Generate positive test cases for a unary function operating on an {@link DataType#DOUBLE}.
     */
    public static void forUnaryDouble(
        List<TestCaseSupplier> suppliers,
        String expectedEvaluatorToString,
        DataType expectedType,
        DoubleFunction<Object> expectedValue,
        double lowerBound,
        double upperBound,
        List<String> warnings
    ) {
        forUnaryDouble(suppliers, expectedEvaluatorToString, expectedType, expectedValue, lowerBound, upperBound, unused -> warnings);
    }

    public static void forUnaryDouble(
        List<TestCaseSupplier> suppliers,
        String expectedEvaluatorToString,
        DataType expectedType,
        DoubleFunction<Object> expectedValue,
        double lowerBound,
        double upperBound,
        DoubleFunction<List<String>> expectedWarnings
    ) {
        unaryNumeric(
            suppliers,
            expectedEvaluatorToString,
            doubleCases(lowerBound, upperBound, true),
            expectedType,
            n -> expectedValue.apply(n.doubleValue()),
            n -> expectedWarnings.apply(n.doubleValue())
        );
    }

    /**
     * Generate positive test cases for a unary function operating on an {@link DataType#BOOLEAN}.
     */
    public static void forUnaryBoolean(
        List<TestCaseSupplier> suppliers,
        String expectedEvaluatorToString,
        DataType expectedType,
        Function<Boolean, Object> expectedValue,
        List<String> warnings
    ) {
        unary(suppliers, expectedEvaluatorToString, booleanCases(), expectedType, v -> expectedValue.apply((Boolean) v), warnings);
    }

    /**
     * Generate positive test cases for a unary function operating on an {@link DataType#GEO_POINT}.
     */
    public static void forUnaryGeoPoint(
        List<TestCaseSupplier> suppliers,
        String expectedEvaluatorToString,
        DataType expectedType,
        Function<BytesRef, Object> expectedValue,
        List<String> warnings
    ) {
        unary(suppliers, expectedEvaluatorToString, geoPointCases(), expectedType, n -> expectedValue.apply((BytesRef) n), warnings);
    }

    /**
     * Generate positive test cases for a unary function operating on an {@link DataType#CARTESIAN_POINT}.
     */
    public static void forUnaryCartesianPoint(
        List<TestCaseSupplier> suppliers,
        String expectedEvaluatorToString,
        DataType expectedType,
        Function<BytesRef, Object> expectedValue,
        List<String> warnings
    ) {
        unary(suppliers, expectedEvaluatorToString, cartesianPointCases(), expectedType, n -> expectedValue.apply((BytesRef) n), warnings);
    }

    /**
     * Generate positive test cases for a unary function operating on an {@link DataType#GEO_SHAPE}.
     */
    public static void forUnaryGeoShape(
        List<TestCaseSupplier> suppliers,
        String expectedEvaluatorToString,
        DataType expectedType,
        Function<BytesRef, Object> expectedValue,
        List<String> warnings
    ) {
        unary(suppliers, expectedEvaluatorToString, geoShapeCases(), expectedType, n -> expectedValue.apply((BytesRef) n), warnings);
    }

    /**
     * Generate positive test cases for a unary function operating on an {@link DataType#CARTESIAN_SHAPE}.
     */
    public static void forUnaryCartesianShape(
        List<TestCaseSupplier> suppliers,
        String expectedEvaluatorToString,
        DataType expectedType,
        Function<BytesRef, Object> expectedValue,
        List<String> warnings
    ) {
        unary(suppliers, expectedEvaluatorToString, cartesianShapeCases(), expectedType, n -> expectedValue.apply((BytesRef) n), warnings);
    }

    /**
     * Generate positive test cases for a unary function operating on an {@link DataType#IP}.
     */
    public static void forUnaryIp(
        List<TestCaseSupplier> suppliers,
        String expectedEvaluatorToString,
        DataType expectedType,
        Function<BytesRef, Object> expectedValue,
        List<String> warnings
    ) {
        unary(suppliers, expectedEvaluatorToString, ipCases(), expectedType, v -> expectedValue.apply((BytesRef) v), warnings);
    }

    /**
     * Generate positive test cases for a unary function operating on an {@link DataType#KEYWORD} and {@link DataType#TEXT}.
     */
    public static void forUnaryStrings(
        List<TestCaseSupplier> suppliers,
        String expectedEvaluatorToString,
        DataType expectedType,
        Function<BytesRef, Object> expectedValue,
        Function<BytesRef, List<String>> expectedWarnings
    ) {
        for (DataType type : DataType.stringTypes()) {
            unary(
                suppliers,
                expectedEvaluatorToString,
                stringCases(type),
                expectedType,
                v -> expectedValue.apply((BytesRef) v),
                v -> expectedWarnings.apply((BytesRef) v)
            );
        }
    }

    public static void forUnaryStrings(
        List<TestCaseSupplier> suppliers,
        String expectedEvaluatorToString,
        DataType expectedType,
        Function<BytesRef, Object> expectedValue,
        List<String> warnings
    ) {
        forUnaryStrings(suppliers, expectedEvaluatorToString, expectedType, expectedValue, unused -> warnings);
    }

    /**
     * Generate positive test cases for a unary function operating on an {@link DataType#VERSION}.
     */
    public static void forUnaryVersion(
        List<TestCaseSupplier> suppliers,
        String expectedEvaluatorToString,
        DataType expectedType,
        Function<Version, Object> expectedValue,
        List<String> warnings
    ) {
        unary(
            suppliers,
            expectedEvaluatorToString,
            versionCases(""),
            expectedType,
            v -> expectedValue.apply(new Version((BytesRef) v)),
            warnings
        );
    }

    private static void unaryNumeric(
        List<TestCaseSupplier> suppliers,
        String expectedEvaluatorToString,
        List<TypedDataSupplier> valueSuppliers,
        DataType expectedOutputType,
        Function<Number, Object> expectedValue,
        Function<Number, List<String>> expectedWarnings
    ) {
        unary(
            suppliers,
            expectedEvaluatorToString,
            valueSuppliers,
            expectedOutputType,
            v -> expectedValue.apply((Number) v),
            v -> expectedWarnings.apply((Number) v)
        );
    }

    private static void unaryNumeric(
        List<TestCaseSupplier> suppliers,
        String expectedEvaluatorToString,
        List<TypedDataSupplier> valueSuppliers,
        DataType expectedOutputType,
        Function<Number, Object> expected,
        List<String> warnings
    ) {
        unaryNumeric(suppliers, expectedEvaluatorToString, valueSuppliers, expectedOutputType, expected, unused -> warnings);
    }

    public static void unary(
        List<TestCaseSupplier> suppliers,
        String expectedEvaluatorToString,
        List<TypedDataSupplier> valueSuppliers,
        DataType expectedOutputType,
        Function<Object, Object> expectedValue,
        Function<Object, List<String>> expectedWarnings
    ) {
        for (TypedDataSupplier supplier : valueSuppliers) {
            suppliers.add(new TestCaseSupplier(supplier.name(), List.of(supplier.type()), () -> {
                TypedData typed = supplier.get();
                Object value = typed.getValue();
                logger.info("Value is " + value + " of type " + value.getClass());
                logger.info("expectedValue is " + expectedValue.apply(value));
                TestCase testCase = new TestCase(
                    List.of(typed),
                    expectedEvaluatorToString,
                    expectedOutputType,
                    equalTo(expectedValue.apply(value))
                );
                for (String warning : expectedWarnings.apply(value)) {
                    testCase = testCase.withWarning(warning);
                }
                return testCase;
            }));
        }

    }

    public static void unary(
        List<TestCaseSupplier> suppliers,
        String expectedEvaluatorToString,
        List<TypedDataSupplier> valueSuppliers,
        DataType expectedOutputType,
        Function<Object, Object> expected,
        List<String> warnings
    ) {
        unary(suppliers, expectedEvaluatorToString, valueSuppliers, expectedOutputType, expected, unused -> warnings);
    }

    /**
     * Generate cases for {@link DataType#INTEGER}.
     * <p>
     *     For multi-row parameters, see {@link MultiRowTestCaseSupplier#intCases}.
     * </p>
     */
    public static List<TypedDataSupplier> intCases(int min, int max, boolean includeZero) {
        List<TypedDataSupplier> cases = new ArrayList<>();
        if (0 <= max && 0 >= min && includeZero) {
            cases.add(new TypedDataSupplier("<0 int>", () -> 0, DataType.INTEGER));
        }

        int lower = Math.max(min, 1);
        int upper = Math.min(max, Integer.MAX_VALUE);
        if (lower < upper) {
            cases.add(new TypedDataSupplier("<positive int>", () -> ESTestCase.randomIntBetween(lower, upper), DataType.INTEGER));
        } else if (lower == upper) {
            cases.add(new TypedDataSupplier("<" + lower + " int>", () -> lower, DataType.INTEGER));
        }

        int lower1 = Math.max(min, Integer.MIN_VALUE);
        int upper1 = Math.min(max, -1);
        if (lower1 < upper1) {
            cases.add(new TypedDataSupplier("<negative int>", () -> ESTestCase.randomIntBetween(lower1, upper1), DataType.INTEGER));
        } else if (lower1 == upper1) {
            cases.add(new TypedDataSupplier("<" + lower1 + " int>", () -> lower1, DataType.INTEGER));
        }
        return cases;
    }

    /**
     * Generate cases for {@link DataType#LONG}.
     * <p>
     *     For multi-row parameters, see {@link MultiRowTestCaseSupplier#longCases}.
     * </p>
     */
    public static List<TypedDataSupplier> longCases(long min, long max, boolean includeZero) {
        List<TypedDataSupplier> cases = new ArrayList<>();
        if (0L <= max && 0L >= min && includeZero) {
            cases.add(new TypedDataSupplier("<0 long>", () -> 0L, DataType.LONG));
        }

        long lower = Math.max(min, 1);
        long upper = Math.min(max, Long.MAX_VALUE);
        if (lower < upper) {
            cases.add(new TypedDataSupplier("<positive long>", () -> ESTestCase.randomLongBetween(lower, upper), DataType.LONG));
        } else if (lower == upper) {
            cases.add(new TypedDataSupplier("<" + lower + " long>", () -> lower, DataType.LONG));
        }

        long lower1 = Math.max(min, Long.MIN_VALUE);
        long upper1 = Math.min(max, -1);
        if (lower1 < upper1) {
            cases.add(new TypedDataSupplier("<negative long>", () -> ESTestCase.randomLongBetween(lower1, upper1), DataType.LONG));
        } else if (lower1 == upper1) {
            cases.add(new TypedDataSupplier("<" + lower1 + " long>", () -> lower1, DataType.LONG));
        }

        return cases;
    }

    /**
     * Generate cases for {@link DataType#UNSIGNED_LONG}.
     * <p>
     *     For multi-row parameters, see {@link MultiRowTestCaseSupplier#ulongCases}.
     * </p>
     */
    public static List<TypedDataSupplier> ulongCases(BigInteger min, BigInteger max, boolean includeZero) {
        List<TypedDataSupplier> cases = new ArrayList<>();

        // Zero
        if (BigInteger.ZERO.compareTo(max) <= 0 && BigInteger.ZERO.compareTo(min) >= 0 && includeZero) {
            cases.add(new TypedDataSupplier("<0 unsigned long>", () -> BigInteger.ZERO, DataType.UNSIGNED_LONG));
        }

        // small values, less than Long.MAX_VALUE
        BigInteger lower1 = min.max(BigInteger.ONE);
        BigInteger upper1 = max.min(BigInteger.valueOf(Long.MAX_VALUE));
        if (lower1.compareTo(upper1) < 0) {
            cases.add(
                new TypedDataSupplier(
                    "<small unsigned long>",
                    () -> ESTestCase.randomUnsignedLongBetween(lower1, upper1),
                    DataType.UNSIGNED_LONG
                )
            );
        } else if (lower1.compareTo(upper1) == 0) {
            cases.add(new TypedDataSupplier("<small unsigned long>", () -> lower1, DataType.UNSIGNED_LONG));
        }

        // Big values, greater than Long.MAX_VALUE
        BigInteger lower2 = min.max(BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE));
        BigInteger upper2 = max.min(ESTestCase.UNSIGNED_LONG_MAX);
        if (lower2.compareTo(upper2) < 0) {
            cases.add(
                new TypedDataSupplier(
                    "<big unsigned long>",
                    () -> ESTestCase.randomUnsignedLongBetween(lower2, upper2),
                    DataType.UNSIGNED_LONG
                )
            );
        } else if (lower2.compareTo(upper2) == 0) {
            cases.add(new TypedDataSupplier("<big unsigned long>", () -> lower2, DataType.UNSIGNED_LONG));
        }
        return cases;
    }

    /**
     * Generate cases for {@link DataType#DOUBLE}.
     * <p>
     *     For multi-row parameters, see {@link MultiRowTestCaseSupplier#doubleCases}.
     * </p>
     */
    public static List<TypedDataSupplier> doubleCases(double min, double max, boolean includeZero) {
        List<TypedDataSupplier> cases = new ArrayList<>();

        // Zeros
        if (0d <= max && 0d >= min && includeZero) {
            cases.add(new TypedDataSupplier("<0 double>", () -> 0.0d, DataType.DOUBLE));
            cases.add(new TypedDataSupplier("<-0 double>", () -> -0.0d, DataType.DOUBLE));
        }

        // Positive small double
        double lower1 = Math.max(0d, min);
        double upper1 = Math.min(1d, max);
        if (lower1 < upper1) {
            cases.add(
                new TypedDataSupplier(
                    "<small positive double>",
                    () -> ESTestCase.randomDoubleBetween(lower1, upper1, true),
                    DataType.DOUBLE
                )
            );
        } else if (lower1 == upper1) {
            cases.add(new TypedDataSupplier("<small positive double>", () -> lower1, DataType.DOUBLE));
        }

        // Negative small double
        double lower2 = Math.max(-1d, min);
        double upper2 = Math.min(0d, max);
        if (lower2 < upper2) {
            cases.add(
                new TypedDataSupplier(
                    "<small negative double>",
                    () -> ESTestCase.randomDoubleBetween(lower2, upper2, true),
                    DataType.DOUBLE
                )
            );
        } else if (lower2 == upper2) {
            cases.add(new TypedDataSupplier("<small negative double>", () -> lower2, DataType.DOUBLE));
        }

        // Positive big double
        double lower3 = Math.max(1d, min); // start at 1 (inclusive) because the density of values between 0 and 1 is very high
        double upper3 = Math.min(Double.MAX_VALUE, max);
        if (lower3 < upper3) {
            cases.add(
                new TypedDataSupplier("<big positive double>", () -> ESTestCase.randomDoubleBetween(lower3, upper3, true), DataType.DOUBLE)
            );
        } else if (lower3 == upper3) {
            cases.add(new TypedDataSupplier("<big positive double>", () -> lower3, DataType.DOUBLE));
        }

        // Negative big double
        // note: Double.MIN_VALUE is the smallest non-zero positive double, not the smallest non-infinite negative double.
        double lower4 = Math.max(-Double.MAX_VALUE, min);
        double upper4 = Math.min(-1, max); // because again, the interval from -1 to 0 is very high density
        if (lower4 < upper4) {
            cases.add(
                new TypedDataSupplier("<big negative double>", () -> ESTestCase.randomDoubleBetween(lower4, upper4, true), DataType.DOUBLE)
            );
        } else if (lower4 == upper4) {
            cases.add(new TypedDataSupplier("<big negative double>", () -> lower4, DataType.DOUBLE));
        }
        return cases;
    }

    /**
     * Generate cases for {@link DataType#BOOLEAN}.
     * <p>
     *     For multi-row parameters, see {@link MultiRowTestCaseSupplier#booleanCases}.
     * </p>
     */
    public static List<TypedDataSupplier> booleanCases() {
        return List.of(
            new TypedDataSupplier("<true>", () -> true, DataType.BOOLEAN),
            new TypedDataSupplier("<false>", () -> false, DataType.BOOLEAN)
        );
    }

    /**
     * Generate cases for {@link DataType#DATETIME}.
     * <p>
     *     For multi-row parameters, see {@link MultiRowTestCaseSupplier#dateCases}.
     * </p>
     */
    public static List<TypedDataSupplier> dateCases() {
        return dateCases(Long.MIN_VALUE, Long.MAX_VALUE);
    }

    /**
     * Generate cases for {@link DataType#DATETIME}.
     * <p>
     *     For multi-row parameters, see {@link MultiRowTestCaseSupplier#dateCases}.
     * </p>
     * Helper function for if you want to specify your min and max range as dates instead of longs.
     */
    public static List<TypedDataSupplier> dateCases(Instant min, Instant max) {
        return dateCases(min.toEpochMilli(), max.toEpochMilli());
    }

    /**
     * Generate cases for {@link DataType#DATETIME}.
     * <p>
     *     For multi-row parameters, see {@link MultiRowTestCaseSupplier#dateCases}.
     * </p>
     */
    public static List<TypedDataSupplier> dateCases(long min, long max) {
        List<TypedDataSupplier> cases = new ArrayList<>();
        if (min <= 0 && max >= 0) {
            cases.add(new TypedDataSupplier("<1970-01-01T00:00:00Z>", () -> 0L, DataType.DATETIME));
        }

        // 1970-01-01T00:00:00Z - 2286-11-20T17:46:40Z
        long lower1 = Math.max(min, 0);
        long upper1 = Math.min(max, 10 * (long) 10e11);
        if (lower1 < upper1) {
            cases.add(new TypedDataSupplier("<date>", () -> ESTestCase.randomLongBetween(lower1, upper1), DataType.DATETIME));
        }

        // 2286-11-20T17:46:40Z - +292278994-08-17T07:12:55.807Z
        long lower2 = Math.max(min, 10 * (long) 10e11);
        long upper2 = Math.min(max, Long.MAX_VALUE);
        if (lower2 < upper2) {
            cases.add(new TypedDataSupplier("<far future date>", () -> ESTestCase.randomLongBetween(lower2, upper2), DataType.DATETIME));
        }

        // very close to +292278994-08-17T07:12:55.807Z, the maximum supported millis since epoch
        long lower3 = Math.max(min, Long.MAX_VALUE / 100 * 99);
        long upper3 = Math.min(max, Long.MAX_VALUE);
        if (lower3 < upper3) {
            cases.add(
                new TypedDataSupplier("<near the end of time>", () -> ESTestCase.randomLongBetween(lower3, upper3), DataType.DATETIME)
            );
        }

        return cases;
    }

    /**
     *
     * @return randomized valid date formats
     */
    public static List<TypedDataSupplier> dateFormatCases() {
        return List.of(
            new TypedDataSupplier("<format as KEYWORD>", () -> new BytesRef(ESTestCase.randomDateFormatterPattern()), DataType.KEYWORD),
            new TypedDataSupplier("<format as TEXT>", () -> new BytesRef(ESTestCase.randomDateFormatterPattern()), DataType.TEXT),
            new TypedDataSupplier("<format as KEYWORD>", () -> new BytesRef("yyyy"), DataType.KEYWORD),
            new TypedDataSupplier("<format as TEXT>", () -> new BytesRef("yyyy"), DataType.TEXT)
        );
    }

    /**
     * Generate cases for {@link DataType#DATE_NANOS}.
     *
     */
    public static List<TypedDataSupplier> dateNanosCases() {
        return dateNanosCases(Instant.EPOCH, DateUtils.MAX_NANOSECOND_INSTANT);
    }

    /**
     * Generate cases for {@link DataType#DATE_NANOS}.
     *
     */
    public static List<TypedDataSupplier> dateNanosCases(Instant minValue, Instant maxValue) {
        // maximum nanosecond date in ES is 2262-04-11T23:47:16.854775807Z
        Instant twentyOneHundred = Instant.parse("2100-01-01T00:00:00Z");
        Instant twentyTwoHundred = Instant.parse("2200-01-01T00:00:00Z");
        Instant twentyTwoFifty = Instant.parse("2250-01-01T00:00:00Z");

        List<TypedDataSupplier> cases = new ArrayList<>();
        if (minValue.isAfter(Instant.EPOCH) == false) {
            cases.add(
                new TypedDataSupplier("<1970-01-01T00:00:00.000000000Z>", () -> DateUtils.toLong(Instant.EPOCH), DataType.DATE_NANOS)
            );
        }

        Instant lower = Instant.EPOCH.isBefore(minValue) ? minValue : Instant.EPOCH;
        Instant upper = twentyOneHundred.isAfter(maxValue) ? maxValue : twentyOneHundred;
        if (upper.isAfter(lower)) {
            cases.add(
                new TypedDataSupplier(
                    "<21st century date nanos>",
                    () -> DateUtils.toLong(ESTestCase.randomInstantBetween(lower, upper)),
                    DataType.DATE_NANOS
                )
            );
        }

        Instant lower2 = twentyOneHundred.isBefore(minValue) ? minValue : twentyOneHundred;
        Instant upper2 = twentyTwoHundred.isAfter(maxValue) ? maxValue : twentyTwoHundred;
        if (upper.isAfter(lower)) {
            cases.add(
                new TypedDataSupplier(
                    "<22nd century date nanos>",
                    () -> DateUtils.toLong(ESTestCase.randomInstantBetween(lower2, upper2)),
                    DataType.DATE_NANOS
                )
            );
        }

        Instant lower3 = twentyTwoHundred.isBefore(minValue) ? minValue : twentyTwoHundred;
        Instant upper3 = twentyTwoFifty.isAfter(maxValue) ? maxValue : twentyTwoFifty;
        if (upper.isAfter(lower)) {
            cases.add(
                new TypedDataSupplier(
                    "<23rd century date nanos>",
                    () -> DateUtils.toLong(ESTestCase.randomInstantBetween(lower3, upper3)),
                    DataType.DATE_NANOS
                )
            );
        }
        return cases;
    }

    public static List<TypedDataSupplier> datePeriodCases() {
        return datePeriodCases(-1000, -13, -32, 1000, 13, 32);
    }

    public static List<TypedDataSupplier> datePeriodCases(int yearMin, int monthMin, int dayMin, int yearMax, int monthMax, int dayMax) {
        final int yMin = Math.max(yearMin, -1000);
        final int mMin = Math.max(monthMin, -13);
        final int dMin = Math.max(dayMin, -32);
        final int yMax = Math.min(yearMax, 1000);
        final int mMax = Math.min(monthMax, 13);
        final int dMax = Math.min(dayMax, 32);
        return List.of(
            new TypedDataSupplier("<zero date period>", () -> Period.ZERO, DataType.DATE_PERIOD, true),
            new TypedDataSupplier(
                "<random date period>",
                () -> Period.of(
                    ESTestCase.randomIntBetween(yMin, yMax),
                    ESTestCase.randomIntBetween(mMin, mMax),
                    ESTestCase.randomIntBetween(dMin, dMax)
                ),
                DataType.DATE_PERIOD,
                true
            )
        );
    }

    public static List<TypedDataSupplier> timeDurationCases() {
        return timeDurationCases(-604800000, 604800000);
    }

    public static List<TypedDataSupplier> timeDurationCases(long minValue, long maxValue) {
        // plus/minus 7 days by default, with caller limits
        final long min = Math.max(minValue, -604800000L);
        final long max = Math.max(maxValue, 604800000L);
        return List.of(
            new TypedDataSupplier("<zero time duration>", () -> Duration.ZERO, DataType.TIME_DURATION, true),
            new TypedDataSupplier(
                "<up to 7 days duration>",
                () -> Duration.ofMillis(ESTestCase.randomLongBetween(min, max)),
                DataType.TIME_DURATION,
                true
            )
        );
    }

    public static List<TypedDataSupplier> geoPointCases() {
        return geoPointCases(ESTestCase::randomBoolean);
    }

    public static List<TypedDataSupplier> cartesianPointCases() {
        return cartesianPointCases(ESTestCase::randomBoolean);
    }

    public static List<TypedDataSupplier> geoShapeCases() {
        return geoShapeCases(ESTestCase::randomBoolean);
    }

    public static List<TypedDataSupplier> cartesianShapeCases() {
        return cartesianShapeCases(ESTestCase::randomBoolean);
    }

    /**
     * Generate cases for {@link DataType#GEO_POINT}.
     * <p>
     *     For multi-row parameters, see {@link MultiRowTestCaseSupplier#geoPointCases}.
     * </p>
     */
    public static List<TypedDataSupplier> geoPointCases(Supplier<Boolean> hasAlt) {
        return List.of(
            new TypedDataSupplier("<geo_point>", () -> GEO.asWkb(GeometryTestUtils.randomPoint(hasAlt.get())), DataType.GEO_POINT)
        );
    }

    /**
     * Generate cases for {@link DataType#CARTESIAN_POINT}.
     * <p>
     *     For multi-row parameters, see {@link MultiRowTestCaseSupplier#cartesianPointCases}.
     * </p>
     */
    public static List<TypedDataSupplier> cartesianPointCases(Supplier<Boolean> hasAlt) {
        return List.of(
            new TypedDataSupplier(
                "<cartesian_point>",
                () -> CARTESIAN.asWkb(ShapeTestUtils.randomPoint(hasAlt.get())),
                DataType.CARTESIAN_POINT
            )
        );
    }

    public static List<TypedDataSupplier> geoShapeCases(Supplier<Boolean> hasAlt) {
        return List.of(
            new TypedDataSupplier(
                "<geo_shape>",
                () -> GEO.asWkb(GeometryTestUtils.randomGeometryWithoutCircle(0, hasAlt.get())),
                DataType.GEO_SHAPE
            )
        );
    }

    public static List<TypedDataSupplier> cartesianShapeCases(Supplier<Boolean> hasAlt) {
        return List.of(
            new TypedDataSupplier(
                "<cartesian_shape>",
                () -> CARTESIAN.asWkb(ShapeTestUtils.randomGeometry(hasAlt.get())),
                DataType.CARTESIAN_SHAPE
            )
        );
    }

    /**
     * Generate cases for {@link DataType#IP}.
     * <p>
     *     For multi-row parameters, see {@link MultiRowTestCaseSupplier#ipCases}.
     * </p>
     */
    public static List<TypedDataSupplier> ipCases() {
        return List.of(
            new TypedDataSupplier(
                "<127.0.0.1 ip>",
                () -> new BytesRef(InetAddressPoint.encode(InetAddresses.forString("127.0.0.1"))),
                DataType.IP
            ),
            new TypedDataSupplier("<ipv4>", () -> new BytesRef(InetAddressPoint.encode(ESTestCase.randomIp(true))), DataType.IP),
            new TypedDataSupplier("<ipv6>", () -> new BytesRef(InetAddressPoint.encode(ESTestCase.randomIp(false))), DataType.IP)
        );
    }

    /**
     * Generate cases for String DataTypes.
     * <p>
     *     For multi-row parameters, see {@link MultiRowTestCaseSupplier#stringCases}.
     * </p>
     */
    public static List<TypedDataSupplier> stringCases(DataType type) {
        List<TypedDataSupplier> result = new ArrayList<>();
        result.add(new TypedDataSupplier("<empty " + type + ">", () -> new BytesRef(""), type));
        result.add(
            new TypedDataSupplier("<short alpha " + type + ">", () -> new BytesRef(ESTestCase.randomAlphaOfLengthBetween(1, 30)), type)
        );
        result.add(
            new TypedDataSupplier("<long alpha " + type + ">", () -> new BytesRef(ESTestCase.randomAlphaOfLengthBetween(300, 3000)), type)
        );
        result.add(
            new TypedDataSupplier(
                "<short unicode " + type + ">",
                () -> new BytesRef(ESTestCase.randomRealisticUnicodeOfLengthBetween(1, 30)),
                type
            )
        );
        result.add(
            new TypedDataSupplier(
                "<long unicode " + type + ">",
                () -> new BytesRef(ESTestCase.randomRealisticUnicodeOfLengthBetween(300, 3000)),
                type
            )
        );
        return result;
    }

    /**
     * Supplier test case data for {@link Version} fields.
     * <p>
     *     For multi-row parameters, see {@link MultiRowTestCaseSupplier#versionCases}.
     * </p>
     */
    public static List<TypedDataSupplier> versionCases(String prefix) {
        return List.of(
            new TypedDataSupplier(
                "<" + prefix + "version major>",
                () -> new Version(Integer.toString(ESTestCase.between(0, 100))).toBytesRef(),
                DataType.VERSION
            ),
            new TypedDataSupplier(
                "<" + prefix + "version major.minor>",
                () -> new Version(ESTestCase.between(0, 100) + "." + ESTestCase.between(0, 100)).toBytesRef(),
                DataType.VERSION
            ),
            new TypedDataSupplier(
                "<" + prefix + "version major.minor.patch>",
                () -> new Version(ESTestCase.between(0, 100) + "." + ESTestCase.between(0, 100) + "." + ESTestCase.between(0, 100))
                    .toBytesRef(),
                DataType.VERSION
            )
        );
    }

    public static String getCastEvaluator(String original, DataType current, DataType target) {
        if (current == target) {
            return original;
        }
        if (target == DataType.LONG) {
            return castToLongEvaluator(original, current);
        }
        if (target == DataType.UNSIGNED_LONG) {
            return castToUnsignedLongEvaluator(original, current);
        }
        if (target == DataType.DOUBLE) {
            return castToDoubleEvaluator(original, current);
        }
        throw new IllegalArgumentException("Invalid numeric cast to [" + target + "]");
    }

    private static String castToLongEvaluator(String original, DataType current) {
        if (current == DataType.LONG) {
            return original;
        }
        if (current == DataType.INTEGER) {
            return "CastIntToLongEvaluator[v=" + original + "]";
        }
        if (current == DataType.DOUBLE) {
            return "CastDoubleToLongEvaluator[v=" + original + "]";
        }
        if (current == DataType.UNSIGNED_LONG) {
            return "CastUnsignedLongToLong[v=" + original + "]";
        }
        throw new UnsupportedOperationException();
    }

    private static String castToUnsignedLongEvaluator(String original, DataType current) {
        if (current == DataType.UNSIGNED_LONG) {
            return original;
        }
        if (current == DataType.INTEGER) {
            return "CastIntToUnsignedLongEvaluator[v=" + original + "]";
        }
        if (current == DataType.LONG) {
            return "CastLongToUnsignedLongEvaluator[v=" + original + "]";
        }
        if (current == DataType.DOUBLE) {
            return "CastDoubleToUnsignedLongEvaluator[v=" + original + "]";
        }
        throw new UnsupportedOperationException();
    }

    public static String castToDoubleEvaluator(String original, DataType current) {
        if (current == DataType.DOUBLE) {
            return original;
        }
        if (current == DataType.INTEGER) {
            return "CastIntToDoubleEvaluator[v=" + original + "]";
        }
        if (current == DataType.LONG) {
            return "CastLongToDoubleEvaluator[v=" + original + "]";
        }
        if (current == DataType.UNSIGNED_LONG) {
            return "CastUnsignedLongToDoubleEvaluator[v=" + original + "]";
        }
        throw new UnsupportedOperationException();
    }

    public static final class TestCase {
        /**
         * The {@link Source} this test case should be run with
         */
        private final Source source;
        /**
         * The {@link Configuration} this test case should use
         */
        private final Configuration configuration;
        /**
         * The parameter values and types to pass into the function for this test run
         */
        private final List<TypedData> data;

        /**
         * The expected toString output for the evaluator this function invocation should generate
         */
        private final Matcher<String> evaluatorToString;
        /**
         * The expected output type for the case being tested
         */
        private final DataType expectedType;
        /**
         * A matcher to validate the output of the function run on the given input data
         */
        private final Matcher<Object> matcher;

        /**
         * Warnings this test is expected to produce
         */
        private final String[] expectedWarnings;

        /**
         * Warnings that are added by calling {@link AbstractFunctionTestCase#evaluator}
         * or {@link Expression#fold} on the expression built by this.
         */
        private final String[] expectedBuildEvaluatorWarnings;

        /**
         * @deprecated use subclasses of {@link ErrorsForCasesWithoutExamplesTestCase}
         */
        @Deprecated
        private final String expectedTypeError;
        private final boolean canBuildEvaluator;

        private final Class<? extends Throwable> foldingExceptionClass;
        private final String foldingExceptionMessage;

        /**
         * Extra data embedded in the test case. Test subclasses can cast
         * as needed and extra <strong>whatever</strong> helps them.
         */
        private final Object extra;

        public TestCase(List<TypedData> data, String evaluatorToString, DataType expectedType, Matcher<?> matcher) {
            this(data, equalTo(evaluatorToString), expectedType, matcher);
        }

        public TestCase(List<TypedData> data, Matcher<String> evaluatorToString, DataType expectedType, Matcher<?> matcher) {
            this(data, evaluatorToString, expectedType, matcher, null, null, null, null, null, null);
        }

        /**
         * Build a test case for type errors.
         * @deprecated use a subclass of {@link ErrorsForCasesWithoutExamplesTestCase} instead
         */
        @Deprecated
        public static TestCase typeError(List<TypedData> data, String expectedTypeError) {
            return new TestCase(data, null, null, null, null, null, expectedTypeError, null, null, null);
        }

        TestCase(
            List<TypedData> data,
            Matcher<String> evaluatorToString,
            DataType expectedType,
            Matcher<?> matcher,
            String[] expectedWarnings,
            String[] expectedBuildEvaluatorWarnings,
            String expectedTypeError,
            Class<? extends Throwable> foldingExceptionClass,
            String foldingExceptionMessage,
            Object extra
        ) {
            this(
                data,
                evaluatorToString,
                expectedType,
                matcher,
                expectedWarnings,
                expectedBuildEvaluatorWarnings,
                expectedTypeError,
                foldingExceptionClass,
                foldingExceptionMessage,
                extra,
                true
            );
        }

        TestCase(
            List<TypedData> data,
            Matcher<String> evaluatorToString,
            DataType expectedType,
            Matcher<?> matcher,
            String[] expectedWarnings,
            String[] expectedBuildEvaluatorWarnings,
            String expectedTypeError,
            Class<? extends Throwable> foldingExceptionClass,
            String foldingExceptionMessage,
            Object extra,
            boolean canBuildEvaluator
        ) {
            this.source = TEST_SOURCE;
            this.configuration = TEST_CONFIGURATION;
            this.data = data;
            this.evaluatorToString = evaluatorToString;
            this.expectedType = expectedType == null ? null : expectedType.noText();
            @SuppressWarnings("unchecked")
            Matcher<Object> downcast = (Matcher<Object>) matcher;
            this.matcher = downcast;
            this.expectedWarnings = expectedWarnings;
            this.expectedBuildEvaluatorWarnings = expectedBuildEvaluatorWarnings;
            this.expectedTypeError = expectedTypeError;
            this.foldingExceptionClass = foldingExceptionClass;
            this.foldingExceptionMessage = foldingExceptionMessage;
            this.extra = extra;
            this.canBuildEvaluator = canBuildEvaluator;
        }

        public Source getSource() {
            return source;
        }

        public Configuration getConfiguration() {
            return configuration;
        }

        public List<TypedData> getData() {
            return data;
        }

        public List<Expression> getDataAsFields() {
            return data.stream().map(TypedData::asField).collect(Collectors.toList());
        }

        public List<Expression> getDataAsDeepCopiedFields() {
            return data.stream().map(TypedData::asDeepCopyOfField).collect(Collectors.toList());
        }

        public List<Expression> getDataAsLiterals() {
            return data.stream().map(e -> e.mapExpression ? e.asMapExpression() : e.asLiteral()).collect(Collectors.toList());
        }

        public List<Object> getDataValues() {
            return data.stream().filter(d -> d.forceLiteral == false).map(TypedData::data).collect(Collectors.toList());
        }

        public List<TypedData> getMultiRowFields() {
            return data.stream().filter(TypedData::isMultiRow).collect(Collectors.toList());
        }

        public boolean canGetDataAsLiterals() {
            return data.stream().noneMatch(d -> d.isMultiRow() && d.multiRowData().size() != 1);
        }

        public boolean canBuildEvaluator() {
            return canBuildEvaluator;
        }

        public Matcher<Object> getMatcher() {
            return matcher;
        }

        public String[] getExpectedWarnings() {
            return expectedWarnings;
        }

        /**
         * Warnings that are added by calling {@link AbstractFunctionTestCase#evaluator}
         * or {@link Expression#fold} on the expression built by this.
         */
        public String[] getExpectedBuildEvaluatorWarnings() {
            return expectedBuildEvaluatorWarnings;
        }

        public Class<? extends Throwable> foldingExceptionClass() {
            return foldingExceptionClass;
        }

        public String foldingExceptionMessage() {
            return foldingExceptionMessage;
        }

        /**
         * @deprecated use subclasses of {@link ErrorsForCasesWithoutExamplesTestCase}
         */
        @Deprecated
        public String getExpectedTypeError() {
            return expectedTypeError;
        }

        /**
         * Extra data embedded in the test case. Test subclasses can cast
         * as needed and extra <strong>whatever</strong> helps them.
         */
        public Object extra() {
            return extra;
        }

        /**
         * Build a new {@link TestCase} with new {@link #data}.
         */
        public TestCase withData(List<TestCaseSupplier.TypedData> data) {
            return new TestCase(
                data,
                evaluatorToString,
                expectedType,
                matcher,
                expectedWarnings,
                expectedBuildEvaluatorWarnings,
                expectedTypeError,
                foldingExceptionClass,
                foldingExceptionMessage,
                extra,
                canBuildEvaluator
            );
        }

        /**
         * Build a new {@link TestCase} with new {@link #extra()}.
         */
        public TestCase withExtra(Object extra) {
            return new TestCase(
                data,
                evaluatorToString,
                expectedType,
                matcher,
                expectedWarnings,
                expectedBuildEvaluatorWarnings,
                expectedTypeError,
                foldingExceptionClass,
                foldingExceptionMessage,
                extra,
                canBuildEvaluator
            );
        }

        public TestCase withWarning(String warning) {
            return new TestCase(
                data,
                evaluatorToString,
                expectedType,
                matcher,
                addWarning(expectedWarnings, warning),
                expectedBuildEvaluatorWarnings,
                expectedTypeError,
                foldingExceptionClass,
                foldingExceptionMessage,
                extra,
                canBuildEvaluator
            );
        }

        /**
         * Warnings that are added by calling {@link AbstractFunctionTestCase#evaluator}
         * or {@link Expression#fold} on the expression built by this.
         */
        public TestCase withBuildEvaluatorWarning(String warning) {
            return new TestCase(
                data,
                evaluatorToString,
                expectedType,
                matcher,
                expectedWarnings,
                addWarning(expectedBuildEvaluatorWarnings, warning),
                expectedTypeError,
                foldingExceptionClass,
                foldingExceptionMessage,
                extra,
                canBuildEvaluator
            );
        }

        private String[] addWarning(String[] warnings, String warning) {
            if (warnings == null) {
                return new String[] { warning };
            }
            String[] newWarnings = Arrays.copyOf(warnings, warnings.length + 1);
            newWarnings[warnings.length] = warning;
            return newWarnings;
        }

        public TestCase withFoldingException(Class<? extends Throwable> clazz, String message) {
            return new TestCase(
                data,
                evaluatorToString,
                expectedType,
                matcher,
                expectedWarnings,
                expectedBuildEvaluatorWarnings,
                expectedTypeError,
                clazz,
                message,
                extra,
                canBuildEvaluator
            );
        }

        /**
         * Build a new {@link TestCase} that can't build an evaluator.
         * <p>
         *     Useful for special cases that can't be executed, but should still be considered.
         * </p>
         */
        public TestCase withoutEvaluator() {
            return new TestCase(
                data,
                evaluatorToString,
                expectedType,
                matcher,
                expectedWarnings,
                expectedBuildEvaluatorWarnings,
                expectedTypeError,
                foldingExceptionClass,
                foldingExceptionMessage,
                extra,
                false
            );
        }

        public DataType expectedType() {
            return expectedType;
        }

        public Matcher<String> evaluatorToString() {
            return evaluatorToString;
        }
    }

    /**
     * Holds a supplier for a data value, along with the type of that value and a name for generating test case names. This mostly
     * exists because we can't generate random values from the test parameter generation functions, and instead need to return
     * suppliers which generate the random values at test execution time.
     */
    public record TypedDataSupplier(String name, Supplier<Object> supplier, DataType type, boolean forceLiteral, boolean multiRow) {

        public TypedDataSupplier(String name, Supplier<Object> supplier, DataType type, boolean forceLiteral) {
            this(name, supplier, type, forceLiteral, false);
        }

        public TypedDataSupplier(String name, Supplier<Object> supplier, DataType type) {
            this(name, supplier, type, false, false);
        }

        public TypedData get() {
            return new TypedData(supplier.get(), type, name, forceLiteral, multiRow);
        }
    }

    /**
     * Holds a data value and the intended parse type of that value
     */
    public static class TypedData {
        public static final TypedData NULL = new TypedData(null, DataType.NULL, "<null>");
        public static final TypedData MULTI_ROW_NULL = TypedData.multiRow(Collections.singletonList(null), DataType.NULL, "<null>");

        private final Object data;
        private final DataType type;
        private final String name;
        private final boolean forceLiteral;
        private final boolean multiRow;
        private final boolean mapExpression;

        /**
         * @param data value to test against
         * @param type type of the value, for building expressions
         * @param name a name for the value, used for generating test case names
         * @param forceLiteral should this data always be converted to a literal and <strong>never</strong> to a field reference?
         * @param multiRow if true, data is expected to be a List of values, one per row
         */
        private TypedData(Object data, DataType type, String name, boolean forceLiteral, boolean multiRow) {
            assert multiRow == false || data instanceof List : "multiRow data must be a List";
            assert multiRow == false || forceLiteral == false : "multiRow data can't be converted to a literal";

            if (type == DataType.UNSIGNED_LONG && data instanceof BigInteger b) {
                this.data = NumericUtils.asLongUnsigned(b);
            } else {
                this.data = data;
            }
            this.type = type;
            this.name = name;
            this.forceLiteral = forceLiteral;
            this.multiRow = multiRow;
            this.mapExpression = data instanceof MapExpression;
        }

        /**
         * @param data value to test against
         * @param type type of the value, for building expressions
         * @param name a name for the value, used for generating test case names
         */
        public TypedData(Object data, DataType type, String name) {
            this(data, type, name, false, false);
        }

        /**
         * Build a value, guessing the type via reflection.
         * @param data value to test against
         * @param name a name for the value, used for generating test case names
         */
        public TypedData(Object data, String name) {
            this(data, DataType.fromJava(data), name);
        }

        /**
         * Create a TypedData object for field to be aggregated.
         * @param data values to test against, one per row
         * @param type type of the value, for building expressions
         * @param name a name for the value, used for generating test case names
         */
        public static TypedData multiRow(List<?> data, DataType type, String name) {
            return new TypedData(data, type, name, false, true);
        }

        /**
         * Return a {@link TypedData} that always returns a {@link Literal} from
         * {@link #asField} and {@link #asDeepCopyOfField}. Use this for things that
         * must be constants.
         */
        public TypedData forceLiteral() {
            return new TypedData(data, type, name, true, multiRow);
        }

        /**
         * Has this been forced to a {@link Literal}.
         */
        public boolean isForceLiteral() {
            return forceLiteral;
        }

        /**
         * If true, the data is expected to be a List of values, one per row.
         */
        public boolean isMultiRow() {
            return multiRow;
        }

        /**
         * Return a {@link TypedData} with the new data.
         *
         * @param data The new data for the {@link TypedData}.
         */
        public TypedData withData(Object data) {
            return new TypedData(data, type, name, forceLiteral, multiRow);
        }

        @Override
        public String toString() {
            if (type == DataType.UNSIGNED_LONG && data instanceof Long longData) {
                return type + "(" + NumericUtils.unsignedLongAsBigInteger(longData).toString() + ")";
            }
            return type.toString() + "(" + (data == null ? "null" : getValue().toString()) + ")";
        }

        /**
         * Convert this into reference to a field.
         */
        public Expression asField() {
            if (forceLiteral) {
                return mapExpression ? asMapExpression() : asLiteral();
            }
            return AbstractFunctionTestCase.field(name, type);
        }

        /**
         * Convert this into an anonymous function that performs a copy of the values loaded from a field.
         */
        public Expression asDeepCopyOfField() {
            if (forceLiteral) {
                return mapExpression ? asMapExpression() : asLiteral();
            }
            return AbstractFunctionTestCase.deepCopyOfField(name, type);
        }

        /**
         * Convert this into a {@link Literal}.
         */
        public Literal asLiteral() {
            if (multiRow) {
                var values = multiRowData();

                if (values.size() != 1) {
                    throw new IllegalStateException("Multirow values require exactly 1 element to be a literal, got " + values.size());
                }

                return new Literal(Source.synthetic(name), values.get(0), type);
            }
            return new Literal(Source.synthetic(name), data, type);
        }

        /**
         * Value to test against.
         */
        public Object data() {
            return data;
        }

        /**
         * Values to test against.
         */
        @SuppressWarnings("unchecked")
        public List<Object> multiRowData() {
            return (List<Object>) data;
        }

        /**
         * If the data is a MapExpression, return it as it is.
         */
        public MapExpression asMapExpression() {
            return mapExpression ? (MapExpression) data : null;
        }

        /**
         * @return the data value being supplied, casting to java objects when appropriate
         */
        public Object getValue() {
            if (data instanceof Long l) {
                if (type == DataType.UNSIGNED_LONG) {
                    return NumericUtils.unsignedLongAsBigInteger(l);
                }
                if (type == DataType.DATETIME) {
                    return Instant.ofEpochMilli(l);
                }
                if (type == DataType.DATE_NANOS) {
                    return DateUtils.toInstant(l);
                }
            }
            return data;
        }

        /**
         * Type of the value. For building {@link Expression}s.
         */
        public DataType type() {
            return type;
        }

        /**
         * A name for the value. Used to generate test names.
         */
        public String name() {
            return name;
        }
    }
}
