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
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geo.ShapeTestUtils;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.AbstractConvertFunction;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.util.NumericUtils;
import org.elasticsearch.xpack.versionfield.Version;
import org.hamcrest.Matcher;

import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;
import java.time.Period;
import java.util.ArrayList;
import java.util.Arrays;
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

import static org.elasticsearch.xpack.ql.util.SpatialCoordinateTypes.CARTESIAN;
import static org.elasticsearch.xpack.ql.util.SpatialCoordinateTypes.GEO;
import static org.hamcrest.Matchers.equalTo;

/**
 * This class exists to give a human-readable string representation of the test case.
 */
public record TestCaseSupplier(String name, List<DataType> types, Supplier<TestCase> supplier)
    implements
        Supplier<TestCaseSupplier.TestCase> {

    private static Logger logger = LogManager.getLogger(TestCaseSupplier.class);
    /**
     * Build a test case without types.
     *
     * @deprecated Supply types
     */
    @Deprecated
    public TestCaseSupplier(String name, Supplier<TestCase> supplier) {
        this(name, null, supplier);
    }

    /**
     * Build a test case named after the types it takes.
     */
    public TestCaseSupplier(List<DataType> types, Supplier<TestCase> supplier) {
        this(nameFromTypes(types), types, supplier);
    }

    static String nameFromTypes(List<DataType> types) {
        return types.stream().map(t -> "<" + t.typeName() + ">").collect(Collectors.joining(", "));
    }

    @Override
    public TestCase get() {
        TestCase supplied = supplier.get();
        if (types != null) {
            for (int i = 0; i < types.size(); i++) {
                if (supplied.getData().get(i).type() != types.get(i)) {
                    throw new IllegalStateException(
                        name + ": supplier/data type mismatch " + supplied.getData().get(i).type() + "/" + types.get(i)
                    );
                }
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
     * fields by casting them to {@link DataTypes#DOUBLE}s.
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
            eval + castToDoubleEvaluator(read, DataTypes.INTEGER) + "]",
            DataTypes.DOUBLE,
            i -> expected.apply(Double.valueOf(i)),
            min.intValue(),
            max.intValue(),
            warnings
        );
        forUnaryLong(
            suppliers,
            eval + castToDoubleEvaluator(read, DataTypes.LONG) + "]",
            DataTypes.DOUBLE,
            i -> expected.apply(Double.valueOf(i)),
            min.longValue(),
            max.longValue(),
            warnings
        );
        forUnaryUnsignedLong(
            suppliers,
            eval + castToDoubleEvaluator(read, DataTypes.UNSIGNED_LONG) + "]",
            DataTypes.DOUBLE,
            ul -> expected.apply(ul.doubleValue()),
            BigInteger.valueOf((int) Math.ceil(min)),
            BigInteger.valueOf((int) Math.floor(max)),
            warnings
        );
        forUnaryDouble(suppliers, eval + read + "]", DataTypes.DOUBLE, expected::apply, min, max, warnings);
        return suppliers;
    }

    /**
     * Generate positive test cases for binary functions that operate on an {@code numeric}
     * fields by casting them to {@link DataTypes#DOUBLE}s.
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
            (lhsType, rhsType) -> name
                + "["
                + lhsName
                + "="
                + castToDoubleEvaluator("Attribute[channel=0]", lhsType)
                + ", "
                + rhsName
                + "="
                + castToDoubleEvaluator("Attribute[channel=1]", rhsType)
                + "]",
            warnings,
            suppliers,
            DataTypes.DOUBLE,
            false
        );
        return suppliers;
    }

    private static void casesCrossProduct(
        BinaryOperator<Object> expected,
        List<TypedDataSupplier> lhsSuppliers,
        List<TypedDataSupplier> rhsSuppliers,
        BiFunction<DataType, DataType, String> evaluatorToString,
        List<String> warnings,
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
        BiFunction<DataType, DataType, String> evaluatorToString,
        DataType expectedType,
        BinaryOperator<Object> expectedValue
    ) {
        return testCaseSupplier(lhsSupplier, rhsSupplier, evaluatorToString, expectedType, expectedValue, List.of());
    }

    private static TestCaseSupplier testCaseSupplier(
        TypedDataSupplier lhsSupplier,
        TypedDataSupplier rhsSupplier,
        BiFunction<DataType, DataType, String> evaluatorToString,
        DataType expectedType,
        BinaryOperator<Object> expectedValue,
        List<String> warnings
    ) {
        String caseName = lhsSupplier.name() + ", " + rhsSupplier.name();
        return new TestCaseSupplier(caseName, List.of(lhsSupplier.type(), rhsSupplier.type()), () -> {
            Object lhs = lhsSupplier.supplier().get();
            Object rhs = rhsSupplier.supplier().get();
            TypedData lhsTyped = new TypedData(
                // TODO there has to be a better way to handle unsigned long
                lhs instanceof BigInteger b ? NumericUtils.asLongUnsigned(b) : lhs,
                lhsSupplier.type(),
                "lhs"
            );
            TypedData rhsTyped = new TypedData(
                rhs instanceof BigInteger b ? NumericUtils.asLongUnsigned(b) : rhs,
                rhsSupplier.type(),
                "rhs"
            );
            TestCase testCase = new TestCase(
                List.of(lhsTyped, rhsTyped),
                evaluatorToString.apply(lhsSupplier.type(), rhsSupplier.type()),
                expectedType,
                equalTo(expectedValue.apply(lhs, rhs))
            );
            for (String warning : warnings) {
                testCase = testCase.withWarning(warning);
            }
            return testCase;
        });
    }

    public static List<TypedDataSupplier> castToDoubleSuppliersFromRange(Double Min, Double Max) {
        List<TypedDataSupplier> suppliers = new ArrayList<>();
        suppliers.addAll(intCases(Min.intValue(), Max.intValue()));
        suppliers.addAll(longCases(Min.longValue(), Max.longValue()));
        suppliers.addAll(ulongCases(BigInteger.valueOf((long) Math.ceil(Min)), BigInteger.valueOf((long) Math.floor(Max))));
        suppliers.addAll(doubleCases(Min, Max));
        return suppliers;
    }

    public record NumericTypeTestConfig(Number min, Number max, BinaryOperator<Number> expected, String evaluatorName) {}

    public record NumericTypeTestConfigs(
        NumericTypeTestConfig intStuff,
        NumericTypeTestConfig longStuff,
        NumericTypeTestConfig doubleStuff
    ) {
        public NumericTypeTestConfig get(DataType type) {
            if (type == DataTypes.INTEGER) {
                return intStuff;
            }
            if (type == DataTypes.LONG) {
                return longStuff;
            }
            if (type == DataTypes.DOUBLE) {
                return doubleStuff;
            }
            throw new IllegalArgumentException("bogus numeric type [" + type + "]");
        }
    }

    private static DataType widen(DataType lhs, DataType rhs) {
        if (lhs == rhs) {
            return lhs;
        }
        if (lhs == DataTypes.DOUBLE || rhs == DataTypes.DOUBLE) {
            return DataTypes.DOUBLE;
        }
        if (lhs == DataTypes.LONG || rhs == DataTypes.LONG) {
            return DataTypes.LONG;
        }
        throw new IllegalArgumentException("Invalid numeric widening lhs: [" + lhs + "] rhs: [" + rhs + "]");
    }

    private static List<TypedDataSupplier> getSuppliersForNumericType(DataType type, Number min, Number max) {
        if (type == DataTypes.INTEGER) {
            return intCases(NumericUtils.saturatingIntValue(min), NumericUtils.saturatingIntValue(max));
        }
        if (type == DataTypes.LONG) {
            return longCases(min.longValue(), max.longValue());
        }
        if (type == DataTypes.UNSIGNED_LONG) {
            return ulongCases(
                min instanceof BigInteger ? (BigInteger) min : BigInteger.valueOf(Math.max(min.longValue(), 0L)),
                max instanceof BigInteger ? (BigInteger) max : BigInteger.valueOf(Math.max(max.longValue(), 0L))
            );
        }
        if (type == DataTypes.DOUBLE) {
            return doubleCases(min.doubleValue(), max.doubleValue());
        }
        throw new IllegalArgumentException("bogus numeric type [" + type + "]");
    }

    public static List<TestCaseSupplier> forBinaryWithWidening(
        NumericTypeTestConfigs typeStuff,
        String lhsName,
        String rhsName,
        List<String> warnings
    ) {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        List<DataType> numericTypes = List.of(DataTypes.INTEGER, DataTypes.LONG, DataTypes.DOUBLE);

        for (DataType lhsType : numericTypes) {
            for (DataType rhsType : numericTypes) {
                DataType expected = widen(lhsType, rhsType);
                NumericTypeTestConfig expectedTypeStuff = typeStuff.get(expected);
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
                    getSuppliersForNumericType(lhsType, expectedTypeStuff.min(), expectedTypeStuff.max()),
                    getSuppliersForNumericType(rhsType, expectedTypeStuff.min(), expectedTypeStuff.max()),
                    evaluatorToString,
                    warnings,
                    suppliers,
                    expected,
                    true
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
        List<String> warnings
    ) {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        casesCrossProduct(
            expected,
            lhsSuppliers,
            rhsSuppliers,
            (lhsType, rhsType) -> name + "[" + lhsName + "=Attribute[channel=0], " + rhsName + "=Attribute[channel=1]]",
            warnings,
            suppliers,
            expectedType,
            true
        );
        return suppliers;
    }

    /**
     * Generate positive test cases for a unary function operating on an {@link DataTypes#INTEGER}.
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
            intCases(lowerBound, upperBound),
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
     * Generate positive test cases for a unary function operating on an {@link DataTypes#LONG}.
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
            longCases(lowerBound, upperBound),
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
     * Generate positive test cases for a unary function operating on an {@link DataTypes#UNSIGNED_LONG}.
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
            ulongCases(lowerBound, upperBound),
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
     * Generate positive test cases for a unary function operating on an {@link DataTypes#DOUBLE}.
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
            doubleCases(lowerBound, upperBound),
            expectedType,
            n -> expectedValue.apply(n.doubleValue()),
            n -> expectedWarnings.apply(n.doubleValue())
        );
    }

    /**
     * Generate positive test cases for a unary function operating on an {@link DataTypes#BOOLEAN}.
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
     * Generate positive test cases for a unary function operating on an {@link DataTypes#DATETIME}.
     */
    public static void forUnaryDatetime(
        List<TestCaseSupplier> suppliers,
        String expectedEvaluatorToString,
        DataType expectedType,
        Function<Instant, Object> expectedValue,
        List<String> warnings
    ) {
        unaryNumeric(
            suppliers,
            expectedEvaluatorToString,
            dateCases(),
            expectedType,
            n -> expectedValue.apply(Instant.ofEpochMilli(n.longValue())),
            warnings
        );
    }

    /**
     * Generate positive test cases for a unary function operating on an {@link EsqlDataTypes#GEO_POINT}.
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
     * Generate positive test cases for a unary function operating on an {@link EsqlDataTypes#CARTESIAN_POINT}.
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
     * Generate positive test cases for a unary function operating on an {@link EsqlDataTypes#GEO_SHAPE}.
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
     * Generate positive test cases for a unary function operating on an {@link EsqlDataTypes#CARTESIAN_SHAPE}.
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
     * Generate positive test cases for a unary function operating on an {@link DataTypes#IP}.
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
     * Generate positive test cases for a unary function operating on an {@link DataTypes#KEYWORD} and {@link DataTypes#TEXT}.
     */
    public static void forUnaryStrings(
        List<TestCaseSupplier> suppliers,
        String expectedEvaluatorToString,
        DataType expectedType,
        Function<BytesRef, Object> expectedValue,
        Function<BytesRef, List<String>> expectedWarnings
    ) {
        for (DataType type : AbstractConvertFunction.STRING_TYPES) {
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
     * Generate positive test cases for a unary function operating on an {@link DataTypes#VERSION}.
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
                Object value = supplier.supplier().get();
                TypedData typed = new TypedData(
                    // TODO there has to be a better way to handle unsigned long
                    value instanceof BigInteger b ? NumericUtils.asLongUnsigned(b) : value,
                    supplier.type(),
                    "value"
                );
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

    public static List<TypedDataSupplier> intCases(int min, int max) {
        List<TypedDataSupplier> cases = new ArrayList<>();
        if (0 <= max && 0 >= min) {
            cases.add(new TypedDataSupplier("<0 int>", () -> 0, DataTypes.INTEGER));
        }

        int lower = Math.max(min, 1);
        int upper = Math.min(max, Integer.MAX_VALUE);
        if (lower < upper) {
            cases.add(new TypedDataSupplier("<positive int>", () -> ESTestCase.randomIntBetween(lower, upper), DataTypes.INTEGER));
        } else if (lower == upper) {
            cases.add(new TypedDataSupplier("<" + lower + " int>", () -> lower, DataTypes.INTEGER));
        }

        int lower1 = Math.max(min, Integer.MIN_VALUE);
        int upper1 = Math.min(max, -1);
        if (lower1 < upper1) {
            cases.add(new TypedDataSupplier("<negative int>", () -> ESTestCase.randomIntBetween(lower1, upper1), DataTypes.INTEGER));
        } else if (lower1 == upper1) {
            cases.add(new TypedDataSupplier("<" + lower1 + " int>", () -> lower1, DataTypes.INTEGER));
        }
        return cases;
    }

    public static List<TypedDataSupplier> longCases(long min, long max) {
        List<TypedDataSupplier> cases = new ArrayList<>();
        if (0L <= max && 0L >= min) {
            cases.add(new TypedDataSupplier("<0 long>", () -> 0L, DataTypes.LONG));
        }

        long lower = Math.max(min, 1);
        long upper = Math.min(max, Long.MAX_VALUE);
        if (lower < upper) {
            cases.add(new TypedDataSupplier("<positive long>", () -> ESTestCase.randomLongBetween(lower, upper), DataTypes.LONG));
        } else if (lower == upper) {
            cases.add(new TypedDataSupplier("<" + lower + " long>", () -> lower, DataTypes.LONG));
        }

        long lower1 = Math.max(min, Long.MIN_VALUE);
        long upper1 = Math.min(max, -1);
        if (lower1 < upper1) {
            cases.add(new TypedDataSupplier("<negative long>", () -> ESTestCase.randomLongBetween(lower1, upper1), DataTypes.LONG));
        } else if (lower1 == upper1) {
            cases.add(new TypedDataSupplier("<" + lower1 + " long>", () -> lower1, DataTypes.LONG));
        }

        return cases;
    }

    public static List<TypedDataSupplier> ulongCases(BigInteger min, BigInteger max) {
        List<TypedDataSupplier> cases = new ArrayList<>();

        // Zero
        if (BigInteger.ZERO.compareTo(max) <= 0 && BigInteger.ZERO.compareTo(min) >= 0) {
            cases.add(new TypedDataSupplier("<0 unsigned long>", () -> BigInteger.ZERO, DataTypes.UNSIGNED_LONG));
        }

        // small values, less than Long.MAX_VALUE
        BigInteger lower1 = min.max(BigInteger.ONE);
        BigInteger upper1 = max.min(BigInteger.valueOf(Long.MAX_VALUE));
        if (lower1.compareTo(upper1) < 0) {
            cases.add(
                new TypedDataSupplier(
                    "<small unsigned long>",
                    () -> ESTestCase.randomUnsignedLongBetween(lower1, upper1),
                    DataTypes.UNSIGNED_LONG
                )
            );
        } else if (lower1.compareTo(upper1) == 0) {
            cases.add(new TypedDataSupplier("<small unsigned long>", () -> lower1, DataTypes.UNSIGNED_LONG));
        }

        // Big values, greater than Long.MAX_VALUE
        BigInteger lower2 = min.max(BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE));
        BigInteger upper2 = max.min(ESTestCase.UNSIGNED_LONG_MAX);
        if (lower2.compareTo(upper2) < 0) {
            cases.add(
                new TypedDataSupplier(
                    "<big unsigned long>",
                    () -> ESTestCase.randomUnsignedLongBetween(lower2, upper2),
                    DataTypes.UNSIGNED_LONG
                )
            );
        } else if (lower2.compareTo(upper2) == 0) {
            cases.add(new TypedDataSupplier("<big unsigned long>", () -> lower2, DataTypes.UNSIGNED_LONG));
        }
        return cases;
    }

    public static List<TypedDataSupplier> doubleCases(double min, double max) {
        List<TypedDataSupplier> cases = new ArrayList<>();

        // Zeros
        if (0d <= max && 0d >= min) {
            cases.add(new TypedDataSupplier("<0 double>", () -> 0.0d, DataTypes.DOUBLE));
            cases.add(new TypedDataSupplier("<-0 double>", () -> -0.0d, DataTypes.DOUBLE));
        }

        // Positive small double
        double lower1 = Math.max(0d, min);
        double upper1 = Math.min(1d, max);
        if (lower1 < upper1) {
            cases.add(
                new TypedDataSupplier(
                    "<small positive double>",
                    () -> ESTestCase.randomDoubleBetween(lower1, upper1, true),
                    DataTypes.DOUBLE
                )
            );
        } else if (lower1 == upper1) {
            cases.add(new TypedDataSupplier("<small positive double>", () -> lower1, DataTypes.DOUBLE));
        }

        // Negative small double
        double lower2 = Math.max(-1d, min);
        double upper2 = Math.min(0d, max);
        if (lower2 < upper2) {
            cases.add(
                new TypedDataSupplier(
                    "<small negative double>",
                    () -> ESTestCase.randomDoubleBetween(lower2, upper2, true),
                    DataTypes.DOUBLE
                )
            );
        } else if (lower2 == upper2) {
            cases.add(new TypedDataSupplier("<small negative double>", () -> lower2, DataTypes.DOUBLE));
        }

        // Positive big double
        double lower3 = Math.max(1d, min); // start at 1 (inclusive) because the density of values between 0 and 1 is very high
        double upper3 = Math.min(Double.MAX_VALUE, max);
        if (lower3 < upper3) {
            cases.add(
                new TypedDataSupplier("<big positive double>", () -> ESTestCase.randomDoubleBetween(lower3, upper3, true), DataTypes.DOUBLE)
            );
        } else if (lower3 == upper3) {
            cases.add(new TypedDataSupplier("<big positive double>", () -> lower3, DataTypes.DOUBLE));
        }

        // Negative big double
        // note: Double.MIN_VALUE is the smallest non-zero positive double, not the smallest non-infinite negative double.
        double lower4 = Math.max(-Double.MAX_VALUE, min);
        double upper4 = Math.min(-1, max); // because again, the interval from -1 to 0 is very high density
        if (lower4 < upper4) {
            cases.add(
                new TypedDataSupplier("<big negative double>", () -> ESTestCase.randomDoubleBetween(lower4, upper4, true), DataTypes.DOUBLE)
            );
        } else if (lower4 == upper4) {
            cases.add(new TypedDataSupplier("<big negative double>", () -> lower4, DataTypes.DOUBLE));
        }
        return cases;
    }

    private static List<TypedDataSupplier> booleanCases() {
        return List.of(
            new TypedDataSupplier("<true>", () -> true, DataTypes.BOOLEAN),
            new TypedDataSupplier("<false>", () -> false, DataTypes.BOOLEAN)
        );
    }

    public static List<TypedDataSupplier> dateCases() {
        return List.of(
            new TypedDataSupplier("<1970-01-01T00:00:00Z>", () -> 0L, DataTypes.DATETIME),
            new TypedDataSupplier(
                "<date>",
                () -> ESTestCase.randomLongBetween(0, 10 * (long) 10e11), // 1970-01-01T00:00:00Z - 2286-11-20T17:46:40Z
                DataTypes.DATETIME
            ),
            new TypedDataSupplier(
                "<far future date>",
                // 2286-11-20T17:46:40Z - +292278994-08-17T07:12:55.807Z
                () -> ESTestCase.randomLongBetween(10 * (long) 10e11, Long.MAX_VALUE),
                DataTypes.DATETIME
            )
        );
    }

    public static List<TypedDataSupplier> datePeriodCases() {
        return List.of(
            new TypedDataSupplier("<zero date period>", () -> Period.ZERO, EsqlDataTypes.DATE_PERIOD),
            new TypedDataSupplier(
                "<random date period>",
                () -> Period.of(
                    ESTestCase.randomIntBetween(-1000, 1000),
                    ESTestCase.randomIntBetween(-13, 13),
                    ESTestCase.randomIntBetween(-32, 32)
                ),
                EsqlDataTypes.DATE_PERIOD
            )
        );
    }

    public static List<TypedDataSupplier> timeDurationCases() {
        return List.of(
            new TypedDataSupplier("<zero time duration>", () -> Duration.ZERO, EsqlDataTypes.TIME_DURATION),
            new TypedDataSupplier(
                "<up to 7 days duration>",
                () -> Duration.ofMillis(ESTestCase.randomLongBetween(-604800000L, 604800000L)), // plus/minus 7 days
                EsqlDataTypes.TIME_DURATION
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

    public static List<TypedDataSupplier> geoPointCases(Supplier<Boolean> hasAlt) {
        return List.of(
            new TypedDataSupplier("<geo_point>", () -> GEO.asWkb(GeometryTestUtils.randomPoint(hasAlt.get())), EsqlDataTypes.GEO_POINT)
        );
    }

    public static List<TypedDataSupplier> cartesianPointCases(Supplier<Boolean> hasAlt) {
        return List.of(
            new TypedDataSupplier(
                "<cartesian_point>",
                () -> CARTESIAN.asWkb(ShapeTestUtils.randomPoint(hasAlt.get())),
                EsqlDataTypes.CARTESIAN_POINT
            )
        );
    }

    public static List<TypedDataSupplier> geoShapeCases(Supplier<Boolean> hasAlt) {
        return List.of(
            new TypedDataSupplier(
                "<geo_shape>",
                () -> GEO.asWkb(GeometryTestUtils.randomGeometryWithoutCircle(0, hasAlt.get())),
                EsqlDataTypes.GEO_SHAPE
            )
        );
    }

    public static List<TypedDataSupplier> cartesianShapeCases(Supplier<Boolean> hasAlt) {
        return List.of(
            new TypedDataSupplier(
                "<cartesian_shape>",
                () -> CARTESIAN.asWkb(ShapeTestUtils.randomGeometry(hasAlt.get())),
                EsqlDataTypes.CARTESIAN_SHAPE
            )
        );
    }

    public static List<TypedDataSupplier> ipCases() {
        return List.of(
            new TypedDataSupplier(
                "<127.0.0.1 ip>",
                () -> new BytesRef(InetAddressPoint.encode(InetAddresses.forString("127.0.0.1"))),
                DataTypes.IP
            ),
            new TypedDataSupplier("<ipv4>", () -> new BytesRef(InetAddressPoint.encode(ESTestCase.randomIp(true))), DataTypes.IP),
            new TypedDataSupplier("<ipv6>", () -> new BytesRef(InetAddressPoint.encode(ESTestCase.randomIp(false))), DataTypes.IP)
        );
    }

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
     */
    public static List<TypedDataSupplier> versionCases(String prefix) {
        return List.of(
            new TypedDataSupplier(
                "<" + prefix + "version major>",
                () -> new Version(Integer.toString(ESTestCase.between(0, 100))).toBytesRef(),
                DataTypes.VERSION
            ),
            new TypedDataSupplier(
                "<" + prefix + "version major.minor>",
                () -> new Version(ESTestCase.between(0, 100) + "." + ESTestCase.between(0, 100)).toBytesRef(),
                DataTypes.VERSION
            ),
            new TypedDataSupplier(
                "<" + prefix + "version major.minor.patch>",
                () -> new Version(ESTestCase.between(0, 100) + "." + ESTestCase.between(0, 100) + "." + ESTestCase.between(0, 100))
                    .toBytesRef(),
                DataTypes.VERSION
            )
        );
    }

    private static String getCastEvaluator(String original, DataType current, DataType target) {
        if (current == target) {
            return original;
        }
        if (target == DataTypes.LONG) {
            return castToLongEvaluator(original, current);
        }
        if (target == DataTypes.UNSIGNED_LONG) {
            return castToUnsignedLongEvaluator(original, current);
        }
        if (target == DataTypes.DOUBLE) {
            return castToDoubleEvaluator(original, current);
        }
        throw new IllegalArgumentException("Invalid numeric cast to [" + target + "]");
    }

    private static String castToLongEvaluator(String original, DataType current) {
        if (current == DataTypes.LONG) {
            return original;
        }
        if (current == DataTypes.INTEGER) {
            return "CastIntToLongEvaluator[v=" + original + "]";
        }
        if (current == DataTypes.DOUBLE) {
            return "CastDoubleToLongEvaluator[v=" + original + "]";
        }
        if (current == DataTypes.UNSIGNED_LONG) {
            return "CastUnsignedLongToLong[v=" + original + "]";
        }
        throw new UnsupportedOperationException();
    }

    private static String castToUnsignedLongEvaluator(String original, DataType current) {
        if (current == DataTypes.UNSIGNED_LONG) {
            return original;
        }
        if (current == DataTypes.INTEGER) {
            return "CastIntToUnsignedLongEvaluator[v=" + original + "]";
        }
        if (current == DataTypes.LONG) {
            return "CastLongToUnsignedLongEvaluator[v=" + original + "]";
        }
        if (current == DataTypes.DOUBLE) {
            return "CastDoubleToUnsignedLongEvaluator[v=" + original + "]";
        }
        throw new UnsupportedOperationException();
    }

    private static String castToDoubleEvaluator(String original, DataType current) {
        if (current == DataTypes.DOUBLE) {
            return original;
        }
        if (current == DataTypes.INTEGER) {
            return "CastIntToDoubleEvaluator[v=" + original + "]";
        }
        if (current == DataTypes.LONG) {
            return "CastLongToDoubleEvaluator[v=" + original + "]";
        }
        if (current == DataTypes.UNSIGNED_LONG) {
            return "CastUnsignedLongToDoubleEvaluator[v=" + original + "]";
        }
        throw new UnsupportedOperationException();
    }

    public static class TestCase {
        /**
         * The {@link Source} this test case should be run with
         */
        private Source source;
        /**
         * The parameter values and types to pass into the function for this test run
         */
        private List<TypedData> data;

        /**
         * The expected toString output for the evaluator this function invocation should generate
         */
        String evaluatorToString;
        /**
         * The expected output type for the case being tested
         */
        DataType expectedType;
        /**
         * A matcher to validate the output of the function run on the given input data
         */
        private Matcher<Object> matcher;

        /**
         * Warnings this test is expected to produce
         */
        private String[] expectedWarnings;

        private Class<? extends Throwable> foldingExceptionClass;
        private String foldingExceptionMessage;

        private final String expectedTypeError;
        private final boolean allTypesAreRepresentable;

        public TestCase(List<TypedData> data, String evaluatorToString, DataType expectedType, Matcher<Object> matcher) {
            this(data, evaluatorToString, expectedType, matcher, null, null);
        }

        public static TestCase typeError(List<TypedData> data, String expectedTypeError) {
            return new TestCase(data, null, null, null, null, expectedTypeError);
        }

        TestCase(
            List<TypedData> data,
            String evaluatorToString,
            DataType expectedType,
            Matcher<Object> matcher,
            String[] expectedWarnings,
            String expectedTypeError
        ) {
            this.source = Source.EMPTY;
            this.data = data;
            this.evaluatorToString = evaluatorToString;
            this.expectedType = expectedType;
            this.matcher = matcher;
            this.expectedWarnings = expectedWarnings;
            this.expectedTypeError = expectedTypeError;
            this.allTypesAreRepresentable = data.stream().allMatch(d -> EsqlDataTypes.isRepresentable(d.type));
        }

        public Source getSource() {
            return source;
        }

        public List<TypedData> getData() {
            return data;
        }

        public List<Expression> getDataAsFields() {
            return data.stream().map(t -> AbstractFunctionTestCase.field(t.name(), t.type())).collect(Collectors.toList());
        }

        public List<Expression> getDataAsDeepCopiedFields() {
            return data.stream().map(t -> AbstractFunctionTestCase.deepCopyOfField(t.name(), t.type())).collect(Collectors.toList());
        }

        public List<Expression> getDataAsLiterals() {
            return data.stream().map(t -> new Literal(Source.synthetic(t.name()), t.data(), t.type())).collect(Collectors.toList());
        }

        public List<Object> getDataValues() {
            return data.stream().map(t -> t.data()).collect(Collectors.toList());
        }

        public boolean allTypesAreRepresentable() {
            return allTypesAreRepresentable;
        }

        public Matcher<Object> getMatcher() {
            return matcher;
        }

        public String[] getExpectedWarnings() {
            return expectedWarnings;
        }

        public Class<? extends Throwable> foldingExceptionClass() {
            return foldingExceptionClass;
        }

        public String foldingExceptionMessage() {
            return foldingExceptionMessage;
        }

        public String getExpectedTypeError() {
            return expectedTypeError;
        }

        public TestCase withWarning(String warning) {
            String[] newWarnings;
            if (expectedWarnings != null) {
                newWarnings = Arrays.copyOf(expectedWarnings, expectedWarnings.length + 1);
                newWarnings[expectedWarnings.length] = warning;
            } else {
                newWarnings = new String[] { warning };
            }
            return new TestCase(data, evaluatorToString, expectedType, matcher, newWarnings, expectedTypeError);
        }

        public <T extends Throwable> TestCase withFoldingException(Class<T> clazz, String message) {
            foldingExceptionClass = clazz;
            foldingExceptionMessage = message;
            return this;
        }
    }

    /**
     * Holds a supplier for a data value, along with the type of that value and a name for generating test case names. This mostly
     * exists because we can't generate random values from the test parameter generation functions, and instead need to return
     * suppliers which generate the random values at test execution time.
     */
    public record TypedDataSupplier(String name, Supplier<Object> supplier, DataType type) {
        public TypedData get() {
            return new TypedData(supplier.get(), type, name);
        }
    }

    /**
     * Holds a data value and the intended parse type of that value
     * @param data - value to test against
     * @param type - type of the value, for building expressions
     * @param name - a name for the value, used for generating test case names
     */
    public record TypedData(Object data, DataType type, String name) {

        public static final TypedData NULL = new TypedData(null, DataTypes.NULL, "<null>");

        public TypedData(Object data, String name) {
            this(data, EsqlDataTypes.fromJava(data), name);
        }

        @Override
        public String toString() {
            if (type == DataTypes.UNSIGNED_LONG && data instanceof Long longData) {
                return type.toString() + "(" + NumericUtils.unsignedLongAsBigInteger(longData).toString() + ")";
            }
            return type.toString() + "(" + (data == null ? "null" : data.toString()) + ")";
        }
    }
}
