/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.util.NumericUtils;
import org.hamcrest.Matcher;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.DoubleBinaryOperator;
import java.util.function.DoubleFunction;
import java.util.function.DoubleUnaryOperator;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.LongFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;

/**
 * This class exists to give a human-readable string representation of the test case.
 */
public record TestCaseSupplier(String name, List<DataType> types, Supplier<TestCase> supplier)
    implements
        Supplier<TestCaseSupplier.TestCase> {
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
                    throw new IllegalStateException("supplier/data type mismatch " + supplied.getData().get(i).type() + "/" + types.get(i));
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
    public static List<TestCaseSupplier> forUnaryCastingToDouble(String name, String argName, DoubleUnaryOperator expected) {
        String read = "Attribute[channel=0]";
        String eval = name + "[" + argName + "=";
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        forUnaryInt(
            suppliers,
            eval + castToDoubleEvaluator(read, DataTypes.INTEGER) + "]",
            DataTypes.DOUBLE,
            i -> expected.applyAsDouble(i)
        );
        forUnaryLong(suppliers, eval + castToDoubleEvaluator(read, DataTypes.LONG) + "]", DataTypes.DOUBLE, l -> expected.applyAsDouble(l));
        forUnaryUnsignedLong(
            suppliers,
            eval + castToDoubleEvaluator(read, DataTypes.UNSIGNED_LONG) + "]",
            DataTypes.DOUBLE,
            ul -> expected.applyAsDouble(ul.doubleValue())
        );
        forUnaryDouble(suppliers, eval + read + "]", DataTypes.DOUBLE, i -> expected.applyAsDouble(i));
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
        DoubleBinaryOperator expected
    ) {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        for (DataType lhsType : EsqlDataTypes.types()) {
            if (lhsType.isNumeric() == false || EsqlDataTypes.isRepresentable(lhsType) == false) {
                continue;
            }
            for (Map.Entry<String, Supplier<Object>> lhsSupplier : RANDOM_VALUE_SUPPLIERS.get(lhsType)) {
                for (DataType rhsType : EsqlDataTypes.types()) {
                    if (rhsType.isNumeric() == false || EsqlDataTypes.isRepresentable(rhsType) == false) {
                        continue;
                    }
                    for (Map.Entry<String, Supplier<Object>> rhsSupplier : RANDOM_VALUE_SUPPLIERS.get(rhsType)) {
                        String caseName = lhsSupplier.getKey() + ", " + rhsSupplier.getKey();
                        suppliers.add(new TestCaseSupplier(caseName, List.of(lhsType, rhsType), () -> {
                            Number lhs = (Number) lhsSupplier.getValue().get();
                            Number rhs = (Number) rhsSupplier.getValue().get();
                            TypedData lhsTyped = new TypedData(
                                // TODO there has to be a better way to handle unsigned long
                                lhs instanceof BigInteger b ? NumericUtils.asLongUnsigned(b) : lhs,
                                lhsType,
                                "lhs"
                            );
                            TypedData rhsTyped = new TypedData(
                                rhs instanceof BigInteger b ? NumericUtils.asLongUnsigned(b) : rhs,
                                rhsType,
                                "rhs"
                            );
                            String lhsEvalName = castToDoubleEvaluator("Attribute[channel=0]", lhsType);
                            String rhsEvalName = castToDoubleEvaluator("Attribute[channel=1]", rhsType);
                            return new TestCase(
                                List.of(lhsTyped, rhsTyped),
                                name + "[" + lhsName + "=" + lhsEvalName + ", " + rhsName + "=" + rhsEvalName + "]",
                                DataTypes.DOUBLE,
                                equalTo(expected.applyAsDouble(lhs.doubleValue(), rhs.doubleValue()))
                            );
                        }));
                    }
                }
            }
        }
        return suppliers;
    }

    /**
     * Generate positive test cases for a unary function operating on an {@link DataTypes#INTEGER}.
     */
    public static void forUnaryInt(
        List<TestCaseSupplier> suppliers,
        String expectedEvaluatorToString,
        DataType expectedType,
        IntFunction<Object> expectedValue
    ) {
        unaryNumeric(suppliers, expectedEvaluatorToString, DataTypes.INTEGER, expectedType, n -> expectedValue.apply(n.intValue()));
    }

    /**
     * Generate positive test cases for a unary function operating on an {@link DataTypes#LONG}.
     */
    public static void forUnaryLong(
        List<TestCaseSupplier> suppliers,
        String expectedEvaluatorToString,
        DataType expectedType,
        LongFunction<Object> expectedValue
    ) {
        unaryNumeric(suppliers, expectedEvaluatorToString, DataTypes.LONG, expectedType, n -> expectedValue.apply(n.longValue()));
    }

    /**
     * Generate positive test cases for a unary function operating on an {@link DataTypes#UNSIGNED_LONG}.
     */
    public static void forUnaryUnsignedLong(
        List<TestCaseSupplier> suppliers,
        String expectedEvaluatorToString,
        DataType expectedType,
        Function<BigInteger, Object> expectedValue
    ) {
        unaryNumeric(suppliers, expectedEvaluatorToString, DataTypes.UNSIGNED_LONG, expectedType, n -> expectedValue.apply((BigInteger) n));
    }

    /**
     * Generate positive test cases for a unary function operating on an {@link DataTypes#DOUBLE}.
     */
    public static void forUnaryDouble(
        List<TestCaseSupplier> suppliers,
        String expectedEvaluatorToString,
        DataType expectedType,
        DoubleFunction<Object> expectedValue
    ) {
        unaryNumeric(suppliers, expectedEvaluatorToString, DataTypes.DOUBLE, expectedType, n -> expectedValue.apply(n.doubleValue()));
    }

    private static void unaryNumeric(
        List<TestCaseSupplier> suppliers,
        String expectedEvaluatorToString,
        DataType inputType,
        DataType expectedOutputType,
        Function<Number, Object> expected
    ) {
        for (Map.Entry<String, Supplier<Object>> supplier : RANDOM_VALUE_SUPPLIERS.get(inputType)) {
            suppliers.add(new TestCaseSupplier(supplier.getKey(), List.of(inputType), () -> {
                Number value = (Number) supplier.getValue().get();
                TypedData typed = new TypedData(
                    // TODO there has to be a better way to handle unsigned long
                    value instanceof BigInteger b ? NumericUtils.asLongUnsigned(b) : value,
                    inputType,
                    "value"
                );
                return new TestCase(List.of(typed), expectedEvaluatorToString, expectedOutputType, equalTo(expected.apply(value)));
            }));
        }
    }

    private static final Map<DataType, List<Map.Entry<String, Supplier<Object>>>> RANDOM_VALUE_SUPPLIERS = Map.ofEntries(
        Map.entry(
            DataTypes.DOUBLE,
            List.of(
                Map.entry("<0 double>", () -> 0.0d),
                Map.entry("<small positive double>", () -> ESTestCase.randomDouble()),
                Map.entry("<small negative double>", () -> -ESTestCase.randomDouble()),
                Map.entry("<big positive double>", () -> ESTestCase.randomDoubleBetween(0, Double.MAX_VALUE, false)),
                Map.entry("<negative positive double>", () -> ESTestCase.randomDoubleBetween(Double.MIN_VALUE, 0 - Double.MIN_NORMAL, true))
            )
        ),
        Map.entry(
            DataTypes.LONG,
            List.of(
                Map.entry("<0 long>", () -> 0L),
                Map.entry("<positive long>", () -> ESTestCase.randomLongBetween(1, Long.MAX_VALUE)),
                Map.entry("<negative long>", () -> ESTestCase.randomLongBetween(Long.MIN_VALUE, -1))
            )
        ),
        Map.entry(
            DataTypes.INTEGER,
            List.of(
                Map.entry("<0 int>", () -> 0),
                Map.entry("<positive long>", () -> ESTestCase.between(1, Integer.MAX_VALUE)),
                Map.entry("<negative long>", () -> ESTestCase.between(Integer.MIN_VALUE, -1))
            )
        ),
        Map.entry(
            DataTypes.UNSIGNED_LONG,
            List.of(
                Map.entry("<0 unsigned long>", () -> BigInteger.ZERO),
                Map.entry("<small unsigned long>", () -> BigInteger.valueOf(ESTestCase.randomLongBetween(1, Integer.MAX_VALUE))),
                Map.entry(
                    "<big unsigned long>",
                    () -> BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.valueOf(ESTestCase.randomLongBetween(1, Integer.MAX_VALUE)))
                )
            )
        )
    );

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
         * The expected toString output for the evaluator this fuction invocation should generate
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

        private final String expectedTypeError;

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

        public List<Expression> getDataAsLiterals() {
            return data.stream().map(t -> new Literal(Source.synthetic(t.name()), t.data(), t.type())).collect(Collectors.toList());
        }

        public List<Object> getDataValues() {
            return data.stream().map(t -> t.data()).collect(Collectors.toList());
        }

        public Matcher<Object> getMatcher() {
            return matcher;
        }

        public String[] getExpectedWarnings() {
            return expectedWarnings;
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
    }

    /**
     * Holds a data value and the intended parse type of that value
     * @param data - value to test against
     * @param type - type of the value, for building expressions
     */
    public record TypedData(Object data, DataType type, String name) {
        public TypedData(Object data, String name) {
            this(data, EsqlDataTypes.fromJava(data), name);
        }
    }
}
