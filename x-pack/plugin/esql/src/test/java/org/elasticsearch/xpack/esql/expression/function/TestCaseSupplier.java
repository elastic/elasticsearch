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

    public static final BigInteger MAX_UNSIGNED_LONG = BigInteger.valueOf(1 << 64).subtract(BigInteger.ONE);
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
            i -> expected.applyAsDouble(i),
            Integer.MIN_VALUE,
            Integer.MAX_VALUE
        );
        forUnaryLong(
            suppliers,
            eval + castToDoubleEvaluator(read, DataTypes.LONG) + "]",
            DataTypes.DOUBLE,
            l -> expected.applyAsDouble(l),
            Long.MIN_VALUE,
            Long.MAX_VALUE
        );
        forUnaryUnsignedLong(
            suppliers,
            eval + castToDoubleEvaluator(read, DataTypes.UNSIGNED_LONG) + "]",
            DataTypes.DOUBLE,
            ul -> expected.applyAsDouble(ul.doubleValue()),
            BigInteger.ZERO,
            MAX_UNSIGNED_LONG
        );
        forUnaryDouble(
            suppliers,
            eval + read + "]",
            DataTypes.DOUBLE,
            i -> expected.applyAsDouble(i),
            Double.NEGATIVE_INFINITY,
            Double.POSITIVE_INFINITY
        );
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
        IntFunction<Object> expectedValue,
        int lowerBound,
        int upperBound
    ) {
        unaryNumeric(
            suppliers,
            expectedEvaluatorToString,
            DataTypes.INTEGER,
            intCases(lowerBound, upperBound),
            expectedType,
            n -> expectedValue.apply(n.intValue())
        );
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
        long upperBound
    ) {
        unaryNumeric(
            suppliers,
            expectedEvaluatorToString,
            DataTypes.LONG,
            longCases(lowerBound, upperBound),
            expectedType,
            n -> expectedValue.apply(n.longValue())
        );
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
        BigInteger upperBound
    ) {
        unaryNumeric(
            suppliers,
            expectedEvaluatorToString,
            DataTypes.UNSIGNED_LONG,
            ulongCases(lowerBound, upperBound),
            expectedType,
            n -> expectedValue.apply((BigInteger) n)
        );
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
        double upperBound
    ) {
        unaryNumeric(
            suppliers,
            expectedEvaluatorToString,
            DataTypes.DOUBLE,
            doubleCases(lowerBound, upperBound),
            expectedType,
            n -> expectedValue.apply(n.doubleValue())
        );
    }

    private static void unaryNumeric(
        List<TestCaseSupplier> suppliers,
        String expectedEvaluatorToString,
        DataType inputType,
        List<Map.Entry<String, Supplier<Object>>> valueSuppliers,
        DataType expectedOutputType,
        Function<Number, Object> expected
    ) {
        for (Map.Entry<String, Supplier<Object>> supplier : valueSuppliers) {
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

    private static List<Map.Entry<String, Supplier<Object>>> intCases(int min, int max) {
        List<Map.Entry<String, Supplier<Object>>> cases = new ArrayList<>();
        if (0 <= max && 0 >= min) {
            cases.add(Map.entry("<0 int>", () -> 0));
        }

        int lower = Math.max(min, 1);
        int upper = Math.min(max, Integer.MAX_VALUE);
        if (lower < upper) {
            cases.add(Map.entry("<positive int>", () -> ESTestCase.randomIntBetween(lower, upper)));
        } else if (lower == upper) {
            cases.add(Map.entry("<" + lower + " int>", () -> lower));
        }

        int lower1 = Math.max(min, Integer.MIN_VALUE);
        int upper1 = Math.min(max, -1);
        if (lower1 < upper1) {
            cases.add(Map.entry("<negative int>", () -> ESTestCase.randomIntBetween(lower1, upper1)));
        } else if (lower1 == upper1) {
            cases.add(Map.entry("<" + lower1 + " int>", () -> lower1));
        }
        return cases;
    }

    private static List<Map.Entry<String, Supplier<Object>>> longCases(long min, long max) {
        List<Map.Entry<String, Supplier<Object>>> cases = new ArrayList<>();
        if (0L <= max && 0L >= min) {
            cases.add(Map.entry("<0 long>", () -> 0L));
        }

        long lower = Math.max(min, 1);
        long upper = Math.min(max, Long.MAX_VALUE);
        if (lower < upper) {
            cases.add(Map.entry("<positive long>", () -> ESTestCase.randomLongBetween(lower, upper)));
        } else if (lower == upper) {
            cases.add(Map.entry("<" + lower + " long>", () -> lower));
        }

        long lower1 = Math.max(min, Long.MIN_VALUE);
        long upper1 = Math.min(max, -1);
        if (lower1 < upper1) {
            cases.add(Map.entry("<negative long>", () -> ESTestCase.randomLongBetween(lower1, upper1)));
        } else if (lower1 == upper1) {
            cases.add(Map.entry("<" + lower1 + " long>", () -> lower1));
        }

        return cases;
    }

    private static List<Map.Entry<String, Supplier<Object>>> ulongCases(BigInteger min, BigInteger max) {
        List<Map.Entry<String, Supplier<Object>>> cases = new ArrayList<>();

        // Zero
        if (BigInteger.ZERO.compareTo(max) <= 0 && BigInteger.ZERO.compareTo(min) >= 0) {
            cases.add(Map.entry("<0 unsigned long>", () -> BigInteger.ZERO));
        }

        // small values, less than Long.MAX_VALUE
        BigInteger lower1 = min.max(BigInteger.ONE);
        BigInteger upper1 = max.min(BigInteger.valueOf(Integer.MAX_VALUE));
        if (lower1.compareTo(upper1) < 0) {
            cases.add(
                Map.entry(
                    "<small unsigned long>",
                    () -> BigInteger.valueOf(ESTestCase.randomLongBetween(lower1.longValue(), upper1.longValue()))
                )
            );
        } else if (lower1.compareTo(upper1) == 0) {
            cases.add(Map.entry("<small unsigned long>", () -> lower1));
        }

        // Big values, greater than Long.MAX_VALUE
        BigInteger lower2 = min.max(BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE));
        BigInteger upper2 = max.min(BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.valueOf(Integer.MAX_VALUE)));
        if (lower2.compareTo(upper2) < 0) {
            cases.add(
                Map.entry(
                    "<small unsigned long>",
                    () -> BigInteger.valueOf(ESTestCase.randomLongBetween(lower2.longValue(), upper2.longValue()))
                )
            );
        } else if (lower2.compareTo(upper2) == 0) {
            cases.add(Map.entry("<small unsigned long>", () -> lower2));
        }
        return cases;
    }

    private static List<Map.Entry<String, Supplier<Object>>> doubleCases(double min, double max) {
        List<Map.Entry<String, Supplier<Object>>> cases = new ArrayList<>();

        // Zeros
        if (0d <= max && 0d >= min) {
            cases.add(Map.entry("<0 double>", () -> 0.0d));
            cases.add(Map.entry("<-0 double>", () -> -0.0d));
        }

        // Positive small double
        double lower1 = Math.max(0d, min);
        double upper1 = Math.min(1d, max);
        if (lower1 < upper1) {
            cases.add(Map.entry("<small positive double>", () -> ESTestCase.randomDoubleBetween(lower1, upper1, true)));
        } else if (lower1 == upper1) {
            cases.add(Map.entry("<small positive double>", () -> lower1));
        }

        // Negative small double
        double lower2 = Math.max(-1d, min);
        double upper2 = Math.min(0d, max);
        if (lower2 < upper2) {
            cases.add(Map.entry("<small negative double>", () -> ESTestCase.randomDoubleBetween(lower2, upper2, true)));
        } else if (lower2 == upper2) {
            cases.add(Map.entry("<small negative double>", () -> lower2));
        }

        // Positive big double
        double lower3 = Math.max(1d, min); // start at 1 (inclusive) because the density of values between 0 and 1 is very high
        double upper3 = Math.min(Double.MAX_VALUE, max);
        if (lower3 < upper3) {
            cases.add(Map.entry("<big positive double>", () -> ESTestCase.randomDoubleBetween(lower3, upper3, true)));
        } else if (lower3 == upper3) {
            cases.add(Map.entry("<big positive double>", () -> lower3));
        }

        // Negative big double
        // note: Double.MIN_VALUE is the smallest non-zero positive double, not the smallest non-infinite negative double.
        double lower4 = Math.max(-Double.MAX_VALUE, min);
        double upper4 = Math.min(-1, max); // because again, the interval from -1 to 0 is very high density
        if (lower4 < upper4) {
            cases.add(Map.entry("<big negative double>", () -> ESTestCase.randomDoubleBetween(lower4, upper4, true)));
        } else if (lower4 == upper4) {
            cases.add(Map.entry("<big negative double>", () -> lower4));
        }
        return cases;
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
