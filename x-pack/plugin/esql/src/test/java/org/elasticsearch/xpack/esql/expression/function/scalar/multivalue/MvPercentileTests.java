/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.StringUtils;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.MultivalueTestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.Matcher;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class MvPercentileTests extends AbstractScalarFunctionTestCase {
    public MvPercentileTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> cases = new ArrayList<>();

        var fieldSuppliers = Stream.of(
            MultivalueTestCaseSupplier.intCases(Integer.MIN_VALUE, Integer.MAX_VALUE, true),
            MultivalueTestCaseSupplier.longCases(Long.MIN_VALUE, Long.MAX_VALUE, true),
            MultivalueTestCaseSupplier.doubleCases(-Double.MAX_VALUE, Double.MAX_VALUE, true)
        ).flatMap(List::stream).toList();

        var percentileSuppliers = Stream.of(
            TestCaseSupplier.intCases(0, 100, true),
            TestCaseSupplier.longCases(0, 100, true),
            TestCaseSupplier.doubleCases(0, 100, true)
        ).flatMap(List::stream).toList();

        for (var fieldSupplier : fieldSuppliers) {
            for (var percentileSupplier : percentileSuppliers) {
                cases.add(makeSupplier(fieldSupplier, percentileSupplier));
            }
        }

        for (var percentileType : List.of(INTEGER, LONG, DOUBLE)) {
            cases.addAll(
                List.of(
                    // Doubles
                    new TestCaseSupplier(
                        "median double",
                        List.of(DOUBLE, percentileType),
                        () -> new TestCaseSupplier.TestCase(
                            List.of(
                                new TestCaseSupplier.TypedData(List.of(-10., 5., 10.), DOUBLE, "field"),
                                percentileWithType(50, percentileType)
                            ),
                            evaluatorString(DOUBLE, percentileType),
                            DOUBLE,
                            equalTo(5.)
                        )
                    ),
                    new TestCaseSupplier(
                        "single value double",
                        List.of(DOUBLE, percentileType),
                        () -> new TestCaseSupplier.TestCase(
                            List.of(
                                new TestCaseSupplier.TypedData(List.of(55.), DOUBLE, "field"),
                                percentileWithType(randomIntBetween(0, 100), percentileType)
                            ),
                            evaluatorString(DOUBLE, percentileType),
                            DOUBLE,
                            equalTo(55.)
                        )
                    ),
                    new TestCaseSupplier(
                        "p0 double",
                        List.of(DOUBLE, percentileType),
                        () -> new TestCaseSupplier.TestCase(
                            List.of(
                                new TestCaseSupplier.TypedData(List.of(-10., 5., 10.), DOUBLE, "field"),
                                percentileWithType(0, percentileType)
                            ),
                            evaluatorString(DOUBLE, percentileType),
                            DOUBLE,
                            equalTo(-10.)
                        )
                    ),
                    new TestCaseSupplier(
                        "p100 double",
                        List.of(DOUBLE, percentileType),
                        () -> new TestCaseSupplier.TestCase(
                            List.of(
                                new TestCaseSupplier.TypedData(List.of(-10., 5., 10.), DOUBLE, "field"),
                                percentileWithType(100, percentileType)
                            ),
                            evaluatorString(DOUBLE, percentileType),
                            DOUBLE,
                            equalTo(10.)
                        )
                    ),
                    new TestCaseSupplier(
                        "averaged double",
                        List.of(DOUBLE, percentileType),
                        () -> new TestCaseSupplier.TestCase(
                            List.of(
                                new TestCaseSupplier.TypedData(List.of(-10., 5., 10.), DOUBLE, "field"),
                                percentileWithType(75, percentileType)
                            ),
                            evaluatorString(DOUBLE, percentileType),
                            DOUBLE,
                            equalTo(7.5)
                        )
                    ),
                    new TestCaseSupplier(
                        "big double difference",
                        List.of(DOUBLE, percentileType),
                        () -> new TestCaseSupplier.TestCase(
                            List.of(
                                new TestCaseSupplier.TypedData(List.of(-Double.MAX_VALUE, Double.MAX_VALUE), DOUBLE, "field"),
                                percentileWithType(50, percentileType)
                            ),
                            evaluatorString(DOUBLE, percentileType),
                            DOUBLE,
                            closeTo(0, 0.0000001)
                        )
                    ),

                    // Int
                    new TestCaseSupplier(
                        "median int",
                        List.of(INTEGER, percentileType),
                        () -> new TestCaseSupplier.TestCase(
                            List.of(
                                new TestCaseSupplier.TypedData(List.of(-10, 5, 10), INTEGER, "field"),
                                percentileWithType(50, percentileType)
                            ),
                            evaluatorString(INTEGER, percentileType),
                            INTEGER,
                            equalTo(5)
                        )
                    ),
                    new TestCaseSupplier(
                        "single value int",
                        List.of(INTEGER, percentileType),
                        () -> new TestCaseSupplier.TestCase(
                            List.of(
                                new TestCaseSupplier.TypedData(List.of(55), INTEGER, "field"),
                                percentileWithType(randomIntBetween(0, 100), percentileType)
                            ),
                            evaluatorString(INTEGER, percentileType),
                            INTEGER,
                            equalTo(55)
                        )
                    ),
                    new TestCaseSupplier(
                        "p0 int",
                        List.of(INTEGER, percentileType),
                        () -> new TestCaseSupplier.TestCase(
                            List.of(
                                new TestCaseSupplier.TypedData(List.of(-10, 5, 10), INTEGER, "field"),
                                percentileWithType(0, percentileType)
                            ),
                            evaluatorString(INTEGER, percentileType),
                            INTEGER,
                            equalTo(-10)
                        )
                    ),
                    new TestCaseSupplier(
                        "p100 int",
                        List.of(INTEGER, percentileType),
                        () -> new TestCaseSupplier.TestCase(
                            List.of(
                                new TestCaseSupplier.TypedData(List.of(-10, 5, 10), INTEGER, "field"),
                                percentileWithType(100, percentileType)
                            ),
                            evaluatorString(INTEGER, percentileType),
                            INTEGER,
                            equalTo(10)
                        )
                    ),
                    new TestCaseSupplier(
                        "averaged int",
                        List.of(INTEGER, percentileType),
                        () -> new TestCaseSupplier.TestCase(
                            List.of(
                                new TestCaseSupplier.TypedData(List.of(-10, 5, 10), INTEGER, "field"),
                                percentileWithType(75, percentileType)
                            ),
                            evaluatorString(INTEGER, percentileType),
                            INTEGER,
                            equalTo(7)
                        )
                    ),
                    new TestCaseSupplier(
                        "big int difference",
                        List.of(INTEGER, percentileType),
                        () -> new TestCaseSupplier.TestCase(
                            List.of(
                                new TestCaseSupplier.TypedData(List.of(Integer.MIN_VALUE, Integer.MAX_VALUE), INTEGER, "field"),
                                percentileWithType(50, percentileType)
                            ),
                            evaluatorString(INTEGER, percentileType),
                            INTEGER,
                            equalTo(-1) // Negative max is 1 smaller than positive max
                        )
                    ),

                    // Long
                    new TestCaseSupplier(
                        "median long",
                        List.of(LONG, percentileType),
                        () -> new TestCaseSupplier.TestCase(
                            List.of(
                                new TestCaseSupplier.TypedData(List.of(-10L, 5L, 10L), LONG, "field"),
                                percentileWithType(50, percentileType)
                            ),
                            evaluatorString(LONG, percentileType),
                            LONG,
                            equalTo(5L)
                        )
                    ),
                    new TestCaseSupplier(
                        "single value long",
                        List.of(LONG, percentileType),
                        () -> new TestCaseSupplier.TestCase(
                            List.of(
                                new TestCaseSupplier.TypedData(List.of(55L), LONG, "field"),
                                percentileWithType(randomIntBetween(0, 100), percentileType)
                            ),
                            evaluatorString(LONG, percentileType),
                            LONG,
                            equalTo(55L)
                        )
                    ),
                    new TestCaseSupplier(
                        "p0 long",
                        List.of(LONG, percentileType),
                        () -> new TestCaseSupplier.TestCase(
                            List.of(
                                new TestCaseSupplier.TypedData(List.of(-10L, 5L, 10L), LONG, "field"),
                                percentileWithType(0, percentileType)
                            ),
                            evaluatorString(LONG, percentileType),
                            LONG,
                            equalTo(-10L)
                        )
                    ),
                    new TestCaseSupplier(
                        "p100 long",
                        List.of(LONG, percentileType),
                        () -> new TestCaseSupplier.TestCase(
                            List.of(
                                new TestCaseSupplier.TypedData(List.of(-10L, 5L, 10L), LONG, "field"),
                                percentileWithType(100, percentileType)
                            ),
                            evaluatorString(LONG, percentileType),
                            LONG,
                            equalTo(10L)
                        )
                    ),
                    new TestCaseSupplier(
                        "averaged long",
                        List.of(LONG, percentileType),
                        () -> new TestCaseSupplier.TestCase(
                            List.of(
                                new TestCaseSupplier.TypedData(List.of(-10L, 5L, 10L), LONG, "field"),
                                percentileWithType(75, percentileType)
                            ),
                            evaluatorString(LONG, percentileType),
                            LONG,
                            equalTo(7L)
                        )
                    ),
                    new TestCaseSupplier(
                        "big long difference",
                        List.of(LONG, percentileType),
                        () -> new TestCaseSupplier.TestCase(
                            List.of(
                                new TestCaseSupplier.TypedData(List.of(Long.MIN_VALUE, Long.MAX_VALUE), LONG, "field"),
                                percentileWithType(50, percentileType)
                            ),
                            evaluatorString(LONG, percentileType),
                            LONG,
                            equalTo(0L)
                        )
                    )
                )
            );

            for (var fieldType : List.of(INTEGER, LONG, DataType.DOUBLE)) {
                cases.add(
                    new TestCaseSupplier(
                        "out of bounds percentile <" + fieldType + ", " + percentileType + ">",
                        List.of(fieldType, percentileType),
                        () -> {
                            var percentile = numberWithType(
                                randomBoolean() ? randomIntBetween(Integer.MIN_VALUE, -1) : randomIntBetween(101, Integer.MAX_VALUE),
                                percentileType
                            );
                            return new TestCaseSupplier.TestCase(
                                List.of(
                                    new TestCaseSupplier.TypedData(numberWithType(0, fieldType), fieldType, "field"),
                                    new TestCaseSupplier.TypedData(percentile, percentileType, "percentile")
                                ),
                                evaluatorString(fieldType, percentileType),
                                fieldType,
                                nullValue()
                            ).withWarning(
                                "Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded."
                            )
                                .withWarning(
                                    "Line 1:1: java.lang.IllegalArgumentException: Percentile parameter must be "
                                        + "a number between 0 and 100, found ["
                                        + percentile.doubleValue()
                                        + "]"
                                );
                        }
                    )
                );
            }
        }
        cases.add(
            new TestCaseSupplier(
                "from example",
                List.of(DOUBLE, INTEGER),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(List.of(-3.34, -6.33, 6.23, -0.31), DOUBLE, "field"),
                        new TestCaseSupplier.TypedData(75, INTEGER, "percentile")
                    ),
                    evaluatorString(DOUBLE, INTEGER),
                    DOUBLE,
                    equalTo(1.325)
                )
            )
        );

        return parameterSuppliersFromTypedDataWithDefaultChecksNoErrors(
            (nullPosition, nullValueDataType, original) -> nullValueDataType == DataType.NULL && nullPosition == 0
                ? DataType.NULL
                : original.expectedType(),
            (nullPosition, nullData, original) -> original,
            cases
        );
    }

    @SuppressWarnings("unchecked")
    private static TestCaseSupplier makeSupplier(
        TestCaseSupplier.TypedDataSupplier fieldSupplier,
        TestCaseSupplier.TypedDataSupplier percentileSupplier
    ) {
        return new TestCaseSupplier(
            "field: " + fieldSupplier.name() + ", percentile: " + percentileSupplier.name(),
            List.of(fieldSupplier.type(), percentileSupplier.type()),
            () -> {
                var fieldTypedData = fieldSupplier.get();
                var percentileTypedData = percentileSupplier.get();

                var values = (List<Number>) fieldTypedData.data();
                var percentile = ((Number) percentileTypedData.data()).doubleValue();

                var expectedMatcher = makePercentileMatcher(values, percentile);

                return new TestCaseSupplier.TestCase(
                    List.of(fieldTypedData, percentileTypedData),
                    evaluatorString(fieldSupplier.type(), percentileSupplier.type()),
                    fieldSupplier.type(),
                    expectedMatcher
                );
            }
        );
    }

    private static Matcher<?> makePercentileMatcher(List<Number> rawValues, double percentile) {
        if (rawValues.isEmpty() || percentile < 0 || percentile > 100) {
            return nullValue();
        }

        if (rawValues.size() == 1) {
            return equalTo(rawValues.get(0));
        }

        int valueCount = rawValues.size();
        var p = percentile / 100.0;
        var index = p * (valueCount - 1);
        var lowerIndex = (int) index;
        var upperIndex = lowerIndex + 1;
        var fraction = index - lowerIndex;

        if (rawValues.get(0) instanceof Integer) {
            var values = rawValues.stream().mapToInt(Number::intValue).sorted().toArray();
            int expected;

            if (percentile == 0) {
                expected = values[0];
            } else if (percentile == 100) {
                expected = values[valueCount - 1];
            } else {
                assert lowerIndex >= 0 && upperIndex < valueCount;
                var difference = (long) values[upperIndex] - values[lowerIndex];
                expected = values[lowerIndex] + (int) (fraction * difference);
            }

            return equalTo(expected);
        }

        if (rawValues.get(0) instanceof Long) {
            var values = rawValues.stream().mapToLong(Number::longValue).sorted().toArray();
            long expected;

            if (percentile == 0) {
                expected = values[0];
            } else if (percentile == 100) {
                expected = values[valueCount - 1];
            } else {
                assert lowerIndex >= 0 && upperIndex < valueCount;
                expected = calculatePercentile(fraction, BigDecimal.valueOf(values[lowerIndex]), BigDecimal.valueOf(values[upperIndex]))
                    .longValue();
            }

            // Double*bigLong may lose precision, we allow a small range
            return anyOf(equalTo(Math.min(expected, expected - 1)), equalTo(expected), equalTo(Math.max(expected, expected + 1)));
        }

        if (rawValues.get(0) instanceof Double) {
            var values = rawValues.stream().mapToDouble(Number::doubleValue).sorted().toArray();
            double expected;

            if (percentile == 0) {
                expected = values[0];
            } else if (percentile == 100) {
                expected = values[valueCount - 1];
            } else {
                assert lowerIndex >= 0 && upperIndex < valueCount;
                expected = calculatePercentile(fraction, new BigDecimal(values[lowerIndex]), new BigDecimal(values[upperIndex]))
                    .doubleValue();
            }

            return closeTo(expected, Math.abs(expected * 0.0000001));
        }

        throw new IllegalArgumentException("Unsupported type: " + rawValues.get(0).getClass());
    }

    private static BigDecimal calculatePercentile(double fraction, BigDecimal lowerValue, BigDecimal upperValue) {
        var difference = upperValue.subtract(lowerValue);
        return lowerValue.add(new BigDecimal(fraction).multiply(difference));
    }

    private static TestCaseSupplier.TypedData percentileWithType(Number value, DataType type) {
        return new TestCaseSupplier.TypedData(numberWithType(value, type), type, "percentile");
    }

    private static Number numberWithType(Number value, DataType type) {
        return switch (type) {
            case INTEGER -> value.intValue();
            case LONG -> value.longValue();
            default -> value.doubleValue();
        };
    }

    private static String evaluatorString(DataType fieldDataType, DataType percentileDataType) {
        var fieldTypeName = StringUtils.underscoreToLowerCamelCase(fieldDataType.name());

        fieldTypeName = fieldTypeName.substring(0, 1).toUpperCase(Locale.ROOT) + fieldTypeName.substring(1);

        var percentileEvaluator = TestCaseSupplier.castToDoubleEvaluator("Attribute[channel=1]", percentileDataType);

        return "MvPercentile" + fieldTypeName + "Evaluator[values=Attribute[channel=0], percentile=" + percentileEvaluator + "]";
    }

    @Override
    protected final Expression build(Source source, List<Expression> args) {
        return new MvPercentile(source, args.get(0), args.get(1));
    }
}
