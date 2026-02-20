/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractAggregationTestCase;
import org.elasticsearch.xpack.esql.expression.function.MultiRowTestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class WeightedAvgTests extends AbstractAggregationTestCase {
    public WeightedAvgTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        var suppliers = new ArrayList<TestCaseSupplier>();

        var numberCases = Stream.of(
            MultiRowTestCaseSupplier.intCases(1000, 1000, Integer.MIN_VALUE, Integer.MAX_VALUE, true),
            // Longs currently fail on overflow
            // Restore after https://github.com/elastic/elasticsearch/issues/110437
            // MultiRowTestCaseSupplier.longCases(1000, 1000, Long.MIN_VALUE, Long.MAX_VALUE, true),
            MultiRowTestCaseSupplier.doubleCases(1000, 1000, -Double.MAX_VALUE, Double.MAX_VALUE, true)
        ).flatMap(List::stream).toList();

        for (var number : numberCases) {
            for (var weight : numberCases) {
                suppliers.add(makeSupplier(number, weight));
            }
        }

        // No rows cases
        var noRowsCases = Stream.of(
            List.of(
                new TestCaseSupplier.TypedDataSupplier("integer no rows", List::of, DataType.INTEGER, false, true, List.of()),
                new TestCaseSupplier.TypedDataSupplier("long no rows", List::of, DataType.LONG, false, true, List.of()),
                new TestCaseSupplier.TypedDataSupplier("double no rows", List::of, DataType.DOUBLE, false, true, List.of())
            )
        ).flatMap(List::stream).toList();

        for (var number : noRowsCases) {
            for (var weight : noRowsCases) {
                suppliers.add(makeSupplier(number, weight));
            }
        }

        suppliers.addAll(
            List.of(
                // Folding
                new TestCaseSupplier(
                    List.of(DataType.INTEGER, DataType.INTEGER),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            TestCaseSupplier.TypedData.multiRow(List.of(5), DataType.INTEGER, "number"),
                            TestCaseSupplier.TypedData.multiRow(List.of(100), DataType.INTEGER, "weight")
                        ),
                        "WeightedAvg[number=Attribute[channel=0],weight=Attribute[channel=1]]",
                        DataType.DOUBLE,
                        equalTo(5.)
                    )
                ),
                new TestCaseSupplier(
                    List.of(DataType.LONG, DataType.INTEGER),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            TestCaseSupplier.TypedData.multiRow(List.of(5L), DataType.LONG, "number"),
                            TestCaseSupplier.TypedData.multiRow(List.of(100), DataType.INTEGER, "weight")
                        ),
                        "WeightedAvg[number=Attribute[channel=0],weight=Attribute[channel=1]]",
                        DataType.DOUBLE,
                        equalTo(5.)
                    )
                ),
                new TestCaseSupplier(
                    List.of(DataType.DOUBLE, DataType.INTEGER),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            TestCaseSupplier.TypedData.multiRow(List.of(5.), DataType.DOUBLE, "number"),
                            TestCaseSupplier.TypedData.multiRow(List.of(100), DataType.INTEGER, "weight")
                        ),
                        "WeightedAvg[number=Attribute[channel=0],weight=Attribute[channel=1]]",
                        DataType.DOUBLE,
                        equalTo(5.)
                    )
                )
            )
        );

        // Same as parameterSuppliersFromTypedDataWithDefaultChecks without withNoRowsExpectingNull(),
        // as it throws exceptions, and it's manually tested here
        return parameterSuppliersFromTypedData(randomizeBytesRefsOffset(suppliers));
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new WeightedAvg(source, args.get(0), args.get(1));
    }

    private static TestCaseSupplier makeSupplier(
        TestCaseSupplier.TypedDataSupplier fieldSupplier,
        TestCaseSupplier.TypedDataSupplier weightSupplier
    ) {
        return new TestCaseSupplier(
            fieldSupplier.name() + ", " + weightSupplier.name(),
            List.of(fieldSupplier.type(), weightSupplier.type()),
            () -> {
                var fieldTypedData = fieldSupplier.get();
                var weightTypedData = weightSupplier.get();

                var fieldValues = fieldTypedData.multiRowData();
                var weightValues = weightTypedData.multiRowData();

                if (fieldValues.size() != weightValues.size()) {
                    throw new IllegalArgumentException("Field and weight values must have the same size");
                }

                var warnings = new HashSet<String>();

                DataType mulType = fieldTypedData.type() == DataType.DOUBLE || weightTypedData.type() == DataType.DOUBLE
                    ? DataType.DOUBLE
                    : (fieldTypedData.type() == DataType.LONG || weightTypedData.type() == DataType.LONG
                        ? DataType.LONG
                        : DataType.INTEGER);

                // Calculate the results one by one to correctly track overflows and exceptions
                var validMulResults = new ArrayList<Double>();
                for (int i = 0; i < fieldValues.size(); i++) {
                    Number fieldNum = (Number) fieldValues.get(i);
                    Number weightNum = (Number) weightValues.get(i);

                    if (mulType == DataType.INTEGER) {
                        try {
                            int result = Math.multiplyExact(fieldNum.intValue(), weightNum.intValue());
                            validMulResults.add((double) result);
                        } catch (ArithmeticException e) {
                            warnings.add("Line 1:1: java.lang.ArithmeticException: integer overflow");
                        }
                    } else if (mulType == DataType.LONG) {
                        try {
                            long result = Math.multiplyExact(fieldNum.longValue(), weightNum.longValue());
                            validMulResults.add((double) result);
                        } catch (ArithmeticException e) {
                            warnings.add("Line 1:1: java.lang.ArithmeticException: long overflow");
                        }
                    } else {
                        double result = fieldNum.doubleValue() * weightNum.doubleValue();
                        if (Double.isFinite(result)) {
                            validMulResults.add(result);
                        } else {
                            warnings.add("Line 1:1: java.lang.ArithmeticException: not a finite double number: " + result);
                        }
                    }
                }

                // If all Mul operations failed, Sum returns null (no values to aggregate)
                // Otherwise, Sum returns the sum of valid values
                Double weightedSum = validMulResults.isEmpty() ? null : validMulResults.stream().mapToDouble(d -> d).sum();
                double totalWeights = weightValues.stream().mapToDouble(v -> ((Number) v).doubleValue()).sum();

                Double expected = null;
                if (weightedSum != null) {
                    if (totalWeights == 0.0 && fieldValues.isEmpty() == false) {
                        warnings.add("Line 1:1: java.lang.ArithmeticException: / by zero");
                    } else if (totalWeights != 0.0) {
                        var result = weightedSum / totalWeights;
                        if (Double.isInfinite(result) || Double.isNaN(result)) {
                            warnings.add("Line 1:1: java.lang.ArithmeticException: / by zero");
                        } else {
                            expected = result;
                        }
                    }
                }

                if (warnings.isEmpty() == false) {
                    warnings.add("Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.");
                }

                return new TestCaseSupplier.TestCase(
                    List.of(fieldTypedData, weightTypedData),
                    "WeightedAvg[number=Attribute[channel=0],weight=Attribute[channel=1]]",
                    DataType.DOUBLE,
                    expected == null ? nullValue() : closeTo(expected, Math.abs(expected * 1e-10))
                ).withWarnings(warnings);
            }
        );
    }
}
