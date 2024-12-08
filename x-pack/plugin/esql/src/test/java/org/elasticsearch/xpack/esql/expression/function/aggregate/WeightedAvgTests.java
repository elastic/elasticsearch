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
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;

public class WeightedAvgTests extends AbstractAggregationTestCase {
    public WeightedAvgTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        var suppliers = new ArrayList<TestCaseSupplier>();

        var numberCases = Stream.of(
            MultiRowTestCaseSupplier.intCases(1000, 1000, Integer.MIN_VALUE, Integer.MAX_VALUE, true),
            MultiRowTestCaseSupplier.longCases(1000, 1000, Long.MIN_VALUE, Long.MAX_VALUE, true),
            MultiRowTestCaseSupplier.doubleCases(1000, 1000, -Double.MAX_VALUE, Double.MAX_VALUE, true)
        ).flatMap(List::stream).toList();

        // Most of those cases, if not all, will be ignored, as they use complex surrogates.
        // Kept here to correctly generate the function types docs, and in case the tests are later improved to support them.
        for (var number : numberCases) {
            for (var weight : numberCases) {
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

        return parameterSuppliersFromTypedDataWithDefaultChecks(suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new WeightedAvg(source, args.get(0), args.get(1));
    }

    private static TestCaseSupplier makeSupplier(
        TestCaseSupplier.TypedDataSupplier fieldSupplier,
        TestCaseSupplier.TypedDataSupplier weightSupplier
    ) {
        return new TestCaseSupplier(List.of(fieldSupplier.type(), weightSupplier.type()), () -> {
            var fieldTypedData = fieldSupplier.get();
            var weightTypedData = weightSupplier.get();

            var fieldValues = fieldTypedData.multiRowData();
            var weightValues = weightTypedData.multiRowData();

            if (fieldValues.size() != weightValues.size()) {
                throw new IllegalArgumentException("Field and weight values must have the same size");
            }

            var weightedSum = IntStream.range(0, fieldValues.size())
                .mapToDouble(i -> ((Number) fieldValues.get(i)).doubleValue() * ((Number) weightValues.get(i)).doubleValue())
                .sum();
            var totalWeights = weightValues.stream().mapToDouble(v -> ((Number) v).doubleValue()).sum();

            var expected = totalWeights == 0 ? null : weightedSum / totalWeights;

            return new TestCaseSupplier.TestCase(
                List.of(fieldTypedData, weightTypedData),
                "WeightedAvg[number=Attribute[channel=0],weight=Attribute[channel=1]]",
                DataType.DOUBLE,
                equalTo(expected)
            );
        });
    }
}
