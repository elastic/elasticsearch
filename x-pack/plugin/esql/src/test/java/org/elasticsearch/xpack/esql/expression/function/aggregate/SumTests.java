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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.esql.core.type.DataType.UNSIGNED_LONG;
import static org.hamcrest.Matchers.equalTo;

public class SumTests extends AbstractAggregationTestCase {
    public SumTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        var suppliers = new ArrayList<TestCaseSupplier>();

        Stream.of(MultiRowTestCaseSupplier.intCases(1, 1000, Integer.MIN_VALUE, Integer.MAX_VALUE, true)
        // Longs currently throw on overflow.
        // Restore after https://github.com/elastic/elasticsearch/issues/110437
        // MultiRowTestCaseSupplier.longCases(1, 1000, Long.MIN_VALUE, Long.MAX_VALUE, true),
        // Doubles currently return +/-Infinity on overflow.
        // Restore after https://github.com/elastic/elasticsearch/issues/111026
        // MultiRowTestCaseSupplier.doubleCases(1, 1000, -Double.MAX_VALUE, Double.MAX_VALUE, true)
        ).flatMap(List::stream).map(SumTests::makeSupplier).collect(Collectors.toCollection(() -> suppliers));

        suppliers.addAll(
            List.of(
                // Folding
                new TestCaseSupplier(
                    List.of(DataType.INTEGER),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(TestCaseSupplier.TypedData.multiRow(List.of(200), DataType.INTEGER, "field")),
                        "Sum[field=Attribute[channel=0]]",
                        DataType.LONG,
                        equalTo(200L)
                    )
                ),
                new TestCaseSupplier(
                    List.of(DataType.LONG),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(TestCaseSupplier.TypedData.multiRow(List.of(200L), DataType.LONG, "field")),
                        "Sum[field=Attribute[channel=0]]",
                        DataType.LONG,
                        equalTo(200L)
                    )
                ),
                new TestCaseSupplier(
                    List.of(DataType.DOUBLE),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(TestCaseSupplier.TypedData.multiRow(List.of(200.), DataType.DOUBLE, "field")),
                        "Sum[field=Attribute[channel=0]]",
                        DataType.DOUBLE,
                        equalTo(200.)
                    )
                )
            )
        );

        return parameterSuppliersFromTypedDataWithDefaultChecks(suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Sum(source, args.get(0));
    }

    private static TestCaseSupplier makeSupplier(TestCaseSupplier.TypedDataSupplier fieldSupplier) {
        return new TestCaseSupplier(List.of(fieldSupplier.type()), () -> {
            var fieldTypedData = fieldSupplier.get();

            Object expected;

            try {
                expected = switch (fieldTypedData.type().widenSmallNumeric()) {
                    case INTEGER -> fieldTypedData.multiRowData()
                        .stream()
                        .map(v -> (Integer) v)
                        .collect(Collectors.summarizingInt(Integer::intValue))
                        .getSum();
                    case LONG -> fieldTypedData.multiRowData()
                        .stream()
                        .map(v -> (Long) v)
                        .collect(Collectors.summarizingLong(Long::longValue))
                        .getSum();
                    case DOUBLE -> {
                        var value = fieldTypedData.multiRowData()
                            .stream()
                            .map(v -> (Double) v)
                            .collect(Collectors.summarizingDouble(Double::doubleValue))
                            .getSum();

                        if (Double.isInfinite(value) || Double.isNaN(value)) {
                            yield null;
                        }

                        yield value;
                    }
                    default -> throw new IllegalStateException("Unexpected value: " + fieldTypedData.type());
                };
            } catch (Exception e) {
                expected = null;
            }

            var dataType = fieldTypedData.type().isWholeNumber() == false || fieldTypedData.type() == UNSIGNED_LONG
                ? DataType.DOUBLE
                : DataType.LONG;

            return new TestCaseSupplier.TestCase(List.of(fieldTypedData), "Sum[field=Attribute[channel=0]]", dataType, equalTo(expected));
        });
    }
}
