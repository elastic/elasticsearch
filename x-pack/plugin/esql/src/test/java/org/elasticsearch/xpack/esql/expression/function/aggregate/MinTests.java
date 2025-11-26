/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.compute.data.AggregateMetricDoubleBlockBuilder;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractAggregationTestCase;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.MultiRowTestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.versionfield.Version;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier.appliesTo;
import static org.hamcrest.Matchers.equalTo;

public class MinTests extends AbstractAggregationTestCase {
    public MinTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        var suppliers = new ArrayList<TestCaseSupplier>();

        Stream.of(
            MultiRowTestCaseSupplier.intCases(1, 1000, Integer.MIN_VALUE, Integer.MAX_VALUE, true),
            MultiRowTestCaseSupplier.longCases(1, 1000, Long.MIN_VALUE, Long.MAX_VALUE, true),
            MultiRowTestCaseSupplier.doubleCases(1, 1000, -Double.MAX_VALUE, Double.MAX_VALUE, true),
            MultiRowTestCaseSupplier.aggregateMetricDoubleCases(1, 1000, -Double.MAX_VALUE, Double.MAX_VALUE),
            MultiRowTestCaseSupplier.dateCases(1, 1000),
            MultiRowTestCaseSupplier.booleanCases(1, 1000),
            MultiRowTestCaseSupplier.ipCases(1, 1000),
            MultiRowTestCaseSupplier.versionCases(1, 1000),
            MultiRowTestCaseSupplier.stringCases(1, 1000, DataType.KEYWORD),
            MultiRowTestCaseSupplier.stringCases(1, 1000, DataType.TEXT),
            MultiRowTestCaseSupplier.exponentialHistogramCases(1, 100)
        ).flatMap(List::stream).map(MinTests::makeSupplier).collect(Collectors.toCollection(() -> suppliers));

        FunctionAppliesTo unsignedLongAppliesTo = appliesTo(FunctionAppliesToLifecycle.GA, "9.2.0", "", true);
        for (TestCaseSupplier.TypedDataSupplier supplier : MultiRowTestCaseSupplier.ulongCases(
            1,
            1000,
            BigInteger.ZERO,
            UNSIGNED_LONG_MAX,
            true
        )) {
            suppliers.add(makeSupplier(supplier.withAppliesTo(unsignedLongAppliesTo)));
        }

        suppliers.addAll(
            List.of(
                // Folding
                new TestCaseSupplier(
                    List.of(DataType.INTEGER),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(TestCaseSupplier.TypedData.multiRow(List.of(200), DataType.INTEGER, "field")),
                        standardAggregatorName("Min", DataType.INTEGER),
                        DataType.INTEGER,
                        equalTo(200)
                    )
                ),
                new TestCaseSupplier(
                    List.of(DataType.LONG),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(TestCaseSupplier.TypedData.multiRow(List.of(200L), DataType.LONG, "field")),
                        standardAggregatorName("Min", DataType.LONG),
                        DataType.LONG,
                        equalTo(200L)
                    )
                ),
                new TestCaseSupplier(
                    List.of(DataType.UNSIGNED_LONG),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            TestCaseSupplier.TypedData.multiRow(List.of(new BigInteger("200")), DataType.UNSIGNED_LONG, "field")
                                .withAppliesTo(unsignedLongAppliesTo)
                        ),
                        standardAggregatorName("Min", DataType.UNSIGNED_LONG),
                        DataType.UNSIGNED_LONG,
                        equalTo(new BigInteger("200"))
                    )
                ),
                new TestCaseSupplier(
                    List.of(DataType.DOUBLE),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(TestCaseSupplier.TypedData.multiRow(List.of(200.), DataType.DOUBLE, "field")),
                        standardAggregatorName("Min", DataType.DOUBLE),
                        DataType.DOUBLE,
                        equalTo(200.)
                    )
                ),
                new TestCaseSupplier(
                    List.of(DataType.DATETIME),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(TestCaseSupplier.TypedData.multiRow(List.of(200L), DataType.DATETIME, "field")),
                        standardAggregatorName("Min", DataType.DATETIME),
                        DataType.DATETIME,
                        equalTo(200L)
                    )
                ),
                new TestCaseSupplier(
                    List.of(DataType.DATE_NANOS),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(TestCaseSupplier.TypedData.multiRow(List.of(200L), DataType.DATE_NANOS, "field")),
                        standardAggregatorName("Min", DataType.DATE_NANOS),
                        DataType.DATE_NANOS,
                        equalTo(200L)
                    )
                ),
                new TestCaseSupplier(
                    List.of(DataType.BOOLEAN),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(TestCaseSupplier.TypedData.multiRow(List.of(true), DataType.BOOLEAN, "field")),
                        standardAggregatorName("Min", DataType.BOOLEAN),
                        DataType.BOOLEAN,
                        equalTo(true)
                    )
                ),
                new TestCaseSupplier(
                    List.of(DataType.IP),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            TestCaseSupplier.TypedData.multiRow(
                                List.of(new BytesRef(InetAddressPoint.encode(InetAddresses.forString("127.0.0.1")))),
                                DataType.IP,
                                "field"
                            )
                        ),
                        standardAggregatorName("Min", DataType.IP),
                        DataType.IP,
                        equalTo(new BytesRef(InetAddressPoint.encode(InetAddresses.forString("127.0.0.1"))))
                    )
                ),
                new TestCaseSupplier(List.of(DataType.KEYWORD), () -> {
                    var value = new BytesRef(randomAlphaOfLengthBetween(0, 50));
                    return new TestCaseSupplier.TestCase(
                        List.of(TestCaseSupplier.TypedData.multiRow(List.of(value), DataType.KEYWORD, "field")),
                        standardAggregatorName("Min", DataType.KEYWORD),
                        DataType.KEYWORD,
                        equalTo(value)
                    );
                }),
                new TestCaseSupplier(List.of(DataType.TEXT), () -> {
                    var value = new BytesRef(randomAlphaOfLengthBetween(0, 50));
                    return new TestCaseSupplier.TestCase(
                        List.of(TestCaseSupplier.TypedData.multiRow(List.of(value), DataType.TEXT, "field")),
                        standardAggregatorName("Min", DataType.TEXT),
                        DataType.KEYWORD,
                        equalTo(value)
                    );
                }),
                new TestCaseSupplier(List.of(DataType.VERSION), () -> {
                    var value = randomBoolean()
                        ? new Version(randomAlphaOfLengthBetween(1, 10)).toBytesRef()
                        : new Version(randomIntBetween(0, 100) + "." + randomIntBetween(0, 100) + "." + randomIntBetween(0, 100))
                            .toBytesRef();
                    return new TestCaseSupplier.TestCase(
                        List.of(TestCaseSupplier.TypedData.multiRow(List.of(value), DataType.VERSION, "field")),
                        standardAggregatorName("Min", DataType.VERSION),
                        DataType.VERSION,
                        equalTo(value)
                    );
                }),
                new TestCaseSupplier(List.of(DataType.AGGREGATE_METRIC_DOUBLE), () -> {
                    var value = new AggregateMetricDoubleBlockBuilder.AggregateMetricDoubleLiteral(
                        randomDouble(),
                        randomDouble(),
                        randomDouble(),
                        randomNonNegativeInt()
                    );
                    return new TestCaseSupplier.TestCase(
                        List.of(TestCaseSupplier.TypedData.multiRow(List.of(value), DataType.AGGREGATE_METRIC_DOUBLE, "field")),
                        standardAggregatorName("Min", DataType.AGGREGATE_METRIC_DOUBLE),
                        DataType.DOUBLE,
                        equalTo(value.min())
                    );

                })
            )
        );

        return parameterSuppliersFromTypedDataWithDefaultChecks(suppliers, false);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Min(source, args.get(0));
    }

    @SuppressWarnings("unchecked")
    private static TestCaseSupplier makeSupplier(TestCaseSupplier.TypedDataSupplier fieldSupplier) {
        return new TestCaseSupplier(fieldSupplier.name(), List.of(fieldSupplier.type()), () -> {
            var fieldTypedData = fieldSupplier.get();
            Comparable<?> expected;
            DataType expectedReturnType;

            if (fieldSupplier.type() == DataType.AGGREGATE_METRIC_DOUBLE) {
                expected = fieldTypedData.multiRowData()
                    .stream()
                    .map(
                        v -> (Comparable<
                            ? super Comparable<?>>) ((Object) ((AggregateMetricDoubleBlockBuilder.AggregateMetricDoubleLiteral) v).min())
                    )
                    .min(Comparator.naturalOrder())
                    .orElse(null);
                expectedReturnType = DataType.DOUBLE;
            } else if (fieldSupplier.type() == DataType.EXPONENTIAL_HISTOGRAM) {
                expected = fieldTypedData.multiRowData()
                    .stream()
                    .map(obj -> (ExponentialHistogram) obj)
                    .filter(histo -> histo.valueCount() > 0) // only non-empty histograms have an influence
                    .map(ExponentialHistogram::min)
                    .min(Comparator.naturalOrder())
                    .orElse(null);
                expectedReturnType = DataType.DOUBLE;
            } else {
                expected = fieldTypedData.multiRowData()
                    .stream()
                    .map(v -> (Comparable<? super Comparable<?>>) v)
                    .min(Comparator.naturalOrder())
                    .orElse(null);
                expectedReturnType = fieldSupplier.type();
            }

            return new TestCaseSupplier.TestCase(
                List.of(fieldTypedData),
                standardAggregatorName("Min", fieldSupplier.type()),
                expectedReturnType,
                equalTo(expected)
            );
        });
    }
}
