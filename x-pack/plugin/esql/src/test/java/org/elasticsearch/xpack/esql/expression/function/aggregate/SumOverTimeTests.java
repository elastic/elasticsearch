/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.compute.data.AggregateMetricDoubleBlockBuilder;
import org.elasticsearch.compute.data.TDigestHolder;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractAggregationTestCase;
import org.elasticsearch.xpack.esql.expression.function.DocsV3Support;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.MultiRowTestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier.appliesTo;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

public class SumOverTimeTests extends AbstractAggregationTestCase {
    public SumOverTimeTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        var suppliers = new ArrayList<TestCaseSupplier>();

        Stream.of(
            MultiRowTestCaseSupplier.intCases(1, 1000, Integer.MIN_VALUE, Integer.MAX_VALUE, true),
            MultiRowTestCaseSupplier.aggregateMetricDoubleCases(1, 1000, -Double.MAX_VALUE, Double.MAX_VALUE),
            MultiRowTestCaseSupplier.exponentialHistogramCases(1, 100),
            MultiRowTestCaseSupplier.tdigestCases(1, 100),
            MultiRowTestCaseSupplier.doubleCases(1, 1000, -Double.MAX_VALUE, Double.MAX_VALUE, true)
        ).flatMap(List::stream).map(SumOverTimeTests::makeSupplier).collect(Collectors.toCollection(() -> suppliers));

        suppliers.addAll(
            List.of(
                new TestCaseSupplier(
                    List.of(DataType.INTEGER),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(TestCaseSupplier.TypedData.multiRow(List.of(200), DataType.INTEGER, "field")),
                        "SumInt",
                        DataType.LONG,
                        equalTo(200L)
                    )
                ),
                new TestCaseSupplier(
                    List.of(DataType.LONG),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(TestCaseSupplier.TypedData.multiRow(List.of(200L), DataType.LONG, "field")),
                        "SumLong",
                        DataType.LONG,
                        equalTo(200L)
                    )
                ),
                new TestCaseSupplier(
                    List.of(DataType.DOUBLE),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(TestCaseSupplier.TypedData.multiRow(List.of(200.), DataType.DOUBLE, "field")),
                        "LossySumDouble",
                        DataType.DOUBLE,
                        equalTo(200.)
                    )
                ),
                new TestCaseSupplier(List.of(DataType.AGGREGATE_METRIC_DOUBLE), () -> {
                    var value = new AggregateMetricDoubleBlockBuilder.AggregateMetricDoubleLiteral(
                        randomDouble(),
                        randomDouble(),
                        randomDouble(),
                        randomNonNegativeInt()
                    );
                    return new TestCaseSupplier.TestCase(
                        List.of(TestCaseSupplier.TypedData.multiRow(List.of(value), DataType.AGGREGATE_METRIC_DOUBLE, "field")),
                        standardAggregatorName("Sum", DataType.AGGREGATE_METRIC_DOUBLE),
                        DataType.DOUBLE,
                        equalTo(value.sum())
                    );
                })
            )
        );

        return parameterSuppliersFromTypedDataWithDefaultChecks(suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new SumOverTime(source, args.get(0), AggregateFunction.NO_WINDOW);
    }

    private static TestCaseSupplier makeSupplier(TestCaseSupplier.TypedDataSupplier fieldSupplier) {
        return new TestCaseSupplier(fieldSupplier.name(), List.of(fieldSupplier.type()), () -> {
            var fieldTypedData = fieldSupplier.get();

            DataType type = fieldTypedData.type().widenSmallNumeric();
            var data = fieldTypedData.multiRowData();
            Object expected = null;
            if (data.isEmpty() == false) {
                expected = switch (type) {
                    case INTEGER -> data.stream().mapToLong(v -> (int) v).sum();
                    case LONG -> data.stream().mapToLong(v -> (long) v).reduce(0L, Math::addExact);
                    case DOUBLE -> data.stream().mapToDouble(v -> (double) v).sum();
                    case AGGREGATE_METRIC_DOUBLE -> data.stream()
                        .mapToDouble(v -> ((AggregateMetricDoubleBlockBuilder.AggregateMetricDoubleLiteral) v).sum())
                        .sum();
                    case EXPONENTIAL_HISTOGRAM -> {
                        var sums = data.stream()
                            .map(obj -> (ExponentialHistogram) obj)
                            .filter(obj -> obj.valueCount() > 0)
                            .mapToDouble(ExponentialHistogram::sum)
                            .toArray();
                        yield sums.length == 0 ? null : Arrays.stream(sums).sum();
                    }
                    case TDIGEST -> {
                        var sums = data.stream()
                            .map(obj -> (TDigestHolder) obj)
                            .filter(obj -> obj.getValueCount() > 0)
                            .mapToDouble(TDigestHolder::getSum)
                            .toArray();
                        yield sums.length == 0 ? null : Arrays.stream(sums).sum();
                    }
                    default -> throw new IllegalStateException("Unexpected value: " + fieldTypedData.type());
                };
            }

            assumeFalse(
                "Sums of doubles may return infinity in their current implementation",
                expected instanceof Double d && Double.isFinite(d) == false
            );

            var returnType = type.isWholeNumber() ? DataType.LONG : DataType.DOUBLE;

            return new TestCaseSupplier.TestCase(
                List.of(fieldTypedData),
                standardAggregatorName(type == DataType.DOUBLE ? "LossySum" : "Sum", fieldSupplier.type()),
                returnType,
                expected instanceof Double d ? closeTo(d, Math.abs(d * 1e-10)) : equalTo(expected)
            );
        });
    }

    public static List<DocsV3Support.Param> signatureTypes(List<DocsV3Support.Param> params) {
        var preview = appliesTo(FunctionAppliesToLifecycle.PREVIEW, "9.3.0", "", false);
        DocsV3Support.Param window = new DocsV3Support.Param(DataType.TIME_DURATION, List.of(preview));
        return List.of(params.get(0), window);
    }
}
