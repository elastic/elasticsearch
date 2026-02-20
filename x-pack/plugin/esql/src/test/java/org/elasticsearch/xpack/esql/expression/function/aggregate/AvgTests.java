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
import org.elasticsearch.xpack.esql.expression.function.MultiRowTestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class AvgTests extends AbstractAggregationTestCase {
    public AvgTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        var suppliers = new ArrayList<TestCaseSupplier>();

        Stream.of(
            MultiRowTestCaseSupplier.intCases(1, 1000, Integer.MIN_VALUE, Integer.MAX_VALUE, true),
            // Longs currently fail on overflow
            // Restore after https://github.com/elastic/elasticsearch/issues/110437
            // MultiRowTestCaseSupplier.longCases(1, 1000, Long.MIN_VALUE, Long.MAX_VALUE, true),
            MultiRowTestCaseSupplier.doubleCases(1, 1000, -Double.MAX_VALUE, Double.MAX_VALUE, true),
            MultiRowTestCaseSupplier.aggregateMetricDoubleCases(1, 1000, -Double.MAX_VALUE, Double.MAX_VALUE),
            MultiRowTestCaseSupplier.exponentialHistogramCases(1, 100),
            MultiRowTestCaseSupplier.tdigestCases(1, 100),

            // No rows cases
            List.of(
                new TestCaseSupplier.TypedDataSupplier("integer no rows", List::of, DataType.INTEGER, false, true, List.of()),
                new TestCaseSupplier.TypedDataSupplier("long no rows", List::of, DataType.LONG, false, true, List.of()),
                new TestCaseSupplier.TypedDataSupplier("double no rows", List::of, DataType.DOUBLE, false, true, List.of())
            )
        ).flatMap(List::stream).map(AvgTests::makeSupplier).collect(Collectors.toCollection(() -> suppliers));

        // Simple cases
        suppliers.addAll(
            List.of(
                new TestCaseSupplier(
                    List.of(DataType.INTEGER),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(TestCaseSupplier.TypedData.multiRow(List.of(200), DataType.INTEGER, "field")),
                        "Avg[field=Attribute[channel=0]]",
                        DataType.DOUBLE,
                        equalTo(200.)
                    )
                ),
                new TestCaseSupplier(
                    List.of(DataType.LONG),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(TestCaseSupplier.TypedData.multiRow(List.of(200L), DataType.LONG, "field")),
                        "Avg[field=Attribute[channel=0]]",
                        DataType.DOUBLE,
                        equalTo(200.)
                    )
                ),
                new TestCaseSupplier(
                    List.of(DataType.DOUBLE),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(TestCaseSupplier.TypedData.multiRow(List.of(200.), DataType.DOUBLE, "field")),
                        "Avg[field=Attribute[channel=0]]",
                        DataType.DOUBLE,
                        equalTo(200.)
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
        return new Avg(source, args.get(0));
    }

    private static TestCaseSupplier makeSupplier(TestCaseSupplier.TypedDataSupplier fieldSupplier) {
        return new TestCaseSupplier(fieldSupplier.name(), List.of(fieldSupplier.type()), () -> {
            var fieldTypedData = fieldSupplier.get();
            var fieldData = fieldTypedData.multiRowData();
            var dataType = fieldTypedData.type().widenSmallNumeric();

            Double expected = null;
            List<String> warnings = null;

            if (fieldData.size() == 1) {
                // For single elements, we directly return them to avoid precision issues
                expected = switch (dataType) {
                    case AGGREGATE_METRIC_DOUBLE -> {
                        var aggMetric = (AggregateMetricDoubleBlockBuilder.AggregateMetricDoubleLiteral) fieldData.get(0);
                        yield aggMetric.sum() / (aggMetric.count().doubleValue());
                    }
                    case EXPONENTIAL_HISTOGRAM -> {
                        var expHisto = (ExponentialHistogram) fieldData.get(0);
                        if (expHisto.valueCount() == 0) {
                            yield null;
                        }
                        yield expHisto.sum() / expHisto.valueCount();
                    }
                    case TDIGEST -> {
                        var tDigest = (TDigestHolder) fieldData.get(0);
                        if (tDigest.getValueCount() == 0) {
                            yield null;
                        }
                        yield tDigest.getSum() / tDigest.getValueCount();
                    }
                    default -> {
                        double value = ((Number) fieldData.get(0)).doubleValue();
                        yield value == -0.0 ? 0.0 : value;
                    }
                };
            } else if (fieldData.size() > 1) {
                expected = switch (dataType) {
                    case INTEGER -> fieldData.stream()
                        .map(v -> (Integer) v)
                        .collect(Collectors.summarizingInt(Integer::intValue))
                        .getAverage();
                    case LONG -> fieldData.stream().map(v -> (Long) v).collect(Collectors.summarizingLong(Long::longValue)).getAverage();
                    case DOUBLE -> fieldData.stream()
                        .map(v -> (Double) v)
                        .collect(Collectors.summarizingDouble(Double::doubleValue))
                        .getAverage();
                    case AGGREGATE_METRIC_DOUBLE -> {
                        double sum = fieldData.stream()
                            .mapToDouble(v -> ((AggregateMetricDoubleBlockBuilder.AggregateMetricDoubleLiteral) v).sum())
                            .sum();
                        long count = fieldData.stream()
                            .mapToLong(v -> ((AggregateMetricDoubleBlockBuilder.AggregateMetricDoubleLiteral) v).count())
                            .sum();
                        yield sum / count;
                    }
                    case EXPONENTIAL_HISTOGRAM -> {
                        double sum = fieldData.stream().mapToDouble(v -> ((ExponentialHistogram) v).sum()).sum();
                        double count = fieldData.stream().mapToLong(v -> ((ExponentialHistogram) v).valueCount()).sum();
                        if (count == 0) {
                            yield null;
                        }
                        yield sum / count;
                    }
                    case TDIGEST -> {
                        double sum = fieldData.stream().mapToDouble(v -> ((TDigestHolder) v).getSum()).sum();
                        double count = fieldData.stream().mapToLong(v -> ((TDigestHolder) v).getValueCount()).sum();
                        if (count == 0) {
                            yield null;
                        }
                        yield sum / count;
                    }
                    default -> throw new IllegalStateException("Unexpected value: " + fieldTypedData.type());
                };
            }

            if (expected != null) {
                if (Double.isFinite(expected) == false) {
                    expected = null;
                    warnings = List.of(
                        "Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.",
                        "Line 1:1: java.lang.ArithmeticException: / by zero"
                    );
                }
            }

            return new TestCaseSupplier.TestCase(
                List.of(fieldTypedData),
                "Avg[field=Attribute[channel=0]]",
                DataType.DOUBLE,
                expected == null ? nullValue() : closeTo(expected, Math.abs(expected * 1e-10))
            ).withWarnings(warnings);
        });
    }

    @Override
    public void testFold() {
        var typedData = testCase.getData().getFirst();
        assumeFalse(
            "AVG expects a different result for -0.0 when folded",
            typedData.type() == DataType.DOUBLE && typedData.multiRowData().size() == 1 && typedData.multiRowData().getFirst().equals(-0.0)
        );

        super.testFold();
    }
}
