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
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractAggregationTestCase;
import org.elasticsearch.xpack.esql.expression.function.MultiRowTestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.MultiRowTestCaseSupplier.IncludingAltitude;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;

public class CountTests extends AbstractAggregationTestCase {
    public CountTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        var suppliers = new ArrayList<TestCaseSupplier>();

        Stream.of(
            MultiRowTestCaseSupplier.nullCases(1, 1000),
            MultiRowTestCaseSupplier.intCases(1, 1000, Integer.MIN_VALUE, Integer.MAX_VALUE, true),
            MultiRowTestCaseSupplier.longCases(1, 1000, Long.MIN_VALUE, Long.MAX_VALUE, true),
            MultiRowTestCaseSupplier.ulongCases(1, 1000, BigInteger.ZERO, UNSIGNED_LONG_MAX, true),
            MultiRowTestCaseSupplier.doubleCases(1, 1000, -Double.MAX_VALUE, Double.MAX_VALUE, true),
            MultiRowTestCaseSupplier.aggregateMetricDoubleCases(1, 1000, -Double.MAX_VALUE, Double.MAX_VALUE),
            MultiRowTestCaseSupplier.dateCases(1, 1000),
            MultiRowTestCaseSupplier.dateNanosCases(1, 1000),
            MultiRowTestCaseSupplier.booleanCases(1, 1000),
            MultiRowTestCaseSupplier.ipCases(1, 1000),
            MultiRowTestCaseSupplier.versionCases(1, 1000),
            MultiRowTestCaseSupplier.geoPointCases(1, 1000, IncludingAltitude.YES),
            MultiRowTestCaseSupplier.geoShapeCasesWithoutCircle(1, 1000, IncludingAltitude.YES),
            MultiRowTestCaseSupplier.cartesianShapeCasesWithoutCircle(1, 1000, IncludingAltitude.YES),
            MultiRowTestCaseSupplier.geohashCases(1, 1000),
            MultiRowTestCaseSupplier.geotileCases(1, 1000),
            MultiRowTestCaseSupplier.geohexCases(1, 1000),
            MultiRowTestCaseSupplier.stringCases(1, 1000, DataType.KEYWORD),
            MultiRowTestCaseSupplier.stringCases(1, 1000, DataType.TEXT)
        ).flatMap(List::stream).map(CountTests::makeSupplier).collect(Collectors.toCollection(() -> suppliers));

        // No rows
        for (var dataType : List.of(
            DataType.NULL,
            DataType.INTEGER,
            DataType.LONG,
            DataType.DOUBLE,
            DataType.DATETIME,
            DataType.DATE_NANOS,
            DataType.BOOLEAN,
            DataType.IP,
            DataType.VERSION,
            DataType.KEYWORD,
            DataType.TEXT,
            DataType.GEO_POINT,
            DataType.CARTESIAN_POINT,
            DataType.UNSIGNED_LONG,
            DataType.AGGREGATE_METRIC_DOUBLE
        )) {
            suppliers.add(
                new TestCaseSupplier(
                    "No rows (" + dataType + ")",
                    List.of(dataType),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(TestCaseSupplier.TypedData.multiRow(List.of(), dataType, "field")),
                        "Count",
                        DataType.LONG,
                        equalTo(0L)
                    )
                )
            );
        }

        // "No rows" expects 0 here instead of null
        return parameterSuppliersFromTypedData(randomizeBytesRefsOffset(suppliers));
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Count(source, args.get(0));
    }

    private static TestCaseSupplier makeSupplier(TestCaseSupplier.TypedDataSupplier fieldSupplier) {
        return new TestCaseSupplier(fieldSupplier.name(), List.of(fieldSupplier.type()), () -> {
            var fieldTypedData = fieldSupplier.get();
            long count;
            if (fieldSupplier.type() == DataType.AGGREGATE_METRIC_DOUBLE) {
                count = fieldTypedData.multiRowData().stream().mapToLong(data -> {
                    var aggMetric = (AggregateMetricDoubleBlockBuilder.AggregateMetricDoubleLiteral) data;
                    if (aggMetric.count() != null) {
                        return aggMetric.count();
                    }
                    return 0;
                }).sum();
            } else {
                count = fieldTypedData.multiRowData().stream().filter(Objects::nonNull).count();
            }

            return new TestCaseSupplier.TestCase(List.of(fieldTypedData), "Count", DataType.LONG, equalTo(count));
        });
    }
}
