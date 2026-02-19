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
import org.elasticsearch.xpack.esql.expression.function.DocsV3Support;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.MultiRowTestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier.appliesTo;
import static org.hamcrest.Matchers.equalTo;

public class AbsentOverTimeTests extends AbstractAggregationTestCase {
    public AbsentOverTimeTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        // TODO Use AbsentTests.parameters() once absent over time allows for dense_vectors
        ArrayList<TestCaseSupplier> suppliers = new ArrayList<>();

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
            MultiRowTestCaseSupplier.geoPointCases(1, 1000, MultiRowTestCaseSupplier.IncludingAltitude.YES),
            MultiRowTestCaseSupplier.geoShapeCasesWithoutCircle(1, 1000, MultiRowTestCaseSupplier.IncludingAltitude.YES),
            MultiRowTestCaseSupplier.cartesianShapeCasesWithoutCircle(1, 1000, MultiRowTestCaseSupplier.IncludingAltitude.YES),
            MultiRowTestCaseSupplier.geohashCases(1, 1000),
            MultiRowTestCaseSupplier.geotileCases(1, 1000),
            MultiRowTestCaseSupplier.geohexCases(1, 1000),
            MultiRowTestCaseSupplier.stringCases(1, 1000, DataType.KEYWORD),
            MultiRowTestCaseSupplier.stringCases(1, 1000, DataType.TEXT),
            MultiRowTestCaseSupplier.exponentialHistogramCases(1, 100),
            MultiRowTestCaseSupplier.tdigestCases(1, 100),
            MultiRowTestCaseSupplier.histogramCases(1, 100)
        ).flatMap(List::stream).map(AbsentTests::makeSupplier).collect(Collectors.toCollection(() -> suppliers));

        // No rows
        List<DataType> types = List.of(
            DataType.AGGREGATE_METRIC_DOUBLE,
            DataType.BOOLEAN,
            DataType.CARTESIAN_POINT,
            DataType.CARTESIAN_SHAPE,
            DataType.DATE_NANOS,
            DataType.DATETIME,
            DataType.DATE_NANOS,
            DataType.DOUBLE,
            DataType.GEO_POINT,
            DataType.GEO_SHAPE,
            DataType.INTEGER,
            DataType.IP,
            DataType.KEYWORD,
            DataType.LONG,
            DataType.TEXT,
            DataType.UNSIGNED_LONG,
            DataType.VERSION,
            DataType.EXPONENTIAL_HISTOGRAM,
            DataType.TDIGEST
        );
        for (var dataType : types) {
            suppliers.add(
                new TestCaseSupplier(
                    "No rows (" + dataType + ")",
                    List.of(dataType),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(TestCaseSupplier.TypedData.multiRow(List.of(), dataType, "field")),
                        "Present",
                        DataType.BOOLEAN,
                        equalTo(true)
                    )
                )
            );
        }

        // "No rows" expects 0 here instead of null
        return parameterSuppliersFromTypedData(randomizeBytesRefsOffset(suppliers));
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new AbsentOverTime(source, args.get(0), AggregateFunction.NO_WINDOW);
    }

    public static List<DocsV3Support.Param> signatureTypes(List<DocsV3Support.Param> params) {
        ArrayList<DocsV3Support.Param> copies = new ArrayList<>(params);
        var preview = appliesTo(FunctionAppliesToLifecycle.PREVIEW, "9.3.0", "", false);
        DocsV3Support.Param window = new DocsV3Support.Param(DataType.TIME_DURATION, List.of(preview));
        copies.add(window);
        return copies;
    }
}
