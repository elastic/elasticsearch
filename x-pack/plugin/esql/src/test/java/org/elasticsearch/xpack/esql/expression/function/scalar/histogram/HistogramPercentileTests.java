/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.histogram;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.exponentialhistogram.ExponentialHistogram;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramQuantile;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.plugin.EsqlCorePlugin;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier.getCastEvaluator;
import static org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier.getSuppliersForNumericType;
import static org.hamcrest.Matchers.equalTo;

public class HistogramPercentileTests extends AbstractScalarFunctionTestCase {

    @Before
    public void setup() {
        assumeTrue(
            "Only when esql_exponential_histogram feature flag is enabled",
            EsqlCorePlugin.EXPONENTIAL_HISTOGRAM_FEATURE_FLAG.isEnabled()
        );
    }

    public HistogramPercentileTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        List<TestCaseSupplier.TypedDataSupplier> validPercentileSuppliers = Stream.of(
            DataType.DOUBLE,
            DataType.INTEGER,
            DataType.LONG,
            DataType.UNSIGNED_LONG
        ).filter(DataType::isNumeric).flatMap(type -> getSuppliersForNumericType(type, 0.0, 100.0, true).stream()).toList();

        List<Double> invalidPercentileValues = List.of(-0.01, 100.05, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY);

        List<TestCaseSupplier.TypedDataSupplier> invalidPercentileSuppliers = invalidPercentileValues.stream()
            .map(value -> new TestCaseSupplier.TypedDataSupplier("<" + value + " double>", () -> value, DataType.DOUBLE))
            .toList();

        List<TestCaseSupplier.TypedDataSupplier> allPercentiles = Stream.concat(
            validPercentileSuppliers.stream(),
            invalidPercentileSuppliers.stream()
        ).toList();

        TestCaseSupplier.casesCrossProduct((histogramObj, percentileObj) -> {
            ExponentialHistogram histogram = (ExponentialHistogram) histogramObj;
            Number percentile = (Number) percentileObj;
            double percVal = percentile.doubleValue();
            if (percVal < 0 || percVal > 100 || Double.isNaN(percVal)) {
                return null;
            }
            double result = ExponentialHistogramQuantile.getQuantile(histogram, percVal / 100.0);
            return Double.isNaN(result) ? null : result;
        },
            TestCaseSupplier.exponentialHistogramCases(),
            allPercentiles,
            (histoType, percentileType) -> equalTo(
                "HistogramPercentileEvaluator[value=Attribute[channel=0], percentile="
                    + getCastEvaluator("Attribute[channel=1]", percentileType, DataType.DOUBLE)
                    + "]"
            ),
            (typedHistoData, typedPercentileData) -> {
                Object percentile = typedPercentileData.getValue();
                if (invalidPercentileValues.contains(percentile)) {
                    return List.of(
                        "Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.",
                        "Line 1:1: java.lang.ArithmeticException: Percentile value must be in the range [0, 100], got: " + percentile
                    );
                } else {
                    return List.of();
                }
            },
            suppliers,
            DataType.DOUBLE,
            false
        );

        return parameterSuppliersFromTypedDataWithDefaultChecks(true, suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new HistogramPercentile(source, args.get(0), args.get(1));
    }

}
