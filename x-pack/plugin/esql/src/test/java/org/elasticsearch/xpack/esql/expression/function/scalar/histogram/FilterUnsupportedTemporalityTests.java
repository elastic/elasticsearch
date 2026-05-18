/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.histogram;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.aggregate.RateTests.TemporalityParameter;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class FilterUnsupportedTemporalityTests extends AbstractScalarFunctionTestCase {

    public FilterUnsupportedTemporalityTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        List<TestCaseSupplier.TypedDataSupplier> histogramSuppliers = Stream.concat(
            TestCaseSupplier.exponentialHistogramCases().stream(),
            TestCaseSupplier.tdigestCases().stream()
        ).toList();

        for (TestCaseSupplier.TypedDataSupplier histoSupplier : histogramSuppliers) {
            for (TemporalityParameter temporality : TemporalityParameter.values()) {
                suppliers.add(makeSupplier(histoSupplier, temporality));
            }
        }

        TestCaseSupplier.TypedDataSupplier nullSupplier = new TestCaseSupplier.TypedDataSupplier("<null>", () -> null, DataType.NULL);
        suppliers.add(
            new TestCaseSupplier(
                "<null, keyword>",
                List.of(DataType.NULL, DataType.KEYWORD),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        nullSupplier.get(),
                        new TestCaseSupplier.TypedData(TemporalityParameter.DELTA.byteValue(), DataType.KEYWORD, "t")
                    ),
                    "LiteralsEvaluator[lit=null]",
                    DataType.NULL,
                    nullValue()
                )
            )
        );
        for (TestCaseSupplier.TypedDataSupplier histoSupplier : histogramSuppliers) {
            suppliers.add(
                new TestCaseSupplier(
                    "<" + histoSupplier.type().typeName() + ", null>",
                    List.of(histoSupplier.type(), DataType.NULL),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(histoSupplier.get(), nullSupplier.get()),
                        "LiteralsEvaluator[lit=null]",
                        histoSupplier.type(),
                        nullValue()
                    )
                )
            );
        }

        return parameterSuppliersFromTypedData(suppliers);
    }

    private static TestCaseSupplier makeSupplier(TestCaseSupplier.TypedDataSupplier histoSupplier, TemporalityParameter temporality) {
        return new TestCaseSupplier(
            "<" + histoSupplier.type().typeName() + ", " + temporality + ">",
            List.of(histoSupplier.type(), DataType.KEYWORD),
            () -> {
                TestCaseSupplier.TypedData histogram = histoSupplier.get();
                TestCaseSupplier.TypedData temporalityData = new TestCaseSupplier.TypedData(
                    temporality.byteValue(),
                    DataType.KEYWORD,
                    "temporality"
                );
                boolean expectsNull = temporality == TemporalityParameter.CUMULATIVE || temporality == TemporalityParameter.INVALID;
                TestCaseSupplier.TestCase result = new TestCaseSupplier.TestCase(
                    List.of(histogram, temporalityData),
                    getExpectedEvaluatorString(histoSupplier.type()),
                    histoSupplier.type(),
                    expectsNull ? nullValue() : equalTo(histogram.getValue())
                );
                if (temporality == TemporalityParameter.CUMULATIVE) {
                    String warningMsg = histoSupplier.type() == DataType.EXPONENTIAL_HISTOGRAM
                        ? "Line 1:1: java.lang.IllegalArgumentException: Some nodes in your cluster don't support cumulative temporality"
                            + " for exponential_histograms yet."
                            + " The affected time series are excluded from aggregations. Upgrade your cluster to fix this."
                        : "Line 1:1: java.lang.IllegalArgumentException: Cumulative temporality is not supported for the tdigest type."
                            + " The affected time series are excluded from aggregations.";
                    result = result.withWarning(
                        "Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded."
                    ).withWarning(warningMsg);
                } else if (temporality == TemporalityParameter.INVALID) {
                    result = result.withWarning(
                        "Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded."
                    )
                        .withWarning(
                            "Line 1:1: org.elasticsearch.compute.aggregation.InvalidTemporalityException: "
                                + "Invalid temporality value: [gotcha], expected [cumulative] or [delta]"
                        );
                }
                return result;
            }
        );
    }

    private static String getExpectedEvaluatorString(DataType histogramType) {
        if (histogramType == DataType.EXPONENTIAL_HISTOGRAM) {
            return "FilterUnsupportedTemporalityExpHistEvaluator[histogram=Attribute[channel=0], temporality=Attribute[channel=1]]";
        }
        return "FilterUnsupportedTemporalityTDigestEvaluator[histogram=Attribute[channel=0], temporality=Attribute[channel=1]]";
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new FilterUnsupportedTemporality(source, args.get(0), args.get(1));
    }

    @Override
    public void testFold() {
        // FilterUnsupportedTemporality cannot be folded
    }
}
