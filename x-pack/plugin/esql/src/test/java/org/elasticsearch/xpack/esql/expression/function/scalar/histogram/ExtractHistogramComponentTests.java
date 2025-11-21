/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.histogram;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.compute.data.ExponentialHistogramBlock;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;
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

import static org.hamcrest.Matchers.equalTo;

public class ExtractHistogramComponentTests extends AbstractScalarFunctionTestCase {

    @Before
    public void setup() {
        assumeTrue(
            "Only when esql_exponential_histogram feature flag is enabled",
            EsqlCorePlugin.EXPONENTIAL_HISTOGRAM_FEATURE_FLAG.isEnabled()
        );
    }

    public ExtractHistogramComponentTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        for (ExponentialHistogramBlock.Component component : ExponentialHistogramBlock.Component.values()) {
            TestCaseSupplier.TypedDataSupplier componentOrdinalSupplier = new TestCaseSupplier.TypedDataSupplier(
                "<" + component + ">",
                component::ordinal,
                DataType.INTEGER,
                true
            );
            for (TestCaseSupplier.TypedDataSupplier histoSupplier : TestCaseSupplier.exponentialHistogramCases()) {
                suppliers.add(
                    new TestCaseSupplier(
                        "<" + histoSupplier.type().typeName() + "," + component + ">",
                        List.of(histoSupplier.type(), DataType.INTEGER),
                        () -> {
                            TestCaseSupplier.TypedData histogram = histoSupplier.get();
                            return new TestCaseSupplier.TestCase(
                                List.of(histogram, componentOrdinalSupplier.get()),
                                "ExtractHistogramComponentEvaluator[field=Attribute[channel=0],component=" + component + "]",
                                getExpectedDataTypeForComponent(component),
                                equalTo(getExpectedValue(histogram, component))
                            );
                        }
                    )
                );
            }
        }
        List<TestCaseSupplier> withNulls = anyNullIsNull(
            suppliers,
            (nullPosition, nullValueDataType, original) -> nullPosition == 1 ? DataType.NULL : original.expectedType(),
            (nullPosition, nullData, original) -> nullData.isForceLiteral() ? equalTo("LiteralsEvaluator[lit=null]") : original
        );
        return parameterSuppliersFromTypedData(withNulls);
    }

    private static Object getExpectedValue(TestCaseSupplier.TypedData histogram, ExponentialHistogramBlock.Component component) {
        ExponentialHistogram value = (ExponentialHistogram) histogram.getValue();
        if (value == null) {
            return null;
        }
        return switch (component) {
            case MIN -> {
                double min = value.min();
                yield Double.isNaN(min) ? null : min;
            }
            case MAX -> {
                double max = value.max();
                yield Double.isNaN(max) ? null : max;
            }
            case SUM -> value.valueCount() > 0 ? value.sum() : null;
            case COUNT -> value.valueCount();
        };
    }

    private static DataType getExpectedDataTypeForComponent(ExponentialHistogramBlock.Component component) {
        return switch (component) {
            case MIN, MAX, SUM -> DataType.DOUBLE;
            case COUNT -> DataType.LONG;
        };
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        assumeTrue("Test sometimes wraps literals as fields", args.get(1).foldable());
        return new ExtractHistogramComponent(source, args.get(0), args.get(1));
    }

}
