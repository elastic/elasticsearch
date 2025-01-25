/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.compute.data.AggregateMetricDoubleLiteral;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.FunctionName;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.Matchers;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

@FunctionName("from_aggregate_double_metric")
public class FromAggregateDoubleMetricTests extends AbstractScalarFunctionTestCase {
    public FromAggregateDoubleMetricTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        assumeTrue("Test sometimes wraps literals as fields", args.get(1).foldable());
        return new FromAggregateDoubleMetric(source, args.get(0), args.get(1));
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        DataType dataType = DataType.AGGREGATE_METRIC_DOUBLE;
        suppliers.add(new TestCaseSupplier(List.of(dataType, DataType.INTEGER), () -> {
            var agg_metric = new AggregateMetricDoubleLiteral(
                randomDoubleBetween(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, true),
                randomDoubleBetween(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, true),
                randomDoubleBetween(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, true),
                randomIntBetween(Integer.MIN_VALUE, Integer.MAX_VALUE)
            );
            int index = randomIntBetween(0, 3);
            Double expectedValue = index == 0 ? agg_metric.getMin()
                : index == 1 ? agg_metric.getMax()
                : index == 2 ? agg_metric.getSum()
                : (Double) agg_metric.getCount().doubleValue();

            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(agg_metric, dataType, "agg_metric"),
                    new TestCaseSupplier.TypedData(index, DataType.INTEGER, "subfield_index").forceLiteral()
                ),
                "FromAggregateDoubleMetricEvaluator[field=Attribute[channel=0],subfieldIndex=" + index + "]",
                index == 3 ? DataType.INTEGER : DataType.DOUBLE,
                index == 3 ? Matchers.equalTo(agg_metric.getCount())
                    : expectedValue == null ? Matchers.nullValue()
                    : Matchers.closeTo(expectedValue, Math.abs(expectedValue * 0.00001))
            );
        }));

        return parameterSuppliersFromTypedData(
            anyNullIsNull(
                suppliers,
                (nullPosition, nullValueDataType, original) -> nullPosition == 1 ? DataType.NULL : original.expectedType(),
                (nullPosition, nullData, original) -> nullData.isForceLiteral() ? Matchers.equalTo("LiteralsEvaluator[lit=null]") : original
            )
        );
    }
}
