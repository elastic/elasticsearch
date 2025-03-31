/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.compute.data.AggregateMetricDoubleBlockBuilder;
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

@FunctionName("from_aggregate_metric_double")
public class FromAggregateMetricDoubleTests extends AbstractScalarFunctionTestCase {
    public FromAggregateMetricDoubleTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        assumeTrue("Test sometimes wraps literals as fields", args.get(1).foldable());
        return new FromAggregateMetricDouble(source, args.get(0), args.get(1));
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        DataType dataType = DataType.AGGREGATE_METRIC_DOUBLE;
        for (int i = 0; i < 4; i++) {
            int index = i;
            suppliers.add(new TestCaseSupplier(List.of(dataType, DataType.INTEGER), () -> {
                var agg_metric = new AggregateMetricDoubleBlockBuilder.AggregateMetricDoubleLiteral(
                    randomDoubleBetween(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, true),
                    randomDoubleBetween(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, true),
                    randomDoubleBetween(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, true),
                    randomIntBetween(Integer.MIN_VALUE, Integer.MAX_VALUE)
                );
                Double expectedValue = index == AggregateMetricDoubleBlockBuilder.Metric.MIN.getIndex() ? agg_metric.min()
                    : index == AggregateMetricDoubleBlockBuilder.Metric.MAX.getIndex() ? agg_metric.max()
                    : index == AggregateMetricDoubleBlockBuilder.Metric.SUM.getIndex() ? agg_metric.sum()
                    : (Double) agg_metric.count().doubleValue();

                return new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(agg_metric, dataType, "agg_metric"),
                        new TestCaseSupplier.TypedData(index, DataType.INTEGER, "subfield_index").forceLiteral()
                    ),
                    "FromAggregateMetricDoubleEvaluator[field=Attribute[channel=0],subfieldIndex=" + index + "]",
                    index == AggregateMetricDoubleBlockBuilder.Metric.COUNT.getIndex() ? DataType.INTEGER : DataType.DOUBLE,
                    index == AggregateMetricDoubleBlockBuilder.Metric.COUNT.getIndex() ? Matchers.equalTo(agg_metric.count())
                        : expectedValue == null ? Matchers.nullValue()
                        : Matchers.closeTo(expectedValue, Math.abs(expectedValue * 0.00001))
                );
            }));
        }

        return parameterSuppliersFromTypedData(
            anyNullIsNull(
                suppliers,
                (nullPosition, nullValueDataType, original) -> nullPosition == 1 ? DataType.NULL : original.expectedType(),
                (nullPosition, nullData, original) -> nullData.isForceLiteral() ? Matchers.equalTo("LiteralsEvaluator[lit=null]") : original
            )
        );
    }

    @Override
    protected Expression serializeDeserializeExpression(Expression expression) {
        // AggregateMetricDoubleLiteral can't be serialized when it's a literal
        return expression;
    }
}
