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

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static java.util.Collections.emptyList;

@FunctionName("to_aggregate_metric_double")
public class ToAggregateMetricDoubleTests extends AbstractScalarFunctionTestCase {
    public ToAggregateMetricDoubleTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        if (args.get(0).dataType() == DataType.AGGREGATE_METRIC_DOUBLE) {
            assumeTrue("Test sometimes wraps literals as fields", args.get(0).foldable());
        }
        return new ToAggregateMetricDouble(source, args.get(0));
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        final String evaluatorStringLeft = "ToAggregateMetricDoubleFrom";
        final String evaluatorStringRight = "Evaluator[field=Attribute[channel=0]]";
        final List<TestCaseSupplier> suppliers = new ArrayList<>();

        TestCaseSupplier.forUnaryInt(
            suppliers,
            evaluatorStringLeft + "Int" + evaluatorStringRight,
            DataType.AGGREGATE_METRIC_DOUBLE,
            i -> new AggregateMetricDoubleBlockBuilder.AggregateMetricDoubleLiteral((double) i, (double) i, (double) i, 1),
            Integer.MIN_VALUE,
            Integer.MAX_VALUE,
            emptyList()
        );
        TestCaseSupplier.forUnaryLong(
            suppliers,
            evaluatorStringLeft + "Long" + evaluatorStringRight,
            DataType.AGGREGATE_METRIC_DOUBLE,
            l -> new AggregateMetricDoubleBlockBuilder.AggregateMetricDoubleLiteral((double) l, (double) l, (double) l, 1),
            Long.MIN_VALUE,
            Long.MAX_VALUE,
            emptyList()
        );
        TestCaseSupplier.forUnaryUnsignedLong(
            suppliers,
            evaluatorStringLeft + "UnsignedLong" + evaluatorStringRight,
            DataType.AGGREGATE_METRIC_DOUBLE,
            ul -> {
                var newVal = ul.doubleValue();
                return new AggregateMetricDoubleBlockBuilder.AggregateMetricDoubleLiteral(newVal, newVal, newVal, 1);
            },
            BigInteger.ZERO,
            UNSIGNED_LONG_MAX,
            emptyList()
        );
        TestCaseSupplier.forUnaryDouble(
            suppliers,
            evaluatorStringLeft + "Double" + evaluatorStringRight,
            DataType.AGGREGATE_METRIC_DOUBLE,
            d -> new AggregateMetricDoubleBlockBuilder.AggregateMetricDoubleLiteral(d, d, d, 1),
            Double.NEGATIVE_INFINITY,
            Double.POSITIVE_INFINITY,
            emptyList()
        );

        return parameterSuppliersFromTypedDataWithDefaultChecksNoErrors(true, suppliers);
    }

}
