/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.search.aggregations.metrics.CompensatedSum;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.NumericUtils;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.DoubleStream;

import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.unsignedLongToDouble;
import static org.hamcrest.Matchers.equalTo;

public class MvAvgTests extends AbstractMultivalueFunctionTestCase {
    public MvAvgTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        BiFunction<Integer, DoubleStream, Matcher<Object>> avg = (size, values) -> {
            CompensatedSum sum = new CompensatedSum();
            values.forEach(sum::add);
            return equalTo(sum.value() / size);
        };
        List<TestCaseSupplier> cases = new ArrayList<>();
        doubles(cases, "mv_avg", "MvAvg", DataType.DOUBLE, avg);
        ints(cases, "mv_avg", "MvAvg", DataType.DOUBLE, (size, data) -> avg.apply(size, data.mapToDouble(v -> (double) v)));
        longs(cases, "mv_avg", "MvAvg", DataType.DOUBLE, (size, data) -> avg.apply(size, data.mapToDouble(v -> (double) v)));
        unsignedLongs(
            cases,
            "mv_avg",
            "MvAvg",
            DataType.DOUBLE,
            /*
             * Converting strait from BigInteger to double will round differently.
             * So we have to go back to encoded `long` and then convert to double
             * using the production conversion. That'll round in the same way.
             */
            (size, data) -> avg.apply(size, data.mapToDouble(v -> unsignedLongToDouble(NumericUtils.asLongUnsigned(v))))
        );
        return parameterSuppliersFromTypedDataWithDefaultChecksNoErrors(true, cases);
    }

    @Override
    protected Expression build(Source source, Expression field) {
        return new MvAvg(source, field);
    }

    @Override
    protected DataType expectedType(List<DataType> argTypes) {
        return DataType.DOUBLE;  // Averages are always a double
    }
}
