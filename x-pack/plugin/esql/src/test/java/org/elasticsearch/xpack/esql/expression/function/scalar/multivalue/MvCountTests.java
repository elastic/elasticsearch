/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.hamcrest.Matcher;

import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class MvCountTests extends AbstractMultivalueFunctionTestCase {
    public MvCountTests(@Name("TestCase") Supplier<TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return parameterSuppliersFromTypedData(List.of(new TestCaseSupplier("mv_count(<double>)", () -> {
            List<Double> mvData = randomList(1, 100, () -> randomDouble());
            return new TestCase(
                Source.EMPTY,
                List.of(new TypedData(mvData, DataTypes.DOUBLE, "field")),
                "MvCount[field=Attribute[channel=0]]",
                equalTo(mvData.size())
            );
        })));
    }

    @Override
    protected Expression build(Source source, Expression field) {
        return new MvCount(source, field);
    }

    @Override
    protected DataType[] supportedTypes() {
        return representable();
    }

    @Override
    protected DataType expectedType(List<DataType> argTypes) {
        return DataTypes.INTEGER;
    }

    @Override
    protected Matcher<Object> resultMatcherForInput(List<?> input, DataType dataType) {
        return input == null ? nullValue() : equalTo(input.size());
    }

}
