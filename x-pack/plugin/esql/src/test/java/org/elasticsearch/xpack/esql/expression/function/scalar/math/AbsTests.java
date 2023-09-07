/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.scalar.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

public class AbsTests extends AbstractScalarFunctionTestCase {
    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        suppliers.add(new TestCaseSupplier(List.of(DataTypes.INTEGER), () -> {
            int arg = randomInt();
            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(arg, DataTypes.INTEGER, "arg")),
                "AbsIntEvaluator[fieldVal=Attribute[channel=0]]",
                DataTypes.INTEGER,
                equalTo(Math.abs(arg))
            );
        }));
        suppliers.add(new TestCaseSupplier(List.of(DataTypes.UNSIGNED_LONG), () -> {
            long arg = randomLong();
            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(arg, DataTypes.UNSIGNED_LONG, "arg")),
                "Attribute[channel=0]",
                DataTypes.UNSIGNED_LONG,
                equalTo(arg)
            );
        }));
        suppliers.add(new TestCaseSupplier(List.of(DataTypes.LONG), () -> {
            long arg = randomLong();
            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(arg, DataTypes.LONG, "arg")),
                "AbsLongEvaluator[fieldVal=Attribute[channel=0]]",
                DataTypes.LONG,
                equalTo(Math.abs(arg))
            );
        }));
        suppliers.add(new TestCaseSupplier(List.of(DataTypes.DOUBLE), () -> {
            double arg = randomDouble();
            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(arg, DataTypes.DOUBLE, "arg")),
                "AbsDoubleEvaluator[fieldVal=Attribute[channel=0]]",
                DataTypes.DOUBLE,
                equalTo(Math.abs(arg))
            );
        }));
        return parameterSuppliersFromTypedData(errorsForCasesWithoutExamples(anyNullIsNull(false, suppliers)));
    }

    public AbsTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Abs(source, args.get(0));
    }

    @Override
    protected List<ArgumentSpec> argSpec() {
        return List.of(required(numerics()));
    }

    @Override
    protected DataType expectedType(List<DataType> argTypes) {
        return argTypes.get(0);
    }
}
