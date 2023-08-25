/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.expression.function.scalar.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

public class Log10Tests extends AbstractScalarFunctionTestCase {
    public Log10Tests(@Name("TestCase") Supplier<TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return parameterSuppliersFromTypedData(List.of(new TestCaseSupplier("Log10 of Double", () -> {
            double arg;
            if (randomBoolean()) {
                arg = randomDouble();
            } else {
                arg = 1 / randomDouble();
            }
            return new TestCase(
                List.of(new TypedData(arg, DataTypes.DOUBLE, "arg")),
                "Log10DoubleEvaluator[val=Attribute[channel=0]]",
                DataTypes.DOUBLE,
                equalTo(Math.log10(arg))
            );
        }), new TestCaseSupplier("Log10(negative double)", () -> {
            double arg = randomDoubleBetween(Double.NEGATIVE_INFINITY, 0, false);
            return new TestCase(
                List.of(new TypedData(arg, DataTypes.DOUBLE, "arg")),
                "Log10DoubleEvaluator[val=Attribute[channel=0]]",
                DataTypes.DOUBLE,
                equalTo(null)
            ).withWarning("Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.")
                .withWarning("java.lang.ArithmeticException: log of non-positive number");
        }), new TestCaseSupplier("Log10(negative integer)", () -> {
            int arg = randomIntBetween(Integer.MIN_VALUE, -1); // it's inclusive
            return new TestCase(
                List.of(new TypedData(arg, DataTypes.INTEGER, "arg")),
                "Log10IntEvaluator[val=Attribute[channel=0]]",
                DataTypes.DOUBLE,
                equalTo(null)
            ).withWarning("Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.")
                .withWarning("java.lang.ArithmeticException: log of non-positive number");
        }), new TestCaseSupplier("Log10(integer)", () -> {
            int arg = randomIntBetween(1, Integer.MAX_VALUE); // it's inclusive
            return new TestCase(
                List.of(new TypedData(arg, DataTypes.INTEGER, "arg")),
                "Log10IntEvaluator[val=Attribute[channel=0]]",
                DataTypes.DOUBLE,
                equalTo(Math.log10(arg))
            );
        }), new TestCaseSupplier("Log10(long)", () -> {
            long arg = randomLongBetween(1, Long.MAX_VALUE); // it's inclusive
            return new TestCase(
                List.of(new TypedData(arg, DataTypes.LONG, "arg")),
                "Log10LongEvaluator[val=Attribute[channel=0]]",
                DataTypes.DOUBLE,
                equalTo(Math.log10(arg))
            );
        }), new TestCaseSupplier("Log10(negative long)", () -> {
            long arg = randomLongBetween(Long.MIN_VALUE, 0); // it's inclusive
            return new TestCase(
                List.of(new TypedData(arg, DataTypes.LONG, "arg")),
                "Log10LongEvaluator[val=Attribute[channel=0]]",
                DataTypes.DOUBLE,
                equalTo(null)
            ).withWarning("Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.")
                .withWarning("java.lang.ArithmeticException: log of non-positive number");
        })

        ));
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Log10(source, args.get(0));
    }

    @Override
    protected List<ArgumentSpec> argSpec() {
        return List.of(required(numerics()));
    }

    @Override
    protected DataType expectedType(List<DataType> argTypes) {
        return DataTypes.DOUBLE;
    }
}
