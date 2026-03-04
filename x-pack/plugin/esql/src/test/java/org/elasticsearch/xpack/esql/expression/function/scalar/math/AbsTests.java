/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

public class AbsTests extends AbstractScalarFunctionTestCase {
    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        TestCaseSupplier.forUnaryInt(
            suppliers,
            "AbsIntEvaluator[fieldVal=Attribute[channel=0]]",
            DataType.INTEGER,
            Math::absExact,
            Integer.MIN_VALUE + 1,
            Integer.MAX_VALUE,
            List.of()
        );
        TestCaseSupplier.forUnaryInt(
            suppliers,
            "AbsIntEvaluator[fieldVal=Attribute[channel=0]]",
            DataType.INTEGER,
            z -> null,
            Integer.MIN_VALUE,
            Integer.MIN_VALUE,
            List.of(
                "Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.",
                "Line 1:1: java.lang.ArithmeticException: Overflow to represent absolute value of Integer.MIN_VALUE"
            )
        );
        TestCaseSupplier.forUnaryUnsignedLong(
            suppliers,
            "Attribute[channel=0]",
            DataType.UNSIGNED_LONG,
            (n) -> n,
            BigInteger.ZERO,
            UNSIGNED_LONG_MAX,
            List.of()
        );
        TestCaseSupplier.forUnaryLong(
            suppliers,
            "AbsLongEvaluator[fieldVal=Attribute[channel=0]]",
            DataType.LONG,
            Math::absExact,
            Long.MIN_VALUE + 1,
            Long.MAX_VALUE,
            List.of()
        );
        TestCaseSupplier.forUnaryLong(
            suppliers,
            "AbsLongEvaluator[fieldVal=Attribute[channel=0]]",
            DataType.LONG,
            z -> null,
            Long.MIN_VALUE,
            Long.MIN_VALUE,
            List.of(
                "Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.",
                "Line 1:1: java.lang.ArithmeticException: Overflow to represent absolute value of Long.MIN_VALUE"
            )
        );
        TestCaseSupplier.forUnaryDouble(
            suppliers,
            "AbsDoubleEvaluator[fieldVal=Attribute[channel=0]]",
            DataType.DOUBLE,
            Math::abs,
            Double.NEGATIVE_INFINITY,
            Double.POSITIVE_INFINITY,
            List.of()
        );
        return parameterSuppliersFromTypedDataWithDefaultChecks(false, suppliers);
    }

    public AbsTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Abs(source, args.get(0));
    }
}
