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

import static org.hamcrest.Matchers.equalTo;

public class AbsTests extends AbstractScalarFunctionTestCase {
    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        suppliers.add(new TestCaseSupplier(List.of(DataType.INTEGER), () -> {
            int arg = randomInt();
            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(arg, DataType.INTEGER, "arg")),
                "AbsIntEvaluator[fieldVal=Attribute[channel=0]]",
                DataType.INTEGER,
                equalTo(Math.abs(arg))
            );
        }));
        TestCaseSupplier.forUnaryUnsignedLong(
            suppliers,
            "Attribute[channel=0]",
            DataType.UNSIGNED_LONG,
            (n) -> n,
            BigInteger.ZERO,
            UNSIGNED_LONG_MAX,
            List.of()
        );
        suppliers.add(new TestCaseSupplier(List.of(DataType.LONG), () -> {
            long arg = randomLong();
            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(arg, DataType.LONG, "arg")),
                "AbsLongEvaluator[fieldVal=Attribute[channel=0]]",
                DataType.LONG,
                equalTo(Math.abs(arg))
            );
        }));
        suppliers.add(new TestCaseSupplier(List.of(DataType.DOUBLE), () -> {
            double arg = randomDouble();
            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(arg, DataType.DOUBLE, "arg")),
                "AbsDoubleEvaluator[fieldVal=Attribute[channel=0]]",
                DataType.DOUBLE,
                equalTo(Math.abs(arg))
            );
        }));
        return parameterSuppliersFromTypedDataWithDefaultChecksNoErrors(false, suppliers);
    }

    public AbsTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Abs(source, args.get(0));
    }
}
