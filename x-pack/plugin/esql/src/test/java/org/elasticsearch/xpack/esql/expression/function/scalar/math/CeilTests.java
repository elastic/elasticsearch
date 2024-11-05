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

public class CeilTests extends AbstractScalarFunctionTestCase {
    public CeilTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        suppliers.addAll(List.of(new TestCaseSupplier("large double value", List.of(DataType.DOUBLE), () -> {
            double arg = 1 / randomDouble();
            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(arg, DataType.DOUBLE, "arg")),
                "CeilDoubleEvaluator[val=Attribute[channel=0]]",
                DataType.DOUBLE,
                equalTo(Math.ceil(arg))
            );
        }), new TestCaseSupplier("integer value", List.of(DataType.INTEGER), () -> {
            int arg = randomInt();
            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(arg, DataType.INTEGER, "arg")),
                "Attribute[channel=0]",
                DataType.INTEGER,
                equalTo(arg)
            );
        }), new TestCaseSupplier("long value", List.of(DataType.LONG), () -> {
            long arg = randomLong();
            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(arg, DataType.LONG, "arg")),
                "Attribute[channel=0]",
                DataType.LONG,
                equalTo(arg)
            );
        })));

        TestCaseSupplier.forUnaryUnsignedLong(
            suppliers,
            "Attribute[channel=0]",
            DataType.UNSIGNED_LONG,
            (n) -> n,
            BigInteger.ZERO,
            UNSIGNED_LONG_MAX,
            List.of()
        );
        return parameterSuppliersFromTypedDataWithDefaultChecks(false, suppliers, (v, p) -> "numeric");
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Ceil(source, args.get(0));
    }
}
