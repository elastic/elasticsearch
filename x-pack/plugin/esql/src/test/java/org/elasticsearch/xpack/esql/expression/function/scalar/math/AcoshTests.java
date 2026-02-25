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

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

public class AcoshTests extends AbstractScalarFunctionTestCase {

    private static final double ACOSH_OF_2 = 1.3169578969248166;
    private static final double ACOSH_OF_10 = 2.993222846126381;

    public AcoshTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        suppliers.add(
            new TestCaseSupplier(
                "acosh(1)",
                List.of(DataType.DOUBLE),
                () -> new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(1.0, DataType.DOUBLE, "arg")),
                    "AcoshEvaluator[val=Attribute[channel=0]]",
                    DataType.DOUBLE,
                    equalTo(0.0)
                )
            )
        );
        suppliers.add(
            new TestCaseSupplier(
                "acosh(2)",
                List.of(DataType.DOUBLE),
                () -> new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(2.0, DataType.DOUBLE, "arg")),
                    "AcoshEvaluator[val=Attribute[channel=0]]",
                    DataType.DOUBLE,
                    closeTo(ACOSH_OF_2, Math.ulp(ACOSH_OF_2))
                )
            )
        );
        suppliers.add(
            new TestCaseSupplier(
                "acosh(10)",
                List.of(DataType.DOUBLE),
                () -> new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(10.0, DataType.DOUBLE, "arg")),
                    "AcoshEvaluator[val=Attribute[channel=0]]",
                    DataType.DOUBLE,
                    closeTo(ACOSH_OF_10, Math.ulp(ACOSH_OF_10))
                )
            )
        );

        // Out of range (x < 1)
        suppliers.addAll(
            TestCaseSupplier.forUnaryCastingToDouble(
                "AcoshEvaluator",
                "val",
                k -> null,
                Double.NEGATIVE_INFINITY,
                0.999999d,
                List.of(
                    "Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.",
                    "Line 1:1: java.lang.ArithmeticException: Acosh input out of range"
                )
            )
        );
        return parameterSuppliersFromTypedDataWithDefaultChecks(true, suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Acosh(source, args.getFirst());
    }
}
