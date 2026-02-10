/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.core.ESSloppyMath;
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

public class AsinhTests extends AbstractScalarFunctionTestCase {

    private static final double ASINH_OF_1 = 0.881373587019543;
    private static final double ASINH_OF_10 = 2.99822295029797;

    public AsinhTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        suppliers.add(
            new TestCaseSupplier(
                "asinh(0)",
                List.of(DataType.DOUBLE),
                () -> new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(0.0, DataType.DOUBLE, "arg")),
                    "AsinhEvaluator[val=Attribute[channel=0]]",
                    DataType.DOUBLE,
                    equalTo(0.0)
                )
            )
        );

        suppliers.add(
            new TestCaseSupplier(
                "asinh(1)",
                List.of(DataType.DOUBLE),
                () -> new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(1.0, DataType.DOUBLE, "arg")),
                    "AsinhEvaluator[val=Attribute[channel=0]]",
                    DataType.DOUBLE,
                    closeTo(ASINH_OF_1, Math.ulp(ASINH_OF_1))
                )
            )
        );

        suppliers.add(
            new TestCaseSupplier(
                "asinh(-1)",
                List.of(DataType.DOUBLE),
                () -> new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(-1.0, DataType.DOUBLE, "arg")),
                    "AsinhEvaluator[val=Attribute[channel=0]]",
                    DataType.DOUBLE,
                    closeTo(-ASINH_OF_1, Math.ulp(ASINH_OF_1))
                )
            )
        );

        suppliers.add(
            new TestCaseSupplier(
                "asinh(10)",
                List.of(DataType.DOUBLE),
                () -> new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(10.0, DataType.DOUBLE, "arg")),
                    "AsinhEvaluator[val=Attribute[channel=0]]",
                    DataType.DOUBLE,
                    closeTo(ASINH_OF_10, Math.ulp(ASINH_OF_10))
                )
            )
        );

        suppliers.addAll(
            TestCaseSupplier.forUnaryCastingToDouble(
                "AsinhEvaluator",
                "val",
                ESSloppyMath::asinh,
                Double.NEGATIVE_INFINITY,
                Double.POSITIVE_INFINITY,
                List.of()
            )
        );
        return parameterSuppliersFromTypedDataWithDefaultChecks(true, suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Asinh(source, args.getFirst());
    }
}
