/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import ch.obermuhlner.math.big.BigDecimalMath;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

public class AtanhTests extends AbstractScalarFunctionTestCase {

    // Canonical formula: https://en.wikipedia.org/wiki/Inverse_hyperbolic_functions#Definitions_in_terms_of_logarithms
    private static double canonicalAtanh(double x) {
        BigDecimal bd = BigDecimal.valueOf(x);
        BigDecimal num = BigDecimal.ONE.add(bd);
        BigDecimal den = BigDecimal.ONE.subtract(bd);
        BigDecimal ratio = num.divide(den, MathContext.DECIMAL128);
        return BigDecimalMath.log(ratio, MathContext.DECIMAL128).multiply(new BigDecimal("0.5")).doubleValue();
    }

    public AtanhTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        suppliers.add(
            new TestCaseSupplier(
                "atanh(0)",
                List.of(DataType.DOUBLE),
                () -> new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(0.0, DataType.DOUBLE, "arg")),
                    "AtanhEvaluator[val=Attribute[channel=0]]",
                    DataType.DOUBLE,
                    equalTo(0.0)
                )
            )
        );

        suppliers.add(
            new TestCaseSupplier(
                "atanh(0.5)",
                List.of(DataType.DOUBLE),
                () -> new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(0.5, DataType.DOUBLE, "arg")),
                    "AtanhEvaluator[val=Attribute[channel=0]]",
                    DataType.DOUBLE,
                    closeTo(canonicalAtanh(0.5), Math.ulp(canonicalAtanh(0.5)))
                )
            )
        );

        suppliers.add(
            new TestCaseSupplier(
                "atanh(-0.5)",
                List.of(DataType.DOUBLE),
                () -> new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(-0.5, DataType.DOUBLE, "arg")),
                    "AtanhEvaluator[val=Attribute[channel=0]]",
                    DataType.DOUBLE,
                    closeTo(canonicalAtanh(-0.5), Math.ulp(canonicalAtanh(-0.5)))
                )
            )
        );

        suppliers.add(
            new TestCaseSupplier(
                "atanh(0.9)",
                List.of(DataType.DOUBLE),
                () -> new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(0.9, DataType.DOUBLE, "arg")),
                    "AtanhEvaluator[val=Attribute[channel=0]]",
                    DataType.DOUBLE,
                    closeTo(canonicalAtanh(0.9), Math.ulp(canonicalAtanh(0.9)))
                )
            )
        );

        // Out of range
        suppliers.add(
            new TestCaseSupplier(
                "atanh(1)",
                List.of(DataType.DOUBLE),
                () -> new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(1.0, DataType.DOUBLE, "arg")),
                    "AtanhEvaluator[val=Attribute[channel=0]]",
                    DataType.DOUBLE,
                    equalTo(null)
                ).withWarning("Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.")
                    .withWarning("Line 1:1: java.lang.ArithmeticException: Atanh input out of range")
            )
        );

        // Out of range
        suppliers.add(
            new TestCaseSupplier(
                "atanh(-1)",
                List.of(DataType.DOUBLE),
                () -> new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(-1.0, DataType.DOUBLE, "arg")),
                    "AtanhEvaluator[val=Attribute[channel=0]]",
                    DataType.DOUBLE,
                    equalTo(null)
                ).withWarning("Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.")
                    .withWarning("Line 1:1: java.lang.ArithmeticException: Atanh input out of range")
            )
        );

        // Out of range
        suppliers.addAll(
            TestCaseSupplier.forUnaryCastingToDouble(
                "AtanhEvaluator",
                "val",
                k -> null,
                1.0000001d,
                Double.POSITIVE_INFINITY,
                List.of(
                    "Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.",
                    "Line 1:1: java.lang.ArithmeticException: Atanh input out of range"
                )
            )
        );

        // Out of range
        suppliers.addAll(
            TestCaseSupplier.forUnaryCastingToDouble(
                "AtanhEvaluator",
                "val",
                k -> null,
                Double.NEGATIVE_INFINITY,
                -1.0000001d,
                List.of(
                    "Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.",
                    "Line 1:1: java.lang.ArithmeticException: Atanh input out of range"
                )
            )
        );
        return parameterSuppliersFromTypedDataWithDefaultChecks(true, suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Atanh(source, args.getFirst());
    }
}
