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
import org.elasticsearch.xpack.esql.expression.function.UnaryTestCaseHelper;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier.unary;
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

        UnaryTestCaseHelper helper = unary().evaluatorToString("AtanhEvaluator[val=%0]").expectedOutputType(DataType.DOUBLE);
        helper.expectedFromDouble(d -> d).castingToDouble(0, 0, true, suppliers);
        helper.expectedFromDouble(d -> {
            double canonical = canonicalAtanh(d);
            double err = Math.ulp(canonical) * 2;
            // Our canonical implementation isn't 100% in line with the real one. That's ok.
            if (Math.abs(d) > 0.94) {
                err *= 2;
            }
            if (Math.abs(d) > 0.98) {
                err *= 8;
            }
            if (Math.abs(d) > 0.998) {
                err *= 4;
            }
            if (Math.abs(d) > 0.9998) {
                err = 0.0001;
            }
            return closeTo(canonical, err);
        }).castingToDouble(Math.nextUp(-1), Math.nextDown(1), false, suppliers);

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

        UnaryTestCaseHelper outOfRange = helper.expectNullAndWarnings(
            o -> List.of("Line 1:1: java.lang.ArithmeticException: Atanh input out of range")
        );
        outOfRange.castingToDouble(1.0, 1.0, false, suppliers);
        outOfRange.castingToDouble(-1.0, -1.0, false, suppliers);
        outOfRange.castingToDouble(Math.nextUp(1.0), Double.POSITIVE_INFINITY, false, suppliers);
        outOfRange.castingToDouble(Double.NEGATIVE_INFINITY, Math.nextDown(-1.0), false, suppliers);
        return parameterSuppliersFromTypedDataWithDefaultChecks(true, suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Atanh(source, args.getFirst());
    }
}
