/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

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

public class ToDegreesTests extends AbstractScalarFunctionTestCase {
    public ToDegreesTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        // TODO multivalue fields
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        TestCaseSupplier.forUnaryInt(
            suppliers,
            evaluatorName("ToDoubleFromIntEvaluator", "i"),
            DataType.DOUBLE,
            Math::toDegrees,
            Integer.MIN_VALUE,
            Integer.MAX_VALUE,
            List.of()
        );
        TestCaseSupplier.forUnaryLong(
            suppliers,
            evaluatorName("ToDoubleFromLongEvaluator", "l"),
            DataType.DOUBLE,
            Math::toDegrees,
            Long.MIN_VALUE,
            Long.MAX_VALUE,
            List.of()
        );
        TestCaseSupplier.forUnaryUnsignedLong(
            suppliers,
            evaluatorName("ToDoubleFromUnsignedLongEvaluator", "l"),
            DataType.DOUBLE,
            ul -> Math.toDegrees(ul.doubleValue()),
            BigInteger.ZERO,
            UNSIGNED_LONG_MAX,
            List.of()
        );
        TestCaseSupplier.forUnaryDouble(suppliers, "ToDegreesEvaluator[deg=Attribute[channel=0]]", DataType.DOUBLE, d -> {
            double deg = Math.toDegrees(d);
            return Double.isNaN(deg) || Double.isInfinite(deg) ? null : deg;
        }, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, d -> {
            double deg = Math.toDegrees(d);
            ArrayList<String> warnings = new ArrayList<>(2);
            if (Double.isNaN(deg) || Double.isInfinite(deg)) {
                warnings.add("Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.");
                warnings.add("Line 1:1: java.lang.ArithmeticException: not a finite double number: " + deg);
            }
            return warnings;
        });
        TestCaseSupplier.unary(
            suppliers,
            "ToDegreesEvaluator[deg=Attribute[channel=0]]",
            List.of(
                new TestCaseSupplier.TypedDataSupplier("Double.MAX_VALUE", () -> Double.MAX_VALUE, DataType.DOUBLE),
                new TestCaseSupplier.TypedDataSupplier("-Double.MAX_VALUE", () -> -Double.MAX_VALUE, DataType.DOUBLE),
                new TestCaseSupplier.TypedDataSupplier("Double.POSITIVE_INFINITY", () -> Double.POSITIVE_INFINITY, DataType.DOUBLE),
                new TestCaseSupplier.TypedDataSupplier("Double.NEGATIVE_INFINITY", () -> Double.NEGATIVE_INFINITY, DataType.DOUBLE)
            ),
            DataType.DOUBLE,
            d -> null,
            d -> List.of(
                "Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.",
                "Line 1:1: java.lang.ArithmeticException: not a finite double number: " + ((double) d > 0 ? "Infinity" : "-Infinity")
            )
        );

        return parameterSuppliersFromTypedDataWithDefaultChecksNoErrors(true, suppliers);
    }

    private static String evaluatorName(String inner, String next) {
        return "ToDegreesEvaluator[deg=" + inner + "[" + next + "=Attribute[channel=0]]]";
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new ToDegrees(source, args.get(0));
    }
}
