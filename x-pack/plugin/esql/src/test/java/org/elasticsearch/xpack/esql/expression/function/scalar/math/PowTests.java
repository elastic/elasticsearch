/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.scalar.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

public class PowTests extends AbstractScalarFunctionTestCase {
    public PowTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return parameterSuppliersFromTypedData(List.of(new TestCaseSupplier("pow(<double>, <int>)", () -> {
            double base = 1 / randomDouble();
            int exponent = between(-30, 30);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(base, DataTypes.DOUBLE, "arg"),
                    new TestCaseSupplier.TypedData(exponent, DataTypes.INTEGER, "exp")
                ),
                "PowDoubleEvaluator[base=Attribute[channel=0], exponent=CastIntToDoubleEvaluator[v=Attribute[channel=1]]]",
                DataTypes.DOUBLE,
                equalTo(Math.pow(base, exponent))
            );
        }),
            new TestCaseSupplier(
                "pow(NaN, 1)",
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(Double.NaN, DataTypes.DOUBLE, "base"),
                        new TestCaseSupplier.TypedData(1.0d, DataTypes.DOUBLE, "exp")
                    ),
                    "PowDoubleEvaluator[base=Attribute[channel=0], exponent=Attribute[channel=1]]",
                    DataTypes.DOUBLE,
                    equalTo(null)
                ).withWarning("Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.")
                    .withWarning("java.lang.ArithmeticException: invalid result: pow(NaN, 1.0)")
            ),
            new TestCaseSupplier(
                "pow(1, NaN)",
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(1.0d, DataTypes.DOUBLE, "base"),
                        new TestCaseSupplier.TypedData(Double.NaN, DataTypes.DOUBLE, "exp")
                    ),
                    "PowDoubleEvaluator[base=Attribute[channel=0], exponent=Attribute[channel=1]]",
                    DataTypes.DOUBLE,
                    equalTo(null)
                ).withWarning("Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.")
                    .withWarning("java.lang.ArithmeticException: invalid result: pow(1.0, NaN)")
            ),
            new TestCaseSupplier(
                "pow(NaN, 0)",
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(Double.NaN, DataTypes.DOUBLE, "base"),
                        new TestCaseSupplier.TypedData(0d, DataTypes.DOUBLE, "exp")
                    ),
                    "PowDoubleEvaluator[base=Attribute[channel=0], exponent=Attribute[channel=1]]",
                    DataTypes.DOUBLE,
                    equalTo(1d)
                )
            ),
            new TestCaseSupplier(
                "pow(0, 0)",
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(0d, DataTypes.DOUBLE, "base"),
                        new TestCaseSupplier.TypedData(0d, DataTypes.DOUBLE, "exp")
                    ),
                    "PowDoubleEvaluator[base=Attribute[channel=0], exponent=Attribute[channel=1]]",
                    DataTypes.DOUBLE,
                    equalTo(1d)
                )
            ),
            new TestCaseSupplier(
                "pow(1, 1)",
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(1, DataTypes.INTEGER, "base"),
                        new TestCaseSupplier.TypedData(1, DataTypes.INTEGER, "base")
                    ),
                    "PowIntEvaluator[base=CastIntToDoubleEvaluator[v=Attribute[channel=0]], "
                        + "exponent=CastIntToDoubleEvaluator[v=Attribute[channel=1]]]",
                    DataTypes.INTEGER,
                    equalTo(1)
                )
            ),
            new TestCaseSupplier(
                "pow(integer, 0)",
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(randomValueOtherThan(0, ESTestCase::randomInt), DataTypes.INTEGER, "base"),
                        new TestCaseSupplier.TypedData(0, DataTypes.INTEGER, "exp")
                    ),
                    "PowIntEvaluator[base=CastIntToDoubleEvaluator[v=Attribute[channel=0]], "
                        + "exponent=CastIntToDoubleEvaluator[v=Attribute[channel=1]]]",
                    DataTypes.INTEGER,
                    equalTo(1)
                )
            ),
            new TestCaseSupplier("pow(integer, 2)", () -> {
                int base = randomIntBetween(-1000, 1000);
                return new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(base, DataTypes.INTEGER, "base"),
                        new TestCaseSupplier.TypedData(2, DataTypes.INTEGER, "exp")
                    ),
                    "PowIntEvaluator[base=CastIntToDoubleEvaluator[v=Attribute[channel=0]], "
                        + "exponent=CastIntToDoubleEvaluator[v=Attribute[channel=1]]]",
                    DataTypes.INTEGER,
                    equalTo((int) Math.pow(base, 2))
                );
            }),
            new TestCaseSupplier(
                "integer overflow case",
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(Integer.MAX_VALUE, DataTypes.INTEGER, "base"),
                        new TestCaseSupplier.TypedData(2, DataTypes.INTEGER, "exp")
                    ),
                    "PowIntEvaluator[base=CastIntToDoubleEvaluator[v=Attribute[channel=0]], "
                        + "exponent=CastIntToDoubleEvaluator[v=Attribute[channel=1]]]",
                    DataTypes.INTEGER,
                    equalTo(null)
                ).withWarning("Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.")
                    .withWarning("java.lang.ArithmeticException: integer overflow")
            ),
            new TestCaseSupplier(
                "long overflow case",
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(Long.MAX_VALUE, DataTypes.LONG, "base"),
                        new TestCaseSupplier.TypedData(2, DataTypes.INTEGER, "exp")
                    ),
                    "PowLongEvaluator[base=CastLongToDoubleEvaluator[v=Attribute[channel=0]], "
                        + "exponent=CastIntToDoubleEvaluator[v=Attribute[channel=1]]]",
                    DataTypes.LONG,
                    equalTo(null)
                ).withWarning("Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.")
                    .withWarning("java.lang.ArithmeticException: long overflow")
            ),
            new TestCaseSupplier(
                "pow(2, 0.5) == sqrt(2)",
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(2, DataTypes.INTEGER, "base"),
                        new TestCaseSupplier.TypedData(0.5, DataTypes.DOUBLE, "exp")
                    ),
                    "PowDoubleEvaluator[base=CastIntToDoubleEvaluator[v=Attribute[channel=0]], exponent=Attribute[channel=1]]",
                    DataTypes.DOUBLE,
                    equalTo(Math.sqrt(2))
                )
            ),
            new TestCaseSupplier(
                "pow(2.0, 0.5) == sqrt(2)",
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(2d, DataTypes.DOUBLE, "base"),
                        new TestCaseSupplier.TypedData(0.5, DataTypes.DOUBLE, "exp")
                    ),
                    "PowDoubleEvaluator[base=Attribute[channel=0], exponent=Attribute[channel=1]]",
                    DataTypes.DOUBLE,
                    equalTo(Math.sqrt(2))
                )
            ),
            new TestCaseSupplier("pow(integer, double)", () -> {
                // Negative numbers to a non-integer power are NaN
                int base = randomIntBetween(0, 1000);
                double exp = randomDoubleBetween(-10.0, 10.0, true);
                double expected = Math.pow(base, exp);
                TestCaseSupplier.TestCase testCase = new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(base, DataTypes.INTEGER, "base"),
                        new TestCaseSupplier.TypedData(exp, DataTypes.DOUBLE, "exp")
                    ),
                    "PowDoubleEvaluator[base=CastIntToDoubleEvaluator[v=Attribute[channel=0]], exponent=Attribute[channel=1]]",
                    DataTypes.DOUBLE,
                    equalTo(expected)
                );
                return testCase;
            }),
            new TestCaseSupplier("fractional power of negative integer is null", () -> {
                // Negative numbers to a non-integer power are NaN
                int base = randomIntBetween(-1000, -1);
                double exp = randomDouble(); // between 0 and 1
                TestCaseSupplier.TestCase testCase = new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(base, DataTypes.INTEGER, "base"),
                        new TestCaseSupplier.TypedData(exp, DataTypes.DOUBLE, "exp")
                    ),
                    "PowDoubleEvaluator[base=CastIntToDoubleEvaluator[v=Attribute[channel=0]], exponent=Attribute[channel=1]]",
                    DataTypes.DOUBLE,
                    equalTo(null)
                ).withWarning("Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.")
                    .withWarning("java.lang.ArithmeticException: invalid result: pow(" + (double) base + ", " + exp + ")");
                return testCase;
            }),
            new TestCaseSupplier(
                "pow(123, -1)",
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(123, DataTypes.INTEGER, "base"),
                        new TestCaseSupplier.TypedData(-1, DataTypes.INTEGER, "exp")
                    ),
                    "PowIntEvaluator[base=CastIntToDoubleEvaluator[v=Attribute[channel=0]], "
                        + "exponent=CastIntToDoubleEvaluator[v=Attribute[channel=1]]]",
                    DataTypes.INTEGER,
                    equalTo(0)
                )
            ),
            new TestCaseSupplier(
                "pow(123L, -1)",
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(123L, DataTypes.LONG, "base"),
                        new TestCaseSupplier.TypedData(-1, DataTypes.INTEGER, "exp")
                    ),
                    "PowLongEvaluator[base=CastLongToDoubleEvaluator[v=Attribute[channel=0]], "
                        + "exponent=CastIntToDoubleEvaluator[v=Attribute[channel=1]]]",
                    DataTypes.LONG,
                    equalTo(0L)
                )
            ),
            new TestCaseSupplier(
                "pow(123D, -1)",
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(123.0, DataTypes.DOUBLE, "base"),
                        new TestCaseSupplier.TypedData(-1, DataTypes.INTEGER, "exp")
                    ),
                    "PowDoubleEvaluator[base=Attribute[channel=0], exponent=CastIntToDoubleEvaluator[v=Attribute[channel=1]]]",
                    DataTypes.DOUBLE,
                    equalTo(1D / 123D)
                )
            ),
            new TestCaseSupplier("pow(integer, 1)", () -> {
                int base = randomInt();
                return new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(base, DataTypes.INTEGER, "base"),
                        new TestCaseSupplier.TypedData(1, DataTypes.INTEGER, "exp")
                    ),
                    "PowIntEvaluator[base=CastIntToDoubleEvaluator[v=Attribute[channel=0]], "
                        + "exponent=CastIntToDoubleEvaluator[v=Attribute[channel=1]]]",
                    DataTypes.INTEGER,
                    equalTo(base)
                );
            }),
            new TestCaseSupplier(
                "pow(1L, 1)",
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(1L, DataTypes.LONG, "base"),
                        new TestCaseSupplier.TypedData(1, DataTypes.INTEGER, "exp")
                    ),
                    "PowLongEvaluator[base=CastLongToDoubleEvaluator[v=Attribute[channel=0]], "
                        + "exponent=CastIntToDoubleEvaluator[v=Attribute[channel=1]]]",
                    DataTypes.LONG,
                    equalTo(1L)
                )
            ),
            new TestCaseSupplier("pow(long, 1)", () -> {
                // Avoid double precision loss
                long base = randomLongBetween(-1L << 51, 1L << 51);
                return new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(base, DataTypes.LONG, "base"),
                        new TestCaseSupplier.TypedData(1, DataTypes.INTEGER, "exp")
                    ),
                    "PowLongEvaluator[base=CastLongToDoubleEvaluator[v=Attribute[channel=0]], "
                        + "exponent=CastIntToDoubleEvaluator[v=Attribute[channel=1]]]",
                    DataTypes.LONG,
                    equalTo(base)
                );
            }),
            new TestCaseSupplier("long-double overflow", () -> {
                long base = 4339622345450989181L; // Not exactly representable as a double
                long expected = 4339622345450989056L;
                return new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(base, DataTypes.LONG, "base"),
                        new TestCaseSupplier.TypedData(1, DataTypes.INTEGER, "exp")
                    ),
                    "PowLongEvaluator[base=CastLongToDoubleEvaluator[v=Attribute[channel=0]], "
                        + "exponent=CastIntToDoubleEvaluator[v=Attribute[channel=1]]]",
                    DataTypes.LONG,
                    equalTo(expected)
                );
            }),
            new TestCaseSupplier("pow(long, 0)", () -> {
                long base = randomLong();
                return new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(base, DataTypes.LONG, "base"),
                        new TestCaseSupplier.TypedData(0, DataTypes.INTEGER, "exp")
                    ),
                    "PowLongEvaluator[base=CastLongToDoubleEvaluator[v=Attribute[channel=0]], "
                        + "exponent=CastIntToDoubleEvaluator[v=Attribute[channel=1]]]",
                    DataTypes.LONG,
                    equalTo(1L)
                );
            }),
            new TestCaseSupplier("pow(long, 2)", () -> {
                long base = randomLongBetween(-1000, 1000);
                return new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(base, DataTypes.LONG, "base"),
                        new TestCaseSupplier.TypedData(2, DataTypes.INTEGER, "exp")
                    ),
                    "PowLongEvaluator[base=CastLongToDoubleEvaluator[v=Attribute[channel=0]], "
                        + "exponent=CastIntToDoubleEvaluator[v=Attribute[channel=1]]]",
                    DataTypes.LONG,
                    equalTo((long) Math.pow(base, 2))
                );
            }),
            new TestCaseSupplier("pow(long, double)", () -> {
                // Negative numbers to non-integer power are NaN
                long base = randomLongBetween(0, 1000);
                double exp = randomDoubleBetween(-10.0, 10.0, true);
                double expected = Math.pow(base, exp);
                TestCaseSupplier.TestCase testCase = new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(base, DataTypes.LONG, "base"),
                        new TestCaseSupplier.TypedData(exp, DataTypes.DOUBLE, "exp")
                    ),
                    "PowDoubleEvaluator[base=CastLongToDoubleEvaluator[v=Attribute[channel=0]], exponent=Attribute[channel=1]]",
                    DataTypes.DOUBLE,
                    equalTo(expected)
                );
                return testCase;
            }),
            new TestCaseSupplier(
                "pow(1D, 1)",
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(1D, DataTypes.DOUBLE, "base"),
                        new TestCaseSupplier.TypedData(1, DataTypes.INTEGER, "exp")
                    ),
                    "PowDoubleEvaluator[base=Attribute[channel=0], exponent=CastIntToDoubleEvaluator[v=Attribute[channel=1]]]",
                    DataTypes.DOUBLE,
                    equalTo(1D)
                )
            ),
            new TestCaseSupplier("pow(double, 1)", () -> {
                double base;
                if (randomBoolean()) {
                    base = randomDouble();
                } else {
                    // Sometimes pick a large number
                    base = 1 / randomDouble();
                }
                return new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(base, DataTypes.DOUBLE, "base"),
                        new TestCaseSupplier.TypedData(1, DataTypes.INTEGER, "exp")
                    ),
                    "PowDoubleEvaluator[base=Attribute[channel=0], exponent=CastIntToDoubleEvaluator[v=Attribute[channel=1]]]",
                    DataTypes.DOUBLE,
                    equalTo(base)
                );
            }),
            new TestCaseSupplier("pow(double, 0)", () -> {
                double base;
                if (randomBoolean()) {
                    base = randomDouble();
                } else {
                    // Sometimes pick a large number
                    base = 1 / randomDouble();
                }
                return new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(base, DataTypes.DOUBLE, "base"),
                        new TestCaseSupplier.TypedData(0, DataTypes.INTEGER, "exp")
                    ),
                    "PowDoubleEvaluator[base=Attribute[channel=0], exponent=CastIntToDoubleEvaluator[v=Attribute[channel=1]]]",
                    DataTypes.DOUBLE,
                    equalTo(1D)
                );
            }),
            new TestCaseSupplier("pow(double, 2)", () -> {
                double base = randomDoubleBetween(-1000, 1000, true);
                return new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(base, DataTypes.DOUBLE, "base"),
                        new TestCaseSupplier.TypedData(2, DataTypes.INTEGER, "exp")
                    ),
                    "PowDoubleEvaluator[base=Attribute[channel=0], exponent=CastIntToDoubleEvaluator[v=Attribute[channel=1]]]",
                    DataTypes.DOUBLE,
                    equalTo(Math.pow(base, 2))
                );
            }),
            new TestCaseSupplier("pow(double, double)", () -> {
                // Negative numbers to a non-integer power are NaN
                double base = randomDoubleBetween(0, 1000, true);
                double exp = randomDoubleBetween(-10.0, 10.0, true);
                TestCaseSupplier.TestCase testCase = new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(base, DataTypes.DOUBLE, "base"),
                        new TestCaseSupplier.TypedData(exp, DataTypes.DOUBLE, "exp")
                    ),
                    "PowDoubleEvaluator[base=Attribute[channel=0], exponent=Attribute[channel=1]]",
                    DataTypes.DOUBLE,
                    equalTo(Math.pow(base, exp))
                );
                return testCase;
            })
        ));
    }

    @Override
    protected DataType expectedType(List<DataType> argTypes) {
        var base = argTypes.get(0);
        var exp = argTypes.get(1);
        if (base.isRational() || exp.isRational()) {
            return DataTypes.DOUBLE;
        } else if (base.size() == Long.BYTES || exp.size() == Long.BYTES) {
            return DataTypes.LONG;
        } else {
            return DataTypes.INTEGER;
        }
    }

    @Override
    protected List<ArgumentSpec> argSpec() {
        return List.of(required(numerics()), required(numerics()));
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Pow(source, args.get(0), args.get(1));
    }

}
