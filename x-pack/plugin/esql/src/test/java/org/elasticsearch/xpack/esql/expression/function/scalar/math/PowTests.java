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

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.compute.data.BlockUtils.toJavaObject;
import static org.hamcrest.Matchers.equalTo;

public class PowTests extends AbstractScalarFunctionTestCase {
    public PowTests(@Name("TestCase") Supplier<TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return parameterSuppliersFromTypedData(List.of(new TestCaseSupplier("pow(<double>, <int>)", () -> {
            double base = 1 / randomDouble();
            int exponent = between(-30, 30);
            return new TestCase(
                List.of(new TypedData(base, DataTypes.DOUBLE, "arg"), new TypedData(exponent, DataTypes.INTEGER, "exp")),
                "PowDoubleEvaluator[base=Attribute[channel=0], exponent=CastIntToDoubleEvaluator[v=Attribute[channel=1]]]",
                DataTypes.DOUBLE,
                equalTo(Math.pow(base, exponent))
            );
        })));
    }

    public void testExamples() {
        // Test NaN
        assertEquals(null, process(Double.NaN, 1));
        assertEquals(null, process(1, Double.NaN));

        // Test with Integers
        assertEquals(1, process(1, 1));
        assertEquals(1, process(randomIntBetween(-1000, 1000), 0));
        int baseInt = randomIntBetween(-1000, 1000);
        assertEquals(baseInt, process(baseInt, 1));
        assertEquals((int) Math.pow(baseInt, 2), process(baseInt, 2));
        assertEquals(0, process(123, -1));
        double exponentDouble = randomDoubleBetween(-10.0, 10.0, true);
        assertWithNanCheck(Math.pow(baseInt, exponentDouble), baseInt, exponentDouble);

        // Test with Longs
        assertEquals(1L, process(1L, 1));
        assertEquals(1L, process(randomLongBetween(-1000, 1000), 0));
        long baseLong = randomLongBetween(-1000, 1000);
        assertEquals(baseLong, process(baseLong, 1));
        assertEquals((long) Math.pow(baseLong, 2), process(baseLong, 2));
        assertEquals(0, process(123, -1));
        assertWithNanCheck(Math.pow(baseLong, exponentDouble), baseLong, exponentDouble);

        // Test with Doubles
        assertEquals(1.0, process(1.0, 1));
        assertEquals(1.0, process(randomDoubleBetween(-1000.0, 1000.0, true), 0));
        double baseDouble = randomDoubleBetween(-1000.0, 1000.0, true);
        assertEquals(baseDouble, process(baseDouble, 1));
        assertEquals(Math.pow(baseDouble, 2), process(baseDouble, 2));
        assertEquals(0, process(123, -1));
        assertWithNanCheck(Math.pow(baseDouble, exponentDouble), baseDouble, exponentDouble);
    }

    private void assertWithNanCheck(double expected, Number base, double exponent) {
        if (Double.isNaN(expected)) {
            ignoreWarning("java.lang.ArithmeticException: invalid result: pow(" + base.doubleValue() + ", " + exponent + ")");
            assertNull("pow(" + base + "," + exponent + ") yields NaN, so we expect NULL", process(base, exponent));
        } else {
            assertEquals("pow(" + base + "," + exponent + ")", expected, process(base, exponent));
        }
    }

    private Object process(Number base, Number exponent) {
        return toJavaObject(
            evaluator(new Pow(Source.EMPTY, field("base", typeOf(base)), field("exponent", typeOf(exponent)))).get()
                .eval(row(List.of(base, exponent))),
            0
        );
    }

    private DataType typeOf(Number val) {
        if (val instanceof Integer) {
            return DataTypes.INTEGER;
        }
        if (val instanceof Long) {
            return DataTypes.LONG;
        }
        if (val instanceof Double) {
            return DataTypes.DOUBLE;
        }
        throw new UnsupportedOperationException("unsupported type [" + val.getClass() + "]");
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

    private List<String> ignoreWarnings = new ArrayList<>();

    private void ignoreWarning(String warning) {
        ignoreWarnings.add(warning);
    }

    @Override
    public void ensureNoWarnings() {
        super.ensureNoWarnings();
        ignoreWarnings.clear();
    }

    @Override
    protected List<String> filteredWarnings() {
        // TODO: This avoids failing the tests for ArithmeticExceptions, but it would be better to assert on the expected warnings
        // That would involve overriding ensureWarnings() and getting access to the threadContext
        List<String> filteredWarnings = super.filteredWarnings();
        filteredWarnings.add("Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.");
        filteredWarnings.add("java.lang.ArithmeticException: invalid result: pow(NaN, 1.0)");
        filteredWarnings.add("java.lang.ArithmeticException: invalid result: pow(1.0, NaN)");
        filteredWarnings.addAll(ignoreWarnings);
        return filteredWarnings;
    }
}
