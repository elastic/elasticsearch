/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.expression.function.scalar.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.compute.data.BlockUtils.toJavaObject;
import static org.hamcrest.Matchers.equalTo;

public class NegTests extends AbstractScalarFunctionTestCase {

    public NegTests(@Name("TestCase") Supplier<TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return parameterSuppliersFromTypedData(List.of(new TestCaseSupplier("Integer", () -> {
            // Ensure we don't have an overflow
            int arg = randomIntBetween((Integer.MIN_VALUE + 1), Integer.MAX_VALUE);
            return new TestCase(
                List.of(new TypedData(arg, DataTypes.INTEGER, "arg")),
                "NegIntsEvaluator[v=Attribute[channel=0]]",
                DataTypes.INTEGER,
                equalTo(Math.negateExact(arg))
            );
        }), new TestCaseSupplier("Long", () -> {
            // Ensure we don't have an overflow
            long arg = randomLongBetween((Long.MIN_VALUE + 1), Long.MAX_VALUE);
            return new TestCase(
                List.of(new TypedData(arg, DataTypes.LONG, "arg")),
                "NegLongsEvaluator[v=Attribute[channel=0]]",
                DataTypes.LONG,
                equalTo(Math.negateExact(arg))
            );
        }), new TestCaseSupplier("Double", () -> {
            double arg = randomDouble();
            return new TestCase(
                List.of(new TypedData(arg, DataTypes.DOUBLE, "arg")),
                "NegDoublesEvaluator[v=Attribute[channel=0]]",
                DataTypes.DOUBLE,
                equalTo(-arg)
            );
        })));
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Neg(source, args.get(0));
    }

    @Override
    protected List<ArgumentSpec> argSpec() {
        // More precisely: numerics without unsigned longs; however, `Neg::resolveType` uses `numeric`.
        return List.of(required(numerics()));
    }

    @Override
    protected DataType expectedType(List<DataType> argTypes) {
        return argTypes.get(0);
    }

    public void testEdgeCases() {
        // Run the assertions for the current test cases type only to avoid running the same assertions multiple times.
        DataType testCaseType = testCase.getData().get(0).type();
        if (testCaseType.equals(DataTypes.INTEGER)) {
            assertEquals(null, process(Integer.MIN_VALUE));
            assertCriticalWarnings(
                "Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.",
                "java.lang.ArithmeticException: integer overflow"
            );

            return;
        }
        if (testCaseType.equals(DataTypes.LONG)) {
            assertEquals(null, process(Long.MIN_VALUE));
            assertCriticalWarnings(
                "Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.",
                "java.lang.ArithmeticException: long overflow"
            );

            return;
        }
        if (testCaseType.equals(DataTypes.DOUBLE)) {
            var negMaxValue = -Double.MAX_VALUE;
            assertEquals(negMaxValue, process(Double.MAX_VALUE));
            assertEquals(Double.MAX_VALUE, process(negMaxValue));

            var negMinValue = -Double.MIN_VALUE;
            assertEquals(negMinValue, process(Double.MIN_VALUE));
            assertEquals(Double.MIN_VALUE, process(negMinValue));

            assertEquals(Double.NEGATIVE_INFINITY, process(Double.POSITIVE_INFINITY));
            assertEquals(Double.POSITIVE_INFINITY, process(Double.NEGATIVE_INFINITY));

            assertEquals(Double.NaN, process(Double.NaN));

            return;
        }
        throw new AssertionError("Edge cases not tested for negation with type [" + testCaseType.typeName() + "]");
    }

    private Object process(Number val) {
        return toJavaObject(evaluator(new Neg(Source.EMPTY, field("val", typeOf(val)))).get().eval(row(List.of(val))), 0);
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
}
