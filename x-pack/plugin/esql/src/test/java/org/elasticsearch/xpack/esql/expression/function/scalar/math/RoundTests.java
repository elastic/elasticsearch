/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.scalar.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.predicate.operator.math.Maths;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.compute.data.BlockUtils.toJavaObject;
import static org.hamcrest.Matchers.equalTo;

public class RoundTests extends AbstractScalarFunctionTestCase {
    public RoundTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return parameterSuppliersFromTypedData(List.of(new TestCaseSupplier("round(<double>, <int>)", () -> {
            double number = 1 / randomDouble();
            int precision = between(-30, 30);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(number, DataTypes.DOUBLE, "number"),
                    new TestCaseSupplier.TypedData(precision, DataTypes.INTEGER, "precision")
                ),
                "RoundDoubleEvaluator[val=Attribute[channel=0], decimals=CastIntToLongEvaluator[v=Attribute[channel=1]]]",
                DataTypes.DOUBLE,
                equalTo(Maths.round(number, precision))
            );
        })));
    }

    public void testExamples() {
        assertEquals(123, process(123));
        assertEquals(123, process(123, randomIntBetween(0, 1024)));
        assertEquals(120, process(123, -1));
        assertEquals(123.5, process(123.45, 1));
        assertEquals(123.0, process(123.45, 0));
        assertEquals(123.0, process(123.45));
        assertEquals(123L, process(123L, 0));
        assertEquals(123L, process(123L, 5));
        assertEquals(120L, process(123L, -1));
        assertEquals(100L, process(123L, -2));
        assertEquals(0L, process(123L, -3));
        assertEquals(0L, process(123L, -100));
        assertEquals(1000L, process(999L, -1));
        assertEquals(1000.0, process(999.0, -1));
        assertEquals(130L, process(125L, -1));
        assertEquals(12400L, process(12350L, -2));
        assertEquals(12400.0, process(12350.0, -2));
        assertEquals(12300.0, process(12349.0, -2));
        assertEquals(-12300L, process(-12349L, -2));
        assertEquals(-12400L, process(-12350L, -2));
        assertEquals(-12400.0, process(-12350.0, -2));
        assertEquals(-100L, process(-123L, -2));
        assertEquals(-120.0, process(-123.45, -1));
        assertEquals(-123.5, process(-123.45, 1));
        assertEquals(-124.0, process(-123.5, 0));
        assertEquals(-123.0, process(-123.45));
        assertEquals(123.456, process(123.456, Integer.MAX_VALUE));
        assertEquals(0.0, process(123.456, Integer.MIN_VALUE));
        assertEquals(0L, process(0L, 0));
        assertEquals(0, process(0, 0));
        assertEquals(Long.MAX_VALUE, process(Long.MAX_VALUE));
        assertEquals(Long.MAX_VALUE, process(Long.MAX_VALUE, 5));
        assertEquals(Long.MIN_VALUE, process(Long.MIN_VALUE));
        assertEquals(Long.MIN_VALUE, process(Long.MIN_VALUE, 5));
    }

    private Object process(Number val) {
        try (
            Block.Ref ref = evaluator(new Round(Source.EMPTY, field("val", typeOf(val)), null)).get(driverContext()).eval(row(List.of(val)))
        ) {
            return toJavaObject(ref.block(), 0);
        }
    }

    private Object process(Number val, int decimals) {
        try (
            Block.Ref ref = evaluator(new Round(Source.EMPTY, field("val", typeOf(val)), field("decimals", DataTypes.INTEGER))).get(
                driverContext()
            ).eval(row(List.of(val, decimals)))
        ) {
            return toJavaObject(ref.block(), 0);
        }
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
        return argTypes.get(0);
    }

    public void testNoDecimalsToString() {
        assertThat(
            evaluator(new Round(Source.EMPTY, field("val", DataTypes.DOUBLE), null)).get(driverContext()).toString(),
            equalTo("RoundDoubleNoDecimalsEvaluator[val=Attribute[channel=0]]")
        );
    }

    @Override
    protected List<ArgumentSpec> argSpec() {
        return List.of(required(numerics()), optional(integers()));
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Round(source, args.get(0), args.size() < 2 ? null : args.get(1));
    }
}
