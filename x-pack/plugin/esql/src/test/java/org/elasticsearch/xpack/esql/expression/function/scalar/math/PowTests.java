/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import org.elasticsearch.xpack.esql.expression.function.scalar.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.hamcrest.Matcher;

import java.util.List;

import static org.elasticsearch.compute.data.BlockUtils.toJavaObject;
import static org.hamcrest.Matchers.equalTo;

public class PowTests extends AbstractScalarFunctionTestCase {

    public void testExamples() {
        // Test NaN
        assertEquals(Double.NaN, process(Double.NaN, 1));
        assertEquals(Double.NaN, process(1, Double.NaN));

        // Test with Integers
        assertEquals(1, process(1, 1));
        assertEquals(1, process(randomIntBetween(-1000, 1000), 0));
        int baseInt = randomIntBetween(-1000, 1000);
        assertEquals(baseInt, process(baseInt, 1));
        assertEquals((int) Math.pow(baseInt, 2), process(baseInt, 2));
        assertEquals(0, process(123, -1));
        double exponentDouble = randomDoubleBetween(-10.0, 10.0, true);
        assertEquals(Math.pow(baseInt, exponentDouble), process(baseInt, exponentDouble));

        // Test with Longs
        assertEquals(1L, process(1L, 1));
        assertEquals(1L, process(randomLongBetween(-1000, 1000), 0));
        long baseLong = randomLongBetween(-1000, 1000);
        assertEquals(baseLong, process(baseLong, 1));
        assertEquals((long) Math.pow(baseLong, 2), process(baseLong, 2));
        assertEquals(0, process(123, -1));
        assertEquals(Math.pow(baseLong, exponentDouble), process(baseLong, exponentDouble));

        // Test with Doubles
        assertEquals(1.0, process(1.0, 1));
        assertEquals(1.0, process(randomDoubleBetween(-1000.0, 1000.0, true), 0));
        double baseDouble = randomDoubleBetween(-1000.0, 1000.0, true);
        assertEquals(baseDouble, process(baseDouble, 1));
        assertEquals(Math.pow(baseDouble, 2), process(baseDouble, 2));
        assertEquals(0, process(123, -1));
        assertEquals(Math.pow(baseDouble, exponentDouble), process(baseDouble, exponentDouble));
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
    protected List<Object> simpleData() {
        return List.of(1 / randomDouble(), between(-30, 30));
    }

    @Override
    protected Expression expressionForSimpleData() {
        return new Pow(Source.EMPTY, field("arg", DataTypes.DOUBLE), field("exp", DataTypes.INTEGER));
    }

    @Override
    protected DataType expectedType(List<DataType> argTypes) {
        var base = argTypes.get(0);
        var exp = argTypes.get(1);
        if (base.isRational() || exp.isRational()) {
            return DataTypes.DOUBLE;
        } else if (base == DataTypes.UNSIGNED_LONG || exp == DataTypes.UNSIGNED_LONG) {
            return DataTypes.DOUBLE;
        } else if (base == DataTypes.LONG || exp == DataTypes.LONG) {
            return DataTypes.LONG;
        } else {
            return DataTypes.INTEGER;
        }
    }

    @Override
    protected Matcher<Object> resultMatcher(List<Object> data, DataType dataType) {
        return equalTo(Math.pow(((Number) data.get(0)).doubleValue(), ((Number) data.get(1)).doubleValue()));
    }

    @Override
    protected String expectedEvaluatorSimpleToString() {
        return "PowDoubleEvaluator[base=Attribute[channel=0], exponent=CastIntToDoubleEvaluator[v=Attribute[channel=1]]]";
    }

    @Override
    protected Expression constantFoldable(List<Object> data) {
        return new Pow(
            Source.EMPTY,
            new Literal(Source.EMPTY, data.get(0), DataTypes.DOUBLE),
            new Literal(Source.EMPTY, data.get(1), DataTypes.INTEGER)
        );
    }

    @Override
    protected List<ArgumentSpec> argSpec() {
        return List.of(required(numerics()), required(numerics()));
    }

    @Override
    protected Expression build(Source source, List<Literal> args) {
        return new Pow(source, args.get(0), args.get(1));
    }
}
