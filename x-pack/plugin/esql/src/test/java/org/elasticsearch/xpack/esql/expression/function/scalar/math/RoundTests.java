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
import org.elasticsearch.xpack.ql.expression.predicate.operator.math.Maths;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.hamcrest.Matcher;

import java.util.List;

import static org.elasticsearch.compute.data.BlockUtils.toJavaObject;
import static org.hamcrest.Matchers.equalTo;

public class RoundTests extends AbstractScalarFunctionTestCase {

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
        return toJavaObject(evaluator(new Round(Source.EMPTY, field("val", typeOf(val)), null)).get().eval(row(List.of(val))), 0);
    }

    private Object process(Number val, int decimals) {
        return toJavaObject(
            evaluator(new Round(Source.EMPTY, field("val", typeOf(val)), field("decimals", DataTypes.INTEGER))).get()
                .eval(row(List.of(val, decimals))),
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
        return new Round(Source.EMPTY, field("arg", DataTypes.DOUBLE), field("precision", DataTypes.INTEGER));
    }

    @Override
    protected DataType expectedType(List<DataType> argTypes) {
        return argTypes.get(0);
    }

    @Override
    protected Matcher<Object> resultMatcher(List<Object> data) {
        return equalTo(Maths.round((Number) data.get(0), (Number) data.get(1)));
    }

    @Override
    protected String expectedEvaluatorSimpleToString() {
        return "RoundDoubleEvaluator[val=Attribute[channel=0], decimals=CastIntToLongEvaluator[v=Attribute[channel=1]]]";
    }

    public void testNoDecimalsToString() {
        assertThat(
            evaluator(new Round(Source.EMPTY, field("val", DataTypes.DOUBLE), null)).get().toString(),
            equalTo("RoundDoubleNoDecimalsEvaluator[val=Attribute[channel=0]]")
        );
    }

    @Override
    protected Expression constantFoldable(List<Object> data) {
        return new Round(
            Source.EMPTY,
            new Literal(Source.EMPTY, data.get(0), DataTypes.DOUBLE),
            new Literal(Source.EMPTY, data.get(1), DataTypes.INTEGER)
        );
    }

    @Override
    protected List<ArgumentSpec> argSpec() {
        return List.of(required(numerics()), optional(integers()));
    }

    @Override
    protected Expression build(Source source, List<Literal> args) {
        return new Round(source, args.get(0), args.size() < 2 ? null : args.get(1));
    }
}
