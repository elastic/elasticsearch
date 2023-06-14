/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.hamcrest.Matcher;

import java.util.List;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class MvMedianTests extends AbstractMultivalueFunctionTestCase {
    @Override
    protected Expression build(Source source, Expression field) {
        return new MvMedian(source, field);
    }

    @Override
    protected DataType[] supportedTypes() {
        return representableNumerics();
    }

    @Override
    protected Matcher<Object> resultMatcherForInput(List<?> input) {
        int middle = input.size() / 2;
        return switch (LocalExecutionPlanner.toElementType(EsqlDataTypes.fromJava(input.get(0)))) {
            case DOUBLE -> {
                DoubleStream s = input.stream().mapToDouble(o -> (Double) o).sorted();
                yield equalTo((input.size() % 2 == 1 ? s.skip(middle).findFirst() : s.skip(middle - 1).limit(2).average()).getAsDouble());
            }
            case INT -> {
                IntStream s = input.stream().mapToInt(o -> (Integer) o).sorted();
                yield equalTo(input.size() % 2 == 1 ? s.skip(middle).findFirst().getAsInt() : s.skip(middle - 1).limit(2).sum() >>> 1);
            }
            case LONG -> {
                LongStream s = input.stream().mapToLong(o -> (Long) o).sorted();
                yield equalTo(input.size() % 2 == 1 ? s.skip(middle).findFirst().getAsLong() : s.skip(middle - 1).limit(2).sum() >>> 1);
            }
            case NULL -> nullValue();
            default -> throw new UnsupportedOperationException("unsupported type " + input);
        };
    }

    @Override
    protected String expectedEvaluatorSimpleToString() {
        return "MvMedian[field=Attribute[channel=0]]";
    }

    public void testRounding() {
        assertThat(
            build(Source.EMPTY, List.of(new Literal(Source.EMPTY, 1, DataTypes.INTEGER), new Literal(Source.EMPTY, 2, DataTypes.INTEGER)))
                .fold(),
            equalTo(1)
        );
        assertThat(
            build(Source.EMPTY, List.of(new Literal(Source.EMPTY, -2, DataTypes.INTEGER), new Literal(Source.EMPTY, -1, DataTypes.INTEGER)))
                .fold(),
            equalTo(-2)
        );
    }
}
