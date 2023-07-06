/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.esql.expression.function.scalar.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.hamcrest.Matcher;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.elasticsearch.compute.data.BlockUtils.toJavaObject;
import static org.hamcrest.Matchers.equalTo;

public class ConcatTests extends AbstractScalarFunctionTestCase {
    @Override
    protected List<Object> simpleData() {
        return List.of(new BytesRef(randomAlphaOfLength(3)), new BytesRef(randomAlphaOfLength(3)));
    }

    @Override
    protected Expression expressionForSimpleData() {
        return new Concat(Source.EMPTY, field("first", DataTypes.KEYWORD), List.of(field("second", DataTypes.KEYWORD)));
    }

    @Override
    protected DataType expectedType(List<DataType> argTypes) {
        return DataTypes.KEYWORD;
    }

    @Override
    protected Matcher<Object> resultMatcher(List<Object> simpleData, DataType dataType) {
        return equalTo(new BytesRef(simpleData.stream().map(o -> ((BytesRef) o).utf8ToString()).collect(Collectors.joining())));
    }

    @Override
    protected String expectedEvaluatorSimpleToString() {
        return "ConcatEvaluator[values=[Attribute[channel=0], Attribute[channel=1]]]";
    }

    @Override
    protected Expression constantFoldable(List<Object> simpleData) {
        return new Concat(
            Source.EMPTY,
            new Literal(Source.EMPTY, simpleData.get(0), DataTypes.KEYWORD),
            List.of(new Literal(Source.EMPTY, simpleData.get(1), DataTypes.KEYWORD))
        );
    }

    @Override
    protected List<ArgumentSpec> argSpec() {
        return List.of(
            required(strings()),
            optional(strings()),
            optional(strings()),
            optional(strings()),
            optional(strings()),
            optional(strings()),
            optional(strings()),
            optional(strings()),
            optional(strings()),
            optional(strings()),
            optional(strings()),
            optional(strings())
        );
    }

    @Override
    protected Expression build(Source source, List<Literal> args) {
        return new Concat(source, args.get(0), args.subList(1, args.size()));
    }

    @Override
    protected Matcher<String> badTypeError(List<ArgumentSpec> specs, int badArgPosition, DataType badArgType) {
        return equalTo("argument of [exp] must be [string], found value [arg" + badArgPosition + "] type [" + badArgType.typeName() + "]");
    }

    public void testMany() {
        List<Object> simpleData = Stream.of("cats", " ", "and", " ", "dogs").map(s -> (Object) new BytesRef(s)).toList();
        assertThat(
            toJavaObject(
                evaluator(
                    new Concat(
                        Source.EMPTY,
                        field("a", DataTypes.KEYWORD),
                        IntStream.range(1, 5).mapToObj(i -> field(Integer.toString(i), DataTypes.KEYWORD)).toList()
                    )
                ).get().eval(row(simpleData)),
                0
            ),
            equalTo(new BytesRef("cats and dogs"))
        );
    }

    public void testSomeConstant() {
        List<Object> simpleData = Stream.of("cats", "and", "dogs").map(s -> (Object) new BytesRef(s)).toList();
        assertThat(
            toJavaObject(
                evaluator(
                    new Concat(
                        Source.EMPTY,
                        field("a", DataTypes.KEYWORD),
                        List.of(
                            new Literal(Source.EMPTY, new BytesRef(" "), DataTypes.KEYWORD),
                            field("b", DataTypes.KEYWORD),
                            new Literal(Source.EMPTY, new BytesRef(" "), DataTypes.KEYWORD),
                            field("c", DataTypes.KEYWORD)
                        )
                    )
                ).get().eval(row(simpleData)),
                0
            ),
            equalTo(new BytesRef("cats and dogs"))
        );
    }
}
