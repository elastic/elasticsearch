/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.expression.function.scalar.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.hamcrest.Matcher;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.joining;
import static org.elasticsearch.compute.data.BlockUtils.toJavaObject;
import static org.hamcrest.Matchers.equalTo;

public class SplitTests extends AbstractScalarFunctionTestCase {
    @Override
    protected List<Object> simpleData() {
        String delimiter = randomAlphaOfLength(1);
        String str = IntStream.range(0, between(1, 5))
            .mapToObj(i -> randomValueOtherThanMany(s -> s.contains(delimiter), () -> randomAlphaOfLength(4)))
            .collect(joining(delimiter));
        return List.of(new BytesRef(str), new BytesRef(delimiter));
    }

    @Override
    protected Expression expressionForSimpleData() {
        return new Split(Source.EMPTY, field("str", DataTypes.KEYWORD), field("delim", DataTypes.KEYWORD));
    }

    @Override
    protected DataType expectedType(List<DataType> argTypes) {
        return DataTypes.KEYWORD;
    }

    @Override
    protected Matcher<Object> resultMatcher(List<Object> data, DataType dataType) {
        String str = ((BytesRef) data.get(0)).utf8ToString();
        String delim = ((BytesRef) data.get(1)).utf8ToString();
        List<BytesRef> split = Arrays.stream(str.split(Pattern.quote(delim))).map(BytesRef::new).toList();
        return equalTo(split.size() == 1 ? split.get(0) : split);
    }

    @Override
    protected String expectedEvaluatorSimpleToString() {
        return "SplitVariableEvaluator[str=Attribute[channel=0], delim=Attribute[channel=1]]";
    }

    @Override
    protected Expression constantFoldable(List<Object> data) {
        return new Split(
            Source.EMPTY,
            new Literal(Source.EMPTY, data.get(0), DataTypes.KEYWORD),
            new Literal(Source.EMPTY, data.get(1), DataTypes.KEYWORD)
        );
    }

    @Override
    protected List<ArgumentSpec> argSpec() {
        return List.of(required(strings()), required(strings()));
    }

    @Override
    protected Expression build(Source source, List<Literal> args) {
        return new Split(source, args.get(0), args.get(1));
    }

    public void testConstantDelimiter() {
        EvalOperator.ExpressionEvaluator eval = evaluator(
            new Split(Source.EMPTY, field("str", DataTypes.KEYWORD), new Literal(Source.EMPTY, new BytesRef(":"), DataTypes.KEYWORD))
        ).get();
        /*
         * 58 is ascii for : and appears in the toString below. We don't convert the delimiter to a
         * string because we aren't really sure it's printable. It could be a tab or a bell or some
         * garbage.
         */
        assert ':' == 58;
        assertThat(eval.toString(), equalTo("SplitSingleByteEvaluator[str=Attribute[channel=0], delim=58]"));
        assertThat(
            toJavaObject(eval.eval(new Page(BytesRefBlock.newConstantBlockWith(new BytesRef("foo:bar"), 1))), 0),
            equalTo(List.of(new BytesRef("foo"), new BytesRef("bar")))
        );
    }
}
