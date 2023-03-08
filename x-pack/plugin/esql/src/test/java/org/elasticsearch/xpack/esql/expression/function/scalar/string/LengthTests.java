/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.expression.function.scalar.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.hamcrest.Matcher;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class LengthTests extends AbstractScalarFunctionTestCase {
    @Override
    protected List<Object> simpleData() {
        return List.of(new BytesRef(randomAlphaOfLength(between(0, 10000))));
    }

    @Override
    protected Expression expressionForSimpleData() {
        return new Length(Source.EMPTY, field("f", DataTypes.KEYWORD));
    }

    @Override
    protected DataType expressionForSimpleDataType() {
        return DataTypes.INTEGER;
    }

    @Override
    protected Matcher<Object> resultMatcher(List<Object> simpleData) {
        return equalTo(UnicodeUtil.codePointCount((BytesRef) simpleData.get(0)));
    }

    @Override
    protected String expectedEvaluatorSimpleToString() {
        return "LengthEvaluator[exp=Keywords[channel=0]]";
    }

    @Override
    protected Expression constantFoldable(List<Object> simpleData) {
        return new Length(Source.EMPTY, new Literal(Source.EMPTY, simpleData.get(0), DataTypes.KEYWORD));
    }

    @Override
    protected List<ArgumentSpec> argSpec() {
        return List.of(required(DataTypes.KEYWORD));
    }

    @Override
    protected Expression build(Source source, List<Literal> args) {
        return new Length(source, args.get(0));
    }

    public void testExamples() {
        EvalOperator.ExpressionEvaluator eval = evaluator(expressionForSimpleData()).get();
        assertThat(eval.computeRow(row(List.of(new BytesRef(""))), 0), equalTo(0));
        assertThat(eval.computeRow(row(List.of(new BytesRef("a"))), 0), equalTo(1));
        assertThat(eval.computeRow(row(List.of(new BytesRef("clump"))), 0), equalTo(5));
        assertThat(eval.computeRow(row(List.of(new BytesRef("☕"))), 0), equalTo(1));  // 3 bytes, 1 code point
        assertThat(eval.computeRow(row(List.of(new BytesRef("❗️"))), 0), equalTo(2));  // 6 bytes, 2 code points
        assertThat(eval.computeRow(row(List.of(new BytesRef(randomAlphaOfLength(100)))), 0), equalTo(100));
        assertThat(eval.computeRow(row(List.of(new BytesRef(randomUnicodeOfCodepointLength(100)))), 0), equalTo(100));
    }
}
