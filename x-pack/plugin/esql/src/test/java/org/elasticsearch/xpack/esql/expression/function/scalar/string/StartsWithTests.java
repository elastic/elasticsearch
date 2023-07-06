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

import static org.hamcrest.Matchers.equalTo;

public class StartsWithTests extends AbstractScalarFunctionTestCase {
    @Override
    protected List<Object> simpleData() {
        String str = randomAlphaOfLength(5);
        String prefix = randomAlphaOfLength(5);
        if (randomBoolean()) {
            str = prefix + str;
        }
        return List.of(new BytesRef(str), new BytesRef(prefix));
    }

    @Override
    protected Expression expressionForSimpleData() {
        return new StartsWith(Source.EMPTY, field("str", DataTypes.KEYWORD), field("prefix", DataTypes.KEYWORD));
    }

    @Override
    protected DataType expectedType(List<DataType> argTypes) {
        return DataTypes.BOOLEAN;
    }

    @Override
    protected Matcher<Object> resultMatcher(List<Object> data, DataType dataType) {
        String str = ((BytesRef) data.get(0)).utf8ToString();
        String prefix = ((BytesRef) data.get(1)).utf8ToString();
        return equalTo(str.startsWith(prefix));
    }

    @Override
    protected String expectedEvaluatorSimpleToString() {
        return "StartsWithEvaluator[str=Attribute[channel=0], prefix=Attribute[channel=1]]";
    }

    @Override
    protected Expression constantFoldable(List<Object> data) {
        return new StartsWith(
            Source.EMPTY,
            new Literal(Source.EMPTY, (BytesRef) data.get(0), DataTypes.KEYWORD),
            new Literal(Source.EMPTY, (BytesRef) data.get(1), DataTypes.KEYWORD)
        );
    }

    @Override
    protected List<ArgumentSpec> argSpec() {
        return List.of(required(strings()), required(strings()));
    }

    @Override
    protected Expression build(Source source, List<Literal> args) {
        return new StartsWith(source, args.get(0), args.get(1));
    }
}
