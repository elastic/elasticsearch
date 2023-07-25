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
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.hamcrest.Matcher;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class StartsWithTests extends AbstractScalarFunctionTestCase {
    @Override
    protected TestCase getSimpleTestCase() {
        String str = randomAlphaOfLength(5);
        String prefix = randomAlphaOfLength(5);
        if (randomBoolean()) {
            str = prefix + str;
        }
        List<TypedData> typedData = List.of(
            new TypedData(new BytesRef(str), DataTypes.KEYWORD, "str"),
            new TypedData(new BytesRef(prefix), DataTypes.KEYWORD, "prefix")
        );
        return new TestCase(Source.EMPTY, typedData, resultsMatcher(typedData));
    }

    @Override
    protected DataType expectedType(List<DataType> argTypes) {
        return DataTypes.BOOLEAN;
    }

    private Matcher<Object> resultsMatcher(List<TypedData> typedData) {
        String str = ((BytesRef) typedData.get(0).data()).utf8ToString();
        String prefix = ((BytesRef) typedData.get(1).data()).utf8ToString();
        return equalTo(str.startsWith(prefix));
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
    protected List<ArgumentSpec> argSpec() {
        return List.of(required(strings()), required(strings()));
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new StartsWith(source, args.get(0), args.get(1));
    }
}
