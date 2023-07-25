/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.esql.expression.function.scalar.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.hamcrest.Matcher;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.compute.data.BlockUtils.toJavaObject;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class MvConcatTests extends AbstractScalarFunctionTestCase {
    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new MvConcat(source, args.get(0), args.get(1));
    }

    @Override
    protected TestCase getSimpleTestCase() {
        List<TypedData> typedData = List.of(
            new TypedData(List.of(new BytesRef("foo"), new BytesRef("bar"), new BytesRef("baz")), DataTypes.KEYWORD, "field"),
            new TypedData(new BytesRef(", "), DataTypes.KEYWORD, "delim")
        );
        return new TestCase(Source.EMPTY, typedData, resultsMatcher(typedData));
    }

    private Matcher<Object> resultsMatcher(List<TypedData> typedData) {
        List<?> field = (List<?>) typedData.get(0).data();
        BytesRef delim = (BytesRef) typedData.get(1).data();
        if (field == null || delim == null) {
            return nullValue();
        }
        return equalTo(
            new BytesRef(field.stream().map(v -> ((BytesRef) v).utf8ToString()).collect(Collectors.joining(delim.utf8ToString())))
        );
    }

    @Override
    protected Matcher<Object> resultMatcher(List<Object> data, DataType dataType) {
        List<?> field = (List<?>) data.get(0);
        BytesRef delim = (BytesRef) data.get(1);
        if (field == null || delim == null) {
            return nullValue();
        }
        return equalTo(
            new BytesRef(field.stream().map(v -> ((BytesRef) v).utf8ToString()).collect(Collectors.joining(delim.utf8ToString())))
        );
    }

    @Override
    protected String expectedEvaluatorSimpleToString() {
        return "MvConcat[field=Attribute[channel=0], delim=Attribute[channel=1]]";
    }

    @Override
    protected List<ArgumentSpec> argSpec() {
        return List.of(required(strings()), required(strings()));
    }

    @Override
    protected DataType expectedType(List<DataType> argTypes) {
        return DataTypes.KEYWORD;
    }

    public void testNull() {
        BytesRef foo = new BytesRef("foo");
        BytesRef bar = new BytesRef("bar");
        BytesRef delim = new BytesRef(";");
        Expression expression = buildFieldExpression(getSimpleTestCase());

        assertThat(toJavaObject(evaluator(expression).get().eval(row(Arrays.asList(Arrays.asList(foo, bar), null))), 0), nullValue());
        assertThat(toJavaObject(evaluator(expression).get().eval(row(Arrays.asList(foo, null))), 0), nullValue());
        assertThat(toJavaObject(evaluator(expression).get().eval(row(Arrays.asList(null, null))), 0), nullValue());

        assertThat(
            toJavaObject(evaluator(expression).get().eval(row(Arrays.asList(Arrays.asList(foo, bar), Arrays.asList(delim, bar)))), 0),
            nullValue()
        );
        assertThat(toJavaObject(evaluator(expression).get().eval(row(Arrays.asList(foo, Arrays.asList(delim, bar)))), 0), nullValue());
        assertThat(toJavaObject(evaluator(expression).get().eval(row(Arrays.asList(null, Arrays.asList(delim, bar)))), 0), nullValue());

        assertThat(toJavaObject(evaluator(expression).get().eval(row(Arrays.asList(null, delim))), 0), nullValue());
    }
}
