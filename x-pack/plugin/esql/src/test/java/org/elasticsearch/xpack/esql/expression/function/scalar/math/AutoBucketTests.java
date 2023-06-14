/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.xpack.esql.expression.function.scalar.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.hamcrest.Matcher;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class AutoBucketTests extends AbstractScalarFunctionTestCase {
    @Override
    protected List<Object> simpleData() {
        return List.of(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2023-02-17T09:00:00.00Z"));
    }

    @Override
    protected Expression expressionForSimpleData() {
        return build(Source.EMPTY, field("arg", DataTypes.DATETIME));
    }

    private Expression build(Source source, Expression arg) {
        return new AutoBucket(
            source,
            arg,
            new Literal(Source.EMPTY, 50, DataTypes.INTEGER),
            new Literal(Source.EMPTY, new BytesRef("2023-02-01T00:00:00.00Z"), DataTypes.KEYWORD),
            new Literal(Source.EMPTY, new BytesRef("2023-03-01T00:00:00.00Z"), DataTypes.KEYWORD)
        );
    }

    @Override
    protected DataType expectedType(List<DataType> argTypes) {
        return argTypes.get(0);
    }

    @Override
    protected Matcher<Object> resultMatcher(List<Object> data) {
        long millis = ((Number) data.get(0)).longValue();
        return equalTo(Rounding.builder(Rounding.DateTimeUnit.DAY_OF_MONTH).build().prepareForUnknown().round(millis));
    }

    @Override
    protected String expectedEvaluatorSimpleToString() {
        return "DateTruncEvaluator[fieldVal=Attribute[channel=0], rounding=Rounding[DAY_OF_MONTH in Z][fixed to midnight]]";
    }

    @Override
    protected Expression constantFoldable(List<Object> data) {
        return build(Source.EMPTY, new Literal(Source.EMPTY, data.get(0), DataTypes.DATETIME));
    }

    @Override
    protected List<ArgumentSpec> argSpec() {
        return List.of(required(DataTypes.DATETIME));
    }

    @Override
    protected Expression build(Source source, List<Literal> args) {
        return build(source, args.get(0));
    }

    @Override
    protected Matcher<String> badTypeError(List<ArgumentSpec> spec, int badArgPosition, DataType badArgType) {
        return equalTo("first argument of [exp] must be [datetime], found value [arg0] type [" + badArgType.typeName() + "]");
    }
}
