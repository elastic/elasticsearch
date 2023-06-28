/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.conditional;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.xpack.esql.expression.function.scalar.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.hamcrest.Matcher;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class IsNotNullTests extends AbstractScalarFunctionTestCase {
    @Override
    protected List<Object> simpleData() {
        return List.of(new BytesRef("cat"));
    }

    @Override
    protected Expression expressionForSimpleData() {
        return new IsNotNull(Source.EMPTY, field("exp", DataTypes.KEYWORD));
    }

    @Override
    protected DataType expectedType(List<DataType> argTypes) {
        return DataTypes.BOOLEAN;
    }

    @Override
    protected Matcher<Object> resultMatcher(List<Object> data) {
        return equalTo(true);
    }

    @Override
    protected void assertSimpleWithNulls(List<Object> data, Block value, int nullBlock) {
        assertFalse(((BooleanBlock) value).asVector().getBoolean(0));
    }

    @Override
    protected String expectedEvaluatorSimpleToString() {
        return "IsNotNullEvaluator[field=Attribute[channel=0]]";
    }

    @Override
    protected Expression constantFoldable(List<Object> data) {
        return new IsNotNull(Source.EMPTY, new Literal(Source.EMPTY, data.get(0), DataTypes.KEYWORD));
    }

    @Override
    protected List<ArgumentSpec> argSpec() {
        return List.of(required(EsqlDataTypes.types().toArray(DataType[]::new)));
    }

    @Override
    protected Expression build(Source source, List<Literal> args) {
        return new IsNotNull(Source.EMPTY, args.get(0));
    }

    public void testAllTypes() {
        for (DataType type : EsqlDataTypes.types()) {
            if (DataTypes.isPrimitive(type) == false) {
                continue;
            }
            Literal lit = randomLiteral(EsqlDataTypes.widenSmallNumericTypes(type));
            assertThat(new IsNotNull(Source.EMPTY, lit).fold(), equalTo(lit.value() != null));
            assertThat(new IsNotNull(Source.EMPTY, new Literal(Source.EMPTY, null, type)).fold(), equalTo(false));
        }
    }
}
