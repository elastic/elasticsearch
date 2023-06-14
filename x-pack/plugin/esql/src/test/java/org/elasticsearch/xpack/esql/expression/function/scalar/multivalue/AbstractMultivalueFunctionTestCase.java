/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.xpack.esql.expression.function.scalar.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.hamcrest.Matcher;

import java.util.List;

import static java.util.Collections.singletonList;
import static org.elasticsearch.compute.data.BlockUtils.toJavaObject;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public abstract class AbstractMultivalueFunctionTestCase extends AbstractScalarFunctionTestCase {
    protected abstract Expression build(Source source, Expression field);

    protected abstract Matcher<Object> resultMatcherForInput(List<?> input);

    protected abstract DataType[] supportedTypes();

    /**
     * Matcher for single valued fields.
     */
    protected Matcher<Object> singleValueMatcher(Object o) {
        return equalTo(o);
    }

    @Override
    protected final List<AbstractScalarFunctionTestCase.ArgumentSpec> argSpec() {
        return List.of(required(supportedTypes()));
    }

    @Override
    protected final List<Object> simpleData() {
        return dataForPosition(supportedTypes()[0]);
    }

    @Override
    protected final Expression expressionForSimpleData() {
        return build(Source.EMPTY, field("f", supportedTypes()[0]));
    }

    @Override
    protected DataType expectedType(List<DataType> argTypes) {
        return argTypes.get(0);
    }

    @Override
    protected final Matcher<Object> resultMatcher(List<Object> data) {
        return resultMatcherForInput((List<?>) data.get(0));
    }

    @Override
    protected final Expression build(Source source, List<Literal> args) {
        return build(source, args.get(0));
    }

    @Override
    protected final Expression constantFoldable(List<Object> data) {
        return build(Source.EMPTY, new Literal(Source.EMPTY, data.get(0), DataTypes.fromJava(((List<?>) data.get(0)).get(0))));
    }

    public final void testVector() {
        for (DataType type : supportedTypes()) {
            List<List<Object>> data = randomList(1, 200, () -> singletonList(randomLiteral(type).value()));
            Expression expression = build(Source.EMPTY, field("f", type));
            Block result = evaluator(expression).get().eval(new Page(BlockUtils.fromList(data)));
            assertThat(result.asVector(), type == DataTypes.NULL ? nullValue() : notNullValue());
            for (int p = 0; p < data.size(); p++) {
                assertThat(toJavaObject(result, p), singleValueMatcher(data.get(p).get(0)));
            }
        }
    }

    public final void testBlock() {
        for (boolean insertNulls : new boolean[] { false, true }) {
            for (DataType type : supportedTypes()) {
                List<List<Object>> data = randomList(
                    1,
                    200,
                    () -> type == DataTypes.NULL || (insertNulls && rarely()) ? singletonList(null) : dataForPosition(type)
                );
                Expression expression = build(Source.EMPTY, field("f", type));
                Block result = evaluator(expression).get().eval(new Page(BlockUtils.fromList(data)));
                for (int p = 0; p < data.size(); p++) {
                    if (data.get(p).get(0) == null) {
                        assertTrue(type.toString(), result.isNull(p));
                    } else {
                        assertFalse(type.toString(), result.isNull(p));
                        assertThat(type.toString(), toJavaObject(result, p), resultMatcherForInput((List<?>) data.get(p).get(0)));
                    }
                }
            }
        }
    }

    public final void testFoldSingleValue() {
        for (DataType type : supportedTypes()) {
            Literal lit = randomLiteral(type);
            Expression expression = build(Source.EMPTY, lit);
            assertTrue(expression.foldable());
            assertThat(expression.fold(), singleValueMatcher(lit.value()));
        }
    }

    public final void testFoldManyValues() {
        for (DataType type : supportedTypes()) {
            List<Object> data = type == DataTypes.NULL ? null : randomList(1, 100, () -> randomLiteral(type).value());
            Expression expression = build(Source.EMPTY, new Literal(Source.EMPTY, data, type));
            assertTrue(expression.foldable());
            assertThat(expression.fold(), resultMatcherForInput(data));
        }
    }

    private List<Object> dataForPosition(DataType type) {
        return List.of(randomList(1, 100, () -> randomLiteral(type).value()));
    }
}
