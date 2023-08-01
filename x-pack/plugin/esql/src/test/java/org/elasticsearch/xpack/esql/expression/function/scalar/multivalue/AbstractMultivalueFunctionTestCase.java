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

    protected abstract Matcher<Object> resultMatcherForInput(List<?> input, DataType dataType);

    protected abstract DataType[] supportedTypes();

    /**
     * Matcher for single valued fields.
     */
    private Matcher<Object> singleValueMatcher(Object o, DataType dataType) {
        return o == null ? nullValue() : resultMatcherForInput(List.of(o), dataType);
    }

    @Override
    protected final List<AbstractScalarFunctionTestCase.ArgumentSpec> argSpec() {
        return List.of(required(supportedTypes()));
    }

    @Override
    protected TestCase getSimpleTestCase() {
        List<Object> data = dataForPosition(supportedTypes()[0]);
        List<TypedData> typedData = List.of(new TypedData(data, supportedTypes()[0], "f"));
        return new TestCase(Source.EMPTY, typedData, resultsMatcher(typedData));
    }

    @Override
    protected DataType expectedType(List<DataType> argTypes) {
        return argTypes.get(0);
    }

    private Matcher<Object> resultsMatcher(List<TypedData> typedData) {
        return resultMatcherForInput((List<?>) typedData.get(0).data(), typedData.get(0).type());
    }

    @Override
    protected final Matcher<Object> resultMatcher(List<Object> data, DataType dataType) {
        return resultMatcherForInput((List<?>) data.get(0), dataType);
    }

    @Override
    protected final Expression build(Source source, List<Expression> args) {
        return build(source, args.get(0));
    }

    public final void testVector() {
        for (DataType type : supportedTypes()) {
            List<List<Object>> data = randomList(1, 200, () -> singletonList(randomLiteral(type).value()));
            Expression expression = build(Source.EMPTY, field("f", type));
            Block result = evaluator(expression).get().eval(new Page(BlockUtils.fromList(data)));
            assertThat(result.asVector(), type == DataTypes.NULL ? nullValue() : notNullValue());
            for (int p = 0; p < data.size(); p++) {
                assertThat(toJavaObject(result, p), singleValueMatcher(data.get(p).get(0), type));
            }
        }
    }

    public final void testBlock() {
        for (boolean insertNulls : new boolean[] { false, true }) {
            for (DataType type : supportedTypes()) {
                List<List<Object>> data = randomList(
                    1,
                    200,
                    () -> type == DataTypes.NULL || (insertNulls && rarely()) ? singletonList(null) : List.of(dataForPosition(type))
                );
                Expression expression = build(Source.EMPTY, field("f", type));
                try {
                    Block result = evaluator(expression).get().eval(new Page(BlockUtils.fromList(data)));
                    for (int p = 0; p < data.size(); p++) {
                        if (data.get(p).get(0) == null) {
                            assertTrue(type.toString(), result.isNull(p));
                        } else {
                            assertFalse(type.toString(), result.isNull(p));
                            assertThat(type.toString(), toJavaObject(result, p), resultMatcherForInput((List<?>) data.get(p).get(0), type));
                        }
                    }
                } catch (ArithmeticException ae) {
                    assertThat(ae.getMessage(), equalTo(type.typeName() + " overflow"));
                }
            }
        }
    }

    public final void testFoldSingleValue() {
        for (DataType type : supportedTypes()) {
            Literal lit = randomLiteral(type);
            Expression expression = build(Source.EMPTY, lit);
            assertTrue(expression.foldable());
            assertThat(expression.fold(), singleValueMatcher(lit.value(), type));
        }
    }

    public final void testFoldManyValues() {
        for (DataType type : supportedTypes()) {
            List<Object> data = type == DataTypes.NULL ? null : randomList(1, 100, () -> randomLiteral(type).value());
            Expression expression = build(Source.EMPTY, new Literal(Source.EMPTY, data, type));
            assertTrue(expression.foldable());
            try {
                assertThat(expression.fold(), resultMatcherForInput(data, type));
            } catch (ArithmeticException ae) {
                assertThat(ae.getMessage(), equalTo(type.typeName() + " overflow"));
            }
        }
    }

    private List<Object> dataForPosition(DataType type) {
        return randomList(1, 100, () -> randomLiteral(type).value());
    }
}
