/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.multivalue;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.hamcrest.Matcher;

import java.util.List;

import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

abstract class AbstractMultivalueFunctionTestCase extends AbstractFunctionTestCase {
    protected abstract DataType[] supportedTypes();

    protected abstract Expression build(Source source, Expression field);

    protected abstract Matcher<Object> resultMatcherForInput(List<?> input);

    @Override
    protected final List<Object> simpleData() {
        return dataForPosition(supportedTypes()[0]);
    }

    @Override
    protected final Expression expressionForSimpleData() {
        return build(Source.EMPTY, field("f", supportedTypes()[0]));
    }

    @Override
    protected final DataType expressionForSimpleDataType() {
        return supportedTypes()[0];
    }

    @Override
    protected final Matcher<Object> resultMatcher(List<Object> data) {
        return resultMatcherForInput((List<?>) data.get(0));
    }

    @Override
    protected final Expression build(Source source, List<Literal> args) {
        return build(source, args.get(0));
    }

    // TODO once we have explicit array types we should assert that non-arrays are noops

    @Override
    protected final Expression constantFoldable(List<Object> data) {
        return build(Source.EMPTY, new Literal(Source.EMPTY, data.get(0), DataTypes.fromJava(((List<?>) data.get(0)).get(0))));
    }

    public void testVector() {
        for (DataType type : supportedTypes()) {
            List<List<Object>> data = randomList(1, 200, () -> List.of(randomLiteral(type).value()));
            Expression expression = expressionForSimpleData();
            Block result = evaluator(expression).get().eval(new Page(BlockUtils.fromList(data)));
            assertThat(result.asVector(), notNullValue());
            for (int p = 0; p < data.size(); p++) {
                assertThat(valueAt(result, p), equalTo(data.get(p).get(0)));
            }
        }
    }

    public void testBlock() {
        for (boolean insertNulls : new boolean[] { false, true }) {
            for (DataType type : supportedTypes()) {
                List<List<Object>> data = randomList(1, 200, () -> insertNulls && rarely() ? singletonList(null) : dataForPosition(type));
                Expression expression = expressionForSimpleData();
                Block result = evaluator(expression).get().eval(new Page(BlockUtils.fromList(data)));
                for (int p = 0; p < data.size(); p++) {
                    if (data.get(p).get(0) == null) {
                        assertTrue(result.isNull(p));
                    } else {
                        assertThat(valueAt(result, p), resultMatcherForInput((List<?>) data.get(p).get(0)));
                    }
                }
            }
        }
    }

    private List<Object> dataForPosition(DataType type) {
        return List.of(randomList(1, 100, () -> randomLiteral(type).value()));
    }
}
