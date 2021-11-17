/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.queryableexpression;

import org.elasticsearch.test.ESTestCase;

import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;

public class QueryableExpressionBuilderTests extends ESTestCase {
    public void testConstantLong() {
        long n = randomLong();
        QueryableExpression e = QueryableExpressionBuilder.constant(n).build(null, null);
        assertThat(e, equalTo(new AbstractLongQueryableExpression.Constant(n)));
    }

    public void testConstantOther() {
        QueryableExpression e = QueryableExpressionBuilder.constant(randomAlphaOfLength(5)).build(null, null);
        assertThat(e, equalTo(QueryableExpression.UNQUERYABLE));
    }

    public void testFieldLong() {
        String key = randomAlphaOfLength(5);
        QueryableExpression f = mock(QueryableExpression.class);
        QueryableExpression e = QueryableExpressionBuilder.field(key).build(Map.of(key, f)::get, null);
        assertThat(e, sameInstance(f));
    }

    public void testFieldUnqueryable() {
        String key = randomAlphaOfLength(5);
        QueryableExpression e = QueryableExpressionBuilder.field(key).build(Map.of(key, QueryableExpression.UNQUERYABLE)::get, null);
        assertThat(e, equalTo(QueryableExpression.UNQUERYABLE));
    }

    public void testParamLong() {
        String key = randomAlphaOfLength(5);
        long n = randomLong();
        QueryableExpression e = QueryableExpressionBuilder.param(key).build(null, Map.of(key, n)::get);
        assertThat(e, equalTo(new AbstractLongQueryableExpression.Constant(n)));
    }

    public void testParamOther() {
        String key = randomAlphaOfLength(5);
        QueryableExpression e = QueryableExpressionBuilder.param(key).build(null, Map.of(key, randomAlphaOfLength(5))::get);
        assertThat(e, equalTo(QueryableExpression.UNQUERYABLE));
    }

    public void testAddConstants() {
        long lhs = randomLong();
        long rhs = randomLong();
        QueryableExpression e = QueryableExpressionBuilder.add(
            QueryableExpressionBuilder.constant(lhs),
            QueryableExpressionBuilder.constant(rhs)
        ).build(null, null);
        assertThat(e, equalTo(new AbstractLongQueryableExpression.Constant(lhs + rhs)));
    }
}
