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

public class DelayedQueryableExpressionTests extends ESTestCase {
    public void testConstantLong() {
        long n = randomLong();
        QueryableExpression e = DelayedQueryableExpression.constant(n).undelay(null, null);
        assertThat(e, equalTo(LongQueryableExpression.constant(n)));
    }

    public void testConstantOther() {
        QueryableExpression e = DelayedQueryableExpression.constant(randomAlphaOfLength(5)).undelay(null, null);
        assertThat(e, equalTo(QueryableExpression.UNQUERYABLE));
    }

    public void testFieldLong() {
        QueryableExpression f = mock(QueryableExpression.class);
        QueryableExpression e = DelayedQueryableExpression.field(randomAlphaOfLength(5)).undelay(k -> f, null);
        assertThat(e, sameInstance(f));
    }

    public void testFieldUnqueryable() {
        QueryableExpression e = DelayedQueryableExpression.field(randomAlphaOfLength(5))
            .undelay(k -> QueryableExpression.UNQUERYABLE, null);
        assertThat(e, equalTo(QueryableExpression.UNQUERYABLE));
    }

    public void testParamLong() {
        String key = randomAlphaOfLength(5);
        long n = randomLong();
        QueryableExpression e = DelayedQueryableExpression.param(key).undelay(null, Map.of(key, n));
        assertThat(e, equalTo(LongQueryableExpression.constant(n)));
    }

    public void testParamOther() {
        String key = randomAlphaOfLength(5);
        QueryableExpression e = DelayedQueryableExpression.param(key).undelay(null, Map.of(key, randomAlphaOfLength(5)));
        assertThat(e, equalTo(QueryableExpression.UNQUERYABLE));
    }

    public void testAddConstants() {
        QueryableExpression e = DelayedQueryableExpression.add(
            DelayedQueryableExpression.constant(1L),
            DelayedQueryableExpression.constant(2L)
        ).undelay(null, null);
        assertThat(e, equalTo(LongQueryableExpression.constant(3)));
    }

    public void testAddFieldAndConstant() {
        QueryableExpression field = LongQueryableExpression.field("f", mock(LongQueryableExpression.LongQueries.class));
        QueryableExpression e = DelayedQueryableExpression.add(
            DelayedQueryableExpression.constant(1L),
            DelayedQueryableExpression.constant(2L)
        ).undelay(null, null);
        assertThat(e, equalTo(LongQueryableExpression.constant(3)));
    }
}
