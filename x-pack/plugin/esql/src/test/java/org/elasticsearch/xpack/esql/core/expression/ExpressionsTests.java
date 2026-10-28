/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.expression;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

public class ExpressionsTests extends ESTestCase {

    public void testCompareStableOrdersNullsFirst() {
        assertEquals(0, Expressions.compareStable(null, null));
        assertThat(Expressions.compareStable(null, Literal.TRUE), lessThan(0));
        assertThat(Expressions.compareStable(Literal.TRUE, null), greaterThan(0));
    }
}
