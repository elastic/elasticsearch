/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Expression.TypeResolution;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mul;

import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;

public class TyperResolutionTests extends ESTestCase {

    public void testMulNumeric() {
        Mul m = new Mul(EMPTY, L(1), L(2));
        assertEquals(TypeResolution.TYPE_RESOLVED, m.typeResolved());
    }

    private static Literal L(Object value) {
        return EsqlTestUtils.of(EMPTY, value);
    }
}
