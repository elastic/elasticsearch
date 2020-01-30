/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ql.expression;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.TestUtils;
import org.elasticsearch.xpack.ql.expression.Expression.TypeResolution;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.Mul;

import static org.elasticsearch.xpack.ql.tree.Source.EMPTY;

public class TyperResolutionTests extends ESTestCase {

    public void testMulNumeric() {
        Mul m = new Mul(EMPTY, L(1), L(2));
        assertEquals(TypeResolution.TYPE_RESOLVED, m.typeResolved());
    }

    private static Literal L(Object value) {
        return TestUtils.of(EMPTY, value);
    }
}
