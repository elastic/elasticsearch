/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.expression.Expression.TypeResolution;
import org.elasticsearch.xpack.sql.expression.literal.IntervalYearMonth;
import org.elasticsearch.xpack.sql.expression.predicate.operator.arithmetic.Mul;
import org.elasticsearch.xpack.sql.type.DataType;

import java.time.Period;

import static org.elasticsearch.xpack.sql.tree.Source.EMPTY;

public class TyperResolutionTests extends ESTestCase {

    public void testMulNumeric() {
        Mul m = new Mul(EMPTY, L(1), L(2));
        assertEquals(TypeResolution.TYPE_RESOLVED, m.typeResolved());
    }

    public void testMulIntervalAndNumber() {
        Mul m = new Mul(EMPTY, L(1), randomYearInterval());
        assertEquals(TypeResolution.TYPE_RESOLVED, m.typeResolved());
    }

    public void testMulNumberAndInterval() {
        Mul m = new Mul(EMPTY, randomYearInterval(), L(1));
        assertEquals(TypeResolution.TYPE_RESOLVED, m.typeResolved());
    }

    public void testMulTypeResolution() throws Exception {
        Mul mul = new Mul(EMPTY, randomYearInterval(), randomYearInterval());
        assertTrue(mul.typeResolved().unresolved());
    }

    private static Literal randomYearInterval() {
        return Literal.of(EMPTY, new IntervalYearMonth(Period.ofMonths(randomInt(123)), DataType.INTERVAL_YEAR_TO_MONTH));
    }

    private static Literal L(Object value) {
        return Literal.of(EMPTY, value);
    }
}
