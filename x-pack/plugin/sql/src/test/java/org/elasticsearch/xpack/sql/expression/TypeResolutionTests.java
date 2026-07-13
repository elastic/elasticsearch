/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.expression;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.expression.Expression.TypeResolution;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.sql.SqlTestUtils;
import org.elasticsearch.xpack.sql.expression.literal.interval.IntervalYearMonth;
import org.elasticsearch.xpack.sql.expression.predicate.operator.arithmetic.Mul;

import java.time.Period;

import static org.elasticsearch.xpack.ql.tree.Source.EMPTY;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.INTERVAL_YEAR_TO_MONTH;

public class TypeResolutionTests extends ESTestCase {

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
        return L(new IntervalYearMonth(Period.ofMonths(randomInt(123)), INTERVAL_YEAR_TO_MONTH));
    }

    private static Literal L(Object value) {
        return SqlTestUtils.literal(value);
    }
}
