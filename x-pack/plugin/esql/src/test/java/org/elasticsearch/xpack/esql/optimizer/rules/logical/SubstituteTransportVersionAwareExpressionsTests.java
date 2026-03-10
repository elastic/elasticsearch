/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.expression.function.aggregate.SummationMode;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getFieldAttribute;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

public class SubstituteTransportVersionAwareExpressionsTests extends ESTestCase {
    private static final TransportVersion ESQL_SUM_LONG_OVERFLOW_FIX = TransportVersion.fromName("esql_sum_long_overflow_fix");

    public void testSumReplacedWithOldVersion() {
        Expression field = getFieldAttribute("f", DataType.LONG);
        Sum sum = new Sum(EMPTY, field);
        TransportVersion oldVersion = TransportVersionUtils.randomVersionNotSupporting(ESQL_SUM_LONG_OVERFLOW_FIX);
        Expression result = SubstituteTransportVersionAwareExpressions.rule(sum, oldVersion);
        assertThat(result, instanceOf(Sum.class));
        assertThat(((Sum) result).useOverflowingLongSupplier(), is(true));
        assertThat(result, not(sameInstance(sum)));
    }

    public void testSumNotReplacedWithCurrentVersion() {
        Expression field = getFieldAttribute("f", DataType.LONG);
        Sum sum = new Sum(EMPTY, field);
        TransportVersion newVersion = TransportVersionUtils.randomVersionSupporting(ESQL_SUM_LONG_OVERFLOW_FIX);
        Expression result = SubstituteTransportVersionAwareExpressions.rule(sum, newVersion);
        assertThat(result, sameInstance(sum));
    }

    /**
     * Checks that if an overflowing sum (used for old transport versions) receives an old transport version, it won't be changed.
     * <p>
     *     This tests idempotence.
     * </p>
     */
    public void testSumAlreadyOverflowingWithOldVersion() {
        Expression field = getFieldAttribute("f", DataType.LONG);
        Sum sum = new Sum(EMPTY, field, Literal.TRUE, AggregateFunction.NO_WINDOW, SummationMode.COMPENSATED_LITERAL, Sum.OVERFLOWING_LONG);
        TransportVersion oldVersion = TransportVersionUtils.randomVersionNotSupporting(ESQL_SUM_LONG_OVERFLOW_FIX);
        Expression result = SubstituteTransportVersionAwareExpressions.rule(sum, oldVersion);
        assertThat(result, sameInstance(sum));
    }

    /**
     * Checks that if an overflowing sum (used for old transport versions) receives a new transport version, it won't be changed again.
     * <p>
     *     <b>This shouldn't happen in practice</b>, unless Sum is purposely initialized with the overflowing=true.
     *     But we're testing it anyway to ensure the behavior doesn't change.
     * </p>
     */
    public void testSumAlreadyOverflowingWithNewVersionKeptOverflowing() {
        Expression field = getFieldAttribute("f", DataType.LONG);
        Sum sum = new Sum(EMPTY, field, Literal.TRUE, AggregateFunction.NO_WINDOW, SummationMode.COMPENSATED_LITERAL, Sum.OVERFLOWING_LONG);
        TransportVersion newVersion = TransportVersionUtils.randomVersionSupporting(ESQL_SUM_LONG_OVERFLOW_FIX);
        Expression result = SubstituteTransportVersionAwareExpressions.rule(sum, newVersion);
        assertThat(result, sameInstance(sum));
    }

    public void testSumDoubleFieldWithOldVersion() {
        Expression field = getFieldAttribute("f", DataType.DOUBLE);
        Sum sum = new Sum(EMPTY, field);
        TransportVersion oldVersion = TransportVersionUtils.randomVersionNotSupporting(ESQL_SUM_LONG_OVERFLOW_FIX);
        Expression result = SubstituteTransportVersionAwareExpressions.rule(sum, oldVersion);
        assertThat(result, instanceOf(Sum.class));
        assertThat(((Sum) result).useOverflowingLongSupplier(), is(true));
    }

    public void testNonTransportVersionAwareUnchanged() {
        Expression field = getFieldAttribute("f", DataType.LONG);
        TransportVersion oldVersion = TransportVersionUtils.randomVersionNotSupporting(ESQL_SUM_LONG_OVERFLOW_FIX);
        Expression result = SubstituteTransportVersionAwareExpressions.rule(field, oldVersion);
        assertThat(result, sameInstance(field));
    }
}
