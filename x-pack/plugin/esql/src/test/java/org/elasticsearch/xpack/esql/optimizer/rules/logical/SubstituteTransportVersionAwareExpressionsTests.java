/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.expression.function.aggregate.SummationMode;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getFieldAttribute;
import static org.elasticsearch.xpack.esql.action.EsqlExecutionInfo.EXECUTION_PROFILE_FORMAT_VERSION;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

public class SubstituteTransportVersionAwareExpressionsTests extends ESTestCase {
    public void testSumReplacedWithOldVersion() {
        Expression field = getFieldAttribute("f", DataType.LONG);
        Sum sum = new Sum(EMPTY, field);
        Expression result = SubstituteTransportVersionAwareExpressions.rule(sum, EXECUTION_PROFILE_FORMAT_VERSION);
        assertThat(result, instanceOf(Sum.class));
        assertThat(((Sum) result).useOverflowingLongSupplier(), is(true));
        assertThat(result, not(sameInstance(sum)));
    }

    public void testSumNotReplacedWithCurrentVersion() {
        Expression field = getFieldAttribute("f", DataType.LONG);
        Sum sum = new Sum(EMPTY, field);
        Expression result = SubstituteTransportVersionAwareExpressions.rule(sum, TransportVersion.current());
        assertThat(result, sameInstance(sum));
    }

    /**
     * Checks that if an overflowing sum (Used for old transport versions) receives an old transport version, it won't be changed.
     * <p>
     *     This tests idempotence.
     * </p>
     */
    public void testSumAlreadyOverflowingWithOldVersion() {
        Expression field = getFieldAttribute("f", DataType.LONG);
        Sum sum = new Sum(EMPTY, field, Literal.TRUE, AggregateFunction.NO_WINDOW, SummationMode.COMPENSATED_LITERAL, true);
        Expression result = SubstituteTransportVersionAwareExpressions.rule(sum, EXECUTION_PROFILE_FORMAT_VERSION);
        assertThat(result, sameInstance(sum));
    }

    /**
     * Checks that if an overflowing sum (Used for old transport versions) receives a new transport version, it won't be changed again.
     * <p>
     *     <b>This shouldn't happen in practice</b>, unless Sum is purposely initialized with the overflowing=true.
     *     But we're testing it anyway to ensure the behavior doesn't change.
     * </p>
     */
    public void testSumAlreadyOverflowingWithNewVersionKeptOverflowing() {
        Expression field = getFieldAttribute("f", DataType.LONG);
        Sum sum = new Sum(EMPTY, field, Literal.TRUE, AggregateFunction.NO_WINDOW, SummationMode.COMPENSATED_LITERAL, true);
        Expression result = SubstituteTransportVersionAwareExpressions.rule(sum, TransportVersion.current());
        assertThat(result, sameInstance(sum));
    }

    public void testSumDoubleFieldWithOldVersion() {
        Expression field = getFieldAttribute("f", DataType.DOUBLE);
        Sum sum = new Sum(EMPTY, field);
        Expression result = SubstituteTransportVersionAwareExpressions.rule(sum, EXECUTION_PROFILE_FORMAT_VERSION);
        assertThat(result, instanceOf(Sum.class));
        assertThat(((Sum) result).useOverflowingLongSupplier(), is(true));
    }

    public void testNonTransportVersionAwareUnchanged() {
        Expression field = getFieldAttribute("f", DataType.LONG);
        Expression result = SubstituteTransportVersionAwareExpressions.rule(field, EXECUTION_PROFILE_FORMAT_VERSION);
        assertThat(result, sameInstance(field));
    }
}
