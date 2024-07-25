/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic;

import org.elasticsearch.xpack.esql.core.expression.predicate.BinaryOperator;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.hamcrest.Matcher;

import java.time.Duration;
import java.time.Period;
import java.time.temporal.TemporalAmount;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.xpack.esql.core.type.DataType.isDateTime;
import static org.elasticsearch.xpack.esql.core.type.DataType.isNull;
import static org.elasticsearch.xpack.esql.core.type.DataType.isNullOrTemporalAmount;
import static org.elasticsearch.xpack.esql.core.type.DataType.isTemporalAmount;
import static org.hamcrest.Matchers.equalTo;

public abstract class AbstractDateTimeArithmeticTestCase extends AbstractArithmeticTestCase {

    @Override
    protected Matcher<Object> resultMatcher(List<Object> data, DataType dataType) {
        Object lhs = data.get(0);
        Object rhs = data.get(1);
        if (lhs instanceof TemporalAmount || rhs instanceof TemporalAmount) {
            Object expectedValue;
            if (lhs instanceof TemporalAmount && rhs instanceof TemporalAmount) {
                assertThat("temporal amounts of different kinds", lhs.getClass(), equalTo(rhs.getClass()));
                if (lhs instanceof Period) {
                    expectedValue = expectedValue((Period) lhs, (Period) rhs);
                } else {
                    expectedValue = expectedValue((Duration) lhs, (Duration) rhs);
                }
            } else if (lhs instanceof TemporalAmount lhsTemporal) {
                expectedValue = expectedValue((long) rhs, lhsTemporal);
            } else { // rhs instanceof TemporalAmount
                expectedValue = expectedValue((long) lhs, (TemporalAmount) rhs);
            }
            return equalTo(expectedValue);
        }
        return super.resultMatcher(data, dataType);
    }

    protected abstract long expectedValue(long datetime, TemporalAmount temporalAmount);

    protected abstract Period expectedValue(Period lhs, Period rhs);

    protected abstract Duration expectedValue(Duration lhs, Duration rhs);

    @Override
    protected final boolean supportsType(DataType type) {
        return DataType.isDateTimeOrTemporal(type) || super.supportsType(type);
    }

    @Override
    protected void validateType(BinaryOperator<?, ?, ?, ?> op, DataType lhsType, DataType rhsType) {
        if (isDateTime(lhsType) || isDateTime(rhsType)) {
            String failureMessage = null;
            if (isDateTime(lhsType) && isDateTime(rhsType)
                || isNullOrTemporalAmount(lhsType) == false && isNullOrTemporalAmount(rhsType) == false) {
                failureMessage = String.format(
                    Locale.ROOT,
                    "[%s] has arguments with incompatible types [%s] and [%s]",
                    op.symbol(),
                    lhsType,
                    rhsType
                );
            } else if (op instanceof Sub && isDateTime(rhsType)) {
                failureMessage = String.format(
                    Locale.ROOT,
                    "[%s] arguments are in unsupported order: cannot subtract a [DATETIME] value [%s] from a [%s] amount [%s]",
                    op.symbol(),
                    op.right().sourceText(),
                    lhsType,
                    op.left().sourceText()
                );
            }
            assertTypeResolution(failureMessage, op, lhsType, rhsType);
        } else if (isTemporalAmount(lhsType) || isTemporalAmount(rhsType)) {
            String failureMessage = isNull(lhsType) || isNull(rhsType) || lhsType == rhsType
                ? null
                : String.format(Locale.ROOT, "[%s] has arguments with incompatible types [%s] and [%s]", op.symbol(), lhsType, rhsType);
            assertTypeResolution(failureMessage, op, lhsType, rhsType);
        } else {
            super.validateType(op, lhsType, rhsType);
        }
    }

    private void assertTypeResolution(String failureMessage, BinaryOperator<?, ?, ?, ?> op, DataType lhsType, DataType rhsType) {
        if (failureMessage != null) {
            assertFalse(op.toString(), op.typeResolved().resolved());
            assertThat(op.toString(), op.typeResolved().message(), equalTo(failureMessage));
        } else {
            assertTrue(op.toString(), op.typeResolved().resolved());
            assertThat(op.toString(), op.dataType(), equalTo(expectedType(lhsType, rhsType)));
        }
    }

    @Override
    protected DataType expectedType(DataType lhsType, DataType rhsType) {
        if (isDateTime(lhsType) || isDateTime(rhsType)) {
            return DataType.DATETIME;
        } else if (isNullOrTemporalAmount(lhsType) || isNullOrTemporalAmount(rhsType)) {
            if (isNull(lhsType)) {
                return rhsType;
            } else if (isNull(rhsType)) {
                return lhsType;
            } else if (lhsType == rhsType) {
                return lhsType;
            } // else: UnsupportedOperationException
        }
        return super.expectedType(lhsType, rhsType);
    }
}
