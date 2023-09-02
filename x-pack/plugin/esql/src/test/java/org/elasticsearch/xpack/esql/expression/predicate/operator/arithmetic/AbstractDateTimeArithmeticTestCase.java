/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic;

import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.expression.predicate.BinaryOperator;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.hamcrest.Matcher;

import java.time.temporal.TemporalAmount;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.xpack.esql.type.EsqlDataTypes.isDateTimeOrTemporal;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypes.isTemporalAmount;
import static org.elasticsearch.xpack.ql.type.DataTypes.isDateTime;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.oneOf;

public abstract class AbstractDateTimeArithmeticTestCase extends AbstractArithmeticTestCase {

    @Override
    protected Matcher<Object> resultMatcher(List<Object> data, DataType dataType) {
        Object lhs = data.get(0);
        Object rhs = data.get(1);
        if (lhs instanceof TemporalAmount || rhs instanceof TemporalAmount) {
            TemporalAmount temporal = lhs instanceof TemporalAmount leftTemporal ? leftTemporal : (TemporalAmount) rhs;
            long datetime = temporal == lhs ? (Long) rhs : (Long) lhs;
            return equalTo(expectedValue(datetime, temporal));
        }
        return super.resultMatcher(data, dataType);
    }

    protected abstract long expectedValue(long datetime, TemporalAmount temporalAmount);

    @Override
    protected final boolean supportsType(DataType type) {
        return EsqlDataTypes.isDateTimeOrTemporal(type) || super.supportsType(type);
    }

    @Override
    protected void validateType(BinaryOperator<?, ?, ?, ?> op, DataType lhsType, DataType rhsType) {
        if (isDateTime(lhsType) && isTemporalAmount(rhsType) || isTemporalAmount(lhsType) && isDateTime(rhsType)) {
            assertTrue(op.toString(), op.typeResolved().resolved());
            assertTrue(op.toString(), isTemporalAmount(lhsType) || isTemporalAmount(rhsType));
            assertFalse(op.toString(), isTemporalAmount(lhsType) && isTemporalAmount(rhsType));
            assertThat(op.toString(), op.dataType(), equalTo(expectedType(lhsType, rhsType)));
            assertThat(op.toString(), op.getClass(), oneOf(Add.class, Sub.class));
        } else if (isDateTimeOrTemporal(lhsType) || isDateTimeOrTemporal(rhsType)) {
            assertFalse(op.toString(), op.typeResolved().resolved());
            assertThat(
                op.toString(),
                op.typeResolved().message(),
                equalTo(
                    String.format(Locale.ROOT, "[%s] has arguments with incompatible types [%s] and [%s]", op.symbol(), lhsType, rhsType)
                )
            );
        } else {
            super.validateType(op, lhsType, rhsType);
        }
    }

    @Override
    protected DataType expectedType(DataType lhsType, DataType rhsType) {
        return isDateTimeOrTemporal(lhsType) ? DataTypes.DATETIME : super.expectedType(lhsType, rhsType);
    }
}
