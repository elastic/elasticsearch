/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.operator.comparison;

import org.elasticsearch.xpack.esql.analysis.Verifier;
import org.elasticsearch.xpack.esql.expression.predicate.operator.AbstractBinaryOperatorTestCase;
import org.elasticsearch.xpack.ql.common.Failure;
import org.elasticsearch.xpack.ql.expression.predicate.BinaryOperator;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.BinaryComparison;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.hamcrest.Matcher;

import java.util.List;
import java.util.Locale;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public abstract class AbstractBinaryComparisonTestCase extends AbstractBinaryOperatorTestCase {
    @SuppressWarnings({ "rawtypes", "unchecked" })
    protected final Matcher<Object> resultMatcher(List<Object> data) {
        Comparable lhs = (Comparable) data.get(0);
        Comparable rhs = (Comparable) data.get(1);
        if (lhs instanceof Double || rhs instanceof Double) {
            return (Matcher<Object>) (Matcher<?>) resultMatcher(((Number) lhs).doubleValue(), ((Number) rhs).doubleValue());
        }
        if (lhs instanceof Long || rhs instanceof Long) {
            return (Matcher<Object>) (Matcher<?>) resultMatcher(((Number) lhs).longValue(), ((Number) rhs).longValue());
        }
        if (lhs instanceof Integer || rhs instanceof Integer) {
            return (Matcher<Object>) (Matcher<?>) resultMatcher(((Number) lhs).intValue(), ((Number) rhs).intValue());
        }
        return (Matcher<Object>) (Matcher<?>) resultMatcher(lhs, rhs);
    }

    protected abstract <T extends Comparable<T>> Matcher<Boolean> resultMatcher(T lhs, T rhs);

    @Override
    protected final DataType expressionForSimpleDataType() {
        return DataTypes.BOOLEAN;
    }

    protected abstract boolean isEquality();

    @Override
    protected final boolean supportsType(DataType type) {
        if (type == DataTypes.BOOLEAN) {
            return isEquality();
        }
        return true;
    }

    @Override
    protected final void validateType(BinaryOperator<?, ?, ?, ?> op, DataType lhsType, DataType rhsType) {
        assertTrue(op.typeResolved().resolved());
        assertThat(op.dataType(), equalTo(DataTypes.BOOLEAN));
        Failure f = Verifier.validateBinaryComparison((BinaryComparison) op);
        if (isEquality() == false && lhsType == DataTypes.BOOLEAN) {
            assertThat(op.toString(), f, not(nullValue()));
            assertThat(
                op.toString(),
                f.message(),
                equalTo(
                    String.format(
                        Locale.ROOT,
                        "first argument of [%s %s] must be [numeric, keyword, ip, datetime or version], found value [] type [%s]",
                        lhsType.typeName(),
                        rhsType.typeName(),
                        lhsType.typeName()
                    )
                )
            );
            return;
        }
        if (lhsType == rhsType || lhsType.isNumeric() && rhsType.isNumeric()) {
            assertThat(op.toString(), f, nullValue());
            return;
        }
        assertThat(op.toString(), f, not(nullValue()));
        assertThat(
            op.toString(),
            f.message(),
            equalTo(
                String.format(
                    Locale.ROOT,
                    "first argument of [%s %s] is [%s] so second argument must also be [%s] but was [%s]",
                    lhsType.typeName(),
                    rhsType.typeName(),
                    lhsType.isNumeric() ? "numeric" : lhsType.typeName(),
                    lhsType.isNumeric() ? "numeric" : lhsType.typeName(),
                    rhsType.typeName()
                )
            )
        );
    }
}
