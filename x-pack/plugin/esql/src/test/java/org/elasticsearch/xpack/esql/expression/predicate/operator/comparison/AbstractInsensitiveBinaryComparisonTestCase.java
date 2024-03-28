/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.operator.comparison;

import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.predicate.operator.AbstractBinaryOperatorTestCase;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.expression.predicate.BinaryOperator;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.hamcrest.Matcher;

import java.util.List;

import static org.elasticsearch.xpack.esql.type.EsqlDataTypes.isSpatial;
import static org.hamcrest.Matchers.equalTo;

public abstract class AbstractInsensitiveBinaryComparisonTestCase extends AbstractBinaryOperatorTestCase {

    @SuppressWarnings({ "rawtypes", "unchecked" })
    protected final Matcher<Object> resultMatcher(List<Object> data, DataType dataType) {
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

    @Override
    protected Matcher<Object> resultsMatcher(List<TestCaseSupplier.TypedData> typedData) {
        Number lhs = (Number) typedData.get(0).data();
        Number rhs = (Number) typedData.get(1).data();
        if (typedData.stream().anyMatch(t -> t.type().equals(DataTypes.DOUBLE))) {
            return equalTo(resultMatcher(lhs.doubleValue(), rhs.doubleValue()));
        }
        if (typedData.stream().anyMatch(t -> t.type().equals(DataTypes.UNSIGNED_LONG))) {
            // TODO: Is this correct behavior for unsigned long?
            return resultMatcher(lhs.longValue(), rhs.longValue());
        }
        if (typedData.stream().anyMatch(t -> t.type().equals(DataTypes.LONG))) {
            return resultMatcher(lhs.longValue(), rhs.longValue());
        }
        if (typedData.stream().anyMatch(t -> t.type().equals(DataTypes.INTEGER))) {
            return resultMatcher(lhs.intValue(), rhs.intValue());
        }
        throw new UnsupportedOperationException();
    }

    protected abstract <T extends Comparable<T>> Matcher<Object> resultMatcher(T lhs, T rhs);

    protected abstract boolean isEquality();

    @Override
    protected final boolean supportsType(DataType type) {
        // Boolean and Spatial types do not support inequality operators
        if (type == DataTypes.BOOLEAN || isSpatial(type)) {
            return isEquality();
        }
        return EsqlDataTypes.isRepresentable(type);
    }

    @Override
    protected boolean supportsTypes(DataType lhsType, DataType rhsType) {
        return super.supportsTypes(lhsType, rhsType) && (lhsType == rhsType || lhsType.isNumeric() && rhsType.isNumeric());
    }

    @Override
    protected void validateType(BinaryOperator<?, ?, ?, ?> op, DataType lhsType, DataType rhsType) {
        assertTrue(op.typeResolved().resolved());
        assertThat(op.dataType(), equalTo(DataTypes.BOOLEAN));
    }
}
