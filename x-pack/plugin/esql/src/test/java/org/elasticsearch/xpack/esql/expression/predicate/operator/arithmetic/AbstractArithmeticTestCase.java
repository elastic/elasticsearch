/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic;

import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.predicate.operator.AbstractBinaryOperatorTestCase;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.expression.predicate.BinaryOperator;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.hamcrest.Matcher;

import java.util.List;
import java.util.Locale;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public abstract class AbstractArithmeticTestCase extends AbstractBinaryOperatorTestCase {
    protected Matcher<Object> resultMatcher(List<Object> data, DataType dataType) {
        Number lhs = (Number) data.get(0);
        Number rhs = (Number) data.get(1);
        if (lhs instanceof Double || rhs instanceof Double) {
            return equalTo(expectedValue(lhs.doubleValue(), rhs.doubleValue()));
        }
        if (lhs instanceof Long || rhs instanceof Long) {
            if (dataType == DataTypes.UNSIGNED_LONG) {
                return equalTo(expectedUnsignedLongValue(lhs.longValue(), rhs.longValue()));
            }
            return equalTo(expectedValue(lhs.longValue(), rhs.longValue()));
        }
        if (lhs instanceof Integer || rhs instanceof Integer) {
            return equalTo(expectedValue(lhs.intValue(), rhs.intValue()));
        }
        throw new UnsupportedOperationException();
    }

    @Override
    protected Matcher<Object> resultsMatcher(List<TestCaseSupplier.TypedData> typedData) {
        Number lhs = (Number) typedData.get(0).data();
        Number rhs = (Number) typedData.get(1).data();
        if (typedData.stream().anyMatch(t -> t.type().equals(DataTypes.DOUBLE))) {
            return equalTo(expectedValue(lhs.doubleValue(), rhs.doubleValue()));
        }
        if (typedData.stream().anyMatch(t -> t.type().equals(DataTypes.UNSIGNED_LONG))) {
            return equalTo(expectedUnsignedLongValue(lhs.longValue(), rhs.longValue()));
        }
        if (typedData.stream().anyMatch(t -> t.type().equals(DataTypes.LONG))) {
            return equalTo(expectedValue(lhs.longValue(), rhs.longValue()));
        }
        if (typedData.stream().anyMatch(t -> t.type().equals(DataTypes.INTEGER))) {
            return equalTo(expectedValue(lhs.intValue(), rhs.intValue()));
        }
        throw new UnsupportedOperationException();
    }

    protected abstract double expectedValue(double lhs, double rhs);

    protected abstract int expectedValue(int lhs, int rhs);

    protected abstract long expectedValue(long lhs, long rhs);

    protected abstract long expectedUnsignedLongValue(long lhs, long rhs);

    @Override
    protected boolean supportsType(DataType type) {
        return type.isNumeric() && EsqlDataTypes.isRepresentable(type);
    }

    @Override
    protected void validateType(BinaryOperator<?, ?, ?, ?> op, DataType lhsType, DataType rhsType) {
        if (DataTypes.isNullOrNumeric(lhsType) && DataTypes.isNullOrNumeric(rhsType)) {
            assertTrue(op.toString(), op.typeResolved().resolved());
            assertThat(op.toString(), op.dataType(), equalTo(expectedType(lhsType, rhsType)));
            return;
        }
        assertFalse(op.toString(), op.typeResolved().resolved());
        if (op instanceof Mul) {
            // TODO why is mul different?
            assertThat(
                op.toString(),
                op.typeResolved().message(),
                equalTo(String.format(Locale.ROOT, "[*] has arguments with incompatible types [%s] and [%s]", lhsType, rhsType))
            );
            return;
        }
        assertThat(
            op.toString(),
            op.typeResolved().message(),
            containsString(
                String.format(Locale.ROOT, "argument of [%s %s] must be [numeric], found value []", lhsType.typeName(), rhsType.typeName())
            )
        );
    }

    protected DataType expectedType(DataType lhsType, DataType rhsType) {
        if (lhsType == DataTypes.DOUBLE || rhsType == DataTypes.DOUBLE) {
            return DataTypes.DOUBLE;
        }
        if (lhsType == DataTypes.UNSIGNED_LONG || rhsType == DataTypes.UNSIGNED_LONG) {
            assertThat(lhsType, is(DataTypes.UNSIGNED_LONG));
            assertThat(rhsType, is(DataTypes.UNSIGNED_LONG));
            return DataTypes.UNSIGNED_LONG;
        }
        if (lhsType == DataTypes.LONG || rhsType == DataTypes.LONG) {
            return DataTypes.LONG;
        }
        if (lhsType == DataTypes.INTEGER || rhsType == DataTypes.INTEGER) {
            return DataTypes.INTEGER;
        }
        if (lhsType == DataTypes.NULL || rhsType == DataTypes.NULL) {
            return DataTypes.NULL;
        }
        throw new UnsupportedOperationException();
    }

    static TestCaseSupplier arithmeticExceptionOverflowCase(
        DataType dataType,
        Supplier<Object> lhsSupplier,
        Supplier<Object> rhsSupplier,
        String evaluator
    ) {
        String typeNameOverflow = dataType.typeName().toLowerCase(Locale.ROOT) + " overflow";
        return new TestCaseSupplier(
            "<" + typeNameOverflow + ">",
            List.of(dataType),
            () -> new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(lhsSupplier.get(), dataType, "lhs"),
                    new TestCaseSupplier.TypedData(rhsSupplier.get(), dataType, "rhs")
                ),
                evaluator + "[lhs=Attribute[channel=0], rhs=Attribute[channel=1]]",
                dataType,
                is(nullValue())
            ).withWarning("Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.")
                .withWarning("Line -1:-1: java.lang.ArithmeticException: " + typeNameOverflow)
        );
    }
}
