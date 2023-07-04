/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.operator;

import org.elasticsearch.xpack.esql.analysis.Verifier;
import org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.common.Failure;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.predicate.BinaryOperator;
import org.elasticsearch.xpack.ql.tree.Location;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.List;
import java.util.Locale;

import static org.elasticsearch.compute.data.BlockUtils.toJavaObject;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public abstract class AbstractBinaryOperatorTestCase extends AbstractFunctionTestCase {
    @Override
    protected final List<Object> simpleData() {
        return List.of(1, randomValueOtherThanMany(v -> rhsOk(v) == false, () -> between(-1, 1)));
    }

    protected boolean rhsOk(Object o) {
        return true;
    }

    @Override
    protected final Expression expressionForSimpleData() {
        return build(Source.EMPTY, field("lhs", DataTypes.INTEGER), field("rhs", DataTypes.INTEGER));
    }

    @Override
    protected Expression build(Source source, List<Literal> args) {
        return build(source, args.get(0), args.get(1));
    }

    protected abstract BinaryOperator<?, ?, ?, ?> build(Source source, Expression lhs, Expression rhs);

    @Override
    protected final Expression constantFoldable(List<Object> data) {
        return build(
            Source.EMPTY,
            List.of(new Literal(Source.EMPTY, data.get(0), DataTypes.INTEGER), new Literal(Source.EMPTY, data.get(1), DataTypes.INTEGER))
        );
    }

    protected abstract boolean supportsType(DataType type);

    public final void testApplyToAllTypes() {
        for (DataType lhsType : EsqlDataTypes.types()) {
            if (EsqlDataTypes.isRepresentable(lhsType) == false || lhsType == DataTypes.NULL) {
                continue;
            }
            if (supportsType(lhsType) == false) {
                continue;
            }
            Literal lhs = randomLiteral(lhsType);
            for (DataType rhsType : EsqlDataTypes.types()) {
                if (EsqlDataTypes.isRepresentable(rhsType) == false || rhsType == DataTypes.NULL) {
                    continue;
                }
                if (supportsType(rhsType) == false) {
                    continue;
                }
                if (false == (lhsType == rhsType || lhsType.isNumeric() && rhsType.isNumeric())) {
                    continue;
                }
                if (lhsType != rhsType && (lhsType == DataTypes.UNSIGNED_LONG || rhsType == DataTypes.UNSIGNED_LONG)) {
                    continue;
                }
                Literal rhs = randomValueOtherThanMany(l -> rhsOk(l.value()) == false, () -> randomLiteral(rhsType));
                BinaryOperator<?, ?, ?, ?> op = build(
                    new Source(Location.EMPTY, lhsType.typeName() + " " + rhsType.typeName()),
                    field("lhs", lhsType),
                    field("rhs", rhsType)
                );
                Object result = toJavaObject(evaluator(op).get().eval(row(List.of(lhs.value(), rhs.value()))), 0);
                // The type's currently only used for distinguishing between LONG and UNSIGNED_LONG. UL requires both operands be of the
                // same type, so either left or right type can be provided below. But otherwise the common type can be used instead.
                assertThat(op.toString(), result, resultMatcher(List.of(lhs.value(), rhs.value()), lhsType));
            }
        }
    }

    public final void testResolveType() {
        for (DataType lhsType : EsqlDataTypes.types()) {
            if (EsqlDataTypes.isRepresentable(lhsType) == false) {
                continue;
            }
            Literal lhs = randomLiteral(lhsType);
            for (DataType rhsType : EsqlDataTypes.types()) {
                if (EsqlDataTypes.isRepresentable(rhsType) == false) {
                    continue;
                }
                Literal rhs = randomLiteral(rhsType);
                BinaryOperator<?, ?, ?, ?> op = build(new Source(Location.EMPTY, lhsType.typeName() + " " + rhsType.typeName()), lhs, rhs);

                if (lhsType == DataTypes.UNSIGNED_LONG || rhsType == DataTypes.UNSIGNED_LONG) {
                    validateUnsignedLongType(op, lhsType, rhsType);
                    continue;
                }
                validateType(op, lhsType, rhsType);
            }
        }
    }

    private void validateUnsignedLongType(BinaryOperator<?, ?, ?, ?> op, DataType lhsType, DataType rhsType) {
        Failure fail = Verifier.validateUnsignedLongOperator(op);
        if (lhsType == rhsType) {
            assertThat(op.toString(), fail, nullValue());
            return;
        }
        assertThat(op.toString(), fail, not(nullValue()));
        assertThat(
            op.toString(),
            fail.message(),
            equalTo(
                String.format(
                    Locale.ROOT,
                    "first argument of [%s] is [%s] and second is [%s]. [unsigned_long] can only be operated on together "
                        + "with another [unsigned_long]",
                    op,
                    lhsType.typeName(),
                    rhsType.typeName()
                )
            )
        );

    }

    protected abstract void validateType(BinaryOperator<?, ?, ?, ?> op, DataType lhsType, DataType rhsType);
}
