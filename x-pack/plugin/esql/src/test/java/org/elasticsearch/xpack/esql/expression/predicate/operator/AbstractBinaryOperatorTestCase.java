/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.operator;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.xpack.esql.analysis.Verifier;
import org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.common.Failure;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.predicate.BinaryOperator;
import org.elasticsearch.xpack.ql.tree.Location;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.hamcrest.Matcher;

import java.util.List;
import java.util.Locale;

import static org.elasticsearch.compute.data.BlockUtils.toJavaObject;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypes.isRepresentable;
import static org.elasticsearch.xpack.ql.type.DataTypeConverter.commonType;
import static org.elasticsearch.xpack.ql.type.DataTypes.isNull;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public abstract class AbstractBinaryOperatorTestCase extends AbstractFunctionTestCase {

    protected abstract Matcher<Object> resultsMatcher(List<TestCaseSupplier.TypedData> typedData);

    /**
     * Return a {@link Matcher} to validate the results of evaluating the function
     *
     * @param data a list of the parameters that were passed to the evaluator
     * @return a matcher to validate correctness against the given data set
     */
    protected abstract Matcher<Object> resultMatcher(List<Object> data, DataType dataType);

    protected boolean rhsOk(Object o) {
        return true;
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return build(source, args.get(0), args.get(1));
    }

    protected abstract BinaryOperator<?, ?, ?, ?> build(Source source, Expression lhs, Expression rhs);

    /**
     * What type is acceptable for any of the function parameters.
     * @param type The type to probe.
     * @return True if the type is supported by the respective function.
     */
    protected abstract boolean supportsType(DataType type);

    /**
     * What combination of parameter types are acceptable by the function.
     * @param lhsType Left argument type.
     * @param rhsType Right argument type.
     * @return True if the type combination is supported by the respective function.
     */
    protected boolean supportsTypes(DataType lhsType, DataType rhsType) {
        if (isNull(lhsType) || isNull(rhsType)) {
            return false;
        }
        if ((lhsType == DataTypes.UNSIGNED_LONG || rhsType == DataTypes.UNSIGNED_LONG) && lhsType != rhsType) {
            // UL can only be operated on together with another UL, so skip non-UL&UL combinations
            return false;
        }
        return supportsType(lhsType) && supportsType(rhsType);
    }

    public final void testApplyToAllTypes() {
        // TODO replace with test cases
        for (DataType lhsType : EsqlDataTypes.types()) {
            for (DataType rhsType : EsqlDataTypes.types()) {
                if (supportsTypes(lhsType, rhsType) == false) {
                    continue;
                }
                Literal lhs = randomLiteral(lhsType);
                Literal rhs = randomValueOtherThanMany(l -> rhsOk(l.value()) == false, () -> randomLiteral(rhsType));
                Object result;
                BinaryOperator<?, ?, ?, ?> op;
                Source src = new Source(Location.EMPTY, lhsType.typeName() + " " + rhsType.typeName());
                if (isRepresentable(lhsType) && isRepresentable(rhsType)) {
                    op = build(src, field("lhs", lhsType), field("rhs", rhsType));
                    try (Block block = evaluator(op).get(driverContext()).eval(row(List.of(lhs.value(), rhs.value())))) {
                        result = toJavaObject(block, 0);
                    }
                } else {
                    op = build(src, lhs, rhs);
                    result = op.fold();
                }
                if (result == null) {
                    assertCriticalWarnings(
                        "Line -1:-1: evaluation of [" + op + "] failed, treating result as null. Only first 20 failures recorded.",
                        "Line -1:-1: java.lang.ArithmeticException: " + commonType(lhsType, rhsType).typeName() + " overflow"
                    );
                } else {
                    // The type's currently only used for distinguishing between LONG and UNSIGNED_LONG. UL requires both operands be of
                    // the same type, so either left or right type can be provided below. But otherwise the common type can be used
                    // instead.
                    assertThat(op.toString(), result, resultMatcher(List.of(lhs.value(), rhs.value()), lhsType));
                }
            }
        }
    }

    public final void testResolveType() {
        for (DataType lhsType : EsqlDataTypes.types()) {
            if (isRepresentable(lhsType) == false) {
                continue;
            }
            Literal lhs = randomLiteral(lhsType);
            for (DataType rhsType : EsqlDataTypes.types()) {
                if (isRepresentable(rhsType) == false) {
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
