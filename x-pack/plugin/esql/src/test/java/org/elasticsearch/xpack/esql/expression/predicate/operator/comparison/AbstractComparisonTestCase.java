/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.operator.comparison;

import org.elasticsearch.xpack.esql.analysis.Verifier;
import org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.common.Failure;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.BinaryComparison;
import org.elasticsearch.xpack.ql.tree.Location;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.hamcrest.Matcher;

import java.util.List;
import java.util.Locale;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public abstract class AbstractComparisonTestCase extends AbstractFunctionTestCase {
    @Override
    protected final List<Object> simpleData() {
        return List.of(1, between(-1, 1));
    }

    @Override
    protected final Expression expressionForSimpleData() {
        return build(Source.EMPTY, field("lhs", DataTypes.INTEGER), field("rhs", DataTypes.INTEGER));
    }

    @Override
    protected Expression build(Source source, List<Literal> args) {
        return build(source, args.get(0), args.get(1));
    }

    protected abstract BinaryComparison build(Source source, Expression lhs, Expression rhs);

    @Override
    protected final DataType expressionForSimpleDataType() {
        return DataTypes.BOOLEAN;
    }

    @Override
    @SuppressWarnings({ "unchecked", "rawtypes" })
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
    protected final Expression constantFoldable(List<Object> data) {
        return build(
            Source.EMPTY,
            List.of(new Literal(Source.EMPTY, data.get(0), DataTypes.INTEGER), new Literal(Source.EMPTY, data.get(1), DataTypes.INTEGER))
        );
    }

    protected abstract boolean isEquality();

    public final void testCompareAllTypes() {
        for (DataType lhsType : EsqlDataTypes.types()) {
            if (EsqlDataTypes.isRepresentable(lhsType) == false || lhsType == DataTypes.NULL) {
                continue;
            }
            Literal lhs = randomLiteral(lhsType);
            for (DataType rhsType : EsqlDataTypes.types()) {
                if (EsqlDataTypes.isRepresentable(rhsType) == false || rhsType == DataTypes.NULL) {
                    continue;
                }
                if (isEquality() == false && lhsType == DataTypes.BOOLEAN) {
                    continue;
                }
                if (false == (lhsType == rhsType || lhsType.isNumeric() && rhsType.isNumeric())) {
                    continue;
                }
                Literal rhs = randomLiteral(rhsType);
                BinaryComparison bc = build(
                    new Source(Location.EMPTY, lhsType.typeName() + " " + rhsType.typeName()),
                    field("lhs", lhsType),
                    field("rhs", rhsType)
                );
                Object result = evaluator(bc).get().computeRow(row(List.of(lhs.value(), rhs.value())), 0);
                assertThat(bc.toString(), result, resultMatcher(List.of(lhs.value(), rhs.value())));
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
                BinaryComparison bc = build(new Source(Location.EMPTY, lhsType.typeName() + " " + rhsType.typeName()), lhs, rhs);
                assertTrue(bc.typeResolved().resolved());
                assertThat(bc.dataType(), equalTo(DataTypes.BOOLEAN));
                Failure f = Verifier.validateBinaryComparison(bc);
                if (isEquality() == false && lhsType == DataTypes.BOOLEAN) {
                    assertThat(bc.toString(), f, not(nullValue()));
                    assertThat(
                        bc.toString(),
                        f.message(),
                        equalTo(
                            String.format(
                                Locale.ROOT,
                                "first argument of [%s %s] must be [keyword or datetime], found value [] type [%s]",
                                lhsType.typeName(),
                                rhsType.typeName(),
                                lhsType.typeName()
                            )
                        )
                    );
                    continue;
                }
                if (lhsType == rhsType || lhsType.isNumeric() && rhsType.isNumeric()) {
                    assertThat(bc.toString(), f, nullValue());
                    continue;
                }
                assertThat(bc.toString(), f, not(nullValue()));
                assertThat(
                    bc.toString(),
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
    }
}
