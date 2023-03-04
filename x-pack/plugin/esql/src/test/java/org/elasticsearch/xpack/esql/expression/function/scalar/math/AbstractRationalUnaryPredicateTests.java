/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import org.elasticsearch.xpack.esql.expression.function.scalar.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.tree.Location;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.hamcrest.Matcher;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public abstract class AbstractRationalUnaryPredicateTests extends AbstractScalarFunctionTestCase {
    protected abstract RationalUnaryPredicate build(Source source, Expression value);

    protected abstract Matcher<Object> resultMatcher(double d);

    @Override
    protected final List<Object> simpleData() {
        return List.of(switch (between(0, 2)) {
            case 0 -> Double.NaN;
            case 1 -> randomBoolean() ? Double.POSITIVE_INFINITY : Double.NEGATIVE_INFINITY;
            case 2 -> randomDouble();
            default -> throw new IllegalArgumentException();
        });
    }

    @Override
    protected final Expression expressionForSimpleData() {
        return build(Source.EMPTY, field("v", DataTypes.DOUBLE));
    }

    @Override
    protected final DataType expressionForSimpleDataType() {
        return DataTypes.BOOLEAN;
    }

    @Override
    protected final Matcher<Object> resultMatcher(List<Object> data) {
        double d = (Double) data.get(0);
        return resultMatcher(d);
    }

    @Override
    protected final Expression constantFoldable(List<Object> data) {
        return build(Source.EMPTY, new Literal(Source.EMPTY, data.get(0), DataTypes.DOUBLE));
    }

    @Override
    public final void testResolveTypeInvalid() {
        for (DataType type : EsqlDataTypes.types()) {
            if (type.isRational() || type == DataTypes.NULL) {
                continue;
            }
            Expression.TypeResolution resolution = build(
                new Source(Location.EMPTY, "foo"),
                new Literal(new Source(Location.EMPTY, "v"), "v", type)
            ).resolveType();
            assertFalse(type.typeName() + " is invalid", resolution.resolved());
            assertThat(resolution.message(), equalTo("argument of [foo] must be [double], found value [v] type [" + type.typeName() + "]"));
        }
    }

    private void testCase(double d) {
        assertThat((Boolean) evaluator(expressionForSimpleData()).get().computeRow(row(List.of(d)), 0), resultMatcher(d));
    }

    public final void testNaN() {
        testCase(Double.NaN);
    }

    public final void testPositiveInfinity() {
        testCase(Double.POSITIVE_INFINITY);
    }

    public final void testNegativeInfinity() {
        testCase(Double.NEGATIVE_INFINITY);
    }

    public final void testPositiveSmallDouble() {
        testCase(randomDouble());
    }

    public final void testNegativeSmallDouble() {
        testCase(-randomDouble());
    }

    public final void testPositiveBigDouble() {
        testCase(1 / randomDouble());
    }

    public final void testNegativeBigDouble() {
        testCase(-1 / randomDouble());
    }

}
