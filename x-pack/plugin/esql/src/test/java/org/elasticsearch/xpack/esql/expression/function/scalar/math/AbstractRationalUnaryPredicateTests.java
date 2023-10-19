/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import org.elasticsearch.xpack.esql.expression.function.scalar.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.hamcrest.Matcher;

import java.util.List;

public abstract class AbstractRationalUnaryPredicateTests extends AbstractScalarFunctionTestCase {
    protected abstract RationalUnaryPredicate build(Source source, Expression value);

    protected abstract Matcher<Object> resultMatcher(double d);

    @Override
    protected DataType expectedType(List<DataType> argTypes) {
        return DataTypes.BOOLEAN;
    }

    @Override
    protected final List<ArgumentSpec> argSpec() {
        return List.of(required(rationals()));
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return build(source, args.get(0));
    }
}
