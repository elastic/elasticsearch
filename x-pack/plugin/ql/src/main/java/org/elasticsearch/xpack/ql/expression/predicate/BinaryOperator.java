/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ql.expression.predicate;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.Expressions.ParamOrdinal;
import org.elasticsearch.xpack.ql.tree.Source;

/**
 * Operator is a specialized binary predicate where both sides have the compatible types
 * (it's up to the analyzer to do any conversion if needed).
 */
public abstract class BinaryOperator<T, U, R, F extends PredicateBiFunction<T, U, R>> extends BinaryPredicate<T, U, R, F> {

    protected BinaryOperator(Source source, Expression left, Expression right, F function) {
        super(source, left, right, function);
    }

    protected abstract TypeResolution resolveInputType(Expression e, Expressions.ParamOrdinal paramOrdinal);

    public abstract BinaryOperator<T, U, R, F> swapLeftAndRight();

    @Override
    protected TypeResolution resolveType() {
        if (!childrenResolved()) {
            return new TypeResolution("Unresolved children");
        }

        TypeResolution resolution = resolveInputType(left(), ParamOrdinal.FIRST);
        if (resolution.unresolved()) {
            return resolution;
        }
        return resolveInputType(right(), ParamOrdinal.SECOND);
    }
}
