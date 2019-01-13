/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.predicate.operator.comparison;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.Nullability;
import org.elasticsearch.xpack.sql.expression.predicate.operator.comparison.BinaryComparisonProcessor.BinaryComparisonOperation;
import org.elasticsearch.xpack.sql.tree.Source;
import org.elasticsearch.xpack.sql.tree.NodeInfo;

/**
 * Implements the MySQL {@code <=>} operator
 */
public class NullEquals extends BinaryComparison {

    public NullEquals(Source source, Expression left, Expression right) {
        super(source, left, right, BinaryComparisonOperation.NULLEQ);
    }

    @Override
    protected NodeInfo<NullEquals> info() {
        return NodeInfo.create(this, NullEquals::new, left(), right());
    }

    @Override
    protected NullEquals replaceChildren(Expression newLeft, Expression newRight) {
        return new NullEquals(source(), newLeft, newRight);
    }

    @Override
    public NullEquals swapLeftAndRight() {
        return new NullEquals(source(), right(), left());
    }

    @Override
    public Nullability nullable() {
        return Nullability.FALSE;
    }
}
