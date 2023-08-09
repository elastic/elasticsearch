/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.expression.predicate.operator.comparison;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Nullability;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.BinaryComparisonProcessor.BinaryComparisonOperation;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.time.ZoneId;

/**
 * Implements the MySQL {@code <=>} operator
 */
public class NullEquals extends BinaryComparison {

    public NullEquals(Source source, Expression left, Expression right, ZoneId zoneId) {
        this(source, left, right, zoneId, false);
    }

    public NullEquals(Source source, Expression left, Expression right, ZoneId zoneId, boolean allowTextType) {
        super(source, left, right, BinaryComparisonOperation.NULLEQ, zoneId, allowTextType);
    }

    @Override
    protected NodeInfo<NullEquals> info() {
        return NodeInfo.create(this, NullEquals::new, left(), right(), zoneId(), supportText());
    }

    @Override
    protected NullEquals replaceChildren(Expression newLeft, Expression newRight) {
        return new NullEquals(source(), newLeft, newRight, zoneId(), supportText());
    }

    @Override
    public NullEquals swapLeftAndRight() {
        return new NullEquals(source(), right(), left(), zoneId(), supportText());
    }

    @Override
    public Nullability nullable() {
        return Nullability.FALSE;
    }

    @Override
    public BinaryComparison reverse() {
        return this;
    }

    @Override
    protected boolean isCommutative() {
        return true;
    }
}
