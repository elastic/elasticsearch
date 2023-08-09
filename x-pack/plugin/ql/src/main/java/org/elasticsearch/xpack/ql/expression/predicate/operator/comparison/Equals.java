/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.expression.predicate.operator.comparison;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.predicate.Negatable;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.BinaryComparisonProcessor.BinaryComparisonOperation;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.time.ZoneId;

public class Equals extends BinaryComparison implements Negatable<BinaryComparison> {

    public Equals(Source source, Expression left, Expression right) {
        this(source, left, right, null, false);
    }

    public Equals(Source source, Expression left, Expression right, ZoneId zoneId) {
        this(source, left, right, zoneId, false);
    }

    public Equals(Source source, Expression left, Expression right, ZoneId zoneId, boolean allowTextType) {
        super(source, left, right, BinaryComparisonOperation.EQ, zoneId, allowTextType);
    }

    @Override
    protected NodeInfo<Equals> info() {
        return NodeInfo.create(this, Equals::new, left(), right(), zoneId(), supportText());
    }

    @Override
    protected Equals replaceChildren(Expression newLeft, Expression newRight) {
        return new Equals(source(), newLeft, newRight, zoneId(), supportText());
    }

    @Override
    public Equals swapLeftAndRight() {
        return new Equals(source(), right(), left(), zoneId(), supportText());
    }

    @Override
    public BinaryComparison negate() {
        return new NotEquals(source(), left(), right(), zoneId(), supportText());
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
