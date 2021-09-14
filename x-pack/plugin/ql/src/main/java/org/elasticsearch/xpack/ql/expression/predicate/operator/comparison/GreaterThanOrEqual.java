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

public class GreaterThanOrEqual extends BinaryComparison implements Negatable<BinaryComparison> {

    public GreaterThanOrEqual(Source source, Expression left, Expression right, ZoneId zoneId) {
        super(source, left, right, BinaryComparisonOperation.GTE, zoneId);
    }

    @Override
    protected NodeInfo<GreaterThanOrEqual> info() {
        return NodeInfo.create(this, GreaterThanOrEqual::new, left(), right(), zoneId());
    }

    @Override
    protected GreaterThanOrEqual replaceChildren(Expression newLeft, Expression newRight) {
        return new GreaterThanOrEqual(source(), newLeft, newRight, zoneId());
    }

    @Override
    public LessThanOrEqual swapLeftAndRight() {
        return new LessThanOrEqual(source(), right(), left(), zoneId());
    }

    @Override
    public LessThan negate() {
        return new LessThan(source(), left(), right(), zoneId());
    }

    @Override
    public BinaryComparison reverse() {
        return new LessThanOrEqual(source(), left(), right(), zoneId());
    }
}
