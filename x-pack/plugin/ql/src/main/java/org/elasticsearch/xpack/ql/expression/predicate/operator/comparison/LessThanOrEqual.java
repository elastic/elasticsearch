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

public class LessThanOrEqual extends BinaryComparison implements Negatable<BinaryComparison> {

    public LessThanOrEqual(Source source, Expression left, Expression right, ZoneId zoneId) {
        this(source, left, right, zoneId, false);
    }

    public LessThanOrEqual(Source source, Expression left, Expression right, ZoneId zoneId, boolean allowTextType) {
        super(source, left, right, BinaryComparisonOperation.LTE, zoneId, allowTextType);
    }

    @Override
    protected NodeInfo<LessThanOrEqual> info() {
        return NodeInfo.create(this, LessThanOrEqual::new, left(), right(), zoneId(), supportText());
    }

    @Override
    protected LessThanOrEqual replaceChildren(Expression newLeft, Expression newRight) {
        return new LessThanOrEqual(source(), newLeft, newRight, zoneId(), supportText());
    }

    @Override
    public GreaterThanOrEqual swapLeftAndRight() {
        return new GreaterThanOrEqual(source(), right(), left(), zoneId(), supportText());
    }

    @Override
    public GreaterThan negate() {
        return new GreaterThan(source(), left(), right(), zoneId(), supportText());
    }

    @Override
    public BinaryComparison reverse() {
        return new GreaterThanOrEqual(source(), left(), right(), zoneId(), supportText());
    }
}
