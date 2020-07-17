/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
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
        super(source, left, right, BinaryComparisonOperation.LTE, zoneId);
    }

    @Override
    protected NodeInfo<LessThanOrEqual> info() {
        return NodeInfo.create(this, LessThanOrEqual::new, left(), right(), zoneId());
    }

    @Override
    protected LessThanOrEqual replaceChildren(Expression newLeft, Expression newRight) {
        return new LessThanOrEqual(source(), newLeft, newRight, zoneId());
    }

    @Override
    public GreaterThanOrEqual swapLeftAndRight() {
        return new GreaterThanOrEqual(source(), right(), left(), zoneId());
    }

    @Override
    public GreaterThan negate() {
        return new GreaterThan(source(), left(), right(), zoneId());
    }
}
