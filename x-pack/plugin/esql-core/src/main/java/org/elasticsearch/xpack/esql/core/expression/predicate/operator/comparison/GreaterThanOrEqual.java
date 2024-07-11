/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.predicate.Negatable;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;
import java.time.ZoneId;

public class GreaterThanOrEqual extends BinaryComparison implements Negatable<BinaryComparison> {

    public GreaterThanOrEqual(Source source, Expression left, Expression right, ZoneId zoneId) {
        super(source, left, right, BinaryComparisonOperation.GTE, zoneId);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException();
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
