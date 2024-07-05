/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;
import java.time.ZoneId;

/**
 * Implements the MySQL {@code <=>} operator
 */
public class NullEquals extends BinaryComparison {

    public NullEquals(Source source, Expression left, Expression right, ZoneId zoneId) {
        super(source, left, right, BinaryComparisonOperation.NULLEQ, zoneId);
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
    protected NodeInfo<NullEquals> info() {
        return NodeInfo.create(this, NullEquals::new, left(), right(), zoneId());
    }

    @Override
    protected NullEquals replaceChildren(Expression newLeft, Expression newRight) {
        return new NullEquals(source(), newLeft, newRight, zoneId());
    }

    @Override
    public NullEquals swapLeftAndRight() {
        return new NullEquals(source(), right(), left(), zoneId());
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
