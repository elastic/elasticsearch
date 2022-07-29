/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.expression.predicate.operator.comparison;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.BinaryComparisonProcessor;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.time.ZoneId;

/**
 * Variant of insensitive equality that understands wildcard patterns (*, !).
 * Should be replaced by the optimizer in case of the wildcard pattern, otherwise behaves just like its parent.
 */
public class InsensitiveWildcardEquals extends InsensitiveEquals {

    public InsensitiveWildcardEquals(Source source, Expression left, Expression right, ZoneId zoneId) {
        super(source, left, right, zoneId);
    }

    @Override
    protected NodeInfo<InsensitiveEquals> info() {
        return NodeInfo.create(this, InsensitiveWildcardEquals::new, left(), right(), zoneId());
    }

    @Override
    protected InsensitiveWildcardEquals replaceChildren(Expression newLeft, Expression newRight) {
        return new InsensitiveWildcardEquals(source(), newLeft, newRight, zoneId());
    }

    @Override
    public InsensitiveWildcardEquals swapLeftAndRight() {
        return new InsensitiveWildcardEquals(source(), right(), left(), zoneId());
    }

    @Override
    public InsensitiveBinaryComparison negate() {
        return new InsensitiveWildcardNotEquals(source(), left(), right(), zoneId());
    }

    @Override
    protected String regularOperatorSymbol() {
        return BinaryComparisonProcessor.BinaryComparisonOperation.EQ.symbol();
    }
}
