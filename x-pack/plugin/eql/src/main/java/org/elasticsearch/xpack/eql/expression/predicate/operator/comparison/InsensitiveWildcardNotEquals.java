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

public class InsensitiveWildcardNotEquals extends InsensitiveNotEquals {

    public InsensitiveWildcardNotEquals(Source source,
                                        Expression left,
                                        Expression right, ZoneId zoneId) {
        super(source, left, right, zoneId);
    }

    @Override
    protected NodeInfo<InsensitiveNotEquals> info() {
        return NodeInfo.create(this, InsensitiveWildcardNotEquals::new, left(), right(), zoneId());
    }

    @Override
    protected InsensitiveWildcardNotEquals replaceChildren(Expression newLeft, Expression newRight) {
        return new InsensitiveWildcardNotEquals(source(), newLeft, newRight, zoneId());
    }

    @Override
    public InsensitiveWildcardNotEquals swapLeftAndRight() {
        return new InsensitiveWildcardNotEquals(source(), right(), left(), zoneId());
    }

    @Override
    public InsensitiveBinaryComparison negate() {
        return new InsensitiveWildcardEquals(source(), left(), right(), zoneId());
    }

    @Override
    protected String regularOperatorSymbol() {
        return BinaryComparisonProcessor.BinaryComparisonOperation.NEQ.symbol();
    }
}
