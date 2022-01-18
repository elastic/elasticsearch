/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.expression.predicate.operator.comparison;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.predicate.Negatable;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.time.ZoneId;

public class InsensitiveEquals extends InsensitiveBinaryComparison implements Negatable<InsensitiveBinaryComparison> {

    public InsensitiveEquals(Source source, Expression left, Expression right, ZoneId zoneId) {
        super(source, left, right, InsensitiveBinaryComparisonProcessor.InsensitiveBinaryComparisonOperation.SEQ, zoneId);
    }

    @Override
    protected NodeInfo<InsensitiveEquals> info() {
        return NodeInfo.create(this, InsensitiveEquals::new, left(), right(), zoneId());
    }

    @Override
    protected InsensitiveEquals replaceChildren(Expression newLeft, Expression newRight) {
        return new InsensitiveEquals(source(), newLeft, newRight, zoneId());
    }

    @Override
    public InsensitiveEquals swapLeftAndRight() {
        return new InsensitiveEquals(source(), right(), left(), zoneId());
    }

    @Override
    public InsensitiveBinaryComparison negate() {
        return new InsensitiveNotEquals(source(), left(), right(), zoneId());
    }

    @Override
    protected String regularOperatorSymbol() {
        return "in";
    }
}
