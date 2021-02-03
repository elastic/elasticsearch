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

public class InsensitiveNotEquals extends InsensitiveBinaryComparison implements Negatable<InsensitiveBinaryComparison> {

    public InsensitiveNotEquals(Source source, Expression left, Expression right, ZoneId zoneId) {
        super(source, left, right, InsensitiveBinaryComparisonProcessor.InsensitiveBinaryComparisonOperation.SNEQ, zoneId);
    }

    @Override
    protected NodeInfo<InsensitiveNotEquals> info() {
        return NodeInfo.create(this, InsensitiveNotEquals::new, left(), right(), zoneId());
    }

    @Override
    protected InsensitiveNotEquals replaceChildren(Expression newLeft, Expression newRight) {
        return new InsensitiveNotEquals(source(), newLeft, newRight, zoneId());
    }

    @Override
    public InsensitiveNotEquals swapLeftAndRight() {
        return new InsensitiveNotEquals(source(), right(), left(), zoneId());
    }

    @Override
    public InsensitiveBinaryComparison negate() {
        return new InsensitiveEquals(source(), left(), right(), zoneId());
    }

    @Override
    protected String regularOperatorSymbol() {
        return "not in";
    }
}




