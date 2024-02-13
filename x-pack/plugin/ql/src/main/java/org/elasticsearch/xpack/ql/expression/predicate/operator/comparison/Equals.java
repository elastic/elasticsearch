/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.expression.predicate.operator.comparison;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.TypedAttribute;
import org.elasticsearch.xpack.ql.expression.predicate.Negatable;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.BinaryComparisonProcessor.BinaryComparisonOperation;
import org.elasticsearch.xpack.ql.planner.ExpressionTranslator;
import org.elasticsearch.xpack.ql.querydsl.query.Query;
import org.elasticsearch.xpack.ql.querydsl.query.RangeQuery;
import org.elasticsearch.xpack.ql.querydsl.query.TermQuery;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.time.ZoneId;

public class Equals extends BinaryComparison implements Negatable<BinaryComparison> {

    public Equals(Source source, Expression left, Expression right) {
        super(source, left, right, BinaryComparisonOperation.EQ, null);
    }

    public Equals(Source source, Expression left, Expression right, ZoneId zoneId) {
        super(source, left, right, BinaryComparisonOperation.EQ, zoneId);
    }

    @Override
    protected NodeInfo<Equals> info() {
        return NodeInfo.create(this, Equals::new, left(), right(), zoneId());
    }

    @Override
    protected Equals replaceChildren(Expression newLeft, Expression newRight) {
        return new Equals(source(), newLeft, newRight, zoneId());
    }

    @Override
    public Equals swapLeftAndRight() {
        return new Equals(source(), right(), left(), zoneId());
    }

    @Override
    public BinaryComparison negate() {
        return new NotEquals(source(), left(), right(), zoneId());
    }

    @Override
    public BinaryComparison reverse() {
        return this;
    }

    @Override
    protected boolean isCommutative() {
        return true;
    }

    @Override
    public Query getQuery(String name, Object value, String format, boolean isDateLiteralComparison, ZoneId zoneId) {
        if (left() instanceof TypedAttribute typedLeft) {
            name = ExpressionTranslator.pushableAttributeName(typedLeft);
        }
        if (isDateLiteralComparison) {
            // dates equality uses a range query because it's the one that has a "format" parameter
            return new RangeQuery(source(), name, value, true, value, true, format, zoneId);
        } else {
            return new TermQuery(source(), name, value);
        }
    }
}
