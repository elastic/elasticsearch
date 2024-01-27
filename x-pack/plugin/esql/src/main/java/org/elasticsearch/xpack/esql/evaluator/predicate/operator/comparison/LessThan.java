/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.xpack.esql.expression.EsqlTypeResolutions;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.TypeResolutions;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.BinaryComparison;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.time.ZoneId;

import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.DEFAULT;

public class LessThan extends org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.LessThan {

    public LessThan(Source source, Expression left, Expression right, ZoneId zoneId) {
        super(source, left, right, zoneId);
    }

    @Override
    protected TypeResolution resolveInputType(Expression e, TypeResolutions.ParamOrdinal paramOrdinal) {
        return EsqlTypeResolutions.isExact(e, sourceText(), DEFAULT);
    }

    @Override
    protected NodeInfo<org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.LessThan> info() {
        return NodeInfo.create(this, LessThan::new, left(), right(), zoneId());
    }

    @Override
    protected org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.LessThan replaceChildren(
        Expression newLeft,
        Expression newRight
    ) {
        return new LessThan(source(), newLeft, newRight, zoneId());
    }

    @Override
    public GreaterThan swapLeftAndRight() {
        return new GreaterThan(source(), right(), left(), zoneId());
    }

    @Override
    public GreaterThanOrEqual negate() {
        return new GreaterThanOrEqual(source(), left(), right(), zoneId());
    }

    @Override
    public BinaryComparison reverse() {
        return new GreaterThan(source(), left(), right(), zoneId());
    }

    @Evaluator(extraName = "Ints")
    static boolean processInts(int lhs, int rhs) {
        return lhs < rhs;
    }

    @Evaluator(extraName = "Longs")
    static boolean processLongs(long lhs, long rhs) {
        return lhs < rhs;
    }

    @Evaluator(extraName = "Doubles")
    static boolean processDoubles(double lhs, double rhs) {
        return lhs < rhs;
    }

    @Evaluator(extraName = "Keywords")  // TODO rename to "Bytes"
    static boolean processKeywords(BytesRef lhs, BytesRef rhs) {
        return lhs.compareTo(rhs) < 0;
    }
}
