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
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.BinaryComparisonProcessor;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.time.ZoneId;

import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.DEFAULT;

public class EqualsIgnoreCase extends BinaryComparison {

    public EqualsIgnoreCase(Source source, Expression left, Expression right, ZoneId zoneId) {
        super(source, left, right, BinaryComparisonProcessor.BinaryComparisonOperation.EQ_IGNORE_CASE, zoneId);
    }

    @Override
    protected TypeResolution resolveInputType(Expression e, TypeResolutions.ParamOrdinal paramOrdinal) {
        return EsqlTypeResolutions.isExact(e, sourceText(), DEFAULT);
    }

    @Override
    public BinaryComparison reverse() {
        return this;
    }

    @Override
    protected NodeInfo<EqualsIgnoreCase> info() {
        return NodeInfo.create(this, EqualsIgnoreCase::new, left(), right(), zoneId());
    }

    @Override
    protected EqualsIgnoreCase replaceChildren(Expression newLeft, Expression newRight) {
        return new EqualsIgnoreCase(source(), newLeft, newRight, zoneId());
    }

    @Override
    public EqualsIgnoreCase swapLeftAndRight() {
        return new EqualsIgnoreCase(source(), right(), left(), zoneId());
    }

    @Evaluator(extraName = "Ints")
    static boolean processInts(int lhs, int rhs) {
        return lhs == rhs;
    }

    @Evaluator(extraName = "Longs")
    static boolean processLongs(long lhs, long rhs) {
        return lhs == rhs;
    }

    @Evaluator(extraName = "Doubles")
    static boolean processDoubles(double lhs, double rhs) {
        return lhs == rhs;
    }

    @Evaluator(extraName = "Keywords")
    static boolean processKeywords(BytesRef lhs, BytesRef rhs) {
        return lhs.utf8ToString().equalsIgnoreCase(rhs.utf8ToString());
    }

    @Evaluator(extraName = "Bools")
    static boolean processBools(boolean lhs, boolean rhs) {
        return lhs == rhs;
    }
}
