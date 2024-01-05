/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.time.ZoneId;

public class InsensitiveEquals extends InsensitiveBinaryComparison {

    public InsensitiveEquals(Source source, Expression left, Expression right, ZoneId zoneId) {
        super(source, left, right, zoneId);
    }

    @Override
    public InsensitiveBinaryComparison reverse() {
        return this;
    }

    @Override
    protected NodeInfo<InsensitiveEquals> info() {
        return NodeInfo.create(this, InsensitiveEquals::new, left(), right(), zoneId());
    }

    @Override
    protected InsensitiveEquals replaceChildren(Expression newLeft, Expression newRight) {
        return new InsensitiveEquals(source(), newLeft, newRight, zoneId());
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

    @Evaluator(extraName = "KeywordsConstant")
    static boolean processKeywords(BytesRef lhs, @Fixed ByteRunAutomaton rhs) {
        return rhs.run(lhs.bytes, lhs.offset, lhs.length);
    }

    @Evaluator(extraName = "Bools")
    static boolean processBools(boolean lhs, boolean rhs) {
        return lhs == rhs;
    }

    public Object symbol() {
        return "=~";
    }

    @Override
    public Boolean fold() {
        if (left().dataType() == DataTypes.TEXT || left().dataType() == DataTypes.KEYWORD) {
            BytesRef leftVal = (BytesRef) left().fold();
            BytesRef rightVal = (BytesRef) right().fold();
            if (leftVal == null || rightVal == null) {
                return null;
            }
            return processKeywords(leftVal, rightVal);
        }
        return new Equals(source(), left(), right()).fold();
    }
}
