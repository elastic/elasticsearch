/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.predicate.operator.comparison;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.EsqlArithmeticOperation;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.predicate.Negatable;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.time.ZoneId;
import java.util.Map;

public class LessThanOrEqual extends EsqlBinaryComparison implements Negatable<EsqlBinaryComparison> {
    private static final Map<DataType, EsqlArithmeticOperation.BinaryEvaluator> evaluatorMap = Map.ofEntries(
        Map.entry(DataTypes.INTEGER, LessThanOrEqualIntsEvaluator.Factory::new),
        Map.entry(DataTypes.DOUBLE, LessThanOrEqualDoublesEvaluator.Factory::new),
        Map.entry(DataTypes.LONG, LessThanOrEqualLongsEvaluator.Factory::new),
        Map.entry(DataTypes.UNSIGNED_LONG, LessThanOrEqualLongsEvaluator.Factory::new),
        Map.entry(DataTypes.DATETIME, LessThanOrEqualLongsEvaluator.Factory::new),
        Map.entry(DataTypes.KEYWORD, LessThanOrEqualKeywordsEvaluator.Factory::new),
        Map.entry(DataTypes.TEXT, LessThanOrEqualKeywordsEvaluator.Factory::new),
        Map.entry(DataTypes.VERSION, LessThanOrEqualKeywordsEvaluator.Factory::new),
        Map.entry(DataTypes.IP, LessThanOrEqualKeywordsEvaluator.Factory::new)
    );

    public LessThanOrEqual(Source source, Expression left, Expression right) {
        this(source, left, right, null);
    }

    public LessThanOrEqual(Source source, Expression left, Expression right, ZoneId zoneId) {
        super(source, left, right, BinaryComparisonOperation.LTE, zoneId, evaluatorMap);
    }

    @Override
    protected NodeInfo<LessThanOrEqual> info() {
        return NodeInfo.create(this, LessThanOrEqual::new, left(), right(), zoneId());
    }

    @Override
    protected LessThanOrEqual replaceChildren(Expression newLeft, Expression newRight) {
        return new LessThanOrEqual(source(), newLeft, newRight, zoneId());
    }

    @Override
    public GreaterThanOrEqual swapLeftAndRight() {
        return new GreaterThanOrEqual(source(), right(), left(), zoneId());
    }

    @Override
    public GreaterThan negate() {
        return new GreaterThan(source(), left(), right(), zoneId());
    }

    @Override
    public EsqlBinaryComparison reverse() {
        return new GreaterThanOrEqual(source(), left(), right(), zoneId());
    }

    @Evaluator(extraName = "Ints")
    static boolean processInts(int lhs, int rhs) {
        return lhs <= rhs;
    }

    @Evaluator(extraName = "Longs")
    static boolean processLongs(long lhs, long rhs) {
        return lhs <= rhs;
    }

    @Evaluator(extraName = "Doubles")
    static boolean processDoubles(double lhs, double rhs) {
        return lhs <= rhs;
    }

    @Evaluator(extraName = "Keywords")
    static boolean processKeywords(BytesRef lhs, BytesRef rhs) {
        return lhs.compareTo(rhs) <= 0;
    }
}
