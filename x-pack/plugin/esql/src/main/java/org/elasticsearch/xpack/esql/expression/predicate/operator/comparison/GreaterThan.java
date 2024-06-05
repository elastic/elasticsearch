/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.predicate.operator.comparison;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.predicate.Negatable;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.EsqlArithmeticOperation;

import java.time.ZoneId;
import java.util.Map;

public class GreaterThan extends EsqlBinaryComparison implements Negatable<EsqlBinaryComparison> {
    private static final Map<DataType, EsqlArithmeticOperation.BinaryEvaluator> evaluatorMap = Map.ofEntries(
        Map.entry(DataType.INTEGER, GreaterThanIntsEvaluator.Factory::new),
        Map.entry(DataType.DOUBLE, GreaterThanDoublesEvaluator.Factory::new),
        Map.entry(DataType.LONG, GreaterThanLongsEvaluator.Factory::new),
        Map.entry(DataType.UNSIGNED_LONG, GreaterThanLongsEvaluator.Factory::new),
        Map.entry(DataType.DATETIME, GreaterThanLongsEvaluator.Factory::new),
        Map.entry(DataType.KEYWORD, GreaterThanKeywordsEvaluator.Factory::new),
        Map.entry(DataType.TEXT, GreaterThanKeywordsEvaluator.Factory::new),
        Map.entry(DataType.VERSION, GreaterThanKeywordsEvaluator.Factory::new),
        Map.entry(DataType.IP, GreaterThanKeywordsEvaluator.Factory::new)
    );

    public GreaterThan(Source source, Expression left, Expression right) {
        super(source, left, right, BinaryComparisonOperation.GT, evaluatorMap);
    }

    public GreaterThan(Source source, Expression left, Expression right, ZoneId zoneId) {
        super(source, left, right, BinaryComparisonOperation.GT, zoneId, evaluatorMap);
    }

    @Override
    protected NodeInfo<GreaterThan> info() {
        return NodeInfo.create(this, GreaterThan::new, left(), right(), zoneId());
    }

    @Override
    protected GreaterThan replaceChildren(Expression newLeft, Expression newRight) {
        return new GreaterThan(source(), newLeft, newRight, zoneId());
    }

    @Override
    public LessThan swapLeftAndRight() {
        return new LessThan(source(), right(), left(), zoneId());
    }

    @Override
    public LessThanOrEqual negate() {
        return new LessThanOrEqual(source(), left(), right(), zoneId());
    }

    @Override
    public EsqlBinaryComparison reverse() {
        return new LessThan(source(), left(), right(), zoneId());
    }

    @Evaluator(extraName = "Ints")
    static boolean processInts(int lhs, int rhs) {
        return lhs > rhs;
    }

    @Evaluator(extraName = "Longs")
    static boolean processLongs(long lhs, long rhs) {
        return lhs > rhs;
    }

    @Evaluator(extraName = "Doubles")
    static boolean processDoubles(double lhs, double rhs) {
        return lhs > rhs;
    }

    @Evaluator(extraName = "Keywords")
    static boolean processKeywords(BytesRef lhs, BytesRef rhs) {
        return lhs.compareTo(rhs) > 0;
    }
}
