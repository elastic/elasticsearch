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

public class LessThan extends EsqlBinaryComparison implements Negatable<EsqlBinaryComparison> {

    private static final Map<DataType, EsqlArithmeticOperation.BinaryEvaluator> evaluatorMap = Map.ofEntries(
        Map.entry(DataType.INTEGER, LessThanIntsEvaluator.Factory::new),
        Map.entry(DataType.DOUBLE, LessThanDoublesEvaluator.Factory::new),
        Map.entry(DataType.LONG, LessThanLongsEvaluator.Factory::new),
        Map.entry(DataType.UNSIGNED_LONG, LessThanLongsEvaluator.Factory::new),
        Map.entry(DataType.DATETIME, LessThanLongsEvaluator.Factory::new),
        Map.entry(DataType.KEYWORD, LessThanKeywordsEvaluator.Factory::new),
        Map.entry(DataType.TEXT, LessThanKeywordsEvaluator.Factory::new),
        Map.entry(DataType.VERSION, LessThanKeywordsEvaluator.Factory::new),
        Map.entry(DataType.IP, LessThanKeywordsEvaluator.Factory::new)
    );

    public LessThan(Source source, Expression left, Expression right) {
        this(source, left, right, null);
    }

    public LessThan(Source source, Expression left, Expression right, ZoneId zoneId) {
        super(source, left, right, BinaryComparisonOperation.LT, zoneId, evaluatorMap);
    }

    @Override
    protected NodeInfo<LessThan> info() {
        return NodeInfo.create(this, LessThan::new, left(), right(), zoneId());
    }

    @Override
    protected LessThan replaceChildren(Expression newLeft, Expression newRight) {
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
    public EsqlBinaryComparison reverse() {
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
