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

public class Equals extends EsqlBinaryComparison implements Negatable<EsqlBinaryComparison> {
    private static final Map<DataType, EsqlArithmeticOperation.BinaryEvaluator> evaluatorMap = Map.ofEntries(
        Map.entry(DataType.BOOLEAN, EqualsBoolsEvaluator.Factory::new),
        Map.entry(DataType.INTEGER, EqualsIntsEvaluator.Factory::new),
        Map.entry(DataType.DOUBLE, EqualsDoublesEvaluator.Factory::new),
        Map.entry(DataType.LONG, EqualsLongsEvaluator.Factory::new),
        Map.entry(DataType.UNSIGNED_LONG, EqualsLongsEvaluator.Factory::new),
        Map.entry(DataType.DATETIME, EqualsLongsEvaluator.Factory::new),
        Map.entry(DataType.GEO_POINT, EqualsGeometriesEvaluator.Factory::new),
        Map.entry(DataType.CARTESIAN_POINT, EqualsGeometriesEvaluator.Factory::new),
        Map.entry(DataType.GEO_SHAPE, EqualsGeometriesEvaluator.Factory::new),
        Map.entry(DataType.CARTESIAN_SHAPE, EqualsGeometriesEvaluator.Factory::new),
        Map.entry(DataType.KEYWORD, EqualsKeywordsEvaluator.Factory::new),
        Map.entry(DataType.TEXT, EqualsKeywordsEvaluator.Factory::new),
        Map.entry(DataType.VERSION, EqualsKeywordsEvaluator.Factory::new),
        Map.entry(DataType.IP, EqualsKeywordsEvaluator.Factory::new)
    );

    public Equals(Source source, Expression left, Expression right) {
        super(source, left, right, BinaryComparisonOperation.EQ, evaluatorMap);
    }

    public Equals(Source source, Expression left, Expression right, ZoneId zoneId) {
        super(source, left, right, BinaryComparisonOperation.EQ, zoneId, evaluatorMap);
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
    public EsqlBinaryComparison reverse() {
        return this;
    }

    @Override
    public EsqlBinaryComparison negate() {
        return new NotEquals(source(), left(), right(), zoneId());
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
        return lhs.equals(rhs);
    }

    @Evaluator(extraName = "Bools")
    static boolean processBools(boolean lhs, boolean rhs) {
        return lhs == rhs;
    }

    @Evaluator(extraName = "Geometries")
    static boolean processGeometries(BytesRef lhs, BytesRef rhs) {
        return lhs.equals(rhs);
    }

}
