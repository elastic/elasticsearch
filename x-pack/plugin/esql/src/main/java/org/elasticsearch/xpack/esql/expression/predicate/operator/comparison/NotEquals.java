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

public class NotEquals extends EsqlBinaryComparison implements Negatable<EsqlBinaryComparison> {
    private static final Map<DataType, EsqlArithmeticOperation.BinaryEvaluator> evaluatorMap = Map.ofEntries(
        Map.entry(DataType.BOOLEAN, NotEqualsBoolsEvaluator.Factory::new),
        Map.entry(DataType.INTEGER, NotEqualsIntsEvaluator.Factory::new),
        Map.entry(DataType.DOUBLE, NotEqualsDoublesEvaluator.Factory::new),
        Map.entry(DataType.LONG, NotEqualsLongsEvaluator.Factory::new),
        Map.entry(DataType.UNSIGNED_LONG, NotEqualsLongsEvaluator.Factory::new),
        Map.entry(DataType.DATETIME, NotEqualsLongsEvaluator.Factory::new),
        Map.entry(DataType.GEO_POINT, NotEqualsGeometriesEvaluator.Factory::new),
        Map.entry(DataType.CARTESIAN_POINT, NotEqualsGeometriesEvaluator.Factory::new),
        Map.entry(DataType.GEO_SHAPE, NotEqualsGeometriesEvaluator.Factory::new),
        Map.entry(DataType.CARTESIAN_SHAPE, NotEqualsGeometriesEvaluator.Factory::new),
        Map.entry(DataType.KEYWORD, NotEqualsKeywordsEvaluator.Factory::new),
        Map.entry(DataType.TEXT, NotEqualsKeywordsEvaluator.Factory::new),
        Map.entry(DataType.VERSION, NotEqualsKeywordsEvaluator.Factory::new),
        Map.entry(DataType.IP, NotEqualsKeywordsEvaluator.Factory::new)
    );

    public NotEquals(Source source, Expression left, Expression right) {
        super(source, left, right, BinaryComparisonOperation.NEQ, evaluatorMap);
    }

    public NotEquals(Source source, Expression left, Expression right, ZoneId zoneId) {
        super(source, left, right, BinaryComparisonOperation.NEQ, zoneId, evaluatorMap);
    }

    @Evaluator(extraName = "Ints")
    static boolean processInts(int lhs, int rhs) {
        return lhs != rhs;
    }

    @Evaluator(extraName = "Longs")
    static boolean processLongs(long lhs, long rhs) {
        return lhs != rhs;
    }

    @Evaluator(extraName = "Doubles")
    static boolean processDoubles(double lhs, double rhs) {
        return lhs != rhs;
    }

    @Evaluator(extraName = "Keywords")
    static boolean processKeywords(BytesRef lhs, BytesRef rhs) {
        return false == lhs.equals(rhs);
    }

    @Evaluator(extraName = "Bools")
    static boolean processBools(boolean lhs, boolean rhs) {
        return lhs != rhs;
    }

    @Evaluator(extraName = "Geometries")
    static boolean processGeometries(BytesRef lhs, BytesRef rhs) {
        return false == lhs.equals(rhs);
    }

    @Override
    public EsqlBinaryComparison reverse() {
        return this;
    }

    @Override
    protected NodeInfo<NotEquals> info() {
        return NodeInfo.create(this, NotEquals::new, left(), right(), zoneId());
    }

    @Override
    protected NotEquals replaceChildren(Expression newLeft, Expression newRight) {
        return new NotEquals(source(), newLeft, newRight, zoneId());
    }

    @Override
    public NotEquals swapLeftAndRight() {
        return new NotEquals(source(), right(), left(), zoneId());
    }

    @Override
    public EsqlBinaryComparison negate() {
        return new Equals(source(), left(), right(), zoneId());
    }
}
