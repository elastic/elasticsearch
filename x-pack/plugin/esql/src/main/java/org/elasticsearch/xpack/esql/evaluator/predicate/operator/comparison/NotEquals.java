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
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.EsqlArithmeticOperation;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.TypeResolutions;
import org.elasticsearch.xpack.ql.expression.predicate.Negatable;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.BinaryComparison;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.BinaryComparisonProcessor;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.time.ZoneId;
import java.util.Map;

import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.DEFAULT;

public class NotEquals extends EsqlBinaryComparison implements Negatable<BinaryComparison> {
    private static final Map<DataType, EsqlArithmeticOperation.BinaryEvaluator> evaluatorMap = Map.ofEntries(
        Map.entry(DataTypes.BOOLEAN, NotEqualsBoolsEvaluator.Factory::new),
        Map.entry(DataTypes.INTEGER, NotEqualsIntsEvaluator.Factory::new),
        Map.entry(DataTypes.DOUBLE, NotEqualsDoublesEvaluator.Factory::new),
        Map.entry(DataTypes.LONG, NotEqualsLongsEvaluator.Factory::new),
        Map.entry(DataTypes.UNSIGNED_LONG, NotEqualsLongsEvaluator.Factory::new),
        Map.entry(DataTypes.DATETIME, NotEqualsLongsEvaluator.Factory::new),
        Map.entry(EsqlDataTypes.GEO_POINT, NotEqualsGeometriesEvaluator.Factory::new),
        Map.entry(EsqlDataTypes.CARTESIAN_POINT, NotEqualsGeometriesEvaluator.Factory::new),
        Map.entry(EsqlDataTypes.GEO_SHAPE, NotEqualsGeometriesEvaluator.Factory::new),
        Map.entry(EsqlDataTypes.CARTESIAN_SHAPE, NotEqualsGeometriesEvaluator.Factory::new),
        Map.entry(DataTypes.KEYWORD, NotEqualsKeywordsEvaluator.Factory::new),
        Map.entry(DataTypes.TEXT, NotEqualsKeywordsEvaluator.Factory::new),
        Map.entry(DataTypes.VERSION, NotEqualsKeywordsEvaluator.Factory::new),
        Map.entry(DataTypes.IP, NotEqualsKeywordsEvaluator.Factory::new)
    );

    public NotEquals(Source source, Expression left, Expression right) {
        super(source, left, right, BinaryComparisonProcessor.BinaryComparisonOperation.NEQ, evaluatorMap);
    }

    public NotEquals(Source source, Expression left, Expression right, ZoneId zoneId) {
        super(source, left, right, BinaryComparisonProcessor.BinaryComparisonOperation.NEQ, zoneId, evaluatorMap);
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
    protected TypeResolution resolveInputType(Expression e, TypeResolutions.ParamOrdinal paramOrdinal) {
        return EsqlTypeResolutions.isExact(e, sourceText(), DEFAULT);
    }

    @Override
    public BinaryComparison reverse() {
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
    public BinaryComparison negate() {
        return new Equals(source(), left(), right(), zoneId());
    }
}
