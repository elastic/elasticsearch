/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.predicate.operator.comparison;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.predicate.Negatable;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.EsqlArithmeticOperation;

import java.time.ZoneId;
import java.util.Map;

public class Equals extends EsqlBinaryComparison implements Negatable<EsqlBinaryComparison> {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "Equals",
        EsqlBinaryComparison::readFrom
    );

    private static final Map<DataType, EsqlArithmeticOperation.BinaryEvaluator> evaluatorMap = Map.ofEntries(
        Map.entry(DataType.BOOLEAN, EqualsBoolsEvaluator.Factory::new),
        Map.entry(DataType.INTEGER, EqualsIntsEvaluator.Factory::new),
        Map.entry(DataType.DOUBLE, EqualsDoublesEvaluator.Factory::new),
        Map.entry(DataType.LONG, EqualsLongsEvaluator.Factory::new),
        Map.entry(DataType.UNSIGNED_LONG, EqualsLongsEvaluator.Factory::new),
        Map.entry(DataType.DATETIME, EqualsLongsEvaluator.Factory::new),
        Map.entry(DataType.DATE_NANOS, EqualsLongsEvaluator.Factory::new),
        Map.entry(DataType.GEO_POINT, EqualsGeometriesEvaluator.Factory::new),
        Map.entry(DataType.CARTESIAN_POINT, EqualsGeometriesEvaluator.Factory::new),
        Map.entry(DataType.GEO_SHAPE, EqualsGeometriesEvaluator.Factory::new),
        Map.entry(DataType.CARTESIAN_SHAPE, EqualsGeometriesEvaluator.Factory::new),
        Map.entry(DataType.KEYWORD, EqualsKeywordsEvaluator.Factory::new),
        Map.entry(DataType.TEXT, EqualsKeywordsEvaluator.Factory::new),
        Map.entry(DataType.VERSION, EqualsKeywordsEvaluator.Factory::new),
        Map.entry(DataType.IP, EqualsKeywordsEvaluator.Factory::new)
    );

    @FunctionInfo(
        returnType = { "boolean" },
        description = "Check if two fields are equal. "
            + "If either field is <<esql-multivalued-fields,multivalued>> then the result is `null`.",
        note = "This is pushed to the underlying search index if one side of the comparison is constant "
            + "and the other side is a field in the index that has both an <<mapping-index>> and <<doc-values>>."
    )
    public Equals(
        Source source,
        @Param(
            name = "lhs",
            type = {
                "boolean",
                "cartesian_point",
                "cartesian_shape",
                "date",
                "double",
                "geo_point",
                "geo_shape",
                "integer",
                "ip",
                "keyword",
                "long",
                "text",
                "unsigned_long",
                "version" },
            description = "An expression."
        ) Expression left,
        @Param(
            name = "rhs",
            type = {
                "boolean",
                "cartesian_point",
                "cartesian_shape",
                "date",
                "double",
                "geo_point",
                "geo_shape",
                "integer",
                "ip",
                "keyword",
                "long",
                "text",
                "unsigned_long",
                "version" },
            description = "An expression."
        ) Expression right
    ) {
        super(source, left, right, BinaryComparisonOperation.EQ, evaluatorMap);
    }

    public Equals(Source source, Expression left, Expression right, ZoneId zoneId) {
        super(source, left, right, BinaryComparisonOperation.EQ, zoneId, evaluatorMap);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
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
