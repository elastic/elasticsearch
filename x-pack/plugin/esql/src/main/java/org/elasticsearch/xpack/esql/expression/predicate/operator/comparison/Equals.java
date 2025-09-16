/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.predicate.operator.comparison;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.predicate.Negatable;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.EsqlArithmeticOperation;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;
import org.elasticsearch.xpack.esql.planner.TranslatorHandler;
import org.elasticsearch.xpack.esql.querydsl.query.EqualsSyntheticSourceDelegate;
import org.elasticsearch.xpack.esql.querydsl.query.SingleValueQuery;

import java.time.ZoneId;
import java.util.Map;

import static org.elasticsearch.xpack.esql.core.type.AtomType.BOOLEAN;
import static org.elasticsearch.xpack.esql.core.type.AtomType.CARTESIAN_POINT;
import static org.elasticsearch.xpack.esql.core.type.AtomType.CARTESIAN_SHAPE;
import static org.elasticsearch.xpack.esql.core.type.AtomType.DATETIME;
import static org.elasticsearch.xpack.esql.core.type.AtomType.DATE_NANOS;
import static org.elasticsearch.xpack.esql.core.type.AtomType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.AtomType.GEOHASH;
import static org.elasticsearch.xpack.esql.core.type.AtomType.GEOHEX;
import static org.elasticsearch.xpack.esql.core.type.AtomType.GEOTILE;
import static org.elasticsearch.xpack.esql.core.type.AtomType.GEO_POINT;
import static org.elasticsearch.xpack.esql.core.type.AtomType.GEO_SHAPE;
import static org.elasticsearch.xpack.esql.core.type.AtomType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.AtomType.IP;
import static org.elasticsearch.xpack.esql.core.type.AtomType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.AtomType.LONG;
import static org.elasticsearch.xpack.esql.core.type.AtomType.TEXT;
import static org.elasticsearch.xpack.esql.core.type.AtomType.UNSIGNED_LONG;
import static org.elasticsearch.xpack.esql.core.type.AtomType.VERSION;

public class Equals extends EsqlBinaryComparison implements Negatable<EsqlBinaryComparison> {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "Equals",
        EsqlBinaryComparison::readFrom
    );

    private static final Map<DataType, EsqlArithmeticOperation.BinaryEvaluator> evaluatorMap = Map.ofEntries(
        Map.entry(BOOLEAN.type(), EqualsBoolsEvaluator.Factory::new),
        Map.entry(INTEGER.type(), EqualsIntsEvaluator.Factory::new),
        Map.entry(DOUBLE.type(), EqualsDoublesEvaluator.Factory::new),
        Map.entry(LONG.type(), EqualsLongsEvaluator.Factory::new),
        Map.entry(UNSIGNED_LONG.type(), EqualsLongsEvaluator.Factory::new),
        Map.entry(DATETIME.type(), EqualsLongsEvaluator.Factory::new),
        Map.entry(DATE_NANOS.type(), EqualsLongsEvaluator.Factory::new),
        Map.entry(GEO_POINT.type(), EqualsGeometriesEvaluator.Factory::new),
        Map.entry(CARTESIAN_POINT.type(), EqualsGeometriesEvaluator.Factory::new),
        Map.entry(GEO_SHAPE.type(), EqualsGeometriesEvaluator.Factory::new),
        Map.entry(CARTESIAN_SHAPE.type(), EqualsGeometriesEvaluator.Factory::new),
        Map.entry(GEOHASH.type(), EqualsLongsEvaluator.Factory::new),
        Map.entry(GEOTILE.type(), EqualsLongsEvaluator.Factory::new),
        Map.entry(GEOHEX.type(), EqualsLongsEvaluator.Factory::new),
        Map.entry(KEYWORD.type(), EqualsKeywordsEvaluator.Factory::new),
        Map.entry(TEXT.type(), EqualsKeywordsEvaluator.Factory::new),
        Map.entry(VERSION.type(), EqualsKeywordsEvaluator.Factory::new),
        Map.entry(IP.type(), EqualsKeywordsEvaluator.Factory::new)
    );

    @FunctionInfo(
        operator = "==",
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
                "geohash",
                "geotile",
                "geohex",
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
                "geohash",
                "geotile",
                "geohex",
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
        super(
            source,
            left,
            right,
            BinaryComparisonOperation.EQ,
            evaluatorMap,
            EqualsNanosMillisEvaluator.Factory::new,
            EqualsMillisNanosEvaluator.Factory::new
        );
    }

    public Equals(Source source, Expression left, Expression right, ZoneId zoneId) {
        super(
            source,
            left,
            right,
            BinaryComparisonOperation.EQ,
            zoneId,
            evaluatorMap,
            EqualsNanosMillisEvaluator.Factory::new,
            EqualsMillisNanosEvaluator.Factory::new
        );
    }

    @Override
    public Translatable translatable(LucenePushdownPredicates pushdownPredicates) {
        if (right() instanceof Literal lit) {
            if (left().dataType().atom() == TEXT && left() instanceof FieldAttribute fa) {
                if (pushdownPredicates.canUseEqualityOnSyntheticSourceDelegate(fa, ((BytesRef) lit.value()).utf8ToString())) {
                    return Translatable.YES_BUT_RECHECK_NEGATED;
                }
            }
        }
        return super.translatable(pushdownPredicates);
    }

    @Override
    public Query asQuery(LucenePushdownPredicates pushdownPredicates, TranslatorHandler handler) {
        if (right() instanceof Literal lit) {
            if (left().dataType().atom() == TEXT && left() instanceof FieldAttribute fa) {
                String value = ((BytesRef) lit.value()).utf8ToString();
                if (pushdownPredicates.canUseEqualityOnSyntheticSourceDelegate(fa, value)) {
                    String name = handler.nameOf(fa);
                    return new SingleValueQuery(new EqualsSyntheticSourceDelegate(source(), name, value), name, true);
                }
            }
        }
        return super.asQuery(pushdownPredicates, handler);
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

    @Evaluator(extraName = "MillisNanos")
    static boolean processMillisNanos(long lhs, long rhs) {
        return DateUtils.compareNanosToMillis(rhs, lhs) == 0;
    }

    @Evaluator(extraName = "NanosMillis")
    static boolean processNanosMillis(long lhs, long rhs) {
        return DateUtils.compareNanosToMillis(lhs, rhs) == 0;
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
