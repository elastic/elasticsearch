/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.geometry.Circle;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.utils.WellKnownBinary;
import org.elasticsearch.xpack.esql.capabilities.TranslationAware;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeMap;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.SpatialDisjoint;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.SpatialIntersects;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.SpatialRelatesUtils;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StDistance;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.EsqlBinaryComparison;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.capabilities.TranslationAware.translatable;
import static org.elasticsearch.xpack.esql.expression.predicate.Predicates.splitAnd;
import static org.elasticsearch.xpack.esql.optimizer.rules.physical.local.PushFiltersToSource.getAliasReplacedBy;

/**
 * When a spatial distance predicate can be pushed down to lucene, this is done by capturing the distance within the same function.
 * In principle this is like re-writing the predicate:
 * <pre>WHERE ST_DISTANCE(field, TO_GEOPOINT("POINT(0 0)")) &lt;= 10000</pre>
 * as:
 * <pre>WHERE ST_INTERSECTS(field, TO_GEOSHAPE("CIRCLE(0,0,10000)"))</pre>.
 * <p>
 * In addition, since the distance could be calculated in a preceding <code>EVAL</code> command, we also need to consider the case:
 * <pre>
 *     FROM index
 *     | EVAL distance = ST_DISTANCE(field, TO_GEOPOINT("POINT(0 0)"))
 *     | WHERE distance &lt;= 10000
 * </pre>
 * And re-write that as:
 * <pre>
 *     FROM index
 *     | WHERE ST_INTERSECTS(field, TO_GEOSHAPE("CIRCLE(0,0,10000)"))
 *     | EVAL distance = ST_DISTANCE(field, TO_GEOPOINT("POINT(0 0)"))
 * </pre>
 * Note that the WHERE clause is both rewritten to an intersection and pushed down closer to the <code>EsQueryExec</code>,
 * which allows the predicate to be pushed down to Lucene in a later rule, <code>PushFiltersToSource</code>.
 */
public class EnableSpatialDistancePushdown extends PhysicalOptimizerRules.ParameterizedOptimizerRule<
    FilterExec,
    LocalPhysicalOptimizerContext> {

    @Override
    protected PhysicalPlan rule(FilterExec filterExec, LocalPhysicalOptimizerContext ctx) {
        PhysicalPlan plan = filterExec;
        if (filterExec.child() instanceof EsQueryExec esQueryExec) {
            plan = rewrite(ctx.foldCtx(), filterExec, esQueryExec, LucenePushdownPredicates.from(ctx.searchStats()));
        } else if (filterExec.child() instanceof EvalExec evalExec && evalExec.child() instanceof EsQueryExec esQueryExec) {
            plan = rewriteBySplittingFilter(
                ctx.foldCtx(),
                filterExec,
                evalExec,
                esQueryExec,
                LucenePushdownPredicates.from(ctx.searchStats())
            );
        }

        return plan;
    }

    private FilterExec rewrite(
        FoldContext ctx,
        FilterExec filterExec,
        EsQueryExec esQueryExec,
        LucenePushdownPredicates lucenePushdownPredicates
    ) {
        // Find and rewrite any binary comparisons that involve a distance function and a literal
        var rewritten = filterExec.condition().transformDown(EsqlBinaryComparison.class, comparison -> {
            ComparisonType comparisonType = ComparisonType.from(comparison.getFunctionType());
            if (comparison.left() instanceof StDistance dist && comparison.right().foldable()) {
                return rewriteComparison(ctx, comparison, dist, comparison.right(), comparisonType);
            } else if (comparison.right() instanceof StDistance dist && comparison.left().foldable()) {
                return rewriteComparison(ctx, comparison, dist, comparison.left(), ComparisonType.invert(comparisonType));
            }
            return comparison;
        });
        if (rewritten.equals(filterExec.condition()) == false
            && translatable(rewritten, lucenePushdownPredicates).finish() == TranslationAware.FinishedTranslatable.YES) {
            return new FilterExec(filterExec.source(), esQueryExec, rewritten);
        }
        return filterExec;
    }

    /**
     * This version of the rewrite will try to split the filter into two parts, one that can be pushed down to the source and
     * one that needs to be kept after the EVAL.
     * For example:
     * <pre>
     *     FROM index
     *     | EVAL distance = ST_DISTANCE(field, TO_GEOPOINT("POINT(0 0)")), other = scale * 2
     *     | WHERE distance &lt;= 10000 AND distance &gt; 5000 AND other &gt; 10
     * </pre>
     * Should be rewritten as:
     * <pre>
     *     FROM index
     *     | WHERE ST_INTERSECTS(field, TO_GEOSHAPE("CIRCLE(0,0,10000)"))
     *         AND ST_DISJOINT(field, TO_GEOSHAPE("CIRCLE(0,0,5000)"))
     *     | EVAL distance = ST_DISTANCE(field, TO_GEOPOINT("POINT(0 0)")), other = scale * 2
     *     | WHERE other &gt; 10
     * </pre>
     */
    private PhysicalPlan rewriteBySplittingFilter(
        FoldContext ctx,
        FilterExec filterExec,
        EvalExec evalExec,
        EsQueryExec esQueryExec,
        LucenePushdownPredicates lucenePushdownPredicates
    ) {
        // Find all pushable distance functions in the EVAL
        Map<NameId, StDistance> distances = getPushableDistances(evalExec.fields(), lucenePushdownPredicates);

        // Don't do anything if there are no distances to push down
        if (distances.isEmpty()) {
            return filterExec;
        }

        // Process the EVAL to get all aliases that might be needed in the filter rewrite
        AttributeMap<Attribute> aliasReplacedBy = getAliasReplacedBy(evalExec);

        // First we split the filter into multiple AND'd expressions, and then we evaluate each individually for distance rewrites
        List<Expression> pushable = new ArrayList<>();
        List<Expression> nonPushable = new ArrayList<>();
        for (Expression exp : splitAnd(filterExec.condition())) {
            Expression resExp = exp.transformUp(ReferenceAttribute.class, r -> aliasReplacedBy.resolve(r, r));
            // Find and rewrite any binary comparisons that involve a distance function and a literal
            var rewritten = rewriteDistanceFilters(ctx, resExp, distances);
            // If all pushable StDistance functions were found and re-written, we need to re-write the FILTER/EVAL combination
            if (rewritten.equals(resExp) == false
                && translatable(rewritten, lucenePushdownPredicates).finish() == TranslationAware.FinishedTranslatable.YES) {
                pushable.add(rewritten);
            } else {
                nonPushable.add(exp);
            }
        }

        // If nothing pushable was rewritten, we can return the original filter
        if (pushable.isEmpty()) {
            return filterExec;
        }

        // Move the rewritten pushable filters below the EVAL
        var distanceFilter = new FilterExec(filterExec.source(), esQueryExec, Predicates.combineAnd(pushable));
        var newEval = new EvalExec(evalExec.source(), distanceFilter, evalExec.fields());
        if (nonPushable.isEmpty()) {
            // No other filters found, we can just return the original eval with the new filter as child
            return newEval;
        } else {
            // Some other filters found, we need to return two filters with the eval in between
            return new FilterExec(filterExec.source(), newEval, Predicates.combineAnd(nonPushable));
        }
    }

    private Map<NameId, StDistance> getPushableDistances(List<Alias> aliases, LucenePushdownPredicates lucenePushdownPredicates) {
        Map<NameId, StDistance> distances = new LinkedHashMap<>();
        aliases.forEach(alias -> {
            if (alias.child() instanceof StDistance distance
                && distance.translatable(lucenePushdownPredicates).finish() == TranslationAware.FinishedTranslatable.YES) {
                distances.put(alias.id(), distance);
            } else if (alias.child() instanceof ReferenceAttribute ref && distances.containsKey(ref.id())) {
                StDistance distance = distances.get(ref.id());
                distances.put(alias.id(), distance);
            }
        });
        return distances;
    }

    private Expression rewriteDistanceFilters(FoldContext ctx, Expression expr, Map<NameId, StDistance> distances) {
        return expr.transformDown(EsqlBinaryComparison.class, comparison -> {
            ComparisonType comparisonType = ComparisonType.from(comparison.getFunctionType());
            if (comparison.left() instanceof ReferenceAttribute r && distances.containsKey(r.id()) && comparison.right().foldable()) {
                StDistance dist = distances.get(r.id());
                return rewriteComparison(ctx, comparison, dist, comparison.right(), comparisonType);
            } else if (comparison.right() instanceof ReferenceAttribute r
                && distances.containsKey(r.id())
                && comparison.left().foldable()) {
                    StDistance dist = distances.get(r.id());
                    return rewriteComparison(ctx, comparison, dist, comparison.left(), ComparisonType.invert(comparisonType));
                }
            return comparison;
        });
    }

    private Expression rewriteComparison(
        FoldContext ctx,
        EsqlBinaryComparison comparison,
        StDistance dist,
        Expression literal,
        ComparisonType comparisonType
    ) {
        Object value = literal.fold(ctx);
        if (value instanceof Number number) {
            if (dist.right().foldable()) {
                return rewriteDistanceFilter(ctx, comparison, dist.left(), dist.right(), number, comparisonType);
            } else if (dist.left().foldable()) {
                return rewriteDistanceFilter(ctx, comparison, dist.right(), dist.left(), number, comparisonType);
            }
        }
        return comparison;
    }

    private Expression rewriteDistanceFilter(
        FoldContext ctx,
        EsqlBinaryComparison comparison,
        Expression spatialExp,
        Expression literalExp,
        Number number,
        ComparisonType comparisonType
    ) {
        DataType shapeDataType = getShapeDataType(spatialExp);
        Geometry geometry = SpatialRelatesUtils.makeGeometryFromLiteral(ctx, literalExp);
        if (geometry instanceof Point point) {
            double distance = number.doubleValue();
            Source source = comparison.source();
            if (comparisonType.lt) {
                distance = comparisonType.eq ? distance : Math.nextDown(distance);
                return new SpatialIntersects(source, spatialExp, makeCircleLiteral(point, distance, literalExp, shapeDataType));
            } else if (comparisonType.gt) {
                distance = comparisonType.eq ? distance : Math.nextUp(distance);
                return new SpatialDisjoint(source, spatialExp, makeCircleLiteral(point, distance, literalExp, shapeDataType));
            } else if (comparisonType.eq) {
                return new And(
                    source,
                    new SpatialIntersects(source, spatialExp, makeCircleLiteral(point, distance, literalExp, shapeDataType)),
                    new SpatialDisjoint(source, spatialExp, makeCircleLiteral(point, Math.nextDown(distance), literalExp, shapeDataType))
                );
            }
        }
        return comparison;
    }

    private Literal makeCircleLiteral(Point point, double distance, Expression literalExpression, DataType shapeDataType) {
        var circle = new Circle(point.getX(), point.getY(), distance);
        var wkb = WellKnownBinary.toWKB(circle, ByteOrder.LITTLE_ENDIAN);
        return new Literal(literalExpression.source(), new BytesRef(wkb), shapeDataType);
    }

    private DataType getShapeDataType(Expression expression) {
        return switch (expression.dataType()) {
            case GEO_POINT, GEO_SHAPE -> DataType.GEO_SHAPE;
            case CARTESIAN_POINT, CARTESIAN_SHAPE -> DataType.CARTESIAN_SHAPE;
            default -> throw new IllegalArgumentException("Unsupported spatial data type: " + expression.dataType());
        };
    }

    /**
     * This enum captures the key differences between various inequalities as perceived from the spatial distance function.
     * In particular, we need to know which direction the inequality points, with lt=true meaning the left is expected to be smaller
     * than the right. And eq=true meaning we expect equality as well. We currently don't support Equals and NotEquals, so the third
     * field disables those.
     */
    enum ComparisonType {
        LTE(true, false, true),
        LT(true, false, false),
        GTE(false, true, true),
        GT(false, true, false),
        EQ(false, false, true);

        private final boolean lt;
        private final boolean gt;
        private final boolean eq;

        ComparisonType(boolean lt, boolean gt, boolean eq) {
            this.lt = lt;
            this.gt = gt;
            this.eq = eq;
        }

        static ComparisonType from(EsqlBinaryComparison.BinaryComparisonOperation op) {
            return switch (op) {
                case LT -> LT;
                case LTE -> LTE;
                case GT -> GT;
                case GTE -> GTE;
                default -> EQ;
            };
        }

        static ComparisonType invert(ComparisonType comparisonType) {
            return switch (comparisonType) {
                case LT -> GT;
                case LTE -> GTE;
                case GT -> LT;
                case GTE -> LTE;
                default -> EQ;
            };
        }
    }
}
