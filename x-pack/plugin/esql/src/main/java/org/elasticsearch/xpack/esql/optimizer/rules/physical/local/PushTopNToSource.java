/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeMap;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.BinarySpatialFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.SpatialRelatesUtils;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StDistance;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.function.BiFunction;

/**
 * We handle two main scenarios here:
 * <ol>
 *     <li>
 *         Queries like `FROM index | SORT field` will be pushed to the source if the field is an indexed field.
 *     </li>
 *     <li>
 *         Queries like `FROM index | EVAL ref = ... | SORT ref` will be pushed to the source if the reference function is pushable,
 *         which can happen under two conditions:
 *         <ul>
 *             <li>
 *                 The reference refers linearly to an indexed field.
 *                 For example: `FROM index | EVAL ref = field | SORT ref`
 *             </li>
 *             <li>
 *                 The reference refers to a distance function that refers to an indexed field and a constant expression.
 *                 For example `FROM index | EVAL distance = ST_DISTANCE(field, POINT(0, 0)) | SORT distance`.
 *                 As with the previous condition, both the attribute and the constant can be further aliased.
 *             </li>
 *         </ul>
 *     </li>
 *     <li>
 *     </li>
 * </ol>
 */
public class PushTopNToSource extends PhysicalOptimizerRules.ParameterizedOptimizerRule<TopNExec, LocalPhysicalOptimizerContext> {

    @Override
    protected PhysicalPlan rule(TopNExec topNExec, LocalPhysicalOptimizerContext ctx) {
        Pushable pushable = evaluatePushable(ctx.foldCtx(), topNExec, LucenePushdownPredicates.from(ctx.searchStats()));
        return pushable.rewrite(topNExec);
    }

    /**
     * Multiple scenarios for pushing down TopN to Lucene source. Each involve checking a combination of conditions and then
     * performing an associated rewrite specific to that scenario. This interface should be extended by each scenario, and
     * include the appropriate rewrite logic.
     */
    interface Pushable {
        PhysicalPlan rewrite(TopNExec topNExec);
    }

    private static final Pushable NO_OP = new NoOpPushable();

    record NoOpPushable() implements Pushable {
        public PhysicalPlan rewrite(TopNExec topNExec) {
            return topNExec;
        }
    }

    record PushableQueryExec(EsQueryExec queryExec) implements Pushable {
        public PhysicalPlan rewrite(TopNExec topNExec) {
            var sorts = buildFieldSorts(topNExec.order());
            var limit = topNExec.limit();
            return queryExec.withSorts(sorts).withLimit(limit);
        }
    }

    record PushableGeoDistance(FieldAttribute fieldAttribute, Order order, Point point) {
        private EsQueryExec.Sort sort() {
            return new EsQueryExec.GeoDistanceSort(fieldAttribute.exactAttribute(), order.direction(), point.getLat(), point.getLon());
        }

        private static PushableGeoDistance from(FoldContext ctx, StDistance distance, Order order) {
            if (distance.left() instanceof Attribute attr && distance.right().foldable()) {
                return from(ctx, attr, distance.right(), order);
            } else if (distance.right() instanceof Attribute attr && distance.left().foldable()) {
                return from(ctx, attr, distance.left(), order);
            }
            return null;
        }

        private static PushableGeoDistance from(FoldContext ctx, Attribute attr, Expression foldable, Order order) {
            if (attr instanceof FieldAttribute fieldAttribute) {
                Geometry geometry = SpatialRelatesUtils.makeGeometryFromLiteral(ctx, foldable);
                if (geometry instanceof Point point) {
                    return new PushableGeoDistance(fieldAttribute, order, point);
                }
            }
            return null;
        }
    }

    record PushableCompoundExec(EvalExec evalExec, EsQueryExec queryExec, List<EsQueryExec.Sort> pushableSorts) implements Pushable {
        public PhysicalPlan rewrite(TopNExec topNExec) {
            // We need to keep the EVAL in place because the coordinator will have its own TopNExec so we need to keep the distance
            return evalExec.replaceChild(queryExec.withSorts(pushableSorts).withLimit(topNExec.limit()));
        }
    }

    private static Pushable evaluatePushable(FoldContext ctx, TopNExec topNExec, LucenePushdownPredicates lucenePushdownPredicates) {
        PhysicalPlan child = topNExec.child();
        if (child instanceof EsQueryExec queryExec
            && queryExec.canPushSorts()
            && canPushDownOrders(topNExec.order(), lucenePushdownPredicates)) {
            // With the simplest case of `FROM index | SORT ...` we only allow pushing down if the sort is on a field
            return new PushableQueryExec(queryExec);
        }
        if (child instanceof EvalExec evalExec && evalExec.child() instanceof EsQueryExec queryExec && queryExec.canPushSorts()) {
            // When we have an EVAL between the FROM and the SORT, we consider pushing down if the sort is on a field and/or
            // a distance function defined in the EVAL. We also move the EVAL to after the SORT.
            List<Order> orders = topNExec.order();
            List<Alias> fields = evalExec.fields();
            LinkedHashMap<NameId, StDistance> distances = new LinkedHashMap<>();
            AttributeMap.Builder<Attribute> aliasReplacedByBuilder = AttributeMap.builder();
            fields.forEach(alias -> {
                // TODO: can we support CARTESIAN also?
                if (alias.child() instanceof StDistance distance && distance.crsType() == BinarySpatialFunction.SpatialCrsType.GEO) {
                    distances.put(alias.id(), distance);
                } else if (alias.child() instanceof Attribute attr) {
                    aliasReplacedByBuilder.put(alias.toAttribute(), attr.toAttribute());
                }
            });
            AttributeMap<Attribute> aliasReplacedBy = aliasReplacedByBuilder.build();

            List<EsQueryExec.Sort> pushableSorts = new ArrayList<>();
            for (Order order : orders) {
                if (lucenePushdownPredicates.isPushableFieldAttribute(order.child())) {
                    pushableSorts.add(
                        new EsQueryExec.FieldSort(
                            ((FieldAttribute) order.child()).exactAttribute(),
                            order.direction(),
                            order.nullsPosition()
                        )
                    );
                } else if (LucenePushdownPredicates.isPushableMetadataAttribute(order.child())) {
                    pushableSorts.add(new EsQueryExec.ScoreSort(order.direction()));
                } else if (order.child() instanceof ReferenceAttribute referenceAttribute) {
                    Attribute resolvedAttribute = aliasReplacedBy.resolve(referenceAttribute, referenceAttribute);
                    if (distances.containsKey(resolvedAttribute.id())) {
                        StDistance distance = distances.get(resolvedAttribute.id());
                        StDistance d = (StDistance) distance.transformDown(ReferenceAttribute.class, r -> aliasReplacedBy.resolve(r, r));
                        PushableGeoDistance pushableGeoDistance = PushableGeoDistance.from(ctx, d, order);
                        if (pushableGeoDistance != null) {
                            pushableSorts.add(pushableGeoDistance.sort());
                        } else {
                            // As soon as we see a non-pushable sort, we know we need a final SORT command
                            break;
                        }
                    } else if (aliasReplacedBy.resolve(referenceAttribute, referenceAttribute) instanceof FieldAttribute fieldAttribute
                        && lucenePushdownPredicates.isPushableFieldAttribute(fieldAttribute)) {
                            // If the SORT refers to a reference to a pushable field, we can push it down
                            pushableSorts.add(
                                new EsQueryExec.FieldSort(fieldAttribute.exactAttribute(), order.direction(), order.nullsPosition())
                            );
                        } else {
                            // If the SORT refers to a non-pushable reference function, the EVAL must remain before the SORT,
                            // and we can no longer push down anything
                            break;
                        }
                } else {
                    // As soon as we see a non-pushable sort, we know we need a final SORT command
                    break;
                }
            }
            if (pushableSorts.isEmpty() == false) {
                return new PushableCompoundExec(evalExec, queryExec, pushableSorts);
            }
        }
        return NO_OP;
    }

    private static boolean canPushDownOrders(List<Order> orders, LucenePushdownPredicates lucenePushdownPredicates) {
        // allow only exact FieldAttributes (no expressions) for sorting
        BiFunction<Expression, LucenePushdownPredicates, Boolean> isSortableAttribute = (exp, lpp) -> lpp.isPushableFieldAttribute(exp)
            // TODO: https://github.com/elastic/elasticsearch/issues/120219
            || (exp instanceof MetadataAttribute ma && MetadataAttribute.SCORE.equals(ma.name()));
        return orders.stream().allMatch(o -> isSortableAttribute.apply(o.child(), lucenePushdownPredicates));
    }

    private static List<EsQueryExec.Sort> buildFieldSorts(List<Order> orders) {
        List<EsQueryExec.Sort> sorts = new ArrayList<>(orders.size());
        for (Order o : orders) {
            if (o.child() instanceof FieldAttribute fa) {
                sorts.add(new EsQueryExec.FieldSort(fa.exactAttribute(), o.direction(), o.nullsPosition()));
            } else if (o.child() instanceof MetadataAttribute ma && MetadataAttribute.SCORE.equals(ma.name())) {
                sorts.add(new EsQueryExec.ScoreSort(o.direction()));
            } else {
                assert false : "unexpected ordering on expression type " + o.child().getClass();
            }
        }
        return sorts;
    }
}
