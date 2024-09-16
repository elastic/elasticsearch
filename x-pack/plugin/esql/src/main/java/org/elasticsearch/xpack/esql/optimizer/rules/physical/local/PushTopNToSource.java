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
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.BinarySpatialFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.SpatialRelatesUtils;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StDistance;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

public class PushTopNToSource extends PhysicalOptimizerRules.ParameterizedOptimizerRule<TopNExec, LocalPhysicalOptimizerContext> {
    @Override
    protected PhysicalPlan rule(TopNExec topNExec, LocalPhysicalOptimizerContext ctx) {
        Pushable pushable = evaluatePushable(topNExec, x -> LucenePushDownUtils.hasIdenticalDelegate(x, ctx.searchStats()));
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

    private static Pushable NO_OP = new NoOpPushable();

    record NoOpPushable() implements Pushable {
        public PhysicalPlan rewrite(TopNExec topNExec) {
            return topNExec;
        }
    }

    record PushableExchangeExec(ExchangeExec exchangeExec, EsQueryExec queryExec) implements Pushable {
        public PhysicalPlan rewrite(TopNExec topNExec) {
            var sorts = buildFieldSorts(topNExec.order());
            var limit = topNExec.limit();
            return exchangeExec.replaceChild(queryExec.withSorts(sorts).withLimit(limit));
        }
    }

    record PushableQueryExec(EsQueryExec queryExec) implements Pushable {
        public PhysicalPlan rewrite(TopNExec topNExec) {
            var sorts = buildFieldSorts(topNExec.order());
            var limit = topNExec.limit();
            return queryExec.withSorts(sorts).withLimit(limit);
        }
    }

    record PushableGeoDistance(EvalExec evalExec, EsQueryExec queryExec, FieldAttribute fieldAttribute, Order order, Point point)
        implements
            Pushable {
        public PhysicalPlan rewrite(TopNExec topNExec) {
            EsQueryExec.GeoDistanceSort distanceSort = new EsQueryExec.GeoDistanceSort(
                fieldAttribute.exactAttribute(),
                order.direction(),
                point.getLat(),
                point.getLon()
            );
            return evalExec.replaceChild(queryExec.withSorts(List.of(distanceSort)).withLimit(topNExec.limit()));
        }
    }

    private static Pushable evaluatePushable(TopNExec topNExec, Predicate<FieldAttribute> hasIdenticalDelegate) {
        PhysicalPlan child = topNExec.child();
        if (child instanceof EsQueryExec queryExec
            && queryExec.canPushSorts()
            && canPushDownOrders(topNExec.order(), hasIdenticalDelegate)) {
            return new PushableQueryExec(queryExec);
        }
        if (child instanceof ExchangeExec exchangeExec
            && exchangeExec.child() instanceof EsQueryExec queryExec
            && queryExec.canPushSorts()
            && canPushDownOrders(topNExec.order(), hasIdenticalDelegate)) {
            return new PushableExchangeExec(exchangeExec, queryExec);
        }
        if (child instanceof EvalExec evalExec && evalExec.child() instanceof EsQueryExec queryExec) {
            List<Order> orders = topNExec.order();
            List<Alias> fields = evalExec.fields();
            // TODO: allow sorting distance together with other fields
            if (orders.size() == 1 && orders.get(0).child() instanceof ReferenceAttribute referenceAttribute) {
                for (Alias field : fields) {
                    if (field.child() instanceof StDistance distance
                        && distance.crsType() == BinarySpatialFunction.SpatialCrsType.GEO
                        && field.id().equals(referenceAttribute.id())) {
                        if (distance.left() instanceof FieldAttribute fieldAttribute && distance.right().foldable()) {
                            Geometry geometry = SpatialRelatesUtils.makeGeometryFromLiteral(distance.right());
                            if (geometry instanceof Point point) {
                                return new PushableGeoDistance(evalExec, queryExec, fieldAttribute, orders.getFirst(), point);
                            }
                        } else if (distance.right() instanceof FieldAttribute fieldAttribute && distance.left().foldable()) {
                            Geometry geometry = SpatialRelatesUtils.makeGeometryFromLiteral(distance.left());
                            if (geometry instanceof Point point) {
                                return new PushableGeoDistance(evalExec, queryExec, fieldAttribute, orders.getFirst(), point);
                            }
                        }
                    }
                }
            }
        }
        return NO_OP;
    }

    private static boolean canPushDownOrders(List<Order> orders, Predicate<FieldAttribute> hasIdenticalDelegate) {
        // allow only exact FieldAttributes (no expressions) for sorting
        return orders.stream().allMatch(o -> LucenePushDownUtils.isPushableFieldAttribute(o.child(), hasIdenticalDelegate));
    }

    private static List<EsQueryExec.Sort> buildFieldSorts(List<Order> orders) {
        List<EsQueryExec.Sort> sorts = new ArrayList<>(orders.size());
        for (Order o : orders) {
            sorts.add(new EsQueryExec.FieldSort(((FieldAttribute) o.child()).exactAttribute(), o.direction(), o.nullsPosition()));
        }
        return sorts;
    }
}
