/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner.mapper;

import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.BinaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.ChangePoint;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LeafPlan;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinConfig;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes;
import org.elasticsearch.xpack.esql.plan.physical.ChangePointExec;
import org.elasticsearch.xpack.esql.plan.physical.EsSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.HashJoinExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitExec;
import org.elasticsearch.xpack.esql.plan.physical.LocalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.LookupJoinExec;
import org.elasticsearch.xpack.esql.plan.physical.OrderExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;

import java.util.List;

/**
 * <p>Maps a (local) logical plan into a (local) physical plan. This class is the equivalent of {@link Mapper} but for data nodes.
 *
 */
public class LocalMapper {

    public PhysicalPlan map(LogicalPlan p) {

        if (p instanceof LeafPlan leaf) {
            return mapLeaf(leaf);
        }

        if (p instanceof UnaryPlan unary) {
            return mapUnary(unary);
        }

        if (p instanceof BinaryPlan binary) {
            return mapBinary(binary);
        }

        return MapperUtils.unsupported(p);
    }

    private PhysicalPlan mapLeaf(LeafPlan leaf) {
        if (leaf instanceof EsRelation esRelation) {
            return new EsSourceExec(esRelation);
        }

        return MapperUtils.mapLeaf(leaf);
    }

    private PhysicalPlan mapUnary(UnaryPlan unary) {
        PhysicalPlan mappedChild = map(unary.child());

        //
        // Pipeline breakers
        //

        if (unary instanceof Aggregate aggregate) {
            List<Attribute> intermediate = MapperUtils.intermediateAttributes(aggregate);
            return MapperUtils.aggExec(aggregate, mappedChild, AggregatorMode.INITIAL, intermediate);
        }

        if (unary instanceof Limit limit) {
            return new LimitExec(limit.source(), mappedChild, limit.limit());
        }

        if (unary instanceof OrderBy o) {
            return new OrderExec(o.source(), mappedChild, o.order());
        }

        if (unary instanceof TopN topN) {
            return new TopNExec(topN.source(), mappedChild, topN.order(), topN.limit(), null);
        }

        if (unary instanceof ChangePoint changePoint) {
            // TODO: ChangePoint shouldn't run on the local node
            // TODO: fix hardcoded 1000
            mappedChild = new TopNExec(changePoint.source(), mappedChild,
                List.of(new Order(changePoint.source(), changePoint.key(), Order.OrderDirection.ASC, Order.NullsPosition.ANY)),
                new Literal(Source.EMPTY, 1000, DataType.INTEGER), null);
            return new ChangePointExec(
                changePoint.source(),
                mappedChild,
                changePoint.value(),
                changePoint.key(),
                changePoint.targetType(),
                changePoint.targetPvalue()
            );
        }

        //
        // Pipeline operators
        //

        return MapperUtils.mapUnary(unary, mappedChild);
    }

    private PhysicalPlan mapBinary(BinaryPlan binary) {
        // special handling for inlinejoin - join + subquery which has to be executed first (async) and replaced by its result
        if (binary instanceof Join join) {
            JoinConfig config = join.config();
            if (config.type() != JoinTypes.LEFT) {
                throw new EsqlIllegalArgumentException("unsupported join type [" + config.type() + "]");
            }

            PhysicalPlan left = map(binary.left());
            PhysicalPlan right = map(binary.right());

            // if the right is data we can use a hash join directly
            if (right instanceof LocalSourceExec localData) {
                return new HashJoinExec(
                    join.source(),
                    left,
                    localData,
                    config.matchFields(),
                    config.leftFields(),
                    config.rightFields(),
                    join.output()
                );
            }
            if (right instanceof EsSourceExec source && source.indexMode() == IndexMode.LOOKUP) {
                return new LookupJoinExec(join.source(), left, right, config.leftFields(), config.rightFields(), join.rightOutputFields());
            }
        }

        return MapperUtils.unsupported(binary);
    }
}
