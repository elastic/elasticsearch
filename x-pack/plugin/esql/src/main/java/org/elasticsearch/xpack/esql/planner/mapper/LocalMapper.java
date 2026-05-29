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
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.BinaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.ExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LeafPlan;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LimitBy;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MetricsInfo;
import org.elasticsearch.xpack.esql.plan.logical.ParameterizedQuery;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.TopNBy;
import org.elasticsearch.xpack.esql.plan.logical.TsInfo;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinConfig;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.physical.EsSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.HashJoinExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitByExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitExec;
import org.elasticsearch.xpack.esql.plan.physical.LocalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.LookupJoinExec;
import org.elasticsearch.xpack.esql.plan.physical.MetricsInfoExec;
import org.elasticsearch.xpack.esql.plan.physical.ParameterizedQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.TopNByExec;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;
import org.elasticsearch.xpack.esql.plan.physical.TsInfoExec;

import java.util.List;

/**
 * <p>Maps a (local) logical plan into a (local) physical plan. This class is the equivalent of {@link Mapper} but for data nodes.
 *
 */
public class LocalMapper {

    public static LocalMapper INSTANCE = new LocalMapper();

    /**
     * Thread-local pragmas used while planning. Set by the call site that invokes
     * {@link #map(LogicalPlan)} so the planner can read per-query opt-in flags like
     * {@code skip_final_aggregation} and {@code data_driver_topn_limit} without
     * threading {@code Configuration} through every method signature.
     */
    private static final ThreadLocal<org.elasticsearch.xpack.esql.plugin.QueryPragmas> PRAGMAS = ThreadLocal.withInitial(
        () -> org.elasticsearch.xpack.esql.plugin.QueryPragmas.EMPTY
    );

    public static org.elasticsearch.xpack.esql.plugin.QueryPragmas currentPragmas() {
        return PRAGMAS.get();
    }

    public static void setPragmas(org.elasticsearch.xpack.esql.plugin.QueryPragmas pragmas) {
        PRAGMAS.set(pragmas == null ? org.elasticsearch.xpack.esql.plugin.QueryPragmas.EMPTY : pragmas);
    }

    public static void clearPragmas() {
        PRAGMAS.remove();
    }

    private LocalMapper() {}

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

        if (leaf instanceof ParameterizedQuery pq) {
            return new ParameterizedQueryExec(pq.source(), pq.output(), pq.matchFields(), pq.joinOnConditions(), null, pq.emptyResult());
        }

        if (leaf instanceof ExternalRelation external) {
            return external.toPhysicalExec();
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
            org.elasticsearch.xpack.esql.plugin.QueryPragmas pragmas = currentPragmas();
            // Pragmas don't reliably propagate to data-driver threads yet, so fall back to JVM-flag.
            boolean skipFinal = pragmas.skipFinalAggregation() || Boolean.getBoolean("esql.skip_final_agg");
            // EXPERIMENTAL: data_driver_topn_limit pushes a TopN(K) into the data driver.
            // To keep results correct when the same group key spans drivers, we run INITIAL
            // (intermediate-state output) so the coordinator's FINAL can merge cross-driver
            // partials BEFORE the global TopN. Setting skip_final_agg still uses SINGLE mode
            // (final output, no FINAL stage) and is only correct when keys are in one driver.
            int topNLimit = pragmas.dataDriverTopNLimit();
            if (topNLimit == 0) {
                topNLimit = Integer.getInteger("esql.data_driver_topn_limit", 0);
            }
            AggregatorMode mode = skipFinal ? AggregatorMode.SINGLE : AggregatorMode.INITIAL;
            PhysicalPlan aggExec = MapperUtils.aggExec(aggregate, mappedChild, mode, intermediate);

            if (topNLimit > 0 && aggregate.aggregates().isEmpty() == false) {
                // For SINGLE-mode output, sort by the final aggregate attribute (e.g. `c`).
                // For INITIAL-mode output, sort by the corresponding intermediate count/sum
                // attribute (the first intermediate attr; FINAL agg later merges partial counts).
                Attribute sortAttr;
                if (mode == AggregatorMode.SINGLE) {
                    sortAttr = aggregate.aggregates().get(0).toAttribute();
                } else {
                    // intermediate layout puts groupings FIRST, then per-agg state attrs.
                    // For the first aggregate `c = COUNT(*)`, its partial-count attr lives at
                    // index numGroupings (e.g. for GROUP BY WatchID, ClientIP -> index 2).
                    int firstAggStateIdx = aggregate.groupings().size();
                    sortAttr = intermediate.get(firstAggStateIdx);
                }
                Order order = new Order(aggregate.source(), sortAttr, Order.OrderDirection.DESC, Order.NullsPosition.FIRST);
                return new TopNExec(
                    aggregate.source(),
                    aggExec,
                    List.of(order),
                    new Literal(aggregate.source(), topNLimit, DataType.INTEGER),
                    null
                );
            }
            return aggExec;
        }

        if (unary instanceof Limit limit) {
            return new LimitExec(limit.source(), mappedChild, limit.limit(), null);
        }

        if (unary instanceof LimitBy limitBy) {
            return new LimitByExec(limitBy.source(), mappedChild, limitBy.limitPerGroup(), limitBy.groupings(), null);
        }

        if (unary instanceof TopN topN) {
            return new TopNExec(topN.source(), mappedChild, topN.order(), topN.limit(), null);
        }

        if (unary instanceof TopNBy topNBy) {
            return new TopNByExec(topNBy.source(), mappedChild, topNBy.order(), topNBy.limitPerGroup(), topNBy.groupings(), null);
        }

        if (unary instanceof MetricsInfo metricsInfo) {
            return new MetricsInfoExec(
                metricsInfo.source(),
                mappedChild,
                metricsInfo.output(),
                metricsInfo.output(),
                MetricsInfoExec.Mode.INITIAL
            );
        }

        if (unary instanceof TsInfo tsInfo) {
            return new TsInfoExec(tsInfo.source(), mappedChild, tsInfo.output(), tsInfo.output(), TsInfoExec.Mode.INITIAL);
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
            // if the right is data we can use a hash join directly
            if (binary.right() instanceof LocalRelation) {
                PhysicalPlan right = map(binary.right());
                if (right instanceof LocalSourceExec localData) {
                    return new HashJoinExec(
                        join.source(),
                        left,
                        localData,
                        config.leftFields(),
                        config.rightFields(),
                        join.rightOutputFields()
                    );
                } else {
                    throw new EsqlIllegalArgumentException("Unsupported right plan for join [" + binary.right().nodeName() + "]");
                }
            }
            EsRelation rightRelation = null;
            if (binary.right() instanceof EsRelation esRelation) {
                rightRelation = esRelation;
            } else if (binary.right() instanceof Filter filter && filter.child() instanceof EsRelation esRelation) {
                rightRelation = esRelation;
            }
            if (rightRelation == null) {
                throw new EsqlIllegalArgumentException("Unsupported right plan for lookup join [" + binary.right().nodeName() + "]");
            }
            if (rightRelation.indexMode() != IndexMode.LOOKUP) {
                throw new EsqlIllegalArgumentException(
                    "To perform a lookup join with index [" + rightRelation.indexPattern() + "], it must be a in lookup index mode"
                );
            }
            // we want to do local physical planning on the lookup node eventually for the right side of the lookup join
            // so here we will wrap the logical plan with a FragmentExec and keep it as is
            FragmentExec fragmentExec = new FragmentExec(binary.right());
            return new LookupJoinExec(
                join.source(),
                left,
                fragmentExec,
                config.leftFields(),
                config.rightFields(),
                join.rightOutputFields(),
                config.joinOnConditions()
            );
        }
        return MapperUtils.unsupported(binary);
    }

}
