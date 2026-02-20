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
import org.elasticsearch.xpack.esql.expression.function.grouping.GroupingFunction;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.BinaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.LeafPlan;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MetricsInfo;
import org.elasticsearch.xpack.esql.plan.logical.PipelineBreaker;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinConfig;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.HashJoinExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitExec;
import org.elasticsearch.xpack.esql.plan.physical.LocalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.LookupJoinExec;
import org.elasticsearch.xpack.esql.plan.physical.MergeExec;
import org.elasticsearch.xpack.esql.plan.physical.MetricsInfoExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;
import org.elasticsearch.xpack.esql.session.Versioned;

import java.util.ArrayList;
import java.util.List;

/**
 * <p>This class is part of the planner</p>
 *
 * <p>Translates the logical plan into a physical plan.  This is where we start to decide what will be executed on the data nodes and what
 * will be executed on the coordinator nodes.  This step creates {@link org.elasticsearch.xpack.esql.plan.physical.FragmentExec} instances,
 * which represent logical plan fragments to be sent to the data nodes and {@link org.elasticsearch.xpack.esql.plan.physical.ExchangeExec}
 * instances, which represent data being sent back from the data nodes to the coordinating node.</p>
 */
public class Mapper {

    public PhysicalPlan map(Versioned<LogicalPlan> versionedPlan) {
        // We ignore the version for now, but it's fine to use later for plans that work
        // differently from some version and up.
        return mapInner(versionedPlan.inner());
    }

    private PhysicalPlan mapInner(LogicalPlan p) {
        if (p instanceof LeafPlan leaf) {
            return mapLeaf(leaf);
        }

        if (p instanceof UnaryPlan unary) {
            return mapUnary(unary);
        }

        if (p instanceof BinaryPlan binary) {
            return mapBinary(binary);
        }

        if (p instanceof Fork fork) {
            return mapFork(fork);
        }

        return MapperUtils.unsupported(p);
    }

    private PhysicalPlan mapLeaf(LeafPlan leaf) {
        if (leaf instanceof EsRelation esRelation) {
            return new FragmentExec(esRelation);
        }

        // ExternalRelation is handled by MapperUtils.mapLeaf()
        // which calls toPhysicalExec() to create coordinator-only source operators
        return MapperUtils.mapLeaf(leaf);
    }

    private PhysicalPlan mapUnary(UnaryPlan unary) {
        PhysicalPlan mappedChild = mapInner(unary.child());

        if (mappedChild instanceof FragmentExec) {
            // COORDINATOR enrich must not be included to the fragment as it has to be executed on the coordinating node
            if (unary instanceof Enrich enrich && enrich.mode() == Enrich.Mode.COORDINATOR) {
                mappedChild = addExchangeForFragment(enrich.child(), mappedChild);
                return MapperUtils.mapUnary(unary, mappedChild);
            }
            // in case of a fragment, push to it any current streaming operator
            if (unary instanceof PipelineBreaker == false
                || (unary instanceof Limit limit && limit.local())
                || (unary instanceof TopN topN && topN.local())) {
                return new FragmentExec(unary);
            }
        }

        //
        // Pipeline breakers
        //
        if (unary instanceof Aggregate aggregate) {
            List<Attribute> intermediate = MapperUtils.intermediateAttributes(aggregate);

            // create both sides of the aggregate (for parallelism purposes), if no fragment is present
            // TODO: might be easier long term to end up with just one node and split if necessary instead of doing that always at this
            // stage
            mappedChild = addExchangeForFragment(aggregate, mappedChild);

            // exchange was added - use the intermediates for the output
            if (mappedChild instanceof ExchangeExec exchange) {
                mappedChild = new ExchangeExec(mappedChild.source(), intermediate, true, exchange.child());
            }
            // if no exchange was added (aggregation happening on the coordinator), try to only create a single-pass agg
            else if (aggregate.groupings()
                .stream()
                .noneMatch(group -> group.anyMatch(expr -> expr instanceof GroupingFunction.NonEvaluatableGroupingFunction))) {
                    return MapperUtils.aggExec(aggregate, mappedChild, AggregatorMode.SINGLE, intermediate);
                } else {
                    mappedChild = MapperUtils.aggExec(aggregate, mappedChild, AggregatorMode.INITIAL, intermediate);
                }

            // The final/reduction agg
            return MapperUtils.aggExec(aggregate, mappedChild, AggregatorMode.FINAL, intermediate);
        }

        if (unary instanceof Limit limit) {
            mappedChild = addExchangeForFragment(limit, mappedChild);
            return new LimitExec(limit.source(), mappedChild, limit.limit(), null);
        }

        if (unary instanceof TopN topN) {
            mappedChild = addExchangeForFragment(topN, mappedChild);
            var topNExec = new TopNExec(topN.source(), mappedChild, topN.order(), topN.limit(), null);

            if (mappedChild instanceof ExchangeExec exchangeExec) {
                // If the data nodes run a TopN, the TopN in the coordinator will receive already sorted data
                boolean sortedInput = exchangeExec.child() instanceof FragmentExec fragmentExec && fragmentExec.fragment() instanceof TopN;
                return sortedInput ? topNExec.withSortedInput() : topNExec;
            }

            return topNExec;
        }

        // MetricsInfo uses a two-phase approach like Aggregate: INITIAL on data nodes extracts
        // metric metadata from shards, FINAL on the coordinator merges rows from all data nodes.
        if (unary instanceof MetricsInfo metricsInfo) {
            mappedChild = addExchangeForFragment(metricsInfo, mappedChild);
            return new MetricsInfoExec(
                metricsInfo.source(),
                mappedChild,
                metricsInfo.output(),
                metricsInfo.output(),
                MetricsInfoExec.Mode.FINAL
            );
        }

        //
        // Pipeline operators
        //
        return MapperUtils.mapUnary(unary, mappedChild);
    }

    private PhysicalPlan mapBinary(BinaryPlan bp) {
        if (bp instanceof Join join) {
            JoinConfig config = join.config();
            if (config.type() != JoinTypes.LEFT) {
                throw new EsqlIllegalArgumentException("unsupported join type [" + config.type() + "]");
            }

            if (join.isRemote()) {
                // This is generally wrong in case of pipeline breakers upstream from the join, but we validate against these.
                // The only potential pipeline breakers upstream should be limits duplicated past the join from PushdownAndCombineLimits,
                // but they are okay to perform on the data nodes because they only serve to reduce the number of rows processed and
                // don't affect correctness due to another limit being downstream.
                return new FragmentExec(bp);
            }

            PhysicalPlan left = mapInner(bp.left());

            // only broadcast joins supported for now - hence push down as a streaming operator
            if (left instanceof FragmentExec) {
                return new FragmentExec(bp);
            }

            PhysicalPlan right = mapInner(bp.right());
            // if the right is data we can use a hash join directly
            if (right instanceof LocalSourceExec localData) {
                return new HashJoinExec(
                    join.source(),
                    left,
                    localData,
                    config.leftFields(),
                    config.rightFields(),
                    join.rightOutputFields()
                );
            }
            if (right instanceof FragmentExec fragment) {
                boolean isIndexModeLookup = isIndexModeLookup(fragment);
                if (isIndexModeLookup) {
                    return new LookupJoinExec(
                        join.source(),
                        left,
                        right,
                        config.leftFields(),
                        config.rightFields(),
                        join.rightOutputFields(),
                        config.joinOnConditions()
                    );
                }
            }
        }
        return MapperUtils.unsupported(bp);
    }

    private static boolean isIndexModeLookup(FragmentExec fragment) {
        // we support 2 cases:
        // EsRelation in index_mode=lookup
        boolean isIndexModeLookup = fragment.fragment() instanceof EsRelation relation && relation.indexMode() == IndexMode.LOOKUP;
        // or Filter(EsRelation) in index_mode=lookup
        isIndexModeLookup = isIndexModeLookup
            || fragment.fragment() instanceof Filter filter
                && filter.child() instanceof EsRelation relation
                && relation.indexMode() == IndexMode.LOOKUP;
        return isIndexModeLookup;
    }

    private PhysicalPlan mapFork(Fork fork) {
        if (fork instanceof UnionAll unionAll) {
            return mapUnionAll(unionAll);
        }
        return new MergeExec(fork.source(), fork.children().stream().map(this::mapInner).toList(), fork.output());
    }

    private PhysicalPlan mapUnionAll(UnionAll unionAll) {
        // after removing the implicit limit attached to each branch, the branch plan may not have a coordinator plan anymore, however
        // ComputeService.executePlan has trouble with executing plan without coordinator plan, adding exchange solves the issue
        int childSize = unionAll.children().size();
        List<PhysicalPlan> newChildren = new ArrayList<>(childSize);
        for (int i = 0; i < childSize; i++) {
            PhysicalPlan child = mapInner(unionAll.children().get(i));
            if (child instanceof FragmentExec) {
                child = new ExchangeExec(child.source(), child);
            }
            newChildren.add(child);
        }
        return new MergeExec(unionAll.source(), newChildren, unionAll.output());
    }

    private PhysicalPlan addExchangeForFragment(LogicalPlan logical, PhysicalPlan child) {
        // in case of fragment, preserve the streaming operator (order-by, limit or topN) for local replanning
        // no need to do it for an aggregate since it gets split
        // and clone it as a physical node along with the exchange
        if (child instanceof FragmentExec) {
            child = new FragmentExec(logical);
            child = new ExchangeExec(child.source(), child);
        }
        return child;
    }
}
