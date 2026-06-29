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
import org.elasticsearch.xpack.esql.plan.logical.Dataset;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.ExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.LeafPlan;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LimitBy;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MaterializedReadSource;
import org.elasticsearch.xpack.esql.plan.logical.MetricsInfo;
import org.elasticsearch.xpack.esql.plan.logical.PipelineBreaker;
import org.elasticsearch.xpack.esql.plan.logical.RemoteViewSource;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.TopNBy;
import org.elasticsearch.xpack.esql.plan.logical.TsInfo;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinConfig;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.HashJoinExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitByExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitExec;
import org.elasticsearch.xpack.esql.plan.physical.LocalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.LookupJoinExec;
import org.elasticsearch.xpack.esql.plan.physical.MaterializedDatasetExec;
import org.elasticsearch.xpack.esql.plan.physical.MaterializedReadExec;
import org.elasticsearch.xpack.esql.plan.physical.MergeExec;
import org.elasticsearch.xpack.esql.plan.physical.MetricsInfoExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.RemoteDatasetExec;
import org.elasticsearch.xpack.esql.plan.physical.RemoteViewExec;
import org.elasticsearch.xpack.esql.plan.physical.TopNByExec;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;
import org.elasticsearch.xpack.esql.plan.physical.TsInfoExec;
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
        return mapInner(lowerLocalDatasets(versionedPlan.inner()));
    }

    /**
     * Strips every {@link Dataset.Boundary#LOCAL} {@code Dataset} wrapper, replacing it with its relation child, before
     * the logical plan is mapped. This is the LOCAL parity anchor: after stripping, the tree is byte-identical to today's
     * (an unwrapped resolved {@code ExternalRelation}), so the rest of mapping — and any fragment the wrapper would
     * otherwise have ended up inside (a {@code FragmentExec} serializes its whole logical fragment) — is unchanged.
     * <p>
     * This is Mapper-local lowering, not an optimizer rule: a LOCAL dataset carries no inline-vs-not decision, so there is
     * nothing for the optimizer to decide and nothing the pushdown rules need to see differently. A {@code REMOTE} /
     * {@code MATERIALIZED} {@code Dataset} is left in place for {@link #mapDataset} to lower to its physical exec.
     */
    private static LogicalPlan lowerLocalDatasets(LogicalPlan plan) {
        return plan.transformUp(Dataset.class, d -> d.boundary() == Dataset.Boundary.LOCAL ? d.relation() : d);
    }

    private PhysicalPlan mapInner(LogicalPlan p) {
        // Boundary-aware dataset lowering. A Dataset is a first-class node that survives analysis + optimization (datasets
        // have no inline-vs-not decision, so there is no optimizer fold rule — unlike views). It is a UnaryPlan, so it
        // must be intercepted before the generic unary dispatch below. The model is additive: a 4th boundary slots in by
        // adding a Dataset.Boundary constant + a case here, leaving the existing three untouched.
        if (p instanceof Dataset dataset) {
            return mapDataset(dataset);
        }

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

    /**
     * Boundary-aware lowering of a {@link Dataset}, the three-way decision at the heart of first-class datasets.
     * <ul>
     *   <li><b>LOCAL</b> — map the dataset's relation child (the resolved {@code ExternalRelation} the dataset produces
     *       today), reaching the exact same physical lowering an unwrapped dataset reaches. This is the parity anchor:
     *       byte-identical to today's external read.</li>
     *   <li><b>REMOTE</b> — the body does not execute locally; lower to a {@link RemoteDatasetExec} carrying the
     *       federation handle and the resolved schema.</li>
     *   <li><b>MATERIALIZED</b> — lower to a {@link MaterializedDatasetExec} carrying the backing-index ref and the
     *       resolved schema.</li>
     * </ul>
     * The REMOTE / MATERIALIZED execs are POC stubs — the decision + node + lowering path are real, the cross-cluster /
     * backing-store source operators are not built yet.
     */
    private PhysicalPlan mapDataset(Dataset dataset) {
        return switch (dataset.boundary()) {
            case LOCAL -> mapInner(dataset.relation());
            case REMOTE -> new RemoteDatasetExec(dataset.source(), dataset.datasetName(), remoteHandle(dataset), dataset.output());
            case MATERIALIZED -> new MaterializedDatasetExec(
                dataset.source(),
                dataset.datasetName(),
                backingIndex(dataset),
                dataset.output()
            );
        };
    }

    private static String remoteHandle(Dataset dataset) {
        Dataset.LoweringTarget target = dataset.loweringTarget();
        if (target == null || target.handle() == null) {
            throw new IllegalArgumentException("REMOTE dataset [" + dataset.datasetName() + "] has no remote-execution handle to lower");
        }
        return target.handle();
    }

    private static String backingIndex(Dataset dataset) {
        Dataset.LoweringTarget target = dataset.loweringTarget();
        if (target == null || target.backingIndex() == null) {
            throw new IllegalArgumentException("MATERIALIZED dataset [" + dataset.datasetName() + "] has no backing index to lower");
        }
        return target.backingIndex();
    }

    private PhysicalPlan mapLeaf(LeafPlan leaf) {
        if (leaf instanceof EsRelation esRelation) {
            return new FragmentExec(esRelation);
        }

        if (leaf instanceof ExternalRelation external) {
            return new FragmentExec(external);
        }

        // Boundary-aware view lowering: a REMOTE / MATERIALIZED view kept its boundary through the optimizer (the
        // InlineView rule lowered it to a first-class source leaf instead of folding it). Lower it the rest of the way to
        // its physical exec. The execution operator behind each exec is a POC stub — the decision + node + lowering path
        // are real, the cross-cluster / backing-store source operators are not built yet.
        if (leaf instanceof RemoteViewSource remote) {
            return new RemoteViewExec(remote.source(), remote.viewName(), remote.handle(), remote.output());
        }

        if (leaf instanceof MaterializedReadSource materialized) {
            return new MaterializedReadExec(
                materialized.source(),
                materialized.viewName(),
                materialized.backingIndex(),
                materialized.output()
            );
        }

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

        if (unary instanceof LimitBy limitBy) {
            mappedChild = addExchangeForFragment(limitBy, mappedChild);
            return new LimitByExec(limitBy.source(), mappedChild, limitBy.limitPerGroup(), limitBy.groupings(), null);
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

        if (unary instanceof TopNBy topNBy) {
            mappedChild = addExchangeForFragment(topNBy, mappedChild);
            return new TopNByExec(topNBy.source(), mappedChild, topNBy.order(), topNBy.limitPerGroup(), topNBy.groupings(), null);
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

        // TsInfo: same two-phase pattern as MetricsInfo but per time-series granularity.
        if (unary instanceof TsInfo tsInfo) {
            mappedChild = addExchangeForFragment(tsInfo, mappedChild);
            return new TsInfoExec(tsInfo.source(), mappedChild, tsInfo.output(), tsInfo.output(), TsInfoExec.Mode.FINAL);
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
        // after removing the implicit limit attached to each branch, the branch plan may not have a coordinator plan anymore, however
        // ComputeService.executePlan has trouble with executing plan without coordinator plan, adding exchange solves the issue
        int childSize = fork.children().size();

        List<PhysicalPlan> newChildren = new ArrayList<>(childSize);
        for (int i = 0; i < childSize; i++) {
            PhysicalPlan child = mapInner(fork.children().get(i));
            if (child instanceof FragmentExec) {
                child = new ExchangeExec(child.source(), child);
            }
            newChildren.add(child);
        }

        return new MergeExec(fork.source(), newChildren, fork.output());
    }

    /**
     * Wraps a bare {@link FragmentExec} in an {@link ExchangeExec} so that ComputeService routes it to data nodes.
     * Subplans(from IN subquery) that contain only streaming operators (no pipeline breakers like Limit/Aggregate)
     * map to a bare FragmentExec and need this wrapping before execution.
     */
    public static PhysicalPlan ensureExchangeForSubPlan(PhysicalPlan plan) {
        if (plan instanceof FragmentExec) {
            return new ExchangeExec(plan.source(), plan);
        }
        return plan;
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
