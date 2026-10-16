/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.physical.BinaryExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeDistribution;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSinkExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.LookupJoinExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.RemoteFetchExec;
import org.elasticsearch.xpack.esql.plan.physical.ShuffleExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.StageSinkExec;
import org.elasticsearch.xpack.esql.plan.physical.StageSourceExec;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

/** Splits coordinator-only exchange markers into stages for the shuffle scheduler. */
final class StagedPlanner {
    /** Execution location selected for a stage after exchange boundaries are cut. */
    enum StagePlacement {
        COORDINATOR,
        SHARDS,
        WORKERS,
        SINGLE
    }

    /** Directed data flow between producer and consumer stages. */
    record Edge(String edgeKey, int producerStage, int consumerStage, ExchangeDistribution distribution) {}

    /** Physical plan assigned to one scheduler placement and its exchange dependencies. */
    record Stage(int id, StagePlacement placement, PhysicalPlan plan, List<String> inputEdges, @Nullable String outputEdge) {
        Stage {
            inputEdges = List.copyOf(inputEdges);
        }
    }

    /** Immutable stage graph produced from a physical plan containing shuffle markers. */
    record StagedPlan(List<Stage> stages, List<Edge> edges) {
        StagedPlan {
            stages = List.copyOf(stages);
            edges = List.copyOf(edges);
        }

        Stage root() {
            return stages.getFirst();
        }

        Edge edge(String key) {
            for (Edge edge : edges) {
                if (edge.edgeKey().equals(key)) {
                    return edge;
                }
            }
            throw new IllegalArgumentException("unknown edge [" + key + "]");
        }
    }

    private StagedPlanner() {}

    static boolean hasShuffle(PhysicalPlan plan) {
        return plan.anyMatch(ShuffleExchangeExec.class::isInstance);
    }

    static StagedPlan split(PhysicalPlan plan) {
        Set<PhysicalPlan> ignored = ignoredPlans(plan);
        if (hasShuffle(plan)) {
            plan = stripLookupFilters(plan, ignored);
        }
        List<StageBuilder> stages = new ArrayList<>();
        List<Edge> edges = new ArrayList<>();
        StageBuilder root = new StageBuilder(0, true, null);
        stages.add(root);
        root.plan = cut(plan, root, ignored, stages, edges);

        List<Stage> result = new ArrayList<>(stages.size());
        for (StageBuilder stage : stages) {
            result.add(new Stage(stage.id, placement(stage, ignored, edges), stage.plan, stage.inputEdges, stage.outputEdge));
        }
        validate(result, edges);
        return new StagedPlan(result, edges);
    }

    private static PhysicalPlan stripLookupFilters(PhysicalPlan plan, Set<PhysicalPlan> ignored) {
        if (ignored.contains(plan)) {
            return plan;
        }
        if (plan instanceof FragmentExec fragment) {
            if (fragment.esFilter() == null || isLookupScan(fragment) == false) {
                return fragment;
            }
            return new FragmentExec(fragment.source(), fragment.fragment(), null, fragment.estimatedRowSize(), fragment.isFromViewBranch());
        }
        List<PhysicalPlan> children = plan.children();
        if (children.isEmpty()) {
            return plan;
        }
        List<PhysicalPlan> rewrittenChildren = new ArrayList<>(children.size());
        boolean changed = false;
        for (PhysicalPlan child : children) {
            PhysicalPlan rewrittenChild = stripLookupFilters(child, ignored);
            rewrittenChildren.add(rewrittenChild);
            changed |= rewrittenChild != child;
        }
        return changed ? plan.replaceChildren(rewrittenChildren) : plan;
    }

    private static boolean isLookupScan(FragmentExec fragment) {
        List<EsRelation> relations = fragment.fragment().collect(EsRelation.class);
        if (relations.size() != 1) {
            return false;
        }
        var indexProperties = relations.getFirst().indexProperties();
        return indexProperties.isEmpty() == false
            && indexProperties.values().stream().allMatch(properties -> properties.indexMode() == IndexMode.LOOKUP);
    }

    /**
     * Separates the shard-driver fragment from the node-level portion of a SHARDS stage.
     */
    static Tuple<ExchangeSinkExec, PhysicalPlan> splitShardsStage(StageSinkExec stagePlan) {
        Set<PhysicalPlan> ignored = ignoredPlans(stagePlan);
        List<FragmentExec> fragments = stagePlan.collect(FragmentExec.class)
            .stream()
            .filter(fragment -> ignored.contains(fragment) == false)
            .toList();
        if (fragments.size() != 1) {
            throw new IllegalStateException(
                "bug: expected one non-ignored fragment in a SHARDS stage but found [" + fragments.size() + "]"
            );
        }

        FragmentExec fragment = fragments.getFirst();
        boolean fragmentIsRootChild = stagePlan.child() == fragment;
        List<Attribute> output = fragmentIsRootChild ? stagePlan.output() : fragment.output();
        boolean intermediateAgg = fragmentIsRootChild && stagePlan.isIntermediateAgg();

        ExchangeSinkExec dataPlan = new ExchangeSinkExec(fragment.source(), output, intermediateAgg, fragment);
        PhysicalPlan nodePlan = stagePlan.transformDown(
            FragmentExec.class,
            candidate -> candidate == fragment ? new ExchangeSourceExec(candidate.source(), output, intermediateAgg) : candidate
        );
        return new Tuple<>(dataPlan, nodePlan);
    }

    private static PhysicalPlan cut(
        PhysicalPlan plan,
        StageBuilder consumer,
        Set<PhysicalPlan> ignored,
        List<StageBuilder> stages,
        List<Edge> edges
    ) {
        if (ignored.contains(plan)) {
            return plan;
        }
        if (plan instanceof ShuffleExchangeExec exchange) {
            String edgeKey = "e" + edges.size();
            StageBuilder producer = new StageBuilder(stages.size(), false, edgeKey);
            stages.add(producer);
            consumer.inputEdges.add(edgeKey);
            edges.add(new Edge(edgeKey, producer.id, consumer.id, exchange.distribution()));

            PhysicalPlan producerPlan = cut(exchange.child(), producer, ignored, stages, edges);
            producer.plan = new StageSinkExec(
                exchange.source(),
                exchange.output(),
                exchange.inBetweenAggs(),
                edgeKey,
                exchange.distribution(),
                exchange.keys(),
                producerPlan
            );
            return new StageSourceExec(exchange.source(), exchange.output(), exchange.inBetweenAggs(), edgeKey);
        }

        List<PhysicalPlan> children = plan.children();
        if (children.isEmpty()) {
            return plan;
        }
        List<PhysicalPlan> rewrittenChildren = new ArrayList<>(children.size());
        boolean changed = false;
        for (PhysicalPlan child : children) {
            PhysicalPlan rewrittenChild = cut(child, consumer, ignored, stages, edges);
            rewrittenChildren.add(rewrittenChild);
            changed |= rewrittenChild != child;
        }
        return changed ? plan.replaceChildren(rewrittenChildren) : plan;
    }

    private static Set<PhysicalPlan> ignoredPlans(PhysicalPlan plan) {
        Set<PhysicalPlan> ignored = Collections.newSetFromMap(new IdentityHashMap<>());
        plan.collect(LookupJoinExec.class).forEach(join -> ignored.add(join.right()));
        plan.collect(RemoteFetchExec.class).forEach(remoteFetch -> ignored.add(remoteFetch.right()));
        return ignored;
    }

    private static StagePlacement placement(StageBuilder stage, Set<PhysicalPlan> ignored, List<Edge> edges) {
        List<FragmentExec> fragments = stage.plan.collect(FragmentExec.class)
            .stream()
            .filter(fragment -> ignored.contains(fragment) == false)
            .toList();
        if (fragments.size() > 1) {
            throw new IllegalStateException("bug: stage [" + stage.id + "] contains multiple non-ignored fragments");
        }
        if (fragments.isEmpty() == false) {
            return StagePlacement.SHARDS;
        }
        if (stage.root) {
            return StagePlacement.COORDINATOR;
        }
        for (String edgeKey : stage.inputEdges) {
            if (edge(edges, edgeKey).distribution() == ExchangeDistribution.HASH) {
                return StagePlacement.WORKERS;
            }
        }
        return StagePlacement.SINGLE;
    }

    private static void validate(List<Stage> stages, List<Edge> edges) {
        for (Stage stage : stages) {
            ExchangeDistribution inputDistribution = null;
            for (String edgeKey : stage.inputEdges()) {
                ExchangeDistribution distribution = edge(edges, edgeKey).distribution();
                if (inputDistribution != null && inputDistribution != distribution) {
                    throw new IllegalStateException("bug: stage [" + stage.id() + "] has mixed input exchange distributions");
                }
                inputDistribution = distribution;
            }

            if (stage.placement() == StagePlacement.SHARDS) {
                if (stage.inputEdges().isEmpty() == false) {
                    throw new IllegalStateException("bug: SHARDS stage [" + stage.id() + "] has input edges");
                }
                if (stage.plan().anyMatch(BinaryExec.class::isInstance)) {
                    throw new IllegalStateException("bug: SHARDS stage [" + stage.id() + "] contains a binary operator");
                }
            }
        }

        for (Edge edge : edges) {
            Stage consumer = stages.get(edge.consumerStage());
            switch (edge.distribution()) {
                case GATHER -> {
                    if (consumer.placement() != StagePlacement.COORDINATOR && consumer.placement() != StagePlacement.SINGLE) {
                        throw new IllegalStateException(
                            "bug: GATHER edge ["
                                + edge.edgeKey()
                                + "] feeds stage ["
                                + consumer.id()
                                + "] at ["
                                + consumer.placement()
                                + "]"
                        );
                    }
                }
                case HASH -> {
                    if (consumer.placement() != StagePlacement.WORKERS) {
                        throw new IllegalStateException(
                            "bug: HASH edge [" + edge.edgeKey() + "] feeds stage [" + consumer.id() + "] at [" + consumer.placement() + "]"
                        );
                    }
                }
            }
        }
    }

    private static Edge edge(List<Edge> edges, String edgeKey) {
        for (Edge edge : edges) {
            if (edge.edgeKey().equals(edgeKey)) {
                return edge;
            }
        }
        throw new IllegalStateException("bug: missing exchange edge [" + edgeKey + "]");
    }

    private static final class StageBuilder {
        private final int id;
        private final boolean root;
        private final List<String> inputEdges = new ArrayList<>();
        private final @Nullable String outputEdge;
        private PhysicalPlan plan;

        private StageBuilder(int id, boolean root, @Nullable String outputEdge) {
            this.id = id;
            this.root = root;
            this.outputEdge = outputEdge;
        }
    }
}
