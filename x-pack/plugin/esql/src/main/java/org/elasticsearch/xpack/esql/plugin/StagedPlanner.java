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
import org.elasticsearch.xpack.esql.plan.logical.ExternalRelation;
import org.elasticsearch.xpack.esql.plan.physical.BinaryExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeDistribution;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSinkExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.LookupJoinExec;
import org.elasticsearch.xpack.esql.plan.physical.MergeExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ShuffleExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.StageSinkExec;
import org.elasticsearch.xpack.esql.plan.physical.StageSourceExec;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
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
    record Edge(String edgeKey, List<Integer> producerStages, int consumerStage, ExchangeDistribution distribution) {
        Edge {
            producerStages = List.copyOf(producerStages);
        }
    }

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

    static boolean isExternalSourceStage(PhysicalPlan plan) {
        return containsExternalSource(plan, ignoredPlans(plan));
    }

    static String externalSourcePath(PhysicalPlan plan) {
        Set<PhysicalPlan> ignored = ignoredPlans(plan);
        Set<String> sourcePaths = new LinkedHashSet<>();
        plan.collect(ExternalSourceExec.class)
            .stream()
            .filter(source -> ignored.contains(source) == false)
            .map(ExternalSourceExec::sourcePath)
            .forEach(sourcePaths::add);
        plan.collect(FragmentExec.class)
            .stream()
            .filter(fragment -> ignored.contains(fragment) == false)
            .forEach(
                fragment -> fragment.fragment().collect(ExternalRelation.class).forEach(relation -> sourcePaths.add(relation.sourcePath()))
            );
        if (sourcePaths.size() != 1) {
            throw new IllegalStateException("bug: expected one external source path in stage but found [" + sourcePaths.size() + "]");
        }
        return sourcePaths.iterator().next();
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
        return lookupScanRelation(fragment) != null;
    }

    @Nullable
    static EsRelation lookupScanRelation(FragmentExec fragment) {
        List<EsRelation> relations = fragment.fragment().collect(EsRelation.class);
        if (relations.size() != 1) {
            return null;
        }
        EsRelation relation = relations.getFirst();
        var indexProperties = relation.indexProperties();
        return indexProperties.isEmpty() == false
            && indexProperties.values().stream().allMatch(properties -> properties.indexMode() == IndexMode.LOOKUP) ? relation : null;
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
        if (plan instanceof MergeExec merge && merge.kind() == MergeExec.Kind.UNION) {
            String edgeKey = "e" + edges.size();
            int edgeSlot = edges.size();
            edges.add(null);
            List<Integer> producerStages = new ArrayList<>(merge.children().size());
            ExchangeDistribution distribution = null;
            for (PhysicalPlan branch : merge.children()) {
                if (branch instanceof ShuffleExchangeExec == false) {
                    throw new IllegalStateException("bug: union branch must be a shuffle exchange");
                }
                ShuffleExchangeExec exchange = (ShuffleExchangeExec) branch;
                if (exchange.inBetweenAggs()) {
                    throw new IllegalStateException("bug: union branch must carry rows, not intermediate states");
                }
                if (distribution != null && distribution != exchange.distribution()) {
                    throw new IllegalStateException("bug: union branches have different distributions");
                }
                distribution = exchange.distribution();
                StageBuilder producer = new StageBuilder(stages.size(), false, edgeKey);
                stages.add(producer);
                producerStages.add(producer.id);
                PhysicalPlan producerPlan = cut(exchange.child(), producer, ignored, stages, edges);
                producer.plan = new StageSinkExec(
                    exchange.source(),
                    exchange.output(),
                    false,
                    edgeKey,
                    exchange.distribution(),
                    exchange.keys(),
                    producerPlan
                );
            }
            if (distribution == null) {
                throw new IllegalStateException("bug: union has no producer branches");
            }
            consumer.inputEdges.add(edgeKey);
            edges.set(edgeSlot, new Edge(edgeKey, producerStages, consumer.id, distribution));
            return new StageSourceExec(merge.source(), merge.output(), false, edgeKey);
        }
        if (plan instanceof ShuffleExchangeExec exchange) {
            String edgeKey = "e" + edges.size();
            StageBuilder producer = new StageBuilder(stages.size(), false, edgeKey);
            stages.add(producer);
            consumer.inputEdges.add(edgeKey);
            edges.add(new Edge(edgeKey, List.of(producer.id), consumer.id, exchange.distribution()));

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
        if (containsExternalSource(stage.plan, ignored)) {
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

    private static boolean containsExternalSource(PhysicalPlan plan, Set<PhysicalPlan> ignored) {
        if (plan.collect(ExternalSourceExec.class).stream().anyMatch(source -> ignored.contains(source) == false)) {
            return true;
        }
        return plan.collect(FragmentExec.class)
            .stream()
            .filter(fragment -> ignored.contains(fragment) == false)
            .anyMatch(fragment -> fragment.fragment().anyMatch(ExternalRelation.class::isInstance));
    }

    private static void validate(List<Stage> stages, List<Edge> edges) {
        for (Stage stage : stages) {
            ExchangeDistribution inputDistribution = null;
            for (String edgeKey : stage.inputEdges()) {
                ExchangeDistribution distribution = edge(edges, edgeKey).distribution();
                if (distribution == ExchangeDistribution.BROADCAST) {
                    continue;
                }
                if (inputDistribution != null && inputDistribution != distribution) {
                    throw new IllegalStateException("bug: stage [" + stage.id() + "] has mixed input exchange distributions");
                }
                inputDistribution = distribution;
            }

            if (stage.placement() == StagePlacement.SHARDS) {
                if (stage.inputEdges()
                    .stream()
                    .anyMatch(edgeKey -> edge(edges, edgeKey).distribution() != ExchangeDistribution.BROADCAST)) {
                    throw new IllegalStateException("bug: SHARDS stage [" + stage.id() + "] has a non-BROADCAST input edge");
                }
                if (shardsDataDriverPlan(stage).anyMatch(BinaryExec.class::isInstance)) {
                    throw new IllegalStateException("bug: SHARDS stage [" + stage.id() + "] data-driver plan contains a binary operator");
                }
            }
        }

        for (Edge edge : edges) {
            for (int producerStageId : edge.producerStages()) {
                Stage producer = stages.get(producerStageId);
                if (edge.edgeKey().equals(producer.outputEdge()) == false) {
                    throw new IllegalStateException(
                        "bug: producer stage [" + producerStageId + "] does not output edge [" + edge.edgeKey() + "]"
                    );
                }
            }
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
                case BROADCAST -> {
                }
            }
        }
    }

    private static PhysicalPlan shardsDataDriverPlan(Stage stage) {
        Set<PhysicalPlan> ignored = ignoredPlans(stage.plan());
        List<FragmentExec> fragments = stage.plan()
            .collect(FragmentExec.class)
            .stream()
            .filter(fragment -> ignored.contains(fragment) == false)
            .toList();
        if (fragments.size() == 1) {
            if (stage.plan() instanceof StageSinkExec stageSink) {
                return splitShardsStage(stageSink).v1();
            }
            return fragments.getFirst();
        }
        List<ExternalSourceExec> externalSources = stage.plan()
            .collect(ExternalSourceExec.class)
            .stream()
            .filter(source -> ignored.contains(source) == false)
            .toList();
        if (fragments.isEmpty() && externalSources.size() == 1) {
            return externalSources.getFirst();
        }
        throw new IllegalStateException(
            "bug: expected one data driver in SHARDS stage ["
                + stage.id()
                + "] but found ["
                + (fragments.size() + externalSources.size())
                + "]"
        );
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
