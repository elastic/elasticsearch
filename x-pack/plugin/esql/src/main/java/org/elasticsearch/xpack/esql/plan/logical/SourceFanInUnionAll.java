/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.common.Failure;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.index.IndexProperties;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * One resolved {@code FROM} expanded to its producers: datasets, indices, and matched
 * cross-project namesakes. The coordinator merges those producers the same way it merges any
 * other {@link UnionAll}.
 * <p>
 * {@link Fork} allows this node inside a branch. A subquery the user wrote stays a plain
 * {@link UnionAll} and is still rejected under {@code FORK}.
 */
public final class SourceFanInUnionAll extends UnionAll {

    /**
     * Producers one {@code FROM} may expand to. Tied to {@link MergePlan#MAX_BRANCHES} so the
     * per-command caps cannot drift.
     */
    public static final int MAX_PRODUCERS = MergePlan.MAX_BRANCHES;

    /** True when {@code count} is more producers than one {@code FROM} may expand to. */
    public static boolean exceedsMaxProducers(int count) {
        return count > MAX_PRODUCERS;
    }

    public SourceFanInUnionAll(Source source, List<LogicalPlan> children, List<Attribute> output) {
        super(source, flattenDirect(children), output);
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, SourceFanInUnionAll::new, children(), output());
    }

    @Override
    public LogicalPlan replaceChildren(List<LogicalPlan> newChildren) {
        return new SourceFanInUnionAll(source(), newChildren, output());
    }

    @Override
    public SourceFanInUnionAll replaceSubPlans(List<LogicalPlan> subPlans) {
        return new SourceFanInUnionAll(source(), subPlans, output());
    }

    @Override
    public SourceFanInUnionAll replaceSubPlansAndOutput(List<LogicalPlan> subPlans, List<Attribute> output) {
        return new SourceFanInUnionAll(source(), subPlans, output);
    }

    @Override
    public LogicalPlan pruneEmptyBranches(Predicate<LogicalPlan> isEmpty) {
        List<LogicalPlan> kept = new ArrayList<>(children().size());
        for (LogicalPlan child : children()) {
            if (isEmpty.test(child) == false) {
                kept.add(child);
            }
        }
        if (kept.size() == children().size()) {
            return this;
        }
        return new SourceFanInUnionAll(source(), kept, output());
    }

    @Override
    public int hashCode() {
        return Objects.hash(SourceFanInUnionAll.class, children());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SourceFanInUnionAll other = (SourceFanInUnionAll) o;
        return Objects.equals(children(), other.children());
    }

    @Override
    public BiConsumer<LogicalPlan, Failures> postAnalysisPlanVerification() {
        return SourceFanInUnionAll::checkSourceFanIn;
    }

    /**
     * Producers under {@code plan}. A nested fan-in and a unary pipeline wrapped around one
     * ({@code WHERE}, {@code EVAL}, {@code STATS}, {@code SORT}, {@code LIMIT}, or a projection)
     * contribute the producers inside them. Any other node is one producer; index reads that
     * {@link #withIndexReadsCollapsed} merged count once.
     */
    public static int producerCount(LogicalPlan plan) {
        if (plan instanceof SourceFanInUnionAll fanIn) {
            int count = 0;
            for (LogicalPlan child : fanIn.children()) {
                count += producerCount(child);
            }
            return count;
        }
        if (isSourcePipelineUnary(plan)) {
            return producerCount(((UnaryPlan) plan).child());
        }
        return 1;
    }

    /**
     * A {@link ViewUnionAll} that is only a resolved {@code FROM}: datasets, indices, or a mix, including a unary
     * pipeline on that expansion beside a matched namesake. An index-only view union, a {@code FORK}, a join, or a
     * subquery is not a source list.
     */
    public static boolean isSourceExpansion(ViewUnionAll view) {
        if (view.children().isEmpty() || view.anyMatch(p -> p instanceof ExternalRelation) == false) {
            return false;
        }
        for (LogicalPlan child : view.children()) {
            if (isPromotable(child) == false) {
                return false;
            }
        }
        return true;
    }

    /**
     * A bare producer ({@link ExternalRelation}, {@link EsRelation}, a nested fan-in, or a {@link Project} over one
     * of those) or a unary pipeline whose leaf is a fan-in.
     */
    private static boolean isPromotable(LogicalPlan plan) {
        LogicalPlan current = plan;
        // A Project may wrap a bare relation. Any other unary is promotable only over a fan-in.
        boolean sawNonProjectUnary = false;
        while (isSourcePipelineUnary(current)) {
            if (current instanceof Project == false) {
                sawNonProjectUnary = true;
            }
            current = ((UnaryPlan) current).child();
        }
        if (current instanceof SourceFanInUnionAll) {
            return true;
        }
        if (sawNonProjectUnary) {
            return false;
        }
        return current instanceof ExternalRelation || current instanceof EsRelation;
    }

    /**
     * A unary command that stays wrapped around a source fan-in: a filter, projection, eval, limit,
     * sort, or aggregate. Any other node is one producer, so a command such as {@code FORK} or a
     * subquery is not walked through.
     */
    public static boolean isSourcePipelineUnary(LogicalPlan plan) {
        return plan instanceof Filter
            || plan instanceof Project
            || plan instanceof Eval
            || plan instanceof Limit
            || plan instanceof OrderBy
            || plan instanceof Aggregate;
    }

    private static void checkSourceFanIn(LogicalPlan plan, Failures failures) {
        if (plan instanceof SourceFanInUnionAll fanIn) {
            if (fanIn.children().isEmpty()) {
                failures.add(Failure.fail(plan, "{} requires at least one branch", plan.getClass().getSimpleName()));
            }
            int producers = producerCount(fanIn);
            if (exceedsMaxProducers(producers)) {
                failures.add(
                    Failure.fail(
                        fanIn,
                        "FROM [{}] resolved to {} sources, exceeding the current limit of {} per FROM. "
                            + "Narrow the pattern, exclude some datasets, or split into multiple queries.",
                        fanIn.sourceText(),
                        producers,
                        MAX_PRODUCERS
                    )
                );
            }
        }
        UnionAll.checkOutputTypes(plan, failures);
    }

    /**
     * Sibling index scans of the same {@link IndexMode} become one {@link EsRelation}. A {@code FROM} already
     * joins local index names into one relation; a matched namesake is the same kind of read and joins that
     * relation instead of running as its own branch. Datasets stay separate. Differing index modes stay
     * separate, because one scan cannot mix them. Scans that map a field to different types also stay
     * separate, so the union-type rules can still reconcile them per branch. Scans that share a concrete index
     * or request different metadata fields stay separate, so each keeps its own copy of the rows.
     * <p>
     * Must run after index resolution and before branch alignment wraps each child in a projection. It is an
     * explicit step rather than part of the constructor, because a node must keep the children it is built with.
     */
    public SourceFanInUnionAll withIndexReadsCollapsed() {
        List<LogicalPlan> children = children();
        Map<IndexMode, List<EsRelation>> byMode = new LinkedHashMap<>();
        for (LogicalPlan child : children) {
            if (child instanceof EsRelation es) {
                byMode.computeIfAbsent(es.indexMode(), mode -> new ArrayList<>()).add(es);
            }
        }
        Map<IndexMode, EsRelation> merged = new LinkedHashMap<>();
        for (var entry : byMode.entrySet()) {
            if (entry.getValue().size() >= 2 && canMergeReads(entry.getValue())) {
                List<Attribute> attributes = mergeAttributes(entry.getValue());
                if (attributes != null) {
                    merged.put(entry.getKey(), mergeEsRelations(entry.getValue(), attributes));
                }
            }
        }
        if (merged.isEmpty()) {
            return this;
        }
        List<LogicalPlan> out = new ArrayList<>(children.size());
        LinkedHashSet<IndexMode> placed = new LinkedHashSet<>();
        for (LogicalPlan child : children) {
            if (child instanceof EsRelation es && merged.containsKey(es.indexMode())) {
                if (placed.add(es.indexMode())) {
                    out.add(merged.get(es.indexMode()));
                }
            } else {
                out.add(child);
            }
        }
        return new SourceFanInUnionAll(source(), out, output());
    }

    /**
     * True when the scans read disjoint concrete indices and request the same metadata fields. Two scans of the
     * same index are two copies of its rows under {@code UNION ALL}, and one merged scan would return them once.
     * Mirrors the guards in {@code ViewCompaction.mergeIfPossible}.
     */
    private static boolean canMergeReads(List<EsRelation> relations) {
        Set<String> metadata = metadataNames(relations.getFirst());
        Map<String, Set<String>> seen = new HashMap<>();
        for (EsRelation es : relations) {
            if (metadataNames(es).equals(metadata) == false) {
                return false;
            }
            for (var entry : es.concreteIndices().entrySet()) {
                Set<String> indices = seen.computeIfAbsent(entry.getKey(), key -> new HashSet<>());
                for (String index : entry.getValue()) {
                    if (indices.add(index) == false) {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    private static Set<String> metadataNames(EsRelation relation) {
        Set<String> names = new HashSet<>();
        for (Attribute attr : relation.output()) {
            if (attr instanceof MetadataAttribute) {
                names.add(attr.name());
            }
        }
        return names;
    }

    private static EsRelation mergeEsRelations(List<EsRelation> relations, List<Attribute> attributes) {
        EsRelation first = relations.get(0);
        LinkedHashSet<String> patterns = new LinkedHashSet<>();
        for (EsRelation es : relations) {
            for (String part : es.indexPattern().split(",")) {
                if (part.isEmpty() == false) {
                    patterns.add(part);
                }
            }
        }
        Map<String, IndexProperties> properties = new LinkedHashMap<>();
        for (EsRelation es : relations) {
            for (var entry : es.indexProperties().entrySet()) {
                properties.putIfAbsent(entry.getKey(), entry.getValue());
            }
        }
        return new EsRelation(
            first.source(),
            String.join(",", patterns),
            first.indexMode(),
            mergeIndexMaps(relations, EsRelation::originalIndices),
            mergeIndexMaps(relations, EsRelation::concreteIndices),
            properties.isEmpty() ? Map.of() : Map.copyOf(properties),
            attributes
        );
    }

    private static Map<String, List<String>> mergeIndexMaps(
        List<EsRelation> relations,
        Function<EsRelation, Map<String, List<String>>> indices
    ) {
        Map<String, LinkedHashSet<String>> acc = new LinkedHashMap<>();
        for (EsRelation es : relations) {
            for (var entry : indices.apply(es).entrySet()) {
                acc.computeIfAbsent(entry.getKey(), key -> new LinkedHashSet<>()).addAll(entry.getValue());
            }
        }
        if (acc.isEmpty()) {
            return Map.of();
        }
        Map<String, List<String>> out = new LinkedHashMap<>();
        for (var entry : acc.entrySet()) {
            out.put(entry.getKey(), List.copyOf(entry.getValue()));
        }
        return Map.copyOf(out);
    }

    /**
     * Union of fields, or {@code null} when two scans map a name to different types. The {@link Analyzer#NO_FIELDS}
     * marker of an empty mapping is dropped once another scan contributes real fields.
     */
    @Nullable
    private static List<Attribute> mergeAttributes(List<EsRelation> relations) {
        LinkedHashMap<String, Attribute> byName = new LinkedHashMap<>();
        for (EsRelation es : relations) {
            for (Attribute attr : es.output()) {
                if (Analyzer.NO_FIELDS_NAME.equals(attr.name())) {
                    continue;
                }
                Attribute existing = byName.putIfAbsent(attr.name(), attr);
                if (existing != null && existing.dataType() != attr.dataType()) {
                    return null;
                }
            }
        }
        return byName.isEmpty() ? Analyzer.NO_FIELDS : List.copyOf(byName.values());
    }

    /** Flattens a fan-in that is itself a direct child. A pipeline wrapped around an inner fan-in stays put. */
    private static List<LogicalPlan> flattenDirect(List<LogicalPlan> children) {
        boolean nested = false;
        for (LogicalPlan child : children) {
            if (child instanceof SourceFanInUnionAll) {
                nested = true;
                break;
            }
        }
        if (nested == false) {
            return children;
        }
        List<LogicalPlan> flat = new ArrayList<>(children.size());
        for (LogicalPlan child : children) {
            if (child instanceof SourceFanInUnionAll fanIn) {
                flat.addAll(fanIn.children());
            } else {
                flat.add(child);
            }
        }
        return flat;
    }
}
