/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.common.Failure;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.PotentiallyUnmappedKeywordEsField;
import org.elasticsearch.xpack.esql.core.type.PotentiallyUnmappedSingleTypeEsField;
import org.elasticsearch.xpack.esql.core.type.TypeConflictedField;
import org.elasticsearch.xpack.esql.index.IndexProperties;
import org.elasticsearch.xpack.esql.session.IndexResolver;
import org.elasticsearch.xpack.esql.view.ViewCompaction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;

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

    public SourceFanInUnionAll(Source source, List<LogicalPlan> children, List<Attribute> output) {
        super(source, flattenDirect(children), output);
        assert children().stream().noneMatch(child -> child.anyMatch(SourceFanInUnionAll::isBranching))
            : "a source fan-in holds one FROM's producers, not a FORK, union, or subquery: " + children();
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
    public BiConsumer<LogicalPlan, Failures> postAnalysisPlanVerification() {
        return super.postAnalysisPlanVerification().andThen(SourceFanInUnionAll::checkProducerCount);
    }

    /** The lone producer when only one is left, otherwise this fan-in. */
    public LogicalPlan collapseSingleChild() {
        return children().size() == 1 ? children().getFirst() : this;
    }

    /**
     * Producers under {@code fanIn}. A child with fan-ins inside counts their producers, whatever pipeline wraps them;
     * any other child is one producer. Index reads that {@link #withIndexReadsCollapsed} merged count once.
     */
    static int producerCount(SourceFanInUnionAll fanIn) {
        int count = 0;
        for (LogicalPlan child : fanIn.children()) {
            count += Math.max(1, producersOfFanInsUnder(child));
        }
        return count;
    }

    /** Producers of the fan-ins inside {@code plan}, or {@code 0} when it has none. */
    private static int producersOfFanInsUnder(LogicalPlan plan) {
        if (plan instanceof SourceFanInUnionAll fanIn) {
            return producerCount(fanIn);
        }
        int count = 0;
        for (LogicalPlan child : plan.children()) {
            count += producersOfFanInsUnder(child);
        }
        return count;
    }

    /**
     * A {@link ViewUnionAll} that is only a resolved {@code FROM}: datasets, indices, or a mix, including a pipeline
     * on that expansion beside a matched namesake. An index-only view union is not a source list, and neither is a
     * branch holding a {@code FORK}, a union, or a subquery. A user-written subquery branch is not a source list either,
     * even when it only reads sources, so {@code FROM v, (FROM ds)} stays a subquery under {@code FORK}.
     * <p>
     * Index and dataset leaves are treated alike, so a view that filters an index promotes the same as one that
     * filters a dataset.
     */
    public static boolean isSourceExpansion(ViewUnionAll view) {
        boolean readsDataset = false;
        for (Map.Entry<String, LogicalPlan> entry : view.namedSubqueries().entrySet()) {
            LogicalPlan branch = entry.getValue();
            if (ViewCompaction.isLiteralSubqueryKey(entry.getKey())) {
                return false; // a subquery the user wrote
            }
            if (branch.anyMatch(SourceFanInUnionAll::isBranching)) {
                return false; // a FORK, union, or subquery inside the branch
            }
            readsDataset = readsDataset || branch.anyMatch(p -> p instanceof ExternalRelation);
        }
        return readsDataset; // an index-only view union stays a view union
    }

    /**
     * A node that makes a branch more than one {@code FROM} with a pipeline on it: a merge other than a fan-in
     * ({@code FORK} or a union), or a subquery. Mirrors what {@code FORK} rejects, so any other command, including
     * one added later, keeps a view branch a source list.
     */
    public static boolean isBranching(LogicalPlan plan) {
        return (plan instanceof MergePlan && plan instanceof SourceFanInUnionAll == false) || plan instanceof Subquery;
    }

    private static void checkProducerCount(LogicalPlan plan, Failures failures) {
        if (plan instanceof SourceFanInUnionAll fanIn) {
            int producers = producerCount(fanIn);
            if (producers > MAX_PRODUCERS) {
                failures.add(
                    Failure.fail(
                        fanIn,
                        // The source text already starts with FROM, so it is quoted as is.
                        "[{}] resolved to {} sources, exceeding the current limit of {} per FROM. "
                            + "Narrow the pattern, exclude some datasets, or split into multiple queries.",
                        fanIn.sourceText(),
                        producers,
                        MAX_PRODUCERS
                    )
                );
            }
        }
    }

    /**
     * Sibling index scans of the same {@link IndexMode} become one {@link EsRelation}. A {@code FROM} already
     * joins local index names into one relation; a matched namesake is the same kind of read and joins that
     * relation instead of running as its own branch. Datasets stay separate. Differing index modes stay
     * separate, because one scan cannot mix them. Compatible subsets are merged independently, so one
     * overlapping or incompatible read does not prevent other reads from merging. Scans with conflicting
     * metadata for the same field stay separate, so field resolution retains each read's mapping and
     * unmapped-field behavior. Scans with exclusions, overlapping concrete indices, or different metadata
     * fields also stay separate, preserving their index-selection scope and copies of rows.
     * <p>
     * Must run after index resolution and before branch alignment wraps each child in a projection. It is an
     * explicit step rather than part of the constructor, because a node must keep the children it is built with.
     *
     * @param loadUnmappedFields marks fields that are unmapped in part of the merged scan as potentially unmapped
     */
    public SourceFanInUnionAll withIndexReadsCollapsed(boolean loadUnmappedFields) {
        // Each index read joins the first group it can merge with. A group's first read is where its merged read goes.
        List<List<EsRelation>> groups = new ArrayList<>();
        Map<EsRelation, List<EsRelation>> groupOf = new IdentityHashMap<>();
        for (LogicalPlan child : children()) {
            if (child instanceof EsRelation read) {
                List<EsRelation> group = groups.stream().filter(g -> canJoin(g, read)).findFirst().orElse(null);
                if (group == null) {
                    group = new ArrayList<>();
                    groups.add(group);
                }
                group.add(read);
                groupOf.put(read, group);
            }
        }
        if (groups.size() == groupOf.size()) {
            return this;
        }
        List<LogicalPlan> out = new ArrayList<>();
        for (LogicalPlan child : children()) {
            if (child instanceof EsRelation read) {
                List<EsRelation> group = groupOf.get(read);
                if (group.getFirst() == read) {
                    out.add(group.size() == 1 ? read : mergeReads(group, loadUnmappedFields));
                }
            } else {
                out.add(child);
            }
        }
        return new SourceFanInUnionAll(source(), out, output());
    }

    /** True when {@code read} can be merged into the reads of {@code group}. */
    private static boolean canJoin(List<EsRelation> group, EsRelation read) {
        if (group.getFirst().indexMode() != read.indexMode()) {
            return false;
        }
        List<EsRelation> candidate = new ArrayList<>(group);
        candidate.add(read);
        return canMergeReads(candidate) && hasCompatibleFields(candidate);
    }

    /**
     * True when the scans read disjoint concrete indices and request the same metadata fields. Two scans of the
     * same index are two copies of its rows under {@code UNION ALL}, and one merged scan would return them once.
     * An exclusion must keep its original read scope rather than filtering a sibling's indices.
     * Mirrors the guards in {@code ViewCompaction.mergeIfPossible}.
     */
    private static boolean canMergeReads(List<EsRelation> relations) {
        Set<String> metadata = metadataNames(relations.getFirst());
        Map<String, Set<String>> seen = new HashMap<>();
        for (EsRelation es : relations) {
            if (metadataNames(es).equals(metadata) == false) {
                return false;
            }
            if (ViewCompaction.containsExclusion(es.indexPattern())) {
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

    private static EsRelation mergeReads(List<EsRelation> relations, boolean loadUnmappedFields) {
        List<Attribute> attributes = unionOfFields(relations);
        if (loadUnmappedFields) {
            attributes = markPartiallyUnmapped(attributes, relations);
        }
        EsRelation first = relations.get(0);
        LinkedHashSet<String> patterns = new LinkedHashSet<>();
        for (EsRelation es : relations) {
            for (String part : es.indexPattern().split(",")) {
                String trimmed = part.trim();
                if (trimmed.isEmpty() == false) {
                    patterns.add(trimmed);
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
            attributes.isEmpty() ? Analyzer.NO_FIELDS : attributes
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
     * True when every field name the reads share has the same field metadata. Matching datatypes alone do not
     * establish that two fields have the same mapping or unmapped-field behavior.
     */
    private static boolean hasCompatibleFields(List<EsRelation> relations) {
        Map<String, Attribute> byName = new HashMap<>();
        for (EsRelation es : relations) {
            for (Attribute attr : es.output()) {
                if (Analyzer.NO_FIELDS_NAME.equals(attr.name())) {
                    continue;
                }
                Attribute existing = byName.putIfAbsent(attr.name(), attr);
                if (existing != null && existing.ignoreId().equals(attr.ignoreId()) == false) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * The fields of all reads, by name. The {@link Analyzer#NO_FIELDS} marker of an empty mapping is left out, so it is
     * dropped once another read contributes real fields.
     */
    private static List<Attribute> unionOfFields(List<EsRelation> relations) {
        LinkedHashMap<String, Attribute> byName = new LinkedHashMap<>();
        for (EsRelation es : relations) {
            for (Attribute attr : es.output()) {
                if (Analyzer.NO_FIELDS_NAME.equals(attr.name()) == false) {
                    byName.putIfAbsent(attr.name(), attr);
                }
            }
        }
        return List.copyOf(byName.values());
    }

    /** Marks a field that only some of the merged reads have as potentially unmapped in the merged read. */
    private static List<Attribute> markPartiallyUnmapped(List<Attribute> attributes, List<EsRelation> relations) {
        Set<String> allIndices = new HashSet<>();
        Map<String, Integer> readsWithField = new HashMap<>();
        Map<String, Set<String>> mappedIndices = new HashMap<>();
        for (EsRelation es : relations) {
            allIndices.addAll(es.concreteQualifiedIndices());
            for (Attribute attr : es.output()) {
                readsWithField.merge(attr.name(), 1, Integer::sum);
                if (attr instanceof FieldAttribute field) {
                    mappedIndices.computeIfAbsent(attr.name(), key -> new HashSet<>()).addAll(mappedIndices(field.field(), es));
                }
            }
        }
        if (allIndices.isEmpty()) {
            return attributes;
        }
        List<Attribute> marked = new ArrayList<>(attributes.size());
        for (Attribute attr : attributes) {
            if (attr instanceof FieldAttribute field && readsWithField.get(attr.name()) < relations.size()) {
                Set<String> mapped = mappedIndices.getOrDefault(attr.name(), Set.of());
                attr = field.withField(markPotentiallyUnmapped(field.field(), field.fieldName().string(), mapped, allIndices.size()));
            }
            marked.add(attr);
        }
        return marked;
    }

    private static Set<String> mappedIndices(EsField field, EsRelation relation) {
        if (field instanceof PotentiallyUnmappedSingleTypeEsField potentiallyUnmapped) {
            return potentiallyUnmapped.mappedIndices();
        }
        if (field instanceof TypeConflictedField conflicted) {
            Set<String> indices = new HashSet<>();
            conflicted.getTypesToIndices().values().forEach(indices::addAll);
            return indices;
        }
        return relation.concreteQualifiedIndices();
    }

    private static EsField markPotentiallyUnmapped(EsField field, String fullName, Set<String> mappedIndices, int numberOfIndices) {
        if (field instanceof PotentiallyUnmappedKeywordEsField
            || field instanceof TypeConflictedField conflicted && conflicted.isPotentiallyUnmapped()
            || mappedIndices.isEmpty()) {
            return field;
        }
        EsField marked = field;
        if (field.getProperties().isEmpty() == false) {
            Map<String, EsField> markedProperties = new LinkedHashMap<>();
            for (var entry : field.getProperties().entrySet()) {
                markedProperties.put(
                    entry.getKey(),
                    markPotentiallyUnmapped(entry.getValue(), fullName + "." + entry.getKey(), mappedIndices, numberOfIndices)
                );
            }
            marked = field.withProperties(markedProperties);
        }
        EsField wrapped = IndexResolver.wrapIfPartiallyUnmapped(marked, marked.getName(), fullName, mappedIndices, numberOfIndices);
        // The keyword wrapper is built from the name alone, so put the marked sub-fields back on it.
        if (wrapped instanceof PotentiallyUnmappedKeywordEsField && marked.getProperties().isEmpty() == false) {
            return wrapped.withProperties(marked.getProperties());
        }
        return wrapped;
    }

    /** Flattens a fan-in that is itself a direct child. A pipeline wrapped around an inner fan-in stays put. */
    private static List<LogicalPlan> flattenDirect(List<LogicalPlan> children) {
        if (children.stream().noneMatch(child -> child instanceof SourceFanInUnionAll)) {
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
