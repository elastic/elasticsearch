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
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.PotentiallyUnmappedKeywordEsField;
import org.elasticsearch.xpack.esql.core.type.PotentiallyUnmappedSingleTypeEsField;
import org.elasticsearch.xpack.esql.core.type.TypeConflictedField;
import org.elasticsearch.xpack.esql.index.IndexProperties;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.session.IndexResolver;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
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
            || plan instanceof Rename
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
     * separate, because one scan cannot mix them. Compatible subsets are merged independently, so one
     * overlapping or incompatible read does not prevent other reads from merging. Scans with conflicting
     * metadata for the same field stay separate, so field resolution retains each read's mapping and
     * unmapped-field behavior. Scans with exclusions, overlapping concrete indices, or different metadata
     * fields also stay separate, preserving their index-selection scope and copies of rows.
     * <p>
     * Must run after index resolution and before branch alignment wraps each child in a projection. It is an
     * explicit step rather than part of the constructor, because a node must keep the children it is built with.
     */
    public SourceFanInUnionAll withIndexReadsCollapsed() {
        return withIndexReadsCollapsed(false);
    }

    /**
     * Collapses compatible sibling index scans and preserves fields that are unmapped in part of the
     * resulting scan when {@code loadUnmappedFields} is enabled.
     */
    public SourceFanInUnionAll withIndexReadsCollapsed(boolean loadUnmappedFields) {
        List<LogicalPlan> children = children();
        Map<IndexMode, List<List<Integer>>> groupsByMode = new LinkedHashMap<>();
        for (int i = 0; i < children.size(); i++) {
            LogicalPlan child = children.get(i);
            if (child instanceof EsRelation es) {
                List<List<Integer>> groups = groupsByMode.computeIfAbsent(es.indexMode(), mode -> new ArrayList<>());
                boolean added = false;
                for (List<Integer> group : groups) {
                    List<EsRelation> candidate = relations(children, group);
                    candidate.add(es);
                    if (canMergeReads(candidate) && mergeAttributes(candidate, false) != null) {
                        group.add(i);
                        added = true;
                        break;
                    }
                }
                if (added == false) {
                    groups.add(new ArrayList<>(List.of(i)));
                }
            }
        }
        Map<Integer, EsRelation> replacements = new HashMap<>();
        Set<Integer> omitted = new HashSet<>();
        for (List<List<Integer>> groups : groupsByMode.values()) {
            for (List<Integer> group : groups) {
                if (group.size() >= 2) {
                    List<EsRelation> relations = relations(children, group);
                    List<Attribute> attributes = mergeAttributes(relations, loadUnmappedFields);
                    assert attributes != null;
                    replacements.put(group.getFirst(), mergeEsRelations(relations, attributes));
                    omitted.addAll(group.subList(1, group.size()));
                }
            }
        }
        if (replacements.isEmpty()) {
            return this;
        }
        List<LogicalPlan> out = new ArrayList<>(children.size());
        for (int i = 0; i < children.size(); i++) {
            EsRelation replacement = replacements.get(i);
            if (replacement != null) {
                out.add(replacement);
            } else if (omitted.contains(i) == false) {
                out.add(children.get(i));
            }
        }
        return new SourceFanInUnionAll(source(), out, output());
    }

    private static List<EsRelation> relations(List<LogicalPlan> children, List<Integer> positions) {
        List<EsRelation> relations = new ArrayList<>(positions.size());
        for (int position : positions) {
            relations.add((EsRelation) children.get(position));
        }
        return relations;
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
            for (String pattern : es.indexPattern().split(",")) {
                if (IndexPattern.isExclusion(pattern.trim())) {
                    return false;
                }
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
     * Union of fields, or {@code null} when a shared name has different field metadata. Matching datatypes alone
     * do not establish that two fields have the same mapping or unmapped-field behavior. The {@link Analyzer#NO_FIELDS}
     * marker of an empty mapping is dropped once another scan contributes real fields.
     */
    @Nullable
    private static List<Attribute> mergeAttributes(List<EsRelation> relations, boolean loadUnmappedFields) {
        LinkedHashMap<String, Attribute> byName = new LinkedHashMap<>();
        Map<String, Integer> relationCounts = loadUnmappedFields ? new HashMap<>() : Map.of();
        Map<String, Set<String>> mappedIndices = loadUnmappedFields ? new HashMap<>() : Map.of();
        Set<String> allIndices = loadUnmappedFields ? new HashSet<>() : Set.of();
        for (EsRelation es : relations) {
            if (loadUnmappedFields) {
                allIndices.addAll(es.concreteQualifiedIndices());
            }
            for (Attribute attr : es.output()) {
                if (Analyzer.NO_FIELDS_NAME.equals(attr.name())) {
                    continue;
                }
                Attribute existing = byName.putIfAbsent(attr.name(), attr);
                if (existing != null && existing.ignoreId().equals(attr.ignoreId()) == false) {
                    return null;
                }
                if (loadUnmappedFields) {
                    relationCounts.merge(attr.name(), 1, Integer::sum);
                    if (attr instanceof FieldAttribute fieldAttribute) {
                        mappedIndices.computeIfAbsent(attr.name(), key -> new HashSet<>())
                            .addAll(mappedIndices(fieldAttribute.field(), es));
                    }
                }
            }
        }
        if (loadUnmappedFields && allIndices.isEmpty() == false) {
            for (var entry : byName.entrySet()) {
                if (relationCounts.get(entry.getKey()) < relations.size() && entry.getValue() instanceof FieldAttribute fieldAttribute) {
                    EsField field = markPotentiallyUnmapped(
                        fieldAttribute.field(),
                        fieldAttribute.fieldName().string(),
                        mappedIndices.getOrDefault(entry.getKey(), Set.of()),
                        allIndices.size()
                    );
                    entry.setValue(fieldAttribute.withField(field));
                }
            }
        }
        return byName.isEmpty() ? Analyzer.NO_FIELDS : List.copyOf(byName.values());
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
        Map<String, EsField> properties = field.getProperties();
        if (properties.isEmpty() == false) {
            Map<String, EsField> partiallyUnmappedProperties = new LinkedHashMap<>();
            for (var entry : properties.entrySet()) {
                partiallyUnmappedProperties.put(
                    entry.getKey(),
                    markPotentiallyUnmapped(entry.getValue(), fullName + "." + entry.getKey(), mappedIndices, numberOfIndices)
                );
            }
            field = field.withProperties(partiallyUnmappedProperties);
        }
        EsField wrapped = IndexResolver.wrapIfPartiallyUnmapped(field, field.getName(), fullName, mappedIndices, numberOfIndices);
        if (wrapped instanceof PotentiallyUnmappedKeywordEsField && properties.isEmpty() == false) {
            return wrapped.withProperties(field.getProperties());
        }
        return wrapped;
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
