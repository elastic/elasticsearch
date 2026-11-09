/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.xpack.esql.analysis.InSubqueryResolver;
import org.elasticsearch.xpack.esql.capabilities.PostAnalysisPlanVerificationAware;
import org.elasticsearch.xpack.esql.common.Failure;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Predicate;

import static org.elasticsearch.xpack.esql.analysis.Analyzer.NO_FIELDS;
import static org.elasticsearch.xpack.esql.core.expression.Expressions.keepExistingUnsupportedAttributes;
import static org.elasticsearch.xpack.esql.core.expression.Expressions.toReferenceAttributesPreservingIds;

/**
 * N-ary logical plan whose children are independently planned branches and whose output is the
 * name-union of those branches. {@link Fork} (the {@code FORK} command), {@link UnionAll} (subquery
 * unions), and {@link BranchPlan} (view-produced unions) are the concrete forms. All of them map
 * to {@code MergeExec} at the physical layer.
 */
public abstract class MergePlan extends LogicalPlan implements PostAnalysisPlanVerificationAware, ExecutesOn.Coordinator {

    private final List<Attribute> output;

    protected MergePlan(Source source, List<LogicalPlan> children, List<Attribute> output) {
        super(source, children);
        this.output = output;
    }

    @Override
    public abstract LogicalPlan replaceChildren(List<LogicalPlan> newChildren);

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("not serialized");
    }

    /**
     * A merge is resolved once its branches are and its output has a column for every column a branch produces. The branches do not
     * have to line up with the output: a branch lacking a column contributes nulls to it, and the optimizer materializes that
     * alignment (see {@link MergeBranchAlignment}). Until then, the output is what the rest of the plan reads.
     * <p>
     * A branch column that is never a merge column does not count: the no-fields marker and IN-subquery marks. Neither does a
     * synthetic attribute that appears in a branch once the merge has columns; it only becomes a merge column if a rule that owns it
     * puts it in the output explicitly (e.g. a union-type conversion pushed into the branches).
     */
    @Override
    public boolean expressionsResolved() {
        if (children().stream().allMatch(LogicalPlan::resolved) == false) {
            return false;
        }
        Set<String> outputNames = new HashSet<>(output.size());
        for (Attribute attr : output) {
            outputNames.add(attr.name());
        }
        for (LogicalPlan child : children()) {
            for (Attribute attr : child.output()) {
                if (outputNames.contains(attr.name()) == false && isMergeColumnCandidate(attr, output)) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Whether a branch column {@code attr} that is not in the merge's {@code output} has to become one of its columns; see
     * {@link #expressionsResolved()}.
     */
    public static boolean isMergeColumnCandidate(Attribute attr, List<Attribute> output) {
        if (attr == NO_FIELDS.getFirst() || InSubqueryResolver.isInSubquerySynthetic(attr)) {
            return false;
        }
        return output.isEmpty() || attr.synthetic() == false;
    }

    /**
     * The columns a merge with branches {@code subplans} and output {@code previous} has once its output is brought up to date with
     * the branches: {@link #outputUnion} of the branches, without the branch columns that {@link #isMergeColumnCandidate} rules
     * out, in the order of {@link #inOrderOf}.
     */
    public static List<Attribute> mergeColumns(List<LogicalPlan> subplans, List<Attribute> previous) {
        Set<String> previousNames = new HashSet<>(previous.size());
        for (Attribute attr : previous) {
            previousNames.add(attr.name());
        }
        List<Attribute> union = new ArrayList<>(outputUnion(subplans));
        union.removeIf(attr -> previousNames.contains(attr.name()) == false && isMergeColumnCandidate(attr, previous) == false);
        return inOrderOf(union, previous);
    }

    /**
     * {@code union}, a merge's new columns (as from {@link #outputUnion}), ordered like the merge's {@code previous} output: the
     * columns it already had keep their position, and new ones come after them, in {@code union}'s order. Once a merge's columns
     * are first determined, later changes to its branches (which can reorder a branch's own output, e.g. an {@code Eval} shadowing
     * one of its columns) must not reorder the columns the query returns.
     */
    public static List<Attribute> inOrderOf(List<Attribute> union, List<Attribute> previous) {
        if (previous.isEmpty()) {
            return union;
        }
        Map<String, Attribute> byName = new LinkedHashMap<>();
        for (Attribute attr : union) {
            byName.putIfAbsent(attr.name(), attr);
        }
        List<Attribute> ordered = new ArrayList<>(union.size());
        for (Attribute attr : previous) {
            Attribute column = byName.remove(attr.name());
            if (column != null) {
                ordered.add(column);
            }
        }
        ordered.addAll(byName.values());
        return ordered;
    }

    public abstract MergePlan replaceSubPlans(List<LogicalPlan> subPlans);

    public abstract MergePlan replaceSubPlansAndOutput(List<LogicalPlan> subPlans, List<Attribute> output);

    /**
     * Re-derives this merge's output from its children's, keeping everything else about the node intact.
     * <p>
     * Deliberately {@code final} and expressed in terms of {@link #replaceSubPlansAndOutput}: a per-subclass
     * implementation only has to construct the node itself, and any such implementation that names a concrete
     * constructor silently downgrades a further subclass. {@link BranchPlan} is the case in point — it carries a
     * named-subqueries map and view-branch keys that a {@code new UnionAll(...)} here would drop, turning its view
     * boundaries back into anonymous union branches. Subclasses only need {@link #replaceSubPlansAndOutput} to be
     * faithful, which they already need for every other rewrite.
     */
    public final MergePlan refreshOutput() {
        return replaceSubPlansAndOutput(children(), refreshedOutput());
    }

    /**
     * Drop branches whose root the {@code isEmpty} predicate considers empty. Each
     * {@link MergePlan} subclass with structural invariants beyond the positional children list
     * (notably {@link BranchPlan}, which carries a named-subqueries map) overrides this method
     * to preserve those invariants.
     * <p>
     * Behaviour:
     * <ul>
     *   <li>nothing pruned → returns {@code this} (cheap no-op);</li>
     *   <li>at least one branch pruned → returns {@code replaceChildren(survivors)} with the
     *       remaining children — this includes the all-empty case, which produces a merge
     *       with zero children. The caller is expected either to short-circuit the
     *       all-empty case before calling (e.g. {@code PruneEmptyMergeBranches} replaces with
     *       a {@code LocalRelation} when every branch reduces to empty) or to let the
     *       analyzer's verifier surface the empty-merge state via {@link #checkBranchCount}.</li>
     * </ul>
     * Single-survivor collapse semantics — a {@link UnionAll}/{@link BranchPlan} with one
     * branch left is equivalent to that branch — are not part of this primitive; callers that
     * want that collapse do it explicitly (see {@code ViewCompaction.stripViewShadowRelations}).
     * A {@link Fork} with a single branch is still a {@link Fork} per FORK syntax.
     */
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
        return replaceChildren(kept);
    }

    /**
     * The output re-derived from the branches, see {@link #mergeColumns}. Once the optimizer has aligned the branches, that is also
     * their columns, in their order.
     */
    protected List<Attribute> refreshedOutput() {
        List<Attribute> converted = toReferenceAttributesPreservingIds(mergeColumns(children(), output()), this.output());
        // LOAD_ALL stamps _unmapped_fields; preserve type-conflict UnsupportedAttributes that union-type resolution Eval-null'd in
        // children.
        if (unmappedFieldsAttributeFromChildren() != null) {
            converted = keepExistingUnsupportedAttributes(converted, this.output());
        }
        return withUnmappedFieldsAttributeFromChildren(converted);
    }

    private List<Attribute> withUnmappedFieldsAttributeFromChildren(List<Attribute> converted) {
        UnmappedFieldsAttribute ufa = unmappedFieldsAttributeFromChildren();
        if (ufa == null) {
            return converted;
        }
        for (int i = 0; i < converted.size(); i++) {
            if (converted.get(i).name().equals(UnmappedFieldsAttribute.ATTRIBUTE_NAME)) {
                // Keep the subtype so that _unmapped_fields is found after the merge and returned.
                converted.set(i, ufa.withId(converted.get(i).id()));
                break;
            }
        }
        return converted;
    }

    @Nullable
    private UnmappedFieldsAttribute unmappedFieldsAttributeFromChildren() {
        UnmappedFieldsAttribute first = null;
        UnmappedFieldsPattern union = UnmappedFieldsPattern.NONE;
        for (LogicalPlan child : children()) {
            for (Attribute attr : child.output()) {
                if (attr instanceof UnmappedFieldsAttribute childUfa) {
                    if (first == null) {
                        first = childUfa;
                    }
                    union = union.union(childUfa.pattern());
                }
            }
        }
        if (first == null) {
            return null;
        }
        if (union.equals(first.pattern())) {
            return first;
        }
        return new UnmappedFieldsAttribute(first.source(), first.dataType(), first.nullable(), first.id(), first.synthetic(), union);
    }

    @Override
    public List<Attribute> output() {
        return output;
    }

    public static List<Attribute> outputUnion(List<LogicalPlan> subplans) {
        List<Attribute> output = new ArrayList<>();
        Set<String> names = new HashSet<>();
        // these are attribute names we know should have an UNSUPPORTED data type in the merge output
        Set<String> unsupportedAttributesNames = outputUnsupportedAttributeNames(subplans);

        for (var subPlan : subplans) {
            for (var attr : subPlan.output()) {
                // When we have multiple attributes with the same name, the ones that have a supported data type take priority.
                // We only add an attribute with an unsupported data type if we know that in the output of the rest of the branches
                // there exists no attribute with the same name and with a supported data type.
                if (attr.dataType() == DataType.UNSUPPORTED && unsupportedAttributesNames.contains(attr.name()) == false) {
                    continue;
                }

                // Keep union-type cast synthetics ($$field$converted_to$type): a conversion above the merge still references them. Drop
                // only IN-subquery marks/consts, which are local to the rewrite and leak if they become merge columns.
                if (names.contains(attr.name()) == false
                    && attr != NO_FIELDS.getFirst()
                    && InSubqueryResolver.isInSubquerySynthetic(attr) == false) {
                    names.add(attr.name());
                    output.add(attr);
                }
            }
        }
        return output;
    }

    /**
     * Returns a list of attribute names that will need to have the {@code UNSUPPORTED} data type in the merge output.
     * These are attributes that are either {@code UNSUPPORTED} or missing in each branch.
     * If two branches have the same attribute name, but only in one of them the data type is {@code UNSUPPORTED}, this constitutes
     * data type conflict, and so this attribute name will not be returned by this function.
     * Data type conflicts are later on checked in {@code postAnalysisPlanVerification}.
     */
    public static Set<String> outputUnsupportedAttributeNames(List<LogicalPlan> subplans) {
        Set<String> unsupportedAttributes = new HashSet<>();
        Set<String> names = new HashSet<>();

        for (var subPlan : subplans) {
            for (var attr : subPlan.output()) {
                var attrName = attr.name();
                if (unsupportedAttributes.contains(attrName) == false
                    && attr.dataType() == DataType.UNSUPPORTED
                    && names.contains(attrName) == false) {
                    unsupportedAttributes.add(attrName);
                } else if (unsupportedAttributes.contains(attrName) && attr.dataType() != DataType.UNSUPPORTED) {
                    unsupportedAttributes.remove(attrName);
                }
                names.add(attrName);
            }
        }

        return unsupportedAttributes;
    }

    @Override
    public BiConsumer<LogicalPlan, Failures> postAnalysisPlanVerification(
        AnalysisRegistry analysisRegistry,
        QueryPragmas pragmas,
        EsqlFlags flags
    ) {
        return (plan, failures) -> {
            if (plan != this) {
                return;
            }
            checkBranchCount(plan, failures, pragmas, flags);
            postAnalysisPlanVerification().accept(plan, failures);
        };
    }

    void checkBranchCount(LogicalPlan plan, Failures failures, QueryPragmas pragmas, EsqlFlags flags) {
        if (plan.children().isEmpty()) {
            failures.add(Failure.fail(plan, "{} requires at least one branch", plan.getClass().getSimpleName()));
        }
    }
}
