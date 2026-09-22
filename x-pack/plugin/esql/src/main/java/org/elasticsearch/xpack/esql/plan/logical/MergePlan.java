/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.capabilities.PostAnalysisPlanVerificationAware;
import org.elasticsearch.xpack.esql.common.Failure;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.plan.logical.join.AbstractSubqueryJoin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static org.elasticsearch.xpack.esql.analysis.Analyzer.NO_FIELDS;
import static org.elasticsearch.xpack.esql.core.expression.Expressions.toReferenceAttributesPreservingIds;

/**
 * N-ary logical plan whose children are independently planned branches and whose output is the
 * name-union of those branches. {@link Fork} (the {@code FORK} command), {@link UnionAll} (subquery
 * unions), and {@link ViewUnionAll} (view-produced unions) are the concrete forms. All of them map
 * to {@code MergeExec} at the physical layer.
 */
public abstract class MergePlan extends LogicalPlan implements PostAnalysisPlanVerificationAware, ExecutesOn.Coordinator {

    public static final int MAX_BRANCHES = 8;
    private final List<Attribute> output;

    protected MergePlan(Source source, List<LogicalPlan> children, List<Attribute> output) {
        super(source, children);
        this.output = output;
    }

    /**
     * Branch-count predicate shared by every {@link MergePlan} and any caller that wants to fail
     * earlier with a more user-facing message. Returns {@code true} if {@code count} would exceed the
     * branch cap. Centralizes the comparison so the cap can move in one place.
     */
    public static boolean exceedsMaxBranches(int count) {
        return count > MAX_BRANCHES;
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

    @Override
    public boolean expressionsResolved() {
        if (children().stream().allMatch(LogicalPlan::resolved) == false) {
            return false;
        }

        if (children().stream()
            .anyMatch(p -> p.outputSet().names().contains(Analyzer.NO_FIELDS_NAME) || output.size() != p.output().size())) {
            return false;
        }

        // All branches must output the same column names; otherwise the merge is not resolved.
        List<String> firstOutputNames = children().getFirst().output().stream().map(Attribute::name).toList();
        Holder<Boolean> resolved = new Holder<>(true);
        children().stream().skip(1).forEach(subPlan -> {
            List<String> names = subPlan.output().stream().map(Attribute::name).toList();
            if (names.equals(firstOutputNames) == false) {
                resolved.set(false);
            }
        });

        return resolved.get();
    }

    public abstract MergePlan replaceSubPlans(List<LogicalPlan> subPlans);

    public abstract MergePlan replaceSubPlansAndOutput(List<LogicalPlan> subPlans, List<Attribute> output);

    /**
     * Re-derives this merge's output from its children's, keeping everything else about the node intact.
     * <p>
     * Deliberately {@code final} and expressed in terms of {@link #replaceSubPlansAndOutput}: a per-subclass
     * implementation only has to construct the node itself, and any such implementation that names a concrete
     * constructor silently downgrades a further subclass. {@link ViewUnionAll} is the case in point — it carries a
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
     * (notably {@link ViewUnionAll}, which carries a named-subqueries map) overrides this method
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
     * Single-survivor collapse semantics — a {@link UnionAll}/{@link ViewUnionAll} with one
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

    protected List<Attribute> refreshedOutput() {
        return withUnmappedFieldsAttributeFromChildren(toReferenceAttributesPreservingIds(outputUnion(children()), this.output()));
    }

    private List<Attribute> withUnmappedFieldsAttributeFromChildren(List<Attribute> converted) {
        UnmappedFieldsAttribute ufa = unmappedFieldsAttributeFromChildren();
        if (ufa == null) {
            return converted;
        }
        for (int i = 0; i < converted.size(); i++) {
            if (converted.get(i).name().equals(UnmappedFieldsAttribute.ATTRIBUTE_NAME)) {
                // Keep the subtype so coordinator expansion can find $$unmapped_fields after the merge.
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

                if (names.contains(attr.name()) == false && attr != NO_FIELDS.getFirst()) {
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
    public BiConsumer<LogicalPlan, Failures> postAnalysisPlanVerification() {
        return MergePlan::checkBranchCount;
    }

    /**
     * Branch-count bounds shared by all {@link MergePlan} subclasses (Fork, UnionAll, ViewUnionAll).
     * Lives at post-analysis verification rather than the constructor so that compaction
     * passes (e.g. ViewCompaction) get a chance to reduce the count first. Called from both
     * {@code Fork::checkFork} and {@code UnionAll::checkUnionAll} since each subclass dispatches
     * to its own {@link #postAnalysisPlanVerification()} override.
     * <p>
     * The lower bound (≥ 1 branch) catches invalid plans where {@link #pruneEmptyBranches}
     * removed every branch — e.g. a CCS subquery whose {@code IndexResolution} came back
     * {@code EMPTY_SUBQUERY} for every sibling. The {@code PruneEmptyMergeBranches} optimizer
     * rule short-circuits this case to a {@code LocalRelation}; rules that don't (the analyzer's
     * {@code PruneEmptyUnionAllBranch}, {@code ViewCompaction.stripViewShadowRelations}) rely on
     * this check to surface the bad state with a clear message rather than letting an empty
     * {@code MergePlan} propagate silently.
     */
    static void checkBranchCount(LogicalPlan plan, Failures failures) {
        if (plan instanceof MergePlan merge) {
            int size = merge.children().size();
            if (exceedsMaxBranches(size)) {
                failures.add(Failure.fail(merge, "FORK supports up to {} branches, got: {}", MAX_BRANCHES, size));
            } else if (size == 0) {
                failures.add(Failure.fail(merge, "{} requires at least one branch", merge.getClass().getSimpleName()));
            }
        }
    }

    /**
     * Traverses the plan tree downward, invoking {@code action} for each {@link MergePlan} encountered,
     * but does not descend into the right-hand side (subquery plan) of an {@link AbstractSubqueryJoin}.
     * The right side is a separate query scope; a FORK or merge inside it is independent of any FORK
     * or merge in the enclosing query.
     */
    static void forEachMergePlanSkippingSubqueries(LogicalPlan plan, Consumer<MergePlan> action) {
        if (plan instanceof MergePlan merge) {
            action.accept(merge);
        }
        List<LogicalPlan> children = plan instanceof AbstractSubqueryJoin join ? List.of(join.left()) : plan.children();
        for (LogicalPlan child : children) {
            forEachMergePlanSkippingSubqueries(child, action);
        }
    }
}
