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
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.capabilities.PostAnalysisPlanVerificationAware;
import org.elasticsearch.xpack.esql.common.Failure;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.AnyNullIsNull;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.stream.IntStream;

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

    protected List<Attribute> refreshedOutput() {
        List<Attribute> converted = toReferenceAttributesPreservingIds(outputUnion(children()), this.output());
        // LOAD_ALL stamps _unmapped_fields; preserve type-conflict UnsupportedAttributes that alignment Eval-null'd in children.
        if (unmappedFieldsAttributeFromChildren() != null) {
            converted = keepExistingUnsupportedAttributes(converted, this.output());
        }
        converted = withScoreAttributeFromChildren(children(), converted);
        return withUnmappedFieldsAttributeFromChildren(converted);
    }

    /**
     * Whether {@link #withScoreAttributeFromChildren} may merge {@code _score} as a {@link MetadataAttribute}. {@link Fork}
     * opts out for now and keeps merging it as a plain reference.
     */
    protected boolean mergesScoreAsMetadata() {
        return true;
    }

    /**
     * Re-classes the merged {@code _score} column of {@code converted} according to what the {@code branches} put in it.
     * <p>
     * {@link Expressions#toReferenceAttributesPreservingIds} turns every merged column into a {@link ReferenceAttribute},
     * but for {@code _score} the attribute class carries a contract: a {@link MetadataAttribute} is the live score that a
     * scoring search further up adds to (see {@code LocalExecutionPlanner#planFilter}), whereas a reference is a value the
     * user computed, which scoring leaves alone. Erasing the class at the merge silently drops the score of every search
     * after it. The class is decided from all branches, not just the first one {@link #outputUnion} keeps for the name,
     * because the column's provenance differs per branch:
     * <ul>
     *   <li>a branch that asked for {@code METADATA _score} contributes the live score;</li>
     *   <li>a branch whose {@code _score} holds nothing but nulls has no say: branch alignment filled the column in because
     *   a sibling has it, or the user wrote {@code EVAL _score = null}, and either way its rows stay {@code null} through
     *   later scoring (see {@code ScoreOperator});</li>
     *   <li>any other branch computed its own {@code _score}, which later scoring must not add to.</li>
     * </ul>
     * The merged column is a {@link MetadataAttribute} when at least one branch contributes the live score and none computed
     * its own, and a reference otherwise. The id is preserved either way, so the alignment projections in the branches and
     * the plans above the merge keep resolving against it.
     *
     * @param branches the branches as they will be after this output is installed, which may differ from {@link #children()}
     *                 while the analyzer is still aligning them
     */
    public List<Attribute> withScoreAttributeFromChildren(List<LogicalPlan> branches, List<Attribute> converted) {
        if (mergesScoreAsMetadata() == false) {
            return converted;
        }
        int index = IntStream.range(0, converted.size())
            .filter(i -> converted.get(i).name().equals(MetadataAttribute.SCORE))
            .findFirst()
            .orElse(-1);
        if (index < 0) {
            return converted;
        }

        boolean live = false;
        boolean userDefined = false;
        for (LogicalPlan branch : branches) {
            switch (branchScore(branch)) {
                case USER_DEFINED -> userDefined = true;
                case LIVE -> live = true;
                case EMPTY -> {
                }
            }
        }
        Attribute merged = converted.get(index);
        boolean asMetadata = live && userDefined == false;
        if (asMetadata == MetadataAttribute.isScoreAttribute(merged)) {
            return converted;
        }
        Attribute replacement = asMetadata
            ? new MetadataAttribute(
                merged.source(),
                MetadataAttribute.SCORE,
                merged.dataType(),
                merged.nullable(),
                merged.id(),
                merged.synthetic(),
                false
            )
            : new ReferenceAttribute(
                merged.source(),
                null,
                MetadataAttribute.SCORE,
                merged.dataType(),
                merged.nullable(),
                merged.id(),
                merged.synthetic()
            );
        List<Attribute> result = new ArrayList<>(converted);
        result.set(index, replacement);
        return result;
    }

    /** What a branch puts in its {@code _score} column, see {@link #withScoreAttributeFromChildren}. */
    private enum BranchScore {
        LIVE,
        EMPTY,
        USER_DEFINED
    }

    private static BranchScore branchScore(LogicalPlan branch) {
        for (Attribute attr : branch.output()) {
            if (attr.name().equals(MetadataAttribute.SCORE)) {
                if (MetadataAttribute.isScoreAttribute(attr)) {
                    return BranchScore.LIVE;
                }
                return producesOnlyNull(branch).test(attr) ? BranchScore.EMPTY : BranchScore.USER_DEFINED;
            }
        }
        // Not aligned yet: the column will be null-filled.
        return BranchScore.EMPTY;
    }

    /**
     * Whether a column of {@code branch}'s output holds nothing but nulls. Branch alignment fills a column a branch
     * lacks this way, so that every branch outputs the same names; a column written as an explicit
     * {@code EVAL x = null} is indistinguishable and equally empty, so both are treated alike.
     * <p>
     * Matched on the attribute's id rather than its name: a branch may assign the name more than once, and only the
     * assignment this attribute came from decides what the branch outputs. Matching by name would let an assignment
     * a later one shadows answer for the column. The assignment is looked for in {@link Eval}s and, since the optimizer
     * folds an alignment {@link Eval} into the {@link Project} above it, in {@link Project}s as well.
     * <p>
     * Null-only values are followed through the branch, bottom-up: an alias of a null-only attribute is null-only too,
     * which covers {@code RENAME} and {@code EVAL x = y}, and a nested {@link MergePlan}'s output column is null-only
     * when that column is null-only in every one of its branches. A nested union whose branches all null-fill a column
     * outputs it as a fresh attribute that no alias defines, so without the latter it would look like a computed value.
     * <p>
     * Walks {@code branch} once, so build one predicate per branch and test every column of a wide branch against it.
     */
    public static Predicate<Attribute> producesOnlyNull(LogicalPlan branch) {
        Set<NameId> onlyNull = new HashSet<>();
        branch.forEachUp(plan -> {
            switch (plan) {
                case Eval eval -> eval.fields().forEach(alias -> markIfOnlyNull(alias, onlyNull));
                case Project project -> project.projections().forEach(ne -> {
                    if (ne instanceof Alias alias) {
                        markIfOnlyNull(alias, onlyNull);
                    }
                });
                case MergePlan merge -> {
                    if (merge.children().isEmpty() == false) {
                        for (Attribute out : merge.output()) {
                            if (merge.children().stream().allMatch(child -> columnOnlyNull(child, out.name(), onlyNull))) {
                                onlyNull.add(out.id());
                            }
                        }
                    }
                }
                default -> {
                }
            }
        });
        return attr -> onlyNull.contains(attr.id());
    }

    private static void markIfOnlyNull(Alias alias, Set<NameId> onlyNull) {
        if (isGuaranteedNull(alias.child(), onlyNull)) {
            onlyNull.add(alias.id());
        }
    }

    /** Whether {@code plan} outputs a column named {@code name} that holds nothing but nulls. */
    private static boolean columnOnlyNull(LogicalPlan plan, String attrName, Set<NameId> onlyNull) {
        for (Attribute attr : plan.output()) {
            if (attr.name().equals(attrName)) {
                return onlyNull.contains(attr.id());
            }
        }
        return false;
    }

    /**
     * {@link Expressions#isGuaranteedNull}, also accepting an attribute already known to be null-only and looking through
     * functions that are null whenever an argument is: a typed null such as {@code null::double}, which is how a null
     * gets the type of the sibling branches' column, is a cast of a null literal until the optimizer folds it.
     */
    private static boolean isGuaranteedNull(Expression e, Set<NameId> onlyNull) {
        if (Expressions.isGuaranteedNull(e) || (e instanceof Attribute attr && onlyNull.contains(attr.id()))) {
            return true;
        }
        return e instanceof AnyNullIsNull && e.children().stream().anyMatch(child -> isGuaranteedNull(child, onlyNull));
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
