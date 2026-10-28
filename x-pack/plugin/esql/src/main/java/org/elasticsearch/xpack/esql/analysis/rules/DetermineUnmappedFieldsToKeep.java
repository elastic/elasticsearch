/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis.rules;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.analysis.UnmappedResolution;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.InlineStats;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MergePlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnmappedFieldsAttribute;
import org.elasticsearch.xpack.esql.plan.logical.UnmappedFieldsPattern;
import org.elasticsearch.xpack.esql.plan.logical.join.AbstractSubqueryJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.local.ResolvingProject;
import org.elasticsearch.xpack.esql.rule.ParameterizedRule;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.esql.core.expression.Expressions.keepExistingUnsupportedAttributes;
import static org.elasticsearch.xpack.esql.core.expression.Expressions.toReferenceAttributesPreservingIds;

/**
 * When {@code SET unmapped_fields="LOAD_ALL"} is in effect, annotates
 * each non-LOOKUP {@link EsRelation} with an {@link UnmappedFieldsAttribute} carrying the
 * {@link UnmappedFieldsPattern} that describes which additional (currently unmapped) source fields
 * would survive to the query output. The shard reads only the fields that pattern includes.
 *
 * <p>When the computed pattern is {@link UnmappedFieldsPattern#NONE}—e.g., a pattern-less {@code KEEP}
 * that can never let an unmapped source field through—the rule leaves the plan untouched, so data nodes
 * never load {@code _source} for it.
 *
 * <p>The rule also leaves the plan untouched when the query has a column named {@code _unmapped_fields} of its own, at any
 * node: a mapped or demand-loaded field, an {@code EVAL} or {@code RENAME} target, an {@code ENRICH} or {@code LOOKUP JOIN}
 * field. The query's column wins, and a warning says so. Adding the
 * {@link UnmappedFieldsAttribute} next to it would not work, since {@code EVAL} shadows by name and union and fork branches line
 * their columns up by name.
 *
 * <p>A view is a boundary this setting does not reach into: nothing inside a view branch is stamped or otherwise looked at, as a view
 * is a virtual index with no {@code _source} of its own. A view has no {@code _unmapped_fields} column, so a union that has one
 * from another branch null-fills it for the view, like for any branch that cannot surface it.
 *
 * <p>{@link MergePlan} is n-ary: each branch is stamped with its own pattern. Named unmapped mentions are aligned
 * like {@code LOAD}: a mention in one branch is materialized in every sibling that can surface it, so that
 * name is excluded from {@code _unmapped_fields}. Extra fields that a branch does not keep (literal
 * {@code KEEP}, {@code STATS}) are not loaded there; a null {@code _unmapped_fields} is appended so the
 * coordinator can still return extras from siblings.
 *
 * <p>Alignment {@link Project}s created during analysis snapshot their projections before this rule runs, so
 * the attribute is re-appended there and {@link MergePlan#refreshOutput()} unions it for the coordinator.
 *
 * <p>The rule runs in the Finish Analysis batch <em>before</em> {@link ResolvedProjects}, so
 * {@link ResolvingProject} nodes — which carry the original wildcard patterns — are still present.
 * For any other {@link UnmappedResolution} the rule is a no-op.
 */
public class DetermineUnmappedFieldsToKeep extends ParameterizedRule<LogicalPlan, LogicalPlan, AnalyzerContext> {
    /** Header warning for a query with a column named like the one of unmapped fields, which therefore is not returned. */
    public static final String UNMAPPED_FIELDS_NAME_TAKEN_WARNING = "The query has a column named ["
        + UnmappedFieldsAttribute.ATTRIBUTE_NAME
        + "], so the unmapped fields are not returned in a column of that name; rename that column to get them";

    @Override
    public LogicalPlan apply(LogicalPlan plan, AnalyzerContext context) {
        if (context.unmappedResolution().loadsAllUnmappedFields() == false) {
            return plan;
        }
        // Finish Analysis still runs if Resolution left names unresolved (DROP hid a field the outer query
        // still mentions). output() on those projections throws UnresolvedException; skip so the Verifier
        // can report Unknown column.
        if (plan.resolved() == false) {
            return plan;
        }
        if (hasColumnNamedLikeUnmappedFields(plan)) {
            // Warn even if no unmapped field could have been returned anyway, e.g. after an exact KEEP: the column of that name is
            // then the query's own, which is not what a user reading its name expects.
            context.deferredHeaderWarnings().add(UNMAPPED_FIELDS_NAME_TAKEN_WARNING);
            return plan;
        }
        UnmappedFieldsPattern pattern = computeUnmappedFieldsToKeep(plan);
        LogicalPlan result;
        if (plan.noneMatch(p -> p instanceof MergePlan || p instanceof AbstractSubqueryJoin)) {
            result = stampAll(plan).transformUp(Project.class, DetermineUnmappedFieldsToKeep::passThroughUnmappedFields);
        } else if (pattern.isNone()) {
            // Exact KEEP/STATS above a merge must not stamp or pass _unmapped_fields through: alignment
            // Projects snapshot before this rule, and replaceChild on a ResolvingProject would re-append it.
            return plan;
        } else {
            // Project pass before merge finish keeps _unmapped_fields on branch Projects. The pass after
            // picks it up on Projects above the merge, whose child output only includes the column after refresh.
            LogicalPlan annotated = annotate(plan, pattern);
            result = ViewUtils.transformUpOutsideViews(annotated, Project.class, DetermineUnmappedFieldsToKeep::passThroughUnmappedFields);
            result = ViewUtils.transformUpOutsideViews(result, MergePlan.class, DetermineUnmappedFieldsToKeep::finishMergeUnmappedFields);
            result = ViewUtils.transformUpOutsideViews(result, Project.class, DetermineUnmappedFieldsToKeep::passThroughUnmappedFields);
        }
        return result;
    }

    /**
     * Whether the query has a column named like {@code _unmapped_fields} of its own, at any node. A view contributes the column it
     * outputs, not whatever its body had along the way.
     */
    private static boolean hasColumnNamedLikeUnmappedFields(LogicalPlan plan) {
        return ViewUtils.anyMatchOutsideViews(
            plan,
            p -> p.output().stream().anyMatch(a -> a.name().equals(UnmappedFieldsAttribute.ATTRIBUTE_NAME))
        );
    }

    /**
     * Computes the {@link UnmappedFieldsPattern} describing which additional (currently unmapped)
     * source fields would survive to the output of {@code plan}.
     * <p>
     * Two things restrict the pattern. KEEP/DROP (as {@link ResolvingProject}) contribute the
     * include/exclude patterns they were written with: each one adds a single OR group, while
     * {@link UnmappedFieldsPattern#intersect} applies AND across chained commands. And every name that any
     * node in the plan outputs is excluded: a mapped field, a name the query introduced (EVAL's aliases,
     * RENAME's targets, ENRICH/LOOKUP JOIN fields) are all already columns of their own, so returning a
     * source field of that name in {@code _unmapped_fields} as well would collide with them.
     * <p>
     * For {@link Join}, only the left side is recursed into. {@link MergePlan} is treated as transparent (no include restriction beyond
     * excluding its output names); branch-local {@code KEEP}/{@code DROP} patterns are applied in {@link #annotate} by recomputing
     * this for each child. Other non-unary plans fall back to {@link UnmappedFieldsPattern#ALL}; those queries are rejected by the
     * {@code Verifier}'s {@code LOAD_ALL} command allow-list.
     */
    private static UnmappedFieldsPattern computeUnmappedFieldsToKeep(LogicalPlan plan) {
        if (plan instanceof Aggregate) {
            return UnmappedFieldsPattern.NONE;
        }
        UnmappedFieldsPattern fromChild = switch (plan) {
            // INLINE STATS preserves input rows via a left join with its Aggregate, which is also its child - so walk the
            // input, i.e. the grandchild. Recursing into the Aggregate would return NONE, which is right for STATS
            // (the unmapped fields can be dropped) but not here.
            case InlineStats inlineStats -> computeUnmappedFieldsToKeep(inlineStats.aggregate().child());
            case UnaryPlan unary -> computeUnmappedFieldsToKeep(unary.child());
            // Only the left side can carry the _unmapped_fields column: apply() skips IndexMode.LOOKUP
            // relations, so the right-hand lookup index never contributes unmapped source fields.
            case Join join -> computeUnmappedFieldsToKeep(join.left());
            default -> UnmappedFieldsPattern.ALL;
        };
        UnmappedFieldsPattern restricted = plan instanceof ResolvingProject project
            ? project.unmappedFieldsPattern().intersect(fromChild)
            : fromChild;
        return restricted.withAdditionalExcludes(Expressions.names(plan.output()));
    }

    /**
     * No {@link MergePlan} or {@link AbstractSubqueryJoin} in the plan: one pattern for the whole query,
     * stamped onto every non-LOOKUP {@link EsRelation} in a single {@code transformUp}.
     */
    private static LogicalPlan stampAll(LogicalPlan plan) {
        UnmappedFieldsPattern pattern = computeUnmappedFieldsToKeep(plan);
        return pattern.isNone() ? plan : plan.transformUp(EsRelation.class, esr -> stamp(esr, pattern));
    }

    /**
     * Stamps {@link UnmappedFieldsAttribute} onto non-LOOKUP {@link EsRelation}s. {@link MergePlan} is
     * n-ary: each branch is annotated with its own pattern. {@link AbstractSubqueryJoin} is binary: join
     * output is the left side. The right is an independently executed key subquery, so it is annotated
     * with {@link UnmappedFieldsPattern#NONE} rather than its own KEEP pattern, otherwise
     * {@code _unmapped_fields} becomes an extra output column after IN arity was already accepted.
     * Every other node is only walked to reach those; recursion stops at a union so a parent
     * pattern cannot stamp through it.
     */
    private static LogicalPlan annotate(LogicalPlan plan, UnmappedFieldsPattern pattern) {
        if (plan instanceof MergePlan merge) {
            List<LogicalPlan> children = merge.children();
            List<LogicalPlan> newChildren = new ArrayList<>(children.size());
            for (int i = 0; i < children.size(); i++) {
                LogicalPlan child = children.get(i);
                if (ViewUtils.isViewBody(merge, i)) {
                    // A view is a virtual index with no _source of its own, and the setting ends at its boundary: it has no
                    // _unmapped_fields column, and where a sibling has one, finishMergeUnmappedFields null-fills the view's.
                    newChildren.add(child);
                } else {
                    LogicalPlan annotated = annotate(child, computeUnmappedFieldsToKeep(child).intersect(pattern));
                    newChildren.add(annotated instanceof Project project ? passThroughUnmappedFields(project) : annotated);
                }
            }
            return merge.replaceChildren(newChildren);
        }
        if (plan instanceof AbstractSubqueryJoin join) {
            return join.replaceChildren(annotate(join.left(), pattern), annotate(join.right(), UnmappedFieldsPattern.NONE));
        }
        if (pattern.isNone()) {
            return plan;
        }
        if (plan instanceof EsRelation esr) {
            return stamp(esr, pattern);
        }
        if (plan.noneMatch(p -> p instanceof MergePlan || p instanceof AbstractSubqueryJoin)) {
            return plan.transformUp(EsRelation.class, esr -> stamp(esr, pattern));
        }
        return plan.replaceChildren(plan.children().stream().map(c -> annotate(c, pattern)).toList());
    }

    private static EsRelation stamp(EsRelation esr, UnmappedFieldsPattern pattern) {
        return pattern.isNone() || esr.indexMode() == IndexMode.LOOKUP
            ? esr
            : esr.withAdditionalAttribute(new UnmappedFieldsAttribute(Source.EMPTY, pattern));
    }

    /**
     * Union alignment {@link Project}s snapshot their projections before this rule adds
     * {@code _unmapped_fields}. {@link ResolvingProject} already re-appends it; other Projects
     * would otherwise drop the column before it is returned.
     */
    private static Project passThroughUnmappedFields(Project project) {
        if (project instanceof ResolvingProject rp && rp.unmappedFieldsPattern().isNone()) {
            return project;
        }
        var noStaleFields = dropStaleNoFields(project);
        List<UnmappedFieldsAttribute> unmapped = CollectionUtils.collect(noStaleFields.child().output(), UnmappedFieldsAttribute.class);
        if (unmapped.isEmpty()) {
            return noStaleFields;
        }
        Set<String> names = new HashSet<>(Expressions.names(noStaleFields.projections()));
        List<UnmappedFieldsAttribute> missing = unmapped.stream()
            .filter(attr -> names.contains(attr.name()) == false)
            .collect(Collectors.toList());
        return noStaleFields.withAdditionalProjections(missing);
    }

    /** Drop {@code <no-fields>} from a KEEP * once the relation has real columns. */
    private static Project dropStaleNoFields(Project project) {
        var kept = project.projections().stream().filter(projection -> Analyzer.NO_FIELDS_NAME.equals(projection.name()) == false).toList();
        return kept.size() < project.projections().size() ? project.withProjections(kept) : project;
    }

    /**
     * Branches whose pattern is {@link UnmappedFieldsPattern#NONE} never get {@code _unmapped_fields} on the relation. If a sibling did,
     * append a null column so union layouts match, then refresh union output so the coordinator sees the {@link UnmappedFieldsAttribute}
     * subtype. In other words, we only pad if at least one child has the attribute and at least one does not.
     */
    private static LogicalPlan finishMergeUnmappedFields(MergePlan merge) {
        return switch (merge) {
            case UnionAll unionAll -> alignUnmappedFields(unionAll);
            case Fork fork -> finishForkUnmappedFields(fork);
            default -> throw new IllegalStateException(Strings.format("unexpected MergePlan [%s]", merge.getClass().getSimpleName()));
        };
    }

    private static LogicalPlan finishForkUnmappedFields(Fork fork) {
        List<LogicalPlan> children = fork.children();
        List<LogicalPlan> newChildren = new ArrayList<>(children.size());
        boolean hasChildWithUnmappedFields = false;
        boolean hasChildWithoutUnmappedFields = false;
        for (LogicalPlan child : children) {
            boolean hasUnmappedField = Expressions.names(child.output()).contains(UnmappedFieldsAttribute.ATTRIBUTE_NAME);
            hasChildWithUnmappedFields |= hasUnmappedField;
            hasChildWithoutUnmappedFields |= hasUnmappedField == false;
            newChildren.add(hasUnmappedField ? child : padNullUnmappedFields(child));
        }
        MergePlan result = hasChildWithoutUnmappedFields && hasChildWithUnmappedFields ? fork.replaceSubPlans(newChildren) : fork;
        return hasChildWithUnmappedFields ? result.refreshOutput() : result;
    }

    private static Eval padNullUnmappedFields(LogicalPlan child) {
        Alias alias = new Alias(Source.EMPTY, UnmappedFieldsAttribute.ATTRIBUTE_NAME, new Literal(Source.EMPTY, null, DataType.SOURCE));
        return new Eval(Source.EMPTY, child, List.of(alias));
    }

    /**
     * Null-fills {@code _unmapped_fields} in branches that cannot surface it and rebuilds the
     * {@link UnionAll} output, appending the {@link UnmappedFieldsAttribute} after conversion.
     * {@link MergePlan#refreshOutput()} unions through the pre-annotation output snapshot, which
     * does not yet contain the attribute, so it would be dropped.
     */
    private static UnionAll alignUnmappedFields(UnionAll unionAll) {
        UnmappedFieldsAttribute unmapped = unionFrom(unionAll);
        if (unmapped == null) {
            return unionAll;
        }
        List<LogicalPlan> newChildren = new ArrayList<>(unionAll.children().size());
        boolean childrenChanged = false;
        for (LogicalPlan child : unionAll.children()) {
            var pad = child.outputSet().names().contains(UnmappedFieldsAttribute.ATTRIBUTE_NAME) == false;
            newChildren.add(pad ? padNullUnmappedFields(child) : child);
            childrenChanged |= pad;
        }
        UnionAll withChildren = childrenChanged ? unionAll.replaceSubPlans(newChildren) : unionAll;
        List<Attribute> withoutUnmapped = MergePlan.outputUnion(withChildren.children())
            .stream()
            .filter(attr -> attr.name().equals(UnmappedFieldsAttribute.ATTRIBUTE_NAME) == false)
            .toList();
        List<Attribute> newOutput = CollectionUtils.combine(
            keepExistingUnsupportedAttributes(toReferenceAttributesPreservingIds(withoutUnmapped, unionAll.output()), unionAll.output()),
            unmapped
        );
        return childrenChanged == false && newOutput.equals(unionAll.output())
            ? unionAll
            : withChildren.replaceSubPlansAndOutput(withChildren.children(), newOutput);
    }

    /** {@code null} when no branch exposes {@code _unmapped_fields}. */
    private static @Nullable UnmappedFieldsAttribute unionFrom(UnionAll unionAll) {
        UnmappedFieldsAttribute first = null;
        UnmappedFieldsPattern union = UnmappedFieldsPattern.NONE;
        for (LogicalPlan child : unionAll.children()) {
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
        return union.equals(first.pattern())
            ? first
            : new UnmappedFieldsAttribute(first.source(), first.dataType(), first.nullable(), first.id(), first.synthetic(), union);
    }
}
