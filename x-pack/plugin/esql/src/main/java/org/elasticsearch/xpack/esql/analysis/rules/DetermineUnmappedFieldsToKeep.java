/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis.rules;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.analysis.UnmappedFieldsOrdering;
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
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnmappedFieldsAttribute;
import org.elasticsearch.xpack.esql.plan.logical.UnmappedFieldsPattern;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.local.ResolvingProject;
import org.elasticsearch.xpack.esql.rule.ParameterizedRule;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * When {@code SET unmapped_fields="LOAD_ALL"} is in effect, annotates
 * each non-LOOKUP {@link EsRelation} with an {@link UnmappedFieldsAttribute} carrying the
 * {@link UnmappedFieldsPattern} that describes which additional (currently unmapped) source fields
 * would survive to the query output. Expanding the {@code _unmapped_fields} column into per-field
 * output columns is a coordinator-level post-processing step and does not affect data-node execution.
 *
 * <p>When the computed pattern is {@link UnmappedFieldsPattern#NONE}—e.g., a pattern-less {@code KEEP}
 * that can never let an unmapped source field through—the rule leaves the plan untouched, so data nodes
 * never load {@code _source} for expansion.
 *
 * <p>{@code FORK} is n-ary: each branch is stamped with its own pattern. Named unmapped mentions are aligned
 * like {@code LOAD}: a mention in one branch is materialized in every sibling that can surface it, so that
 * name is excluded from {@code $$unmapped_fields}. Extra fields that a branch does not keep (literal
 * {@code KEEP}, {@code STATS}) are not loaded there; a null {@code $$unmapped_fields} is appended so the
 * coordinator can still expand extras from siblings.
 *
 * <p>Alignment {@link Project}s created during analysis snapshot their projections before this rule runs, so
 * the attribute is re-appended there and {@link Fork#refreshOutput()} unions it for the coordinator.
 *
 * <p>The rule runs in the Finish Analysis batch <em>before</em> {@link ResolvedProjects}, so
 * {@link ResolvingProject} nodes — which carry the original wildcard patterns — are still present.
 * For any other {@link UnmappedResolution} the rule is a no-op.
 */
public class DetermineUnmappedFieldsToKeep extends ParameterizedRule<LogicalPlan, LogicalPlan, AnalyzerContext> {

    private final Consumer<UnmappedFieldsOrdering> registerUnmappedFieldsOrdering;

    public DetermineUnmappedFieldsToKeep(Consumer<UnmappedFieldsOrdering> registerUnmappedFieldsOrdering) {
        this.registerUnmappedFieldsOrdering = registerUnmappedFieldsOrdering;
    }

    @Override
    public LogicalPlan apply(LogicalPlan plan, AnalyzerContext context) {
        if (context.unmappedResolution().loadsAllUnmappedFields() == false) {
            return plan;
        }
        boolean hasFork = plan.anyMatch(p -> p instanceof Fork fork && isForkCommand(fork));
        LogicalPlan annotated = hasFork ? annotate(plan, computeUnmappedFieldsToKeep(plan)) : stampAll(plan);
        LogicalPlan withUnmappedOnProjects = annotated.transformUp(Project.class, DetermineUnmappedFieldsToKeep::passThroughUnmappedFields);
        LogicalPlan result = hasFork
            // Project pass before FORK finish keeps $$unmapped_fields on branch Projects. The pass after
            // picks it up on Projects above the FORK, whose child output only includes the column after refresh.
            ? withUnmappedOnProjects.transformUp(Fork.class, DetermineUnmappedFieldsToKeep::finishForkUnmappedFields)
                .transformUp(Project.class, DetermineUnmappedFieldsToKeep::passThroughUnmappedFields)
            : withUnmappedOnProjects;
        if (carriesUnmappedFieldsAttribute(result)) {
            registerUnmappedFieldsOrdering.accept(leaves -> withLeavesInPlaceOfSyntheticColumn(result, leaves).output());
        }
        return result;
    }

    /**
     * The plan with {@code leaves} standing in for the synthetic column, so asking it for its output re-runs every projection
     * against a relation shaped exactly as it would have been had those fields been mapped: {@code ResolvingProject#replaceChild}
     * re-invokes the real KEEP/DROP/RENAME resolvers, and EVAL and friends recompute their output on top.
     * {@link Fork} snapshots its output, so the leaves are spliced into that list in place of {@code $$unmapped_fields}.
     * {@link Fork#refreshOutput()} cannot do this: it mints new {@link org.elasticsearch.xpack.esql.core.expression.NameId}s,
     * and expansion matches discovered fields by id.
     */
    private static LogicalPlan withLeavesInPlaceOfSyntheticColumn(LogicalPlan annotated, List<Attribute> leaves) {
        return annotated.transformUp(EsRelation.class, esr -> {
            List<Attribute> realAttributes = new ArrayList<>(esr.output().size());
            boolean carriesSyntheticColumn = false;
            for (Attribute a : esr.output()) {
                if (a instanceof UnmappedFieldsAttribute) {
                    carriesSyntheticColumn = true;
                } else {
                    realAttributes.add(a);
                }
            }
            return carriesSyntheticColumn ? esr.withAttributes(realAttributes).withAdditionalAttributes(leaves) : esr;
        }).transformUp(Fork.class, fork -> replaceSyntheticColumnInForkOutput(fork, leaves));
    }

    private static Fork replaceSyntheticColumnInForkOutput(Fork fork, List<Attribute> leaves) {
        List<Attribute> newOutput = new ArrayList<>(fork.output().size() + leaves.size());
        boolean replaced = false;
        for (Attribute attr : fork.output()) {
            if (attr instanceof UnmappedFieldsAttribute || attr.name().equals(UnmappedFieldsAttribute.ATTRIBUTE_NAME)) {
                if (replaced) {
                    throw new IllegalStateException(
                        "expected at most one " + UnmappedFieldsAttribute.ATTRIBUTE_NAME + " in FORK output, got " + fork.output()
                    );
                }
                newOutput.addAll(leaves);
                replaced = true;
            } else {
                newOutput.add(attr);
            }
        }
        return replaced ? fork.replaceSubPlansAndOutput(fork.children(), newOutput) : fork;
    }

    private static boolean carriesUnmappedFieldsAttribute(LogicalPlan plan) {
        return plan.anyMatch(p -> p instanceof EsRelation esr && esr.output().stream().anyMatch(a -> a instanceof UnmappedFieldsAttribute));
    }

    /**
     * Computes the {@link UnmappedFieldsPattern} describing which additional (currently unmapped)
     * source fields would survive to the output of {@code plan}.
     * <p>
     * Two things restrict the pattern. KEEP/DROP (as {@link ResolvingProject}) contribute the
     * include/exclude patterns they were written with: each one adds a single OR group, while
     * {@link UnmappedFieldsPattern#intersect} applies AND across chained commands. And every name that any
     * node in the plan outputs is excluded: a mapped field, a name the query introduced (EVAL's aliases,
     * RENAME's targets, ENRICH/LOOKUP JOIN fields) are all already columns of their own, so expanding a
     * source field of that name would collide with them.
     * <p>
     * For {@link Join}, only the left side is recursed into. {@link Fork} is treated as transparent (no include restriction beyond
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
            // (expansion can be dropped) but not here.
            case InlineStats inlineStats -> computeUnmappedFieldsToKeep(inlineStats.aggregate().child());
            case UnaryPlan unary -> computeUnmappedFieldsToKeep(unary.child());
            // Only the left side can carry the $$unmapped_fields column: apply() skips IndexMode.LOOKUP
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
     * No {@link Fork} in the plan: one pattern for the whole query, stamped onto every non-LOOKUP
     * {@link EsRelation} in a single {@code transformUp}.
     */
    private static LogicalPlan stampAll(LogicalPlan plan) {
        UnmappedFieldsPattern pattern = computeUnmappedFieldsToKeep(plan);
        return pattern.isNone() ? plan : plan.transformUp(EsRelation.class, esr -> stamp(esr, pattern));
    }

    /**
     * Stamps {@link UnmappedFieldsAttribute} onto non-LOOKUP {@link EsRelation}s. {@link Fork} is the
     * other special case: each branch is annotated with its own pattern. Every other node is only
     * walked to reach those two; recursion stops at a Fork so a parent pattern cannot stamp through it.
     */
    private static LogicalPlan annotate(LogicalPlan plan, UnmappedFieldsPattern pattern) {
        if (plan instanceof Fork fork && isForkCommand(fork)) {
            return fork.replaceChildren(
                fork.children().stream().map(c -> annotate(c, computeUnmappedFieldsToKeep(c).intersect(pattern))).toList()
            );
        }
        if (pattern.isNone()) {
            return plan;
        }
        if (plan instanceof EsRelation esr) {
            return stamp(esr, pattern);
        }
        return plan.replaceChildren(plan.children().stream().map(c -> annotate(c, pattern)).toList());
    }

    /** Checks the plan is an actual {@code FORK} command, and not one of its subtypes (which aren't {@code FORK} commands). */
    private static boolean isForkCommand(Fork fork) {
        return fork.getClass() == Fork.class;
    }

    private static EsRelation stamp(EsRelation esr, UnmappedFieldsPattern pattern) {
        return pattern.isNone() || esr.indexMode() == IndexMode.LOOKUP
            ? esr
            : esr.withAdditionalAttribute(new UnmappedFieldsAttribute(Source.EMPTY, pattern));
    }

    /**
     * FORK alignment {@link Project}s snapshot their projections before this rule adds
     * {@code $$unmapped_fields}. {@link ResolvingProject} already re-appends it; other Projects
     * would otherwise drop the column before the coordinator can expand it.
     */
    private static Project passThroughUnmappedFields(Project project) {
        List<UnmappedFieldsAttribute> unmapped = CollectionUtils.collect(project.child().output(), UnmappedFieldsAttribute.class);
        if (unmapped.isEmpty()) {
            return project;
        }
        Set<String> names = new HashSet<>(Expressions.names(project.projections()));
        List<UnmappedFieldsAttribute> missing = unmapped.stream()
            .filter(attr -> names.contains(attr.name()) == false)
            .collect(Collectors.toList());
        return missing.isEmpty() ? project : project.withProjections(CollectionUtils.combine(project.projections(), missing));
    }

    /**
     * Branches whose pattern is {@link UnmappedFieldsPattern#NONE} never get {@code $$unmapped_fields} on the relation. If a sibling did,
     * append a null column so FORK layouts match, then refresh FORK output so the coordinator sees the {@link UnmappedFieldsAttribute}
     * subtype. In other words, we only pad if at least one child has the attribute and at least one does not.
     */
    private static LogicalPlan finishForkUnmappedFields(Fork fork) {
        if (isForkCommand(fork) == false) {
            return fork;
        }
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
        Fork result = hasChildWithoutUnmappedFields && hasChildWithUnmappedFields ? fork.replaceSubPlans(newChildren) : fork;
        return hasChildWithUnmappedFields ? result.refreshOutput() : result;
    }

    private static Eval padNullUnmappedFields(LogicalPlan child) {
        Alias alias = new Alias(Source.EMPTY, UnmappedFieldsAttribute.ATTRIBUTE_NAME, new Literal(Source.EMPTY, null, DataType.KEYWORD));
        return new Eval(Source.EMPTY, child, List.of(alias));
    }
}
