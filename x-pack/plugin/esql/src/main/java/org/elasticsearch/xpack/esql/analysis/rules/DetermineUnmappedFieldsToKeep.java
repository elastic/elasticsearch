/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis.rules;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.analysis.UnmappedResolution;
import org.elasticsearch.xpack.esql.core.expression.Alias;
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
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnmappedFieldsAttribute;
import org.elasticsearch.xpack.esql.plan.logical.UnmappedFieldsPattern;
import org.elasticsearch.xpack.esql.plan.logical.local.ResolvingProject;
import org.elasticsearch.xpack.esql.rule.ParameterizedRule;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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

    @Override
    public LogicalPlan apply(LogicalPlan plan, AnalyzerContext context) {
        if (context.unmappedResolution().loadsAllUnmappedFields() == false) {
            return plan;
        }
        LogicalPlan annotated = annotate(plan, computeUnmappedFieldsToKeep(plan));
        annotated = annotated.transformUp(Project.class, DetermineUnmappedFieldsToKeep::passThroughUnmappedFields);
        annotated = annotated.transformUp(Fork.class, DetermineUnmappedFieldsToKeep::padForkChildren);
        annotated = annotated.transformUp(Fork.class, DetermineUnmappedFieldsToKeep::refreshForkOutput);
        // A second pass picks up $$unmapped_fields on Projects above the FORK, whose child output
        // only includes the column after refreshForkOutput.
        return annotated.transformUp(Project.class, DetermineUnmappedFieldsToKeep::passThroughUnmappedFields);
    }

    /**
     * Computes the {@link UnmappedFieldsPattern} describing which additional (unreferenced and currently unmapped)
     * {@code _source} fields would survive to the output of {@code plan} if it was in the output of an {@code EsRelation} that is the
     * plan's only leaf.
     * <p>
     * {@link Fork} is treated as transparent here (no include restriction beyond excluding its output names). Branch-local
     * {@code KEEP}/{@code DROP} patterns are applied in {@link #annotate} by recomputing this for each child.
     */
    private static UnmappedFieldsPattern computeUnmappedFieldsToKeep(LogicalPlan plan) {
        if (plan instanceof InlineStats inlineStats) {
            // INLINE STATS preserves input rows via a left join with its Aggregate. Walk the input, not the
            // Aggregate: that node returns NONE so STATS can drop expansion, which must not apply here.
            UnmappedFieldsPattern fromChild = computeUnmappedFieldsToKeep(inlineStats.aggregate().child());
            return fromChild.withAdditionalExcludes(Expressions.names(plan.output()));
        }
        if (plan instanceof Aggregate) {
            return UnmappedFieldsPattern.NONE;
        }
        UnmappedFieldsPattern fromChild = plan instanceof UnaryPlan unary
            ? computeUnmappedFieldsToKeep(unary.child())
            : UnmappedFieldsPattern.ALL;
        UnmappedFieldsPattern restricted = plan instanceof ResolvingProject project
            ? project.unmappedFieldsPattern().intersect(fromChild)
            : fromChild;
        return restricted.withAdditionalExcludes(Expressions.names(plan.output()));
    }

    /**
     * Stamps {@link UnmappedFieldsAttribute} onto non-LOOKUP {@link EsRelation}s. {@link Fork} is the
     * other special case: each branch is annotated with its own pattern. Every other node is only
     * walked to reach those two.
     */
    private static LogicalPlan annotate(LogicalPlan plan, UnmappedFieldsPattern pattern) {
        if (plan instanceof Fork fork && (plan instanceof UnionAll) == false) {
            List<LogicalPlan> newChildren = new ArrayList<>(fork.children().size());
            for (LogicalPlan child : fork.children()) {
                newChildren.add(annotate(child, computeUnmappedFieldsToKeep(child).intersect(pattern)));
            }
            return fork.replaceChildren(newChildren);
        }
        if (pattern.isNone()) {
            return plan;
        }
        if (plan instanceof EsRelation esr) {
            return stamp(esr, pattern);
        }
        if (plan.anyMatch(p -> p instanceof Fork && (p instanceof UnionAll) == false)) {
            return plan.replaceChildren(plan.children().stream().map(c -> annotate(c, pattern)).toList());
        }
        return plan.transformUp(EsRelation.class, esr -> stamp(esr, pattern));
    }

    private static EsRelation stamp(EsRelation esr, UnmappedFieldsPattern pattern) {
        if (pattern.isNone() || esr.indexMode() == IndexMode.LOOKUP) {
            return esr;
        }
        return esr.withAdditionalAttribute(new UnmappedFieldsAttribute(Source.EMPTY, pattern));
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
        List<UnmappedFieldsAttribute> missing = new ArrayList<>();
        for (UnmappedFieldsAttribute attr : unmapped) {
            if (names.contains(attr.name()) == false) {
                missing.add(attr);
            }
        }
        if (missing.isEmpty()) {
            return project;
        }
        return project.withProjections(CollectionUtils.combine(project.projections(), missing));
    }

    /**
     * Branches whose pattern is {@link UnmappedFieldsPattern#NONE} never get {@code $$unmapped_fields}
     * on the relation. If a sibling did, append a null column so FORK layouts match and the coordinator
     * can expand extras from the branches that loaded them.
     */
    private static Fork padForkChildren(Fork fork) {
        if (fork instanceof UnionAll) {
            return fork;
        }
        List<LogicalPlan> padded = padNullUnmappedFields(fork.children());
        if (padded == fork.children()) {
            return fork;
        }
        return (Fork) fork.replaceChildren(padded);
    }

    private static List<LogicalPlan> padNullUnmappedFields(List<LogicalPlan> children) {
        boolean anyUnmapped = false;
        for (LogicalPlan child : children) {
            if (CollectionUtils.collect(child.output(), UnmappedFieldsAttribute.class).isEmpty() == false) {
                anyUnmapped = true;
                break;
            }
        }
        if (anyUnmapped == false) {
            return children;
        }
        List<LogicalPlan> padded = new ArrayList<>(children.size());
        boolean changed = false;
        for (LogicalPlan child : children) {
            if (Expressions.names(child.output()).contains(UnmappedFieldsAttribute.ATTRIBUTE_NAME)) {
                padded.add(child);
                continue;
            }
            changed = true;
            padded.add(
                new Eval(
                    child.source(),
                    child,
                    List.of(
                        new Alias(
                            child.source(),
                            UnmappedFieldsAttribute.ATTRIBUTE_NAME,
                            new Literal(child.source(), null, DataType.KEYWORD)
                        )
                    )
                )
            );
        }
        return changed ? padded : children;
    }

    private static Fork refreshForkOutput(Fork fork) {
        if (fork instanceof UnionAll) {
            return fork;
        }
        boolean anyUnmapped = false;
        for (LogicalPlan child : fork.children()) {
            if (CollectionUtils.collect(child.output(), UnmappedFieldsAttribute.class).isEmpty() == false) {
                anyUnmapped = true;
                break;
            }
        }
        if (anyUnmapped == false) {
            return fork;
        }
        return fork.refreshOutput();
    }
}
