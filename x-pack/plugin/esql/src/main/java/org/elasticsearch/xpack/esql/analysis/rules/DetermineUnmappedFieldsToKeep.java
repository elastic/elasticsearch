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
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnmappedFieldsAttribute;
import org.elasticsearch.xpack.esql.plan.logical.UnmappedFieldsPattern;
import org.elasticsearch.xpack.esql.plan.logical.local.ResolvingProject;
import org.elasticsearch.xpack.esql.rule.ParameterizedRule;

import java.util.List;

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
        UnmappedFieldsPattern pattern = computeUnmappedFieldsToKeep(plan);
        if (pattern.isNone()) {
            return plan;
        }
        List<UnmappedFieldsPattern.KeepTerm> keepOrder = outermostKeepOrder(plan);
        return plan.transformUp(EsRelation.class, esr -> {
            if (esr.indexMode() == IndexMode.LOOKUP) {
                return esr;
            }
            return esr.withAdditionalAttribute(new UnmappedFieldsAttribute(Source.EMPTY, pattern, keepOrder));
        });
    }

    /**
     * The projection terms of the top-most {@code KEEP}, in written order, or empty when no {@code KEEP} governs the output order.
     * The coordinator replays them over the real columns plus the expanded leaves so a {@code LOAD_ALL} output honors {@code KEEP}'s
     * left-to-right column contract (see {@link UnmappedFieldsPattern#keepOrdered}).
     * <p>
     * Only the top-most projection is consulted, and only when it is a {@code KEEP}: a {@code DROP}/{@code RENAME} above it would
     * reorder or rename columns in ways these name-based terms can no longer describe, so the output falls back to the natural
     * real-then-alphabetical order (unchanged from before this ordering support). Non-projection commands ({@code EVAL}, {@code WHERE},
     * {@code SORT}, {@code LIMIT}) do not change which projection governs order, so the walk descends through them to that top
     * {@code KEEP}; a column an {@code EVAL} appended above the {@code KEEP} trails its output at the coordinator (it did not exist when
     * {@code KEEP} ran — see {@link UnmappedFieldsPattern#keepOrdered} and the post-processor's layout). The plan is a linear unary
     * chain here — {@code LOAD_ALL} currently rejects non-unary plans in the {@code Verifier}.
     */
    private static List<UnmappedFieldsPattern.KeepTerm> outermostKeepOrder(LogicalPlan plan) {
        for (LogicalPlan p = plan; p instanceof UnaryPlan unary; p = unary.child()) {
            if (p instanceof ResolvingProject project) {
                return project.isKeep() ? project.keepOrderTerms() : List.of();
            }
        }
        return List.of();
    }

    /**
     * Computes the {@link UnmappedFieldsPattern} describing which additional (currently unmapped)
     * source fields would survive to the output of {@code plan}.
     * <p>
     * Two things restrict the pattern. KEEP/DROP/RENAME (as {@link ResolvingProject}) contribute the
     * include/exclude patterns they were written with: each one adds a single OR group, while
     * {@link UnmappedFieldsPattern#intersect} applies AND across chained commands. And every name that any
     * node in the plan outputs is excluded: a mapped field, a name the query introduced (EVAL's aliases,
     * RENAME's targets) and the synthetic {@code _unmapped_fields} column are all already columns of their
     * own, so expanding a source field of that name would collide with them.
     * <p>
     * Non-unary plans fall back to {@link UnmappedFieldsPattern#ALL} so no field is ever accidentally
     * suppressed; those queries are currently (and temporarily) rejected by the {@code Verifier}'s {@code LOAD_ALL} command allow-list.
     */
    private static UnmappedFieldsPattern computeUnmappedFieldsToKeep(LogicalPlan plan) {
        UnmappedFieldsPattern fromChild = plan instanceof UnaryPlan unary
            ? computeUnmappedFieldsToKeep(unary.child())
            : UnmappedFieldsPattern.ALL;
        UnmappedFieldsPattern restricted = plan instanceof ResolvingProject project
            ? project.unmappedFieldsPattern().intersect(fromChild)
            : fromChild;
        return restricted.withAdditionalExcludes(Expressions.names(plan.output()));
    }
}
