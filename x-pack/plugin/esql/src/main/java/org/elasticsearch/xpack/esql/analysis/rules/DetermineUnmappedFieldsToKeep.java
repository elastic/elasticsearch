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
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.InlineStats;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnmappedFieldsAttribute;
import org.elasticsearch.xpack.esql.plan.logical.UnmappedFieldsPattern;
import org.elasticsearch.xpack.esql.plan.logical.local.ResolvingProject;
import org.elasticsearch.xpack.esql.rule.ParameterizedRule;

import java.util.ArrayList;
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
        LogicalPlan annotated = plan.transformUp(EsRelation.class, esr -> {
            if (esr.indexMode() == IndexMode.LOOKUP) {
                return esr;
            }
            return esr.withAdditionalAttribute(new UnmappedFieldsAttribute(Source.EMPTY, pattern));
        });
        // Captured while the ResolvingProjects still hold their resolvers - ResolvedProjects strips them two rules later.
        context.unmappedFieldsOrdering(leaves -> withLeavesInPlaceOfSyntheticColumn(annotated, leaves).output());
        return annotated;
    }

    /**
     * The plan with {@code leaves} standing in for the synthetic column, so asking it for its output re-runs every projection
     * against a relation shaped exactly as it would have been had those fields been mapped: {@code ResolvingProject#replaceChild}
     * re-invokes the real KEEP/DROP/RENAME resolvers, and EVAL and friends recompute their output on top. Nothing is mirrored.
     */
    private static LogicalPlan withLeavesInPlaceOfSyntheticColumn(LogicalPlan annotated, List<Attribute> leaves) {
        return annotated.transformUp(EsRelation.class, esr -> {
            List<Attribute> real = new ArrayList<>(esr.output().size());
            boolean carriesSyntheticColumn = false;
            for (Attribute a : esr.output()) {
                if (a instanceof UnmappedFieldsAttribute) {
                    carriesSyntheticColumn = true;
                } else {
                    real.add(a);
                }
            }
            // The synthetic column is appended last, so the leaves land where it sat: still relation columns, hence ahead of
            // anything a later EVAL adds.
            return carriesSyntheticColumn ? esr.withAttributes(real).withAdditionalAttributes(leaves) : esr;
        });
    }

    /**
     * Computes the {@link UnmappedFieldsPattern} describing which additional (unreferenced and currently unmapped)
     * {@code _source} fields would survive to the output of {@code plan} if it was in the output of an {@code EsRelation} that is the
     * plan's only leaf. (Does not cover n-ary plans.)
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
}
