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
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.local.ResolvingProject;
import org.elasticsearch.xpack.esql.rule.ParameterizedRule;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

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

    private final Consumer<UnmappedFieldsOrdering> registerUnmappedFieldsOrdering;

    public DetermineUnmappedFieldsToKeep(Consumer<UnmappedFieldsOrdering> registerUnmappedFieldsOrdering) {
        this.registerUnmappedFieldsOrdering = registerUnmappedFieldsOrdering;
    }

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

        registerUnmappedFieldsOrdering.accept(leaves -> withLeavesInPlaceOfSyntheticColumn(annotated, leaves).output());
        return annotated;
    }

    /**
     * The plan with {@code leaves} standing in for the synthetic column, so asking it for its output re-runs every projection
     * against a relation shaped exactly as it would have been had those fields been mapped: {@code ResolvingProject#replaceChild}
     * re-invokes the real KEEP/DROP/RENAME resolvers, and EVAL and friends recompute their output on top
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
        });
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
     * For {@link Join}, only the left side is recursed into.
     * Other non-unary plans fall back to {@link UnmappedFieldsPattern#ALL}; those queries are rejected by the
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
}
