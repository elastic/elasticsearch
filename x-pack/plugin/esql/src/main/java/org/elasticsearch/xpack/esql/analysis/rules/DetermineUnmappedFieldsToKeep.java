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
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnmappedFieldsAttribute;
import org.elasticsearch.xpack.esql.plan.logical.UnmappedFieldsPattern;
import org.elasticsearch.xpack.esql.plan.logical.local.ResolvingProject;
import org.elasticsearch.xpack.esql.rule.ParameterizedRule;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
        Map<String, String> renames = outermostRenames(plan);
        return plan.transformUp(EsRelation.class, esr -> {
            if (esr.indexMode() == IndexMode.LOOKUP) {
                return esr;
            }
            return esr.withAdditionalAttribute(new UnmappedFieldsAttribute(Source.EMPTY, pattern, keepOrder, renames));
        });
    }

    private static List<UnmappedFieldsPattern.KeepTerm> outermostKeepOrder(LogicalPlan plan) {
        for (LogicalPlan p = plan; p instanceof UnaryPlan unary; p = unary.child()) {
            if (p instanceof ResolvingProject project) {
                // return the last seen KEEP command's projections
                if (project.isKeep()) {
                    return project.keepOrderTerms();
                }
            }
        }
        return List.of();
    }

    private static Map<String, String> outermostRenames(LogicalPlan plan) {
        Map<String, String> renames = new HashMap<>();
        for (LogicalPlan p = plan; p instanceof UnaryPlan unary; p = unary.child()) {
            if (p instanceof ResolvingProject project) {
                if (project.isKeep()) {
                    break;
                }

                for (NamedExpression ne : project.projections()) {
                    if (ne instanceof Alias alias && alias.child() instanceof NamedExpression orig) {
                        String newName = alias.name();
                        String originalName = orig.name();
                        boolean boundByOuterRename = renames.containsKey(newName) || renames.containsValue(newName);
                        renames.replaceAll((k, v) -> v.equals(newName) ? originalName : v);
                        if (boundByOuterRename == false) {
                            renames.put(newName, originalName);
                        }
                    }
                }
            }
        }
        return renames.isEmpty() ? Map.of() : Map.copyOf(renames);
    }

    /**
     * Computes the {@link UnmappedFieldsPattern} describing which additional (unreferenced and currently unmapped)
     * {@code _source} fields would survive to the output of {@code plan} if it was in the output of an {@code EsRelation} that is the
     * plan's only leaf. (Does not cover n-ary plans.)
     */
    private static UnmappedFieldsPattern computeUnmappedFieldsToKeep(LogicalPlan plan) {
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
