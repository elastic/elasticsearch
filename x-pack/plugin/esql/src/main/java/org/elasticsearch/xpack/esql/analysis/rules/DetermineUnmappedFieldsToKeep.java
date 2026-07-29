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
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnmappedFieldsAttribute;
import org.elasticsearch.xpack.esql.plan.logical.UnmappedFieldsPattern;
import org.elasticsearch.xpack.esql.plan.logical.local.ResolvingProject;
import org.elasticsearch.xpack.esql.rule.ParameterizedRule;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * When {@code SET unmapped_fields="LOAD_ALL"} is in effect, annotates
 * each non-LOOKUP {@link EsRelation} with an {@link UnmappedFieldsAttribute} carrying the
 * {@link UnmappedFieldsPattern} that describes which additional (currently unmapped) source fields
 * would survive to the query output. Expanding the {@code _unmapped_fields} column into per-field
 * output columns is a coordinator-level post-processing step and does not affect data-node execution.
 *
 * <p>The rule runs in the Finish Analysis batch <em>before</em> {@link ResolvedProjects}, so
 * {@link ResolvingProject} nodes — which carry the original wildcard patterns — are still present.
 * For any other {@link UnmappedResolution} the rule is a no-op.
 */
public class DetermineUnmappedFieldsToKeep extends ParameterizedRule<LogicalPlan, LogicalPlan, AnalyzerContext> {

    @Override
    public LogicalPlan apply(LogicalPlan plan, AnalyzerContext context) {
        if (context.unmappedResolution() != UnmappedResolution.LOAD_ALL) {
            return plan;
        }
        UnmappedFieldsPattern pattern = computeUnmappedFieldsToKeep(plan);
        return plan.transformUp(EsRelation.class, esr -> {
            if (esr.indexMode() == IndexMode.LOOKUP) {
                return esr;
            }
            List<String> outputNames = esr.output().stream().map(Attribute::name).toList();
            UnmappedFieldsPattern refined = pattern.withAdditionalExcludes(outputNames);
            return esr.withAdditionalAttribute(new UnmappedFieldsAttribute(Source.EMPTY, refined));
        });
    }

    /**
     * Computes the {@link UnmappedFieldsPattern} describing which additional (currently unmapped)
     * source fields would survive to the output of {@code plan}.
     * <p>
     * Two things restrict the pattern. KEEP/DROP/RENAME (as {@link ResolvingProject}) contribute the
     * include/exclude patterns they were written with: each one adds a single OR group, while
     * {@link UnmappedFieldsPattern#intersect} applies AND across chained commands. And any node that
     * introduces a new name — EVAL's aliases, RENAME's targets — shadows the source field of that name.
     * <p>
     * Non-unary plans fall back to {@link UnmappedFieldsPattern#ALL} so no field is ever accidentally
     * suppressed; those queries are currently (and temporarily) rejected by the {@code Verifier}'s {@code LOAD_ALL} command allow-list.
     */
    private static UnmappedFieldsPattern computeUnmappedFieldsToKeep(LogicalPlan plan) {
        return switch (plan) {
            case ResolvingProject project -> project.unmappedFieldsPattern().intersect(patternFromChildOf(project));
            case UnaryPlan unary -> patternFromChildOf(unary);
            default -> UnmappedFieldsPattern.ALL;
        };
    }

    private static UnmappedFieldsPattern patternFromChildOf(UnaryPlan plan) {
        return computeUnmappedFieldsToKeep(plan.child()).withAdditionalExcludes(introducedNames(plan));
    }

    // TODO extract; several optimizer rules compare a node's output against its child's to find what it introduces
    private static List<String> introducedNames(UnaryPlan plan) {
        Set<String> childNames = plan.child().output().stream().map(Attribute::name).collect(Collectors.toSet());
        return plan.output().stream().map(Attribute::name).filter(name -> childNames.contains(name) == false).toList();
    }
}
