/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.plan.logical.BinaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MergePlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.UnaryOperator;

/**
 * Shared helpers for the {@code TranslateTimeSeries*} rules.
 */
final class TranslateTimeSeriesUtils {

    private TranslateTimeSeriesUtils() {}

    /**
     * Applies {@code transformer} to every {@code EsRelation} reachable through the main input path of {@code plan}. The right-hand side
     * of a {@code BinaryPlan} (a lookup index, or an {@code IN}-subquery rewritten to a {@code SemiJoin}) is intentionally skipped: those
     * subtrees are separate time-series sources with their own time-series metadata, which the outer aggregate must not modify. Crossing
     * that boundary would otherwise inject the outer aggregate's metadata into a nested subquery relation that already carries its own,
     * producing a relation with duplicate metadata attributes.
     */
    static LogicalPlan transformTimeSeriesSource(LogicalPlan plan, UnaryOperator<EsRelation> transformer) {
        if (plan instanceof EsRelation relation) {
            return transformer.apply(relation);
        }
        if (plan instanceof BinaryPlan binary) {
            LogicalPlan newLeft = transformTimeSeriesSource(binary.left(), transformer);
            return newLeft == binary.left() ? binary : binary.replaceLeft(newLeft);
        }
        List<LogicalPlan> newChildren = new ArrayList<>(plan.children().size());
        boolean changed = false;
        for (LogicalPlan child : plan.children()) {
            LogicalPlan newChild = transformTimeSeriesSource(child, transformer);
            changed |= newChild != child;
            newChildren.add(newChild);
        }
        return changed ? plan.replaceChildren(newChildren) : plan;
    }

    /**
     * Makes {@code attributes} available to an aggregate over {@code plan}: adds them to every {@code EsRelation} reachable through the
     * main input path, and carries them up to the output of {@code plan}. Like {@link #transformTimeSeriesSource}, it never descends into
     * the right-hand side of a {@code BinaryPlan}.
     * <p>
     * The time-series rules run after the analyzer has fixed the outputs of the plans between the source and the aggregate, so the new
     * attributes have to be carried up through them explicitly:
     * <ul>
     *   <li>A {@code Project} (e.g. a {@code KEEP}) keeps each attribute that its input has.</li>
     *   <li>A {@code MergePlan} (e.g. a {@code FORK}) gets each attribute as one more output column. Its branches read their own copies
     *   of the source, so each branch produces its own attribute for that column, given by {@code branchAttribute}. The merge lines the
     *   branch outputs up with its columns by name.</li>
     * </ul>
     *
     * @param addToRelation adds the attributes to a source relation, returning the relation itself when it already has them
     * @param branchAttribute the attribute that {@code branch} of a merge produces for the merge column {@code attribute}
     */
    static LogicalPlan addToTimeSeriesSource(
        LogicalPlan plan,
        List<? extends Attribute> attributes,
        BiFunction<EsRelation, List<? extends Attribute>, EsRelation> addToRelation,
        BiFunction<LogicalPlan, Attribute, Attribute> branchAttribute
    ) {
        if (plan instanceof EsRelation relation) {
            return addToRelation.apply(relation, attributes);
        }
        if (plan instanceof MergePlan merge) {
            List<LogicalPlan> branches = new ArrayList<>(merge.children().size());
            for (LogicalPlan branch : merge.children()) {
                List<Attribute> branchAttributes = new ArrayList<>(attributes.size());
                for (Attribute attribute : attributes) {
                    branchAttributes.add(branchAttribute.apply(branch, attribute));
                }
                branches.add(addToTimeSeriesSource(branch, branchAttributes, addToRelation, branchAttribute));
            }
            List<Attribute> output = new ArrayList<>(merge.output());
            for (Attribute attribute : attributes) {
                if (merge.outputSet().contains(attribute) == false) {
                    output.add(attribute);
                }
            }
            return merge.replaceSubPlansAndOutput(branches, output);
        }
        if (plan instanceof BinaryPlan binary) {
            LogicalPlan newLeft = addToTimeSeriesSource(binary.left(), attributes, addToRelation, branchAttribute);
            return newLeft == binary.left() ? binary : binary.replaceLeft(newLeft);
        }
        List<LogicalPlan> newChildren = new ArrayList<>(plan.children().size());
        boolean changed = false;
        for (LogicalPlan child : plan.children()) {
            LogicalPlan newChild = addToTimeSeriesSource(child, attributes, addToRelation, branchAttribute);
            changed |= newChild != child;
            newChildren.add(newChild);
        }
        LogicalPlan newPlan = changed ? plan.replaceChildren(newChildren) : plan;
        if (newPlan instanceof Project project) {
            List<NamedExpression> projections = new ArrayList<>(project.projections());
            for (Attribute attribute : attributes) {
                if (project.outputSet().contains(attribute) == false && project.inputSet().contains(attribute)) {
                    projections.add(attribute);
                }
            }
            if (projections.size() != project.projections().size()) {
                return project.withProjections(projections);
            }
        }
        return newPlan;
    }
}
