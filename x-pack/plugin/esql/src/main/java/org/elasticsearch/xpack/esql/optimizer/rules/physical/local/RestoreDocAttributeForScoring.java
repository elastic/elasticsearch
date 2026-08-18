/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.rule.Rule;

import java.util.ArrayList;
import java.util.List;

/**
 * Keeps {@code _doc} in a {@link ProjectExec} when an ancestor needs it.
 *
 * {@code ReplaceFieldWithConstantOrNull} builds its projection from the logical relation
 * output, before {@link ReplaceSourceAttributes} adds {@code _doc}. A surviving projection
 * can therefore drop an attribute required by a later scoring or full-text expression. This
 * rule adds back the attribute from the child. It must reuse that instance because late
 * materialization relies on attribute identity.
 */
public class RestoreDocAttributeForScoring extends Rule<PhysicalPlan, PhysicalPlan> {

    @Override
    public PhysicalPlan apply(PhysicalPlan plan) {
        return restore(plan, false);
    }

    private static PhysicalPlan restore(PhysicalPlan plan, boolean docNeededAbove) {
        boolean docNeededByChildren = docNeededAbove || DocVectorConsumers.consumesDocVector(plan);

        List<PhysicalPlan> children = plan.children();
        List<PhysicalPlan> newChildren = new ArrayList<>(children.size());
        boolean changed = false;
        for (PhysicalPlan child : children) {
            PhysicalPlan newChild = restore(child, docNeededByChildren);
            newChildren.add(newChild);
            changed |= newChild != child;
        }
        PhysicalPlan result = changed ? plan.replaceChildren(newChildren) : plan;

        if (docNeededAbove && result instanceof ProjectExec projectExec) {
            Attribute docAttribute = docAttributeOf(projectExec.child());
            if (docAttribute != null && projectExec.outputSet().contains(docAttribute) == false) {
                List<NamedExpression> newProjections = new ArrayList<>(projectExec.projections());
                newProjections.add(docAttribute);
                result = new ProjectExec(projectExec.source(), projectExec.child(), newProjections);
            }
        }

        return result;
    }

    private static Attribute docAttributeOf(PhysicalPlan plan) {
        for (Attribute attribute : plan.outputSet()) {
            if (EsQueryExec.isDocAttribute(attribute)) {
                return attribute;
            }
        }
        return null;
    }
}
