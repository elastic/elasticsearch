/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.NestedExpandExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plan.physical.UnaryExec;

import java.util.ArrayList;
import java.util.List;

/**
 * Keeps {@code _doc} through the projections below NESTED_EXPAND on a data node, so it can read each parent's child
 * documents from the index. Without {@code _doc} it reads them from {@code _source}.
 */
public final class KeepDocForNestedExpand extends PhysicalOptimizerRules.OptimizerRule<NestedExpandExec> {
    @Override
    protected PhysicalPlan rule(NestedExpandExec expand) {
        PhysicalPlan child = keepDoc(expand.child());
        return child == expand.child() ? expand : expand.replaceChild(child);
    }

    private static PhysicalPlan keepDoc(PhysicalPlan plan) {
        if (doc(plan) != null || plan instanceof UnaryExec == false) {
            return plan;
        }
        UnaryExec unary = (UnaryExec) plan;
        PhysicalPlan child = keepDoc(unary.child());
        Attribute doc = doc(child);
        if (doc == null) {
            return plan;
        }
        if (unary instanceof ProjectExec project) {
            List<NamedExpression> projections = new ArrayList<>(project.projections());
            projections.add(doc);
            return new ProjectExec(project.source(), child, projections);
        }
        PhysicalPlan replaced = unary.replaceChild(child);
        return doc(replaced) == null ? plan : replaced;
    }

    private static Attribute doc(PhysicalPlan plan) {
        return plan.output().stream().filter(EsQueryExec::isDocAttribute).findFirst().orElse(null);
    }
}
