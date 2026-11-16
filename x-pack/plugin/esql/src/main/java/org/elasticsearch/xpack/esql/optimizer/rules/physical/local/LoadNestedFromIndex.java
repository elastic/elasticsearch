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
import org.elasticsearch.xpack.esql.plan.physical.LookupJoinExec;
import org.elasticsearch.xpack.esql.plan.physical.NestedExpandExec;
import org.elasticsearch.xpack.esql.plan.physical.NestedReduceExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plan.physical.UnaryExec;

import java.util.ArrayList;
import java.util.List;

/**
 * NESTED_EXPAND, NESTED_FLATTEN and NESTED reductions read each parent's child documents through the parent's {@code _doc}.
 * This keeps {@code _doc} through the projections below them, which would otherwise drop it as unused.
 */
public final class LoadNestedFromIndex extends PhysicalOptimizerRules.OptimizerRule<PhysicalPlan> {
    @Override
    protected PhysicalPlan rule(PhysicalPlan plan) {
        if ((plan instanceof NestedExpandExec || plan instanceof NestedReduceExec) == false) {
            return plan;
        }
        UnaryExec unary = (UnaryExec) plan;
        PhysicalPlan child = keepDoc(unary.child());
        return child == unary.child() ? plan : unary.replaceChild(child);
    }

    private static PhysicalPlan keepDoc(PhysicalPlan plan) {
        if (doc(plan) != null) {
            return plan;
        }
        if (plan instanceof LookupJoinExec join) {
            PhysicalPlan left = keepDoc(join.left());
            return left == join.left() ? plan : join.replaceChildren(left, join.right());
        }
        if (plan instanceof UnaryExec == false) {
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
