/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.LookupJoinExec;
import org.elasticsearch.xpack.esql.plan.physical.NestedExpandExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plan.physical.UnaryExec;

import java.util.ArrayList;
import java.util.List;

/**
 * {@code NESTED_EXPAND} reads each parent's child documents through the parent's {@code _doc}. This keeps {@code _doc}
 * through the projections below it, which would otherwise drop it as unused, and through the commands that would replace
 * it with a column of the query also named {@code _doc}.
 */
public final class LoadNestedFromIndex extends PhysicalOptimizerRules.OptimizerRule<PhysicalPlan> {
    /** A name no column of a query can take, for {@code _doc} where a column of the query takes its own. */
    private static final String CARRIED_DOC = Attribute.rawTemporaryName("nested_expand", EsQueryExec.DOC_ID_FIELD.getName());

    @Override
    protected PhysicalPlan rule(PhysicalPlan plan) {
        if (plan instanceof NestedExpandExec == false) {
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
            Attribute doc = doc(left);
            if (doc == null) {
                return plan;
            }
            PhysicalPlan replaced = join.replaceChildren(left, join.right());
            if (doc(replaced) == null) {
                replaced = join.replaceChildren(carryDoc(left, doc), join.right());
            }
            return doc(replaced) == null ? plan : replaced;
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
            projections.add(namesADocColumn(project.output()) ? new Alias(doc.source(), CARRIED_DOC, doc, null, true) : doc);
            return new ProjectExec(project.source(), child, projections);
        }
        PhysicalPlan replaced = unary.replaceChild(child);
        if (doc(replaced) == null) {
            replaced = unary.replaceChild(carryDoc(child, doc));
        }
        return doc(replaced) == null ? plan : replaced;
    }

    /** Adds {@code doc} to the output of {@code plan} under a name no command above it can give a column. */
    private static PhysicalPlan carryDoc(PhysicalPlan plan, Attribute doc) {
        List<NamedExpression> projections = new ArrayList<>(plan.output());
        projections.add(new Alias(doc.source(), CARRIED_DOC, doc, null, true));
        return new ProjectExec(doc.source(), plan, projections);
    }

    private static boolean namesADocColumn(List<Attribute> output) {
        String docName = EsQueryExec.DOC_ID_FIELD.getName();
        return output.stream().anyMatch(attribute -> attribute.name().equals(docName));
    }

    private static Attribute doc(PhysicalPlan plan) {
        return plan.output().stream().filter(EsQueryExec::isDocAttribute).findFirst().orElse(null);
    }
}
