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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * {@code NESTED_EXPAND} reads each parent's child documents through the parent's {@code _doc}. This keeps {@code _doc}
 * through the projections below it, which would otherwise drop it as unused, and through the commands that would replace
 * it with a column of the query also named {@code _doc}.
 */
public final class LoadNestedFromIndex extends PhysicalOptimizerRules.OptimizerRule<PhysicalPlan> {
    /** The name for {@code _doc} where a column of the query takes its own, made unique by {@link #carriedDocName}. */
    private static final String CARRIED_DOC = Attribute.rawTemporaryName("nested_expand", EsQueryExec.DOC_ID_FIELD.getName());

    @Override
    protected PhysicalPlan rule(PhysicalPlan plan) {
        if (plan instanceof NestedExpandExec == false) {
            return plan;
        }
        UnaryExec unary = (UnaryExec) plan;
        PhysicalPlan child = keepDoc(unary.child(), carriedDocName(plan));
        return child == unary.child() ? plan : unary.replaceChild(child);
    }

    /**
     * A name for {@code _doc} that no column in {@code plan} has. Commands merge their output by name, so a column of the
     * query with the carried name would replace {@code _doc}, and the {@code $$} prefix does not keep a query from using it.
     */
    private static String carriedDocName(PhysicalPlan plan) {
        Set<String> names = new HashSet<>();
        plan.forEachDown(node -> node.output().forEach(attribute -> names.add(attribute.name())));
        String name = CARRIED_DOC;
        for (int suffix = 1; names.contains(name); suffix++) {
            name = CARRIED_DOC + "$" + suffix;
        }
        return name;
    }

    private static PhysicalPlan keepDoc(PhysicalPlan plan, String carriedName) {
        if (doc(plan) != null) {
            return plan;
        }
        if (plan instanceof LookupJoinExec join) {
            PhysicalPlan left = keepDoc(join.left(), carriedName);
            Attribute doc = doc(left);
            if (doc == null) {
                return plan;
            }
            PhysicalPlan replaced = join.replaceChildren(left, join.right());
            if (doc(replaced) == null) {
                replaced = join.replaceChildren(carryDoc(left, doc, carriedName), join.right());
            }
            return doc(replaced) == null ? plan : replaced;
        }
        if (plan instanceof UnaryExec == false) {
            return plan;
        }
        UnaryExec unary = (UnaryExec) plan;
        PhysicalPlan child = keepDoc(unary.child(), carriedName);
        Attribute doc = doc(child);
        if (doc == null) {
            return plan;
        }
        if (unary instanceof ProjectExec project) {
            List<NamedExpression> projections = new ArrayList<>(project.projections());
            projections.add(namesADocColumn(project.output()) ? new Alias(doc.source(), carriedName, doc, null, true) : doc);
            return new ProjectExec(project.source(), child, projections);
        }
        PhysicalPlan replaced = unary.replaceChild(child);
        if (doc(replaced) == null) {
            replaced = unary.replaceChild(carryDoc(child, doc, carriedName));
        }
        return doc(replaced) == null ? plan : replaced;
    }

    /** Adds {@code doc} to the output of {@code plan} as {@code carriedName}, which no column above it has. */
    private static PhysicalPlan carryDoc(PhysicalPlan plan, Attribute doc, String carriedName) {
        List<NamedExpression> projections = new ArrayList<>(plan.output());
        projections.add(new Alias(doc.source(), carriedName, doc, null, true));
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
